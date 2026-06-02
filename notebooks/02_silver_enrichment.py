# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 02 · Silver Enrichment
# MAGIC
# MAGIC **Bricksurance SE** — Phase 2. Collapses the seven governed `bronze_*`
# MAGIC tables into **one enriched row per claim**: `silver_claims_enriched` — the
# MAGIC "everything we know before a handler picks up the phone" view, plus the
# MAGIC assembled claim lifecycle and ML training labels for later phases.
# MAGIC
# MAGIC > **About this demo.** Synthetic data only — fictional company, policies and
# MAGIC > figures. No real Guidewire integration, no real customer data.
# MAGIC
# MAGIC **Pattern:** `bronze_*` (governed) → **`silver_claims_enriched` (materialised)**.
# MAGIC
# MAGIC **Implementation:** a standalone notebook writing a managed Delta table —
# MAGIC the enrichment is heavy deterministic business logic (handler routing,
# MAGIC label noise, reserve reconstruction) needing no DLT streaming/expectations,
# MAGIC and this avoids coupling to the Phase 1 pipeline's full-refresh semantics.
# MAGIC
# MAGIC **Deterministic:** all "random" choices derive from `crc32(claim_public_id)`
# MAGIC (no `rand()`), so resets reproduce identically. The vivid claim `cc:900001`
# MAGIC is exempt from label noise.

# COMMAND ----------

import os
import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema (fixed)")

catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
print(f"[target] {catalog}.{schema}")


def tbl(name):
    return f"`{catalog}`.`{schema}`.{name}"


# Reuse the resilient per-key tagger from the Phase 0 helper module.
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_helper_dir = "/Workspace" + os.path.dirname(_ctx.notebookPath().get())
if _helper_dir not in sys.path:
    sys.path.insert(0, _helper_dir)
import claims_data_gen as cdg

VALID_LOSS_CAUSES = ("vehcollision", "waterdamage", "windhail", "fire")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Read bronze + aggregate child tables to claim level

# COMMAND ----------

claim = spark.table(tbl("bronze_gw_cc_claim")).withColumnRenamed("claim_status", "src_claim_status")

# Exposure -> claim level. Bronze carries a single case reserve per exposure;
# we sum across exposures (1 per claim here) into paid + outstanding case reserve.
exposure = (
    spark.table(tbl("bronze_gw_cc_exposure"))
    .groupBy("claim_public_id")
    .agg(
        F.sum("reserve_amount").alias("case_reserve"),
        F.sum("paid_amount").alias("paid_amount"),
    )
)

incident = (
    spark.table(tbl("bronze_gw_cc_incident"))
    .select("claim_public_id", "incident_type", "description_text")
    .dropDuplicates(["claim_public_id"])
)

# Contact -> claimant postcode + third_party_involved (any third_party contact).
contact = (
    spark.table(tbl("bronze_gw_cc_contact"))
    .groupBy("claim_public_id")
    .agg(
        F.max("postcode_district").alias("postcode_district"),
        F.max(F.expr("CASE WHEN contact_role = 'third_party' THEN 1 ELSE 0 END")).alias("_tp"),
    )
)

policy = (
    spark.table(tbl("bronze_gw_pc_policy"))
    .select("policy_number", "product", "sum_insured", "annual_premium",
            "effective_date", "expiry_date")
    .dropDuplicates(["policy_number"])
)

fraud = (
    spark.table(tbl("bronze_fraud_signals_raw"))
    .select("claim_public_id", "fraud_score", "fraud_flag", "prior_claims_12m", "days_since_incident")
    .dropDuplicates(["claim_public_id"])
)

weather = (
    spark.table(tbl("ref_weather_index"))
    .select("postcode_district", "flood_risk_score", "wind_risk_score", "freeze_risk_score")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Join chain (spine = bronze_gw_cc_claim) — left joins keep all claims

# COMMAND ----------

df = (
    claim
    .join(exposure, "claim_public_id", "left")
    .join(incident, "claim_public_id", "left")
    .join(contact, "claim_public_id", "left")
    .join(policy, "policy_number", "left")          # bad-policy rows -> null policy (by design)
    .join(fraud, "claim_public_id", "left")         # quarantined fraud rows -> null (by design)
    .join(weather, "postcode_district", "left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Derived enrichment fields

# COMMAND ----------

df = (
    df
    .withColumn("third_party_involved", F.expr("coalesce(_tp, 0) = 1"))
    .withColumn(
        "peril_type",
        F.expr("""
            CASE loss_cause
                WHEN 'vehcollision' THEN 'motor_tp'
                WHEN 'waterdamage'  THEN 'home_escape_water'
                WHEN 'windhail'     THEN 'home_storm'
                WHEN 'fire'         THEN 'home_fire'
            END
        """),
    )
    .withColumn("reporting_lag_days", F.expr("datediff(report_date, loss_date)"))
    .withColumn("policy_tenure_years", F.expr("round(datediff(loss_date, effective_date) / 365.25, 2)"))
    # product from policy where available, else inferred from peril (keeps non-null).
    .withColumn("product", F.expr("coalesce(product, CASE WHEN peril_type = 'motor_tp' THEN 'motor' ELSE 'home' END)"))
    .withColumn("sum_insured_to_reported_ratio",
                F.expr("round(total_incurred / nullif(sum_insured, 0), 4)"))
    .withColumn("is_high_value", F.expr("total_incurred > 10000"))
    .withColumn(
        "is_potential_fraud",
        F.expr("coalesce(fraud_score > 70, false) "
               "OR coalesce(prior_claims_12m > 2 AND days_since_incident > 14, false)"),
    )
    .withColumn(
        "at_fault",
        # Motor only (null for home); deterministic ~55% at-fault.
        F.expr("CASE WHEN peril_type = 'motor_tp' "
               "THEN pmod(crc32(concat(claim_public_id, '|fault')), 100) < 55 ELSE NULL END"),
    )
    # Weather composite, peril-weighted blend on 0–10 risk scores.
    .withColumn(
        "weather_risk_composite",
        F.expr("""
            round(CASE peril_type
                WHEN 'home_escape_water' THEN 0.45*freeze_risk_score + 0.40*flood_risk_score + 0.15*wind_risk_score
                WHEN 'home_storm'        THEN 0.70*wind_risk_score   + 0.20*flood_risk_score + 0.10*freeze_risk_score
                WHEN 'home_fire'         THEN (flood_risk_score + wind_risk_score + freeze_risk_score) / 3.0
                ELSE                          (flood_risk_score + wind_risk_score + freeze_risk_score) / 3.0
            END, 2)
        """),
    )
    # Recovery / subrogation signals (Phase 11): a motor third-party loss where
    # OUR policyholder is NOT at fault can be recovered from the third party's
    # insurer. recoverable_amount ≈ 60% of the incurred for those claims.
    .withColumn(
        "third_party_at_fault",
        F.expr("peril_type = 'motor_tp' AND coalesce(third_party_involved, false) "
               "AND coalesce(at_fault, true) = false"),
    )
    .withColumn(
        "recovery_flag",
        F.expr("coalesce(third_party_at_fault, false)"),
    )
    .withColumn(
        "recoverable_amount",
        F.expr("CASE WHEN coalesce(third_party_at_fault, false) "
               "THEN CAST(round(total_incurred * 0.6) AS decimal(12,2)) ELSE CAST(0 AS decimal(12,2)) END"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Claim lifecycle (reserves, status, settlement, leakage)

# COMMAND ----------

df = (
    df
    .withColumn("paid_amount", F.expr("coalesce(paid_amount, 0)"))
    .withColumn("case_reserve", F.expr("coalesce(case_reserve, 0)"))
    # Ultimate incurred = paid to date + outstanding case reserve (under-reserved
    # for home escape-of-water by construction in Phase 0).
    .withColumn("ultimate_reserve", F.expr("CAST(paid_amount + case_reserve AS decimal(12,2))"))
    # Opening (initial) reserve reconstructed deterministically — the CDA landing
    # carries a single booked reserve, so we model a plausible opening estimate
    # (escape-of-water biased lower). Feeds the reserve-development story.
    .withColumn(
        "_opening_factor",
        F.expr("greatest(0.30, 0.60 + pmod(crc32(concat(claim_public_id, '|open')), 70) / 100.0 "
               "- CASE WHEN peril_type = 'home_escape_water' THEN 0.15 ELSE 0 END)"),
    )
    .withColumn("initial_reserve", F.expr("CAST(round(ultimate_reserve * _opening_factor) AS decimal(12,2))"))
    # Severity tier (claim size) from ultimate_reserve — the PRIMARY driver of
    # both settle time and leakage; report_channel is layered on as a multiplier.
    .withColumn("_severity", F.expr("""
        CASE WHEN ultimate_reserve < 2000  THEN 'low'
             WHEN ultimate_reserve < 10000 THEN 'medium'
             WHEN ultimate_reserve < 50000 THEN 'high'
             ELSE 'large_loss' END
    """))
)

# --- days_to_settle: peril+severity base, report_channel as a MULTIPLIER ---
# Base settle time is driven by peril and claim size. report_channel scales it
# (digital STP ~0.55x, broker ~0.85x, phone ~1.15x) with deterministic jitter so
# it's an aggregate tendency, not a fixed rule. Large/complex losses dampen the
# channel effect (they settle slowly through ANY channel). Deterministic by seed.
df = (
    df
    .withColumn("_peril_base", F.expr("""
        CASE peril_type WHEN 'motor_tp' THEN 22 WHEN 'home_escape_water' THEN 35
                        WHEN 'home_storm' THEN 33 WHEN 'home_fire' THEN 48 ELSE 30 END"""))
    .withColumn("_severity_add", F.expr("""
        CASE _severity WHEN 'low' THEN 0 WHEN 'medium' THEN 12 WHEN 'high' THEN 45 ELSE 95 END"""))
    .withColumn("_settle_noise", F.expr("pmod(crc32(concat(claim_public_id, '|settlenoise')), 28) - 8"))
    .withColumn("_base_days", F.expr("greatest(_peril_base + _severity_add + _settle_noise, 4)"))
    .withColumn("_chan_mult", F.expr("""
        (CASE report_channel WHEN 'digital' THEN 0.55 WHEN 'broker_email' THEN 0.85
                             WHEN 'phone' THEN 1.15 ELSE 1.0 END)
        * (0.92 + pmod(crc32(concat(claim_public_id, '|chanj')), 16) / 100.0)"""))
    .withColumn("_chan_damp", F.expr("""
        CASE _severity WHEN 'large_loss' THEN 0.25 WHEN 'high' THEN 0.55 ELSE 1.0 END"""))
    .withColumn("_settle_days",
                F.expr("greatest(CAST(round(_base_days * (1 + (_chan_mult - 1) * _chan_damp)) AS INT), 3)"))
    .withColumn("_modeled_settle_date", F.expr("date_add(report_date, _settle_days)"))
    # Status is AGE-DRIVEN (a real book settles claims as their modeled settle date
    # passes), NOT frozen from the source feed — otherwise old claims stay open forever
    # and the open inventory becomes implausibly large/old. A claim has settled once its
    # modeled settlement date has arrived.
    .withColumn("_settle_passed", F.expr("_modeled_settle_date <= current_date()"))
    # A small long-tail of complex / large losses stays open past its modeled date
    # (litigation / dispute) — the genuine backlog a head of claims worries about.
    .withColumn("_litigating", F.expr(
        "_settle_passed AND _severity IN ('large_loss','high') "
        "AND pmod(crc32(concat(claim_public_id, '|lit')), 100) < (CASE _severity WHEN 'large_loss' THEN 18 ELSE 6 END)"))
    .withColumn("_settled_now", F.expr("_settle_passed AND NOT _litigating"))
)

# --- claim lifecycle status ---
# Open inventory = claims whose modeled settle date hasn't arrived yet (recent) plus
# the small litigation tail (old, complex, genuinely outstanding). Everything older
# has settled/declined/withdrawn. This makes open inventory realistic and recent.
df = df.withColumn("claim_status", F.expr("""
    CASE
        WHEN _settled_now AND is_potential_fraud
             AND pmod(crc32(concat(claim_public_id, '|st')), 100) < 40 THEN 'declined'
        WHEN _settled_now AND pmod(crc32(concat(claim_public_id, '|st')), 100) < 5 THEN 'withdrawn'
        WHEN _settled_now                          THEN 'settled'
        WHEN _litigating                           THEN 'under_investigation'
        WHEN is_potential_fraud                    THEN 'under_investigation'
        ELSE 'open'
    END
"""))
df = (
    df
    .withColumn("settlement_date", F.expr(
        "CASE WHEN claim_status IN ('settled','declined','withdrawn') THEN _modeled_settle_date ELSE NULL END"))
    # days_to_settle = the modeled settle duration (= datediff(settlement_date, report_date)),
    # so the channel signal is clean for the settled population.
    .withColumn("days_to_settle", F.expr(
        "CASE WHEN claim_status IN ('settled','declined','withdrawn') THEN _settle_days ELSE NULL END"))
)

# --- leakage_flag: severity-driven base rate, report_channel as a MULTIPLIER ---
# Claims-leakage indicator. Claim size (severity) sets the base propensity; the
# channel scales it (digital STP cleaner ~0.7x, phone leakier ~1.45x). Large
# losses leak more through ANY channel. Modeled deterministically (crc32 uniform
# vs probability) so resets reproduce — decoupled from the opening-reserve recon.
df = (
    df
    .withColumn("_leak_base", F.expr("""
        CASE _severity WHEN 'low' THEN 0.055 WHEN 'medium' THEN 0.075
                       WHEN 'high' THEN 0.13 ELSE 0.20 END"""))
    .withColumn("_leak_chan_mult", F.expr("""
        CASE report_channel WHEN 'digital' THEN 0.70 WHEN 'broker_email' THEN 1.0
                            WHEN 'phone' THEN 1.45 ELSE 1.0 END"""))
    .withColumn("_leak_prob", F.expr("least(_leak_base * _leak_chan_mult, 0.9)"))
    .withColumn("leakage_flag",
                F.expr("(pmod(crc32(concat(claim_public_id, '|leak')), 100000) / 100000.0) < _leak_prob"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Handler assignment (deterministic, routed by peril / severity)

# COMMAND ----------

# Route to a team, then pick a handler within that team deterministically by
# crc32(claim_public_id). Complex/large/SIU → senior/specialist teams.
df = df.withColumn(
    "_routed_team",
    F.expr("""
        CASE
            WHEN is_potential_fraud THEN 'siu'
            WHEN peril_type = 'motor_tp' AND (is_high_value OR ultimate_reserve > 10000) THEN 'motor_complex'
            WHEN peril_type = 'motor_tp' THEN 'motor_fast_track'
            ELSE 'home_property'
        END
    """),
)

handlers = spark.table(tbl("ref_handlers")).select("handler_id", "grade", "team")
_w = Window.partitionBy("team").orderBy("handler_id")
handlers_idx = handlers.withColumn("_h_idx", F.row_number().over(_w) - 1)
team_size = handlers.groupBy("team").agg(F.count("*").alias("_t_size"))

df = (
    df
    .join(team_size, df["_routed_team"] == team_size["team"], "left")
    .drop(team_size["team"])
    .withColumn("_h_pick", F.expr("pmod(crc32(claim_public_id), _t_size)"))
)

df = (
    df.join(
        handlers_idx.select(
            F.col("team").alias("_h_team"),
            F.col("_h_idx"),
            F.col("handler_id"),
            F.col("grade").alias("handler_grade"),
        ),
        (df["_routed_team"] == F.col("_h_team")) & (df["_h_pick"] == F.col("_h_idx")),
        "left",
    )
    .drop("_h_team", "_h_idx")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · ML training labels (triage_decision with ~10% noise, reserve_bracket)

# COMMAND ----------

# Base triage rule. Missing fraud (quarantined in Phase 1) -> falls through to escalate.
df = df.withColumn(
    "_triage_base",
    F.expr("""
        CASE
            WHEN coalesce(fraud_score, 999) < 40 AND total_incurred < 3000
                 AND coalesce(prior_claims_12m, 999) = 0 THEN 'pay_direct'
            WHEN coalesce(fraud_score, -1) > 70
                 OR (coalesce(prior_claims_12m, -1) > 2 AND reporting_lag_days > 14) THEN 'refer_siu'
            ELSE 'escalate'
        END
    """),
)

# ~10% deterministic label noise, rotating to a *different* label. The vivid
# claim cc:900001 is exempt so its label stays reproducible.
df = df.withColumn(
    "triage_decision",
    F.expr("""
        CASE
            WHEN claim_public_id <> 'cc:900001'
                 AND pmod(crc32(concat(claim_public_id, '|lblnoise')), 100) < 10
            THEN element_at(
                   array('pay_direct','refer_siu','escalate'),
                   CAST(1 + pmod(
                         (CASE _triage_base WHEN 'pay_direct' THEN 0 WHEN 'refer_siu' THEN 1 ELSE 2 END)
                         + 1 + pmod(crc32(concat(claim_public_id, '|shift')), 2),
                       3) AS INT)
                 )
            ELSE _triage_base
        END
    """),
)

df = df.withColumn(
    "reserve_bracket",
    F.expr("""
        CASE
            WHEN ultimate_reserve < 2000  THEN 'low'
            WHEN ultimate_reserve < 10000 THEN 'medium'
            WHEN ultimate_reserve < 50000 THEN 'high'
            ELSE 'large_loss'
        END
    """),
).withColumn("triage_source", F.lit("historical"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Final projection + write managed Delta table

# COMMAND ----------

OUTPUT_COLS = [
    "claim_public_id", "claim_number", "policy_number",
    "loss_date", "report_date", "reporting_lag_days", "report_channel",
    "peril_type", "loss_cause", "incident_type", "description_text",
    "product", "sum_insured", "annual_premium", "effective_date", "expiry_date",
    "policy_tenure_years", "sum_insured_to_reported_ratio",
    "postcode_district", "third_party_involved",
    "flood_risk_score", "wind_risk_score", "freeze_risk_score", "weather_risk_composite",
    "total_incurred", "paid_amount", "case_reserve", "initial_reserve", "ultimate_reserve",
    "fraud_score", "fraud_flag", "prior_claims_12m", "days_since_incident",
    "is_high_value", "is_potential_fraud", "at_fault",
    "third_party_at_fault", "recovery_flag", "recoverable_amount",
    "claim_status", "settlement_date", "days_to_settle", "leakage_flag",
    "handler_id", "handler_grade",
    "triage_decision", "reserve_bracket", "triage_source",
]

silver = df.select(*OUTPUT_COLS).withColumn("_silver_built_at", F.current_timestamp())

fqn = tbl("silver_claims_enriched")
(silver.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(fqn))

# Tag layer=silver. TBLPROPERTIES carry the metadata even where governed tag
# policies block the project/owner UC tags (this workspace blocks both). NB:
# 'owner' is a reserved table property (the table owner principal), so the
# intended owner is recorded under the non-reserved key 'wb_owner'.
spark.sql(f"""
    ALTER TABLE {fqn} SET TBLPROPERTIES (
        'project' = 'claims_workbench', 'layer' = 'silver', 'wb_owner' = 'wryszka'
    )
""")
applied, skipped = cdg.set_tags_safe(spark, f"TABLE {fqn}", {
    "project": "claims_workbench", "layer": "silver", "owner": "wryszka",
})
print(f"silver_claims_enriched written. UC tags applied={applied} skipped={skipped}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Targeted check

# COMMAND ----------

import json

n_rows = spark.table(fqn).count()
n_distinct = spark.table(fqn).select("claim_public_id").distinct().count()
n_bronze_claim = spark.table(tbl("bronze_gw_cc_claim")).count()
print(f"silver rows: {n_rows:,} | distinct claim_public_id: {n_distinct:,} | bronze claims: {n_bronze_claim:,}")
assert n_rows == n_distinct, f"duplicate claim_public_id rows! {n_rows} != {n_distinct}"
assert n_rows == n_bronze_claim, f"row count {n_rows} != bronze claim count {n_bronze_claim}"
assert 118_000 <= n_rows <= 119_500, f"silver row count off: {n_rows:,}"
print("[OK] one row per claim, count matches bronze claim count.")

# COMMAND ----------

# --- Vivid claim cc:900001 full enriched row ---
vivid = spark.table(fqn).where("claim_public_id = 'cc:900001'").collect()
assert len(vivid) == 1, f"expected 1 vivid row, found {len(vivid)}"
vd = vivid[0].asDict()
print("Vivid claim cc:900001 enriched row:")
print(json.dumps(vd, indent=2, default=str))

assert vd["peril_type"] == "motor_tp", vd["peril_type"]
assert vd["fraud_score"] == 74, vd["fraud_score"]
assert vd["prior_claims_12m"] == 2, vd["prior_claims_12m"]
assert vd["reporting_lag_days"] == 18, vd["reporting_lag_days"]
assert vd["is_potential_fraud"] is True, vd["is_potential_fraud"]
assert vd["triage_decision"] == "refer_siu", vd["triage_decision"]
# reserve_bracket consistent with ultimate_reserve
ur = float(vd["ultimate_reserve"])
expected_bracket = ("low" if ur < 2000 else "medium" if ur < 10000 else "high" if ur < 50000 else "large_loss")
assert vd["reserve_bracket"] == expected_bracket, (vd["reserve_bracket"], ur)
print(f"[OK] vivid claim checks pass (ultimate_reserve={ur:,.2f} -> {expected_bracket}).")

# COMMAND ----------

# --- Null-rate summary on key derived columns (should be ~0) + expected-null cols ---
KEY_DERIVED = [
    "peril_type", "reporting_lag_days", "weather_risk_composite", "is_high_value",
    "is_potential_fraud", "claim_status", "leakage_flag", "ultimate_reserve",
    "initial_reserve", "triage_decision", "reserve_bracket", "handler_id", "handler_grade",
]
EXPECTED_NULL = [
    "fraud_score",            # ~1% quarantined in Phase 1
    "sum_insured",            # ~1% bad-policy FK miss (by design)
    "policy_tenure_years",    # ~1% bad-policy
    "at_fault",               # null for all home claims (by design)
    "settlement_date",        # null for open / under_investigation (by design)
]
exprs = [F.round(F.avg(F.col(c).isNull().cast("double")), 5).alias(c) for c in KEY_DERIVED + EXPECTED_NULL]
rates = spark.table(fqn).select(*exprs).collect()[0].asDict()

print("Null rates — KEY DERIVED (expect ~0):")
for c in KEY_DERIVED:
    print(f"  {c:<28} {rates[c]}")
print("Null rates — EXPECTED NULLS (by design / Phase 1 quarantine):")
for c in EXPECTED_NULL:
    print(f"  {c:<28} {rates[c]}")

for c in KEY_DERIVED:
    assert rates[c] == 0.0, f"unexpected nulls in key derived column {c}: {rates[c]}"
print("[OK] no nulls in key derived columns.")
