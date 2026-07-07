# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 03 · Gold Analytics
# MAGIC
# MAGIC **Bricksurance SE** — Phase 3. Turns `silver_claims_enriched` into
# MAGIC business-ready aggregates a head of claims / CDO reads directly, plus an
# MAGIC empty HITL audit shell the app will append to in Phase 8.
# MAGIC
# MAGIC > **About this demo.** Synthetic data only — fictional company, policies and
# MAGIC > figures. No real Guidewire integration, no real customer data.
# MAGIC
# MAGIC **Pattern:** `silver_claims_enriched` → **`gold_*` (business aggregates)**.
# MAGIC Every table answers a question a claims leader actually asks.
# MAGIC
# MAGIC | Table | Question |
# MAGIC |-------|----------|
# MAGIC | `gold_reserve_development` | Are we reserving accurately? |
# MAGIC | `gold_settlement_performance` | How fast and how clean do we settle? |
# MAGIC | `gold_geo_clustering` | Where is risk concentrated? |
# MAGIC | `gold_handler_scorecard` | How is my team performing? |
# MAGIC | `gold_handler_decisions` | HITL audit shell (empty; app writes it in Phase 8) |

# COMMAND ----------

import os
import sys
from pyspark.sql import functions as F
from pyspark.sql import types as T

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema (fixed)")

catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
print(f"[target] {catalog}.{schema}")


def tbl(name):
    return f"`{catalog}`.`{schema}`.{name}"


_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_helper_dir = "/Workspace" + os.path.dirname(_ctx.notebookPath().get())
if _helper_dir not in sys.path:
    sys.path.insert(0, _helper_dir)
import claims_data_gen as cdg


def write_gold(df, table, layer="gold"):
    """Overwrite a managed Delta table, record metadata via TBLPROPERTIES
    (always works) and attempt UC tags (governed keys skipped gracefully)."""
    fqn = tbl(table)
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(fqn))
    # 'owner' is a reserved table property -> recorded as 'wb_owner'.
    spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES "
              f"('project'='claims_workbench','layer'='{layer}','wb_owner'='wryszka')")
    cdg.set_tags_safe(spark, f"TABLE {fqn}",
                      {"project": "claims_workbench", "layer": layer, "owner": "wryszka"})
    return spark.table(fqn).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Base = silver_claims_enriched
# MAGIC `report_channel` is carried in silver (Phase 2), so gold reads everything
# MAGIC from the silver consumption table directly.

# COMMAND ----------

base = (
    spark.table(tbl("silver_claims_enriched"))
    .withColumn("accident_qtr", F.expr("concat(year(loss_date), '-Q', quarter(loss_date))"))
    .withColumn("is_closed", F.expr("claim_status IN ('settled','declined','withdrawn')"))
    .withColumn("is_open", F.expr("claim_status IN ('open','under_investigation')"))
    # Modeled proxy: touchpoints scale with claim duration. Real interaction
    # counts arrive once the Phase 8 app logs handler/customer contacts.
    .withColumn("touchpoints",
                F.expr("2 + floor(coalesce(days_to_settle, reporting_lag_days, 0) / 20)"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · gold_reserve_development — "Are we reserving accurately?"
# MAGIC Grain: accident_qtr × peril_type × reserve_bracket.
# MAGIC avg_development_ratio = sum(ultimate) / sum(initial) (aggregate development ratio).

# COMMAND ----------

reserve_dev = (
    base.groupBy("accident_qtr", "peril_type", "reserve_bracket")
    .agg(
        F.count("*").alias("count_claims"),
        F.sum("initial_reserve").cast("decimal(18,2)").alias("sum_initial_reserve"),
        F.sum("ultimate_reserve").cast("decimal(18,2)").alias("sum_ultimate_reserve"),
        F.sum("paid_amount").cast("decimal(18,2)").alias("sum_paid"),
    )
    .withColumn("avg_development_ratio",
                F.expr("round(sum_ultimate_reserve / nullif(sum_initial_reserve, 0), 3)"))
    # IBNR indicator: reserves developing adversely (>15% upward) -> provision indicated.
    .withColumn("ibnr_indicator", F.expr("avg_development_ratio > 1.15"))
)
n_reserve_dev = write_gold(reserve_dev, "gold_reserve_development")
print(f"gold_reserve_development rows: {n_reserve_dev:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · gold_settlement_performance — "How fast and how clean do we settle?"
# MAGIC Grain: handler_grade × peril_type × report_channel.

# COMMAND ----------

settlement = (
    base.groupBy("handler_grade", "peril_type", "report_channel")
    .agg(
        F.round(F.avg("days_to_settle"), 1).alias("avg_days_to_settle"),
        F.round(F.expr("percentile_approx(days_to_settle, 0.9)"), 1).alias("p90_days_to_settle"),
        F.round(100 * F.avg(F.expr("CASE WHEN leakage_flag THEN 1 ELSE 0 END")), 2).alias("leakage_rate_pct"),
        F.round(F.avg("paid_amount"), 2).alias("avg_paid_amount"),
        F.round(100 * F.avg(F.expr("CASE WHEN is_closed THEN 1 ELSE 0 END")), 2).alias("closure_rate_pct"),
    )
)
n_settlement = write_gold(settlement, "gold_settlement_performance")
print(f"gold_settlement_performance rows: {n_settlement:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · gold_geo_clustering — "Where is risk concentrated?"
# MAGIC Grain: postcode_district × peril_type.

# COMMAND ----------

# Denominator: distinct policies seen in each district (policies are not
# geocoded in the CDA feed, so we proxy with claiming policies per district).
pol_per_district = (
    base.groupBy("postcode_district")
    .agg(F.countDistinct("policy_number").alias("_district_policies"))
)

geo = (
    base.groupBy("postcode_district", "peril_type")
    .agg(
        F.count("*").alias("claim_count"),
        F.round(F.avg("paid_amount"), 2).alias("avg_paid_amount"),
        F.round(100 * F.avg(F.expr("CASE WHEN fraud_flag THEN 1 ELSE 0 END")), 2).alias("fraud_flag_rate"),
        F.round(F.avg("weather_risk_composite"), 2).alias("weather_risk_composite"),
    )
    .join(pol_per_district, "postcode_district", "left")
    .withColumn("claims_per_1000_policies",
                F.expr("round(claim_count * 1000.0 / nullif(_district_policies, 0), 1)"))
    .drop("_district_policies")
)
# PRICING HOOK: geo risk concentration (district × peril frequency + weather
# composite) is the natural feed into the Bricksurance pricing workbench —
# rating-area loadings would consume this table. No integration here, just the marker.
n_geo = write_gold(geo, "gold_geo_clustering")
print(f"gold_geo_clustering rows: {n_geo:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4b · ref_postcode_centroid + gold_geo_map — map-ready geography
# MAGIC Approx lat/lon for each postcode district so the AI/BI dashboard can render a
# MAGIC concentration map (bubble size = claim volume, colour = fraud / weather risk).
# MAGIC Static reference data; the view re-aggregates `gold_geo_clustering` to one point
# MAGIC per district, so it self-heals on every reset.

# COMMAND ----------

_CENTROIDS = [
    ("B1", 52.479, -1.908), ("B15", 52.464, -1.928), ("BL1", 53.585, -2.435), ("BL3", 53.567, -2.443),
    ("BS1", 51.453, -2.597), ("CB1", 52.198, 0.137), ("CF10", 51.479, -3.176), ("CO1", 51.889, 0.903),
    ("CV1", 52.408, -1.510), ("E1", 51.516, -0.060), ("EC1", 51.524, -0.099), ("GL1", 51.864, -2.238),
    ("LE1", 52.635, -1.132), ("LS1", 53.797, -1.546), ("LS6", 53.819, -1.575), ("M1", 53.479, -2.236),
    ("M14", 53.443, -2.222), ("M20", 53.413, -2.230), ("N1", 51.538, -0.103), ("NE1", 54.972, -1.613),
    ("NG1", 52.954, -1.149), ("OL1", 53.546, -2.116), ("OL9", 53.537, -2.139), ("OX1", 51.750, -1.260),
    ("RG1", 51.456, -0.969), ("S1", 53.380, -1.466), ("SE1", 51.501, -0.091), ("SW1", 51.497, -0.137),
    ("WN3", 53.535, -2.640), ("WN5", 53.530, -2.668),
]
centroids = spark.createDataFrame(_CENTROIDS, "postcode_district string, lat double, lon double")
write_gold(centroids, "ref_postcode_centroid", layer="reference")
spark.sql(f"""CREATE OR REPLACE VIEW {tbl('gold_geo_map')} AS
 SELECT g.postcode_district, c.lat, c.lon,
        sum(g.claim_count) AS claims,
        round(avg(g.fraud_flag_rate), 2) AS fraud_rate,
        round(avg(g.weather_risk_composite), 2) AS weather_risk,
        round(sum(g.claim_count * g.avg_paid_amount) / nullif(sum(g.claim_count), 0), 0) AS avg_paid
 FROM {tbl('gold_geo_clustering')} g
 JOIN {tbl('ref_postcode_centroid')} c USING (postcode_district)
 GROUP BY g.postcode_district, c.lat, c.lon""")
print(f"ref_postcode_centroid rows: {centroids.count()} · gold_geo_map view created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4c · ref_broker + gold_broker_claims — the Broker Portal book
# MAGIC Brokers clog the claims helpline asking "any update on my client's claim?". The
# MAGIC Broker Portal answers that self-service — each broker sees **only their own book**.
# MAGIC
# MAGIC The synthetic data has no producer dimension, so brokers are assigned
# MAGIC **deterministically** from `hash(policy_number)` (40/30/20% across three brokers,
# MAGIC 10% direct) — stable across resets. `gold_broker_claims` exposes **broker-safe
# MAGIC columns only** (status, stage, paid, outstanding — never fraud indicators or
# MAGIC handler fields: that's the column-security half of the story), and the three
# MAGIC `v_broker_*_claims` views are the **mock row filter** — in production this is one
# MAGIC view + a Unity Catalog ROW FILTER function keyed on the broker's identity
# MAGIC (`is_account_group_member`), not three views.

# COMMAND ----------

_BROKERS = [
    ("BRK-001", "Aldgate Risk Partners", "ARP-114", "Commercial motor & fleet",
     "Priya Nair", "claims@aldgaterisk.example"),
    ("BRK-002", "Caldwell & Vane", "CDV-227", "Household & high-net-worth property",
     "James Caldwell", "claims@caldwellvane.example"),
    ("BRK-003", "Northgate Insurance Brokers", "NGB-305", "SME & regional retail",
     "Ellen Okafor", "claims@northgatebrokers.example"),
]
brokers_ref = spark.createDataFrame(
    _BROKERS, "broker_id string, broker_name string, producer_code string, "
              "segment string, contact_name string, contact_email string")
write_gold(brokers_ref, "ref_broker", layer="reference")

# Broker-safe book view. Client names are deterministic synthetics (salted hashes of
# the policy number) so brokers see a human book, not bare policy numbers.
_FIRST = "'Amelia','Oliver','Sophia','Harry','Isla','George','Ava','Noah','Emily','Jack','Grace','Leo','Freya','Oscar','Poppy','Arthur'"
_LAST = "'Hughes','Patel','Walsh','Thompson','Okafor','Bennett','Kaur','Murray','Ellis','Nowak','Doyle','Ferguson','Ademola','Price','Whitfield','Sharma'"
spark.sql(f"""CREATE OR REPLACE VIEW {tbl('gold_broker_claims')}
 COMMENT 'Broker Portal book: broker-safe columns only (no fraud/handler fields). Broker assigned deterministically from hash(policy_number).' AS
 WITH assigned AS (
   SELECT s.*, CASE WHEN pmod(abs(hash(s.policy_number)), 10) <= 3 THEN 'BRK-001'
                    WHEN pmod(abs(hash(s.policy_number)), 10) <= 6 THEN 'BRK-002'
                    WHEN pmod(abs(hash(s.policy_number)), 10) <= 8 THEN 'BRK-003'
                    ELSE 'DIRECT' END AS broker_id
   FROM {tbl('silver_claims_enriched')} s)
 SELECT broker_id, claim_public_id, claim_number, policy_number,
   concat(element_at(array({_FIRST}), pmod(abs(hash(policy_number, 'f')), 16) + 1), ' ',
          element_at(array({_LAST}),  pmod(abs(hash(policy_number, 's')), 16) + 1)) AS client_name,
   product, peril_type, loss_cause, postcode_district,
   loss_date, report_date, claim_status,
   CASE WHEN claim_status = 'settled' THEN 'Settled & closed'
        WHEN claim_status = 'declined' THEN 'Declined'
        WHEN claim_status = 'withdrawn' THEN 'Withdrawn'
        WHEN claim_status = 'under_investigation' THEN 'Under review — additional checks'
        WHEN paid_amount > 0 THEN 'Payment in progress'
        WHEN days_since_incident <= 7 THEN 'New — being assessed'
        ELSE 'In handling' END AS stage,
   CASE WHEN claim_status IN ('settled','declined','withdrawn') THEN 'None — file closed'
        WHEN claim_status = 'under_investigation' THEN 'We may contact your client for more information'
        WHEN paid_amount > 0 THEN 'Payment on its way to your client'
        WHEN days_since_incident <= 7 THEN 'Assessment in progress — no action needed'
        ELSE 'With the handling team' END AS next_step,
   paid_amount,
   greatest(total_incurred - paid_amount, 0) AS outstanding_estimate,
   CASE WHEN settlement_date IS NOT NULL THEN settlement_date
        ELSE greatest(report_date, date_sub(current_date(), pmod(abs(hash(claim_public_id)), 21))) END AS last_update,
   datediff(coalesce(settlement_date, current_date()), report_date) AS days_open
 FROM assigned""")

# Mock row filter: one pre-filtered view per broker (what the portal session reads).
_BROKER_VIEWS = {"BRK-001": "v_broker_aldgate_claims",
                 "BRK-002": "v_broker_caldwell_claims",
                 "BRK-003": "v_broker_northgate_claims"}
for bid, vname in _BROKER_VIEWS.items():
    bname = next(b[1] for b in _BROKERS if b[0] == bid)
    spark.sql(f"""CREATE OR REPLACE VIEW {tbl(vname)}
      COMMENT 'Mock row filter: {bname} book only. Production: single view + UC ROW FILTER on is_account_group_member().' AS
      SELECT * FROM {tbl('gold_broker_claims')} WHERE broker_id = '{bid}'""")
mix = spark.sql(f"SELECT broker_id, count(*) n FROM {tbl('gold_broker_claims')} GROUP BY broker_id ORDER BY broker_id").collect()
print("gold_broker_claims by broker: " + ", ".join(f"{r['broker_id']}={r['n']:,}" for r in mix))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4d · ref_vulnerability_standards + gold_vulnerability_flags — Consumer Duty
# MAGIC The centralised **vulnerability standard**: category definitions, the claim-level
# MAGIC indicators used to flag, the handling protocol for humans, and the guidance AI
# MAGIC agents must follow. Flags are **deterministic** over silver signals (distress
# MAGIC perils, repeat claimants, channel/tenure proxy) plus a **synthetic
# MAGIC customer-declared cohort** (salted hash, ~5%) — labelled synthetic; in production
# MAGIC the declared flag comes from FNOL/CRM declared data. Both objects are governed UC
# MAGIC assets the app links straight back to.

# COMMAND ----------

_VUL_STANDARDS = [
    ("VUL-01", "Life event — severe loss",
     "The insured event itself has upended the customer's life: home uninhabitable, "
     "serious disruption, acute distress.",
     "peril = home fire; escape of water with incurred > £25k",
     "Priority handling by a single named handler. Proactive contact at least twice a week. "
     "Offer alternative accommodation and emergency payment early. No automated repudiation — "
     "any adverse decision is made and delivered by a human.",
     "Never auto-close. Use plain, empathetic language. Surface urgency to the handler and "
     "flag accommodation/emergency-payment entitlements proactively."),
    ("VUL-02", "Resilience — financial strain",
     "Customers with low financial resilience for whom delay or excess collection causes real hardship.",
     "3+ claims in the last 12 months (repeat claimant)",
     "Check affordability before collecting excess; offer staged collection. Prioritise interim "
     "payments. Review for fair value and signpost free debt advice where appropriate.",
     "Check the file for hardship indicators and surface them. Recommend interim payment where "
     "cover is clear. Do not recommend cash-settlement pressure tactics."),
    ("VUL-03", "Capability — access & understanding",
     "Customers who struggle with digital journeys, complex documents or financial terminology.",
     "phone-only reporting from the longest-tenure customers (proxy signal)",
     "Plain-language letters; confirm understanding on calls; never force a digital-only journey; "
     "allow extra time at every stage.",
     "Simplify all generated wording to plain English. Offer the phone channel in every "
     "communication. Repeat key facts back for confirmation."),
    ("VUL-04", "Health — declared vulnerability",
     "A physical or mental health condition the customer has declared that affects how the claim "
     "must be handled. Special-category data under UK GDPR.",
     "declared at FNOL / on the CRM record (synthetic ~5% cohort in this demo)",
     "Route to a vulnerability-trained handler. Follow the declared-needs plan on file. Record "
     "the lawful basis and consent for using the declared data. Extra time; no pressure at decision points.",
     "No automated decisions of any kind — always hand to a human. Follow the declared-needs "
     "plan verbatim. Never reference the health condition in generated customer wording."),
]
vul_std = spark.createDataFrame(
    _VUL_STANDARDS, "category_id string, category string, definition string, "
                    "indicators string, handling_protocol string, agent_guidance string")
write_gold(vul_std, "ref_vulnerability_standards", layer="reference")

spark.sql(f"""CREATE OR REPLACE VIEW {tbl('gold_vulnerability_flags')}
 COMMENT 'Claim-level vulnerability flags (Consumer Duty). Deterministic over silver signals + synthetic declared cohort (is_synthetic=true). Protocols in ref_vulnerability_standards.' AS
 WITH s AS (SELECT claim_public_id, peril_type, total_incurred, prior_claims_12m,
                   report_channel, policy_tenure_years, claim_status
            FROM {tbl('silver_claims_enriched')})
 SELECT claim_public_id, 'VUL-01' AS category_id, 'high' AS severity,
        'Severe home fire — home likely uninhabitable' AS rationale, false AS is_synthetic, claim_status
 FROM s WHERE peril_type = 'home_fire'
 UNION ALL
 SELECT claim_public_id, 'VUL-01', 'medium',
        'Major escape of water (incurred over £25k) — significant disruption to the home', false, claim_status
 FROM s WHERE peril_type = 'home_escape_water' AND total_incurred > 25000
 UNION ALL
 SELECT claim_public_id, 'VUL-02', 'medium',
        concat('Repeat claimant — ', prior_claims_12m, ' claims in 12 months; check affordability and fair value'), false, claim_status
 FROM s WHERE prior_claims_12m >= 3
 UNION ALL
 SELECT claim_public_id, 'VUL-03', 'low',
        'Long-tenure, phone-only contact — possible digital-capability barrier (proxy signal)', false, claim_status
 FROM s WHERE report_channel = 'phone' AND policy_tenure_years > 4.5
 UNION ALL
 SELECT claim_public_id, 'VUL-04', 'high',
        'Customer-declared vulnerability on record (synthetic cohort in this demo — declared FNOL/CRM data in production)', true, claim_status
 FROM s WHERE pmod(abs(hash(claim_public_id, 'v')), 100) < 5""")
vmix = spark.sql(f"SELECT category_id, count(*) n FROM {tbl('gold_vulnerability_flags')} GROUP BY category_id ORDER BY category_id").collect()
print("gold_vulnerability_flags: " + ", ".join(f"{r['category_id']}={r['n']:,}" for r in vmix))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4e · gold_sla_prediction + gold_qa_scores — predictive oversight
# MAGIC **Predictive SLA**: instead of reporting breaches after the fact, predict them —
# MAGIC each open claim's expected total days = its own elapsed time vs the **cohort
# MAGIC benchmark** (average settlement days for settled claims of the same peril ×
# MAGIC value band, straight from the book's actuals). If the prediction exceeds the
# MAGIC per-peril SLA, it's flagged BEFORE the clock runs out.
# MAGIC
# MAGIC **QA on every claim**: deterministic adherence checks codified in SQL and run
# MAGIC over **100% of the book** (vs the industry's sampled handful) — flags, never
# MAGIC overrides. Both are self-healing views; no compute, no copies.

# COMMAND ----------

_SLA_CASE = "CASE peril_type WHEN 'motor_tp' THEN 30 WHEN 'home_fire' THEN 60 ELSE 45 END"
spark.sql(f"""CREATE OR REPLACE VIEW {tbl('gold_sla_prediction')}
 COMMENT 'Predictive SLA: open claims vs cohort benchmark (avg settled days per peril x value band). sla_outlook = breached | predicted_breach | on_track.' AS
 WITH s AS (
   SELECT claim_public_id, peril_type, is_high_value, total_incurred, claim_status, report_date,
          {_SLA_CASE} AS sla_days
   FROM {tbl('silver_claims_enriched')}),
 bench AS (
   SELECT peril_type, is_high_value, round(avg(days_to_settle), 1) AS expected_days, count(*) AS cohort_n
   FROM {tbl('silver_claims_enriched')}
   WHERE claim_status = 'settled' AND days_to_settle IS NOT NULL
   GROUP BY peril_type, is_high_value)
 SELECT s.claim_public_id, s.peril_type, s.is_high_value, s.total_incurred, s.claim_status,
        s.report_date, s.sla_days, b.expected_days, b.cohort_n,
        datediff(current_date(), s.report_date) AS days_elapsed,
        greatest(datediff(current_date(), s.report_date), cast(b.expected_days AS int)) AS predicted_total_days,
        CASE WHEN datediff(current_date(), s.report_date) > s.sla_days THEN 'breached'
             WHEN greatest(datediff(current_date(), s.report_date), cast(b.expected_days AS int)) > s.sla_days THEN 'predicted_breach'
             ELSE 'on_track' END AS sla_outlook
 FROM s JOIN bench b USING (peril_type, is_high_value)
 WHERE s.claim_status IN ('open','under_investigation')""")

spark.sql(f"""CREATE OR REPLACE VIEW {tbl('gold_qa_scores')}
 COMMENT 'QA on every claim: 6 deterministic adherence checks over 100% of the book. Flags, never overrides. qa_band = clean | attention | fail.' AS
 WITH d AS (SELECT claim_public_id, max(disposition) AS disposition
            FROM {tbl('gold_claim_disposition')} GROUP BY claim_public_id),
 h AS (SELECT claim_public_id,
              max(CASE WHEN override_flag AND length(coalesce(override_reason, '')) = 0 THEN 1 ELSE 0 END) AS missing_override_reason
       FROM {tbl('gold_handler_decisions')} GROUP BY claim_public_id),
 checks AS (
   SELECT s.claim_public_id, s.peril_type, s.claim_status, s.total_incurred, s.handler_id,
     CASE WHEN s.triage_decision IS NOT NULL THEN 1 ELSE 0 END AS chk_triage_recorded,
     CASE WHEN s.initial_reserve > 0 THEN 1 ELSE 0 END AS chk_reserve_set,
     CASE WHEN coalesce(s.days_to_settle, datediff(current_date(), s.report_date)) <= {_SLA_CASE} THEN 1 ELSE 0 END AS chk_within_sla,
     CASE WHEN s.fraud_score > 70 AND s.triage_decision = 'pay_direct' THEN 0 ELSE 1 END AS chk_fraud_not_fasttracked,
     CASE WHEN coalesce(dd.disposition, '') = 'auto_closed' AND s.total_incurred > 10000 THEN 0 ELSE 1 END AS chk_autoclose_in_appetite,
     CASE WHEN coalesce(hh.missing_override_reason, 0) = 1 THEN 0 ELSE 1 END AS chk_override_reasoned
   FROM {tbl('silver_claims_enriched')} s
   LEFT JOIN d dd USING (claim_public_id) LEFT JOIN h hh USING (claim_public_id))
 SELECT *,
   round(100.0 * (chk_triage_recorded + chk_reserve_set + chk_within_sla + chk_fraud_not_fasttracked
                  + chk_autoclose_in_appetite + chk_override_reasoned) / 6, 1) AS qa_score,
   CASE WHEN (chk_triage_recorded + chk_reserve_set + chk_within_sla + chk_fraud_not_fasttracked
              + chk_autoclose_in_appetite + chk_override_reasoned) = 6 THEN 'clean'
        WHEN (chk_triage_recorded + chk_reserve_set + chk_within_sla + chk_fraud_not_fasttracked
              + chk_autoclose_in_appetite + chk_override_reasoned) >= 5 THEN 'attention'
        ELSE 'fail' END AS qa_band
 FROM checks""")
sp = spark.sql(f"SELECT sla_outlook, count(*) n FROM {tbl('gold_sla_prediction')} GROUP BY sla_outlook").collect()
qa = spark.sql(f"SELECT qa_band, count(*) n FROM {tbl('gold_qa_scores')} GROUP BY qa_band").collect()
print("gold_sla_prediction: " + ", ".join(f"{r['sla_outlook']}={r['n']:,}" for r in sp))
print("gold_qa_scores: " + ", ".join(f"{r['qa_band']}={r['n']:,}" for r in qa))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4f · gold_reserve_adequacy — the £ gap and the reason, per open claim
# MAGIC The workshop-validated shape: a reserve recommendation **with a plain-language
# MAGIC explanation and a benchmark against historicals**. The benchmark is the book's
# MAGIC own settled outcomes: how did comparable claims (same peril × value band)
# MAGIC actually develop vs their initial reserve? Open claims get a suggested reserve,
# MAGIC the £ gap, and the reason — self-healing view, no compute.

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE VIEW {tbl('gold_reserve_adequacy')}
 COMMENT 'Reserve adequacy per open claim: suggested reserve = initial x cohort development ratio (settled comparables, peril x value band), the GBP gap, and the plain-language reason.' AS
 WITH bench AS (
   SELECT peril_type, is_high_value,
          round(sum(coalesce(ultimate_reserve, paid_amount)) / nullif(sum(initial_reserve), 0), 3) AS dev_ratio,
          count(*) AS cohort_n
   FROM {tbl('silver_claims_enriched')}
   WHERE claim_status = 'settled' AND initial_reserve > 0
   GROUP BY peril_type, is_high_value)
 SELECT s.claim_public_id, s.peril_type, s.is_high_value, s.claim_status, s.total_incurred,
        s.initial_reserve, b.dev_ratio, b.cohort_n,
        round(s.initial_reserve * b.dev_ratio, 0) AS suggested_reserve,
        round(s.initial_reserve * b.dev_ratio - s.initial_reserve, 0) AS reserve_gap,
        CASE WHEN (s.initial_reserve * b.dev_ratio - s.initial_reserve) > greatest(500, 0.15 * s.initial_reserve) THEN 'under_reserved'
             WHEN (s.initial_reserve * b.dev_ratio - s.initial_reserve) < -greatest(500, 0.15 * s.initial_reserve) THEN 'over_reserved'
             ELSE 'adequate' END AS adequacy,
        concat('Comparable settled ', s.peril_type, CASE WHEN s.is_high_value THEN ' high-value' ELSE '' END,
               ' claims developed to ', b.dev_ratio, 'x their initial reserve (', b.cohort_n, ' settled claims in the cohort).') AS reason
 FROM {tbl('silver_claims_enriched')} s
 JOIN bench b USING (peril_type, is_high_value)
 WHERE s.claim_status IN ('open','under_investigation') AND s.initial_reserve > 0""")
ra = spark.sql(f"SELECT adequacy, count(*) n, round(sum(CASE WHEN adequacy='under_reserved' THEN reserve_gap ELSE 0 END)/1e6,2) gap_m FROM {tbl('gold_reserve_adequacy')} GROUP BY adequacy").collect()
print("gold_reserve_adequacy: " + ", ".join(f"{r['adequacy']}={r['n']:,}" for r in ra))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4g · ref_supplier + gold_supplier_scorecard — the supplier accountability layer
# MAGIC The workshop pain: supplier performance data is invisible to the claims team.
# MAGIC This puts **cost, cycle time and quality per repairer** in one governed view.
# MAGIC The synthetic book has no repairer dimension, so jobs are assigned
# MAGIC **deterministically** (peril + light-damage rule + postcode hash — stable across
# MAGIC resets, labelled synthetic); the METRICS are then computed from the real claim
# MAGIC rows each supplier carries, so differences are genuine (the drying specialist
# MAGIC really does carry water jobs, the mobile repairer really does carry cheap fast
# MAGIC ones). Peer comparison + steer recommendation per trade.

# COMMAND ----------

_SUPPLIERS = [
    ("SUP-101", "Crownfield Accident Repair", "motor bodyshop", "National motor bodyshop network"),
    ("SUP-102", "Reeves & Drayton Bodyworks", "motor bodyshop", "Regional bodyshop group — North"),
    ("SUP-103", "Apex Vehicle Repair Group", "motor bodyshop", "Regional bodyshop group — South"),
    ("SUP-104", "Silverline Mobile Repairs", "motor bodyshop", "Mobile SMART repairs — light damage"),
    ("SUP-201", "Restorex Property Services", "general restoration", "National property restoration"),
    ("SUP-202", "AquaDry Response", "water damage & drying", "Escape-of-water & drying specialist"),
    ("SUP-203", "Hearthstone Building Contractors", "fire & rebuild", "Fire reinstatement & rebuild"),
    ("SUP-204", "Northgate Restoration Co.", "general restoration", "Regional restoration — North"),
]
sup_ref = spark.createDataFrame(
    _SUPPLIERS, "supplier_id string, supplier_name string, trade string, segment string")
write_gold(sup_ref, "ref_supplier", layer="reference")

_H = "pmod(abs(hash(postcode_district, 'sup')), 10)"
spark.sql(f"""CREATE OR REPLACE VIEW {tbl('gold_supplier_scorecard')}
 COMMENT 'Supplier accountability: cost / cycle / quality per repairer with peer indices and a steer recommendation. Job assignment is deterministic-synthetic (peril + damage size + postcode hash); metrics are computed from the real claims each supplier carries.' AS
 WITH assigned AS (
   SELECT s.*, CASE peril_type WHEN 'motor_tp' THEN 30 WHEN 'home_fire' THEN 60 ELSE 45 END AS sla_days,
     CASE
       WHEN peril_type = 'motor_tp' AND paid_amount < 1500 AND {_H} <= 5 THEN 'SUP-104'
       WHEN peril_type = 'motor_tp' THEN CASE WHEN {_H} <= 3 THEN 'SUP-101' WHEN {_H} <= 6 THEN 'SUP-102' ELSE 'SUP-103' END
       WHEN peril_type = 'home_escape_water' THEN CASE WHEN {_H} <= 4 THEN 'SUP-202' WHEN {_H} <= 7 THEN 'SUP-201' ELSE 'SUP-204' END
       WHEN peril_type = 'home_fire' THEN CASE WHEN {_H} <= 5 THEN 'SUP-203' ELSE 'SUP-201' END
       ELSE CASE WHEN {_H} <= 4 THEN 'SUP-201' WHEN {_H} <= 7 THEN 'SUP-204' ELSE 'SUP-203' END
     END AS supplier_id
   FROM {tbl('silver_claims_enriched')} s WHERE paid_amount > 0),
 agg AS (
   SELECT supplier_id, count(*) AS jobs,
     sum(CASE WHEN claim_status IN ('open','under_investigation') THEN 1 ELSE 0 END) AS open_jobs,
     round(avg(paid_amount), 0) AS avg_paid,
     round(sum(paid_amount) / 1e6, 2) AS total_paid_m,
     round(avg(CASE WHEN claim_status = 'settled' THEN days_to_settle END), 1) AS avg_cycle_days,
     round(100 * avg(CASE WHEN leakage_flag THEN 1.0 ELSE 0.0 END), 2) AS leakage_rate_pct,
     round(100 * avg(CASE WHEN claim_status = 'settled' AND days_to_settle <= sla_days THEN 1.0
                          WHEN claim_status = 'settled' THEN 0.0 END), 1) AS sla_hit_pct
   FROM assigned GROUP BY supplier_id)
 SELECT r.supplier_id, r.supplier_name, r.trade, r.segment,
   a.jobs, a.open_jobs, a.avg_paid, a.total_paid_m, a.avg_cycle_days, a.leakage_rate_pct, a.sla_hit_pct,
   round(a.avg_paid / avg(a.avg_paid) OVER (PARTITION BY r.trade), 3) AS cost_index,
   round(a.avg_cycle_days / avg(a.avg_cycle_days) OVER (PARTITION BY r.trade), 3) AS cycle_index,
   CASE WHEN a.avg_paid > 1.08 * avg(a.avg_paid) OVER (PARTITION BY r.trade)
          OR a.leakage_rate_pct > 1.25 * avg(a.leakage_rate_pct) OVER (PARTITION BY r.trade) THEN 'review'
        WHEN a.avg_paid <= avg(a.avg_paid) OVER (PARTITION BY r.trade)
         AND coalesce(a.avg_cycle_days, 0) <= avg(a.avg_cycle_days) OVER (PARTITION BY r.trade) THEN 'preferred'
        ELSE 'watch' END AS steer
 FROM {tbl('ref_supplier')} r JOIN agg a USING (supplier_id)""")
sc = spark.sql(f"SELECT supplier_id, jobs, avg_paid, avg_cycle_days, leakage_rate_pct, steer FROM {tbl('gold_supplier_scorecard')} ORDER BY trade, supplier_id").collect()
for r in sc:
    print(f"  {r['supplier_id']}: jobs={r['jobs']:,} avg_paid=£{r['avg_paid']} cycle={r['avg_cycle_days']}d leak={r['leakage_rate_pct']}% → {r['steer']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · gold_handler_scorecard — "How is my team performing?"
# MAGIC Grain: handler_id.

# COMMAND ----------

handlers = spark.table(tbl("ref_handlers")).select("handler_id", "handler_name", "grade", "team")

handler_agg = (
    base.groupBy("handler_id")
    .agg(
        F.sum(F.expr("CASE WHEN is_open THEN 1 ELSE 0 END")).alias("caseload"),
        F.round(F.avg("days_to_settle"), 1).alias("avg_days_to_settle"),
        F.round(100 * F.avg(F.expr("CASE WHEN leakage_flag THEN 1 ELSE 0 END")), 2).alias("leakage_rate_pct"),
        F.round(F.avg("touchpoints"), 1).alias("customer_touchpoints_avg"),
    )
)

scorecard = (
    handlers.join(handler_agg, "handler_id", "left")
    .withColumn("caseload", F.expr("coalesce(caseload, 0)"))
    # override_rate_pct: placeholder until the Phase 8 app logs HITL overrides.
    .withColumn("override_rate_pct", F.lit(0.0).cast("double"))
    .select("handler_id", "handler_name", "grade", "team", "caseload",
            "avg_days_to_settle", "leakage_rate_pct", "override_rate_pct",
            "customer_touchpoints_avg")
)
n_scorecard = write_gold(scorecard, "gold_handler_scorecard")
print(f"gold_handler_scorecard rows: {n_scorecard:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · gold_handler_decisions — HITL audit shell (empty; app appends in Phase 8)

# COMMAND ----------

decisions_schema = T.StructType([
    T.StructField("decision_id", T.StringType()),
    T.StructField("claim_public_id", T.StringType()),
    T.StructField("model_recommendation", T.StringType()),
    T.StructField("model_confidence", T.DoubleType()),
    T.StructField("handler_action", T.StringType()),
    T.StructField("override_flag", T.BooleanType()),
    T.StructField("override_reason", T.StringType()),
    T.StructField("handler_id", T.StringType()),
    T.StructField("decision_ts", T.TimestampType()),
])
empty_decisions = spark.createDataFrame([], decisions_schema)
n_decisions = write_gold(empty_decisions, "gold_handler_decisions")
print(f"gold_handler_decisions rows: {n_decisions:,} (audit shell — empty by design)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6b · gold_claim_disposition — auto-close audit shell (Phase 11; 10_auto_close fills it)
# MAGIC The auto-close workflow (`10_auto_close.py`) overwrites this with one row per
# MAGIC claim carrying the disposition + full per-rule reasoning. Shell-created here so
# MAGIC `fn_decision_reasoning` resolves even before the first auto-close run.

# COMMAND ----------

disposition_schema = T.StructType([
    T.StructField("claim_public_id", T.StringType()),
    T.StructField("disposition", T.StringType()),
    T.StructField("model_decision", T.StringType()),
    T.StructField("model_confidence", T.DoubleType()),
    T.StructField("total_incurred", T.DecimalType(12, 2)),
    T.StructField("fraud_score", T.IntegerType()),
    T.StructField("data_complete", T.BooleanType()),
    T.StructField("nonfraud_rule_fired", T.BooleanType()),
    T.StructField("rules_passed", T.ArrayType(T.StringType())),
    T.StructField("rules_failed", T.ArrayType(T.StringType())),
    T.StructField("fired_rules", T.ArrayType(T.StringType())),
    T.StructField("reasoning", T.StringType()),
    T.StructField("evaluated_ts", T.TimestampType()),
])
if not spark.catalog.tableExists(tbl("gold_claim_disposition")):
    n_disp = write_gold(spark.createDataFrame([], disposition_schema), "gold_claim_disposition")
    print(f"gold_claim_disposition shell created ({n_disp} rows).")
else:
    print("gold_claim_disposition already exists — left intact for the auto-close run.")

# claim_image_severity shell (Phase 12) — 14_image_severity.py fills it via a vision FM;
# shell-created here so fn_image_severity resolves even before the first seed run.
image_sev_schema = T.StructType([
    T.StructField("claim_public_id", T.StringType()),
    T.StructField("image_url", T.StringType()),
    T.StructField("image_file", T.StringType()),
    T.StructField("severity", T.StringType()),
    T.StructField("rationale", T.StringType()),
    T.StructField("assessment_ts", T.TimestampType()),
])
if not spark.catalog.tableExists(tbl("claim_image_severity")):
    n_img = write_gold(spark.createDataFrame([], image_sev_schema), "claim_image_severity")
    print(f"claim_image_severity shell created ({n_img} rows).")
else:
    print("claim_image_severity already exists — left intact for the image-severity run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Targeted check + headline numbers

# COMMAND ----------

# --- existence + row counts ---
for t in ["gold_reserve_development", "gold_settlement_performance", "gold_geo_clustering",
          "gold_handler_scorecard", "gold_handler_decisions"]:
    print(f"  {t:<30} {spark.table(tbl(t)).count():>8,} rows")
assert spark.table(tbl("gold_handler_decisions")).count() == 0, "audit shell should be empty"

# COMMAND ----------

# --- HEADLINE 1: reserve development ratio by peril (the under-reserving story) ---
print("Reserve development ratio (sum_ultimate / sum_initial) by peril:")
dev_by_peril = (
    spark.table(tbl("gold_reserve_development"))
    .groupBy("peril_type")
    .agg(F.sum("sum_initial_reserve").alias("si"), F.sum("sum_ultimate_reserve").alias("su"))
    .withColumn("development_ratio", F.expr("round(su / si, 3)"))
    .orderBy(F.desc("development_ratio"))
)
dev_by_peril.show(truncate=False)
eow_ratio = (
    dev_by_peril.where("peril_type = 'home_escape_water'").collect()[0]["development_ratio"]
)
print(f">>> HEADLINE: home_escape_water development ratio = {eow_ratio} "
      f"(seeded ~28% under-reserving target ~1.28 — reporting the REAL number).")

# COMMAND ----------

# --- HEADLINE 2: NW escape-of-water clustering ---
print("Top districts by escape-of-water claims_per_1000_policies:")
eow_geo = (
    spark.table(tbl("gold_geo_clustering"))
    .where("peril_type = 'home_escape_water'")
    .withColumn("is_nw", F.expr("postcode_district rlike '^(M|BL|OL|WN)[0-9]'"))
    .orderBy(F.desc("claims_per_1000_policies"))
)
eow_geo.select("postcode_district", "is_nw", "claim_count",
               "claims_per_1000_policies", "weather_risk_composite").show(12, truncate=False)
nw_cmp = (
    eow_geo.groupBy("is_nw")
    .agg(F.round(F.avg("claims_per_1000_policies"), 1).alias("avg_eow_per_1000"),
         F.sum("claim_count").alias("eow_claims"))
)
nw_cmp.show(truncate=False)

# --- HEADLINE 3: digital vs phone settlement days (per-claim, accurate) ---
chan = (
    base.groupBy("report_channel")
    .agg(
        F.round(F.avg("days_to_settle"), 1).alias("avg_days_to_settle"),
        F.round(100 * F.avg(F.expr("CASE WHEN leakage_flag THEN 1 ELSE 0 END")), 2).alias("leakage_rate_pct"),
        F.count("*").alias("claims"),
    )
    .orderBy("report_channel")
)
print("Settlement speed & leakage by channel (per-claim):")
chan.show(truncate=False)
rows = {r["report_channel"]: r for r in chan.collect()}
if "digital" in rows and "phone" in rows:
    print(f">>> HEADLINE: digital settles in {rows['digital']['avg_days_to_settle']}d "
          f"(leakage {rows['digital']['leakage_rate_pct']}%) vs phone "
          f"{rows['phone']['avg_days_to_settle']}d (leakage {rows['phone']['leakage_rate_pct']}%) "
          f"— STP business case (reporting REAL numbers).")
