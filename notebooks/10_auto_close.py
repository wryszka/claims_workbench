# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 10 · Auto-close (straight-through) workflow
# MAGIC
# MAGIC **Phase 11, Stage A.** Evaluates every triaged claim against the **risk-appetite
# MAGIC band** and decides a disposition. A claim **auto-closes & pays (simulated)** when:
# MAGIC
# MAGIC > triage = `pay_direct` **AND** model confidence ≥ `conf_threshold`
# MAGIC > **AND** total_incurred ≤ `amount_cap` **AND** fraud_score ≤ `fraud_floor`
# MAGIC > **AND** FNOL data complete.
# MAGIC
# MAGIC Otherwise → **escalated** to a handler. **The workflow/model decides — no agent
# MAGIC has pay authority.** Thresholds live in `auto_close_config` (driven by the app
# MAGIC slider). The output `gold_claim_disposition` stores the raw decision inputs per
# MAGIC claim *and* the full per-rule reasoning, so the app can **re-segment live** in
# MAGIC pure SQL when the slider moves (no model re-score needed).

# COMMAND ----------

# MAGIC %pip install lightgbm scikit-learn mlflow pandas --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import mlflow
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType

mlflow.set_registry_uri("databricks-uc")

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema (fixed)")
dbutils.widgets.text("conf_threshold", "", "Min pay_direct confidence % (blank = keep config)")
dbutils.widgets.text("amount_cap", "", "Max total_incurred GBP (blank = keep config)")
dbutils.widgets.text("fraud_floor", "", "Max fraud_score (blank = keep config)")

catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"


def tbl(t):
    return f"`{catalog}`.`{schema}`.{t}"


print(f"[target] {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Risk-appetite config (`auto_close_config`) — defaults, override via widgets

# COMMAND ----------

DEFAULTS = {"conf_threshold": 85.0, "amount_cap": 2000.0, "fraud_floor": 20.0}

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tbl('auto_close_config')} (
  config_key STRING, conf_threshold DOUBLE, amount_cap DOUBLE, fraud_floor DOUBLE, updated_ts TIMESTAMP
) USING DELTA
COMMENT 'Auto-close risk-appetite thresholds (single row, key=default). Driven by the app slider.'
""")
if spark.table(tbl("auto_close_config")).count() == 0:
    spark.sql(f"""INSERT INTO {tbl('auto_close_config')} VALUES
        ('default', {DEFAULTS['conf_threshold']}, {DEFAULTS['amount_cap']}, {DEFAULTS['fraud_floor']}, current_timestamp())""")

# Widget overrides update the persisted config (so the slider's choice sticks).
ov = {k: dbutils.widgets.get(k).strip() for k in ("conf_threshold", "amount_cap", "fraud_floor")}
if any(ov.values()):
    cur = spark.table(tbl("auto_close_config")).where("config_key='default'").collect()[0].asDict()
    new = {k: float(ov[k]) if ov[k] else float(cur[k]) for k in DEFAULTS}
    spark.sql(f"""MERGE INTO {tbl('auto_close_config')} t USING (SELECT 'default' k) s
        ON t.config_key = s.k WHEN MATCHED THEN UPDATE SET
        conf_threshold={new['conf_threshold']}, amount_cap={new['amount_cap']},
        fraud_floor={new['fraud_floor']}, updated_ts=current_timestamp()""")

cfg = spark.table(tbl("auto_close_config")).where("config_key='default'").collect()[0].asDict()
CONF, CAP, FLOOR = float(cfg["conf_threshold"]), float(cfg["amount_cap"]), float(cfg["fraud_floor"])
print(f"thresholds: confidence >= {CONF}% | amount <= GBP {CAP:,.0f} | fraud_score <= {FLOOR:.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1b · Dynamic rule engine config (`rule_config`) — explainable business rules
# MAGIC R1 fraud, R2 reporting lag, R3 prior-claims velocity, R4 amount/sum-insured anomaly,
# MAGIC R5 severity/amount consistency. Each emits a flag + plain-English reason; any fired
# MAGIC rule blocks auto-close (model + workflow decide). Params adjustable in the app settings.
# MAGIC R6/R7 (telematics, image severity) are Phase 12 — clean hooks left below.

# COMMAND ----------

RULE_DEFAULTS = {"lag_limit": 14.0, "velocity_limit": 1.0, "ratio_ceiling": 0.9, "severity_mult": 5.0,
                 "speed_margin": 10.0, "severe_min_amount": 5000.0, "minor_max_amount": 20000.0}
RULE_COLS = list(RULE_DEFAULTS.keys())
# Recreate the config if its schema drifted (CREATE-IF-NOT-EXISTS keeps an old schema,
# so adding R6/R7 columns needs a rebuild).
try:
    have = set(spark.table(tbl("rule_config")).columns)
    if not set(RULE_COLS).issubset(have):
        spark.sql(f"DROP TABLE IF EXISTS {tbl('rule_config')}")
except Exception:
    pass
cols_ddl = ", ".join(f"{c} DOUBLE" for c in RULE_COLS)
spark.sql(f"""CREATE TABLE IF NOT EXISTS {tbl('rule_config')} (config_key STRING, {cols_ddl}, updated_ts TIMESTAMP)
USING DELTA COMMENT 'Dynamic rule-engine params: R2 lag, R3 velocity, R4 ratio, R5 severity, R6 speed_margin, R7 severe/minor amount bands. R1 fraud uses auto_close_config.fraud_floor.'""")
if spark.table(tbl("rule_config")).count() == 0:
    vals = ", ".join(str(RULE_DEFAULTS[c]) for c in RULE_COLS)
    spark.sql(f"INSERT INTO {tbl('rule_config')} VALUES ('default', {vals}, current_timestamp())")
rc = spark.table(tbl("rule_config")).where("config_key='default'").collect()[0].asDict()
LAG, VEL, RATIOC, SEVM = float(rc["lag_limit"]), float(rc["velocity_limit"]), float(rc["ratio_ceiling"]), float(rc["severity_mult"])
SPDM, SEVMIN, MINMAX = float(rc["speed_margin"]), float(rc["severe_min_amount"]), float(rc["minor_max_amount"])
print(f"rules: R2 lag>{LAG:.0f}d | R3 prior>{VEL:.0f} | R4 ratio>{RATIOC} | R5 >{SEVM:.0f}x norm | R6 speed>limit+{SPDM:.0f} | R7 severe<£{SEVMIN:.0f}/minor>£{MINMAX:.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Batch-score the champion triage model (probabilities → decision + confidence)

# COMMAND ----------

# The triage model is logged with pyfunc_predict_fn='predict_proba' (06_triage_proba_relog),
# so predict() returns the per-class probability array. classes_ are alphabetical:
# ['escalate','pay_direct','refer_siu'] → pay_direct is index 1.
CLASSES = ["escalate", "pay_direct", "refer_siu"]
model_uri = f"models:/{catalog}.{schema}.model_triage_classifier@champion"

feat = spark.table(tbl("feature_triage"))
feat_cols = [c for c in feat.columns if c != "claim_public_id"]   # exact training order
predict_proba = mlflow.pyfunc.spark_udf(spark, model_uri, result_type=ArrayType(DoubleType()),
                                        env_manager="local")
scored = (feat.withColumn("proba", predict_proba(*[F.col(c).cast("double") for c in feat_cols]))
              .select("claim_public_id",
                      F.element_at("proba", 2).alias("pay_direct_prob"),   # 1-indexed
                      F.expr("array_position(proba, array_max(proba))").alias("argmax_pos"),
                      F.col("proba")))
scored = scored.withColumn("model_decision",
                           F.element_at(F.array(*[F.lit(c) for c in CLASSES]),
                                        F.col("argmax_pos").cast("int")))
scored = scored.withColumn("model_confidence", F.round(F.expr("array_max(proba) * 100"), 1))
print(f"scored {scored.count():,} claims")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Join silver inputs + FNOL completeness, then evaluate the disposition

# COMMAND ----------

silver = spark.table(tbl("silver_claims_enriched")).select(
    "claim_public_id", "total_incurred", "fraud_score", "loss_date", "report_date",
    "peril_type", "postcode_district", "description_text",
    "reporting_lag_days", "prior_claims_12m", "sum_insured_to_reported_ratio",
    "speed_at_incident", "posted_speed_limit")

# FNOL data complete: every field a handler needs at first notification is present.
silver = silver.withColumn("data_complete", F.expr(
    "loss_date IS NOT NULL AND report_date IS NOT NULL AND peril_type IS NOT NULL "
    "AND total_incurred IS NOT NULL AND postcode_district IS NOT NULL "
    "AND description_text IS NOT NULL AND length(trim(description_text)) > 0"))

# Per-peril severity norm for R5 (mean incurred by peril) — joined back per claim.
peril_norm = (silver.groupBy("peril_type")
              .agg(F.avg("total_incurred").alias("peril_avg_incurred")))
silver = silver.join(peril_norm, "peril_type", "left")

# Image severity (Phase 12, R7) — joined per claim; most claims have no photo (null).
try:
    img = spark.table(tbl("claim_image_severity")).select("claim_public_id", "severity").dropDuplicates(["claim_public_id"])
    silver = silver.join(img, "claim_public_id", "left")
except Exception:
    silver = silver.withColumn("severity", F.lit(None).cast("string"))

df = scored.join(silver, "claim_public_id", "inner")

# The decisioning checks: the risk-appetite BAND (triage/confidence/amount/complete)
# plus the dynamic RULE ENGINE (R1-R5). Each is a pass/fail with a plain-English reason.
# auto-close requires model = pay_direct AND every check passes (no rule fired); any
# failure escalates with the failed checks as the reasons. (Smart-Claims-style rules
# layer complementing the model; R6 telematics / R7 image-severity are Phase 12 hooks.)
CHECKS = {
    # band (A1)
    "r_decision": (f"model_decision = 'pay_direct'",                              "triage = pay_direct",            "model_decision"),
    "r_conf":     (f"model_confidence >= {CONF}",                                 f"confidence >= {CONF:.0f}%",     "concat(model_confidence, '%')"),
    "r_amount":   (f"total_incurred <= {CAP}",                                    f"amount <= GBP {CAP:,.0f}",      "concat('GBP ', format_number(total_incurred, 0))"),
    "r_complete": (f"data_complete",                                              "FNOL data complete",             "CAST(data_complete AS STRING)"),
    # dynamic rule engine (A1.5)
    "R1": (f"fraud_score <= {FLOOR}",                                             f"R1 fraud-score <= {FLOOR:.0f}", "concat('fraud ', CAST(fraud_score AS INT))"),
    "R2": (f"coalesce(reporting_lag_days,0) <= {LAG}",                            f"R2 reporting-lag <= {LAG:.0f}d","concat(CAST(coalesce(reporting_lag_days,0) AS INT), 'd')"),
    "R3": (f"coalesce(prior_claims_12m,0) <= {VEL}",                              f"R3 prior-claims <= {VEL:.0f}",  "concat(CAST(coalesce(prior_claims_12m,0) AS INT), ' prior')"),
    "R4": (f"sum_insured_to_reported_ratio IS NULL OR sum_insured_to_reported_ratio <= {RATIOC}", f"R4 amount/sum-insured ok", "concat('ratio ', round(coalesce(sum_insured_to_reported_ratio,0),3))"),
    "R5": (f"peril_avg_incurred IS NULL OR total_incurred <= {SEVM} * peril_avg_incurred",        "R5 severity consistent",    "concat('GBP ', format_number(total_incurred,0), ' vs norm GBP ', format_number(coalesce(peril_avg_incurred,0),0))"),
    # R6 telematics speed-vs-limit (motor; null telematics passes). R7 image severity vs reported amount.
    "R6": (f"speed_at_incident IS NULL OR speed_at_incident <= coalesce(posted_speed_limit,999) + {SPDM}", "R6 speed vs limit", "concat(coalesce(CAST(speed_at_incident AS INT),0), ' in ', coalesce(CAST(posted_speed_limit AS INT),0), ' zone')"),
    "R7": (f"severity IS NULL OR NOT ((severity='severe' AND total_incurred < {SEVMIN}) OR (severity='minor' AND total_incurred > {MINMAX}))", "R7 image severity vs reported", "concat('photo ', coalesce(severity,'none'), ' vs GBP ', format_number(total_incurred,0))"),
}
RULE_KEYS = ["R1", "R2", "R3", "R4", "R5", "R6", "R7"]
NONFRAUD_RULES = ["R2", "R3", "R4", "R5", "R6", "R7"]   # R1 (fraud) is the slider; these gate the live re-segment

for k, (pass_sql, _l, _v) in CHECKS.items():
    df = df.withColumn(k, F.expr(pass_sql))
df = df.withColumn("auto_ok", F.expr(" AND ".join(CHECKS.keys())))
df = df.withColumn("disposition", F.expr("CASE WHEN auto_ok THEN 'auto_closed' ELSE 'escalated' END"))
df = df.withColumn("nonfraud_rule_fired", F.expr(" OR ".join(f"(NOT {k})" for k in NONFRAUD_RULES)))

passed = F.array_compact(F.array(*[
    F.expr(f"CASE WHEN {k} THEN concat('{label} [', CAST({val} AS STRING), ']') ELSE NULL END")
    for k, (_p, label, val) in CHECKS.items()]))
failed = F.array_compact(F.array(*[
    F.expr(f"CASE WHEN NOT {k} THEN concat('{label} [', CAST({val} AS STRING), ']') ELSE NULL END")
    for k, (_p, label, val) in CHECKS.items()]))
# Just the fired rule CODES (R1..R5) for a compact display.
fired_codes = F.array_compact(F.array(*[F.expr(f"CASE WHEN NOT {k} THEN '{k}' ELSE NULL END") for k in RULE_KEYS]))
df = df.withColumn("rules_passed", passed).withColumn("rules_failed", failed).withColumn("fired_rules", fired_codes)

df = df.withColumn("reasoning", F.expr("""
    concat(
      CASE WHEN disposition = 'auto_closed'
        THEN 'AUTO-CLOSED & PAID (simulated): model said pay_direct and every check passed (no rule fired). '
        ELSE concat('ESCALATED to a handler: ', CAST(size(rules_failed) AS STRING), ' check(s) failed',
                    CASE WHEN size(fired_rules) > 0 THEN concat(' [rules: ', concat_ws(',', fired_rules), ']') ELSE '' END, '. ') END,
      'Passed: ', concat_ws('; ', rules_passed),
      CASE WHEN size(rules_failed) > 0 THEN concat('. Failed: ', concat_ws('; ', rules_failed)) ELSE '' END,
      '. The model + workflow decided; no agent has pay authority.')
"""))

out = df.select(
    "claim_public_id", "disposition", "model_decision",
    F.col("model_confidence").cast("double"),
    F.col("total_incurred").cast("decimal(12,2)"),
    F.col("fraud_score").cast("int"),
    "data_complete", "nonfraud_rule_fired", "rules_passed", "rules_failed", "fired_rules", "reasoning",
    F.current_timestamp().alias("evaluated_ts"))

(out.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(tbl("gold_claim_disposition")))
spark.sql(f"ALTER TABLE {tbl('gold_claim_disposition')} SET TBLPROPERTIES "
          f"('project'='claims_workbench','layer'='gold','wb_owner'='wryszka')")
print(f"gold_claim_disposition written: {out.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · % auto-closed + targeted checks (heroes)

# COMMAND ----------

agg = spark.sql(f"""
  SELECT count(*) total,
         sum(CASE WHEN disposition='auto_closed' THEN 1 ELSE 0 END) auto_closed,
         round(100.0 * avg(CASE WHEN disposition='auto_closed' THEN 1 ELSE 0 END), 1) pct_auto_closed
  FROM {tbl('gold_claim_disposition')}""").collect()[0]
print(f">>> % AUTO-CLOSED = {agg['pct_auto_closed']}%  ({agg['auto_closed']:,} of {agg['total']:,})")

# Rule-engine summary: how often each rule fires across the book.
rule_summary = spark.sql(f"""
  SELECT r AS rule, count(*) fired FROM {tbl('gold_claim_disposition')}
  LATERAL VIEW explode(fired_rules) t AS r GROUP BY r ORDER BY r""").collect()
print("Rule firing across the book:", {x['rule']: x['fired'] for x in rule_summary})

for cid in ["cc:900002", "cc:900001"]:
    r = spark.table(tbl("gold_claim_disposition")).where(F.col("claim_public_id") == cid).collect()
    if r:
        d = r[0].asDict()
        print(f"\n{cid}: disposition={d['disposition']} | model={d['model_decision']} @ {d['model_confidence']}% "
              f"| GBP {float(d['total_incurred']):,.0f} | fraud {d['fraud_score']} | fired_rules={d['fired_rules']}")
        print(f"   reasoning: {d['reasoning']}")

hero2 = spark.table(tbl("gold_claim_disposition")).where("claim_public_id='cc:900002'").collect()
hero1 = spark.table(tbl("gold_claim_disposition")).where("claim_public_id='cc:900001'").collect()
assert hero2 and hero2[0]["disposition"] == "auto_closed", "cc:900002 should auto-close"
assert hero2 and len(hero2[0]["fired_rules"]) == 0, "cc:900002 should fire NO rules (clean)"
assert hero1 and hero1[0]["disposition"] == "escalated", "cc:900001 should escalate"
assert hero1 and len(hero1[0]["fired_rules"]) > 0, "cc:900001 should fire blocking rules"
print(f"\n[OK] cc:900002 auto-closed (no rules) & cc:900001 escalated (rules {hero1[0]['fired_rules']}).")

dbutils.notebook.exit(json.dumps({
    "pct_auto_closed": float(agg["pct_auto_closed"]), "auto_closed": int(agg["auto_closed"]),
    "total": int(agg["total"]), "thresholds": {"conf": CONF, "cap": CAP, "fraud_floor": FLOOR}}))
