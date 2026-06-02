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
    "peril_type", "postcode_district", "description_text")

# FNOL data complete: every field a handler needs at first notification is present.
silver = silver.withColumn("data_complete", F.expr(
    "loss_date IS NOT NULL AND report_date IS NOT NULL AND peril_type IS NOT NULL "
    "AND total_incurred IS NOT NULL AND postcode_district IS NOT NULL "
    "AND description_text IS NOT NULL AND length(trim(description_text)) > 0"))

df = scored.join(silver, "claim_public_id", "inner")

# Per-rule pass/fail with the contributing values (the audit reasoning).
df = (df
    .withColumn("r_decision", F.expr("model_decision = 'pay_direct'"))
    .withColumn("r_conf", F.expr(f"model_confidence >= {CONF}"))
    .withColumn("r_amount", F.expr(f"total_incurred <= {CAP}"))
    .withColumn("r_fraud", F.expr(f"fraud_score <= {FLOOR}"))
    .withColumn("r_complete", F.col("data_complete"))
    .withColumn("auto_ok", F.expr("r_decision AND r_conf AND r_amount AND r_fraud AND r_complete"))
    .withColumn("disposition", F.expr("CASE WHEN auto_ok THEN 'auto_closed' ELSE 'escalated' END")))

# rules_passed / rules_failed as readable arrays: "<rule label> [<actual value>]".
# Each `val` is a pure SQL string expression for the contributing value.
rule_exprs = {
    "r_decision": (f"triage = pay_direct",        "model_decision"),
    "r_conf":     (f"confidence >= {CONF:.0f}%",  "concat(model_confidence, '%')"),
    "r_amount":   (f"amount <= GBP {CAP:,.0f}",   "concat('GBP ', format_number(total_incurred, 0))"),
    "r_fraud":    (f"fraud_score <= {FLOOR:.0f}", "concat('fraud ', CAST(fraud_score AS INT))"),
    "r_complete": (f"FNOL data complete",         "CAST(data_complete AS STRING)"),
}
passed = F.array_compact(F.array(*[
    F.expr(f"CASE WHEN {r} THEN concat('{label} [', CAST({val} AS STRING), ']') ELSE NULL END")
    for r, (label, val) in rule_exprs.items()]))
failed = F.array_compact(F.array(*[
    F.expr(f"CASE WHEN NOT {r} THEN concat('{label} [', CAST({val} AS STRING), ']') ELSE NULL END")
    for r, (label, val) in rule_exprs.items()]))
df = df.withColumn("rules_passed", passed).withColumn("rules_failed", failed)

df = df.withColumn("reasoning", F.expr("""
    concat(
      CASE WHEN disposition = 'auto_closed'
        THEN concat('AUTO-CLOSED & PAID (simulated): all risk-appetite rules passed. ')
        ELSE concat('ESCALATED to a handler: ', CAST(size(rules_failed) AS STRING),
                    ' rule(s) failed. ') END,
      'Passed: ', concat_ws('; ', rules_passed),
      CASE WHEN size(rules_failed) > 0 THEN concat('. Failed: ', concat_ws('; ', rules_failed)) ELSE '' END,
      '. The workflow decided; no agent has pay authority.')
"""))

out = df.select(
    "claim_public_id", "disposition", "model_decision",
    F.col("model_confidence").cast("double"),
    F.col("total_incurred").cast("decimal(12,2)"),
    F.col("fraud_score").cast("int"),
    "data_complete", "rules_passed", "rules_failed", "reasoning",
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

for cid in ["cc:900002", "cc:900001"]:
    r = spark.table(tbl("gold_claim_disposition")).where(F.col("claim_public_id") == cid).collect()
    if r:
        d = r[0].asDict()
        print(f"\n{cid}: disposition={d['disposition']} | model={d['model_decision']} @ {d['model_confidence']}% "
              f"| GBP {float(d['total_incurred']):,.0f} | fraud {d['fraud_score']}")
        print(f"   reasoning: {d['reasoning']}")

hero2 = spark.table(tbl("gold_claim_disposition")).where("claim_public_id='cc:900002'").collect()
hero1 = spark.table(tbl("gold_claim_disposition")).where("claim_public_id='cc:900001'").collect()
assert hero2 and hero2[0]["disposition"] == "auto_closed", "cc:900002 should auto-close"
assert hero1 and hero1[0]["disposition"] == "escalated", "cc:900001 should escalate"
print("\n[OK] cc:900002 auto-closed & cc:900001 escalated.")

dbutils.notebook.exit(json.dumps({
    "pct_auto_closed": float(agg["pct_auto_closed"]), "auto_closed": int(agg["auto_closed"]),
    "total": int(agg["total"]), "thresholds": {"conf": CONF, "cap": CAP, "fraud_floor": FLOOR}}))
