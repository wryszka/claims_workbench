# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 04 · Feature Engineering
# MAGIC
# MAGIC **Bricksurance SE** — Phase 4. Two governed Unity Catalog **feature tables**
# MAGIC built with `FeatureEngineeringClient`, keyed by `claim_public_id`, sourced
# MAGIC from `silver_claims_enriched`.
# MAGIC
# MAGIC > **About this demo.** Synthetic data only — fictional company, policies and
# MAGIC > figures. No real Guidewire integration, no real customer data.
# MAGIC
# MAGIC - `feature_triage`  — features for the Phase 5 FNOL triage classifier
# MAGIC - `feature_reserve` — features for the Phase 5 reserve-bracket model
# MAGIC
# MAGIC **Features only — no label columns.** `triage_decision` and `reserve_bracket`
# MAGIC are targets, joined at training time from silver. (`triage_decision_encoded`
# MAGIC appears in `feature_reserve` as an *input* feature — the historical triage
# MAGIC outcome — not as that model's target.)
# MAGIC
# MAGIC **NOTE for later phases (not built here):** these tables are consumed in
# MAGIC Phase 5 via `FeatureLookup(table_name=..., lookup_key='claim_public_id')`
# MAGIC and logged with the model via `fe.log_model(...)` so model serving in the
# MAGIC Phase 8 app auto-joins the features at inference time by `claim_public_id`.
# MAGIC
# MAGIC **Encoding is persisted** (`ref_feature_encodings`) so Phase 5 training and
# MAGIC Phase 8 serving encode categoricals identically — inconsistent encoding
# MAGIC between train and serve is a silent killer.

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys
from pyspark.sql import functions as F
from databricks.feature_engineering import FeatureEngineeringClient

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema (fixed)")

catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
print(f"[target] {catalog}.{schema}")


def tbl(name):
    return f"{catalog}.{schema}.{name}"


_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_helper_dir = "/Workspace" + os.path.dirname(_ctx.notebookPath().get())
if _helper_dir not in sys.path:
    sys.path.insert(0, _helper_dir)
import claims_data_gen as cdg

fe = FeatureEngineeringClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Deterministic categorical encodings (persisted)
# MAGIC Fixed canonical mappings — independent of the data so train and serve always
# MAGIC agree, and unseen values map to -1. Persisted to `ref_feature_encodings`.

# COMMAND ----------

ENCODINGS = {
    "peril_type": {"home_escape_water": 0, "home_fire": 1, "home_storm": 2, "motor_tp": 3},
    "report_channel": {"broker_email": 0, "digital": 1, "phone": 2},
    "handler_grade": {"junior": 0, "senior": 1, "specialist": 2},
    "triage_decision": {"escalate": 0, "pay_direct": 1, "refer_siu": 2},
}


def encode(col, feature):
    """CASE expression mapping raw value -> index from ENCODINGS (unseen -> -1)."""
    whens = " ".join(f"WHEN '{k}' THEN {v}" for k, v in ENCODINGS[feature].items())
    return F.expr(f"CASE {col} {whens} ELSE -1 END")


# Persist the mapping so Phase 5 / Phase 8 encode identically.
enc_rows = [(feat, raw, idx) for feat, m in ENCODINGS.items() for raw, idx in m.items()]
enc_df = spark.createDataFrame(enc_rows, "feature string, raw_value string, encoded_index int")
enc_fqn = tbl("ref_feature_encodings")
(enc_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(enc_fqn))
spark.sql(f"ALTER TABLE {enc_fqn} SET TBLPROPERTIES "
          f"('project'='claims_workbench','layer'='feature','wb_owner'='wryszka')")
cdg.set_tags_safe(spark, f"TABLE {enc_fqn}",
                  {"project": "claims_workbench", "layer": "feature", "owner": "wryszka"})
print(f"Persisted {enc_df.count()} encoding rows to {enc_fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Build feature DataFrames from silver (features only, no labels)

# COMMAND ----------

silver = spark.table(tbl("silver_claims_enriched"))

# --- feature_triage ---
feature_triage = silver.select(
    "claim_public_id",
    encode("peril_type", "peril_type").alias("peril_type_encoded"),
    encode("report_channel", "report_channel").alias("report_channel_encoded"),
    # Cast derived numerics to double — silver carries some as DecimalType, which
    # becomes Python Decimal (object dtype) in pandas and breaks LightGBM/serving.
    F.expr("CAST(log1p(total_incurred) AS double)").alias("reported_amount_log"),
    F.expr("CAST(coalesce(sum_insured_to_reported_ratio, 0.0) AS double)").alias("sum_insured_to_reported_ratio"),
    F.expr("coalesce(fraud_score, 0)").alias("fraud_score"),
    F.expr("coalesce(prior_claims_12m, 0)").alias("prior_claims_12m"),
    F.col("reporting_lag_days"),
    F.expr("CAST(coalesce(policy_tenure_years, 0.0) AS double)").alias("policy_tenure_years"),
    F.expr("CAST(weather_risk_composite AS double)").alias("weather_risk_composite"),
    F.expr("CAST(is_high_value AS INT)").alias("is_high_value"),
    F.expr("CAST(coalesce(at_fault, false) AS INT)").alias("at_fault"),
    F.expr("CAST(coalesce(third_party_involved, false) AS INT)").alias("third_party_involved"),
    F.col("flood_risk_score").alias("postcode_flood_risk"),
)

# --- feature_reserve ---
feature_reserve = silver.select(
    "claim_public_id",
    encode("peril_type", "peril_type").alias("peril_type_encoded"),
    encode("handler_grade", "handler_grade").alias("handler_grade_encoded"),
    F.expr("CAST(log1p(total_incurred) AS double)").alias("reported_amount_log"),
    F.expr("coalesce(fraud_score, 0)").alias("fraud_score"),
    F.expr("coalesce(prior_claims_12m, 0)").alias("prior_claims_12m"),
    F.expr("CAST(weather_risk_composite AS double)").alias("weather_risk_composite"),
    # days_open: time on book — settled claims use days_to_settle, open use now-report.
    F.expr("""
        CASE WHEN claim_status IN ('settled','declined','withdrawn') THEN days_to_settle
             ELSE datediff(current_date(), report_date) END
    """).alias("days_open"),
    encode("triage_decision", "triage_decision").alias("triage_decision_encoded"),
    F.expr("CAST(log1p(coalesce(sum_insured, 0)) AS double)").alias("sum_insured_log"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Create / upsert the UC feature tables (idempotent)

# COMMAND ----------

def upsert_feature_table(name, df, description):
    fqn = tbl(name)
    if spark.catalog.tableExists(fqn):
        fe.write_table(name=fqn, df=df, mode="merge")   # idempotent upsert by PK
        action = "merged"
    else:
        fe.create_table(name=fqn, primary_keys=["claim_public_id"], df=df, description=description)
        action = "created"
    # layer=feature metadata (TBLPROPERTIES always works; UC tags governed-skipped here).
    spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES "
              f"('project'='claims_workbench','layer'='feature','wb_owner'='wryszka')")
    cdg.set_tags_safe(spark, f"TABLE {fqn}",
                      {"project": "claims_workbench", "layer": "feature", "owner": "wryszka"})
    print(f"{name}: {action}, {spark.table(fqn).count():,} rows")


upsert_feature_table("feature_triage", feature_triage,
                     "FNOL triage classifier features (Phase 5). PK claim_public_id. No labels.")
upsert_feature_table("feature_reserve", feature_reserve,
                     "Reserve-bracket model features (Phase 5). PK claim_public_id. No labels.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Targeted check

# COMMAND ----------

import json

n_silver = silver.count()
for name in ["feature_triage", "feature_reserve"]:
    fqn = tbl(name)
    n = spark.table(fqn).count()
    n_distinct = spark.table(fqn).select("claim_public_id").distinct().count()
    print(f"{name}: {n:,} rows | {n_distinct:,} distinct keys | silver {n_silver:,}")
    assert n == n_distinct, f"{name}: duplicate primary keys ({n} != {n_distinct})"
    assert n == n_silver, f"{name}: row count {n} != silver {n_silver}"
    # Confirm PK registered in UC (primary key constraint present).
    pk = spark.sql(f"""
        SELECT constraint_name FROM {catalog}.information_schema.table_constraints
        WHERE table_schema = '{schema}' AND table_name = '{name}' AND constraint_type = 'PRIMARY KEY'
    """).count()
    assert pk >= 1, f"{name}: no PRIMARY KEY constraint registered"
    print(f"  [OK] PK constraint registered, one row per claim_public_id.")

# COMMAND ----------

# --- vivid claim cc:900001 feature vectors ---
vt = spark.table(tbl("feature_triage")).where("claim_public_id = 'cc:900001'").collect()[0].asDict()
vr = spark.table(tbl("feature_reserve")).where("claim_public_id = 'cc:900001'").collect()[0].asDict()
print("feature_triage[cc:900001]:")
print(json.dumps(vt, indent=2, default=str))
print("feature_reserve[cc:900001]:")
print(json.dumps(vr, indent=2, default=str))

assert vt["fraud_score"] == 74, vt["fraud_score"]
assert vt["prior_claims_12m"] == 2, vt["prior_claims_12m"]
assert vt["reporting_lag_days"] == 18, vt["reporting_lag_days"]
assert vt["peril_type_encoded"] == ENCODINGS["peril_type"]["motor_tp"], vt["peril_type_encoded"]
assert vr["fraud_score"] == 74 and vr["prior_claims_12m"] == 2, vr
print("[OK] vivid claim feature vectors carry fraud_score=74, prior_claims_12m=2, reporting_lag_days=18.")

# COMMAND ----------

# --- encoding mapping persisted ---
enc = spark.table(tbl("ref_feature_encodings"))
print(f"ref_feature_encodings: {enc.count()} rows across {enc.select('feature').distinct().count()} features")
enc.orderBy("feature", "encoded_index").show(50, truncate=False)
assert enc.count() == sum(len(m) for m in ENCODINGS.values()), "encoding row count mismatch"
print("[OK] encoding mapping persisted for deterministic train/serve encoding.")
