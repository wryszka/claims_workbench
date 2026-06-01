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
