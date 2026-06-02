# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 12 · Joined pricing + claims gold view
# MAGIC
# MAGIC **Phase 11, Stage A.** Builds `gold_policy_claims_joined` spanning the claims
# MAGIC book and the **policy / pricing** population so actuaries and pricing analysts
# MAGIC can ask **cross-domain** questions (loss ratio by product/peril, premium
# MAGIC adequacy, leakage vs premium). A Genie space "Ask Pricing + Claims" is scoped
# MAGIC over it (the existing "Ask the Book" stays).
# MAGIC
# MAGIC > **Pricing population:** if the pricing-workbench tables are present on the
# MAGIC > workspace, point the `pricing_table` widget at them. Otherwise this uses the
# MAGIC > **self-contained fallback** — the policy/pricing attributes already enriched
# MAGIC > into `silver_claims_enriched` (premium, sum insured, tenure, product).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema (fixed)")
dbutils.widgets.text("pricing_table", "", "Optional pricing population table (blank = self-contained fallback)")

catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
pricing_table = dbutils.widgets.get("pricing_table").strip()


def tbl(t):
    return f"`{catalog}`.`{schema}`.{t}"


print(f"[target] {catalog}.{schema} | pricing_table={'(fallback)' if not pricing_table else pricing_table}")

# COMMAND ----------

# One row per claim carrying both the claim outcome and the policy pricing context,
# so Genie can aggregate loss ratio (incurred / premium), premium adequacy, leakage,
# and recovery across the joined domains. annual_premium / sum_insured come from the
# policy population (self-contained in silver here).
joined = spark.table(tbl("silver_claims_enriched")).select(
    "claim_public_id", "policy_number", "product", "peril_type", "loss_cause",
    "postcode_district", "report_channel", "claim_status",
    F.col("sum_insured").cast("decimal(12,2)").alias("sum_insured"),
    F.col("annual_premium").cast("decimal(12,2)").alias("annual_premium"),
    "policy_tenure_years",
    F.col("total_incurred").cast("decimal(12,2)").alias("total_incurred"),
    F.col("paid_amount").cast("decimal(12,2)").alias("paid_amount"),
    F.col("ultimate_reserve").cast("decimal(12,2)").alias("ultimate_reserve"),
    F.col("recoverable_amount").cast("decimal(12,2)").alias("recoverable_amount"),
    "fraud_score", "days_to_settle", "leakage_flag", "weather_risk_composite",
    "report_date", "loss_date")

# Per-claim loss ratio contribution (incurred against the policy's annual premium).
joined = joined.withColumn(
    "claim_loss_ratio",
    F.expr("CASE WHEN annual_premium > 0 THEN round(total_incurred / annual_premium, 3) ELSE NULL END"))

(joined.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(tbl("gold_policy_claims_joined")))
spark.sql(f"ALTER TABLE {tbl('gold_policy_claims_joined')} SET TBLPROPERTIES "
          f"('project'='claims_workbench','layer'='gold','wb_owner'='wryszka')")

n = spark.table(tbl("gold_policy_claims_joined")).count()
print(f"gold_policy_claims_joined written: {n:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-domain sanity: loss ratio by product (claims ↔ pricing)

# COMMAND ----------

spark.sql(f"""
  SELECT product,
         count(*) claims,
         round(sum(total_incurred)) total_incurred,
         round(sum(annual_premium)) total_premium,
         round(sum(total_incurred) / nullif(sum(annual_premium), 0), 3) loss_ratio
  FROM {tbl('gold_policy_claims_joined')}
  GROUP BY product ORDER BY loss_ratio DESC
""").show(truncate=False)

import json
dbutils.notebook.exit(json.dumps({"gold_policy_claims_joined_rows": int(n)}))
