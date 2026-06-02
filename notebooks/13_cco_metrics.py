# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 13 · CCO metrics snapshot (trends)
# MAGIC
# MAGIC **Phase 11 / CCO uplift.** Builds `gold_cco_metrics_daily`, the daily snapshot
# MAGIC behind the Control Tower trend arrows and the Trends view. Computes **today's**
# MAGIC metrics from silver + disposition + reserve development, and seeds a deterministic
# MAGIC **rolling 12-week history** (no RNG — derived from today's values with a gentle
# MAGIC drift) so trends render immediately and improve toward today. Idempotent: rewrites
# MAGIC the table each run, so a reset re-anchors the whole series to current_date().

# COMMAND ----------

import json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, LongType

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema (fixed)")
catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"


def tbl(t):
    return f"`{catalog}`.`{schema}`.{t}"


PERIL_SLA_SQL = "(CASE peril_type WHEN 'motor_tp' THEN 30 WHEN 'home_fire' THEN 60 ELSE 45 END)"
print(f"[target] {catalog}.{schema}")

# COMMAND ----------

# Today's real metrics.
m = spark.sql(f"""
  SELECT
    count(*) total,
    sum(CASE WHEN claim_status IN ('open','under_investigation') THEN 1 ELSE 0 END) open_inv,
    sum(CASE WHEN claim_status IN ('open','under_investigation')
              AND datediff(current_date(), report_date) > {PERIL_SLA_SQL} THEN 1 ELSE 0 END) past_sla,
    sum(CASE WHEN total_incurred>50000 AND claim_status IN ('open','under_investigation') THEN 1 ELSE 0 END) large_losses,
    round(sum(CASE WHEN claim_status IN ('open','under_investigation') THEN ultimate_reserve ELSE 0 END)) total_reserves,
    round(100.0*avg(CASE WHEN leakage_flag THEN 1 ELSE 0 END),1) leakage_rate,
    round(avg(CASE WHEN days_to_settle IS NOT NULL THEN days_to_settle END),1) avg_settle_days,
    round(100.0*avg(CASE WHEN coalesce(is_potential_fraud,false) THEN 1 ELSE 0 END),1) fraud_refer_rate,
    sum(CASE WHEN recovery_flag AND claim_status IN ('open','under_investigation') THEN 1 ELSE 0 END) recovery_count,
    round(sum(CASE WHEN recovery_flag AND claim_status IN ('open','under_investigation') THEN recoverable_amount ELSE 0 END)) recoverable_total
  FROM {tbl('silver_claims_enriched')}""").collect()[0].asDict()

disp = spark.sql(f"""
  SELECT round(100.0*avg(CASE WHEN disposition='auto_closed' THEN 1 ELSE 0 END),1) pct_auto_closed,
         sum(CASE WHEN disposition='auto_closed' THEN 1 ELSE 0 END) auto_closed
  FROM {tbl('gold_claim_disposition')}""").collect()[0].asDict()

eow = spark.sql(f"""
  SELECT round(sum(sum_ultimate_reserve)/nullif(sum(sum_initial_reserve),0),3) dev_ratio
  FROM {tbl('gold_reserve_development')} WHERE peril_type='home_escape_water'""").collect()[0].asDict()

open_inv = int(m["open_inv"]) or 1
sla_breach_pct = round(100.0 * int(m["past_sla"]) / open_inv, 1)
today = {
    "total": int(m["total"]), "open_inv": open_inv,
    "pct_auto_closed": float(disp["pct_auto_closed"] or 0), "auto_closed": int(disp["auto_closed"] or 0),
    "escalated": int(m["total"]) - int(disp["auto_closed"] or 0),
    "past_sla": int(m["past_sla"]), "sla_breach_pct": sla_breach_pct,
    "large_losses": int(m["large_losses"]), "total_reserves": float(m["total_reserves"] or 0),
    "leakage_rate": float(m["leakage_rate"] or 0), "avg_settle_days": float(m["avg_settle_days"] or 0),
    "fraud_refer_rate": float(m["fraud_refer_rate"] or 0),
    "recovery_count": int(m["recovery_count"]), "recoverable_total": float(m["recoverable_total"] or 0),
    "eow_dev_ratio": float(eow["dev_ratio"] or 0),
}
print(json.dumps(today, indent=2))

# COMMAND ----------

# Deterministic rolling history: weekly points k = 12 (oldest) .. 0 (today). Metrics
# drift so today is the best point (auto-close up, leakage/settle/SLA down, recovery up).
def wobble(k, amp):
    return ((k * 7) % 5 - 2) * amp   # small deterministic +/- variation

rows = []
import datetime
for k in range(12, -1, -1):
    rows.append({
        "snapshot_date": F.expr(f"date_sub(current_date(), {k*7})"),
        "_k": k,
    })

# Build via SQL so date_sub evaluates server-side (no Python date use).
hist = []
for k in range(12, -1, -1):
    pac = max(1.0, round(today["pct_auto_closed"] - 0.45 * k + wobble(k, 0.1), 1))
    leak = round(today["leakage_rate"] + 0.16 * k + wobble(k, 0.05), 1)
    settle = round(today["avg_settle_days"] + 0.9 * k + wobble(k, 0.2), 1)
    sla = round(today["sla_breach_pct"] + 0.7 * k + wobble(k, 0.15), 1)
    rec = round(today["recoverable_total"] * (1 - 0.018 * k), 0)
    oi = int(round(today["open_inv"] * (1 + 0.004 * k)))
    ac = int(round(today["auto_closed"] * max(0.2, 1 - 0.05 * k)))
    hist.append((k, pac, leak, settle, sla, rec, oi, ac))

schema_t = StructType([
    StructField("snapshot_date", DateType()), StructField("total", LongType()),
    StructField("open_inv", LongType()), StructField("pct_auto_closed", DoubleType()),
    StructField("auto_closed", LongType()), StructField("escalated", LongType()),
    StructField("past_sla", LongType()), StructField("sla_breach_pct", DoubleType()),
    StructField("large_losses", LongType()), StructField("total_reserves", DoubleType()),
    StructField("leakage_rate", DoubleType()), StructField("avg_settle_days", DoubleType()),
    StructField("fraud_refer_rate", DoubleType()), StructField("recovery_count", LongType()),
    StructField("recoverable_total", DoubleType()), StructField("eow_dev_ratio", DoubleType()),
])

# Assemble with SQL date_sub for each k, union together.
parts = []
for (k, pac, leak, settle, sla, rec, oi, ac) in hist:
    parts.append(spark.sql(f"""
        SELECT date_sub(current_date(), {k*7}) AS snapshot_date,
               {today['total']}L total, {oi}L open_inv, {pac}D pct_auto_closed, {ac}L auto_closed,
               {today['total']-ac}L escalated, CAST(round({oi}*{sla}/100.0) AS BIGINT) past_sla, {sla}D sla_breach_pct,
               {today['large_losses']}L large_losses, {today['total_reserves']}D total_reserves,
               {leak}D leakage_rate, {settle}D avg_settle_days, {today['fraud_refer_rate']}D fraud_refer_rate,
               {today['recovery_count']}L recovery_count, {rec}D recoverable_total, {today['eow_dev_ratio']}D eow_dev_ratio
    """))
df = parts[0]
for p in parts[1:]:
    df = df.unionByName(p)

(df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
   .saveAsTable(tbl("gold_cco_metrics_daily")))
spark.sql(f"ALTER TABLE {tbl('gold_cco_metrics_daily')} SET TBLPROPERTIES "
          f"('project'='claims_workbench','layer'='gold','wb_owner'='wryszka')")
n = spark.table(tbl("gold_cco_metrics_daily")).count()
print(f"gold_cco_metrics_daily written: {n} weekly snapshots")
spark.sql(f"SELECT cast(snapshot_date AS string) d, pct_auto_closed, leakage_rate, avg_settle_days, sla_breach_pct FROM {tbl('gold_cco_metrics_daily')} ORDER BY snapshot_date").show(13, truncate=False)
dbutils.notebook.exit(json.dumps({"snapshots": int(n), "today": today}))
