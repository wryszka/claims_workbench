# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 01b · Tag Bronze Tables
# MAGIC
# MAGIC DLT manages the bronze tables and may reset Unity Catalog tags on a full
# MAGIC refresh, so UC tags are applied as a short **post-step** after the pipeline
# MAGIC run (rather than mid-pipeline). Re-run this after any full refresh of
# MAGIC `claims_workbench_01_bronze_dlt`.
# MAGIC
# MAGIC Applies `project=claims_workbench`, `layer=bronze`, `owner=wryszka`.
# MAGIC Tagging is resilient to governed tag policies (a restricted key is logged
# MAGIC and skipped, not fatal — same approach as Phase 0).

# COMMAND ----------

import os
import sys

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema (fixed)")

catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
print(f"[target] {catalog}.{schema}")

# Reuse the resilient per-key tagger from the Phase 0 helper module.
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_helper_dir = "/Workspace" + os.path.dirname(_ctx.notebookPath().get())
if _helper_dir not in sys.path:
    sys.path.insert(0, _helper_dir)
import claims_data_gen as cdg

# COMMAND ----------

BRONZE_TABLES = [
    "bronze_gw_cc_claim",
    "bronze_gw_cc_exposure",
    "bronze_gw_cc_incident",
    "bronze_gw_cc_contact",
    "bronze_gw_pc_policy",
    "bronze_fraud_signals_raw",
    "bronze_weather_raw",
    "bronze_quarantine_claims",
    "bronze_quarantine_fraud_signals",
]

tags = {"project": "claims_workbench", "layer": "bronze", "owner": "wryszka"}
existing = {r["tableName"] for r in spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()}

for t in BRONZE_TABLES:
    if t not in existing:
        print(f"[skip] {t} not found (pipeline not run yet?)")
        continue
    applied, skipped = cdg.set_tags_safe(spark, f"TABLE `{catalog}`.`{schema}`.`{t}`", tags)
    print(f"[tagged] {t}: applied={applied} skipped={skipped}")

print("Done.")
