# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 00 · Setup & Data Generation
# MAGIC
# MAGIC **Bricksurance SE** — Phase 0 of the Claims Intelligence Workbench.
# MAGIC
# MAGIC This notebook scaffolds Unity Catalog objects and lands a **synthetic
# MAGIC Guidewire ClaimCenter Cloud Data Access (CDA) simulation** — ~120,000
# MAGIC motor & home property claims over the trailing 36 months.
# MAGIC
# MAGIC > **About this demo.** This is a synthetic demonstration. All company
# MAGIC > names, policy data, and financial figures are entirely fictional.
# MAGIC > There is no real Guidewire integration and no real customer data is used.
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Resolves the target catalog (widget → else workspace current catalog).
# MAGIC 2. Creates `<catalog>.claims_workbench` and tags it.
# MAGIC 3. Generates 9 Delta tables (Guidewire CDA + enrichment + reference) and tags each.
# MAGIC 4. Runs a targeted check (row counts, sample row, the vivid claim, tags).
# MAGIC
# MAGIC Everything is **idempotent** (overwrite) and uses **rolling dates** anchored
# MAGIC to `current_date()`, so re-running never duplicates and the demo never goes stale.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Install dependencies
# MAGIC `dbldatagen` drives the synthetic generation. If your workspace blocks the
# MAGIC default index, fall back to: `%pip install --break-system-packages dbldatagen`.

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Resolve catalog & schema
# MAGIC The `catalog` widget is **empty by default**. Leave it blank and we resolve
# MAGIC to the workspace's current catalog at run time — so it "just works" on any
# MAGIC dev workspace. Set the widget (or pass the DAB `catalog` variable) to pin one.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema (fixed)")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"

if not catalog:
    catalog = spark.catalog.currentCatalog()
    print(f"[catalog] widget blank -> resolved to workspace current catalog: {catalog}")
else:
    print(f"[catalog] using widget value: {catalog}")

print(f"[schema]  {schema}")
print(f"[target]  {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Create & tag the schema
# MAGIC Tags are applied per key and resilient to **governed tag policies** — if a
# MAGIC workspace restricts the allowed values for a tag key (e.g. `project`), that
# MAGIC key is logged and skipped rather than failing the run. The full tag scheme
# MAGIC applies unchanged on ungoverned workspaces.

# COMMAND ----------

import os
import sys

# Load the helper module first (needed for set_tags_safe).
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_nb_path = _ctx.notebookPath().get()
_helper_dir = "/Workspace" + os.path.dirname(_nb_path)
if _helper_dir not in sys.path:
    sys.path.insert(0, _helper_dir)
import claims_data_gen as cdg

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
applied, skipped = cdg.set_tags_safe(
    spark, f"SCHEMA `{catalog}`.`{schema}`",
    {"project": "claims_workbench", "owner": "wryszka", "demo": "bricksurance_se"},
)
print(f"Schema `{catalog}`.`{schema}` ready. Tags applied: {applied}")
if skipped:
    print(f"Tags skipped (governed tag policy on this workspace): {skipped}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Generate & tag all tables
# MAGIC Generation logic lives in the reusable helper module `claims_data_gen.py`
# MAGIC (same folder), so Phase 9 (reset) can re-anchor the dates with one call.

# COMMAND ----------

# Helper module already imported in section 3 (cdg).
print(f"Helper module loaded from: {_helper_dir}")
print(f"Vivid claim id: {cdg.VIVID_CLAIM_ID}")

# COMMAND ----------

# anchor=None -> dates roll relative to current_date(). Phase 9 can pass a date.
counts = cdg.generate_all(spark, catalog, schema, anchor=None)

print("Row counts:")
for tbl, n in counts.items():
    print(f"  {tbl:<28} {n:>8,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Targeted check
# MAGIC Not a full smoke test — just confirm the data landed and the demo invariants hold.

# COMMAND ----------

import json

# --- 5a · Row-count assertions ---
n_claims = counts["bronze_gw_cc_claim"]
n_handlers = counts["ref_handlers"]
assert 119_000 <= n_claims <= 121_500, f"cc_claim count off: {n_claims:,}"
assert 75 <= n_handlers <= 85, f"handlers count off: {n_handlers}"
print(f"[OK] cc_claim ≈ 120k (actual {n_claims:,}), handlers ≈ 80 (actual {n_handlers}).")

# COMMAND ----------

# --- 5b · One sample row from bronze_gw_cc_claim (pretty-printed) ---
sample = (
    spark.table(f"`{catalog}`.`{schema}`.bronze_gw_cc_claim")
    .where("claim_public_id <> 'cc:900001'")
    .limit(1)
    .collect()[0]
    .asDict()
)
print("Sample claim:")
print(json.dumps(sample, indent=2, default=str))

# COMMAND ----------

# --- 5c · The SACRED vivid claim cc:900001 ---
v = (
    spark.table(f"`{catalog}`.`{schema}`.bronze_gw_cc_claim")
    .where("claim_public_id = 'cc:900001'")
    .collect()
)
assert len(v) == 1, f"expected exactly 1 vivid claim, found {len(v)}"
vd = v[0].asDict()
assert vd["loss_cause"] == "vehcollision", vd
assert vd["report_channel"] == "phone", vd
assert vd["total_incurred"] == 8500, vd

vf = (
    spark.table(f"`{catalog}`.`{schema}`.bronze_fraud_signals_raw")
    .where("claim_public_id = 'cc:900001'")
    .collect()[0]
    .asDict()
)
assert vf["fraud_score"] == 74 and vf["prior_claims_12m"] == 2 and vf["days_since_incident"] == 18, vf

vc = (
    spark.table(f"`{catalog}`.`{schema}`.bronze_gw_cc_contact")
    .where("claim_public_id = 'cc:900001'")
    .collect()[0]
    .asDict()
)
assert vc["postcode_district"] == "M1", vc
print("[OK] Vivid claim cc:900001 present with correct attributes:")
print(json.dumps({**vd, **vf, "postcode_district": vc["postcode_district"]}, indent=2, default=str))

# COMMAND ----------

# --- 5d · Confirm UC tags landed (governed tag policies may restrict some keys) ---
schema_tags = spark.sql(f"""
    SELECT tag_name, tag_value
    FROM `{catalog}`.information_schema.schema_tags
    WHERE schema_name = '{schema}'
""").collect()
schema_tag_map = {r["tag_name"]: r["tag_value"] for r in schema_tags}
print(f"Schema tags applied: {schema_tag_map}")

table_tag_cov = spark.sql(f"""
    SELECT tag_name, count(*) AS n_tables
    FROM `{catalog}`.information_schema.table_tags
    WHERE schema_name = '{schema}'
    GROUP BY tag_name ORDER BY tag_name
""").collect()
table_tag_map = {r["tag_name"]: r["n_tables"] for r in table_tag_cov}
print(f"Table tag coverage: {table_tag_map}")

# At least one UC tag must have applied somewhere; if a governed tag policy
# blocked 'project' on this workspace, the run still succeeds and other keys
# (owner/demo/layer) carry through.
assert schema_tag_map or table_tag_map, "no UC tags applied to schema or any table"
if "project" in schema_tag_map or "project" in table_tag_map:
    print("[OK] project=claims_workbench tag present.")
else:
    print("[OK] UC tags present; 'project' skipped by this workspace's governed "
          "tag policy (expected on governed workspaces — applies cleanly elsewhere).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done — Phase 0 complete
# MAGIC All tables landed, tagged, and verified. Next phases (DLT, models, agents,
# MAGIC app) build on top of these bronze/reference tables. The vivid claim
# MAGIC **cc:900001** is reproducible and survives every reset.
