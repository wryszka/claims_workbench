# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Workbench — 08b · Stage B backend test
# MAGIC Verifies the Ingestion / Transformation / Governance endpoints + the
# MAGIC USE_CACHE toggle's effect on synthesis, against live infra.

# COMMAND ----------

# MAGIC %pip install databricks-sdk requests nest_asyncio --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, sys, json, asyncio
import nest_asyncio
nest_asyncio.apply()

os.environ["DATABRICKS_APP_NAME"] = "backend-test"
os.environ.setdefault("CATALOG_NAME", "lr_serverless_aws_us_catalog")
os.environ.setdefault("SCHEMA_NAME", "claims_workbench")
os.environ.setdefault("WAREHOUSE_ID", "ab79eced8207d29b")

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_app = "/Workspace" + os.path.dirname(_ctx.notebookPath().get()).replace("/notebooks", "/app")
if _app not in sys.path:
    sys.path.insert(0, _app)
from server import claims_service as svc  # noqa: E402

CID = "cc:900001"

# COMMAND ----------

ing = asyncio.run(svc.ingestion_status())
print("INGESTION:", json.dumps({k: ing[k] for k in ["pipeline_name", "state", "pass_rate",
      "total_evaluated", "quarantined_claims", "quarantined_fraud"]}, default=str))
print("  expectations:", len(ing.get("expectations", [])), "rules")

enr = asyncio.run(svc.enrichment(CID))
print(f"\nENRICHMENT cc:900001: {len(enr)} fields | peril={enr.get('peril_type')} "
      f"sum_insured={enr.get('sum_insured')} reserve_bracket={enr.get('reserve_bracket')} "
      f"weather={enr.get('weather_risk_composite')}")

gov = asyncio.run(svc.governance_links())
print("\nGOVERNANCE:", json.dumps({"dashboard_url": gov.get("dashboard_url"),
      "genie_url": gov.get("genie_url"), "lineage_assets": len(gov.get("lineage", []))}, default=str))

reset = svc.reset_available()
print(f"\nRESET job available: {reset} (expect False until Phase 9)")

# COMMAND ----------

# USE_CACHE toggle effect on synthesis
svc.set_cache_mode(False)
s_live = asyncio.run(svc.get_synthesis(CID))      # bypass -> live + save
svc.set_cache_mode(True)
s_cached = asyncio.run(svc.get_synthesis(CID))     # hit -> from cache
print(f"USE_CACHE=False -> synthesis cache={s_live['cache']}")
print(f"USE_CACHE=True  -> synthesis cache={s_cached['cache']}")

evidence = {
    "ingestion_pass_rate": ing.get("pass_rate"),
    "quarantined": (ing.get("quarantined_claims") or 0) + (ing.get("quarantined_fraud") or 0),
    "ingestion_state": str(ing.get("state")),
    "enrichment_fields": len(enr),
    "dashboard_resolved": bool(gov.get("dashboard_url")),
    "genie_resolved": bool(gov.get("genie_url")),
    "lineage_assets": len(gov.get("lineage", [])),
    "reset_available": reset,
    "synthesis_live": s_live["cache"],
    "synthesis_cached": s_cached["cache"],
}
print(json.dumps(evidence, indent=2))
dbutils.notebook.exit(json.dumps(evidence))
