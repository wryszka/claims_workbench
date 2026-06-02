# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 08 · App backend test (Stage A)
# MAGIC
# MAGIC Exercises the **real app backend** (`app/server/claims_service.py`) against live
# MAGIC infra — proves the Claims AI panels, synthesis (cache-first), and the HITL
# MAGIC decision write work, without needing the React UI deployed (deploy = Stage C).

# COMMAND ----------

# MAGIC %pip install databricks-sdk requests nest_asyncio --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, sys, json, asyncio
import nest_asyncio
nest_asyncio.apply()  # notebooks already run an event loop; allow asyncio.run()

# Make the WorkspaceClient use ambient notebook auth (not a CLI profile).
os.environ["DATABRICKS_APP_NAME"] = "backend-test"
os.environ.setdefault("CATALOG_NAME", "lr_serverless_aws_us_catalog")
os.environ.setdefault("SCHEMA_NAME", "claims_workbench")
os.environ.setdefault("WAREHOUSE_ID", "ab79eced8207d29b")
os.environ.setdefault("USE_CACHE", "true")

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_app_dir = "/Workspace" + os.path.dirname(_ctx.notebookPath().get()).replace("/notebooks", "/app")
if _app_dir not in sys.path:
    sys.path.insert(0, _app_dir)

from server import claims_service as svc  # noqa: E402

CID = "cc:900001"

# COMMAND ----------

claims = asyncio.run(svc.list_claims(10))
print(f"list_claims -> {len(claims)} rows; first = {claims[0]['claim_public_id']} (vivid pinned: {claims[0]['claim_public_id']=='cc:900001'})")

panels = asyncio.run(svc.get_panels(CID))
print("\nPANELS for cc:900001:")
print("  summary:", json.dumps(panels["summary"]))
print("  triage :", json.dumps(panels["triage"]))
print("  reserve:", json.dumps(panels["reserve"]))
print("  fraud  :", json.dumps(panels["fraud"]))
print("  policy :", json.dumps(panels["policy"]))

# COMMAND ----------

synth = asyncio.run(svc.get_synthesis(CID))
print(f"SYNTHESIS (endpoint={synth['endpoint'][-24:]}, supervisor={synth['supervisor']}, cache={synth['cache']}):")
print(synth["text"][:600])

# COMMAND ----------

acc = asyncio.run(svc.log_decision(CID, panels["triage"].get("decision", ""),
                                   panels["triage"].get("confidence"), "accept", False, ""))
ovr = asyncio.run(svc.log_decision(CID, panels["triage"].get("decision", ""),
                                   panels["triage"].get("confidence"), "override", True, "Local knowledge"))
print("ACCEPT decision:", json.dumps(acc))
print("OVERRIDE decision:", json.dumps(ovr))

print("\ngold_handler_decisions (recent):")
for d in asyncio.run(svc.recent_decisions(5)):
    print(" ", json.dumps(d, default=str))

# COMMAND ----------

evidence = {
    "claims_listed": len(claims),
    "vivid_pinned_first": claims[0]["claim_public_id"] == "cc:900001",
    "triage_decision": panels["triage"].get("decision"),
    "triage_confidence": panels["triage"].get("confidence"),
    "reserve_bracket": panels["reserve"].get("bracket"),
    "fraud_score": panels["fraud"].get("fraud_score"),
    "prior_claims_12m": panels["fraud"].get("prior_claims_12m"),
    "reporting_lag_days": panels["fraud"].get("reporting_lag_days"),
    "synthesis_chars": len(synth.get("text") or ""),
    "synthesis_cache": synth.get("cache"),
    "synthesis_supervisor": synth.get("supervisor"),
    "accept_decision_id": acc["decision_id"],
    "override_decision_id": ovr["decision_id"],
    "audit_rows": len(asyncio.run(svc.recent_decisions(50))),
}
print(json.dumps(evidence, indent=2))
dbutils.notebook.exit(json.dumps(evidence))
