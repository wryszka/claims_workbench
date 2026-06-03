# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 09 · Reset: clear + re-warm cache
# MAGIC
# MAGIC Final task of `claims_workbench_99_reset_demo`. Clears
# MAGIC `cache_agent_responses` and re-warms the vivid claim against the synthesis
# MAGIC endpoint (Supervisor, or Context fallback) and the two sub-agents, so the
# MAGIC app is instant immediately after a reset. No model retrain.

# COMMAND ----------

# MAGIC %pip install databricks-sdk requests nest_asyncio --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, sys, json, asyncio
import nest_asyncio
nest_asyncio.apply()

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse for the cache layer (blank = resolve)")
catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
warehouse_id = dbutils.widgets.get("warehouse_id").strip()

os.environ["DATABRICKS_APP_NAME"] = "reset-job"     # WorkspaceClient uses ambient auth
os.environ["CATALOG_NAME"] = catalog
os.environ["SCHEMA_NAME"] = schema

# The cache layer (agent_cache / server.sql) talks to a SQL warehouse — config defaults
# to the SERVERLESS warehouse id, which doesn't exist on dev, so cache writes silently
# fail. Set WAREHOUSE_ID from the job var, or resolve a running warehouse on THIS
# workspace, BEFORE importing config (it reads env at import).
if not warehouse_id:
    try:
        from databricks.sdk import WorkspaceClient as _WC0
        whs = list(_WC0().warehouses.list())
        warehouse_id = next((x.id for x in whs if str(x.state) and "RUNNING" in str(x.state)), whs[0].id if whs else "")
    except Exception as e:
        print(f"warehouse resolution skipped: {str(e)[:120]}")
if warehouse_id:
    os.environ["WAREHOUSE_ID"] = warehouse_id
    print(f"cache warehouse: {warehouse_id}")

# Resolve the agent serving endpoints from THIS workspace by substring (dev uses full
# names agents_<cat>-<schema>-agent_context, serverless truncates to agent_cont) and set
# the CLAIMS_EP_* env BEFORE importing config — otherwise config falls back to the
# serverless defaults and warming 404s on dev. Cache-warming is best-effort regardless.
from databricks.sdk import WorkspaceClient as _WC      # noqa: E402
try:
    _eps = [e.name for e in _WC().serving_endpoints.list()]
    _frau = next((n for n in _eps if "agent_frau" in n), "")
    _cont = next((n for n in _eps if "agent_cont" in n), "")
    if _frau:
        os.environ["CLAIMS_EP_FRAUD"] = _frau
    if _cont:
        os.environ["CLAIMS_EP_CONTEXT"] = _cont
    print(f"resolved endpoints — fraud={_frau or '(default)'} context={_cont or '(default)'}")
except Exception as e:
    print(f"endpoint resolution skipped ({str(e)[:120]}) — using config defaults")

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_app = "/Workspace" + os.path.dirname(_ctx.notebookPath().get()).replace("/notebooks", "/app")
if _app not in sys.path:
    sys.path.insert(0, _app)
from server import claims_service as svc      # noqa: E402
from utils.agent_cache import get_agent_response  # noqa: E402
from utils import config                      # noqa: E402

CID = "cc:900001"

# COMMAND ----------

# 1) Clear the cache
spark.sql(f"TRUNCATE TABLE `{catalog}`.`{schema}`.cache_agent_responses")
print("cache cleared.")

# 1b) Wipe ephemeral app-created sandbox claims (Phase 12 B2) — sacred heroes untouched.
try:
    if spark.catalog.tableExists(f"`{catalog}`.`{schema}`.app_sandbox_claims"):
        n = spark.table(f"`{catalog}`.`{schema}`.app_sandbox_claims").count()
        spark.sql(f"TRUNCATE TABLE `{catalog}`.`{schema}`.app_sandbox_claims")
        print(f"sandbox claims wiped: {n} row(s).")
    else:
        print("sandbox claims table not present (nothing to wipe).")
except Exception as e:
    print(f"sandbox wipe skipped: {e}")

# 2) Re-warm the vivid claim — synthesis (exact app input) + both sub-agents.
# Best-effort: a warming hiccup must NEVER fail the reset (the cache was already cleared).
warmed = {}
try:
    syn = asyncio.run(svc.get_synthesis(CID, use_cache=False))      # bypass -> real + save
    warmed["synthesis"] = {"endpoint": syn["endpoint"][-24:], "cache": syn["cache"], "chars": len(syn.get("text") or "")}
except Exception as e:
    warmed["synthesis"] = f"error: {str(e)[:120]}"

for label, ep, prompt in [
    ("fraud", config.ENDPOINT_FRAUD, "Assess the fraud risk for claim cc:900001."),
    ("context", config.ENDPOINT_CONTEXT, "Give me the before-you-pick-up-the-phone brief for claim cc:900001."),
]:
    inp = {"messages": [{"role": "user", "content": prompt}], "custom_inputs": {"claim_public_id": CID}}
    try:
        out = get_agent_response(ep, inp, use_cache=False)
        warmed[label] = out["cache"]
    except Exception as e:
        warmed[label] = f"error: {e}"

rows = spark.sql(f"SELECT count(*) c FROM `{catalog}`.`{schema}`.cache_agent_responses").collect()[0]["c"]
print(json.dumps({"warmed": warmed, "cache_rows": int(rows)}, indent=2))
dbutils.notebook.exit(json.dumps({"cache_rows": int(rows), "warmed": list(warmed.keys())}))
