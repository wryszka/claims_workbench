# Databricks notebook source
# MAGIC %md
# MAGIC # 08c · Cached vivid-journey timing (Stage C, <3s target)
# MAGIC The app fetches panels and synthesis in parallel; this times each component
# MAGIC with warm endpoints + cache-first synthesis.

# COMMAND ----------

# MAGIC %pip install databricks-sdk requests nest_asyncio --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, sys, json, time, asyncio
import nest_asyncio
nest_asyncio.apply()
os.environ["DATABRICKS_APP_NAME"] = "perf-test"
os.environ.setdefault("CATALOG_NAME", "lr_serverless_aws_us_catalog")
os.environ.setdefault("SCHEMA_NAME", "claims_workbench")
os.environ.setdefault("WAREHOUSE_ID", "ab79eced8207d29b")
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_app = "/Workspace" + os.path.dirname(_ctx.notebookPath().get()).replace("/notebooks", "/app")
sys.path.insert(0, _app)
from server import claims_service as svc  # noqa: E402
CID = "cc:900001"

# Warm: endpoints + synthesis cache (use_cache=False saves the exact synthesis input)
asyncio.run(svc.get_panels(CID))
svc.set_cache_mode(False); asyncio.run(svc.get_synthesis(CID))
svc.set_cache_mode(True)

# Timed (warm + cached) — the two run in parallel in the app
t0 = time.time(); asyncio.run(svc.get_panels(CID)); t_panels = time.time() - t0
t0 = time.time(); s = asyncio.run(svc.get_synthesis(CID)); t_synth = time.time() - t0
journey = max(t_panels, t_synth)  # app fetches both concurrently

ev = {"panels_secs": round(t_panels, 2), "synthesis_secs": round(t_synth, 2),
      "synthesis_cache": s["cache"], "parallel_journey_secs": round(journey, 2),
      "under_3s": journey < 3.0}
print(json.dumps(ev, indent=2))
dbutils.notebook.exit(json.dumps(ev))
