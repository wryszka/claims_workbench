# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 06c · Cache layer + pre-warm
# MAGIC
# MAGIC **Phase 6, Stage C.** Creates the cache table, exercises the cache-first
# MAGIC wrapper (`app/utils/agent_cache.py`), and pre-warms `cache_agent_responses`
# MAGIC for the vivid claim against the sub-agents (and the Supervisor, once its
# MAGIC endpoint name is set).
# MAGIC
# MAGIC > Cache-first, **no TTL**, `USE_CACHE` switch. Generic over any endpoint.

# COMMAND ----------

# MAGIC %pip install databricks-sdk requests --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, sys, json, time

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema")
dbutils.widgets.text("warehouse_id", "ab79eced8207d29b", "SQL warehouse")
dbutils.widgets.text("fraud_endpoint", "agents_lr_serverless_aws_us_catalog-claims_workbench-agent_frau", "Fraud agent endpoint")
dbutils.widgets.text("context_endpoint", "agents_lr_serverless_aws_us_catalog-claims_workbench-agent_cont", "Context agent endpoint")
dbutils.widgets.text("supervisor_endpoint", "", "Supervisor endpoint (blank until created)")

catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
warehouse_id = dbutils.widgets.get("warehouse_id").strip()
fraud_ep = dbutils.widgets.get("fraud_endpoint").strip()
context_ep = dbutils.widgets.get("context_endpoint").strip()
supervisor_ep = dbutils.widgets.get("supervisor_endpoint").strip()

# config.py reads these at import time → set before importing the wrapper.
os.environ["CLAIMS_CATALOG"] = catalog
os.environ["CLAIMS_SCHEMA"] = schema
os.environ["CLAIMS_WAREHOUSE_ID"] = warehouse_id

# COMMAND ----------

# Create the cache table (idempotent) + layer=agent metadata.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.cache_agent_responses (
  cache_key     STRING,
  agent_name    STRING,
  input_json    STRING,
  response_json STRING,
  created_ts    TIMESTAMP,
  mode          STRING
) USING DELTA
COMMENT 'Cache-first store for Claims AI agent/endpoint responses (no TTL). mode=real.'
""")
spark.sql(f"ALTER TABLE `{catalog}`.`{schema}`.cache_agent_responses "
          f"SET TBLPROPERTIES ('project'='claims_workbench','layer'='agent','wb_owner'='wryszka')")
print("cache table ready.")

# COMMAND ----------

# Make the wrapper importable (synced under .../files/app).
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_app_dir = "/Workspace" + os.path.dirname(_ctx.notebookPath().get()).replace("/notebooks", "/app")
if _app_dir not in sys.path:
    sys.path.insert(0, _app_dir)
from utils.agent_cache import get_agent_response, cache_key  # noqa: E402
from utils.config import USE_CACHE, CACHE_TABLE  # noqa: E402
print(f"wrapper from {_app_dir} | USE_CACHE default={USE_CACHE} | table={CACHE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cache-first test + pre-warm (evidence returned via notebook exit)

# COMMAND ----------

def _count_key(k):
    return int(spark.sql(f"SELECT count(*) c FROM {CACHE_TABLE} WHERE cache_key='{k}'").collect()[0]["c"])

# Unique-per-run input so call#1 is a genuine miss (one nonce, reused for all 3 calls).
_nonce = str(time.time())
test_input = {"messages": [{"role": "user", "content": "Assess the fraud risk for claim cc:900001."}],
              "custom_inputs": {"claim_public_id": "cc:900001", "_test_nonce": _nonce}}
_key = cache_key(fraud_ep, test_input)
pre = _count_key(_key)


def _timed(use_cache):
    t0 = time.time()
    out = get_agent_response(fraud_ep, test_input, use_cache=use_cache)
    return out["cache"], round(time.time() - t0, 2), out["response"].get("messages", [{}])[0].get("content", "")[:80]


c1, t1, s1 = _timed(True)     # expect miss  (real + save)
c2, t2, s2 = _timed(True)     # expect hit   (from cache, fast)
c3, t3, s3 = _timed(False)    # expect bypass (real)

# Pre-warm the vivid claim against the sub-agents (and supervisor if set).
VIVID = "cc:900001"
prompts = {fraud_ep: "Assess the fraud risk for claim cc:900001.",
           context_ep: "Give me the before-you-pick-up-the-phone brief for claim cc:900001."}
if supervisor_ep:
    prompts[supervisor_ep] = "Help me handle claim cc:900001: triage, reserve, fraud risk and a briefing."
warmed = {}
for ep, prompt in prompts.items():
    inp = {"messages": [{"role": "user", "content": prompt}], "custom_inputs": {"claim_public_id": VIVID}}
    warmed[ep[-20:]] = get_agent_response(ep, inp, use_cache=False)["cache"]

total_rows = int(spark.sql(f"SELECT count(*) c FROM {CACHE_TABLE}").collect()[0]["c"])
evidence = {
    "pre_count_for_test_key": pre,
    "call1_use_cache_true":  {"cache": c1, "secs": t1},
    "call2_use_cache_true":  {"cache": c2, "secs": t2},
    "call3_use_cache_false": {"cache": c3, "secs": t3},
    "hit_faster_than_miss": bool(t2 < t1),
    "same_response_hit_vs_miss": bool(s1 == s2),
    "prewarmed": warmed,
    "cache_rows_total": total_rows,
    "supervisor_set": bool(supervisor_ep),
}
print(json.dumps(evidence, indent=2))
dbutils.notebook.exit(json.dumps(evidence))
