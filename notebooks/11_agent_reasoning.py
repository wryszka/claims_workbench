# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 11 · Agent-reasoning persistence
# MAGIC
# MAGIC **Phase 11, Stage A.** Creates `agent_reasoning_log` and populates it by calling
# MAGIC each deployed sub-agent for the hero claims. Every row captures an agent's
# MAGIC reasoning so it is **queryable and regulator-viewable** — the Audit agent and
# MAGIC the governance "what's collected" page read this table.
# MAGIC
# MAGIC > Run AFTER the agents are deployed (06_agents) and after 10_auto_close (so the
# MAGIC > disposition the Challenge/Audit agents reason over exists).

# COMMAND ----------

# MAGIC %pip install databricks-sdk requests --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json, uuid, time
import requests
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema (fixed)")
dbutils.widgets.text("claim_ids", "cc:900001,cc:900002", "Hero claims to log reasoning for")

catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
claim_ids = [c.strip() for c in dbutils.widgets.get("claim_ids").split(",") if c.strip()]


def tbl(t):
    return f"`{catalog}`.`{schema}`.{t}"


def esc(v):
    return (v or "").replace("'", "''")


print(f"[target] {catalog}.{schema} | claims={claim_ids}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · agent_reasoning_log (regulator-viewable)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tbl('agent_reasoning_log')} (
  reasoning_id    STRING,
  claim_public_id STRING,
  agent_name      STRING,
  input           STRING,
  reasoning_text  STRING,
  output          STRING,
  created_ts      TIMESTAMP
) USING DELTA
COMMENT 'Persisted agent reasoning for each claim — queryable, regulator-viewable. Populated from agent responses / MLflow traces.'
""")
spark.sql(f"ALTER TABLE {tbl('agent_reasoning_log')} SET TBLPROPERTIES "
          f"('project'='claims_workbench','layer'='agent','wb_owner'='wryszka')")
print("agent_reasoning_log ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Resolve the deployed agent endpoints (DAB/agents auto-truncate the name)

# COMMAND ----------

w = WorkspaceClient()
host = w.config.host.rstrip("/")
hdr = w.config._header_factory()
all_eps = [e.name for e in w.serving_endpoints.list()]

# agents.deploy truncates the UC model name (e.g. agent_fraud -> ...agent_frau,
# agent_context -> ...agent_cont). Match on 'agent_' + first 4 letters of the role.
AGENTS = {"fraud": "Fraud", "context": "Claim 360 / Dossier", "challenge": "Challenge",
          "recovery": "Recovery / Subrogation", "audit": "Audit / Reasoning"}
PROMPTS = {
    "fraud":     "Assess the fraud risk for claim {cid}.",
    "context":   "Give me the full Claim 360 dossier for claim {cid}.",
    "challenge": "Give me the second opinion — argue the opposite of the current disposition for claim {cid}.",
    "recovery":  "Is there any recovery or subrogation potential on claim {cid}?",
    "audit":     "Explain, for a regulator, how the decision on claim {cid} was reached.",
}


def find_ep(role):
    token = "agent_" + role[:4]
    return next((n for n in all_eps if token in n), None)


resolved = {r: find_ep(r) for r in AGENTS}
print(json.dumps(resolved, indent=2))


def invoke(ep, cid, prompt):
    payload = {"messages": [{"role": "user", "content": prompt}],
               "custom_inputs": {"claim_public_id": cid}}
    # Generous timeout: scale-to-zero endpoints can cold-start on first call.
    r = requests.post(f"{host}/serving-endpoints/{ep}/invocations",
                      headers={**hdr, "Content-Type": "application/json"},
                      json=payload, timeout=300)
    r.raise_for_status()
    body = r.json()
    msgs = body.get("messages") or (body.get("output") or {}).get("messages") or []
    text = msgs[-1].get("content", "") if msgs else json.dumps(body)[:2000]
    return text, body

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Populate reasoning for the hero claims

# COMMAND ----------

rows = []
for cid in claim_ids:
    for role, agent_name in AGENTS.items():
        ep = resolved.get(role)
        if not ep:
            print(f"  [skip] {role}: endpoint not found")
            continue
        prompt = PROMPTS[role].format(cid=cid)
        try:
            text, body = invoke(ep, cid, prompt)
            rows.append((str(uuid.uuid4()), cid, agent_name, prompt, text,
                         json.dumps(body.get("custom_outputs", {}), default=str)[:4000]))
            print(f"  [ok] {agent_name} / {cid}: {text[:80]!r}")
        except Exception as e:
            print(f"  [err] {agent_name} / {cid}: {str(e)[:120]}")

if rows:
    # Replace any prior reasoning for these claims so re-runs don't duplicate.
    ids = ",".join(f"'{esc(c)}'" for c in claim_ids)
    spark.sql(f"DELETE FROM {tbl('agent_reasoning_log')} WHERE claim_public_id IN ({ids})")
    df = spark.createDataFrame(
        rows, "reasoning_id string, claim_public_id string, agent_name string, "
              "input string, reasoning_text string, output string")
    from pyspark.sql import functions as F
    df = df.withColumn("created_ts", F.current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(tbl("agent_reasoning_log"))

n = spark.table(tbl("agent_reasoning_log")).count()
print(f"\nagent_reasoning_log rows: {n}")
spark.sql(f"SELECT claim_public_id, agent_name, left(reasoning_text, 90) preview "
          f"FROM {tbl('agent_reasoning_log')} ORDER BY claim_public_id, agent_name").show(truncate=False)
dbutils.notebook.exit(json.dumps({"reasoning_rows": int(n), "agents": list(resolved.keys())}))
