# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 06 · LLM sub-agents (Fraud + Context)
# MAGIC
# MAGIC **Phase 6, Stage B.** Two custom **ChatAgent** agents with a Claude tool-use
# MAGIC loop, deployed via `agents.deploy()` as Model Serving endpoints, each in its
# MAGIC own MLflow experiment (tracing auto-on for ChatAgent). Tools call the **real**
# MAGIC Phase 6 UC functions (and, for Context, the real Genie space). LLM =
# MAGIC `databricks-claude-sonnet-4-6`.
# MAGIC
# MAGIC Run once per agent: set `agent` = `fraud` or `context`.

# COMMAND ----------

dbutils.widgets.text("agent", "fraud", "agent: fraud|context|challenge|recovery|audit|reserving|adjuster|coverage|conduct")
dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema")
dbutils.widgets.text("fm_endpoint", "databricks-claude-sonnet-4-6", "Foundation model endpoint")
dbutils.widgets.text("warehouse_id", "ab79eced8207d29b", "SQL warehouse for tool execution")
dbutils.widgets.text("genie_space_id", "01f15e4e509f1410b5596f5c90b20ca4", "Genie space id (context agent)")

# COMMAND ----------

# MAGIC %pip install -U mlflow databricks-agents databricks-sdk requests --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json, uuid, os
import mlflow
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentMessage, ChatAgentResponse

mlflow.set_registry_uri("databricks-uc")

agent          = dbutils.widgets.get("agent").strip()
catalog        = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema         = dbutils.widgets.get("schema").strip() or "claims_workbench"
fm_endpoint    = dbutils.widgets.get("fm_endpoint").strip()
warehouse_id   = dbutils.widgets.get("warehouse_id").strip()
genie_space_id = dbutils.widgets.get("genie_space_id").strip()
fqn            = f"{catalog}.{schema}"
user           = spark.sql("select current_user()").collect()[0][0]

FRAUD_SYSTEM = """You are a fraud-risk assistant for a Bricksurance SE claims handler.
For the claim under review, FIRST call get_fraud_signals and get_claim_summary, then judge.
Output, in plain English (no jargon):
  Risk level: one of LOW / MEDIUM / HIGH (on its own line)
  Then 2-3 sentences explaining WHY, citing the specific signals that drove it
  (fraud score out of 100, number of prior claims in 12 months, how many days after
  the incident it was reported). Be concrete and cite the numbers.
Guidance: a fraud score over 70 is a strong HIGH signal; 2+ prior claims plus a late
report (over 14 days) is also elevated. Never invent signals the tools did not return."""

# Claim 360 / Dossier — the elevated Context agent: "everything in one place".
CONTEXT_SYSTEM = """You are the Claim 360 / Dossier assistant for Bricksurance SE. Assemble
EVERYTHING about a claim into one narrative brief a handler can read in 30 seconds. FIRST call
get_claim_summary, get_policy_history, get_fraud_signals and get_recovery_signals for the claim
under review. You may call ask_the_book for portfolio context. Then write a structured dossier:
  - Policyholder & policy: product, sum insured, tenure, annual premium
  - The claim: peril, amount (GBP), channel, status, incident description
  - History & risk: prior claims, fraud signals, reporting lag, weather/enrichment context
  - Recovery: whether money is recoverable from a third party
Keep it tight and skimmable, plain English, money in GBP with commas. Do not give a fraud
verdict or a pay decision — you assemble context; the model and workflow decide."""

CHALLENGE_SYSTEM = """You are the Challenge / Second-Opinion agent for a Bricksurance SE handler.
Your job is to argue the OPPOSITE of the current disposition, so the handler hears the other side
before acting. FIRST call get_decision_reasoning (it returns the disposition, the model decision
and confidence, and which rules passed/failed), then call get_fraud_signals and get_claim_summary
as needed.
  - If the claim was AUTO-CLOSED / pay_direct: make the case for CAUTION — what could have been
    missed, what would justify a second look.
  - If the claim was ESCALATED: make the case for RELEASING it — why it might be safe to pay.
Open with one line: "Challenge: <the opposite stance>." Then 2-3 sentences citing specific numbers.
Be fair, not contrarian for its own sake. You do NOT decide and you have NO pay authority."""

RECOVERY_SYSTEM = """You are the Recovery / Subrogation agent for Bricksurance SE. FIRST call
get_recovery_signals and get_claim_summary for the claim under review. Then state, in plain English:
  Recovery potential: one of NONE / POSSIBLE / LIKELY (on its own line)
  Then 1-2 sentences explaining why, citing fault (who was at fault), third-party involvement,
  the peril, and the recoverable amount in GBP with commas if any.
Guidance: a motor third-party loss where OUR policyholder is NOT at fault is recoverable from the
third party's insurer. Home perils and at-fault motor losses are generally not recoverable."""

AUDIT_SYSTEM = """You are the Audit / Reasoning agent for Bricksurance SE — you write the
regulator-readable explanation of how a claim's decision was reached. FIRST call
get_decision_reasoning for the claim under review (it returns the disposition, which auto-close
rules passed/failed, the contributing values and the model confidence), then get_claim_summary.
Write a clear, factual explanation a compliance officer or regulator could read:
  - The decision (auto-closed & paid, or escalated to a handler) and who/what made it
  - The rules evaluated and their outcomes, with the actual values
  - That a model + deterministic workflow decided — no agent had pay authority
Plain English, money in GBP with commas. Do not speculate beyond the recorded reasoning."""

# ---- Phase 11 / CCO uplift: senior-expert "second set of eyes" personas. Each one
# PROPOSES for a human to sign off; none has decision or pay authority. ----
RESERVING_SYSTEM = """You are a Senior Reserving Actuary at Bricksurance SE giving a second
opinion on a single claim's reserve. FIRST call get_claim_summary and get_policy_history.
Then, in plain English for a handler:
  Reserve view: ADEQUATE / LIGHT / HEAVY (on its own line)
  Then 2-3 evidence-based sentences. Cite the reported amount (GBP), the peril and the policy
  (sum insured, product). Domain knowledge to apply: home escape-of-water is systematically
  under-reserved at first notification (it develops ~25% above the opening estimate), so be
  sceptical of light EoW reserves; motor third-party bodily-injury can deteriorate too. If you
  think the reserve is light, PROPOSE an indicative overlay (a GBP uplift or %) for the human
  actuary to sign off — never book it yourself."""

ADJUSTER_SYSTEM = """You are a Senior Loss Adjuster at Bricksurance SE giving an experienced
second opinion on a claim. FIRST call get_claim_summary, get_policy_history and
get_recovery_signals (and get_fraud_signals if useful). Then, plain English for a handler:
  - Your read on the claim and whether the quantum looks right for this peril and amount
  - 2-3 specific things to verify or inspect before settling (e.g. proof of loss, photos,
    third-party details, engineer/contractor report)
  - Any handling red flags
Be concrete and practical, money in GBP with commas. You advise; the handler decides."""

COVERAGE_SYSTEM = """You are Coverage Counsel at Bricksurance SE. Your job is the coverage
question: does the policy actually respond to this loss? FIRST call get_policy_history and
get_claim_summary. Then, plain English:
  Coverage view: LIKELY COVERED / QUERY / LIKELY EXCLUDED (on its own line)
  Then 2-3 sentences on why — product vs peril fit, sum insured vs the reported amount,
  policy tenure, and the conditions/exclusions you would check (e.g. wear-and-tear, gradual
  damage, maintenance, unoccupancy). Flag if the reported amount approaches the sum insured.
You give a coverage opinion for a human to confirm; you do not decline or pay claims."""

CONDUCT_SYSTEM = """You are a Consumer-Duty / Fair-Outcomes Reviewer at Bricksurance SE
(UK FCA Consumer Duty). FIRST call get_claim_summary and get_decision_reasoning (and
get_fraud_signals if useful). Then, plain English:
  Fair-outcome view: FAIR / REVIEW (on its own line)
  Then 2-3 sentences checking: was the customer treated fairly and consistently with similar
  claims; are there vulnerability signals to consider (e.g. distress perils, repeated contact);
  is the decision explainable and proportionate; would it withstand Ombudsman scrutiny.
Flag anything to review for fair value or vulnerable-customer handling. You advise on conduct;
you do not decide the claim."""

AGENTS = {
    "fraud":     {"uc_model": "agent_fraud", "experiment": "claims_workbench_agent_fraud",
                  "system": FRAUD_SYSTEM, "tools": ["get_fraud_signals", "get_claim_summary"], "genie": False},
    "context":   {"uc_model": "agent_context", "experiment": "claims_workbench_agent_context",
                  "system": CONTEXT_SYSTEM,
                  "tools": ["get_claim_summary", "get_policy_history", "get_fraud_signals",
                            "get_recovery_signals", "ask_the_book"], "genie": True},
    "challenge": {"uc_model": "agent_challenge", "experiment": "claims_workbench_agent_challenge",
                  "system": CHALLENGE_SYSTEM,
                  "tools": ["get_decision_reasoning", "get_fraud_signals", "get_claim_summary"],
                  "genie": False},
    "recovery":  {"uc_model": "agent_recovery", "experiment": "claims_workbench_agent_recovery",
                  "system": RECOVERY_SYSTEM, "tools": ["get_recovery_signals", "get_claim_summary"], "genie": False},
    "audit":     {"uc_model": "agent_audit", "experiment": "claims_workbench_agent_audit",
                  "system": AUDIT_SYSTEM, "tools": ["get_decision_reasoning", "get_claim_summary"], "genie": False},
    "reserving": {"uc_model": "agent_reserving", "experiment": "claims_workbench_agent_reserving",
                  "system": RESERVING_SYSTEM,
                  "tools": ["get_claim_summary", "get_policy_history"], "genie": False},
    "adjuster":  {"uc_model": "agent_adjuster", "experiment": "claims_workbench_agent_adjuster",
                  "system": ADJUSTER_SYSTEM,
                  "tools": ["get_claim_summary", "get_policy_history", "get_recovery_signals", "get_fraud_signals"], "genie": False},
    "coverage":  {"uc_model": "agent_coverage", "experiment": "claims_workbench_agent_coverage",
                  "system": COVERAGE_SYSTEM,
                  "tools": ["get_policy_history", "get_claim_summary"], "genie": False},
    "conduct":   {"uc_model": "agent_conduct", "experiment": "claims_workbench_agent_conduct",
                  "system": CONDUCT_SYSTEM,
                  "tools": ["get_claim_summary", "get_decision_reasoning", "get_fraud_signals"], "genie": False},
}
cfg = AGENTS[agent]
agent_uc_name = f"{fqn}.{cfg['uc_model']}"
mlflow.set_experiment(f"/Users/{user}/{cfg['experiment']}")
print(f"agent={agent} model={agent_uc_name} fm={fm_endpoint}")

# COMMAND ----------

TOOL_SCHEMAS = {
    "get_fraud_signals": {"description": "Return raw fraud signals for a claim (fraud score 0-100, fraud flag, prior claims in 12 months, days since incident, reporting lag).",
        "input_schema": {"type": "object", "properties": {"claim_public_id": {"type": "string"}}, "required": ["claim_public_id"]}},
    "get_claim_summary": {"description": "Return the core claim summary (peril, total incurred GBP, report channel, postcode district, incident description, status).",
        "input_schema": {"type": "object", "properties": {"claim_public_id": {"type": "string"}}, "required": ["claim_public_id"]}},
    "get_policy_history": {"description": "Return the policy summary behind a claim (product, sum insured, tenure years, annual premium, prior claims in 12 months).",
        "input_schema": {"type": "object", "properties": {"claim_public_id": {"type": "string"}}, "required": ["claim_public_id"]}},
    "ask_the_book": {"description": "Ask a portfolio/book-level analytics question in natural language over the gold tables.",
        "input_schema": {"type": "object", "properties": {"question": {"type": "string"}}, "required": ["question"]}},
    "get_recovery_signals": {"description": "Return recovery/subrogation signals for a claim (recovery flag, third-party at fault, recoverable GBP amount, at-fault, third-party involved, peril).",
        "input_schema": {"type": "object", "properties": {"claim_public_id": {"type": "string"}}, "required": ["claim_public_id"]}},
    "get_decision_reasoning": {"description": "Return the workflow disposition (auto_closed/escalated) for a claim with the full reasoning: which auto-close rules passed/failed, the values, and the model confidence.",
        "input_schema": {"type": "object", "properties": {"claim_public_id": {"type": "string"}}, "required": ["claim_public_id"]}},
    "get_triage": {"description": "Score the triage model for a claim: recommended decision (pay_direct/escalate/refer_siu), confidence %, and top reasons.",
        "input_schema": {"type": "object", "properties": {"claim_public_id": {"type": "string"}}, "required": ["claim_public_id"]}},
    "get_reserve": {"description": "Predict the reserve bracket (LOW/MEDIUM/HIGH/LARGE LOSS) and indicative GBP range for a claim.",
        "input_schema": {"type": "object", "properties": {"claim_public_id": {"type": "string"}}, "required": ["claim_public_id"]}},
}


def _run_sql(sql):
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState
    w = WorkspaceClient()
    wid = os.environ.get("AGENT_WAREHOUSE_ID", "ab79eced8207d29b")
    r = w.statement_execution.execute_statement(statement=sql, warehouse_id=wid, wait_timeout="50s")
    if r.status and r.status.state == StatementState.FAILED:
        raise RuntimeError(r.status.error.message if r.status.error else "SQL failed")
    if not (r.manifest and r.manifest.schema and r.manifest.schema.columns):
        return []
    cols = [c.name for c in r.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in (r.result.data_array or [])] if r.result else []


def _genie_ask(space_id, question):
    from databricks.sdk import WorkspaceClient
    if not space_id or not question:
        return {"error": "no space or question"}
    try:
        w = WorkspaceClient()
        m = w.genie.start_conversation_and_wait(space_id=space_id, content=question)
        out = {"answer": None, "query": None}
        for att in (m.attachments or []):
            if att.text and att.text.content:
                out["answer"] = att.text.content[:1500]
            if att.query and att.query.query:
                out["query"] = att.query.query[:600]
        return out
    except Exception as e:
        return {"error": f"genie unavailable: {e}"}


def _call_fm(endpoint, messages, tools):
    import requests
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    host = w.config.host.rstrip("/")
    hdr = w.config._header_factory()
    r = requests.post(f"{host}/serving-endpoints/{endpoint}/invocations",
                      headers={**hdr, "Content-Type": "application/json"},
                      json={"messages": messages, "tools": tools, "tool_choice": "auto",
                            "max_tokens": 1024, "temperature": 0.1}, timeout=120)
    r.raise_for_status()
    return r.json()


class ClaimsSubAgent(ChatAgent):
    def __init__(self, catalog, schema, fm_endpoint, system, tool_names, genie_space_id):
        self.catalog = catalog; self.schema = schema; self.fm_endpoint = fm_endpoint
        self.system = system; self.tool_names = tool_names; self.genie_space_id = genie_space_id

    def _fn(self, fn, cid):
        rows = _run_sql(f"SELECT to_json(`{self.catalog}`.`{self.schema}`.{fn}('{cid}')) AS r")
        return json.loads(rows[0]["r"]) if rows and rows[0].get("r") else {"error": "no row"}

    def _tool(self, name, args):
        cid = (args or {}).get("claim_public_id", "")
        if name == "get_fraud_signals":      return self._fn("fn_fraud_signals", cid)
        if name == "get_claim_summary":      return self._fn("fn_claim_summary", cid)
        if name == "get_policy_history":     return self._fn("fn_policy_history", cid)
        if name == "get_recovery_signals":   return self._fn("fn_recovery_signals", cid)
        if name == "get_decision_reasoning": return self._fn("fn_decision_reasoning", cid)
        if name == "get_triage":             return self._fn("fn_triage_claim", cid)
        if name == "get_reserve":            return self._fn("fn_reserve_claim", cid)
        if name == "ask_the_book":           return _genie_ask(self.genie_space_id, (args or {}).get("question", ""))
        return {"error": f"unknown tool {name}"}

    def predict(self, messages, context=None, custom_inputs=None) -> ChatAgentResponse:
        cid = (custom_inputs or {}).get("claim_public_id")
        system = self.system + (f"\n\nThe claim under review is claim_public_id='{cid}'. "
                                f"Call your tools with this id." if cid else "")
        full = [{"role": "system", "content": system}]
        for m in messages:
            full.append({"role": m.role, "content": m.content or ""})
        tools = [{"type": "function", "function": {"name": n, "description": TOOL_SCHEMAS[n]["description"],
                                                   "parameters": TOOL_SCHEMAS[n]["input_schema"]}} for n in self.tool_names]
        trace, final = [], ""
        for _hop in range(6):
            resp = _call_fm(self.fm_endpoint, full, tools)
            choices = resp.get("choices") or []
            if not choices:
                break
            msg = choices[0].get("message") or {}
            tcs = msg.get("tool_calls") or []
            if tcs:
                full.append({"role": "assistant", "content": msg.get("content") or "", "tool_calls": tcs})
                for tc in tcs:
                    fnm = (tc.get("function") or {}).get("name")
                    raw = (tc.get("function") or {}).get("arguments") or "{}"
                    try:
                        a = json.loads(raw) if isinstance(raw, str) else (raw or {})
                    except Exception:
                        a = {}
                    res = self._tool(fnm, a)
                    trace.append({"tool": fnm, "args": a})
                    full.append({"role": "tool", "tool_call_id": tc.get("id") or fnm,
                                 "content": json.dumps(res, default=str)[:8000]})
                continue
            final = msg.get("content") or ""
            break
        return ChatAgentResponse(
            messages=[ChatAgentMessage(role="assistant", content=final, id=str(uuid.uuid4()))],
            custom_outputs={"trace": trace, "model": self.fm_endpoint})

# COMMAND ----------

# Quick local smoke test before logging
_local = ClaimsSubAgent(catalog, schema, fm_endpoint, cfg["system"], cfg["tools"], genie_space_id)
_resp = _local.predict([ChatAgentMessage(role="user", content="Assess this claim.", id="u1")],
                       custom_inputs={"claim_public_id": "cc:900001"})
print("LOCAL TEST:", _resp.messages[0].content[:500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log + register + deploy

# COMMAND ----------

from mlflow.models.resources import (DatabricksServingEndpoint, DatabricksFunction,
                                      DatabricksTable, DatabricksGenieSpace, DatabricksSQLWarehouse)

fn_for_tool = {"get_fraud_signals": "fn_fraud_signals", "get_claim_summary": "fn_claim_summary",
               "get_policy_history": "fn_policy_history", "get_recovery_signals": "fn_recovery_signals",
               "get_decision_reasoning": "fn_decision_reasoning", "get_triage": "fn_triage_claim",
               "get_reserve": "fn_reserve_claim"}
resources = [DatabricksServingEndpoint(endpoint_name=fm_endpoint),
             DatabricksTable(table_name=f"{fqn}.silver_claims_enriched")]
# fn_triage_claim / fn_decision_reasoning read other endpoints/tables; declaring the
# function is enough for serving auth (the function's own grants cover its body).
for t in cfg["tools"]:
    if t in fn_for_tool:
        resources.append(DatabricksFunction(function_name=f"{fqn}.{fn_for_tool[t]}"))
if "get_decision_reasoning" in cfg["tools"]:
    resources.append(DatabricksTable(table_name=f"{fqn}.gold_claim_disposition"))
if cfg["genie"]:
    resources.append(DatabricksGenieSpace(genie_space_id=genie_space_id))
    resources.append(DatabricksSQLWarehouse(warehouse_id=warehouse_id))
    for gt in ["gold_reserve_development", "gold_settlement_performance", "gold_geo_clustering", "gold_handler_scorecard"]:
        resources.append(DatabricksTable(table_name=f"{fqn}.{gt}"))

input_example = {"messages": [{"role": "user", "content": "Assess this claim."}],
                 "custom_inputs": {"claim_public_id": "cc:900001"}}

with mlflow.start_run(run_name=f"{agent}_agent"):
    mi = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=ClaimsSubAgent(catalog, schema, fm_endpoint, cfg["system"], cfg["tools"], genie_space_id),
        resources=resources, input_example=input_example,
        registered_model_name=agent_uc_name,
        pip_requirements=["mlflow", "databricks-sdk>=0.30.0", "requests"])
    print("logged:", mi.model_uri)

from mlflow.tracking import MlflowClient
mc = MlflowClient(registry_uri="databricks-uc")
version = max(int(v.version) for v in mc.search_model_versions(f"name='{agent_uc_name}'"))
print(f"registered {agent_uc_name} v{version}")

# COMMAND ----------

from databricks import agents
dep = agents.deploy(model_name=agent_uc_name, model_version=version, scale_to_zero=True,
                    environment_vars={"AGENT_WAREHOUSE_ID": warehouse_id},
                    tags={"project": "claims_workbench", "layer": "agent", "agent": agent})
ep_name = getattr(dep, "endpoint_name", None) or getattr(dep, "endpoint", None)
print("agents.deploy ->", ep_name)
dbutils.notebook.exit(json.dumps({"agent": agent, "uc_model": agent_uc_name,
                                  "version": version, "endpoint_name": str(ep_name)}))
