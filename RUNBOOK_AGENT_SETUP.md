# Claims AI — Agent Setup Runbook

Exact, ordered steps to (re)create the Phase 6 AI layer on any workspace. The
UC functions, Genie space, and sub-agents are scriptable; the **managed
Supervisor Agent** is created in the Agents UI (Stage C section below).

> Synthetic demo for **Bricksurance SE**. No real Guidewire integration, no real customer data.

Prereqs (all stages): serverless enabled, Unity Catalog, Mosaic AI Model Serving,
a non-zero serverless budget, supported region. Phases 0–5 deployed (gold tables,
feature tables, and the two model-serving endpoints `*-claims-workbench-triage`
and `*-claims-workbench-reserve` READY).

---

## Stage A — UC function tools

Run the notebook **`notebooks/06_agent_tools.py`** (resolves the serving-endpoint
names at run time, so it is portable across the DAB dev-mode name prefix). It
creates five UC functions in `{catalog}.claims_workbench`:

| Function | Capability (also the routing description) |
|----------|-------------------------------------------|
| `fn_triage_claim` | Decide how to handle a claim: pay_direct / escalate / refer_siu + confidence % + reasons |
| `fn_reserve_claim` | Predict reserve bracket (LOW/MEDIUM/HIGH/LARGE LOSS) + £ range |
| `fn_fraud_signals` | Raw governed fraud signals (no model) |
| `fn_policy_history` | Policy summary + prior-claims count |
| `fn_claim_summary` | Core enriched claim context |

> UC functions do not support UC tags (`ALTER FUNCTION ... SET TAGS` is
> unsupported in Unity Catalog); `layer=agent` is recorded in each function's
> COMMENT instead.

Triage confidence note: the Phase 5 triage endpoint is re-logged by
`notebooks/06_triage_proba_relog.py` to return class **probabilities** (so
`fn_triage_claim` can report a real confidence %); redeploy the triage endpoint
to that version before creating the functions.

---

## Stage A — Genie space "Claims AI — Ask the Book"

Create over the four gold tables (the Genie create API requires the table list
**sorted by identifier**):

```bash
python3 scripts/create_genie_space.py \
  --catalog <catalog> --schema claims_workbench \
  --warehouse-id <sql_warehouse_id> --profile DEFAULT
# prints GENIE_SPACE_ID=<id>
```

**Current dev Genie space ID:** `01f15e4e509f1410b5596f5c90b20ca4`
(workspace `fevm-lr-serverless-aws-us`, over the 4 gold tables).

### Curated questions + instructions (add in the Genie UI)

The create API accepts only `data_sources`; add these in **Genie → the space →
Settings/Instructions** (paste-ready):

**General instructions:**
> Bricksurance SE claims analytics over the gold tables. "Leakage" = paid exceeded
> the opening reserve. "EoW" / "escape of water" = peril_type 'home_escape_water'.
> NW districts start with M, BL, OL or WN. Money is GBP. Reserve brackets are
> LOW / MEDIUM / HIGH / LARGE LOSS.

**Example questions (Sample questions):**
- Which postcode districts have the worst escape-of-water leakage?
- How much faster do digital claims settle vs phone?
- Show handlers with override rates above 40%
- Total open reserve by peril?

**Verified test:** "How much faster do digital claims settle versus phone?" →
Genie answered digital ≈ 34.7 days vs phone ≈ 61.4 days (≈ 26.7 days faster),
querying `gold_settlement_performance`.

---

## Stage B — LLM sub-agents (Fraud + Context)

Run **`notebooks/06_agents.py`** once per agent (`agent` widget = `fraud`, then
`context`). It logs a `ChatAgent` (Claude `databricks-claude-sonnet-4-6` tool-use
loop calling the real UC functions / Genie), registers it in UC, and deploys via
`agents.deploy()` (tracing auto-on). Each agent uses **its own MLflow experiment**.

| Agent | UC model | MLflow experiment | Serving endpoint (auto-named by agents.deploy) |
|-------|----------|-------------------|-----------------------------------------------|
| Fraud | `…claims_workbench.agent_fraud` | `/Users/<you>/claims_workbench_agent_fraud` | `agents_lr_serverless_aws_us_catalog-claims_workbench-agent_frau` |
| Context | `…claims_workbench.agent_context` | `/Users/<you>/claims_workbench_agent_context` | `agents_lr_serverless_aws_us_catalog-claims_workbench-agent_cont` |

- **Fraud** tools: `fn_fraud_signals`, `fn_claim_summary`. Returns a LOW/MEDIUM/HIGH
  risk level + 2-3 sentence narrative citing the specific signals.
- **Context** tools: `fn_claim_summary`, `fn_policy_history`, and the **Genie space**
  (`ask_the_book`). Returns the "before you pick up the phone" brief.

> `agents.deploy()` auto-names the endpoint (`agents_<catalog>-<schema>-<model>`,
> truncated to 63 chars) — it does not accept a custom endpoint name. Use the names
> above (capture from `databricks serving-endpoints list`) when wiring the Supervisor.
> Invoke with the **invocations API** (`POST /serving-endpoints/<name>/invocations`)
> passing `{"messages":[…], "custom_inputs":{"claim_public_id":"…"}}` — the CLI
> `serving-endpoints query` strips `custom_inputs`.

**Verified (vivid `cc:900001`):** Fraud → **HIGH** (fraud 74/100, 2 prior claims,
reported 18 days after). Context → coherent brief (motor, 2.7y tenure, £227 premium,
£5,037 sum insured; £8,500 collision under investigation; flags the over-sum-insured
amount + 2 prior claims). Both called the real tools (trace confirms).

---

## Stage C — caching layer

- Table `{catalog}.claims_workbench.cache_agent_responses` (cache_key, agent_name,
  input_json, response_json, created_ts, mode) — created by `notebooks/06c_cache_prewarm.py`.
- Wrapper `app/utils/agent_cache.py` → `get_agent_response(agent_name, input_dict, use_cache)`:
  cache-first, **no TTL**, keyed by `sha256(agent_name + canonical_json(input))`, generic
  over any endpoint (sub-agents AND the supervisor). `USE_CACHE` is the single switch in
  `app/utils/config.py` (env-overridable). Verified: miss→real+save, hit→cache (much faster),
  `use_cache=False`→bypass(real).

## Stage C — Managed Supervisor Agent "Claims AI" (created in the Agents UI)

The Supervisor is a **managed** agent — create it once in the Agents UI on the workspace.
These steps recreate it on any workspace.

**1. Prereqs checklist**
- Serverless enabled; Unity Catalog; Mosaic AI Model Serving; non-zero serverless budget.
- Embedding endpoint `databricks-gte-large-en` available, with guardrails/rate-limits disabled.
- Supported region. Phases 0–6 (Stages A & B) deployed: the 5 UC functions, the Genie
  space, and the two sub-agent endpoints are live.

**2. Create the agent**
- Databricks → **Agents → Create agent → Supervisor agent**.
- Name `claims_workbench_claims_ai`, display name **"Claims AI"**.

**3. Add subagents / tools** (paste these descriptions verbatim — the supervisor routes on them):

| Type | Add | Paste-ready description |
|------|-----|-------------------------|
| UC function | `{catalog}.claims_workbench.fn_triage_claim` | *Decide how to handle a new claim: pay directly, escalate, or refer to SIU. Returns the recommended decision, a confidence percentage and the top reasons. Input: a claim_public_id.* |
| UC function | `{catalog}.claims_workbench.fn_reserve_claim` | *Predict the financial reserve bracket for a claim (LOW, MEDIUM, HIGH or LARGE LOSS) and an indicative £ range. Input: a claim_public_id.* |
| Agent | `agents_…claims_workbench-agent_frau` (Fraud) | *Assess fraud risk for a claim and explain why — returns a LOW/MEDIUM/HIGH risk level with a plain-English narrative citing the specific signals (fraud score, prior claims, reporting lag). Input: a claim_public_id.* |
| Agent | `agents_…claims_workbench-agent_cont` (Context) | *Produce a handler briefing on the policyholder and claim history — the "before you pick up the phone" brief. Input: a claim_public_id.* |
| Genie space | `01f15e4e509f1410b5596f5c90b20ca4` (Ask the Book) | *Answer portfolio / book-level analytics questions about the claims book — reserve development, settlement speed by channel, geographic risk clustering and handler performance.* |

**4. Grant access** — the supervisor cannot reach an unshared subagent. For EACH of the
two sub-agent serving endpoints and the Genie space, grant the supervisor's service
principal **CAN QUERY** (endpoints) / **CAN RUN** (Genie). UC functions: grant **EXECUTE**.

**5. Permissions + test** — give yourself/your team CAN MANAGE on the supervisor. Open
**Playground** and test with the vivid claim:
> "I've got a new claim, cc:900001. How should I handle it — give me the triage decision,
> the reserve bracket, the fraud risk, and a briefing before I call the policyholder."
Expect it to route to `fn_triage_claim` (refer_siu), `fn_reserve_claim` (MEDIUM), the Fraud
agent (HIGH) and the Context agent (brief).

**6. Record the endpoint** — copy the deployed supervisor **endpoint name** into
`app/utils/config.py` → `CLAIMS_EP_SUPERVISOR` (or set env `CLAIMS_EP_SUPERVISOR`), and here:

**Supervisor endpoint name:** `__________________________` *(fill after creation)*

## Stage C — cache pre-warm (C3)

After the Supervisor exists, run `notebooks/06c_cache_prewarm.py` with the
`supervisor_endpoint` widget set to its endpoint name. It warms `cache_agent_responses`
for the vivid claim `cc:900001` against the two sub-agents **and** the supervisor (so the
first demo call returns instantly from cache). Sub-agents are pre-warmed already; re-run
with the supervisor name to add it.
