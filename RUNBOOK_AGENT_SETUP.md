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

## Stage C — Managed Supervisor Agent "Claims AI"

_(to be completed in Stage C — created in the Agents UI; paste-ready subagent/tool
descriptions, access grants, Playground test prompt, and resulting endpoint name.)_
