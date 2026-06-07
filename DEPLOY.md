# Deploy to a new workspace — runbook

The demo is portable: one catalog/schema, env‑driven app config, ids resolved by name/substring where possible. The order below matters because models must exist before serving, agents before the app grants. Budget ~45–60 min end to end.

## 0 · Prereqs on the target workspace
- Unity Catalog; a **catalog you can write to** + a schema (default `claims_workbench`).
- A **serverless SQL warehouse** (instant‑on; autoscale max ≥2 for concurrency).
- **Mosaic AI** Model Serving + Agent Framework; **Foundation Model API** (vision: `databricks-claude-sonnet-4-6`; embeddings for any vector use); **serverless** enabled with a non‑zero budget; supported region.
- Databricks CLI profile for the workspace.

## 1 · Point the bundle at the workspace — `databricks.yml`
Set the target's `workspace.host`, `profile`, and the vars: `catalog`, `schema`, `warehouse_id`. (The dev target is the template.) Then **clear any stale bundle state** if this repo was deployed elsewhere:
```
rm -rf .databricks ; databricks bundle validate -t <target>
```

## 2 · Two‑pass deploy (serving + app deferred)
Models and agents don't exist yet, so deploy the pipeline/jobs/notebooks first; the serving‑endpoint + app resources come after training.
```
databricks bundle deploy -t <target> --var triage_model_version=1 --var reserve_model_version=1
```
(If the serving/app resources fail on first deploy because models/agents are absent, that's expected — they succeed in step 6/8.)

## 3 · Build the data + models
Run the build chain (it does data_gen → bronze DLT → silver → gold → features → auto_close → agent_tools → join/metrics/governance/doc_ingest → deploy agents → reasoning):
```
databricks bundle run claims_workbench_11_stage_a -t <target>
```
Note the trained model versions; re‑deploy serving with the right `--var *_model_version` if not 1.

## 4 · Serving endpoints
`bundle deploy` (re‑run) creates the two model‑serving endpoints from `@champion`. Confirm both are READY.

## 5 · Genie spaces (the Context agent references the book space)
```
python scripts/create_genie_space.py --catalog <catalog> --warehouse-id <wh> --profile <profile>            # book space
python scripts/create_genie_space.py --space joined --catalog <catalog> --warehouse-id <wh> --profile <profile>  # pricing+claims
```
Capture the two space ids.

## 6 · Agents
`06_agents` (run by stage_a, or re‑run per agent) deploys the 9 agents. **Gotcha:** `agents.deploy` names endpoints with the **full** model name on some workspaces and a **truncated** one on others — the app resolves by substring (`agent_frau`/`agent_cont`/etc.), so no manual rename needed.

## 7 · Dashboard
`bundle deploy` creates the "Claims Portfolio — Board View" dashboard. **Publish it with embedded credentials** so the Insight embed renders for any viewer:
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(profile="<profile>")
# resolve id by name, then:
w.lakeview.publish(dashboard_id="<id>", warehouse_id="<wh>", embed_credentials=True)
```

## 8 · App config + deploy
Edit `app/app.yaml` env for the target:
- `CATALOG_NAME`, `SCHEMA_NAME`, `WAREHOUSE_ID` (the serverless SQL warehouse).
- `CLAIMS_EP_FRAUD`, `CLAIMS_EP_CONTEXT` (the resolved agent endpoint names; or leave and rely on roster substring resolution).
- `GENIE_SPACE_ID`, `GENIE_JOINED_SPACE_ID`, `DASHBOARD_ID` (from steps 5/7).
- `USE_CACHE=true`, `RESET_JOB_NAME=claims_workbench_99_reset_demo`.
Then deploy + push the app:
```
databricks bundle deploy -t <target>
databricks apps deploy claims-workbench --source-code-path "<workspace>/.bundle/claims_workbench/<target>/files/app" --profile <profile>
```

## 9 · Grant the app service principal
The app runs as its SP — grant it (find the SP id from the app):
- Schema: `USE SCHEMA`, `SELECT`, `EXECUTE`, `MODIFY` on `<catalog>.claims_workbench`.
- Warehouse: `CAN_USE`.
- Serving endpoints: `CAN_QUERY` on the **2 models + 9 agents**.
- Reset job: `CAN_MANAGE_RUN` (so the Reset button works).

## 10 · Warm + verify
```
# Reset re-anchors dates to today + warms the cache (the cache layer needs the dev warehouse,
# which 09_reset_cache resolves; the reset job passes warehouse_id).
databricks bundle run claims_workbench_99_reset_demo -t <target>
databricks bundle run claims_workbench_98_smoke_test -t <target>   # expect 11/11
```
Open the app → Home loads, the three heroes open instantly, Ingestion shows the source map, Insight embeds the dashboard.

## Known gotchas (all handled, listed for awareness)
- **Agent endpoint names differ per workspace** → resolved by substring; don't hardcode.
- **Cache layer warehouse** → `config.WAREHOUSE_ID` defaults to the original (serverless) workspace; the app gets it from `app.yaml`, and `09_reset_cache` resolves/takes it as a param. On a fresh workspace set `WAREHOUSE_ID` in app.yaml + pass `${var.warehouse_id}` to the reset job (already wired).
- **`config.py` defaults** point at the original workspace (catalog, warehouse, agent/genie ids) — always overridden by `app.yaml` env; only matters if you run the app with no env.
- **Recreating UC functions (`CREATE OR REPLACE`) revokes EXECUTE grants** to agent serving SPs deployed earlier → re‑deploy those agents or re‑grant.
- **Lakeview dashboard widgets need `pageType: PAGE_TYPE_CANVAS`** and numeric columns need a `numberFormat` (already set) — else they render "no fields selected".
- `06c_cache_prewarm` and `08*` notebooks are legacy/standalone and default to the original workspace — they're not in any job; ignore or delete.
- **prod = the serverless workspace** (`fevm-lr-serverless-aws-us`); **dev = `fevm-lr-dev-aws-us`**. Keep them separate.
