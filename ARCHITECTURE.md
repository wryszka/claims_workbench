# Architecture & asset map — Claims Intelligence Workbench

How to navigate the workspace when you look under the covers. Everything lives in one catalog/schema (`${catalog}.claims_workbench`), built by numbered notebooks, served by a Databricks App.

## The medallion flow
```
Guidewire CDA + feeds → landing_* → bronze_* (Lakeflow DLT + expectations + quarantine)
   → silver_claims_enriched → gold_* (analytics, disposition, metrics) + feature_* (ML)
   → models (triage, reserve) → agents (Mosaic AI) → Databricks App
```

## Notebooks (run order) — `notebooks/`
| # | Notebook | Builds |
|---|---|---|
| 00 | `00_setup_and_data_generation` (+ `claims_data_gen`) | Synthetic landing data (claims, policies, fraud, weather, telematics); rolls dates to today (seed 42). Sacred heroes cc:900001/2/3. |
| 01 | `01_bronze_dlt_pipeline` | DLT bronze: `bronze_gw_cc_*`, `bronze_gw_pc_policy`, `bronze_fraud_signals_raw`, `bronze_weather_raw`, `bronze_telematics` + expectations + `bronze_quarantine_*`. |
| 01b | `01b_tag_bronze` | UC tags/comments on bronze. |
| 02 | `02_silver_enrichment` | `silver_claims_enriched` (the joined, enriched single source). |
| 03 | `03_gold_analytics` | `gold_reserve_development`, `gold_settlement_performance`, `gold_geo_clustering`, `gold_handler_scorecard`, `gold_handler_decisions`, `claim_image_severity` (shell). |
| 04 | `04_feature_engineering` | `feature_triage`, `feature_reserve`, `ref_feature_encodings`. |
| 05 | `05_ml_models` (+ `06_triage_proba_relog`) | `model_triage_classifier` @champion, `model_reserve_bracket` @champion. |
| 06 | `06_agent_tools` | 9 UC functions `fn_*` (claim_summary, triage_claim, reserve_claim, fraud_signals, policy_history, recovery_signals, decision_reasoning, telematics_signals, image_severity). |
| 06 | `06_agents` | Deploys the 9 Mosaic AI agents (ChatAgent). |
| 07 | `07_governance` | Tags, masking, sensitivity tiers, audit shells. |
| 10 | `10_auto_close` | Batch-scores triage → `gold_claim_disposition` (decision, confidence, fired_rules) + `auto_close_config`, `rule_config`. |
| 11 | `11_agent_reasoning` | `agent_reasoning_log` (regulator-viewable). |
| 12 | `12_pricing_claims_join` | `gold_policy_claims_joined` (the joined Genie space). |
| 13 | `13_cco_metrics` | `gold_cco_metrics_daily` (12-week trend series). |
| 14 | `14_image_severity` | Vision FM → `claim_image_severity` (photo → severity). |
| 15 | `15_doc_ingest` | Auto Loader on a Volume → `bronze_claim_documents`; vision extract → `gold_document_extractions`; + `gold_ingestion_sources`, `gold_ingestion_quality`. |
| 98 | `98_smoke_test` | End-to-end 11-step smoke (one row per step, pass/fail). |
| 09 | `09_reset_cache` | Final reset task: truncate cache, wipe sandbox, re-warm. |
| — | `06c_cache_prewarm`, `08*_test` | **Legacy/standalone helpers — not wired into any job. Ignore.** |

## Jobs (`resources/*.yml`)
- **`claims_workbench_11_stage_a`** — the full build chain (data_gen → bronze → silver → [gold, features] → auto_close → agent_tools → [join, cco_metrics, governance, doc_ingest] → deploy agents → reasoning).
- **`claims_workbench_99_reset_demo`** — re-anchors dates to today, full-refresh bronze, rebuild, auto_close, doc_ingest, cache reset. Triggered by the app's **Reset** button.
- **`claims_workbench_98_smoke_test`** — the 11/11 smoke.

## Models & serving
- `model_triage_classifier` @champion (proba-relogged) · `model_reserve_bracket` @champion.
- Serving endpoints (scale-to-zero, feature-vector contract): `…claims-workbench-triage`, `…claims-workbench-reserve`. **Used live only by Try-a-claim**; the interactive views read the batch-scored `gold_claim_disposition` (see Speed).

## Agents (Mosaic AI, 9) + Genie + dashboard
- Endpoints `agents_<cat>-claims_workbench-agent_{fraud,context,challenge,recovery,audit,reserving,adjuster,coverage,conduct}` (dev uses full names; serverless truncates — **resolve by substring** `agent_frau`/`agent_cont`).
- Supervisor: managed Supervisor pending → falls back to the Context agent (`CLAIMS_EP_SUPERVISOR` blank).
- Genie: book space + `gold_policy_claims_joined` ("Ask Pricing + Claims") space.
- Lakeview dashboard: "Claims Portfolio — Board View" (3 pages) — embedded in Insight, published with `embed_credentials=True`.

## The app (`app/`)
- `app.py` (FastAPI) serves `frontend/dist/index.html` (self-contained vanilla-JS SPA — the working artifact; the React `src/` is the parallel port).
- `server/claims_service.py` — all data logic; `server/routes/claims.py` — `/api/*`; `server/sql.py` — async warehouse executor (INLINE disposition); `utils/agent_cache.py` — cache-first agent wrapper; `utils/config.py` — env-driven config.
- Auth: app **service principal** for SQL/serving; reads via the SQL warehouse.

## Speed notes (why it's fast)
- **Interactive panels read precomputed tables** (`silver` + `gold_claim_disposition` + `claim_image_severity`) in ONE query — no scale-to-zero model calls. Live models only in Try-a-claim.
- Slow REST (`list_pipelines`) is cached per process; ingestion counts are one round-trip.
- SQL warehouse is **serverless, autoscaling 1→4 clusters** — concurrent users parallelise (verified: 8 simultaneous panel loads ~1.3s each).

## Cross-cutting
- Unity Catalog lineage + tags (`project`/`layer`/`owner`); PII/SECRET tiers + masking + CMK note; `agent_reasoning_log` for the audit trail; cache-first for the supervisor.
