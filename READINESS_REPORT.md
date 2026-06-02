# Claims Intelligence Workbench — Readiness Report (Phase 10)
## Bricksurance SE · demo-readiness verification

Generated from the Phase 10 verification pass. **Honest status:** green = verified
here; amber = needs a human/UI step or couldn't be fully verified in this
environment; red = not done / known gap. Anything not provable in this CLI
environment (the real Vite build, the managed Supervisor in Playground) is marked
amber/red with the exact step needed — not marked green.

---

## Part 1 — Automated smoke test (`claims_workbench_98_smoke_test`)

The single full end-to-end test: `notebooks/98_smoke_test.py`, run as the
serverless job `claims_workbench_98_smoke_test`. Asserts at every hop; prints a
one-row-per-step PASS/FAIL table; fails the run loudly if any step is red.

**Result: 11 / 11 PASS** _(run with `run_reset=false` — data already fresh from the
Phase 9 reset; the `run_reset=true` path triggers `claims_workbench_99_reset_demo`
first and is proven separately in Phase 9)._

| # | Step | Status | What it asserts |
|---|------|--------|-----------------|
| 1 | reset + dates | ✅ PASS | `max(report_date)` within 5 days of today (date freshness); optional full reset re-run |
| 2 | landing / bronze | ✅ PASS | 7 landing + 7 bronze tables populated; quarantine non-empty; expectation metrics present in the pipeline event log |
| 3 | silver | ✅ PASS | `silver_claims_enriched` ≈ bronze claim count; 0 duplicate `claim_public_id`; key derived columns 0 null |
| 4 | gold + headlines | ✅ PASS | 5 gold tables present; the 3 headline numbers computed and in range (EoW dev ratio, NW EoW clustering, digital-vs-phone settle days) |
| 5 | features | ✅ PASS | `feature_triage` + `feature_reserve` present, PK unique; encoder (`ref_feature_encodings`) persisted |
| 6 | models + serving | ✅ PASS | both UC models have a `@champion` alias; both serving endpoints READY (resolved by substring — DAB dev-prefixes endpoint names) |
| 7 | UC-function tools | ✅ PASS | all 5 functions return for `cc:900001`; `fn_triage_claim` → `refer_siu` with sensible confidence |
| 8 | sub-agents | ✅ PASS | fraud + context agents return non-empty sensible responses for the vivid claim |
| 9 | cache-first | ✅ PASS | miss→real+save, hit→fast (`t_hit < t_miss`), `use_cache=False`→live bypass |
| 10 | audit write | ✅ PASS | simulated HITL decision lands in `gold_handler_decisions` with all fields, then cleaned up |
| 11 | reachability | ✅ PASS | Lakeview "Claims Portfolio" dashboard resolves; Genie space reachable by id |

> **Headline numbers (real, from the green run):**
> - EoW reserve development ratio **1.252** (~25% under-reserving) — runbook says ~25% ✅
> - NW escape-of-water **503.6 vs 167.6 per-1000** = **3.0×** — runbook says ~3× ✅
> - Digital settles **32.7d vs phone 57.7d** = **43% faster** — runbook says ~43% ✅
> - Vivid `cc:900001`: `fn_triage_claim` → **refer_siu @ 80.4%**, reserve **MEDIUM** ✅
> - Cache-first: miss **15.6s** → hit **1.07s** → live (use_cache=False) **14.8s** ✅
> - Scale: 118,822 claims (silver = bronze, 0 dupes); quarantine 1,179; 1.3M expectation records.

A FAIL on any step raises at the end with the failing step name, so the job goes
red in the run list (fail-loudly), while still printing the full table for triage.

---

## Part 2 — Manual-verification items (the smoke test can't reach these)

| Item | Status | How to verify / step needed |
|------|--------|------------------------------|
| **Managed Claims AI supervisor** | 🟡 AMBER | A serving endpoint `workbench-supervisor` exists and is **READY** on the workspace, but `app/utils/config.py → CLAIMS_EP_SUPERVISOR` is **blank**, so the app's synthesis box currently falls back to the **Context** sub-agent (by design, graceful). To go green: open the Agents UI / Playground, run the vivid-claim prompt from `RUNBOOK_AGENT_SETUP.md` Stage C, confirm it routes to `fn_triage_claim`(refer_siu) + `fn_reserve_claim`(MEDIUM) + Fraud(HIGH) + Context(brief), then set `CLAIMS_EP_SUPERVISOR=workbench-supervisor` (env or config) and redeploy the app. **Not marked green — Playground routing not verified in this CLI pass.** |
| **Real Vite app build** | 🔴 RED | The deployed Databricks App serves the **self-contained `app/frontend/dist/index.html` fallback**, not a fresh React/Vite build. npm has no registry egress in this environment, so the Vite build was **not produced or verified here**. Step needed: on a registry-reachable machine run `npm install && npm run build` in `app/frontend/`, redeploy, and eyeball that it matches the pricing-app framework with the full vivid-claim journey < 3s. Until then the fallback HTML is what the room sees — note its URL. |
| **Cache pre-warmed for the vivid claim** | 🟢 GREEN | The Phase 9 reset's final `cache_reset` task TRUNCATEs then re-warms `cache_agent_responses` for `cc:900001` against the supervisor (or Context fallback) + fraud + context agents. Smoke step 8 confirms the warmed responses are non-empty; step 9 confirms cache-first timing. Verify in-app: `cc:900001` synthesis loads instantly. |
| **USE_CACHE = ON for the walkthrough** | 🟢 GREEN | `USE_CACHE` defaults to `True` in `config.py` (env-overridable). Smoke step 9 also proves the **off** path (`use_cache=False` → live call) works. The app top-bar toggle flips it live for the "is it really running?" beat. |

---

## Part 3 — Portability (Standard 10)

| Check | Status | Evidence |
|-------|--------|----------|
| `bundle validate -t dev` from clean state | 🟢 GREEN | Validates with only the benign `app/frontend/node_modules` sync-exclude warning. |
| **Catalog via variable, no hardcoding** | 🟢 GREEN | `bundle validate --var="catalog=some_other_catalog"` succeeds and the alternate catalog flows into resource definitions, e.g. serving `entity_name = some_other_catalog.claims_workbench.model_triage_classifier`. No real catalog is baked into resource YAML. |
| **Schema fixed** | 🟢 GREEN | `schema=claims_workbench` is a pinned variable used everywhere; tables are numeric/medallion-named within the one schema. |
| Second-workspace deploy | 🟡 AMBER | No second workspace/catalog was available in this pass, so this is a **validate/dry-run** against an alternate catalog variable (above) rather than a live cross-workspace `deploy`. To fully close: `bundle deploy -t dev --var="catalog=<other>"` on a second workspace. |
| **Agent Bricks / Mosaic AI prereqs documented** | 🟢 GREEN | `RUNBOOK_AGENT_SETUP.md` documents: serverless enabled, Unity Catalog, Mosaic AI Model Serving, embedding endpoint `databricks-gte-large-en` (guardrails/rate-limits disabled), non-zero serverless budget, supported region, and the managed-Supervisor UI steps. |
| **Pricing-population dependency + fallback noted** | 🟢 GREEN | Data is self-generated (Phase 0 `claims_data_gen.py`, seed=42) — no external pricing dependency. The self-contained `dist/index.html` is the documented app fallback when a fresh Vite build isn't available. |

Catalog defaults to `lr_serverless_aws_us_catalog` for this workspace; override via
`--var="catalog=..."` (and set the workspace `host` in `databricks.yml`, which ships
as `REPLACE-ME` for portability).

---

## Summary

- **Automated chain: green** — all 11 smoke-test steps pass; the demo's data, models,
  tools, agents, cache, audit write and dashboards all work together end to end.
- **Two items to close before a high-stakes demo:** (1) build + deploy the **real Vite
  app** (red — fallback HTML in use); (2) **verify the managed Supervisor in Playground**
  and wire `CLAIMS_EP_SUPERVISOR` (amber — endpoint exists, routing unverified, Context
  fallback active). Both have exact steps above and matching items in the pre-demo checklist.
- **Portability: green** for the variable-driven catalog and documented prereqs; the only
  amber is the absence of a live second-workspace deploy (dry-run done instead).
