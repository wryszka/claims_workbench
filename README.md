# Claims Intelligence Workbench — Databricks Accelerator

Synthetic **Guidewire ClaimCenter** claims intelligence for **Bricksurance SE**, built as a redeployable Databricks Asset Bundle. Motor Third Party + Home Property, end to end on the Lakehouse.

> **Phases 0–1** of a multi-phase build.
> **Phase 0** — scaffold + a synthetic Guidewire CDA **landing zone** (`landing_*`, ~120k claims), UC-tagged.
> **Phase 1** — a real **bronze DLT pipeline** that reads the landing zone and produces governed `bronze_*` tables with data-quality expectations + quarantine.
> Silver, gold, ML, agents, and the app come in later phases.

## The flow, literally

```
                LANDING ZONE                         BRONZE (DLT, governed)
  Guidewire CDA drop (Phase 0 notebook)      Phase 1 DLT pipeline
    landing_gw_cc_claim    ─────────────►      bronze_gw_cc_claim     ┐
    landing_gw_cc_exposure ─────────────►      bronze_gw_cc_exposure  │
    landing_gw_cc_incident ─────────────►      bronze_gw_cc_incident  │ expectations
    landing_gw_cc_contact  ─────────────►      bronze_gw_cc_contact   │  + typing
    landing_gw_pc_policy   ─────────────►      bronze_gw_pc_policy    │
    landing_fraud_signals  ─────────────►      bronze_fraud_signals_raw
    landing_weather        ─────────────►      bronze_weather_raw     ┘
    ref_handlers, ref_weather_index                   │
                                                      ├─► bronze_quarantine_claims
                                                      └─► bronze_quarantine_fraud_signals
                                   ▼
        Phase 2 features → Phase 3 reserving → ML / agents / app   (future)
```

## Quick Start

```bash
# 1. Point the bundle at your workspace (set host in databricks.yml, or use a profile)
#    and confirm the dev-target catalog (databricks.yml -> targets.dev.variables.catalog).
# 2. Deploy scaffold + DLT pipeline
databricks bundle deploy -t dev

# 3. Phase 0 — generate the landing zone:
#    run notebooks/00_setup_and_data_generation.py (catalog widget blank = workspace current)

# 4. Phase 1 — run the bronze DLT pipeline, then tag its tables:
databricks bundle run claims_workbench_01_bronze_dlt -t dev
#    then run notebooks/01b_tag_bronze.py (applies project/layer/owner tags)
```

## Catalog — portable, with one pinned line for DLT

The top-level `catalog` variable is **empty by default**: the Phase 0 notebook auto-resolves to the workspace's current catalog via `spark.catalog.currentCatalog()` at run time — no config on a fresh dev workspace.

A **DLT pipeline needs an explicit target catalog** (it can't resolve one at runtime like a notebook), so the `dev` target pins `catalog`. Change that one line for another workspace, or override:

```bash
databricks bundle deploy -t dev --var="catalog=my_catalog"
```

The schema is fixed as `claims_workbench`.

## Phase 0 — the landing zone (`landing_*`)

Produced by `notebooks/00_setup_and_data_generation.py` (generation logic in `notebooks/claims_data_gen.py`), simulating a raw Guidewire ClaimCenter CDA drop.

| Table | Layer | Rows | Notes |
|-------|-------|------|-------|
| `landing_gw_cc_claim` | landing | ~120k | Claim header (`cc:NNNNNN`, `BSE-CC-{yyyy}-{seq}`) |
| `landing_gw_cc_exposure` | landing | ~120k | Coverage / reserve / paid amounts |
| `landing_gw_cc_incident` | landing | ~120k | Incident type + templated description text |
| `landing_gw_cc_contact` | landing | ~120k | Claimant / third-party / witness + UK postcode district |
| `landing_gw_pc_policy` | landing | ~55k | PolicyCenter policy (motor / home), only policies referenced by a claim |
| `landing_fraud_signals` | landing | ~120k | Rule-seeded fraud score, prior claims, report lag |
| `landing_weather` | landing | ~30 | Per-district flood / wind / freeze risk |
| `ref_handlers` | ref | ~80 | Claim handlers (grade / team / BU) |
| `ref_weather_index` | ref | ~30 | Materialised weather feed for joins |

All dates are **rolling** relative to `current_date()`, so the demo never goes stale. Schema tagged `project=claims_workbench`, `owner=wryszka`, `demo=bricksurance_se`; tables tagged `layer=landing|ref`.

## Phase 1 — bronze DLT pipeline (`claims_workbench_01_bronze_dlt`)

`notebooks/01_bronze_dlt_pipeline.py` reads the landing zone and produces 7 governed, lightly-typed `bronze_*` tables (`bronze_gw_cc_claim`, `_exposure`, `_incident`, `_contact`, `bronze_gw_pc_policy`, `bronze_fraud_signals_raw`, `bronze_weather_raw`) plus 2 quarantine tables. Streaming reads (`skipChangeCommits`), amounts cast to `decimal(12,2)`, dates/timestamps typed.

**Expectations show all three DLT behaviours:**

| Rule | Type | Behaviour |
|------|------|-----------|
| `valid_policy_number` (`RLIKE '^BSE-'`) | `expect` | track & retain (bad policy numbers flagged, kept) |
| `valid_report_channel` | `expect` | track & retain |
| `valid_loss_cause` | `expect_or_drop` | drop garbage typecodes → `bronze_quarantine_claims` |
| `fraud_score_range` (`0–100`) | `expect_or_drop` | drop out-of-range → `bronze_quarantine_fraud_signals` |

The ~3% malformed records seeded in Phase 0 surface here: bad `policy_number` rows are **tracked** (retained, visible in metrics), while invalid `loss_cause` and out-of-range `fraud_score` rows are **dropped and quarantined** — *no claims data lost, bad records quarantined not silently dropped.* Expectation pass/fail metrics are visible in the DLT pipeline event log / UI.

UC tags (`project/layer=bronze/owner`) are applied by the post-step `notebooks/01b_tag_bronze.py` (DLT can reset tags on a full refresh, so re-run it after one).

## Deliberately-seeded business signals

These are intentional — later phases tell stories with them:

- **Long tail:** 80% of claims report < £5k, tail to £250k.
- **Under-reserving:** home escape-of-water reserves are systematically ~28% light vs. what's needed (the "+28% under-reserving" story in Phase 3).
- **Geo skew:** north-west districts (M, BL, OL, WN) get ~3× escape-of-water frequency.
- **Quarantine bait:** ~3% intentionally malformed rows — bad `policy_number` (tracked), invalid `loss_cause` and out-of-range `fraud_score` (dropped & quarantined) — for the Phase 1 DLT quarantine demo.

## The vivid demo claim — `cc:900001` (SACRED)

One specific claim is hand-seeded with **fixed, reproducible** attributes. It survives every reset and is always findable by ID:

| Attribute | Value |
|-----------|-------|
| `claim_public_id` | `cc:900001` |
| `loss_cause` | `vehcollision` (Motor TP) |
| `postcode_district` | `M1` |
| `total_incurred` | £8,500 |
| `report_channel` | `phone` |
| `fraud_score` | 74 (`fraud_flag = true`) |
| `prior_claims_12m` | 2 |
| `days_since_incident` | 18 |

```sql
-- in the landing zone (Phase 0) and, after Phase 1, in governed bronze:
SELECT * FROM <catalog>.claims_workbench.landing_gw_cc_claim WHERE claim_public_id = 'cc:900001';
SELECT * FROM <catalog>.claims_workbench.bronze_gw_cc_claim  WHERE claim_public_id = 'cc:900001';
```

## Repository Structure

```
claims_workbench/
├── databricks.yml                         # DAB definition (catalog var, dev target, includes resources/)
├── README.md
├── notebooks/
│   ├── 00_setup_and_data_generation.py    # Phase 0 — generate the landing zone (run this)
│   ├── claims_data_gen.py                 # reusable generation module (roll_dates, generate_all)
│   ├── 01_bronze_dlt_pipeline.py          # Phase 1 — bronze DLT pipeline source
│   └── 01b_tag_bronze.py                  # Phase 1 — post-step: tag bronze tables
├── resources/
│   └── bronze_pipeline.yml                # Phase 1 — DLT pipeline resource
├── app/                                   # (future) Databricks App
└── data/seed/                             # (future) static seed assets
```

## Reusable rolling-date helper

`claims_data_gen.roll_dates(df, days_ago_col, out_col, anchor=None)` derives every
date as `anchor - n days` (anchor defaults to `current_date()`). Phase 9 (reset)
re-anchors the entire dataset with one call — no hardcoded years anywhere.

## Visual language (for later app phases)

The future app inherits the house style of the sibling Bricksurance workbenches
(Pricing Workbench, Solvency II QRT): React 19 + Tailwind 4 + Lucide icons + Vite,
slate-900 (`#1e293b`) dark nav, blue accents, `gray-100` background, system font,
and "Bricksurance SE" branding with an amber "About this demo" disclaimer card.

## Disclaimer

This is a synthetic demonstration. All company names, policy data, and financial
figures are entirely fictional. The data simulates a Guidewire ClaimCenter Cloud
Data Access (CDA) landing — there is no real Guidewire integration and no real
customer data is used.
