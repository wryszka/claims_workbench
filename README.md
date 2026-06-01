# Claims Intelligence Workbench — Databricks Accelerator

Synthetic **Guidewire ClaimCenter** claims intelligence for **Bricksurance SE**, built as a redeployable Databricks Asset Bundle. Motor Third Party + Home Property, end to end on the Lakehouse.

> **Phase 0** of a multi-phase build. This phase scaffolds the bundle, generates a synthetic Guidewire CDA landing (~120k claims), and tags everything in Unity Catalog. DLT, ML models, agents, and the app come in later phases.

## The flow, literally

```
Guidewire ClaimCenter (CDA)        Enrichment feeds          Reference
  bronze_gw_cc_claim                bronze_fraud_signals_raw   ref_handlers
  bronze_gw_cc_exposure             bronze_weather_raw         ref_weather_index
  bronze_gw_cc_incident                   │                         │
  bronze_gw_cc_contact                    │                         │
  bronze_gw_pc_policy                     │                         │
        └──────────────┬─────────────────┴─────────────────────────┘
                       ▼
            <catalog>.claims_workbench   (UC-tagged Delta tables)
                       │
                       ▼
        Phase 1 DLT → Phase 2 features → Phase 3 reserving →
        Phase ... ML / agents / app   (future)
```

## Quick Start

One-command deploy to your dev workspace:

```bash
# 1. Point the bundle at your workspace (set host in databricks.yml, or use a profile)
# 2. Deploy
databricks bundle deploy -t dev

# 3. Run the setup notebook in the workspace:
#    notebooks/00_setup_and_data_generation.py
#    Leave the `catalog` widget blank to use the workspace's current catalog.
```

The setup notebook installs `dbldatagen`, creates `<catalog>.claims_workbench`, generates and tags all tables, and runs a targeted check at the end.

## Catalog — it just works, and it's trivially changeable

The DAB `catalog` variable is **empty by default**. When empty, the setup notebook resolves to the workspace's current catalog via `spark.catalog.currentCatalog()` at run time — no config needed on a fresh dev workspace.

To pin a specific catalog, override in one line:

```bash
databricks bundle deploy -t dev --var="catalog=my_catalog"
```

or set the `catalog` widget when running the notebook. The schema is fixed as `claims_workbench`.

## What lands in Unity Catalog

| Table | Layer | Rows | Notes |
|-------|-------|------|-------|
| `bronze_gw_cc_claim` | bronze | ~120k | Guidewire CDA claim header (`cc:NNNNNN`, `BSE-CC-{yyyy}-{seq}`) |
| `bronze_gw_cc_exposure` | bronze | ~120k | Coverage / reserve / paid amounts |
| `bronze_gw_cc_incident` | bronze | ~120k | Incident type + templated description text |
| `bronze_gw_cc_contact` | bronze | ~120k | Claimant / third-party / witness + UK postcode district |
| `bronze_gw_pc_policy` | bronze | ~70k | PolicyCenter policy (motor / home) |
| `bronze_fraud_signals_raw` | bronze | ~120k | Rule-seeded fraud score, prior claims, report lag |
| `bronze_weather_raw` | bronze | ~30 | Per-district flood / wind / freeze risk |
| `ref_handlers` | ref | ~80 | Claim handlers (grade / team / BU) |
| `ref_weather_index` | ref | ~30 | Materialised weather feed for joins |

All dates are **rolling** relative to `current_date()`, so the demo never goes stale. UC tags applied: `project=claims_workbench`, `owner=wryszka`, `layer=<bronze\|ref>` (tables); `demo=bricksurance_se` (schema).

## Deliberately-seeded business signals

These are intentional — later phases tell stories with them:

- **Long tail:** 80% of claims report < £5k, tail to £250k.
- **Under-reserving:** home escape-of-water reserves are systematically ~28% light vs. what's needed (the "+28% under-reserving" story in Phase 3).
- **Geo skew:** north-west districts (M, BL, OL, WN) get ~3× escape-of-water frequency.
- **Quarantine bait:** ~2% intentionally malformed rows (bad `policy_number` / out-of-range `fraud_score`) for the Phase 1 DLT quarantine demo.

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
SELECT * FROM <catalog>.claims_workbench.bronze_gw_cc_claim WHERE claim_public_id = 'cc:900001';
```

## Repository Structure

```
claims_workbench/
├── databricks.yml                         # DAB definition (catalog variable, dev target)
├── README.md
├── notebooks/
│   ├── 00_setup_and_data_generation.py    # orchestration notebook (run this)
│   └── claims_data_gen.py                 # reusable generation module (roll_dates, generate_all)
├── resources/                             # (future) jobs / pipelines / app resources
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
