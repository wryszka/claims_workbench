# Claims Intelligence Workbench — Governance Notes

Governance posture for **Bricksurance SE**, in plain English for a CDO / compliance lead.
Everything below is backed by genuine Unity Catalog / MLflow / DLT output — see
`notebooks/07_governance.py`.

> Synthetic demonstration. No real Guidewire integration and no real customer data.

## Sensitivity tiers — PII and SECRET (Phase 11)

The workbench classifies data in two protected tiers, each enforced by a dynamic
masking view (works on any workspace, no governed tags required):

| Tier | Examples | View | Privileged group | Non-privileged sees |
|------|----------|------|------------------|---------------------|
| **PII** | postcode district, handler name | `v_claims_masked` | `claims_workbench_pii_readers` | truncated / hashed |
| **SECRET** | claim narrative (`description_text`), derived injury/health signal | `v_claims_secret` | `claims_workbench_secret_readers` | narrative withheld, health flag NULL |

**Secret** sits a tier above PII: the free-text claim description can carry health /
injury detail and other special-category data, so it is withheld from everyone except
SIU / the Data Protection Officer.

### CMK / Lakebase positioning note
Secret-tier data warrants **Customer-Managed Keys (CMK)**: the customer holds the
encryption key in their own KMS, so storage is encrypted under a key Databricks cannot
use unilaterally — required for special-category data under many regulators. When this
demo is productionised, the Secret-tier columns (and any operational store such as
**Lakebase** used as the claims system-of-record / CMS) should be provisioned in a
**CMK-enabled** workspace/store, with tighter audit logging than the PII tier. The
masking views are the access-control layer; CMK is the encryption-at-rest layer beneath.
The `agent_reasoning_log` (regulator-viewable decision reasoning) is governed at the PII
tier — it records reasoning over claims, not raw narrative.

- **End-to-end lineage** — every claim is traceable through the platform, from the raw
  Guidewire landing record → governed bronze → enriched silver → ML features → registered
  model → live decision, viewable in Unity Catalog's Lineage tab.
- **PII protection** — claimant location and handler identity are masked by default
  (`v_claims_masked`): non-privileged staff see a truncated postcode and a pseudonymised
  handler, while only the data-protection / SIU group sees the raw values.
- **Model provenance** — each model carries a full card in Unity Catalog (version, champion
  alias, training run, source notebook, and accuracy / macro-F1), so we can always show
  which data and code produced a given decision.
- **Data quality** — the bronze pipeline enforces explicit quality rules and quarantines bad
  records rather than dropping them silently; ~98–99% of records pass, and every failure is
  retained and inspectable.
- **Audit trail** — `gold_handler_decisions` records every model recommendation, the handler's
  action, any override and its reason, and a timestamp — the FCA / Consumer-Duty accountability
  trail (populated once the app goes live in Phase 8).

## Admin one-liner — enable native `project`/`owner` tag filtering

This workspace enforces a **governed tag policy** that restricts the allowed *values* for the
`project` and `owner` tag keys, so `ALTER TABLE … SET TAGS ('project'='claims_workbench')`
was rejected. The demo therefore uses a `TBLPROPERTIES('project'='claims_workbench')` fallback
on every asset (always works; queryable). To make Catalog Explorer's native tag-filtering work
too, a metastore admin adds the values to the tag policy:

- **Catalog Explorer → Settings → Tag policies** → edit key `project` → add allowed value
  `claims_workbench` (and key `owner` → add `wryszka`); **or** via API:
  `databricks tag-policies update --key project --add-allowed-value claims_workbench`.

After that, re-running the `set_tags_safe(...)` calls applies the UC tags and the assets show
up under the `project=claims_workbench` tag filter natively. No code change required — the
fallback property remains as belt-and-braces.
