# Claims Workbench — Feature Backlog (living register)

The master list of features that make this a **comprehensive claims-system demo**.
Sources: client workshops (anonymised — everything here is **Bricksurance SE**), the
practitioner review (2026-06-08), and a domain gap-scan of what a full claims platform
covers. Add rows as new workshops surface requests; move rows to **Live** as they ship.

**Status**: `Live` = built and in the app · `Tile` = coming-soon placeholder exists in
the app · `Idea` = not yet represented anywhere.
**Size**: S = hours · M = ~a day · L = multi-day.

---

## A · Intake & FNOL

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| FNOL call co-pilot | Live transcript prompts: required info collected? gaps flagged, vulnerability cues | Claims AI / new Voice panel | workshop | L | Tile (voice intake) |
| Call summarisation → claim record | Synthetic FNOL transcript → FM-generated structured summary, handler approves | Try a claim / Work a claim | workshop | M | Tile |
| Automated note-taking | Every interaction transcribed + structured to the claim record | Work a claim | workshop | M | Tile (folded into co-pilot) |
| Dynamic FNOL questioning | Next question adapts to prior answers; no redundant questions | Try a claim | workshop | M | Tile |
| Digital FNOL journey | Customer-facing web/app/chat notification flow | New "Customer" lane | gap-scan | L | Tile |
| Customer portal | Policyholder self-service status (reuse the Broker Portal pattern 1:1) | New screen next to Broker Portal | gap-scan | M | Tile |
| Duplicate claim detection | Same loss reported twice (fuzzy match on policy/date/peril) | Ingestion / Fraud | gap-scan | S | Tile |

## B · Handling & decisioning

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| Customer comms drafting | Personalised, channel-aware update drafts; handler approves | Work a claim | workshop + review | M | Tile |
| **Customer vulnerability standards** | Flag + classification + recommended handling protocol at point of interaction; complaints/outcomes dashboard | FNOL 360 + Work a claim + Governance | workshop | M | Tile — **top gap** |
| Reserve adequacy | £ + reason vs peer claims; root-cause of adjustments; benchmark vs historicals | Work a claim | review + workshop | M | Tile |
| Deterministic reason codes | Stable code set behind every model/rule decision | Work a claim / Governance | review | S | Tile |
| Payments & indemnity control | Payee verification, dual authorisation, excess collection, push-payment fraud check | Work a claim / Governance | gap-scan | L | Tile |
| Bodily injury / PI lane | Injury coding, rehab, care costs, OIC/MOJ portal, Ogden-rate sensitivity | New peril + screens | gap-scan | L | Tile |
| Total loss & salvage | Valuation vs market, UK Cat markers, salvage recovery tracking | Work a claim / Worklists | gap-scan | M | Tile |
| ClaimCenter write-back | Full handling primitives in-line; decisions/reserves/notes back to the SoR | Work a claim | review | L | Tile |

## C · Fraud & SIU

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| Telematics vs reported account | Surface speed-at-incident vs limit + harsh braking against the FNOL story (data already in silver) | Fraud & SIU | workshop | S | Tile — cheap win |
| Fraud attribution analytics | Which signals/actions actually caught confirmed fraud; feed back into rules | Fraud & SIU models tab | workshop | M | Tile |
| Network / ring detection | Shared phones, addresses, repairers, claim graphs | Fraud & SIU | review | L | Tile |
| Bureau & device signals | CIFAS/IFB-style external signals, device fingerprints | Fraud & SIU | review | M | Tile |
| SIU case management | Investigation workflow, evidence chain, outcomes | Fraud & SIU | gap-scan | L | Tile |

## D · Recovery & downstream

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| Recovery / subrogation pack | Auto-drafted recovery pack: liability evidence, counterparty, heads of loss | Work a claim | review | M | Tile |
| Reinsurance recovery | Large-loss notification + XoL recovery tracking (bridges to the Reinsurance Workbench demo) | Control Tower / new | gap-scan | M | Tile |
| Litigation early-warning | Litigation propensity + settle-vs-defend economics + legal spend | Control Tower | review + gap-scan | M | Tile |
| Reopen-risk | Score settled claims for reopen likelihood | Control Tower | review | M | Tile |

## E · Suppliers & delegated

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| Supplier accountability layer | Cost / quality / cycle-time per repairer in one governed view | New Insight tab or screen | workshop | M | Tile |
| Repairer & parts steering | Right repairer by capacity, quality score, parts availability | Work a claim | review + workshop | M | Tile |
| Repair evidence validation | Photos/video vs claimed scope of work; quality + fraud flags (extends the photo agent) | Fraud / Work a claim | workshop | M | Tile |
| TPA / delegated authority oversight | Bordereaux ingestion, delegated-handler audit | Governance / Ingestion | gap-scan | L | Tile |

## F · Oversight, quality & compliance

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| **QA on every interaction** | AI adherence scoring on 100% of claims (vs sampled); flags, never overrides; feeds coaching | Governance | workshop | M | Tile — **top gap** |
| Complaints, FOS & CSAT lens | Complaints volumes, root cause, FOS escalations, satisfaction by segment | Insight / Governance | workshop + gap-scan | M | Tile |
| Predictive SLA intervention | Predicted-breach flag before the SLA clock runs out (near-SLA worklist exists) | Control Tower | workshop | S | Tile |
| Leakage audit lens | Drill the existing leakage_flag into a worklist + £ story | Control Tower / Worklists | gap-scan | S | Tile |
| Consumer Duty outcomes | Outcome monitoring by cohort (extends fair outcomes) | Governance | gap-scan | M | Tile |
| Sanctions / AML payee screening | Screen payees before settlement | Governance / payments | gap-scan | S | Tile |
| Claims triangles → actuarial feed | Development triangles from the claims book (bridges to the Solvency II demo's chain-ladder) | Insight / Governance | gap-scan | M | Tile |
| Claims pricing gap | Quote-time cost assumptions vs settlement actuals (repair-basket inflation); bridges to Pricing Workbench | Insight | workshop | M | Tile |
| Indemnity factorial testing | Auditable controlled experiments attributing indemnity impact to changes | Governance | workshop | L | Tile (park) |

## G · Catastrophe & events

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| Catastrophe surge mode | Event footprint, surge triage rules, staffing view | Control Tower | review | L | Tile |
| Weather event early-warning | Storm footprint on the geo map + pre-event customer contact (weather data exists) | Insight Geography | gap-scan | M | Tile |

## H · Distribution (Broker Portal — base shipped 2026-07-07)

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| Broker portal (row-filtered book) | Self-service status, search, book-at-a-glance, Genie | Broker Portal | workshop | — | **Live** |
| Document upload | Broker drops documents into the claim file | Broker Portal | — | M | Tile |
| Bordereaux downloads | Monthly claims bordereaux from the governed views | Broker Portal | — | M | Tile |
| Status notifications | Email/webhook on stage change | Broker Portal | — | M | Tile |
| API / Delta Sharing | Row-filtered views to broker platforms | Broker Portal | — | M | Tile |

---

## Delivery principles (apply to every phase — non-negotiable)

1. **Full governance.** Every process, function and activity is recorded and easily
   surfaced via Databricks: new features write their outputs/decisions to governed UC
   tables (gold_* / audit), appear in Claim Track where claim-level, and the app links
   each one back to the UC object ("see it in Unity Catalog ↗").
2. **Real process, no cut corners.** If someone looks under the covers, the whole thing
   is there in Databricks: real notebooks in the DAB, real tables/views/functions/models
   in UC, real FM/agent calls (cache-first for demo speed, never faked outputs).
   Anything illustrative is labelled illustrative in the UI.
3. **Cheap.** Serverless everything, scale to zero, no always-on compute; views over
   copies; reuse existing schema/warehouse/endpoints; batch-precompute where a live call
   adds cost without adding story.
4. **Documented.** Every feature ships with its demo story: a talk-track entry in the
   demo doc (BEAT or appendix), an in-app explainer card (what this is, why it matters,
   where it lives), and a FEATURE_BACKLOG status flip. A human must be able to show it
   and say why it's being shown.

## Delivery plan

**Phase 0 — placeholders (DONE 2026-07-07).** All backlog rows are now visible in the
app as coming-soon tiles on the screen where they'll live (41 tiles) + the Roadmap page
regrouped. The app IS the roadmap; future workshops point at tiles and prioritise.

**Phase 1 — copy-level enrichment (S).** The ~70%-of-calls-are-live-claims framing in
the Broker Portal story card; "the auditor's single view" positioning on Claim Track;
closes workshop items with zero build. *Governance/doc: demo-doc lines updated.*

**Phase 2 — Vulnerability standards (M) — top gap.** `ref_vulnerability_standards` UC
table (categories, definitions, handling protocols); deterministic vulnerability
signals at silver (distress perils, repeat claimants, channel patterns — honestly
labelled synthetic where synthetic); flag + protocol panel in FNOL 360 / Work a claim;
Governance dashboard tile (volumes, outcomes, complaints proxy by category); agents read
the protocol via the context function. *Every surfaced flag traceable to the standards
table; shows up in Claim Track.*

**Phase 3 — Customer comms drafting (M).** Real FM call (Claude via Foundation Model
API) drafting the acknowledgement / decision letter / settlement breakdown from claim
context; handler approves; drafts + approvals persisted to `gold_comms_drafts` (the
audit trail IS the feature); cache-first for the demo heroes. *Promotes an existing
tile; zero standing cost (on-demand FM).*

**Phase 4 — small oversight wins (3×S).** Predictive SLA breach (rule-based predictor
over pace-vs-SLA, written to a gold view + Control Tower tile + worklist); leakage audit
lens (existing `leakage_flag` → worklist + £ story); QA-coverage panel in Governance
(batch FM+rules adherence scoring on the decided claims → `gold_qa_scores`, "100% vs
sampled" story). *All batch/serverless; all land as governed tables first, UI second.*

**Phase 5 — Reserve adequacy (M).** Cohort development benchmark in SQL (comparable
claims by peril/value band), £ gap + reason per open claim → `gold_reserve_adequacy`;
Work-a-claim card + Control Tower drill. *Workshop-validated shape: recommendation +
plain-language explanation + historical benchmark.*

**Phase 6 — Supplier accountability layer (M).** Deterministic repairer dimension
(same salted-hash pattern as brokers), `gold_supplier_scorecard` (cost/quality/cycle
per repairer); Insight tab + feeds the repairer-steering tile later. *Views over
copies; one schema.*

**Then:** re-prioritise from client workshops — the tiles make that conversation visual.
Bigger L items (BI lane, payments control, cat surge, ring detection, TPA oversight)
stay parked until a client pulls them forward.
