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
| Call summarisation → claim record | Synthetic FNOL transcript → FM-generated structured summary, handler approves | Try a claim / Work a claim | workshop | M | Idea |
| Automated note-taking | Every interaction transcribed + structured to the claim record | Work a claim | workshop | M | Idea (fold into co-pilot) |
| Dynamic FNOL questioning | Next question adapts to prior answers; no redundant questions | Try a claim | workshop | M | Idea |
| Digital FNOL journey | Customer-facing web/app/chat notification flow | New "Customer" lane | gap-scan | L | Idea |
| Customer portal | Policyholder self-service status (reuse the Broker Portal pattern 1:1) | New screen next to Broker Portal | gap-scan | M | Idea |
| Duplicate claim detection | Same loss reported twice (fuzzy match on policy/date/peril) | Ingestion / Fraud | gap-scan | S | Idea |

## B · Handling & decisioning

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| Customer comms drafting | Personalised, channel-aware update drafts; handler approves | Work a claim | workshop + review | M | Tile |
| **Customer vulnerability standards** | Flag + classification + recommended handling protocol at point of interaction; complaints/outcomes dashboard | FNOL 360 + Work a claim + Governance | workshop | M | Idea — **top gap** |
| Reserve adequacy | £ + reason vs peer claims; root-cause of adjustments; benchmark vs historicals | Work a claim | review + workshop | M | Tile |
| Deterministic reason codes | Stable code set behind every model/rule decision | Work a claim / Governance | review | S | Tile |
| Payments & indemnity control | Payee verification, dual authorisation, excess collection, push-payment fraud check | Work a claim / Governance | gap-scan | L | Idea |
| Bodily injury / PI lane | Injury coding, rehab, care costs, OIC/MOJ portal, Ogden-rate sensitivity | New peril + screens | gap-scan | L | Idea |
| Total loss & salvage | Valuation vs market, UK Cat markers, salvage recovery tracking | Work a claim / Worklists | gap-scan | M | Idea |
| ClaimCenter write-back | Full handling primitives in-line; decisions/reserves/notes back to the SoR | Work a claim | review | L | Tile |

## C · Fraud & SIU

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| Telematics vs reported account | Surface speed-at-incident vs limit + harsh braking against the FNOL story (data already in silver) | Fraud & SIU | workshop | S | Idea — cheap win |
| Fraud attribution analytics | Which signals/actions actually caught confirmed fraud; feed back into rules | Fraud & SIU models tab | workshop | M | Idea |
| Network / ring detection | Shared phones, addresses, repairers, claim graphs | Fraud & SIU | review | L | Tile |
| Bureau & device signals | CIFAS/IFB-style external signals, device fingerprints | Fraud & SIU | review | M | Tile |
| SIU case management | Investigation workflow, evidence chain, outcomes | Fraud & SIU | gap-scan | L | Idea |

## D · Recovery & downstream

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| Recovery / subrogation pack | Auto-drafted recovery pack: liability evidence, counterparty, heads of loss | Work a claim | review | M | Tile |
| Reinsurance recovery | Large-loss notification + XoL recovery tracking (bridges to the Reinsurance Workbench demo) | Control Tower / new | gap-scan | M | Idea |
| Litigation early-warning | Litigation propensity + settle-vs-defend economics + legal spend | Control Tower | review + gap-scan | M | Tile |
| Reopen-risk | Score settled claims for reopen likelihood | Control Tower | review | M | Tile |

## E · Suppliers & delegated

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| Supplier accountability layer | Cost / quality / cycle-time per repairer in one governed view | New Insight tab or screen | workshop | M | Idea |
| Repairer & parts steering | Right repairer by capacity, quality score, parts availability | Work a claim | review + workshop | M | Tile |
| Repair evidence validation | Photos/video vs claimed scope of work; quality + fraud flags (extends the photo agent) | Fraud / Work a claim | workshop | M | Idea |
| TPA / delegated authority oversight | Bordereaux ingestion, delegated-handler audit | Governance / Ingestion | gap-scan | L | Idea |

## F · Oversight, quality & compliance

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| **QA on every interaction** | AI adherence scoring on 100% of claims (vs sampled); flags, never overrides; feeds coaching | Governance | workshop | M | Idea — **top gap** |
| Complaints, FOS & CSAT lens | Complaints volumes, root cause, FOS escalations, satisfaction by segment | Insight / Governance | workshop + gap-scan | M | Idea |
| Predictive SLA intervention | Predicted-breach flag before the SLA clock runs out (near-SLA worklist exists) | Control Tower | workshop | S | Idea |
| Leakage audit lens | Drill the existing leakage_flag into a worklist + £ story | Control Tower / Worklists | gap-scan | S | Idea |
| Consumer Duty outcomes | Outcome monitoring by cohort (extends fair outcomes) | Governance | gap-scan | M | Idea |
| Sanctions / AML payee screening | Screen payees before settlement | Governance / payments | gap-scan | S | Idea |
| Claims triangles → actuarial feed | Development triangles from the claims book (bridges to the Solvency II demo's chain-ladder) | Insight / Governance | gap-scan | M | Idea |
| Claims pricing gap | Quote-time cost assumptions vs settlement actuals (repair-basket inflation); bridges to Pricing Workbench | Insight | workshop | M | Idea |
| Indemnity factorial testing | Auditable controlled experiments attributing indemnity impact to changes | Governance | workshop | L | Idea (park) |

## G · Catastrophe & events

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| Catastrophe surge mode | Event footprint, surge triage rules, staffing view | Control Tower | review | L | Tile |
| Weather event early-warning | Storm footprint on the geo map + pre-event customer contact (weather data exists) | Insight Geography | gap-scan | M | Idea |

## H · Distribution (Broker Portal — base shipped 2026-07-07)

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| Broker portal (row-filtered book) | Self-service status, search, book-at-a-glance, Genie | Broker Portal | workshop | — | **Live** |
| Document upload | Broker drops documents into the claim file | Broker Portal | — | M | Tile |
| Bordereaux downloads | Monthly claims bordereaux from the governed views | Broker Portal | — | M | Tile |
| Status notifications | Email/webhook on stage change | Broker Portal | — | M | Tile |
| API / Delta Sharing | Row-filtered views to broker platforms | Broker Portal | — | M | Tile |

---

## Recommended build order (next 6)

1. **Copy-level enrichment (S, do first)** — the ~70%-of-calls-are-live-claims stat into the Broker Portal story; "auditor's single view" line on Claim Track; telematics-vs-account line in Fraud & SIU.
2. **Vulnerability standards (M)** — the only top workshop theme with no answer in the app.
3. **Customer comms drafting (M)** — already the #1 wow pick, now client-validated; promotes an existing tile to real.
4. **QA-every-interaction + predictive SLA + leakage lens (S+S+S)** — three small oversight wins that make Governance/Control Tower feel complete.
5. **Reserve adequacy (M)** — promotes an existing tile; workshop asked for exactly this shape (recommendation + explanation + benchmark).
6. **Supplier accountability layer (M)** — new Insight tab; two workshop use cases + a repeat theme.

Everything else stays visible as coming-soon tiles / Roadmap entries until a client asks.
