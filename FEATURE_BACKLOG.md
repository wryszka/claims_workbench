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
| Call summarisation → claim record | Batch ai_query analysis of every transcript: summary, sentiment, missing info, vulnerability cues, complaint risk | Work a claim › Calls & comms | workshop | M | **Live** (2026-07-07; post-call batch — live co-pilot still Tile) |
| Automated note-taking | Every interaction transcribed + structured to the claim record | Work a claim | workshop | M | Partially live (post-call, via call insights); live in-call = co-pilot Tile |
| Dynamic FNOL questioning | Next question adapts to prior answers; no redundant questions | Try a claim | workshop | M | Tile |
| Digital FNOL journey | Customer-facing web/app/chat notification flow | New "Customer" lane | gap-scan | L | Tile |
| Customer portal | Policyholder self-service status (reuse the Broker Portal pattern 1:1) | New screen next to Broker Portal | gap-scan | M | Tile |
| Duplicate claim detection | Same loss reported twice (fuzzy match on policy/date/peril) | Ingestion / Fraud | gap-scan | S | Tile |

## B · Handling & decisioning

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| Customer comms drafting | Live Claude FM drafts (ack / update / settlement / decision) with the claim's vulnerability guidance in the prompt; handler approves; audited in gold_comms_drafts | Work a claim › Calls & comms | workshop + review | M | **Live** (2026-07-07) |
| **Customer vulnerability standards** | Flag + classification + recommended handling protocol at point of interaction; outcomes dashboard | Work a claim (protocol card) + Governance › Vulnerability tab | workshop | M | **Live** (2026-07-07; FNOL 360 surfacing + agent-prompt wiring = next increment) |
| Reserve adequacy | Suggested reserve + £ gap + plain reason vs the book's settled comparables (gold_reserve_adequacy) | Work a claim (auto card) | review + workshop | M | **Live** (2026-07-07) |
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
| Supplier accountability layer | Cost / cycle / quality per repairer + peer indices + steer (gold_supplier_scorecard) | Insight › 🔩 Suppliers tab | workshop | M | **Live** (2026-07-07) |
| Repairer & parts steering | Right repairer by capacity, quality score, parts availability | Work a claim | review + workshop | M | Tile |
| Repair evidence validation | Photos/video vs claimed scope of work; quality + fraud flags (extends the photo agent) | Fraud / Work a claim | workshop | M | Tile |
| TPA / delegated authority oversight | Bordereaux ingestion, delegated-handler audit | Governance / Ingestion | gap-scan | L | Tile |

## F · Oversight, quality & compliance

| Feature | What it is | Where it fits | Source | Size | Status |
|---|---|---|---|---|---|
| **QA on every interaction** | 6 deterministic adherence checks over 100% of the book (gold_qa_scores); flags, never overrides | Governance › QA tab | workshop | M | **Live** (2026-07-07; FM-scored narrative QA = future increment) |
| Complaints, FOS & CSAT lens | Complaints volumes, root cause, FOS escalations, satisfaction by segment | Insight / Governance | workshop + gap-scan | M | Tile |
| Predictive SLA intervention | Cohort-benchmark prediction (gold_sla_prediction) → Control Tower tile + worklist | Control Tower + Worklists | workshop | S | **Live** (2026-07-07) |
| Leakage audit lens | leakage_flag → 💸 Leakage worklist (paid £ ranked) | Worklists | gap-scan | S | **Live** (2026-07-07) |
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

**Phase 1 — copy-level enrichment (S) — DONE 2026-07-07.** The ~70%-of-calls-are-live-claims
framing in the Broker Portal story card; "the auditor's single view" positioning on Claim
Track; demo-doc lines updated (repo + Google Doc).

**Phase 2 — Vulnerability standards (M) — DONE 2026-07-07 (core).**
`ref_vulnerability_standards` UC table (4 categories: definitions, indicators, handling
protocol, agent guidance) + `gold_vulnerability_flags` view (deterministic: home fire /
big escape-of-water / repeat claimant / tenure+phone proxy, plus a ~5% salted-hash
**synthetic declared cohort, labelled**). Surfaced: Governance › **Vulnerability** tab
(headline, standards cards, click-through flagged claims) + a real protocol card on
**Work a claim** (replaces the tile; clean claims show "checked, no indicators").
Both objects UC-linked from the app. *Next increment: FNOL 360 surfacing + wiring
agent_guidance into the live agent prompts.*

**Phase 3 — Customer comms drafting + call-transcript analysis (M) — DONE 2026-07-07.**
(a) **Comms**: live Claude FM drafts (acknowledgement / update / settlement / decision)
from the claim facts **with the claim's vulnerability agent_guidance in the prompt**
(Phase 2 wired into a real generation path); handler approves in-app; every draft +
approval written to `gold_comms_drafts`. (b) **Transcripts**: `bronze_call_transcripts`
(deterministic synthetic calls — heroes, a distressed fire victim, a broker status-chase,
capability/financial-strain calls, and two `sales` calls to prove the pipeline is
**source-agnostic**) + `gold_call_insights` = REAL batch `ai_query` analysis (summary,
sentiment, intent, missing info, vulnerability cues, follow-ups, complaint risk). Both
surfaced on Work a claim › **Calls & comms**; notebook `13_call_transcripts.py`.
*Zero standing cost: pay-per-token FM, no serving infra.*

**Phase 4 — small oversight wins (3×S) — DONE 2026-07-07.** (a) **Predictive SLA**:
`gold_sla_prediction` view — expected total days per open claim from the **cohort
benchmark** (avg settled days per peril × value band, the book's own actuals) vs the
per-peril SLA → `breached | predicted_breach | on_track`; Control Tower tile ("512 on
pace to breach — intervene now") + ⏱️ Predicted-breach worklist. (b) **Leakage lens**:
💸 Leakage worklist (settled + leakage_flag, ranked by paid £). (c) **QA on every
claim**: `gold_qa_scores` view — 6 deterministic adherence checks over 100% of the book
(triage recorded, reserve set, within SLA, high-fraud-never-fast-tracked, auto-close in
appetite, overrides reasoned) → Governance › **QA** tab (coverage 100% vs ~2–5% sampled,
check-failure breakdown, failing claims click through to Work a claim). Real finding in
the synthetic book: 176 claims fast-tracked with fraud score > 70. *Views only — no
compute, no copies; flags, never overrides.*

**Phase 5 — Reserve adequacy (M) — DONE 2026-07-07.** `gold_reserve_adequacy` view:
suggested reserve = initial × the cohort development ratio (settled comparables by
peril × value band, the book's own actuals), £ gap, plain-language reason, adequacy
band (±15% / £500 dead-band). Every open claim on Work a claim gets an automatic card:
under-reserved → amber card (initial / suggested / cohort ×, "Why:", never an automatic
adjustment); otherwise a slim "in line with comparables" note. The view independently
rediscovers the seeded escape-of-water 1.25× under-reserving: 1,626 open claims,
£21.2M total gap. *View only; notebook 03 §4f.*

**Phase 6 — Supplier accountability layer (M) — DONE 2026-07-07.** `ref_supplier`
(8 fictional repairers: 4 motor incl. a mobile SMART specialist, drying / fire-rebuild /
2 general restoration) + `gold_supplier_scorecard` view: jobs assigned deterministically
(peril + light-damage rule + postcode hash — labelled synthetic), metrics computed from
the real claims each supplier carries (so differences are genuine job-mix effects:
Silverline £646 avg / 25d cycle / preferred; heavy bodyshops £20k / review), peer cost
& cycle indices per trade, steer = preferred / watch / review. Surfaced: Insight ›
**🔩 Suppliers** tab + a "Supplier scorecard" button on the main Insight page. £1,175M
through the panel. Honesty caption: severity-adjusted like-for-like = next increment.
*This is the data layer the repairer-steering tile will act on.*

**Then:** re-prioritise from client workshops — the tiles make that conversation visual.
Bigger L items (BI lane, payments control, cat surge, ring detection, TPA oversight)
stay parked until a client pulls them forward.
