# Claims Intelligence Workbench — Ageas Demo Track & Test Script

**Audience:** Ageas (UK personal lines — motor & home) claims leadership + data/IT
**App (dev):** https://claims-workbench-7474656169654171.aws.databricksapps.com
**Insurer in the demo:** *Bricksurance SE* (synthetic — say so up front)
**Built on:** Databricks Smart Claims accelerator + EY Future of Claims framework, extended to personal lines with Guidewire CDA ingestion, Mosaic AI agents, auto-close, governance.

> One line to open with: *"This is one governed lakehouse that ingests every channel of claim data, decides the routine claims automatically, gives your handlers an AI second opinion on the hard ones, and shows the regulator exactly how every decision was made — all on Databricks."*

---

## The three hero claims (your demo spine)

| Claim | Peril | Reported | Signals | Outcome | The point |
|---|---|---|---|---|---|
| **cc:900002** | Home — escape of water | £420 | fraud 8, 0 prior, next-day report | **Auto-closed & paid** (model pay_direct @ ~90%, all rules pass) | Straight-through processing — the efficiency win |
| **cc:900001** | Motor TP | £8,500 | fraud 74, 2 prior, speeding (95 in 50 zone) | **Escalated → SIU** (refer_siu @ ~80%, rules R1/R2/R3/R4/R6 fire) | The hard claim — model + rules + agents + human |
| **cc:900003** | Motor TP | £600 (reported "minor knock") | clean otherwise — but **photo shows a severe crash** | **Escalated on R7 alone** | The wow — the photo catches what every other signal misses |

---

## Pre-flight checklist (do this before the room)

1. **Freshness:** Open the app → Control Tower. If the data looks stale, click **↺ Reset demo** (bottom-left) — it re-anchors all dates to today, clears the sandbox, re-warms the cache (~10–15 min; do it well ahead, not live).
2. **Cache mode:** Bottom-left toggle should read **AI: cached** — keeps agent answers instant in the room. (Flip to *live* only if asked "is it really calling the model?")
3. **Smoke check:** Load **Work a claim** and confirm cc:900001, cc:900002, cc:900003 all open. Load **Ingestion** and confirm the quality scorecard shows ~99.5%.
4. **Browser:** full-screen, one tab, zoom ~110% so the back row can read it.

---

# THE 20-MINUTE TRACK

Six acts. Each act lists the **click path**, the **talk track**, and **what it proves**.

### Act 1 — The book at a glance *(Head of Claims, 3 min)*
**Click:** `Head of Claims → Control Tower`
- Walk the **Efficiency** vs **Effectiveness** tiles (RAG + trend arrows). Land on the **% auto-closed** hero (≈£ and handler-hours saved).
- Point to **Recovery on the table** and **EoW reserve development** headline cards.
- Read one line from the **Monday brief** ("what needs you") — note each line opens a worklist.
- **Drag the auto-close risk-appetite dial** (confidence / amount / fraud) — the segment count re-computes live.
- **Talk:** *"Your CCO opens here every Monday. Efficiency on the left, P&L protection on the right, and a risk-appetite dial — move it and the book re-segments live. No agent has pay authority; the workflow decides."*
- **Proves:** real portfolio analytics + governed straight-through policy you control.

### Act 2 — Straight-through: the claim that closed itself *(3 min)*
**Click:** `Handler → Work a claim` → select **cc:900002**
- Disposition banner: **AUTO-CLOSED & PAID (simulated)**. Show the green **every rule passed** chips.
- Show triage **PAY DIRECT @ ~90%**, the reserve bracket, fraud 8/100.
- **Talk:** *"A clean £420 escape-of-water claim. The model said pay, every rule passed, so it straight-through-processed — no human touched it. Scale that across the ~5% of the book that qualifies and that's the FTE you give back to the hard claims."*
- **Proves:** model + rule engine auto-close, fully reasoned.

### Act 3 — The handler's day & a real decision *(Handler, 4 min)*
**Click:** `Handler → My Queue`
- This is **Sarah Chen, Senior, Motor Complex**. The triage layer sifted 3,000+ open motor claims down to ~24 that need her. Three buckets: **Needs you today / This week / When you can**. Read the **monitoring-agent morning brief**.
- Click **cc:900001** (top of "Needs you today") → opens **Work a claim**.
- Show **REFER TO SIU @ ~80%**, reserve, fraud **74/100**, and the **rule engine** chips that fired (fraud, lag, prior claims, speeding).
- Scroll to the **Agent reasoning trail** (5 agents — Dossier, Fraud, Challenge, Recovery, Audit) — regulator-viewable.
- Open a **Second opinion** (e.g., Senior Reserving Actuary or Loss Adjuster) → live agent answer.
- Use the **HITL bar**: click **Accept** (or **Override** with a reason) → decision logged.
- **Talk:** *"Sarah doesn't triage 3,000 claims — the platform did. She gets a worklist, a model recommendation, a reasoning trail she can defend, and a second opinion on demand. She decides; the AI advises."*
- **Proves:** persona worklist, model decision, multi-agent reasoning, expert second opinion, human-in-the-loop.

### Act 4 — THE WOW: the photo that catches the lie *(4 min)*
**Click:** `Handler → Work a claim` → select **cc:900003**
- Reported as a **minor £600 knock**. Everything looks clean — fraud 12, reported next day, driving 28 in a 30 zone.
- Scroll to the **photo / image-severity panel**: the FNOL photo reads **SEVERE**. The disposition is **ESCALATED — rule R7 fired** (image severity vs reported amount).
- **Then go to** `The platform → Claims AI` → click the first pinned question: **"Does the photo on cc:900003 match the reported account?"**
- The **Fraud agent** answers live: the photo directly contradicts the £600 report → HIGH risk, hold for inspection.
- **Talk:** *"Every structured signal said 'pay it'. The photo said otherwise. A vision model read the image at the point of ingest, the rule engine caught the mismatch, and the fraud agent explained it — in plain English, with the photo as evidence. That's leakage and fraud you cannot catch with structured data alone."*
- **Proves:** unstructured + AI at ingest, the rule engine, the agent narrative — the Smart Claims signature moment.

### Act 5 — The intelligence layer *(The platform, 3 min)*
**Click:** stay on `The platform → Claims AI`
- Top strip: **Models decide → Agents reason → Experts review → every step audited.**
- Type or click an example question → the **supervisor routes** it to the right specialist; answer returns (note the **cached/live** pill).
- Scroll the **bench**: one supervisor, 5 specialist agents, 4 expert reviewers, 2 model tools, 2 Genie spaces — each a **real Unity Catalog-registered endpoint** you can click through to.
- **Talk:** *"One front door. Ask in plain English, the supervisor dispatches to the specialist best placed to answer, and every node here is a governed, independently deployable Databricks endpoint — not a black box."*
- **Proves:** Mosaic AI multi-agent supervision, Genie, cache-first, real endpoints.

### Act 6 — Trust & the platform underneath *(Data & Governance, 3 min)*
**Click:** `Data & Governance → Governance`
- **AI agents & reasoning audit:** every agent's reasoning on the hero claims, **regulator-viewable**.
- **What's collected & how it's used:** the data inventory with **PII / SECRET tiers, CMK note, masking**.
- **Claim track:** type a claim number → lifecycle timeline + documents received/awaited/**missing** + completeness %.
- **Fair outcomes:** consistency by channel/peril + run the **Consumer-Duty reviewer** on a claim.

**Click:** `Data & Governance → Ingestion`
- The medallion path; the **source map** — Guidewire CC/PC **plus** fraud, weather, telematics, documents (9 live feeds).
- The **quality contract** scorecard (per expectation: table, action, pass %). The **quarantine drill** — click a reason, see the actual rows held back (no silent data loss).
- The **Auto Loader + vision** spotlight (the cc:900003 photo) and **Inspect the input** (raw bronze rows).
- **Talk:** *"Everything you just saw sits on one governed lakehouse. Every channel and format lands here, a quality contract checks every record, nothing is silently dropped, and the regulator can see exactly what was collected and how every decision was reasoned."*
- **Proves:** Lakeflow ingestion, DLT quality + quarantine, governance, lineage — the foundation.

**Close (30 sec):** *"Decide the routine automatically, arm your handlers on the hard ones, catch what structured data can't, and prove it all to the regulator — one platform, your data, your models, your governance."*

---

# USER STORIES (also your end-to-end test script)

Each story = **persona · story · steps · expected result**. Run top to bottom to confirm the whole tool works.

### US-1 — Portfolio at a glance
*As Head of Claims, I want one view of book health so I know what needs me.*
- **Steps:** Control Tower.
- **Expect:** Efficiency + Effectiveness tiles with RAG/trends; % auto-closed hero; Monday brief populated; recovery + reserve cards.

### US-2 — Re-segment auto-close live
*As Head of Claims, I want to set risk appetite and see the impact instantly.*
- **Steps:** Control Tower → drag the **Min confidence / Max amount / Max fraud** sliders.
- **Expect:** the auto-closed / escalated counts re-compute within ~1s (no re-score, rule-aware).

### US-3 — Work a list, not a pile (handler queue)
*As a handler (Sarah Chen), I want a prioritised worklist.*
- **Steps:** Handler → My Queue.
- **Expect:** persona header; 3 buckets; **cc:900001 and cc:900003 in "Needs you today"**; cc:900002 absent (it's home); monitoring brief text.

### US-4 — Auto-closed claim
*As Head of Claims, I want to see a claim that closed with no human touch.*
- **Steps:** Work a claim → **cc:900002**.
- **Expect:** **AUTO-CLOSED & PAID** banner; all rules pass (green); triage PAY DIRECT ~90%.

### US-5 — Escalated SIU claim with reasoning
*As a handler, I want the model call, the rules, and the agents' reasoning on a hard claim.*
- **Steps:** Work a claim → **cc:900001**.
- **Expect:** REFER TO SIU ~80%; fraud 74; fired rules incl. fraud/lag/prior/speed; **5-agent reasoning trail** renders.

### US-6 — Second opinion from an expert agent
*As a handler, I want a senior specialist to sanity-check.*
- **Steps:** On cc:900001 → "Second opinions" → click **Get opinion** on any expert (Reserving / Adjuster / Coverage / Conduct).
- **Expect:** a live (or cached) plain-English opinion appears in the card.

### US-7 — Record a human decision (HITL)
*As a handler, I want to accept or override and have it logged.*
- **Steps:** On any claim → HITL bar → **Accept** (or **Override** + reason).
- **Expect:** green "Decision logged" confirmation with an ID + timestamp.

### US-8 — The photo discrepancy (vision + rules)
*As an SIU lead, I want the system to catch a claim where the photo contradicts the report.*
- **Steps:** Work a claim → **cc:900003** → scroll to the image panel.
- **Expect:** image severity **SEVERE**; disposition **ESCALATED**; **R7** in the fired rules.

### US-9 — Ask the supervisor (agent routing)
*As anyone, I want to ask a plain-English question and get a routed, sourced answer.*
- **Steps:** The platform → Claims AI → click the pinned **"Does the photo on cc:900003 match…"** question.
- **Expect:** Fraud agent answer citing the photo-vs-£600 discrepancy; cached/live pill shows.

### US-10 — Create a claim & score it live
*As a handler/product owner, I want to test a hypothetical claim against the real models.*
- **Steps:** Handler → Try a claim → pick **"Late, suspicious motor"** → **⚡ Score this claim**.
- **Expect:** in ~1–3s: REFER TO SIU, escalated, rules R1/R2/R3 fired. (Try "Clean motor knock" → PAY DIRECT, auto-closed.)

### US-11 — Fraud & SIU view
*As an SIU analyst, I want the fraud concentration and the high-score open queue.*
- **Steps:** Fraud & SIU.
- **Expect:** refer rate, score-band distribution, SIU queue table (click a row → claim).

### US-12 — Team performance & trends
*As Head of Claims, I want desk performance and direction of travel.*
- **Steps:** Head of Claims → Insight → toggle **Performance** / **Trends**.
- **Expect:** team/handler leakage + settle tables; 12-week sparklines (auto-close, leakage, settle, SLA, recovery).

### US-13 — Ingestion: sources + quality contract
*As a data lead, I want to see every feed and the quality gate.*
- **Steps:** Data & Governance → Ingestion.
- **Expect:** source map (9 live / 3 roadmap); quality scorecard ~99.5% with table/action/pass%; freshness tile (last CDA batch).

### US-14 — No silent data loss (quarantine drill)
*As a data/risk lead, I want to inspect what failed the quality gate.*
- **Steps:** Ingestion → quarantine card → click **invalid_loss_cause** (or fraud_score_out_of_range).
- **Expect:** the actual quarantined rows render (held back, not deleted).

### US-15 — Data quality by dimension + analytics
*As a data lead, I want DQ beyond the pipeline + basic analytics.*
- **Steps:** Ingestion → "Data quality by dimension" + "What's in the book".
- **Expect:** Completeness/Uniqueness/Referential integrity green; **Timeliness amber**; by-peril/channel/amount bars; 12-month volume bars.

### US-16 — Inspect the raw input
*As a data engineer, I want to see raw rows exactly as ingested.*
- **Steps:** Ingestion → "Inspect the input" → click **Guidewire ClaimCenter — claims**.
- **Expect:** a live sample of bronze rows incl. `cda_batch_ts` and `_bronze_ingested_at`.

### US-17 — Unstructured at ingest (Auto Loader + vision)
*As an innovation lead, I want to see photos turned into structured data automatically.*
- **Steps:** Ingestion → unstructured spotlight.
- **Expect:** the 4 documents (incl. cc:900003 photo = SEVERE, a police report), each joined to its claim; source = Auto Loader + vision FM.

### US-18 — Governance: reasoning audit + what's collected
*As a compliance officer, I want a defensible audit trail + a data inventory.*
- **Steps:** Data & Governance → Governance → the three tabs.
- **Expect:** agent reasoning log; PII/SECRET inventory + CMK note; claim-track timeline with missing-docs + completeness %; fair-outcomes consistency.

### US-19 — Reset to "today"
*As a demo owner, I want to re-anchor everything to the current date.*
- **Steps:** bottom-left → **↺ Reset demo** → watch progress.
- **Expect:** job triggers; on success the app reloads with dates re-anchored, sandbox cleared, cache re-warmed.

---

# POINT DEMOS — the Q&A drawer (when someone asks)

Pull these up on demand; they map to common questions.

- **"Is it really calling a model, or is this canned?"** → Flip the bottom-left toggle to **AI: live**, re-ask a Claims AI question → watch it call the endpoint (slower). Or Try-a-claim → tweak the numbers → re-score → the decision changes.
- **"How do you decide what auto-closes?"** → Control Tower dial + Work-a-claim disposition chips (the band + R1–R7 rule engine).
- **"What if the model is wrong?"** → HITL override on any claim → logged with reason; nothing auto-pays outside the band.
- **"Can you handle our other systems, not just Guidewire?"** → Ingestion source map (Guidewire CC/PC + fraud + weather + telematics + documents; roadmap: real-time FNOL, call transcripts, DVLA).
- **"What about documents and photos?"** → Ingestion unstructured spotlight + the cc:900003 discrepancy.
- **"Where did the bad records go?"** → Ingestion quarantine drill — the actual rows, held back not dropped.
- **"How do we prove a decision to the FCA / PRA?"** → Governance reasoning audit + claim track + fair outcomes (Consumer Duty).
- **"How do you handle PII / sensitive data?"** → Governance "what's collected" — PII/SECRET tiers, masking, CMK.
- **"Can the business ask its own questions of the data?"** → Claims AI → Genie spaces ("Ask the Book", "Ask Pricing + Claims").
- **"Is the reserve adequate / is this a fair outcome / does the policy respond?"** → Work-a-claim → Second opinions (Reserving Actuary / Coverage Counsel / Consumer-Duty reviewer).
- **"Where's the recovery/subrogation money?"** → Control Tower "Recovery on the table" → worklist; or the Recovery agent on a claim.
- **"What's the architecture / what did you build on?"** → The platform → Learn (EY 7-stage value chain + Smart Claims provenance).

---

## Notes & honesty (keep the demo credible)
- *Bricksurance SE* is synthetic; figures illustrate the platform, not a real book. The "About this demo" disclaimer is on the Control Tower.
- The supervisor currently falls back to the Context agent for synthesis (managed Supervisor endpoint is the next step); routing and specialists are real.
- Loss/combined ratios are flagged **illustrative**; reserve adequacy and recovery figures are computed from the synthetic book.
- Cache-first keeps the room fast; everything runs live with the toggle flipped.
