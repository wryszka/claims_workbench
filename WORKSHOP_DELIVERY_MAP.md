# You asked, we built it — workshop → demo map

At the claims AI value-roadmap workshop, three breakout groups prioritised ~28 use cases.
This maps every one to the **Bricksurance SE Claims Intelligence Workbench**: what is
**LIVE** in the working demo today, and what is a visible **🧭 roadmap tile** in the app,
on the exact screen where it will live.
(Google Doc version: `1RWa2OG2UFt6afu6KwH4c6SzN5NPH6apLJu5IO1Lpo8A`.)

## 1 · Claims Intake, Triage & Routing

| Workshop use case | Status | Where in the demo |
|---|---|---|
| Live claims handler call support agent | 🧭 tile (post-call half ✅) | Try a claim tile; post-call analysis already live in Work a claim › Calls & comms |
| Call summarisation agent | ✅ LIVE | Calls & comms — batch ai_query on every transcript: summary, sentiment, missing info, complaint risk (see cc:900001's two calls) |
| Auto-routing agent | ✅ LIVE | Triage model + rule engine + auto-close dial (Control Tower); handler sign-off in Work a claim; assignment logic in My Queue |
| Customer claims communication agent | ✅ LIVE (drafting) | Vulnerability-aware letters drafted by Claude, human-approved, audited in gold_comms_drafts; scheduling/notifications = tiles |
| Broker Portal | ✅ LIVE | Sidebar — row-filtered mock sign-in, plain-English status + next step, book-at-a-glance, Genie on the broker's view; 4 phase-2 tiles |
| Customer vulnerability standards | ✅ LIVE | One governed UC standard (definitions, flags, handler protocol + agent guidance); Governance › Vulnerability + automatic claim card |

## 2 · Investigation, Decisioning & Fraud

| Workshop use case | Status | Where in the demo |
|---|---|---|
| Dynamic real-time questioning | 🧭 tile | Try a claim |
| Supplier performance data sharing | ✅ LIVE | Insight › Suppliers — cost/cycle/quality per repairer, peer indices, steer |
| Claims simplification for auditors | ✅ LIVE | Governance › Claim track — the auditor's single view + PDF closure packages in a UC Volume |
| Next best action + chat with your data | ✅ LIVE | Claims AI (interrogable recommendations + reasoning trail) + Genie in Insight |
| Personalised communications | ✅ LIVE (first increment) | Comms drafts adapt to vulnerability guidance; channel preference = roadmap |
| Automated note taking | ✅ LIVE (post-call) | Transcripts → structured insights on the claim record; in-call = co-pilot tile |
| Sales cockpit | foundation ✅ | Out of claims scope, but the transcript pipeline already ingests sales calls (gold_call_insights) |
| Automate to connect | noted | Outbound telephony — out of scope |
| Driving telematics (×3) | data ✅ · 🧭 tiles | Speed/braking already in the claim record + telematics map in Insight › Geography; fraud cross-check tile; pricing → Pricing Workbench demo |

## 3 · Performance & Oversight

| Workshop use case | Status | Where in the demo |
|---|---|---|
| Indemnity (factorial testing) | 🧭 tile | Governance |
| Claims pricing (quote vs actuals) | 🧭 tile | Insight (+ Pricing Workbench bridge) |
| Reserves management | ✅ LIVE | Reserve-adequacy card on every open claim: suggested £, gap, plain reason, own-book benchmark (finds the EoW 1.25× / £21.2M story) |
| Evidencing repairs completed | 🧭 tile (foundation ✅) | Fraud & SIU tile; the live photo-severity agent (cc:900003) is the foundation |
| Quality assurance | ✅ LIVE | Governance › QA — 6 codified checks on 100% of the book; found 176 fast-tracked high-fraud claims |
| Surfacing the right metrics | ✅ LIVE | Control Tower (targets/RAG/daily brief/predictive SLA) + live Lakeview dashboard |
| Natural language data exploration | ✅ LIVE | Genie embedded (Insight + Broker Portal) |
| Claim fraud identification attribution | 🧭 tile (first cut ✅) | Fraud & SIU; model card top signals |
| Claims performance insight agent (5 sub-ideas) | ✅ LIVE across the app | FNOL 360 · triage/auto-close · decision support · predictive SLA · supplier scorecard |

## Cross-cutting themes (the five the workshop kept repeating)

Call-transcript processing — **LIVE** (batch; co-pilot on roadmap) · Customer comms &
personalisation — **LIVE** (first increment) · NL data interface — **LIVE** (Genie ×2) ·
Claim routing & triage — **LIVE** · Supplier accountability layer — **LIVE**.

## In one sentence

Everything above is either working software or a visible tile on the exact screen where
it will live — walk the app, and the roadmap IS the workshop output. Suggested next
builds together: complaints & FOS lens, live call co-pilot, customer portal, repairer-
steering actions.
