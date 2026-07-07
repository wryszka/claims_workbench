# Claims Intelligence Workbench — the three‑beat demo (with strong examples)

**The spine (memorise this):** one platform puts **all the data together** → which is the only way you can **see and govern** the whole process → which is the only way you can **safely automate it with AI**. *You can't govern what's scattered; you can't safely deploy AI you can't prove.* **FNOL is where all three converge — so it's the centrepiece.**

Three heroes thread through every beat:
- **cc:900002** — home escape‑of‑water, £420, clean → **auto‑closes & pays** (the straight‑through win).
- **cc:900001** — motor, £8,500, fraud 74, 2 prior, speeding → **escalated to SIU** (the hard claim).
- **cc:900003** — motor, reported £600 "minor knock" but the **photo is severe** → escalates on the image alone (the discrepancy).

---

## BEAT 1 — Platform: all the data together at FNOL  *(~7 min, the centrepiece)*

**The point:** a claim lands and, because every source lives on one platform, the full picture assembles in real time — so you can answer the only question that matters at FNOL: **close now, or escalate to a human?**

**Screen:** **Handler → 🛰️ FNOL 360** (then **Ingestion** for the breadth).

**Strong example — run all three heroes:**
1. **FNOL 360 → cc:900002.** Decision headline: **CLOSE — auto‑close & pay**. Scroll the **"every source converging on this claim"** list — Guidewire ClaimCenter + PolicyCenter, claims history (CUE), fraud intelligence, weather, the leak‑sensor corroboration — all assembled. Then **Decision factors**: every check green. *"The clean claim pays itself — no human touched it."*
2. **FNOL 360 → cc:900001.** Headline: **ESCALATE — route to a human**. Same sources, but now fraud 74, 2 prior (CUE), 95‑in‑a‑50 (telematics). **Decision factors**: 4 of 11 checks failed → routed to SIU, each with its value. *"Same machinery, opposite outcome — and you can see exactly why."*
3. **FNOL 360 → cc:900003.** The model said pay_direct and the band would auto‑close the £600 claim — but the **photo (R7) alone** holds it. *"Every structured signal said pay; the photo said otherwise."*
4. **Ingestion** — show the **source map**: ~9 live feeds + the exotic roadmap set (CUE, MIAFTR/MID, Cifas/IFB, smart‑home leak sensor, connected‑car telemetry, call transcripts, dashcam, DVLA, geospatial/flood, repair pricing). *"Every channel, every format, every latency — one governed lakehouse."*

**Anchor line:** *"A claims system is a system of record, not a system of intelligence. The lakehouse is the only thing that sits across every source and serves it live at the moment of the call. The more you bring together at FNOL, the more you can safely decide — you raise the auto‑close rate by adding data, not loosening the threshold."*

---

## BEAT 2 — Governance: control of the process  *(~3–4 min)*

**The point:** now it's all on one platform you can **see and govern** the whole thing — live visibility, AI/BI, and every decision + reason audited. And that's what *unblocks* automation.

**Screen:** **Control Tower** → **Insight** (live Databricks dashboard + Genie) → **Data & Governance → Governance**.

**Strong example:**
1. **Control Tower** — the four vital signs (straight‑through %, aged/SLA, recovery, under‑reserved), the **Daily brief**, and the **auto‑close risk‑appetite dial** (drag it → the book re‑segments live).
2. **Insight** — the **live Lakeview dashboard** (your team edits it; Genie answers it in plain English). *"Self‑serve MI — not a hand‑built screen."*
3. **Governance →** *What's collected* (PII / SECRET tiers, masking, CMK) · *AI agents & reasoning audit* (every agent's reasoning on the heroes, **regulator‑viewable**) · *Claim track* (lifecycle + docs received/missing + completeness % — also **the auditor's single view**, and closed claims carry a PDF closure package) · *Fair outcomes* (Consumer‑Duty consistency + run the reviewer on a claim) · *Vulnerability* (**one governed standard in UC** — definitions, flags, the handling protocol for humans and the guidance agents must follow; click an open flagged claim → the protocol appears on the claim in Work a claim).

**Anchor line:** *"This is what lets you actually automate — you can't safely auto‑decide a claim you can't prove was fair. Governance isn't the brake; it's the enabler. The real blocker to auto‑closing claims is conduct fear — 'can we defend this to the FCA?' — and this removes it."*

---

## BEAT 3 — AI agents: enrich the process and assist the humans  *(~3–4 min)*

**The point:** on the governed platform, agents do the heavy lifting — assemble the dossier, give a second opinion, catch fraud, flag recovery — **helping handlers, not replacing them.**

**Screen:** **Claims AI** → **Work a claim** (second opinions + reasoning trail + the claim record).

**Strong example:**
1. **Claims AI** — "Models decide → Agents reason → Experts review → every step audited." Click the pinned **"Does the photo on cc:900003 match the reported account?"** → the **Fraud agent** answers live, citing the severe photo vs the £600 report. Then show the **bench** — each tile jumps to where that agent actually works in the app.
2. **Work a claim → cc:900001** — the **orchestrated brief**, a **Second opinion** (Reserving Actuary / Loss Adjuster on demand), the **agent reasoning trail**, and **📄 Generate claim record (PDF)** — the one audit‑ready document with everything in one place.

**Anchor line (safety):** *"The models and rules decide; the agents advise and challenge. The agent can pull a claim out for review — it can never push one through. It can only ever add caution. The human always acts. That asymmetry is what makes a high auto‑close rate defensible."*

---

## BEAT 4 (optional) — Broker Portal: broker calls off the helpline  *(~3 min)*

**The pain (say this first):** *"In a typical claims operation, up to ~70% of inbound handler calls are about claims already in flight — status chasing, not new losses — and brokers ringing on their clients' behalf are a big slice of it. Calls that don't change the claim, only relay where it is."*

**Screen:** **🤝 Broker Portal** (sidebar, under Distribution).

**Run:**
1. Open **Broker Portal** — read the story card: the helpline problem and the self-service answer, with the four governance badges.
2. **Sign in as Aldgate Risk Partners** (mock login — any broker card works). Point at the dark header: signed-in broker + the **🔒 ROW-FILTERED VIEW** badge.
3. **Governance line:** this session reads `v_broker_aldgate_claims` — a Unity Catalog **view filtered to this broker's book**, broker-safe columns only (**no fraud indicators, no handler fields**). For a technical audience, click *"See the view in Unity Catalog ↗"*.
4. **Your open claims:** search a client name, click a row — *"what happens next"* in plain English. That line is the exact question the broker used to ring about.
5. **Your book at a glance** — stage / time-open / peril bars: the broker's mini-MI without an analyst.
6. **Genie:** ask *"my open claims over £10,000"*. Line: in production this Genie space sits over the row-filtered view, so answers can only come from the broker's own book.
7. Close on the **coming-soon tiles** — document upload, bordereaux, status notifications, API / Delta Sharing to broker platforms (Acturis, Applied): the phase-2 roadmap.

**If asked "how do real brokers log in?":** SSO/OAuth into the portal; identity maps to a Unity Catalog **ROW FILTER** (`is_account_group_member`) on **one** view — the three per-broker views in the demo are the mock of exactly that — plus **column masks** for internal fields. Broker access is audited like everything else.

**Numbers if probed:** three fictional brokers hold ~40/30/20% of the book (~10% direct), assigned deterministically from `hash(policy_number)` — the split survives demo resets. Aldgate ≈ 3,469 open claims.

---

## Close — turn it into a discovery question
*"That's the arc: one platform brings the data together, which lets you govern the process, which lets you safely automate it with AI — and FNOL is where all three pay off at once. Where would you start — is it the data‑assembly problem at FNOL, the governance/automation blocker, or the agent assist for your handlers?"*

---

## If a practitioner probes (anchor facts)
- The **front of the claim decides its cost** — triage/reserve/routing in the first hours dominate the outcome.
- **Leakage** is the most controllable cost lever — mostly self‑inflicted, up to ~10% of spend.
- **Reserve adequacy rolls straight into pricing and capital** — mis‑reserve once, mis‑price and mis‑hold capital.
- The **fraud hit‑rate gap is pure leakage** — good is 3–4%, weak <1%.
- **Governance is what unblocks automation** — automation *and* governance together is the unlock.
- **"Is the AI deciding?"** → No. A deterministic engine (data + ML + rules + your risk‑appetite threshold) decides what's safe to auto‑close; the agent second‑opinions and can only add caution.
- **"Does it replace Guidewire?"** → No. Guidewire stays the system of record; this is the system of intelligence downstream of it.

---

## Appendix A — FNOL 360 (how the screen works)
For the selected claim it shows three stacked blocks: **the decision** (close vs escalate + model call/confidence) → **every source converging** (each feed with its system, format · latency badge, the real value it carried, and a live/illustrative tag) → **decision factors** (every check, its value, and pass/fail, plus the model's plain‑English drivers). It reads top‑to‑bottom as *sources in → decision out → exactly why.*

## Appendix B — what we ingest (live vs roadmap)
**Live in the demo:** Guidewire ClaimCenter (claim, incident, exposure, contact) · PolicyCenter (policy) · fraud signals · weather/peril · motor telematics · documents & photos (Auto Loader + vision FM).
**Exotic roadmap feeds shown on the source map** (the "bring it all together" breadth): cross‑insurer claims history (**CUE**), motor anti‑fraud & theft (**MIAFTR / MID**), fraud intelligence (**Cifas / IFB**), **smart‑home leak sensor** (IoT stream), **connected‑car crash telemetry**, **FNOL call transcripts** (speech‑to‑text + LLM), **dashcam / CCTV video**, **DVLA** vehicle & keeper, **geospatial & flood maps** (JBA / what3words), **repair & parts pricing** (Audatex / Glass's).
The story: most insurers can't bring these together — Databricks ingests every format (structured, events, unstructured, geospatial) at every latency (batch, streaming, file‑arrival, API) into one governed place, and runs the AI on it at FNOL.

---

## Pre‑flight (Tuesday morning)
1. Run **↺ Reset demo** so all dates read current.
2. Confirm the app is **RUNNING** and the three heroes load instantly (cache on).
3. Open the tabs you'll use: FNOL 360 (cc:900001), Control Tower, Governance, Claims AI.

*Pairs with TUESDAY_NARRATIVE.md (the timed talk‑track). This sheet is the build + the strong example per beat.*
