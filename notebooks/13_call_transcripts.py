# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 13 · Call transcripts & comms drafting
# MAGIC
# MAGIC Two governed assets behind the **Calls & Comms** card in Work-a-claim:
# MAGIC
# MAGIC 1. **`bronze_call_transcripts`** — synthetic but realistic call transcripts.
# MAGIC    Deterministic (no randomness), tagged by `source` so the SAME pipeline handles
# MAGIC    any transcript stream: `claims_helpline` today, `sales` included to prove it,
# MAGIC    broker line / renewals tomorrow.
# MAGIC 2. **`gold_call_insights`** — REAL batch LLM analysis of every transcript via
# MAGIC    `ai_query` on the Foundation Model API (Claude): summary, sentiment, intent,
# MAGIC    missing information, vulnerability cues, follow-ups, complaint risk. This is
# MAGIC    the platform's batch-inference story — no bespoke serving infra, pay-per-token,
# MAGIC    scale to zero.
# MAGIC
# MAGIC Also creates the **`gold_comms_drafts`** shell — the audit table the app writes
# MAGIC every AI-drafted customer communication (and its human approval) into.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog (blank = current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema")
# fm_endpoint must be ai_query/batch-enabled: claude-sonnet-4-5 works; claude-sonnet-5 does NOT.
dbutils.widgets.text("fm_endpoint", "databricks-claude-sonnet-4-5", "FM endpoint for analysis")
catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
FM = dbutils.widgets.get("fm_endpoint").strip() or "databricks-claude-sonnet-4-5"
def tbl(n): return f"{catalog}.{schema}.{n}"
print(f"target: {catalog}.{schema} · FM: {FM}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · bronze_call_transcripts — deterministic synthetic calls
# MAGIC Heroes get linked calls (cc:900001/2/3); plus vulnerability-rich, broker-chase and
# MAGIC FNOL calls, and two `sales` calls to prove the pipeline is source-agnostic.

# COMMAND ----------

_T = [
    # (call_id, claim_public_id, source, caller_type, agent_id, days_after_report, duration_sec, transcript)
    ("CALL-9001-1", "cc:900001", "claims_helpline", "policyholder", "H-201", 0, 540, """Agent: Bricksurance claims, you're speaking with Dan. How can I help?
Caller: I need to report an accident. Someone went into the back of my car.
Agent: I'm sorry to hear that. Are you and everyone involved okay?
Caller: Yes, fine. Look, it happened maybe two, three weeks ago. I've been busy.
Agent: Okay — do you have the exact date? It matters for the claim.
Caller: Around the 12th. Or the 14th. One of those.
Agent: And the other driver's details — registration, insurer?
Caller: I didn't get them. It was chaos. But my neighbour does bodywork, he says it's about four grand.
Agent: Was it reported to the police, any photos from the scene?
Caller: No police, it was minor. I have photos of my car from yesterday.
Agent: Were there any witnesses or dashcam footage?
Caller: My mate was with me, he saw everything.
Agent: Alright. I've logged the claim. Given the delay in reporting and the missing third-party details, a colleague may call you back for more information.
Caller: Why? It's a simple claim. How fast do I get paid?"""),
    ("CALL-9001-2", "cc:900001", "claims_helpline", "policyholder", "H-214", 9, 410, """Agent: Bricksurance claims, Priya speaking.
Caller: I reported my accident over a week ago and nothing has happened. Nobody calls me back.
Agent: Let me look... I can see it's with our review team at the moment.
Caller: Review team? What is there to review? I gave you everything.
Agent: There are a few details outstanding — the third party's information and the exact incident date.
Caller: I told the last guy everything I know. This is a joke. My mate already started the repair.
Agent: I understand this is frustrating. I'd advise not to proceed with repairs before the assessment.
Caller: Too late. If this isn't sorted this week I'm going to the ombudsman and putting it all over social media.
Agent: I hear you. I'm escalating your case to a senior handler today and you'll get a call within 24 hours.
Caller: Fine. Last chance."""),
    ("CALL-9002-1", "cc:900002", "claims_helpline", "policyholder", "H-207", 0, 380, """Agent: Bricksurance claims, this is Sam. How can I help?
Caller: Hi, I want to report a claim. A storm last night blew tiles off my roof and cracked a window.
Agent: Sorry to hear that — is the house secure and is everyone safe?
Caller: All safe. I've put a tarp up. It's not huge, maybe fifteen hundred pounds of damage.
Agent: Do you have photos?
Caller: Yes, I took them this morning, I can upload them in the app.
Agent: Perfect. Date of the storm was last night, the 3rd?
Caller: Yes, overnight.
Agent: Your policy covers storm damage and your excess is 250 pounds. Based on what you've described this looks straightforward.
Caller: Great. How long does it usually take?
Agent: If the photos support the estimate, this can be approved for settlement very quickly — often the same week.
Caller: Brilliant, thank you Sam, really helpful."""),
    ("CALL-9003-1", "cc:900003", "claims_helpline", "policyholder", "H-201", 2, 460, """Agent: Bricksurance claims, Dan speaking.
Caller: Hi, you asked me to call about the photos on my claim. The bumper scrape.
Agent: Yes — thanks for calling. Our assessment flagged that the damage in the photo looks more extensive than the 600 pounds reported.
Caller: Well, the photo makes it look worse than it is. It's just a scrape and a small dent.
Agent: The image shows deformation around the wheel arch as well. Was there any previous damage?
Caller: ...There was an old ding there from a car park last year. I never claimed for it.
Agent: Okay, that's helpful context. We may send an engineer to inspect rather than settle off the photo.
Caller: Is that really necessary? I just want it sorted quickly.
Agent: It protects both of us — if the new damage is what you say, the inspection confirms it quickly.
Caller: Fine, send them round."""),
    ("CALL-1001", None, "claims_helpline", "policyholder", "H-214", 1, 720, """Agent: Bricksurance claims, Priya speaking.
Caller: (distressed) Our house caught fire last night. The kitchen is gone. We're at my sister's.
Agent: I'm so sorry. Take your time. Is everyone safe?
Caller: Yes... the kids are shaken. We can't go back in. The smoke is everywhere, all through the bedrooms.
Agent: You said you can't stay in the house — do you have somewhere for more than a few nights?
Caller: My sister's is tiny. We can't stay long. I don't know what we're supposed to do. I can't think straight.
Agent: That's exactly what we're here for. Your policy includes alternative accommodation and I can arrange an emergency payment today for essentials.
Caller: (crying) Thank you. I didn't sleep. My husband's medication was in the house too.
Agent: We'll arrange access with the fire officer for essentials like medication. I'm marking your claim as a priority — you'll have one named handler, and we'll call you every few days, you won't have to chase us.
Caller: Thank you. Please just... talk slowly if you send letters, everything is a blur right now.
Agent: Understood. We'll keep everything in plain language and always follow up by phone."""),
    ("CALL-1002", None, "claims_helpline", "broker", "H-207", 5, 300, """Agent: Bricksurance claims, Sam speaking.
Caller: Hi Sam, it's Priya Nair at Aldgate Risk Partners. Chasing an update on two of my clients' claims.
Agent: Morning Priya. Go ahead.
Caller: First one, motor claim for Henderson Logistics, reported about ten days ago. Any movement?
Agent: It's with the engineer, report due back Thursday.
Caller: Okay. Second — the warehouse escape of water. My client says nobody has been in touch this week.
Agent: The loss adjuster visit is booked for Monday. A letter went out yesterday.
Caller: Right. You know, half my Mondays are these calls. If I could see the status myself I wouldn't be ringing you.
Agent: Understood — I'll note the feedback. Anything else?
Caller: No, that's it. Thanks Sam."""),
    ("CALL-1003", None, "claims_helpline", "policyholder", "H-201", 3, 620, """Agent: Bricksurance claims, Dan speaking.
Caller: Hello... I got a letter about my claim, and it says to use the app to upload documents. I don't have a smartphone.
Agent: No problem at all — we can do everything by post or over the phone.
Caller: My grandson set up the online thing but I don't understand it. The letter had a code, QR something.
Agent: Please don't worry about the app. Can I take the details over the phone now, and I'll post you a freepost envelope for the documents?
Caller: Oh, that would be much better. I've been with you thirty years, it was never this complicated.
Agent: I understand. I'm putting a note on your file that you prefer phone and post — you shouldn't be pushed to the app again.
Caller: Thank you dear. And could you speak up a little?
Agent: Of course. Let's go through it slowly, step by step."""),
    ("CALL-1004", None, "claims_helpline", "policyholder", "H-214", 4, 480, """Agent: Bricksurance claims, Priya speaking.
Caller: Hi... it's about my claim, the leak in the bathroom. You've approved it, but the letter says I pay the excess first. It's 350 pounds.
Agent: That's right — the excess comes off the settlement.
Caller: The thing is, this is my third claim this year with the burst pipe and the break-in. I just don't have 350 right now. I'm on reduced hours since March.
Agent: Thank you for telling me — that's important. We have options: we can deduct the excess from the payout rather than asking you to pay upfront.
Caller: You can do that? The letter didn't say that.
Agent: We can. And given the circumstances I'll also ask a colleague to check whether an interim payment can be released for the emergency plumber invoice.
Caller: That would honestly be a lifesaver. It's been a horrible year.
Agent: I've noted it on your file. You'll hear from us within two days, and everything in writing will show the deduction option."""),
    ("CALL-2001", None, "sales", "customer", "S-102", 0, 420, """Agent: Bricksurance renewals, Tom speaking.
Caller: Hi, I got my renewal quote and it's gone up ninety pounds. I've not claimed. I want to know why.
Agent: Let me look. Part of it is market-wide repair cost inflation; part is a change in your postcode's flood mapping.
Caller: Flood mapping? I'm on a hill.
Agent: I understand. I can re-run the quote with an updated rebuild figure and a higher voluntary excess — that often brings it down.
Caller: What would 500 excess do?
Agent: That brings the premium four pounds below last year's. I can also add home emergency cover for three pounds a month — given the age of your boiler it's worth considering.
Caller: Leave the boiler thing. Do the 500 excess.
Agent: Done. New documents will be with you today. Anything else?
Caller: No — you've saved the renewal, just about."""),
    ("CALL-2002", None, "sales", "customer", "S-105", 0, 360, """Agent: Bricksurance new business, Aisha speaking.
Caller: Hi, I'm buying my first flat and the solicitor says I need buildings insurance from exchange. I don't really know what any of it means.
Agent: Congratulations! Happy to explain. Buildings covers the structure — walls, roof, kitchen. Contents is your belongings.
Caller: The flat is leasehold, second floor.
Agent: Then the freeholder usually insures the building — you'd typically only need contents. Worth checking your lease before buying anything.
Caller: Oh! The other company just quoted me for both.
Agent: Check the lease first — I'd rather you buy the right thing than more things. Shall I quote contents only for now?
Caller: Yes please. That's really honest of you.
Agent: Quote is 9 pounds a month for 40,000 pounds of contents with accidental damage. I'll email it — no pressure, it holds for 30 days."""),
]

from pyspark.sql import functions as F, types as T
rows = [(c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7].strip()) for c in _T]
schema_t = T.StructType([
    T.StructField("call_id", T.StringType()),
    T.StructField("claim_public_id", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("caller_type", T.StringType()),
    T.StructField("agent_id", T.StringType()),
    T.StructField("days_after_report", T.IntegerType()),
    T.StructField("duration_sec", T.IntegerType()),
    T.StructField("transcript", T.StringType()),
])
df = spark.createDataFrame(rows, schema_t)
# Anchor call timestamps: claim-linked calls = report_date + offset; others = recent days.
silver = spark.table(tbl("silver_claims_enriched")).select("claim_public_id", "report_date")
df = (df.join(silver, "claim_public_id", "left")
        .withColumn("call_ts", F.expr(
            "CASE WHEN report_date IS NOT NULL THEN timestamp(date_add(report_date, days_after_report)) "
            "ELSE timestamp(date_sub(current_date(), pmod(abs(hash(call_id)), 14))) END"))
        .drop("report_date"))
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl("bronze_call_transcripts"))
spark.sql(f"COMMENT ON TABLE {tbl('bronze_call_transcripts')} IS "
          f"'Synthetic call transcripts (deterministic). source column makes the pipeline stream-agnostic: claims_helpline + sales today, broker/renewals tomorrow.'")
print(f"bronze_call_transcripts: {spark.table(tbl('bronze_call_transcripts')).count()} calls")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · gold_call_insights — REAL batch LLM analysis via ai_query
# MAGIC One SQL statement analyses every transcript on the Foundation Model API — the
# MAGIC platform's batch-inference pattern (no serving infra, pay-per-token).

# COMMAND ----------

_INSTR = (
    "You analyse insurer call transcripts. Reply with ONLY a JSON object, no markdown fences, keys: "
    "summary (2 sentences max), sentiment (positive|neutral|negative), intent (short phrase), "
    "missing_info (array of strings - required information NOT captured on the call, empty if none), "
    "vulnerability_cues (array of strings - signs of customer vulnerability per FCA guidance, empty if none), "
    "follow_up_actions (array of strings), complaint_risk (low|medium|high). Transcript follows:\\n\\n"
)
J_SCHEMA = ("summary string, sentiment string, intent string, missing_info array<string>, "
            "vulnerability_cues array<string>, follow_up_actions array<string>, complaint_risk string")
spark.sql(f"""CREATE OR REPLACE TABLE {tbl('gold_call_insights')} AS
 WITH raw AS (
   SELECT call_id, claim_public_id, source, caller_type, agent_id, call_ts, duration_sec, transcript,
          ai_query('{FM}', concat('{_INSTR}', transcript)) AS resp
   FROM {tbl('bronze_call_transcripts')})
 SELECT call_id, claim_public_id, source, caller_type, agent_id, call_ts, duration_sec, transcript,
        j.summary, j.sentiment, j.intent, j.missing_info, j.vulnerability_cues,
        j.follow_up_actions, j.complaint_risk,
        '{FM}' AS model_endpoint, current_timestamp() AS analysed_ts
 FROM raw
 LATERAL VIEW explode(array(from_json(regexp_extract(resp, '(?s)\\\\{{.*\\\\}}', 0), '{J_SCHEMA}'))) t AS j""")
spark.sql(f"COMMENT ON TABLE {tbl('gold_call_insights')} IS "
          f"'Batch LLM analysis of every call transcript via ai_query ({FM}): summary, sentiment, missing info, vulnerability cues, complaint risk. Source-agnostic.'")
chk = spark.sql(f"SELECT call_id, sentiment, complaint_risk, size(vulnerability_cues) vc, summary IS NOT NULL ok FROM {tbl('gold_call_insights')} ORDER BY call_id")
chk.show(20, truncate=False)
bad = spark.sql(f"SELECT count(*) c FROM {tbl('gold_call_insights')} WHERE summary IS NULL").collect()[0]["c"]
assert bad == 0, f"{bad} transcripts failed to parse — inspect resp"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · gold_comms_drafts — the comms audit shell
# MAGIC The app INSERTs every AI-drafted customer communication here (and the approval).
# MAGIC Left intact if it already exists — it's an audit trail.

# COMMAND ----------

if not spark.catalog.tableExists(tbl("gold_comms_drafts")):
    spark.sql(f"""CREATE TABLE {tbl('gold_comms_drafts')} (
        comm_id string, claim_public_id string, comm_type string, channel string,
        draft_text string, vulnerability_context string, model_endpoint string,
        status string, drafted_ts timestamp, approved_by string, approved_ts timestamp)
        COMMENT 'Audit trail of AI-drafted customer communications: every draft, the vulnerability guidance applied, and the human approval.'""")
    print("gold_comms_drafts shell created.")
else:
    print("gold_comms_drafts already exists — left intact (audit trail).")

import json
out = {"calls": spark.table(tbl("bronze_call_transcripts")).count(),
       "insights": spark.table(tbl("gold_call_insights")).count()}
dbutils.notebook.exit(json.dumps(out))
