# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 15 · Unstructured ingest + ingestion metadata
# MAGIC
# MAGIC Two things the Ingestion page needs:
# MAGIC
# MAGIC 1. **Unstructured ingest (the wow).** Claim photos / documents land in a UC
# MAGIC    Volume inbox. **Auto Loader** (`cloudFiles`) incrementally ingests every new
# MAGIC    file into `bronze_claim_documents`, then a **vision foundation model** extracts
# MAGIC    structured fields (damage severity, document type, a one-line summary) into
# MAGIC    `gold_document_extractions`, **joined back to the claim** by the reference in
# MAGIC    the filename. Most claims stacks can't put photos + structured data in one
# MAGIC    governed place — this does, and runs AI at the point of ingest.
# MAGIC
# MAGIC 2. **Governed ingestion metadata** so the app reads it via SQL (the app SP can't
# MAGIC    read the DLT event log over REST):
# MAGIC    * `gold_ingestion_sources` — the multi-source map (Guidewire CC/PC, fraud,
# MAGIC      weather enrichment, telematics/IoT, documents) with live row counts + an
# MAGIC      honest live/roadmap tag and the Databricks ingestion tool for each.
# MAGIC    * `gold_ingestion_quality` — the Lakeflow DLT expectations scorecard
# MAGIC      (passed/failed per rule), read once from the pipeline event log.

# COMMAND ----------

# MAGIC %pip install requests --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import base64, json, re, time
import requests
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema (fixed)")
dbutils.widgets.text("fm_endpoint", "databricks-claude-sonnet-4-6", "Vision foundation-model endpoint")
dbutils.widgets.text("pipeline_name", "claims_workbench_01_bronze_dlt", "Bronze DLT pipeline (for the quality scorecard)")
catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
fm = dbutils.widgets.get("fm_endpoint").strip()
pipeline_name = dbutils.widgets.get("pipeline_name").strip()


def tbl(t):
    return f"`{catalog}`.`{schema}`.{t}"


VOL_IMG = f"/Volumes/{catalog}/{schema}/claim_images"
VOL_INBOX = f"/Volumes/{catalog}/{schema}/claim_inbox"
VOL_CKPT = f"/Volumes/{catalog}/{schema}/ingest_checkpoints"
print(f"[target] {catalog}.{schema} | inbox {VOL_INBOX} | fm {fm}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Volumes + seed the inbox
# MAGIC Files are named with the claim reference (`cc-900003_…`) so the extraction step
# MAGIC can join the document straight back to the claim — exactly what a real FNOL
# MAGIC upload portal would carry in its metadata.

# COMMAND ----------

for v in ("claim_images", "claim_inbox", "ingest_checkpoints"):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.{v}")

# Seed the inbox from the damage photos we already hold (idempotent overwrite).
# A genuine portal would drop heterogenous files; we mimic two photos + a text report.
SEED_DOCS = [
    {"src": f"{VOL_IMG}/motor_severe.jpg", "name": "cc-900003_front_damage.jpg"},
    {"src": f"{VOL_IMG}/motor_severe.jpg", "name": "cc-900001_collision_scene.jpg"},
    {"src": f"{VOL_IMG}/home_fire.jpg",    "name": "cc-100018_fire_damage.jpg"},
]
for d in SEED_DOCS:
    try:
        dbutils.fs.cp(d["src"], f"{VOL_INBOX}/{d['name']}")
    except Exception as e:
        print(f"  seed {d['name']} skipped: {str(e)[:100]}")

# A non-image document (police report) — shows the inbox is format-agnostic.
report = ("METROPOLITAN POLICE — INCIDENT REPORT\nRef: cc-900001\n"
          "Two-vehicle collision at junction. Front-end impact to insured vehicle. "
          "Third party admitted fault at scene. No injuries reported. Vehicle recovered.\n")
dbutils.fs.put(f"{VOL_INBOX}/cc-900001_police_report.txt", report, overwrite=True)
print("inbox seeded:", [f.name for f in dbutils.fs.ls(VOL_INBOX)])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Auto Loader — incremental file ingest → `bronze_claim_documents`
# MAGIC `cloudFiles` (directory listing) picks up only NEW files each run, tracks schema,
# MAGIC and checkpoints — the same pattern whether it's three files or three million.

# COMMAND ----------

ckpt = f"{VOL_CKPT}/bronze_claim_documents"
(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "binaryFile")
   .option("cloudFiles.schemaLocation", ckpt)
   .load(VOL_INBOX)
   .select(
       F.col("path"),
       F.element_at(F.split(F.col("path"), "/"), -1).alias("file_name"),
       F.col("modificationTime").alias("ingested_at"),
       F.col("length").alias("bytes"),
       F.col("content"))
   .writeStream
   .option("checkpointLocation", ckpt)
   .trigger(availableNow=True)
   .toTable(tbl("bronze_claim_documents")))

# availableNow drains then stops; wait for it.
while spark.streams.active:
    [s.awaitTermination(5) for s in spark.streams.active]
spark.sql(f"ALTER TABLE {tbl('bronze_claim_documents')} SET TBLPROPERTIES "
          f"('project'='claims_workbench','layer'='bronze','wb_owner'='wryszka')")
ndocs = spark.table(tbl("bronze_claim_documents")).count()
print(f"bronze_claim_documents: {ndocs} files ingested via Auto Loader")
spark.sql(f"SELECT file_name, bytes FROM {tbl('bronze_claim_documents')} ORDER BY file_name").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · AI extraction at ingest — vision FM → `gold_document_extractions`

# COMMAND ----------

w = WorkspaceClient()
HOST = w.config.host.rstrip("/")


def claim_ref(file_name):
    m = re.search(r"cc-(\d+)", file_name)
    return f"cc:{m.group(1)}" if m else None


def doc_type(file_name):
    fl = file_name.lower()
    if fl.endswith((".jpg", ".jpeg", ".png")):
        return "photo"
    if "report" in fl or fl.endswith((".txt", ".pdf")):
        return "report"
    return "document"


def infer_photo(content_bytes):
    """Vision FM → (severity, summary)."""
    b64 = base64.b64encode(content_bytes).decode()
    prompt = ("You are a motor/property claims assessor. From this photo reply with exactly: "
              "one word severity (minor, moderate or severe), then ' | ', then a one-sentence "
              "description of the visible damage. Reply with only that.")
    body = {"messages": [{"role": "user", "content": [
        {"type": "text", "text": prompt},
        {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + b64}}]}],
        "max_tokens": 90, "temperature": 0}
    r = requests.post(f"{HOST}/serving-endpoints/{fm}/invocations",
                      headers={**w.config._header_factory(), "Content-Type": "application/json"},
                      json=body, timeout=120)
    r.raise_for_status()
    txt = r.json()["choices"][0]["message"]["content"].strip()
    sev = next((s for s in ("severe", "moderate", "minor") if s in txt.lower()), "moderate")
    summary = txt.split("|", 1)[1].strip() if "|" in txt else txt
    return sev, summary[:300]


rows = []
for d in spark.table(tbl("bronze_claim_documents")).collect():
    fn = d["file_name"]; dt = doc_type(fn); cid = claim_ref(fn)
    sev = None; summary = ""
    try:
        if dt == "photo":
            sev, summary = infer_photo(bytes(d["content"]))
        else:
            text = bytes(d["content"]).decode("utf-8", "ignore")
            summary = " ".join(text.split())[:300]
        print(f"  {fn}: type={dt} claim={cid} severity={sev}")
    except Exception as e:
        summary = f"extraction failed: {str(e)[:120]}"
        print(f"  {fn}: {summary}")
    rows.append((fn, dt, cid, sev, summary, "Auto Loader (cloudFiles) + vision FM"))

schema_t = StructType([
    StructField("file_name", StringType()), StructField("doc_type", StringType()),
    StructField("claim_public_id", StringType()), StructField("severity", StringType()),
    StructField("extracted_summary", StringType()), StructField("source_tool", StringType())])
ext = (spark.createDataFrame(rows, schema_t).withColumn("ingested_at", F.current_timestamp()))
(ext.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
   .saveAsTable(tbl("gold_document_extractions")))
spark.sql(f"ALTER TABLE {tbl('gold_document_extractions')} SET TBLPROPERTIES "
          f"('project'='claims_workbench','layer'='gold','wb_owner'='wryszka')")
print(f"\ngold_document_extractions: {ext.count()} rows")
spark.sql(f"SELECT file_name, doc_type, claim_public_id, severity, left(extracted_summary,60) s "
          f"FROM {tbl('gold_document_extractions')} ORDER BY file_name").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · `gold_ingestion_sources` — the multi-source map (live row counts + honest tags)

# COMMAND ----------

def cnt(t):
    try:
        return int(spark.table(tbl(t)).count())
    except Exception:
        return None

# (group, name, system, channel, format, latency, tool, table, status, note)
SRC = [
    ("System of record", "Guidewire ClaimCenter — claims", "Guidewire ClaimCenter", "broker / phone / digital",
     "structured", "batch / CDC", "Guidewire CDA", "bronze_gw_cc_claim", "live", "Core claim records (FNOL → settlement)."),
    ("System of record", "Guidewire ClaimCenter — incidents", "Guidewire ClaimCenter", "—",
     "structured", "batch / CDC", "Guidewire CDA", "bronze_gw_cc_incident", "live", "Loss/incident detail per claim."),
    ("System of record", "Guidewire ClaimCenter — exposures", "Guidewire ClaimCenter", "—",
     "structured", "batch / CDC", "Guidewire CDA", "bronze_gw_cc_exposure", "live", "Coverage exposures per claim."),
    ("System of record", "Guidewire ClaimCenter — contacts", "Guidewire ClaimCenter", "—",
     "structured", "batch / CDC", "Guidewire CDA", "bronze_gw_cc_contact", "live", "Claimants / third parties."),
    ("System of record", "Guidewire PolicyCenter — policies", "Guidewire PolicyCenter", "—",
     "structured", "batch / CDC", "Guidewire CDA", "bronze_gw_pc_policy", "live", "Policy terms behind each claim."),
    ("Risk & fraud", "Fraud signals", "Fraud engine", "—",
     "structured", "batch", "Lakeflow Connect", "bronze_fraud_signals_raw", "live", "Governed fraud score / flags."),
    ("Third-party enrichment", "Weather & peril", "Met Office / peril model", "—",
     "structured", "batch", "Lakeflow Connect", "bronze_weather_raw", "live", "Flood / wind / freeze risk by district."),
    ("Telematics & IoT", "Motor telematics", "Telematics provider", "—",
     "structured", "near-real-time", "Structured Streaming", "bronze_telematics", "live", "Speed / harsh-braking at incident (motor)."),
    ("Documents & photos", "Claim photos & reports", "FNOL upload portal", "digital",
     "unstructured", "file arrival", "Auto Loader + vision FM", "bronze_claim_documents", "live", "Photos / reports → AI-extracted at ingest."),
    ("FNOL channels", "Real-time FNOL stream", "Web / IVR / mobile", "digital / phone",
     "events", "streaming", "Structured Streaming / Kafka", None, "roadmap", "First-notice events as they happen."),
    ("Documents & photos", "Call transcripts", "Contact-centre", "phone",
     "unstructured", "file arrival", "Auto Loader + LLM", None, "roadmap", "Speech-to-text → entity extraction."),
    ("Third-party enrichment", "DVLA / vehicle data", "DVLA", "—",
     "structured", "API / batch", "Lakeflow Connect", None, "roadmap", "Vehicle keeper / write-off markers."),
]
src_rows = [(g, n, sy, ch, fmt, lat, tool, t, (cnt(t) if t else None), st, note)
            for (g, n, sy, ch, fmt, lat, tool, t, st, note) in SRC]
src_schema = StructType([
    StructField("source_group", StringType()), StructField("source_name", StringType()),
    StructField("system", StringType()), StructField("channel", StringType()),
    StructField("format", StringType()), StructField("latency", StringType()),
    StructField("databricks_tool", StringType()), StructField("table_name", StringType()),
    StructField("row_count", LongType()), StructField("status", StringType()),
    StructField("note", StringType())])
src_df = spark.createDataFrame(src_rows, src_schema)
(src_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
   .saveAsTable(tbl("gold_ingestion_sources")))
spark.sql(f"ALTER TABLE {tbl('gold_ingestion_sources')} SET TBLPROPERTIES "
          f"('project'='claims_workbench','layer'='gold','wb_owner'='wryszka')")
live = src_df.where("status='live'").count()
print(f"gold_ingestion_sources: {src_df.count()} sources ({live} live)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · `gold_ingestion_quality` — Lakeflow DLT expectations scorecard
# MAGIC Read once from the pipeline event log (this job runs with pipeline access); the
# MAGIC app then reads the persisted table via SQL. Falls back to quarantine-derived
# MAGIC counts if the event log is unavailable.

# COMMAND ----------

expectations = {}
try:
    hdr = w.config._header_factory()
    pipes = [p for p in w.pipelines.list_pipelines() if pipeline_name in (p.name or "")]
    pid = pipes[0].pipeline_id if pipes else None
    if pid:
        evs = requests.get(f"{HOST}/api/2.0/pipelines/{pid}/events?max_results=250",
                           headers=hdr, timeout=60).json().get("events", [])
        for e in evs:
            dq = (e.get("details", {}).get("flow_progress", {}) or {}).get("data_quality")
            if not dq:
                continue
            for ex in dq.get("expectations", []) or []:
                c = expectations.setdefault(ex.get("name"), {"passed": 0, "failed": 0})
                c["passed"] += int(ex.get("passed_records") or 0)
                c["failed"] += int(ex.get("failed_records") or 0)
    print(f"event-log expectations: {len(expectations)} rules from pipeline {pid}")
except Exception as e:
    print(f"event-log read failed ({str(e)[:100]}) — using quarantine-derived counts")

q_rows = []
if expectations:
    for name, c in sorted(expectations.items()):
        q_rows.append((name, int(c["passed"]), int(c["failed"]), "bronze (Lakeflow DLT)"))
else:
    # Portable fallback: passed = bronze count, failed = quarantined-for-that-reason.
    bclaim = cnt("bronze_gw_cc_claim") or 0
    bfraud = cnt("bronze_fraud_signals_raw") or 0
    qc = {r["quarantine_reason"]: int(r["n"]) for r in spark.sql(
        f"SELECT quarantine_reason, count(*) n FROM {tbl('bronze_quarantine_claims')} GROUP BY quarantine_reason").collect()}
    qf = {r["quarantine_reason"]: int(r["n"]) for r in spark.sql(
        f"SELECT quarantine_reason, count(*) n FROM {tbl('bronze_quarantine_fraud_signals')} GROUP BY quarantine_reason").collect()}
    q_rows = [
        ("valid_loss_cause", bclaim, qc.get("invalid_loss_cause", 0), "bronze claims"),
        ("fraud_score_range", bfraud, qf.get("fraud_score_out_of_range", 0), "bronze fraud signals"),
    ]
qual_schema = StructType([
    StructField("rule", StringType()), StructField("passed", LongType()),
    StructField("failed", LongType()), StructField("dataset", StringType())])
qual_df = (spark.createDataFrame(q_rows, qual_schema)
           .withColumn("refreshed_at", F.current_timestamp()))
(qual_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
   .saveAsTable(tbl("gold_ingestion_quality")))
spark.sql(f"ALTER TABLE {tbl('gold_ingestion_quality')} SET TBLPROPERTIES "
          f"('project'='claims_workbench','layer'='gold','wb_owner'='wryszka')")
tot = qual_df.agg(F.sum("passed").alias("p"), F.sum("failed").alias("f")).collect()[0]
pr = round(100 * (tot["p"] or 0) / max((tot["p"] or 0) + (tot["f"] or 0), 1), 2)
print(f"gold_ingestion_quality: {qual_df.count()} rules · pass-rate {pr}%")
qual_df.show(truncate=False)

# COMMAND ----------

# Light assertions — the page leans on these.
assert spark.table(tbl("gold_document_extractions")).where("claim_public_id='cc:900003' AND severity='severe'").count() >= 1, \
    "cc:900003 photo should extract as severe"
assert spark.table(tbl("gold_ingestion_sources")).where("status='live'").count() >= 6, "expect >=6 live sources"
assert spark.table(tbl("gold_ingestion_quality")).count() >= 2, "expect quality scorecard rows"
print("[OK] unstructured ingest + ingestion metadata ready.")
dbutils.notebook.exit(json.dumps({"docs": int(ndocs), "sources_live": int(live), "pass_rate": pr}))
