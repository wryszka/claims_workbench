# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 14 · Image-severity AI function (Phase 12)
# MAGIC
# MAGIC **Light, both perils.** No CV training pipeline — a vision-capable foundation model
# MAGIC (`databricks-claude-sonnet-4-6`) infers damage severity (minor / moderate / severe)
# MAGIC + a short rationale from the claim photo. We seed a handful of sample accident /
# MAGIC damage images (in the `claim_images` Volume); most claims carry no image (null-safe).
# MAGIC Results land in `claim_image_severity`, read by `fn_image_severity` and rule **R7**.
# MAGIC
# MAGIC > The **discrepancy hero** cc:900003: reported as a minor £600 knock, but the photo
# MAGIC > shows a severe crash — R7 fires, the Fraud/Challenge agents cite it, and it escalates.

# COMMAND ----------

# MAGIC %pip install requests --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import base64, json, time
import requests
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema (fixed)")
dbutils.widgets.text("fm_endpoint", "databricks-claude-sonnet-4-6", "Vision foundation-model endpoint")
catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
fm = dbutils.widgets.get("fm_endpoint").strip()


def tbl(t):
    return f"`{catalog}`.`{schema}`.{t}"


VOL = f"/Volumes/{catalog}/{schema}/claim_images"
W_URL = "https://commons.wikimedia.org/wiki/Special:FilePath"
print(f"[target] {catalog}.{schema} | volume {VOL} | fm {fm}")

# COMMAND ----------

w = WorkspaceClient()
HOST = w.config.host.rstrip("/")


def infer_severity(image_path):
    """Call the vision FM on a base64 image → (severity, rationale)."""
    with open(image_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    prompt = ("You are a motor/property claims damage assessor. Classify the damage in this "
              "photo as exactly one word — minor, moderate or severe — then ' | ' then a one-sentence reason. "
              "Reply with only that.")
    body = {"messages": [{"role": "user", "content": [
        {"type": "text", "text": prompt},
        {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + b64}}]}],
        "max_tokens": 80, "temperature": 0}
    r = requests.post(f"{HOST}/serving-endpoints/{fm}/invocations",
                      headers={**w.config._header_factory(), "Content-Type": "application/json"},
                      json=body, timeout=120)
    r.raise_for_status()
    txt = r.json()["choices"][0]["message"]["content"].strip()
    sev = "moderate"
    for s in ("severe", "moderate", "minor"):
        if s in txt.lower():
            sev = s
            break
    rationale = txt.split("|", 1)[1].strip() if "|" in txt else txt
    return sev, rationale[:300]

# COMMAND ----------

# Seed map: claim → (image file in the Volume, public display URL). The two heroes use the
# severe car crash; a large home-fire claim gets the fire photo (severe + consistent).
# cc:900002 (clean auto-close hero) deliberately has NO photo — keeps its null-safe path.
big_home = (spark.table(tbl("silver_claims_enriched"))
            .where("peril_type LIKE 'home%' AND total_incurred > 80000")
            .select("claim_public_id").limit(1).collect())
SEED = [
    {"claim": "cc:900003", "file": "motor_severe.jpg", "url": f"{W_URL}/Car_crash_1.jpg"},   # discrepancy hero
    {"claim": "cc:900001", "file": "motor_severe.jpg", "url": f"{W_URL}/Car_crash_1.jpg"},   # severe + consistent (£8.5k)
]
if big_home:
    SEED.append({"claim": big_home[0]["claim_public_id"], "file": "home_fire.jpg", "url": f"{W_URL}/House_fire.jpg"})

rows = []
for s in SEED:
    try:
        sev, why = infer_severity(f"{VOL}/{s['file']}")
        rows.append((s["claim"], s["url"], s["file"], sev, why))
        print(f"  {s['claim']}: {sev} — {why[:80]}")
    except Exception as e:
        print(f"  {s['claim']}: vision inference failed ({str(e)[:120]})")

schema_t = StructType([
    StructField("claim_public_id", StringType()), StructField("image_url", StringType()),
    StructField("image_file", StringType()), StructField("severity", StringType()),
    StructField("rationale", StringType())])
df = (spark.createDataFrame(rows, schema_t).withColumn("assessment_ts", F.current_timestamp()))
(df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
   .saveAsTable(tbl("claim_image_severity")))
spark.sql(f"ALTER TABLE {tbl('claim_image_severity')} SET TBLPROPERTIES "
          f"('project'='claims_workbench','layer'='gold','wb_owner'='wryszka')")
n = spark.table(tbl("claim_image_severity")).count()
print(f"\nclaim_image_severity written: {n} rows")
spark.sql(f"SELECT claim_public_id, severity, left(rationale,70) r FROM {tbl('claim_image_severity')}").show(truncate=False)

# The discrepancy hero must read as severe (vs its £600 report) so R7 fires.
h3 = spark.table(tbl("claim_image_severity")).where("claim_public_id='cc:900003'").collect()
assert h3 and h3[0]["severity"] == "severe", f"cc:900003 image should infer 'severe' (got {h3[0]['severity'] if h3 else None})"
print("[OK] cc:900003 photo inferred SEVERE — R7 will fire vs the £600 report.")
dbutils.notebook.exit(json.dumps({"rows": int(n)}))
