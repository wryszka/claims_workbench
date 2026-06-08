# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 12 · Closure packages
# MAGIC
# MAGIC When a claim is **closed (resolved)**, we compile the complete file — summary,
# MAGIC lifecycle, the AI/triage decisions made on the way, documents, financials and any
# MAGIC agent reasoning — into a single **PDF closure package** and store it in a governed
# MAGIC **Unity Catalog Volume**. One artefact, everything in one place, kept on file.
# MAGIC
# MAGIC We don't generate one for every claim here — just a couple of real settled claims as
# MAGIC worked examples. They are registered in `gold_claim_packages` and surface in the app
# MAGIC (Governance → Claim track) marked **(closed)** with a download link.

# COMMAND ----------

# MAGIC %pip install fpdf2 databricks-sdk nest_asyncio --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, sys, json, asyncio
import nest_asyncio
nest_asyncio.apply()      # notebooks run inside a live event loop — allow asyncio.run()
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "", "Catalog (blank = current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse (blank = resolve)")
dbutils.widgets.text("claim_ids", "cc:162673,cc:197223", "Closed claims to package (comma-separated)")
catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
warehouse_id = dbutils.widgets.get("warehouse_id").strip()
claim_ids = [c.strip() for c in dbutils.widgets.get("claim_ids").split(",") if c.strip()]

os.environ["DATABRICKS_APP_NAME"] = "closure-packages"
os.environ["CATALOG_NAME"] = catalog
os.environ["SCHEMA_NAME"] = schema

# Resolve a SQL warehouse for the app's cache/SQL layer (config defaults to the serverless
# id, which doesn't exist on dev) BEFORE importing config.
if not warehouse_id:
    try:
        from databricks.sdk import WorkspaceClient as _WC0
        whs = list(_WC0().warehouses.list())
        warehouse_id = next((x.id for x in whs if "RUNNING" in str(x.state)), whs[0].id if whs else "")
    except Exception as e:
        print(f"warehouse resolution skipped: {str(e)[:120]}")
if warehouse_id:
    os.environ["WAREHOUSE_ID"] = warehouse_id

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_app = "/Workspace" + os.path.dirname(_ctx.notebookPath().get()).replace("/notebooks", "/app")
if _app not in sys.path:
    sys.path.insert(0, _app)
from server import claims_service as svc      # noqa: E402  — reuse the exact track the app shows

fq = f"`{catalog}`.`{schema}`"
VOL = "claim_closure_packages"
VOL_DIR = f"/Volumes/{catalog}/{schema}/{VOL}"

# COMMAND ----------

# 1) Governed Volume to hold the packages + a registry the app reads.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {fq}.{VOL} COMMENT 'Per-claim closure packages (PDF) generated when a claim is resolved.'")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq}.gold_claim_packages (
  claim_public_id STRING, file_name STRING, volume_path STRING,
  peril_type STRING, total_incurred DOUBLE, disposition STRING,
  size_bytes BIGINT, sections INT, generated_at TIMESTAMP
) USING DELTA COMMENT 'Registry of generated claim closure packages.'""")
print("volume + registry ready:", VOL_DIR)

# COMMAND ----------

from fpdf import FPDF

V, AMBER, RED, INK, MUTE = (16, 122, 87), (180, 83, 9), (185, 28, 28), (15, 23, 42), (100, 116, 139)

def _s(x):
    """Latin-1-safe text for the core PDF font."""
    if x is None:
        return ""
    t = str(x)
    for a, b in [("—", "-"), ("–", "-"), ("•", "*"), ("’", "'"), ("‘", "'"),
                 ("“", '"'), ("”", '"'), ("…", "..."), ("→", "->"), ("✓", "[done]")]:
        t = t.replace(a, b)
    return t.encode("latin-1", "replace").decode("latin-1")

def _gbp(v):
    try:
        return "£{:,.0f}".format(float(v))
    except Exception:
        return "—"

class Pkg(FPDF):
    def header(self):
        if self.page_no() == 1:
            return
        self.set_font("Helvetica", "", 8); self.set_text_color(*MUTE)
        self.cell(0, 6, _s("Bricksurance SE — Claim Closure Package"), align="L")
        self.cell(0, 6, _s(self.title), align="R", ln=1); self.ln(2)

    def footer(self):
        self.set_y(-12); self.set_font("Helvetica", "", 7); self.set_text_color(*MUTE)
        self.cell(0, 6, _s("Illustrative demo data — Bricksurance SE. Stored in Unity Catalog Volume."), align="L")
        self.cell(0, 6, f"Page {self.page_no()}", align="R")

def _W(pdf):
    return pdf.w - pdf.l_margin - pdf.r_margin

def section(pdf, title):
    pdf.ln(2); pdf.set_x(pdf.l_margin); pdf.set_font("Helvetica", "B", 12); pdf.set_text_color(*V)
    pdf.multi_cell(_W(pdf), 8, _s(title)); pdf.set_draw_color(220, 220, 228)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y()); pdf.ln(2)
    pdf.set_text_color(*INK)

def kv(pdf, k, v):
    pdf.set_x(pdf.l_margin)
    pdf.set_font("Helvetica", "", 10); pdf.set_text_color(*MUTE)
    pdf.cell(55, 6, _s(k))
    pdf.set_text_color(*INK); pdf.set_font("Helvetica", "B", 10)
    pdf.multi_cell(_W(pdf) - 55, 6, _s(v))

def build_pdf(cid, data):
    c = data.get("claim", {})
    disp = data.get("disposition")
    pdf = Pkg(); pdf.title = cid
    pdf.set_auto_page_break(auto=True, margin=16); pdf.add_page()

    # Cover
    pdf.set_font("Helvetica", "B", 20); pdf.set_text_color(*INK)
    pdf.cell(0, 12, _s("Claim Closure Package"), ln=1)
    pdf.set_font("Helvetica", "B", 13); pdf.set_text_color(*V)
    pdf.cell(0, 8, _s(f"{cid}   ·   CLOSED"), ln=1)
    pdf.set_font("Helvetica", "", 10); pdf.set_text_color(*MUTE)
    pdf.cell(0, 6, _s(datetime.now(timezone.utc).strftime("Compiled %Y-%m-%d %H:%M UTC")), ln=1)

    section(pdf, "1 · Claim summary")
    kv(pdf, "Peril / product", f"{c.get('peril_type','—')}  ·  {c.get('product','—')}")
    kv(pdf, "Status", c.get("claim_status", "—"))
    kv(pdf, "Location (postcode district)", c.get("postcode_district", "—"))
    kv(pdf, "Reported via", f"{c.get('report_channel','—')}  ·  {c.get('reporting_lag_days','—')} days after loss")
    kv(pdf, "Loss / report date", f"{c.get('loss_date','—')}  ->  {c.get('report_date','—')}")

    section(pdf, "2 · Financials")
    kv(pdf, "Total incurred", _gbp(c.get("total_incurred")))
    kv(pdf, "Reserve bracket", c.get("reserve_bracket", "—"))
    kv(pdf, "Recovery", ("up to " + _gbp(c.get("recoverable_amount")) + " (subrogation)") if c.get("recovery_flag") else "none")
    kv(pdf, "Days to settle", c.get("days_to_settle", "—"))
    kv(pdf, "Settlement date", c.get("settlement_date", "—"))

    section(pdf, "3 · AI & triage decisions")
    kv(pdf, "Model recommendation", f"{disp or c.get('triage_decision','—')}")
    kv(pdf, "Fraud score", f"{c.get('fraud_score','—')} / 100")
    kv(pdf, "Handler", f"{c.get('handler_id','—')}  ·  {c.get('handler_grade','—')}")

    section(pdf, "4 · Lifecycle")
    W = _W(pdf)
    for e in data.get("lifecycle", []):
        pdf.set_x(pdf.l_margin)
        pdf.set_text_color(*(V if e.get("status") == "done" else AMBER))
        pdf.set_font("Helvetica", "B", 10); pdf.cell(5, 6, _s("*"))
        pdf.set_text_color(*INK)
        pdf.multi_cell(W - 5, 6, _s(f"{e.get('stage','')}   {e.get('when','') or ''}"))
        pdf.set_font("Helvetica", "", 9); pdf.set_text_color(*MUTE)
        pdf.set_x(pdf.l_margin + 5); pdf.multi_cell(W - 5, 5, _s(e.get("detail", "")))
        pdf.set_text_color(*INK)

    section(pdf, "5 · Documents on file")
    pdf.set_font("Helvetica", "", 10)
    for d in data.get("documents", []):
        st = d.get("status", "")
        pdf.set_x(pdf.l_margin)
        pdf.set_text_color(*(V if st == "received" else AMBER if st == "awaited" else RED))
        pdf.cell(5, 6, _s("*")); pdf.set_text_color(*INK)
        pdf.cell(120, 6, _s(d.get("name", ""))); pdf.set_text_color(*MUTE)
        pdf.cell(40, 6, _s(st), ln=1); pdf.set_text_color(*INK)

    acts = data.get("actions", [])
    if acts:
        section(pdf, "6 · Agent reasoning")
        for a in acts:
            pdf.set_x(pdf.l_margin); pdf.set_font("Helvetica", "B", 10); pdf.set_text_color(*INK)
            pdf.multi_cell(W, 6, _s(a.get("actor", "")))
            pdf.set_x(pdf.l_margin); pdf.set_font("Helvetica", "", 9); pdf.set_text_color(*MUTE)
            pdf.multi_cell(W, 5, _s(a.get("detail", ""))); pdf.ln(1)
        pdf.set_text_color(*INK)

    gaps = data.get("gaps", [])
    section(pdf, f"{'7' if acts else '6'} · Outstanding items")
    pdf.set_font("Helvetica", "", 10)
    if gaps:
        for g in gaps:
            pdf.set_x(pdf.l_margin)
            pdf.set_text_color(*AMBER); pdf.cell(5, 6, _s("*")); pdf.set_text_color(*INK)
            pdf.multi_cell(W - 5, 6, _s(g))
    else:
        pdf.set_x(pdf.l_margin); pdf.set_text_color(*V)
        pdf.multi_cell(W, 6, _s("[done] Nothing outstanding — a complete, clean closed file."))
        pdf.set_text_color(*INK)

    fn = cid.replace(":", "_") + "_closure.pdf"
    nsec = 7 if acts else 6
    return bytes(pdf.output()), fn, nsec

# COMMAND ----------

import io
from databricks.sdk import WorkspaceClient
_w = WorkspaceClient()      # serverless blocks file:/tmp via dbutils.fs — write to the Volume via the Files API

rows = []
errors = []
for cid in claim_ids:
    try:
        data = asyncio.run(svc.claim_track(cid))
        if not data.get("found"):
            print(f"skip {cid}: not found"); errors.append(f"{cid}: not found"); continue
        pdf_bytes, fn, nsec = build_pdf(cid, data)
        vol_path = f"{VOL_DIR}/{fn}"
        _w.files.upload(vol_path, io.BytesIO(pdf_bytes), overwrite=True)
        size = len(pdf_bytes)
        c = data.get("claim", {})
        rows.append((cid, fn, vol_path, c.get("peril_type"),
                     float(c.get("total_incurred") or 0), data.get("disposition"),
                     int(size), int(nsec)))
        print(f"packaged {cid}: {fn} ({size:,} bytes, {nsec} sections)")
    except Exception as e:
        import traceback
        print(f"FAILED {cid}: {type(e).__name__}: {str(e)[:200]}")
        errors.append(f"{cid}: {type(e).__name__}: {str(e)[:160]}")
        traceback.print_exc()

# Register (replace rows for these claims).
if rows:
    from pyspark.sql import functions as F
    df = spark.createDataFrame(rows, "claim_public_id string, file_name string, volume_path string, peril_type string, total_incurred double, disposition string, size_bytes long, sections int").withColumn("generated_at", F.current_timestamp())
    ids = "', '".join(claim_ids)
    spark.sql(f"DELETE FROM {fq}.gold_claim_packages WHERE claim_public_id IN ('{ids}')")
    df.write.mode("append").saveAsTable(f"{fq}.gold_claim_packages")

display(spark.table(f"{fq}.gold_claim_packages"))

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"packaged": [r[0] for r in rows], "errors": errors, "volume": VOL_DIR}))
