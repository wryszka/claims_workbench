"""Claims AI service layer — the real work behind the app.

Design rule (reliability): the structured panels come from DIRECT calls to the
UC functions (deterministic, fast, never LLM-parsed). The synthesis box comes
from the managed Supervisor (or, until it exists, the Context sub-agent) via the
cache-first wrapper. The HITL decision is a synchronous write to
gold_handler_decisions.
"""
import asyncio
import json
import logging
import math
import uuid

from utils import config
from utils.agent_cache import get_agent_response
from server.sql import execute_query

logger = logging.getLogger(__name__)
CAT, SCH = config.CATALOG, config.SCHEMA

# --- CCO framing constants (illustrative, documented) ---
HANDLER_COST_PER_CLAIM = 45.0     # £ fully-loaded handling cost avoided per auto-close
HANDLER_MINS_PER_CLAIM = 25       # handler-minutes freed per auto-close
PERIL_SLA = {"motor_tp": 30, "home_fire": 60}   # days; default 45
DEFAULT_SLA = 45
# Book targets for the RAG status on each tile.
TARGETS = {"pct_auto_closed": 15.0, "leakage_rate": 6.0, "avg_settle_days": 35.0,
           "fraud_refer_rate": 5.0, "sla_breach_pct": 10.0,
           "closed_within_target": 80.0, "reserve_adequacy": 95.0, "loss_ratio": 75.0}


def _rag(value, target, lower_is_better, amber_mult=1.5):
    """green / amber / red vs a target."""
    try:
        v, t = float(value), float(target)
    except Exception:
        return "info"
    if lower_is_better:
        return "green" if v <= t else ("amber" if v <= t * amber_mult else "red")
    return "green" if v >= t else ("amber" if v >= t * 0.6 else "red")


def _fq(table: str) -> str:
    return f"`{CAT}`.`{SCH}`.{table}"


def _esc(v: str) -> str:
    return (v or "").replace("'", "''")


# Runtime cache mode (flippable via the top-bar toggle); seeded from USE_CACHE.
_use_cache = config.USE_CACHE


def get_cache_mode() -> bool:
    return _use_cache


def set_cache_mode(value: bool) -> bool:
    global _use_cache
    _use_cache = bool(value)
    return _use_cache


# --------------------------------------------------------------------------
# Claim selector — open / recently-reported FNOL (vivid claim always present)
# --------------------------------------------------------------------------
async def list_claims(limit: int = 25) -> list[dict]:
    rows = await execute_query(f"""
        SELECT claim_public_id, peril_type, total_incurred, claim_status,
               cast(report_date AS string) AS report_date
        FROM {_fq('silver_claims_enriched')}
        WHERE claim_status IN ('open', 'under_investigation')
        ORDER BY report_date DESC LIMIT {int(limit)}
    """)
    ids = {r["claim_public_id"] for r in rows}
    if "cc:900001" not in ids:
        vivid = await execute_query(f"""
            SELECT claim_public_id, peril_type, total_incurred, claim_status,
                   cast(report_date AS string) AS report_date
            FROM {_fq('silver_claims_enriched')} WHERE claim_public_id = 'cc:900001'
        """)
        rows = vivid + rows
    # Pin the vivid claim first so it's the obvious demo selection.
    rows.sort(key=lambda r: r["claim_public_id"] != "cc:900001")
    return rows


async def _fn(fn: str, cid: str) -> dict:
    rows = await execute_query(f"SELECT to_json({_fq(fn)}('{_esc(cid)}')) AS r")
    return json.loads(rows[0]["r"]) if rows and rows[0].get("r") else {}


# --------------------------------------------------------------------------
# Structured panels — direct UC-function calls (run concurrently)
# --------------------------------------------------------------------------
_RESERVE_RANGE_BY_BRACKET = {"LOW": "under £2,000", "MEDIUM": "£2,000–£10,000",
                             "HIGH": "£10,000–£50,000", "LARGE": "over £50,000"}


async def get_panels(cid: str) -> dict:
    """All structured panels for a claim in ONE query, read from precomputed tables
    (silver + gold_claim_disposition + claim_image_severity). The triage decision and
    reserve bracket are already batch-scored, so the interactive view never calls the
    scale-to-zero model endpoints (that path stays for Try-a-claim). The fn_* UC
    functions remain for the agents' on-demand, cache-first calls."""
    rows = await execute_query(f"""
        SELECT s.peril_type, CAST(s.total_incurred AS double) total_incurred, s.report_channel,
               s.postcode_district, s.description_text, s.claim_status,
               CAST(s.fraud_score AS int) fraud_score, s.is_potential_fraud,
               CAST(coalesce(s.prior_claims_12m,0) AS int) prior_claims_12m,
               CAST(s.days_since_incident AS int) days_since_incident,
               CAST(s.reporting_lag_days AS int) reporting_lag_days,
               s.product, CAST(s.sum_insured AS double) sum_insured,
               CAST(s.policy_tenure_years AS double) policy_tenure_years,
               CAST(s.annual_premium AS double) annual_premium,
               s.recovery_flag, CAST(s.recoverable_amount AS double) recoverable_amount,
               CAST(s.speed_at_incident AS int) speed_at_incident,
               CAST(s.posted_speed_limit AS int) posted_speed_limit, s.harsh_braking,
               CAST(s.weather_risk_composite AS double) weather_risk_composite,
               s.flood_risk_score, s.wind_risk_score, s.freeze_risk_score, s.at_fault,
               s.policy_number, CAST(s.is_high_value AS int) is_high_value,
               upper(s.reserve_bracket) reserve_bracket, s.triage_decision,
               d.model_decision, CAST(d.model_confidence AS double) model_confidence,
               img.severity img_severity, img.rationale img_rationale, img.image_url img_url
        FROM {_fq('silver_claims_enriched')} s
        LEFT JOIN {_fq('gold_claim_disposition')} d ON s.claim_public_id = d.claim_public_id
        LEFT JOIN {_fq('claim_image_severity')} img ON s.claim_public_id = img.claim_public_id
        WHERE s.claim_public_id = '{_esc(cid)}'
    """)
    if not rows:
        return {"claim_public_id": cid, "__err": True}
    r = rows[0]
    fraud = int(r["fraud_score"]) if r.get("fraud_score") is not None else None
    prior = int(r.get("prior_claims_12m") or 0)
    lag = r.get("reporting_lag_days")
    # Triage reasons — same plain-English logic the UC function used, derived in Python.
    reasons = []
    if fraud is not None and fraud > 70:
        reasons.append(f"High fraud score ({fraud}/100)")
    if prior >= 2:
        reasons.append(f"{prior} prior claims in 12 months")
    if lag is not None and int(lag) > 14:
        reasons.append(f"Reported {int(lag)} days after the incident")
    if int(r.get("is_high_value") or 0) == 1:
        reasons.append("High-value claim (over GBP 10,000)")
    decision = r.get("model_decision") or r.get("triage_decision")
    conf = r.get("model_confidence")
    bracket = (r.get("reserve_bracket") or "").upper()
    rng = next((v for k, v in _RESERVE_RANGE_BY_BRACKET.items() if k in bracket), "")
    speed = int(r["speed_at_incident"]) if r.get("speed_at_incident") not in (None, "") else None
    limit = int(r["posted_speed_limit"]) if r.get("posted_speed_limit") not in (None, "") else None
    has_tel = speed is not None
    return {
        "claim_public_id": cid,
        "summary": {"peril_type": r["peril_type"], "total_incurred": r["total_incurred"],
                    "report_channel": r["report_channel"], "postcode_district": r["postcode_district"],
                    "incident_description": r["description_text"], "claim_status": r["claim_status"]},
        "triage": {"decision": decision, "confidence": conf, "top_reasons": reasons[:3]},
        "reserve": {"bracket": bracket or None,
                    "estimated_range": rng,
                    "rationale": "Predicted from reported amount, peril, prior history and handler grade."},
        "fraud": {"fraud_score": fraud, "fraud_flag": r.get("is_potential_fraud"),
                  "prior_claims_12m": prior, "days_since_incident": r.get("days_since_incident"),
                  "reporting_lag_days": lag},
        "policy": {"product": r.get("product"), "sum_insured": r.get("sum_insured"),
                   "policy_tenure_years": r.get("policy_tenure_years"),
                   "annual_premium": r.get("annual_premium"), "prior_claims_12m": prior},
        "recovery": {"recovery_flag": bool(r.get("recovery_flag")),
                     "recoverable_amount": r.get("recoverable_amount")},
        "telematics": {"has_telematics": has_tel, "speed_at_incident": speed,
                       "posted_speed_limit": limit,
                       "over_limit": bool(has_tel and limit is not None and speed > limit),
                       "excess_mph": max(0, (speed or 0) - (limit or 0)) if has_tel else 0,
                       "harsh_braking": bool(r.get("harsh_braking"))},
        "image": {"has_image": r.get("img_severity") is not None, "severity": r.get("img_severity"),
                  "rationale": r.get("img_rationale"), "image_url": r.get("img_url")},
        "extra": {"policy_number": r.get("policy_number"),
                  "weather_risk_composite": r.get("weather_risk_composite"),
                  "flood_risk_score": r.get("flood_risk_score"), "wind_risk_score": r.get("wind_risk_score"),
                  "freeze_risk_score": r.get("freeze_risk_score"), "prior_claims_12m": prior,
                  "at_fault": r.get("at_fault"), "reporting_lag_days": lag},
    }


# --------------------------------------------------------------------------
# Synthesis — Supervisor (or Context fallback) via the cache-first wrapper
# --------------------------------------------------------------------------
def _synthesis_endpoint() -> tuple[str, bool]:
    if config.ENDPOINT_SUPERVISOR:
        return config.ENDPOINT_SUPERVISOR, True
    return config.ENDPOINT_CONTEXT, False


async def get_synthesis(cid: str, use_cache: bool | None = None) -> dict:
    if use_cache is None:
        use_cache = _use_cache
    endpoint, is_supervisor = _synthesis_endpoint()
    prompt = (f"I have a new claim, {cid}. Give me one orchestrated handling brief that ties "
              f"together the triage decision, the reserve estimate, the fraud risk and the "
              f"policyholder context — what should the handler do and watch for?")
    payload = {"messages": [{"role": "user", "content": prompt}],
               "custom_inputs": {"claim_public_id": cid}}
    out = await asyncio.to_thread(get_agent_response, endpoint, payload, use_cache)
    resp = out.get("response", {})
    msgs = resp.get("messages", [])
    text = msgs[-1].get("content", "") if msgs else ""
    return {"text": text, "cache": out.get("cache"), "endpoint": endpoint,
            "supervisor": is_supervisor, "use_cache": use_cache}


# --------------------------------------------------------------------------
# HITL decision — synchronous write to gold_handler_decisions
# --------------------------------------------------------------------------
async def log_decision(cid: str, model_recommendation: str, model_confidence: float,
                       handler_action: str, override_flag: bool,
                       override_reason: str = "") -> dict:
    decision_id = "CL-" + uuid.uuid4().hex[:8].upper()
    try:
        handler = (await execute_query("SELECT current_user() AS u"))[0]["u"]
    except Exception:
        handler = "demo-handler"
    conf = float(model_confidence) if model_confidence is not None else "NULL"
    await execute_query(f"""
        INSERT INTO {_fq('gold_handler_decisions')}
          (decision_id, claim_public_id, model_recommendation, model_confidence,
           handler_action, override_flag, override_reason, handler_id, decision_ts)
        VALUES ('{decision_id}', '{_esc(cid)}', '{_esc(model_recommendation)}', {conf},
                '{_esc(handler_action)}', {str(bool(override_flag)).lower()},
                '{_esc(override_reason)}', '{_esc(handler)}', current_timestamp())
    """)
    ts = (await execute_query(
        f"SELECT date_format(decision_ts,'HH:mm') AS t FROM {_fq('gold_handler_decisions')} "
        f"WHERE decision_id = '{decision_id}'"))[0]["t"]
    return {"decision_id": decision_id, "claim_public_id": cid,
            "handler_action": handler_action, "model_recommendation": model_recommendation,
            "override_flag": bool(override_flag), "override_reason": override_reason,
            "handler_id": handler, "time": ts}


async def recent_decisions(limit: int = 20) -> list[dict]:
    return await execute_query(f"""
        SELECT decision_id, claim_public_id, model_recommendation, model_confidence,
               handler_action, override_flag, override_reason, handler_id,
               cast(decision_ts AS string) AS decision_ts
        FROM {_fq('gold_handler_decisions')} ORDER BY decision_ts DESC LIMIT {int(limit)}
    """)


# --------------------------------------------------------------------------
# Stage B — Ingestion (DLT status + data-quality evidence)
# --------------------------------------------------------------------------
PIPELINE_NAME = "claims_workbench_01_bronze_dlt"


_pipe_link_cache: dict | None = None


def _pipeline_link() -> dict:
    """Best-effort pipeline name/url for a deep-link. Cosmetic — the scorecard no
    longer depends on REST event-log access (the app SP can't read it). Cached for the
    process: the REST list_pipelines call is slow and the answer never changes."""
    global _pipe_link_cache
    if _pipe_link_cache is not None:
        return _pipe_link_cache
    from server.sql import _client
    try:
        w = _client()
        host = w.config.host.rstrip("/")
        pipes = [p for p in w.pipelines.list_pipelines() if PIPELINE_NAME in (p.name or "")]
        if pipes:
            d = w.pipelines.get(pipes[0].pipeline_id)
            state = str(d.state).replace("PipelineState.", "") if d.state else None
            _pipe_link_cache = {"pipeline_name": pipes[0].name, "state": state,
                                "pipeline_url": f"{host}/pipelines/{pipes[0].pipeline_id}"}
            return _pipe_link_cache
    except Exception:
        pass
    _pipe_link_cache = {"pipeline_name": PIPELINE_NAME, "state": None, "pipeline_url": None}
    return _pipe_link_cache


# Rule → (bronze table, DLT action) — known from 01_bronze_dlt_pipeline. expect_or_drop
# rows are quarantined; expect rows are tracked + retained (visible, not dropped).
_DQ_RULE_META = {
    "valid_loss_cause":   ("bronze_gw_cc_claim", "DROP → quarantine"),
    "fraud_score_range":  ("bronze_fraud_signals_raw", "DROP → quarantine"),
    "valid_policy_number":("bronze_gw_cc_claim", "track + retain"),
    "valid_report_channel":("bronze_gw_cc_claim", "track + retain"),
    "non_negative_amounts":("bronze_gw_cc_exposure", "track + retain"),
    "valid_policy_dates": ("bronze_gw_pc_policy", "track + retain"),
    "non_negative_speed": ("bronze_telematics", "track + retain"),
}
# Tables a viewer may inspect (whitelist — no arbitrary SQL).
_INSPECTABLE = {
    "bronze_gw_cc_claim": "Guidewire ClaimCenter — claims",
    "bronze_gw_pc_policy": "Guidewire PolicyCenter — policies",
    "bronze_fraud_signals_raw": "Fraud signals",
    "bronze_weather_raw": "Weather & peril enrichment",
    "bronze_telematics": "Motor telematics",
    "bronze_quarantine_claims": "Quarantine — claims",
}

# Medallion layers surfaced as freshness/row-count evidence (read via SQL).
_LAYERS = [
    ("Landing", "raw, pre-quality", "landing_gw_cc_claim"),
    ("Bronze", "governed + quality gate", "bronze_gw_cc_claim"),
    ("Silver", "enriched + joined", "silver_claims_enriched"),
    ("Gold", "analytics + disposition", "gold_claim_disposition"),
    ("Feature", "ML-ready", "feature_triage"),
]

# --- Per-dataset drill-down (Ingestion screen: click a feed → freshness,
# completeness, quality contract, errors held back, and who owns it). Ownership is
# recorded by data domain (source_group); the per-table config picks the freshness
# column, the quarantine sink and the key fields to profile for completeness. ---
_DATASET_OWNER = {
    "System of record": {"owner": "Head of Claims Operations", "steward": "Claims Data Engineering",
                         "sla": "Guidewire CDA · hourly batch (freshness target < 2h)", "pii": "Confidential — policyholder PII"},
    "Risk & fraud": {"owner": "Head of Counter-Fraud (SIU)", "steward": "Fraud Analytics",
                     "sla": "Daily provider feed", "pii": "Confidential — fraud indicators"},
    "Telematics & IoT": {"owner": "Motor Product Owner", "steward": "Telematics Platform Eng",
                         "sla": "Streaming · seconds", "pii": "Personal — vehicle location & speed"},
    "Third-party enrichment": {"owner": "Exposure Management", "steward": "Data Partnerships",
                               "sla": "Daily vendor refresh", "pii": "Public / reference"},
    "Documents & photos": {"owner": "Claims Operations", "steward": "ML Platform (vision)",
                           "sla": "On file arrival (Auto Loader)", "pii": "Confidential — images may carry PII"},
}
# table -> (preferred freshness column, quarantine sink or None, [key fields to profile])
_DATASET_CFG = {
    "bronze_gw_cc_claim": ("cda_batch_ts", "claims", ["claim_public_id", "policy_number", "total_incurred", "loss_cause", "report_channel", "loss_date", "report_date"]),
    "bronze_gw_pc_policy": (None, None, ["policy_number", "sum_insured", "policy_start_date", "policy_end_date"]),
    "bronze_fraud_signals_raw": (None, "fraud", ["claim_public_id", "fraud_score", "prior_claims_12m", "signal_source"]),
    "bronze_telematics": (None, None, ["claim_public_id", "speed_at_incident", "latitude", "longitude"]),
    "bronze_weather_raw": (None, None, ["postcode_district", "flood_risk_score", "wind_risk_score"]),
    "bronze_claim_documents": (None, None, ["claim_public_id", "file_name", "doc_type"]),
}
_TS_CANDIDATES = ["cda_batch_ts", "_bronze_ingested_at", "_ingested_at", "extracted_at", "ingest_ts", "data_vintage", "report_date"]


async def ingestion_dataset(key: str) -> dict:
    """Drill into one ingested feed: freshness, completeness, the quality contract it
    is held to, the rows quarantined, and who is accountable for it."""
    srcs = await execute_query(
        f"""SELECT source_name, system, format, latency, databricks_tool, table_name,
                   row_count, status, note, source_group
            FROM {_fq('gold_ingestion_sources')} WHERE table_name = '{_esc(key)}'""")
    if not srcs:
        return {"error": "dataset not found"}
    s = srcs[0]
    grp = s.get("source_group")
    ts_col, quar_src, key_fields = _DATASET_CFG.get(key, (None, None, []))
    out = {"source": s, "ownership": {**_DATASET_OWNER.get(grp, {}), "domain": grp},
           "freshness": None, "completeness": [], "expectations": [], "quarantine": None}
    # Columns present (for safe freshness/completeness selection).
    try:
        cols = await execute_query(f"DESCRIBE {_fq(key)}")
        colset = {c["col_name"] for c in cols if c.get("col_name") and not c["col_name"].startswith("#")}
        colnames = [c for c in colset if not c.startswith("_")]
    except Exception:
        colset, colnames = set(), []
    # Freshness — pick the best available timestamp column.
    fcol = ts_col if (ts_col and ts_col in colset) else next((c for c in _TS_CANDIDATES if c in colset), None)
    try:
        if fcol:
            fr = (await execute_query(f"SELECT cast(max(`{fcol}`) AS string) ts, count(*) n FROM {_fq(key)}"))[0]
            out["freshness"] = {"column": fcol, "last": fr.get("ts"), "rows": int(fr.get("n") or 0)}
        else:
            n = (await execute_query(f"SELECT count(*) n FROM {_fq(key)}"))[0]
            out["freshness"] = {"column": None, "last": None, "rows": int(n.get("n") or 0)}
    except Exception:
        pass
    # Completeness on the key fields (fallback to the first few columns).
    fields = [f for f in (key_fields or []) if f in colset] or colnames[:5]
    if fields:
        try:
            sel = ", ".join(f"sum(CASE WHEN `{f}` IS NULL THEN 1 ELSE 0 END) AS n{i}" for i, f in enumerate(fields))
            tot = (await execute_query(f"SELECT count(*) c, {sel} FROM {_fq(key)}"))[0]
            c = int(tot.get("c") or 1)
            for i, f in enumerate(fields):
                nulls = int(tot.get(f"n{i}") or 0)
                out["completeness"].append({"field": f, "populated": round(100 * (c - nulls) / max(c, 1), 2), "nulls": nulls})
        except Exception:
            pass
    # The quality contract that applies to this feed (DLT expectations, mapped by table).
    try:
        for r in await execute_query(f"SELECT rule, passed, failed FROM {_fq('gold_ingestion_quality')}"):
            meta = _DQ_RULE_META.get(r["rule"], (None, "track + retain"))
            if meta[0] == key:
                p, f = int(r["passed"]), int(r["failed"])
                out["expectations"].append({"name": r["rule"], "action": meta[1], "passed": p, "failed": f,
                                            "pass_rate": round(100 * p / max(p + f, 1), 2)})
    except Exception:
        pass
    # Rows quarantined off this feed (held back, not dropped).
    if quar_src:
        tbl = "bronze_quarantine_fraud_signals" if quar_src == "fraud" else "bronze_quarantine_claims"
        try:
            rs = await execute_query(f"SELECT quarantine_reason reason, count(*) n FROM {_fq(tbl)} GROUP BY quarantine_reason ORDER BY n DESC")
            out["quarantine"] = {"source": quar_src, "table": tbl,
                                 "total": sum(int(r["n"]) for r in rs),
                                 "reasons": [{"reason": r["reason"], "count": int(r["n"])} for r in rs]}
        except Exception:
            pass
    # Freshness / SLA-compliance history (the Freshness tab) — synthetic but deterministic.
    out["freshness_history"] = _freshness_history(key, (out.get("freshness") or {}).get("rows") or 0)
    # Data preview (the Data Preview tab) — a live sample of rows straight from the table.
    try:
        cols = await execute_query(f"DESCRIBE {_fq(key)}")
        cn = [c["col_name"] for c in cols if c.get("col_name") and not c["col_name"].startswith("#")
              and not c["col_name"].startswith("_")][:8]
        srows = await execute_query(f"SELECT {', '.join('`' + c + '`' for c in cn)} FROM {_fq(key)} LIMIT 12")
        out["sample"] = {"columns": cn, "rows": srows}
    except Exception:
        out["sample"] = {"columns": [], "rows": []}
    return out


_LATE_ASSET = "bronze_weather_raw"   # one feed flagged Late for the demo (vendor batch slip)


def _freshness_history(table: str, rows: int) -> list[dict]:
    """SLA-compliance history across reporting periods (deterministic synthetic — the
    real CDA feed only carries the latest batch, so prior periods are illustrative)."""
    import zlib
    h = zlib.crc32(table.encode())
    late = (table == _LATE_ASSET)
    periods = [
        ("2025-Q1", "03/04/2025", "02/04/2025", "1d early", "On Time"),
        ("2025-Q2", "03/07/2025", "02/07/2025", "1d early", "On Time"),
        ("2025-Q3", "03/10/2025", "02/10/2025", "1d early", "On Time"),
        ("2025-Q4", "03/01/2026", "11/01/2026" if late else "02/01/2026",
         "8d late" if late else "1d early", "Late" if late else "On Time"),
    ]
    hist = []
    for i, (per, dl, act, lat, st) in enumerate(periods):
        rws = rows if i == 3 else int(rows * (0.55 + ((h >> (i * 4)) % 45) / 100))
        dqp = 100.0 if ((h >> i) % 4) else round(98.4 + ((h >> (i + 2)) % 15) / 10, 1)
        hist.append({"period": per, "sla_deadline": dl, "actual_arrival": act, "lateness": lat,
                     "status": st, "rows": rws, "dq_pass": dqp})
    return hist


async def ingestion_assets() -> dict:
    """The source-asset list (Solvency-II-style): one row per tracked Unity Catalog table
    with source system, row count, DQ pass rate and SLA status. Click → drill (dataset)."""
    try:
        srcs = await execute_query(f"""
            SELECT source_name, system, table_name, row_count, source_group
            FROM {_fq('gold_ingestion_sources')}
            WHERE status='live' AND table_name IS NOT NULL
            ORDER BY source_group, source_name""")
    except Exception:
        srcs = []
    # DQ pass rate per table from the DLT expectations.
    dq = {}
    try:
        for r in await execute_query(f"SELECT rule, passed, failed FROM {_fq('gold_ingestion_quality')}"):
            t = _DQ_RULE_META.get(r["rule"], (None, None))[0]
            if not t:
                continue
            a = dq.setdefault(t, [0, 0])
            a[0] += int(r["passed"]); a[1] += int(r["failed"])
    except Exception:
        pass
    assets = []
    late = issue = clean = 0
    for s in srcs:
        t = s["table_name"]; pf = dq.get(t)
        dqp = round(100 * pf[0] / max(pf[0] + pf[1], 1), 1) if pf else 100.0
        sla = "Late" if t == _LATE_ASSET else "On Time"
        if dqp < 100:
            status = "dq_issue"; issue += 1
        elif sla == "Late":
            status = "late"; late += 1
        else:
            status = "clean"; clean += 1
        assets.append({"source_name": s["source_name"], "table": t, "source_system": s.get("system"),
                       "rows": int(s["row_count"]) if s.get("row_count") is not None else None,
                       "dq_pass": dqp, "sla_status": sla, "status": status})
    return {"assets": assets, "counts": {"late": late, "dq_issue": issue, "clean": clean}}


async def ingestion_status() -> dict:
    out = {"pipeline_name": PIPELINE_NAME, "state": None, "pipeline_url": None,
           "sources": [], "expectations": [], "pass_rate": 0.0, "total_evaluated": 0,
           "layers": [], "quarantined_claims": 0, "quarantined_fraud": 0,
           "documents_count": 0, "scorecard_ready": False}
    # Quality scorecard (governed table — SP reads via SQL).
    try:
        q = await execute_query(
            f"SELECT rule, passed, failed, dataset FROM {_fq('gold_ingestion_quality')} ORDER BY failed DESC, rule")
        def _exp(r):
            p, f = int(r["passed"]), int(r["failed"])
            meta = _DQ_RULE_META.get(r["rule"], (r.get("dataset"), "track + retain"))
            return {"name": r["rule"], "passed": p, "failed": f, "dataset": r.get("dataset"),
                    "table": meta[0], "action": meta[1],
                    "pass_rate": round(100 * p / max(p + f, 1), 2)}
        out["expectations"] = [_exp(r) for r in q]
        tp = sum(e["passed"] for e in out["expectations"])
        tf = sum(e["failed"] for e in out["expectations"])
        out["pass_rate"] = round(100 * tp / max(tp + tf, 1), 2)
        out["total_evaluated"] = tp + tf
        out["scorecard_ready"] = bool(out["expectations"])
    except Exception as e:
        logger.warning("quality scorecard read failed: %s", e)
    # Multi-source map.
    try:
        out["sources"] = await execute_query(f"""
            SELECT source_group, source_name, system, channel, format, latency,
                   databricks_tool, table_name, row_count, status, note
            FROM {_fq('gold_ingestion_sources')}
            ORDER BY CASE status WHEN 'live' THEN 0 ELSE 1 END, source_group, source_name""")
    except Exception:
        out["sources"] = []
    # Per-layer freshness / row counts.
    try:
        sel = ", ".join(f"(SELECT count(*) FROM {_fq(t)}) AS c{i}" for i, (_, _, t) in enumerate(_LAYERS))
        row = (await execute_query(f"SELECT {sel}"))[0]
        out["layers"] = [{"layer": L[0], "desc": L[1], "table": L[2], "rows": int(row[f"c{i}"])}
                         for i, L in enumerate(_LAYERS)]
    except Exception:
        out["layers"] = []
    # Quarantine + documents + freshness — one round-trip via scalar subqueries.
    try:
        m = (await execute_query(f"""
            SELECT (SELECT count(*) FROM {_fq('bronze_quarantine_claims')}) qc,
                   (SELECT count(*) FROM {_fq('bronze_quarantine_fraud_signals')}) qf,
                   (SELECT count(*) FROM {_fq('gold_document_extractions')}) docs,
                   (SELECT cast(max(cda_batch_ts) AS string) FROM {_fq('bronze_gw_cc_claim')}) last_batch,
                   (SELECT cast(max(report_date) AS string) FROM {_fq('bronze_gw_cc_claim')}) last_report,
                   (SELECT count(*) FROM {_fq('bronze_gw_cc_claim')}) claims"""))[0]
        out["quarantined_claims"] = int(m.get("qc") or 0)
        out["quarantined_fraud"] = int(m.get("qf") or 0)
        out["documents_count"] = int(m.get("docs") or 0)
        out["freshness"] = {"last_batch": m.get("last_batch"), "last_report": m.get("last_report"),
                            "claims": int(m.get("claims") or 0)}
    except Exception:
        out["freshness"] = {}
    out["inspectable"] = [{"table": t, "label": lab} for t, lab in _INSPECTABLE.items()]
    out.update(await asyncio.to_thread(_pipeline_link))
    return out


async def ingestion_profile() -> list[dict]:
    """Sensible data-quality checks across dimensions (computed live on silver).
    Completeness / uniqueness / validity / referential integrity / timeliness."""
    s = _fq("silver_claims_enriched")
    try:
        r = (await execute_query(f"""
            SELECT count(*) n,
                   sum(CASE WHEN claim_public_id IS NULL THEN 1 ELSE 0 END) null_id,
                   sum(CASE WHEN total_incurred IS NULL THEN 1 ELSE 0 END) null_amt,
                   sum(CASE WHEN loss_date IS NULL OR report_date IS NULL THEN 1 ELSE 0 END) null_dates,
                   count(*) - count(DISTINCT claim_public_id) dup_id,
                   sum(CASE WHEN sum_insured IS NULL THEN 1 ELSE 0 END) no_policy,
                   round(avg(reporting_lag_days),1) avg_lag,
                   round(100.0*avg(CASE WHEN reporting_lag_days <= 7 THEN 1 ELSE 0 END),1) within7,
                   sum(CASE WHEN reporting_lag_days < 0 THEN 1 ELSE 0 END) neg_lag
            FROM {s}"""))[0]
    except Exception:
        return []
    n = int(r["n"] or 1)
    def pct_ok(bad):
        return round(100 * (n - int(bad or 0)) / max(n, 1), 2)
    def st(v, warn):
        return "pass" if v >= warn else "warn"
    rows = [
        ("Completeness", "Key fields populated (id, amount, dates)",
         f"{pct_ok(int(r['null_id'] or 0)+int(r['null_amt'] or 0)+int(r['null_dates'] or 0))}%",
         st(pct_ok(int(r['null_id'] or 0)+int(r['null_amt'] or 0)+int(r['null_dates'] or 0)), 99)),
        ("Uniqueness", "No duplicate claim IDs", f"{int(r['dup_id'] or 0)} dupes",
         "pass" if int(r["dup_id"] or 0) == 0 else "warn"),
        ("Referential integrity", "Claim resolves to a policy",
         f"{pct_ok(r['no_policy'])}% matched", st(pct_ok(r["no_policy"]), 97)),
        ("Timeliness", "Reported within 7 days of loss", f"{r['within7']}% (avg {r['avg_lag']}d)",
         st(float(r["within7"] or 0), 50)),
        ("Plausibility", "No negative reporting lag", f"{int(r['neg_lag'] or 0)} anomalies",
         "pass" if int(r["neg_lag"] or 0) == 0 else "warn"),
    ]
    return [{"dimension": d, "check": c, "value": v, "status": s} for d, c, v, s in rows]


async def ingestion_analytics() -> dict:
    """Basic analytics over the ingested book — by peril, channel, volume over time, amount bands."""
    s = _fq("silver_claims_enriched")
    out = {"by_peril": [], "by_channel": [], "by_month": [], "amount_bands": []}
    try:
        out["by_peril"] = await execute_query(f"""
            SELECT peril_type peril, count(*) n, CAST(round(avg(total_incurred),0) AS double) avg_incurred
            FROM {s} GROUP BY peril_type ORDER BY n DESC""")
        out["by_channel"] = await execute_query(f"""
            SELECT report_channel channel, count(*) n FROM {s} GROUP BY report_channel ORDER BY n DESC""")
        out["by_month"] = await execute_query(f"""
            SELECT date_format(date_trunc('month', report_date),'yyyy-MM') month, count(*) n
            FROM {s} WHERE report_date >= add_months(current_date(), -12)
            GROUP BY 1 ORDER BY 1""")
        out["amount_bands"] = await execute_query(f"""
            SELECT CASE WHEN total_incurred < 1000 THEN '1 · < £1k'
                        WHEN total_incurred < 5000 THEN '2 · £1k–5k'
                        WHEN total_incurred < 25000 THEN '3 · £5k–25k'
                        WHEN total_incurred < 100000 THEN '4 · £25k–100k'
                        ELSE '5 · £100k+' END band, count(*) n
            FROM {s} GROUP BY 1 ORDER BY 1""")
    except Exception:
        pass
    return out


async def ingestion_sample(table: str, limit: int = 8) -> dict:
    """Inspect the input — a sample of raw rows + the schema for a whitelisted source table."""
    if table not in _INSPECTABLE:
        return {"error": "table not inspectable", "columns": [], "rows": []}
    try:
        cols = await execute_query(f"DESCRIBE {_fq(table)}")
        colnames = [c["col_name"] for c in cols if c.get("col_name") and not c["col_name"].startswith("#")][:12]
        rows = await execute_query(f"SELECT {', '.join(colnames)} FROM {_fq(table)} LIMIT {int(limit)}")
        return {"table": table, "label": _INSPECTABLE[table], "columns": colnames, "rows": rows}
    except Exception as e:
        return {"error": str(e)[:160], "columns": [], "rows": []}


async def ingestion_quarantine(reason: str | None = None, limit: int = 25) -> dict:
    """The 'no silent data loss' drill-down — real quarantined rows + why they failed."""
    reasons = []
    for src, tbl in (("claims", "bronze_quarantine_claims"), ("fraud", "bronze_quarantine_fraud_signals")):
        try:
            rs = await execute_query(
                f"SELECT quarantine_reason reason, count(*) n FROM {_fq(tbl)} GROUP BY quarantine_reason")
            for r in rs:
                reasons.append({"source": src, "reason": r["reason"], "count": int(r["n"])})
        except Exception:
            pass
    rows = []
    if reason:
        tbl = "bronze_quarantine_fraud_signals" if reason == "fraud_score_out_of_range" else "bronze_quarantine_claims"
        cols = ("claim_public_id, fraud_score, prior_claims_12m, signal_source, quarantine_reason, cast(_quarantined_at AS string) at"
                if tbl.endswith("fraud_signals")
                else "claim_public_id, policy_number, loss_cause, report_channel, cast(total_incurred AS double) total_incurred, quarantine_reason, cast(_quarantined_at AS string) at")
        try:
            rows = await execute_query(
                f"SELECT {cols} FROM {_fq(tbl)} WHERE quarantine_reason='{_esc(reason)}' LIMIT {int(limit)}")
        except Exception:
            rows = []
    return {"reasons": reasons, "reason": reason, "rows": rows}


async def ingestion_documents(limit: int = 20) -> list[dict]:
    """Unstructured spotlight — files Auto-Loaded + AI-extracted, joined to the claim."""
    try:
        return await execute_query(f"""
            SELECT file_name, doc_type, claim_public_id, severity, extracted_summary,
                   source_tool, cast(ingested_at AS string) ingested_at
            FROM {_fq('gold_document_extractions')} ORDER BY file_name LIMIT {int(limit)}
        """)
    except Exception:
        return []


# --------------------------------------------------------------------------
# Stage B — Transformation (silver enrichment for the selected claim)
# --------------------------------------------------------------------------
async def enrichment(cid: str) -> dict:
    rows = await execute_query(f"""
        SELECT claim_public_id, claim_number, policy_number, peril_type, loss_cause,
               cast(loss_date AS string) loss_date, cast(report_date AS string) report_date,
               report_channel, reporting_lag_days, incident_type, description_text,
               product, sum_insured, annual_premium, policy_tenure_years,
               sum_insured_to_reported_ratio, postcode_district, third_party_involved,
               flood_risk_score, wind_risk_score, freeze_risk_score, weather_risk_composite,
               total_incurred, paid_amount, initial_reserve, ultimate_reserve,
               fraud_score, prior_claims_12m, days_since_incident, is_high_value,
               is_potential_fraud, at_fault, claim_status, days_to_settle, leakage_flag,
               handler_id, handler_grade, triage_decision, reserve_bracket
        FROM {_fq('silver_claims_enriched')} WHERE claim_public_id = '{_esc(cid)}'
    """)
    return rows[0] if rows else {}


# --------------------------------------------------------------------------
# Stage B — Governance & Portfolio (dashboard + genie + lineage links)
# --------------------------------------------------------------------------
def _governance_sync() -> dict:
    import requests
    from server.sql import _client
    w = _client()
    host = w.config.host.rstrip("/")
    hdr = w.config._header_factory()
    did = config.DASHBOARD_ID
    if not did:
        try:
            r = requests.get(f"{host}/api/2.0/lakeview/dashboards?page_size=200", headers=hdr, timeout=60).json()
            for d in r.get("dashboards", []):
                if "Claims Portfolio" in (d.get("display_name") or ""):
                    did = d.get("dashboard_id")
                    break
        except Exception as e:
            logger.warning("dashboard lookup failed: %s", e)
    gid = config.GENIE_SPACE_ID
    chain = ["landing_gw_cc_claim", "bronze_gw_cc_claim", "silver_claims_enriched",
             "feature_triage", "model_triage_classifier", "gold_handler_decisions"]
    lineage = [{"asset": a, "explore_url": f"{host}/explore/data/{config.CATALOG}/{config.SCHEMA}/{a}"} for a in chain]
    return {
        "dashboard_id": did,
        "dashboard_url": f"{host}/dashboardsv3/{did}" if did else None,
        "dashboard_embed_url": f"{host}/embed/dashboardsv3/{did}" if did else None,
        "genie_url": f"{host}/genie/rooms/{gid}" if gid else None,
        "genie_embed_url": f"{host}/embed/genie/rooms/{gid}" if gid else None,
        "lineage": lineage,
    }


async def governance_links() -> dict:
    return await asyncio.to_thread(_governance_sync)


def find_reset_job():
    """Resolve the reset job id — DAB dev mode prefixes the name ("[dev user] ...")
    so match by substring, not exact name."""
    from server.sql import _client
    w = _client()
    j = next((x for x in w.jobs.list(name=config.RESET_JOB_NAME)), None)
    if not j:
        j = next((x for x in w.jobs.list()
                  if config.RESET_JOB_NAME in ((x.settings.name if x.settings else "") or "")), None)
    return j


def reset_available() -> bool:
    try:
        return find_reset_job() is not None
    except Exception:
        return False


# --------------------------------------------------------------------------
# Phase 11 Stage B — Control Tower, auto-close slider, monitoring lens, ask-window
# --------------------------------------------------------------------------
def _sla_sql() -> str:
    cases = " ".join(f"WHEN '{p}' THEN {d}" for p, d in PERIL_SLA.items())
    return f"(CASE peril_type {cases} ELSE {DEFAULT_SLA} END)"


async def control_tower() -> dict:
    """Control-tower tiles with per-peril SLA, targets/RAG, £-FTE framing, the recovery
    and reserve headlines, and trend vs the last metrics snapshot."""
    s = _fq("silver_claims_enriched")
    d = _fq("gold_claim_disposition")
    sla = _sla_sql()
    base = (await execute_query(f"""
        SELECT count(*) total,
          sum(CASE WHEN claim_status='open' THEN 1 ELSE 0 END) open,
          sum(CASE WHEN claim_status IN ('open','under_investigation') THEN 1 ELSE 0 END) open_inv,
          sum(CASE WHEN claim_status='under_investigation' THEN 1 ELSE 0 END) investigating,
          sum(CASE WHEN claim_status='settled' THEN 1 ELSE 0 END) closed,
          sum(CASE WHEN claim_status='declined' THEN 1 ELSE 0 END) declined,
          sum(CASE WHEN claim_status IN ('open','under_investigation')
                    AND datediff(current_date(), report_date) > {sla} THEN 1 ELSE 0 END) past_sla,
          sum(CASE WHEN total_incurred > 50000 AND claim_status IN ('open','under_investigation') THEN 1 ELSE 0 END) large_losses,
          round(sum(CASE WHEN claim_status IN ('open','under_investigation') THEN ultimate_reserve ELSE 0 END)) total_reserves,
          round(100.0 * avg(CASE WHEN leakage_flag THEN 1 ELSE 0 END), 1) leakage_rate,
          round(avg(CASE WHEN days_to_settle IS NOT NULL THEN days_to_settle END), 1) avg_settle_days,
          round(100.0 * avg(CASE WHEN coalesce(is_potential_fraud,false) THEN 1 ELSE 0 END), 1) fraud_refer_rate,
          sum(CASE WHEN recovery_flag AND claim_status IN ('open','under_investigation') THEN 1 ELSE 0 END) recovery_count,
          round(sum(CASE WHEN recovery_flag AND claim_status IN ('open','under_investigation') THEN recoverable_amount ELSE 0 END)) recoverable_total,
          sum(CASE WHEN claim_status='withdrawn' THEN 1 ELSE 0 END) withdrawn,
          sum(CASE WHEN claim_status='settled' AND days_to_settle <= {sla} THEN 1 ELSE 0 END) closed_within,
          round(sum(CASE WHEN claim_status IN ('open','under_investigation') THEN initial_reserve ELSE 0 END)) init_open,
          round(sum(CASE WHEN claim_status IN ('open','under_investigation') THEN ultimate_reserve ELSE 0 END)) ult_open
        FROM {s}"""))[0]
    handlers_n = int((await execute_query(f"SELECT count(*) h FROM {_fq('ref_handlers')}"))[0]["h"]) or 80
    try:
        disp = (await execute_query(f"""
            SELECT sum(CASE WHEN disposition='auto_closed' THEN 1 ELSE 0 END) auto_closed,
                   sum(CASE WHEN disposition='escalated' THEN 1 ELSE 0 END) escalated,
                   round(100.0 * avg(CASE WHEN disposition='auto_closed' THEN 1 ELSE 0 END), 1) pct_auto_closed
            FROM {d}"""))[0]
    except Exception:
        disp = {"auto_closed": 0, "escalated": 0, "pct_auto_closed": 0}
    # Reserve under-reserving headline (escape of water).
    try:
        rd = (await execute_query(f"""
            SELECT round(sum(sum_ultimate_reserve)/nullif(sum(sum_initial_reserve),0),3) dev_ratio,
                   round(sum(sum_ultimate_reserve) - sum(sum_initial_reserve)) gap
            FROM {_fq('gold_reserve_development')} WHERE peril_type='home_escape_water'"""))[0]
    except Exception:
        rd = {"dev_ratio": None, "gap": None}
    # Trend vs the most recent prior metrics snapshot.
    prev = {}
    try:
        rows = await execute_query(f"""
            SELECT pct_auto_closed, leakage_rate, avg_settle_days, sla_breach_pct, recoverable_total
            FROM {_fq('gold_cco_metrics_daily')} WHERE snapshot_date < current_date()
            ORDER BY snapshot_date DESC LIMIT 1""")
        prev = rows[0] if rows else {}
    except Exception:
        prev = {}

    open_inv = int(base["open_inv"]) or 1
    past_sla = int(base["past_sla"])
    sla_breach_pct = round(100.0 * past_sla / open_inv, 1)
    ac = int(disp["auto_closed"])
    gbp_saved = round(ac * HANDLER_COST_PER_CLAIM)
    hours_freed = round(ac * HANDLER_MINS_PER_CLAIM / 60)

    def trend(key, cur, lower_is_better):
        if not prev or prev.get(key) is None:
            return None
        try:
            delta = round(float(cur) - float(prev[key]), 1)
        except Exception:
            return None
        good = (delta < 0) if lower_is_better else (delta > 0)
        return {"delta": delta, "dir": "up" if delta > 0 else ("down" if delta < 0 else "flat"), "good": good}

    # Derived EY KPIs (real where the data supports it).
    settled = int(base["closed"]); declined = int(base["declined"]); withdrawn = int(base["withdrawn"])
    closed_total = settled + declined + withdrawn
    pct_within_target = round(100.0 * int(base["closed_within"]) / settled, 1) if settled else None
    closed_without_pay = round(100.0 * (declined + withdrawn) / closed_total, 1) if closed_total else None
    pct_litigated = round(100.0 * int(base["investigating"]) / int(base["total"]), 1) if int(base["total"]) else None
    reserve_adequacy = round(100.0 * float(base["init_open"] or 0) / float(base["ult_open"]), 1) if base.get("ult_open") else None
    claims_per_fte = round(int(base["open_inv"]) / handlers_n)
    # Loss / Combined ratio: the synthetic book over-indexes severity vs premium, so these
    # are shown as ILLUSTRATIVE reference values (see the About/disclaimer) — the structure
    # and placement is the point; the precise value is not a calibrated P&L.
    loss_ratio, expense_ratio = 71.0, 28.0
    combined_ratio = round(loss_ratio + expense_ratio, 1)

    efficiency = [
        {"key": "open", "label": "Open inventory", "value": int(base["open_inv"]), "fmt": "num",
         "sub": f"{int(base['investigating']):,} under investigation", "rag": "info", "worklist": "aged"},
        {"key": "escalated", "label": "Escalated", "value": ac and int(disp["escalated"]) or int(disp["escalated"]), "fmt": "num",
         "sub": "routed to a handler", "rag": "info", "worklist": None},
        {"key": "settle", "label": "Avg settlement / cycle time", "value": base["avg_settle_days"], "fmt": "days",
         "sub": f"target <= {TARGETS['avg_settle_days']:.0f} days", "rag": _rag(base["avg_settle_days"], TARGETS["avg_settle_days"], True),
         "trend": trend("avg_settle_days", base["avg_settle_days"], True), "worklist": None},
        {"key": "within", "label": "% closed within target", "value": pct_within_target, "fmt": "pct",
         "sub": f"settled within peril SLA · target >= {TARGETS['closed_within_target']:.0f}%",
         "rag": _rag(pct_within_target, TARGETS["closed_within_target"], False), "worklist": None},
        {"key": "sla", "label": "Aged / past SLA", "value": past_sla, "fmt": "num",
         "sub": f"{sla_breach_pct}% of open · per-peril SLA", "rag": _rag(sla_breach_pct, TARGETS["sla_breach_pct"], True), "worklist": "aged"},
        {"key": "fte", "label": "Claims per handler", "value": claims_per_fte, "fmt": "num",
         "sub": f"open caseload / {handlers_n} FTE", "rag": "info", "worklist": None},
    ]
    effectiveness = [
        {"key": "loss_ratio", "label": "Loss ratio", "value": loss_ratio, "fmt": "pct",
         "sub": "incurred ÷ earned premium", "rag": _rag(loss_ratio, TARGETS["loss_ratio"], True), "illustrative": True, "worklist": None},
        {"key": "combined_ratio", "label": "Combined ratio", "value": combined_ratio, "fmt": "pct",
         "sub": f"loss + expense (~{expense_ratio:.0f}%)", "rag": _rag(combined_ratio, 100.0, True, amber_mult=1.05), "illustrative": True, "worklist": None},
        {"key": "reserve_adequacy", "label": "Reserve adequacy", "value": reserve_adequacy, "fmt": "pct",
         "sub": "initial ÷ ultimate (open) · <100% = under-reserved", "rag": _rag(reserve_adequacy, TARGETS["reserve_adequacy"], False), "worklist": "underreserved"},
        {"key": "recovery", "label": "Recovery identified", "value": int(base["recoverable_total"] or 0), "fmt": "gbp",
         "sub": f"{int(base['recovery_count']):,} open claims", "rag": "info", "worklist": "recovery"},
        {"key": "leakage", "label": "Leakage rate", "value": base["leakage_rate"], "fmt": "pct",
         "sub": f"target <= {TARGETS['leakage_rate']:.0f}%", "rag": _rag(base["leakage_rate"], TARGETS["leakage_rate"], True),
         "trend": trend("leakage_rate", base["leakage_rate"], True), "worklist": None},
        {"key": "cwp", "label": "Closed without pay", "value": closed_without_pay, "fmt": "pct",
         "sub": "declined or withdrawn", "rag": "info", "worklist": None},
        {"key": "litigated", "label": "% litigated", "value": pct_litigated, "fmt": "pct",
         "sub": "in investigation / dispute", "rag": "info", "worklist": None},
        {"key": "fraud", "label": "Fraud-refer rate", "value": base["fraud_refer_rate"], "fmt": "pct",
         "sub": "elevated signals → SIU", "rag": "info", "worklist": "high_fraud"},
        {"key": "large", "label": "Large losses (open)", "value": int(base["large_losses"]), "fmt": "num",
         "sub": "over £50,000 — senior review", "rag": "info", "worklist": "large"},
    ]
    return {
        "total": int(base["total"]), "open_inv": int(base["open_inv"]),
        "hero": {"pct_auto_closed": disp["pct_auto_closed"], "auto_closed": ac, "escalated": int(disp["escalated"]),
                 "gbp_saved": gbp_saved, "hours_freed": hours_freed, "target": TARGETS["pct_auto_closed"],
                 "rag": _rag(disp["pct_auto_closed"], TARGETS["pct_auto_closed"], False),
                 "trend": trend("pct_auto_closed", disp["pct_auto_closed"], False)},
        "recovery": {"count": int(base["recovery_count"]), "total": int(base["recoverable_total"] or 0),
                     "trend": trend("recoverable_total", base["recoverable_total"] or 0, False)},
        "reserve": {"dev_ratio": float(rd["dev_ratio"]) if rd.get("dev_ratio") is not None else None,
                    "gap": int(float(rd["gap"])) if rd.get("gap") is not None else None,
                    "adequacy": reserve_adequacy},
        "sla": {"open_inv": int(base["open_inv"]), "past_sla": past_sla, "breach_pct": sla_breach_pct},
        "efficiency": efficiency, "effectiveness": effectiveness,
    }


async def segment_auto_close(conf: float, cap: float, fraud: float) -> dict:
    """Re-segment % auto-closed LIVE for the slider — pure SQL over the stored
    raw decision inputs in gold_claim_disposition (no model re-score)."""
    d = _fq("gold_claim_disposition")
    r = (await execute_query(f"""
        SELECT count(*) total,
          sum(CASE WHEN model_decision='pay_direct' AND model_confidence >= {float(conf)}
                    AND total_incurred <= {float(cap)} AND fraud_score <= {float(fraud)}
                    AND data_complete AND NOT coalesce(nonfraud_rule_fired, false) THEN 1 ELSE 0 END) auto_closed
        FROM {d}"""))[0]
    total = int(r["total"]) or 1
    ac = int(r["auto_closed"])
    return {"conf_threshold": conf, "amount_cap": cap, "fraud_floor": fraud,
            "total": int(r["total"]), "auto_closed": ac, "escalated": int(r["total"]) - ac,
            "pct_auto_closed": round(100.0 * ac / total, 1)}


async def auto_close_config() -> dict:
    try:
        r = (await execute_query(
            f"SELECT conf_threshold, amount_cap, fraud_floor FROM {_fq('auto_close_config')} "
            f"WHERE config_key='default'"))[0]
        return {"conf_threshold": float(r["conf_threshold"]), "amount_cap": float(r["amount_cap"]),
                "fraud_floor": float(r["fraud_floor"])}
    except Exception:
        return {"conf_threshold": 85.0, "amount_cap": 2000.0, "fraud_floor": 20.0}


_RULE_DESCRIPTIONS = [
    {"code": "R1", "name": "Fraud threshold", "desc": "Fraud score above the floor.", "param": "fraud_floor"},
    {"code": "R2", "name": "Reporting lag", "desc": "Reported too many days after the incident.", "param": "lag_limit"},
    {"code": "R3", "name": "Prior-claims velocity", "desc": "Too many prior claims in 12 months.", "param": "velocity_limit"},
    {"code": "R4", "name": "Amount / sum-insured anomaly", "desc": "Claim close to (or above) the sum insured.", "param": "ratio_ceiling"},
    {"code": "R5", "name": "Severity / amount consistency", "desc": "Reported amount far above the peril norm.", "param": "severity_mult"},
    {"code": "R6", "name": "Telematics speed-vs-limit", "desc": "Phase 12 — hook reserved.", "param": None},
    {"code": "R7", "name": "Image severity vs reported", "desc": "Phase 12 — hook reserved.", "param": None},
]


async def rules() -> dict:
    """Dynamic rule-engine config + which rules are firing across the book."""
    cfg = {}
    try:
        ac = (await execute_query(f"SELECT fraud_floor FROM {_fq('auto_close_config')} WHERE config_key='default'"))[0]
        rc = (await execute_query(
            f"SELECT lag_limit, velocity_limit, ratio_ceiling, severity_mult FROM {_fq('rule_config')} WHERE config_key='default'"))[0]
        cfg = {"fraud_floor": float(ac["fraud_floor"]), **{k: float(rc[k]) for k in rc}}
    except Exception:
        cfg = {}
    try:
        fired = await execute_query(f"""
            SELECT r rule, count(*) n FROM {_fq('gold_claim_disposition')}
            LATERAL VIEW explode(fired_rules) t AS r GROUP BY r ORDER BY r""")
    except Exception:
        fired = []
    return {"config": cfg, "descriptions": _RULE_DESCRIPTIONS, "firing": fired}


async def monday_brief() -> dict:
    """The Monday-morning brief — prioritised, money-framed, and each item links to a
    worklist you can act on. Grouped: needs you now / money on the table / risk."""
    t = await control_tower()
    sla, rec, res, hero = t["sla"], t["recovery"], t["reserve"], t["hero"]
    tiles = (t.get("efficiency") or []) + (t.get("effectiveness") or [])   # was t["tiles"] (renamed)
    needs, money, risk = [], [], []
    if sla["past_sla"]:
        needs.append({"text": f"{sla['past_sla']:,} open claims are past their per-peril SLA ({sla['breach_pct']}% of open) — reallocate or chase.",
                      "action": "Open the SLA worklist", "worklist": "aged"})
    large = next((x["value"] for x in tiles if x["key"] == "large"), 0)
    if large:
        needs.append({"text": f"{large:,} large losses (over £50,000) are open — confirm reserves and senior ownership.",
                      "action": "Open large-loss worklist", "worklist": "large"})
    needs.append({"text": f"{hero['pct_auto_closed']}% of eligible claims auto-close straight-through, freeing ≈{hero['hours_freed']:,} handler-hours (≈£{hero['gbp_saved']:,}). Lifting the appetite frees more.",
                  "action": "Review auto-close appetite", "worklist": "autoclose"})
    if rec["total"]:
        money.append({"text": f"£{rec['total']:,} is recoverable across {rec['count']:,} open claims — recovery is chronically under-pursued.",
                      "action": "Open recovery worklist", "worklist": "recovery"})
    if res.get("dev_ratio") and res["dev_ratio"] > 1.1:
        money.append({"text": f"Escape-of-water is developing at {res['dev_ratio']}× initial reserve (~{round((res['dev_ratio']-1)*100)}% under-reserved) — a ≈£{abs(res['gap']):,} provision gap.",
                      "action": "See under-reserved claims", "worklist": "underreserved"})
    leak = next((x for x in tiles if x["key"] == "leakage"), {})
    if leak.get("rag") in ("amber", "red"):
        risk.append({"text": f"Leakage is {leak['value']}% (target ≤ {TARGETS['leakage_rate']}%){_trend_phrase(leak.get('trend'))}.",
                     "action": None, "worklist": None})
    fr = next((x for x in tiles if x["key"] == "fraud"), {})
    risk.append({"text": f"{fr.get('value','—')}% of claims carry elevated fraud signals — SIU focus.",
                 "action": "Open fraud queue", "worklist": "high_fraud"})
    return {"sections": [
        {"title": "Needs you now", "icon": "⏰", "items": needs},
        {"title": "Money on the table", "icon": "💷", "items": money},
        {"title": "Risk to watch", "icon": "⚠️", "items": risk}]}


def _trend_phrase(tr):
    if not tr:
        return ""
    return f", { 'up' if tr['dir']=='up' else 'down' } {abs(tr['delta'])} vs last snapshot"


# Backwards-compatible alias.
async def monitoring_lens() -> dict:
    return await monday_brief()


async def operations_view() -> dict:
    """Where the claims are right now — by status, by peril, by age, and by team
    (with SLA breach). The 'which claims are where' operational picture."""
    s = _fq("silver_claims_enriched")
    sla = _sla_sql()
    out = {"by_status": [], "open_by_peril": [], "open_by_age": [], "by_team": []}
    try:
        out["by_status"] = await execute_query(f"""
            SELECT claim_status status, count(*) n FROM {s} GROUP BY claim_status ORDER BY n DESC""")
        out["open_by_peril"] = await execute_query(f"""
            SELECT peril_type peril, count(*) n,
                   sum(CASE WHEN datediff(current_date(), report_date) > {sla} THEN 1 ELSE 0 END) breached
            FROM {s} WHERE claim_status IN ('open','under_investigation')
            GROUP BY peril_type ORDER BY n DESC""")
        out["open_by_age"] = await execute_query(f"""
            SELECT CASE WHEN datediff(current_date(), report_date) <= 30 THEN '1 · 0–30 days'
                        WHEN datediff(current_date(), report_date) <= 90 THEN '2 · 31–90 days'
                        WHEN datediff(current_date(), report_date) <= 180 THEN '3 · 91–180 days'
                        ELSE '4 · 180+ days' END bucket, count(*) n
            FROM {s} WHERE claim_status IN ('open','under_investigation') GROUP BY 1 ORDER BY 1""")
        out["by_team"] = await execute_query(f"""
            SELECT coalesce(h.team,'unassigned') team, count(*) n,
                   sum(CASE WHEN datediff(current_date(), s.report_date) > {sla} THEN 1 ELSE 0 END) breached,
                   round(avg(s.fraud_score),0) avg_fraud
            FROM {s} s LEFT JOIN {_fq('ref_handlers')} h ON s.handler_id = h.handler_id
            WHERE s.claim_status IN ('open','under_investigation')
            GROUP BY h.team ORDER BY n DESC""")
    except Exception as e:
        logger.warning("operations_view failed: %s", e)
    return out


# --------------------------------------------------------------------------
# Worklists — turn a number into an actionable, clickable queue of claims.
# --------------------------------------------------------------------------
_WORKLISTS = {
    "recovery": {"title": "Recovery / subrogation opportunities",
                 "where": "recovery_flag AND claim_status IN ('open','under_investigation')",
                 "order": "recoverable_amount DESC", "metric": ("recoverable_amount", "gbp", "Recoverable")},
    "aged": {"title": "Open claims past their SLA",
             "where": f"claim_status IN ('open','under_investigation') AND datediff(current_date(), report_date) > {_sla_sql() if False else ''}",
             "order": "report_date ASC", "metric": ("age_days", "days", "Age")},
    "large": {"title": "Open large losses (over £50,000)",
              "where": "total_incurred > 50000 AND claim_status IN ('open','under_investigation')",
              "order": "total_incurred DESC", "metric": ("total_incurred", "gbp", "Incurred")},
    "underreserved": {"title": "Under-reserved escape-of-water (open)",
                      "where": "peril_type='home_escape_water' AND claim_status IN ('open','under_investigation') AND ultimate_reserve > initial_reserve",
                      "order": "(ultimate_reserve - initial_reserve) DESC",
                      "metric": ("reserve_gap", "gbp", "Reserve gap")},
    "high_fraud": {"title": "High fraud-score open claims (SIU queue)",
                   "where": "fraud_score > 70 AND claim_status IN ('open','under_investigation')",
                   "order": "fraud_score DESC", "metric": ("fraud_score", "num", "Fraud score")},
    "autoclose": {"title": "Auto-closed straight-through (this run)",
                  "where": None, "order": None, "metric": ("model_confidence", "pct", "Confidence")},
}


async def worklist(kind: str, limit: int = 100) -> dict:
    spec = _WORKLISTS.get(kind)
    if not spec:
        return {"kind": kind, "title": "Unknown worklist", "rows": []}
    s = _fq("silver_claims_enriched")
    if kind == "autoclose":
        rows = await execute_query(f"""
            SELECT claim_public_id, model_decision, round(model_confidence,1) metric, total_incurred
            FROM {_fq('gold_claim_disposition')} WHERE disposition='auto_closed'
            ORDER BY model_confidence DESC LIMIT {int(limit)}""")
        return {"kind": kind, "title": spec["title"], "metric_label": "Confidence", "metric_fmt": "pct", "rows": rows}
    mcol, mfmt, mlabel = spec["metric"]
    where = spec["where"]
    if kind == "aged":
        where = f"claim_status IN ('open','under_investigation') AND datediff(current_date(), report_date) > {_sla_sql()}"
        msel = "datediff(current_date(), report_date) AS metric"
    elif kind == "underreserved":
        msel = "(ultimate_reserve - initial_reserve) AS metric"
    else:
        msel = f"{mcol} AS metric"
    rows = await execute_query(f"""
        SELECT claim_public_id, peril_type, total_incurred, claim_status, {msel}
        FROM {s} WHERE {where} ORDER BY {spec['order']} LIMIT {int(limit)}""")
    return {"kind": kind, "title": spec["title"], "metric_label": mlabel, "metric_fmt": mfmt, "rows": rows}


# --------------------------------------------------------------------------
# Handler / team performance (wires in the orphaned gold_handler_scorecard).
# --------------------------------------------------------------------------
async def handlers() -> dict:
    sc = _fq("gold_handler_scorecard")
    try:
        teams = await execute_query(f"""
            SELECT team, count(*) handlers, sum(caseload) caseload,
                   round(avg(avg_days_to_settle),1) avg_settle, round(avg(leakage_rate_pct),2) leakage_pct
            FROM {sc} GROUP BY team ORDER BY leakage_pct DESC""")
        worst = await execute_query(f"""
            SELECT handler_id, grade, team, caseload, avg_days_to_settle, leakage_rate_pct
            FROM {sc} WHERE caseload > 0 ORDER BY leakage_rate_pct DESC LIMIT 10""")
        overall = (await execute_query(f"""
            SELECT count(*) handlers, sum(caseload) caseload, round(avg(avg_days_to_settle),1) avg_settle,
                   round(avg(leakage_rate_pct),2) leakage_pct FROM {sc}"""))[0]
        return {"teams": teams, "worst": worst, "overall": overall}
    except Exception:
        return {"teams": [], "worst": [], "overall": {}}


# --------------------------------------------------------------------------
# Fraud & SIU view.
# --------------------------------------------------------------------------
async def fraud_view() -> dict:
    s = _fq("silver_claims_enriched")
    buckets = await execute_query(f"""
        SELECT CASE WHEN fraud_score>70 THEN '70-100 (high)' WHEN fraud_score>=40 THEN '40-69 (medium)' ELSE '0-39 (low)' END band,
               count(*) n FROM {s} GROUP BY 1 ORDER BY 1 DESC""")
    summary = (await execute_query(f"""
        SELECT round(100.0*avg(CASE WHEN coalesce(is_potential_fraud,false) THEN 1 ELSE 0 END),1) refer_rate,
               sum(CASE WHEN fraud_score>70 AND claim_status IN ('open','under_investigation') THEN 1 ELSE 0 END) high_open,
               round(avg(fraud_score),1) avg_score FROM {s}"""))[0]
    queue = await execute_query(f"""
        SELECT claim_public_id, peril_type, total_incurred, fraud_score, prior_claims_12m, reporting_lag_days
        FROM {s} WHERE fraud_score>70 AND claim_status IN ('open','under_investigation')
        ORDER BY fraud_score DESC LIMIT 15""")
    return {"summary": summary, "buckets": buckets, "queue": queue, "models": await fraud_models()}


async def fraud_models() -> dict:
    """The models behind fraud detection — the trained fraud model (card + MLflow/UC
    links), the triage model that consumes the score, and the Fraud agent endpoint."""
    from server.sql import _client
    try:
        host = (_client().config.host or "").rstrip("/")
    except Exception:
        host = ""
    cat, sch = config.CATALOG, config.SCHEMA

    def uc(model):
        return f"{host}/explore/data/models/{cat}/{sch}/{model}" if host else None

    out = {"fraud": None,
           "triage": {"name": "model_triage_classifier", "uc_url": uc("model_triage_classifier"),
                      "note": "Consumes the fraud score as a feature — drives the refer-to-SIU decision."},
           "agent": None}
    try:
        rows = await execute_query(
            f"""SELECT model_name, model_version, auc, precision_at, recall_at, base_rate,
                       top_features, run_id, experiment_id FROM {_fq('gold_fraud_model_card')} LIMIT 1""")
        if rows:
            r = rows[0]; mn = (r["model_name"] or "").split(".")[-1]
            ml = (f"{host}/ml/experiments/{r['experiment_id']}/runs/{r['run_id']}"
                  if (host and r.get("run_id") and r.get("experiment_id")) else None)
            out["fraud"] = {"name": mn, "version": r["model_version"], "auc": r["auc"],
                            "precision": r["precision_at"], "recall": r["recall_at"], "base_rate": r["base_rate"],
                            "top_features": json.loads(r["top_features"] or "[]"),
                            "uc_url": uc(mn), "mlflow_url": ml}
    except Exception:
        pass
    try:
        eps = [e.name for e in _client().serving_endpoints.list()]
        fa = next((n for n in eps if "agent_frau" in n), None)
        if fa:
            out["agent"] = {"name": fa, "url": f"{host}/ml/endpoints/{fa}" if host else None}
    except Exception:
        pass
    return out


# --------------------------------------------------------------------------
# Trends — time series from the daily metrics snapshot.
# --------------------------------------------------------------------------
async def trends() -> dict:
    try:
        rows = await execute_query(f"""
            SELECT cast(snapshot_date AS string) d, pct_auto_closed, leakage_rate, avg_settle_days,
                   sla_breach_pct, recoverable_total, open_inv
            FROM {_fq('gold_cco_metrics_daily')} ORDER BY snapshot_date""")
        return {"series": rows}
    except Exception:
        return {"series": []}


async def ask(question: str, cid: str | None = None, use_cache: bool | None = None) -> dict:
    """Unified ask-window — route a free-text question through the Claims AI
    supervisor (or Context fallback) via the cache-first wrapper."""
    if use_cache is None:
        use_cache = _use_cache
    endpoint, is_supervisor = _synthesis_endpoint()
    custom = {"claim_public_id": cid} if cid else {}
    payload = {"messages": [{"role": "user", "content": question}], "custom_inputs": custom}
    out = await asyncio.to_thread(get_agent_response, endpoint, payload, use_cache)
    resp = out.get("response", {})
    msgs = resp.get("messages", [])
    text = msgs[-1].get("content", "") if msgs else ""
    return {"text": text, "cache": out.get("cache"), "endpoint": endpoint,
            "supervisor": is_supervisor, "use_cache": use_cache, "question": question, "cid": cid}


# --------------------------------------------------------------------------
# Phase 11 Stage C — hero claim disposition, agent reasoning, data inventory
# --------------------------------------------------------------------------
async def claim_disposition(cid: str) -> dict:
    """The auto-close / escalation disposition for a claim with the full per-rule
    reasoning (from gold_claim_disposition)."""
    try:
        rows = await execute_query(f"""
            SELECT to_json(named_struct(
                'disposition', disposition, 'model_decision', model_decision,
                'model_confidence', model_confidence, 'total_incurred', total_incurred,
                'fraud_score', fraud_score, 'data_complete', data_complete,
                'rules_passed', rules_passed, 'rules_failed', rules_failed,
                'fired_rules', fired_rules, 'reasoning', reasoning)) j
            FROM {_fq('gold_claim_disposition')} WHERE claim_public_id = '{_esc(cid)}'""")
        return json.loads(rows[0]["j"]) if rows and rows[0].get("j") else {}
    except Exception:
        return {}


async def claim_reasoning(cid: str) -> list[dict]:
    """Persisted agent reasoning for a claim (regulator-viewable)."""
    try:
        return await execute_query(f"""
            SELECT agent_name, reasoning_text, cast(created_ts AS string) created_ts
            FROM {_fq('agent_reasoning_log')} WHERE claim_public_id = '{_esc(cid)}'
            ORDER BY agent_name""")
    except Exception:
        return []


# Data inventory — the "what's collected" catalogue. Static metadata (the
# governance documentation) so it renders without scanning every table; the
# per-page demoer narration + sensitivity tiers come straight from Phase 7/11.
_INVENTORY = [
    {"source": "Guidewire ClaimCenter (CDA)", "table": "landing_gw_cc_claim / bronze_gw_cc_claim",
     "fields": "claim id, policy, loss/report date, peril, channel, amount, status",
     "tier": "PII", "retention": "7 years", "masking": "postcode masked in v_claims_masked",
     "used_for": "The spine of every claim — drives triage, reserving and the control tower."},
    {"source": "Guidewire ClaimCenter", "table": "bronze_gw_cc_incident",
     "fields": "incident type, free-text description (health/injury possible)",
     "tier": "SECRET", "retention": "7 years", "masking": "narrative withheld in v_claims_secret (CMK)",
     "used_for": "Context for the dossier and fraud agents; never shown raw to non-privileged roles."},
    {"source": "Guidewire ClaimCenter", "table": "bronze_gw_cc_contact",
     "fields": "contact role, claimant postcode district", "tier": "PII",
     "retention": "7 years", "masking": "postcode masked",
     "used_for": "Third-party detection (recovery) and weather-risk join by district."},
    {"source": "Guidewire PolicyCenter", "table": "bronze_gw_pc_policy",
     "fields": "product, sum insured, premium, effective/expiry", "tier": "PII",
     "retention": "10 years", "masking": "policy-level, restricted to handlers",
     "used_for": "Policy context for the dossier; loss-ratio and premium-adequacy analytics."},
    {"source": "Internal fraud rules", "table": "bronze_fraud_signals_raw",
     "fields": "fraud score, flag, prior claims, reporting lag", "tier": "SECRET",
     "retention": "7 years", "masking": "SIU-only; Secret tier",
     "used_for": "Triage refer-to-SIU decisions and the Fraud agent's risk verdict."},
    {"source": "Weather feed", "table": "bronze_weather_raw / ref_weather_index",
     "fields": "flood / wind / freeze risk by district", "tier": "Public",
     "retention": "indefinite", "masking": "none",
     "used_for": "Weather-risk composite for reserving and geographic concentration."},
    {"source": "Derived (silver)", "table": "silver_claims_enriched",
     "fields": "all enriched signals incl recovery, weather composite, ML labels",
     "tier": "PII + SECRET", "retention": "7 years", "masking": "PII + Secret views",
     "used_for": "The single enriched record every model, agent and tile reads from."},
    {"source": "ML + workflow", "table": "gold_claim_disposition",
     "fields": "disposition, model decision/confidence, per-rule reasoning",
     "tier": "Internal", "retention": "7 years", "masking": "decision audit",
     "used_for": "Auto-close vs escalate; the slider re-segments this live; the audit of why."},
    {"source": "Agents (MLflow traces)", "table": "agent_reasoning_log",
     "fields": "agent, input, reasoning, output, timestamp", "tier": "Internal",
     "retention": "7 years", "masking": "regulator-viewable decision reasoning",
     "used_for": "The regulator-readable record of what each agent reasoned, per claim."},
    {"source": "Human-in-the-loop", "table": "gold_handler_decisions",
     "fields": "handler action, override flag/reason, attribution, timestamp",
     "tier": "Internal", "retention": "7 years", "masking": "FCA / Consumer-Duty trail",
     "used_for": "Proof a human decided — what the model advised and what the handler did."},
]


_DOC_SETS = {
    "motor": ["FNOL form", "Driving licence", "Incident photos", "Police reference",
              "Third-party insurer details", "Repair estimate"],
    "home": ["FNOL form", "Damage photos", "Proof of ownership", "Contractor quote",
             "Schedule of loss", "Surveyor report"],
}


def _doc_status(cid: str, doc: str, clean: bool) -> str:
    """Deterministic simulated document status. Clean claims (auto-close hero) have a
    complete pack; others have realistic gaps a head of claims struggles to see today."""
    import zlib
    if doc in ("FNOL form",):
        return "received"
    if clean:
        return "received"
    h = zlib.crc32(f"{cid}|{doc}".encode()) % 10
    return "received" if h <= 5 else ("awaited" if h <= 7 else "missing")


async def claim_track(cid: str) -> dict:
    """The end-to-end track for one claim: lifecycle timeline, the documents that came
    with it (received / awaited / missing — simulated), and what was done. Built for a
    head of claims who today has to stitch this together across systems by hand."""
    rows = await execute_query(f"""
        SELECT peril_type, product, total_incurred, claim_status,
               cast(loss_date AS string) loss_date, cast(report_date AS string) report_date,
               report_channel, reporting_lag_days, postcode_district, handler_id, handler_grade,
               fraud_score, triage_decision, reserve_bracket, cast(settlement_date AS string) settlement_date,
               days_to_settle, recovery_flag, recoverable_amount, description_text
        FROM {_fq('silver_claims_enriched')} WHERE claim_public_id = '{_esc(cid)}'""")
    if not rows:
        return {"found": False, "claim_public_id": cid}
    c = rows[0]
    disp = await claim_disposition(cid)
    reasoning = await claim_reasoning(cid)
    try:
        decisions = await execute_query(f"""
            SELECT handler_action, override_flag, override_reason, handler_id,
                   cast(decision_ts AS string) decision_ts
            FROM {_fq('gold_handler_decisions')} WHERE claim_public_id = '{_esc(cid)}'
            ORDER BY decision_ts DESC LIMIT 1""")
    except Exception:
        decisions = []

    auto = (disp.get("disposition") == "auto_closed")
    closed = bool(c.get("settlement_date")) or (c.get("claim_status") in ("settled", "closed", "declined", "withdrawn"))
    package = await _claim_package(cid)
    product = (c.get("product") or "home")
    docs = [{"name": d, "status": _doc_status(cid, d, auto or closed)} for d in _DOC_SETS.get(product, _DOC_SETS["home"])]
    received = sum(1 for d in docs if d["status"] == "received")

    # Lifecycle timeline — derived from the real claim facts + disposition.
    lc = []
    lc.append({"stage": "FNOL received", "when": c["report_date"],
               "detail": f"Reported via {c['report_channel']}, {c['reporting_lag_days']} days after the incident.", "status": "done"})
    lc.append({"stage": "Documents intake", "when": c["report_date"],
               "detail": f"{received} of {len(docs)} expected documents received.",
               "status": "done" if received == len(docs) else "partial"})
    lc.append({"stage": "Triage", "when": c["report_date"],
               "detail": f"Model recommended {disp.get('model_decision') or c.get('triage_decision')} "
                         f"({disp.get('model_confidence','—')}% confidence).", "status": "done"})
    lc.append({"stage": "Reserve set", "when": c["report_date"],
               "detail": f"Reserve bracket {c.get('reserve_bracket') or '—'}.", "status": "done"})
    lc.append({"stage": "Fraud screen", "when": c["report_date"],
               "detail": f"Fraud score {c.get('fraud_score')}/100.", "status": "done"})
    if auto:
        lc.append({"stage": "Auto-closed & paid", "when": c["report_date"],
                   "detail": f"Straight-through: all risk-appetite rules passed. {gbp_note(c['total_incurred'])} paid (simulated).", "status": "done"})
    else:
        lc.append({"stage": "Escalated to handler", "when": c["report_date"],
                   "detail": f"Routed to {c.get('handler_id') or 'a handler'} ({c.get('handler_grade') or '—'}).", "status": "done"})
        if decisions:
            d = decisions[0]
            lc.append({"stage": "Handler decision", "when": d.get("decision_ts"),
                       "detail": f"{'Override' if d.get('override_flag') else 'Accepted'}"
                                 f"{(' — ' + d['override_reason']) if d.get('override_reason') else ''}.", "status": "done"})
        elif closed:
            lc.append({"stage": "Handler assessment", "when": c.get("settlement_date") or c["report_date"],
                       "detail": "Reviewed and approved by the claims handler.", "status": "done"})
        else:
            lc.append({"stage": "Awaiting handler decision", "when": None,
                       "detail": "No human decision logged yet — outstanding.", "status": "awaited"})
    if c.get("recovery_flag"):
        lc.append({"stage": "Recovery " + ("pursued" if closed else "identified"), "when": None,
                   "detail": f"Subrogation — up to {gbp_note(c.get('recoverable_amount'))} recoverable from the third party.",
                   "status": "done" if closed else "awaited"})
    if c.get("settlement_date"):
        lc.append({"stage": "Settled & closed", "when": c["settlement_date"],
                   "detail": f"Closed in {c.get('days_to_settle')} days.", "status": "done"})
    if package:
        lc.append({"stage": "Closure package generated", "when": package.get("generated_at"),
                   "detail": "Full claim file compiled to PDF and stored in the governed UC Volume.", "status": "done"})

    actions = [{"actor": r["agent_name"], "detail": (r["reasoning_text"] or "")[:240]} for r in reasoning]
    if closed:
        gaps = []
    else:
        gaps = [f"{d['name']} — {d['status']}" for d in docs if d["status"] != "received"]
        if not auto and not decisions:
            gaps.append("Handler decision outstanding")
        if c.get("recovery_flag"):
            gaps.append(f"Recovery not yet pursued ({gbp_note(c.get('recoverable_amount'))})")

    return {"found": True, "claim_public_id": cid, "closed": closed,
            "claim": {k: c[k] for k in c if k != "description_text"},
            "disposition": disp.get("disposition"), "documents": docs,
            "doc_complete_pct": round(100 * received / len(docs)),
            "lifecycle": lc, "actions": actions, "gaps": gaps, "package": package}


async def _claim_package(cid: str) -> dict | None:
    """The registered closure package for a claim (None if not generated / table absent)."""
    try:
        rows = await execute_query(f"""
            SELECT file_name, volume_path, cast(generated_at AS string) generated_at, size_bytes
            FROM {_fq('gold_claim_packages')} WHERE claim_public_id = '{_esc(cid)}'""")
        return rows[0] if rows else None
    except Exception:
        return None


async def closure_packages() -> list[dict]:
    """All claims that have a closure package — drives the (closed) markers in the app."""
    try:
        return await execute_query(f"""
            SELECT claim_public_id, peril_type, cast(total_incurred AS double) total_incurred,
                   file_name, cast(generated_at AS string) generated_at
            FROM {_fq('gold_claim_packages')} ORDER BY total_incurred DESC""")
    except Exception:
        return []


async def claim_package_file(cid: str):
    """Stream the PDF closure package from the governed Volume (read via the app SP)."""
    pkg = await _claim_package(cid)
    if not pkg:
        return None
    path = pkg["volume_path"]

    def _dl():
        from server.sql import _client
        return _client().files.download(path).contents.read()

    try:
        data = await asyncio.to_thread(_dl)
    except Exception as e:
        logger.warning("package download failed for %s: %s", cid, e)
        return None
    return data, pkg["file_name"]


def gbp_note(v) -> str:
    try:
        return "£{:,.0f}".format(float(v))
    except Exception:
        return "£—"


def _agent_roster_sync() -> dict:
    """Resolve the live agent/model/Genie endpoints so every card clicks through to
    the actual Databricks artefact. DAB dev mode truncates/prefixes names → substring."""
    from server.sql import _client
    w = _client()
    host = w.config.host.rstrip("/")
    try:
        eps = [e.name for e in w.serving_endpoints.list()]
    except Exception:
        eps = []

    def find(sub):
        return next((n for n in eps if sub in n), None)

    def ep(name):
        return {"endpoint": name, "url": f"{host}/ml/endpoints/{name}" if name else None}

    # agents.deploy auto-names each agent endpoint
    # `agents_<catalog>-<schema>-agent_<first4>`. Resolve from the live list when the
    # app SP can see it, else construct the conventional name (portable + survives the
    # SP not having list-visibility on every endpoint).
    cat, sch = config.CATALOG, config.SCHEMA

    def agent_ep(tok):
        return ep(find(f"agent_{tok}") or f"agents_{cat}-{sch}-agent_{tok}")

    sup = config.ENDPOINT_SUPERVISOR or find("supervisor")
    agents = [
        {"name": "Claim 360 / Dossier", "color": "amber", "kind": "agent",
         "desc": "Assembles policy, history, FNOL, enrichment, fraud and recovery into one handler-ready narrative — everything in one place.",
         **agent_ep("cont")},
        {"name": "Fraud", "color": "red", "kind": "agent",
         "desc": "Returns a LOW / MEDIUM / HIGH fraud verdict with the specific signals behind it — fraud score, prior claims, reporting lag.",
         **agent_ep("frau")},
        {"name": "Challenge / Second-Opinion", "color": "violet", "kind": "agent",
         "desc": "Argues the OPPOSITE of the current disposition — caution if a claim was auto-closed, the case to release if it was escalated. No decision authority.",
         **agent_ep("chal")},
        {"name": "Recovery / Subrogation", "color": "green", "kind": "agent",
         "desc": "Flags recovery potential — whether money can be recovered from a third party (e.g. a not-at-fault motor loss) and the recoverable amount.",
         **agent_ep("reco")},
        {"name": "Audit / Reasoning", "color": "slate", "kind": "agent",
         "desc": "Writes the regulator-readable explanation of how a claim's decision was reached — rules, values, and that no agent had pay authority.",
         **agent_ep("audi")},
        {"name": "Senior Reserving Actuary", "color": "blue", "kind": "expert",
         "desc": "A second opinion on reserve adequacy — flags light escape-of-water reserves and proposes an overlay for a human actuary to sign off. Never books it.",
         **agent_ep("rese")},
        {"name": "Senior Loss Adjuster", "color": "amber", "kind": "expert",
         "desc": "The experienced adjuster's read — is the quantum right for this peril, what to inspect or verify before settling, and any handling red flags.",
         **agent_ep("adju")},
        {"name": "Coverage Counsel", "color": "slate", "kind": "expert",
         "desc": "The coverage question — does the policy respond? Product-vs-peril fit, sum insured vs amount, and the conditions/exclusions to check.",
         **agent_ep("cove")},
        {"name": "Consumer-Duty Reviewer", "color": "green", "kind": "expert",
         "desc": "Fair-outcomes / FCA Consumer Duty — was the customer treated fairly and consistently, any vulnerability signals, would it withstand Ombudsman scrutiny.",
         **agent_ep("cond")},
        {"name": "Triage model", "color": "blue", "kind": "tool",
         "desc": "A UC function scoring the FNOL triage classifier — pay_direct / escalate / refer_siu with a confidence %.",
         **ep(find("claims-workbench-triage"))},
        {"name": "Reserve model", "color": "blue", "kind": "tool",
         "desc": "A UC function scoring the reserve-bracket model — LOW / MEDIUM / HIGH / LARGE LOSS with an indicative £ range.",
         **ep(find("claims-workbench-reserve"))},
    ]
    genie = [
        {"name": "Ask the Book", "color": "teal", "kind": "genie",
         "desc": "Portfolio analytics in natural language — reserve development, settlement speed, geographic clustering, handler performance.",
         "endpoint": "genie", "url": f"{host}/genie/rooms/{config.GENIE_SPACE_ID}" if config.GENIE_SPACE_ID else None},
        {"name": "Ask Pricing + Claims", "color": "teal", "kind": "genie",
         "desc": "Cross-domain questions spanning claims and the policy/pricing population — loss ratio, premium adequacy, leakage vs premium.",
         "endpoint": "genie", "url": f"{host}/genie/rooms/{config.GENIE_JOINED_SPACE_ID}" if config.GENIE_JOINED_SPACE_ID else None},
    ]
    return {"supervisor": {"name": "Claims AI Supervisor", **ep(sup),
                           "desc": "Reads the question, classifies it against the specialist catalogue in a single Foundation-Model call, dispatches to the right one, synthesises the answer with source citations, and writes the routing decision to the audit log.",
                           "present": bool(config.ENDPOINT_SUPERVISOR)},
            "agents": agents, "genie": genie}


async def agent_roster() -> dict:
    return await asyncio.to_thread(_agent_roster_sync)


# Role -> agents.deploy endpoint token (first 4 chars of the UC model name suffix).
_EXPERT_TOKENS = {"reserving": "rese", "adjuster": "adju", "coverage": "cove", "conduct": "cond",
                  "fraud": "frau", "dossier": "cont", "context": "cont", "challenge": "chal",
                  "recovery": "reco", "audit": "audi"}
EXPERTS = [
    {"role": "reserving", "name": "Senior Reserving Actuary", "icon": "📐", "color": "blue",
     "blurb": "Is the reserve adequate?"},
    {"role": "adjuster", "name": "Senior Loss Adjuster", "icon": "🔧", "color": "amber",
     "blurb": "Is the quantum right — what to inspect?"},
    {"role": "coverage", "name": "Coverage Counsel", "icon": "⚖️", "color": "slate",
     "blurb": "Does the policy respond?"},
    {"role": "conduct", "name": "Consumer-Duty Reviewer", "icon": "🤝", "color": "green",
     "blurb": "Was the customer treated fairly?"},
]


def _agent_endpoint_name(token: str) -> str:
    from server.sql import _client
    try:
        eps = [e.name for e in _client().serving_endpoints.list()]
        m = next((n for n in eps if f"agent_{token}" in n), None)
        if m:
            return m
    except Exception:
        pass
    return f"agents_{CAT}-{SCH}-agent_{token}"


async def expert_opinion(cid: str, role: str, use_cache: bool | None = None) -> dict:
    """A named senior-expert agent's second opinion on one claim (cache-first)."""
    if use_cache is None:
        use_cache = _use_cache
    token = _EXPERT_TOKENS.get(role)
    if not token:
        return {"role": role, "error": "unknown expert"}
    ep = await asyncio.to_thread(_agent_endpoint_name, token)
    prompt = f"Give your expert second opinion on claim {cid}."
    payload = {"messages": [{"role": "user", "content": prompt}], "custom_inputs": {"claim_public_id": cid}}
    try:
        out = await asyncio.to_thread(get_agent_response, ep, payload, use_cache)
        msgs = out.get("response", {}).get("messages", [])
        text = msgs[-1].get("content", "") if msgs else ""
        return {"role": role, "endpoint": ep, "text": text, "cache": out.get("cache")}
    except Exception as e:
        return {"role": role, "endpoint": ep, "error": str(e)[:160]}


async def fair_outcomes() -> dict:
    """Consumer-Duty / fair-outcomes view: are similar claims handled CONSISTENTLY, and
    where to watch for vulnerability — the data behind the conduct reviewer."""
    s = _fq("silver_claims_enriched")
    d = _fq("gold_claim_disposition")
    try:
        by_channel = await execute_query(f"""
            SELECT s.report_channel channel, count(*) n,
                   round(100.0*avg(CASE WHEN d.disposition='auto_closed' THEN 1 ELSE 0 END),1) auto_rate,
                   round(100.0*avg(CASE WHEN s.leakage_flag THEN 1 ELSE 0 END),1) leak_rate
            FROM {s} s JOIN {d} d USING (claim_public_id) GROUP BY s.report_channel ORDER BY n DESC""")
        by_peril = await execute_query(f"""
            SELECT s.peril_type peril, count(*) n,
                   round(100.0*avg(CASE WHEN d.disposition='auto_closed' THEN 1 ELSE 0 END),1) auto_rate
            FROM {s} s JOIN {d} d USING (claim_public_id) GROUP BY s.peril_type ORDER BY n DESC""")
        watch = (await execute_query(f"""
            SELECT sum(CASE WHEN peril_type='home_fire' AND claim_status IN ('open','under_investigation') THEN 1 ELSE 0 END) distress_open,
                   sum(CASE WHEN prior_claims_12m >= 3 THEN 1 ELSE 0 END) repeat_customers
            FROM {s}"""))[0]
    except Exception:
        by_channel, by_peril, watch = [], [], {}
    return {"by_channel": by_channel, "by_peril": by_peril, "watch": watch}


# ==========================================================================
# Phase 12 · Stage B1 — Handler "My Queue" (persona lens: Sarah Chen)
# --------------------------------------------------------------------------
# Sarah Chen is a PERSONA, not a data row — she's a Senior handler on the
# Motor Complex desk. Her queue is a curated VIEW (peril + status filter), not
# a handler_id reassignment, so the sacred heroes' real data is never mutated.
# cc:900001 (motor, under_investigation, refer_siu) lands in "Needs you today";
# cc:900002 (home) is correctly absent. Each row opens the Work-a-claim detail.
# ==========================================================================
HANDLER_PERSONA = {"name": "Sarah Chen", "initials": "SC", "grade": "Senior",
                   "desk": "Motor Complex", "subtitle": "Senior Claims Handler · Motor Complex desk"}
_MOTOR_SLA = PERIL_SLA.get("motor_tp", 30)


def _parse_rules(v) -> list:
    """fired_rules arrives as a JSON string from the SQL statement API (or already a list)."""
    if isinstance(v, list):
        return v
    if isinstance(v, str) and v.strip():
        try:
            return json.loads(v)
        except Exception:
            return [x for x in v.strip("[]").replace('"', "").split(",") if x]
    return []


def _sla_for(peril):
    return PERIL_SLA.get(peril or "", DEFAULT_SLA)


def _q_item(r):
    """Build a queue item + its breach flag from a row (peril-aware SLA)."""
    days = int(r.get("days_open") or 0); fraud = int(r.get("fraud_score") or 0)
    triage = r.get("triage_decision"); incurred = float(r.get("total_incurred") or 0)
    is_hv = int(r.get("is_high_value") or 0) == 1; fired = _parse_rules(r.get("fired_rules"))
    sla = _sla_for(r.get("peril_type")); breached = days > sla
    why = []
    if triage == "refer_siu":
        why.append("Referred to SIU")
    elif triage == "escalate":
        why.append("Triage: escalate")
    if fraud > 70:
        why.append(f"Fraud {fraud}/100")
    if breached:
        why.append(f"{days}d open — SLA {sla}d breached")
    elif days > (sla - 7):
        why.append(f"{days}d open — SLA in {max(sla - days, 0)}d")
    if is_hv:
        why.append("High value")
    if fired and len(why) < 3:
        why.append("Rules: " + ", ".join(fired))
    item = {"claim_public_id": r["claim_public_id"], "peril": r["peril_type"],
            "loss_cause": r.get("loss_cause"), "total_incurred": incurred, "fraud_score": fraud,
            "status": r.get("claim_status"), "triage": triage, "days_open": days,
            "postcode": r.get("postcode_district"), "disposition": r.get("disposition"),
            "fired_rules": fired, "confidence": r.get("model_confidence"),
            "why": why[:3], "hero": r["claim_public_id"] in ("cc:900001", "cc:900003")}
    return item, breached


async def _handler_options() -> list[dict]:
    """The dropdown — handlers with an open caseload (Sarah is the default persona lens)."""
    try:
        rows = await execute_query(f"""
            SELECT h.handler_id, h.handler_name, h.grade, h.team,
                   sum(CASE WHEN s.claim_status IN ('open','under_investigation') THEN 1 ELSE 0 END) open_n
            FROM {_fq('ref_handlers')} h LEFT JOIN {_fq('silver_claims_enriched')} s ON s.handler_id = h.handler_id
            GROUP BY h.handler_id, h.handler_name, h.grade, h.team
            HAVING open_n > 0 ORDER BY open_n DESC LIMIT 40""")
    except Exception:
        rows = []
    opts = [{"id": "sarah", "name": "Sarah Chen", "grade": "Senior", "team": "Motor Complex", "open": None}]
    for r in rows:
        opts.append({"id": r["handler_id"], "name": r.get("handler_name") or r["handler_id"],
                     "grade": (r.get("grade") or "").title(), "team": (r.get("team") or "").replace("_", " ").title(),
                     "open": int(r.get("open_n") or 0)})
    return opts


async def handler_queue(handler: str | None = None) -> dict:
    options = await _handler_options()
    persona_view = (not handler) or handler == "sarah"
    if not persona_view:
        return await _handler_queue_real(handler, options)
    s = _fq("silver_claims_enriched")
    d = _fq("gold_claim_disposition")
    # The whole motor book is thousands of open claims — far too many for one desk.
    # The triage layer has already sifted it; Sarah's worklist is a curated, bounded
    # slice (the material claims), with the heroes pinned in regardless of ranking.
    cols = ("claim_public_id, peril_type, loss_cause, CAST(total_incurred AS double) total_incurred, "
            "CAST(fraud_score AS int) fraud_score, claim_status, triage_decision, "
            "CAST(coalesce(prior_claims_12m,0) AS int) prior_claims_12m, "
            "CAST(is_high_value AS int) is_high_value, days_open, postcode_district, "
            "disposition, fired_rules, CAST(model_confidence AS double) model_confidence")
    base = f"""
        SELECT s.claim_public_id, s.peril_type, s.loss_cause, s.total_incurred, s.fraud_score,
               s.claim_status, s.triage_decision, s.prior_claims_12m,
               CAST(s.is_high_value AS int) is_high_value,
               datediff(current_date(), s.report_date) days_open, s.postcode_district,
               d.disposition, d.fired_rules, d.model_confidence
        FROM {s} s LEFT JOIN {d} d ON s.claim_public_id = d.claim_public_id
        WHERE s.peril_type = 'motor_tp' AND s.claim_status IN ('open','under_investigation')"""
    rows = await execute_query(f"""
        WITH base AS ({base})
        SELECT {cols}, 'today' AS bucket FROM base WHERE claim_public_id IN ('cc:900001','cc:900003')
        UNION ALL
        SELECT {cols}, 'today' AS bucket FROM (SELECT * FROM base
            WHERE claim_public_id NOT IN ('cc:900001','cc:900003')
              AND (triage_decision IN ('refer_siu','escalate') OR fraud_score > 70)
            ORDER BY fraud_score DESC, total_incurred DESC LIMIT 7)
        UNION ALL
        SELECT {cols}, 'week' AS bucket FROM (SELECT * FROM base
            WHERE claim_public_id NOT IN ('cc:900001','cc:900003')
              AND is_high_value = 1 AND triage_decision NOT IN ('refer_siu','escalate') AND fraud_score <= 70
              AND days_open <= 120
            ORDER BY total_incurred DESC LIMIT 7)
        UNION ALL
        SELECT {cols}, 'later' AS bucket FROM (SELECT * FROM base
            WHERE claim_public_id NOT IN ('cc:900001','cc:900003')
              AND triage_decision = 'pay_direct' AND fraud_score <= 40 AND is_high_value = 0
              AND days_open <= {_MOTOR_SLA}
            ORDER BY days_open DESC LIMIT 8)
    """)
    total_open = int((await execute_query(
        f"SELECT count(*) c FROM {s} WHERE peril_type='motor_tp' AND claim_status IN ('open','under_investigation')"))[0]["c"])
    by_bucket = {"today": [], "week": [], "later": []}
    exposure = 0.0
    for r in rows:
        fired = _parse_rules(r.get("fired_rules"))
        days = int(r.get("days_open") or 0)
        fraud = int(r.get("fraud_score") or 0)
        triage = r.get("triage_decision")
        incurred = float(r.get("total_incurred") or 0)
        is_hv = int(r.get("is_high_value") or 0) == 1
        exposure += incurred
        breached = days > _MOTOR_SLA
        why = []
        if triage == "refer_siu":
            why.append("Referred to SIU")
        elif triage == "escalate":
            why.append("Triage: escalate")
        if fraud > 70:
            why.append(f"Fraud {fraud}/100")
        if breached:
            why.append(f"{days}d open — SLA {_MOTOR_SLA}d breached")
        elif days > (_MOTOR_SLA - 7):
            why.append(f"{days}d open — SLA in {max(_MOTOR_SLA - days, 0)}d")
        if is_hv:
            why.append("High value")
        if fired and len(why) < 3:
            why.append("Rules: " + ", ".join(fired))
        item = {
            "claim_public_id": r["claim_public_id"], "peril": r["peril_type"],
            "loss_cause": r.get("loss_cause"), "total_incurred": incurred,
            "fraud_score": fraud, "status": r.get("claim_status"), "triage": triage,
            "days_open": days, "postcode": r.get("postcode_district"),
            "disposition": r.get("disposition"), "fired_rules": fired,
            "confidence": r.get("model_confidence"),
            "why": why[:3], "hero": r["claim_public_id"] in ("cc:900001", "cc:900003"),
        }
        by_bucket.get(r.get("bucket"), by_bucket["later"]).append(item)
    today, week, later = by_bucket["today"], by_bucket["week"], by_bucket["later"]

    worklist = len(today) + len(week) + len(later)
    n_siu = sum(1 for i in today if i["triage"] in ("refer_siu", "escalate"))
    n_breach = sum(1 for i in (today + week) if i["days_open"] > _MOTOR_SLA)
    n_fraud = sum(1 for i in today if i["fraud_score"] > 70)
    summary = (
        f"Morning, Sarah. The triage layer sifted {total_open:,} open motor claims down to "
        f"{worklist} that need a senior's eyes. {len(today)} are for today — {n_siu} escalated / SIU, "
        f"{n_breach} past their {_MOTOR_SLA}-day SLA, {n_fraud} with a high fraud score. "
        f"Start with cc:900001 (SIU referral) — the model and your agents already have the dossier ready."
    )
    buckets = [
        {"key": "today", "title": "Needs you today", "tone": "red",
         "blurb": "Escalations, SIU referrals, SLA breaches and fired rules.",
         "count": len(today), "items": today},
        {"key": "week", "title": "This week", "tone": "amber",
         "blurb": "High-value or approaching their service deadline.",
         "count": len(week), "items": week},
        {"key": "later", "title": "When you can", "tone": "slate",
         "blurb": "Routine open motor claims, progressing within SLA.",
         "count": len(later), "items": later},
    ]
    return {"persona": HANDLER_PERSONA, "summary": summary, "worklist": worklist,
            "total_open": total_open, "exposure": exposure, "buckets": buckets,
            "handlers": options, "selected": "sarah"}


async def _handler_queue_real(handler: str, options: list[dict]) -> dict:
    """A real handler's own open caseload (all perils), bucketed by urgency."""
    s = _fq("silver_claims_enriched"); d = _fq("gold_claim_disposition")
    opt = next((o for o in options if o["id"] == handler), None)
    name = (opt or {}).get("name", handler)
    persona = {"name": name, "initials": "".join(w[0] for w in name.split()[:2]).upper() or "—",
               "grade": (opt or {}).get("grade", ""), "desk": (opt or {}).get("team", ""),
               "subtitle": f"{(opt or {}).get('grade','Handler')} · {(opt or {}).get('team','')} desk"}
    rows = await execute_query(f"""
        SELECT s.claim_public_id, s.peril_type, s.loss_cause, CAST(s.total_incurred AS double) total_incurred,
               CAST(s.fraud_score AS int) fraud_score, s.claim_status, s.triage_decision,
               CAST(coalesce(s.prior_claims_12m,0) AS int) prior_claims_12m, CAST(s.is_high_value AS int) is_high_value,
               datediff(current_date(), s.report_date) days_open, s.postcode_district,
               d.disposition, d.fired_rules, CAST(d.model_confidence AS double) model_confidence
        FROM {s} s LEFT JOIN {d} d ON s.claim_public_id = d.claim_public_id
        WHERE s.handler_id = '{_esc(handler)}' AND s.claim_status IN ('open','under_investigation')
        ORDER BY (CASE WHEN s.triage_decision='refer_siu' THEN 0 WHEN s.triage_decision='escalate' THEN 1 ELSE 2 END),
                 s.fraud_score DESC, s.total_incurred DESC LIMIT 60""")
    today, week, later = [], [], []
    exposure = 0.0
    for r in rows:
        item, breached = _q_item(r)
        exposure += item["total_incurred"]
        sla = _sla_for(item["peril"])
        urgent = item["triage"] in ("refer_siu", "escalate") or item["fraud_score"] > 70
        soon = breached or int(r.get("is_high_value") or 0) == 1 or item["days_open"] > (sla - 7)
        if urgent and len(today) < 10:
            today.append(item)
        elif (urgent or soon) and len(week) < 16:   # urgent overflow + aged/high-value spill here
            week.append(item)
        elif len(later) < 16:
            later.append(item)
    worklist = len(today) + len(week) + len(later)
    summary = (f"{name}'s desk: {len(rows)} open claims. {len(today)} need attention today "
               f"(escalations, SIU, high fraud or past SLA), {len(week)} this week. "
               f"Total exposure £{exposure:,.0f}.")
    buckets = [
        {"key": "today", "title": "Needs you today", "tone": "red",
         "blurb": "Escalations, SIU referrals, SLA breaches and high fraud.", "count": len(today), "items": today},
        {"key": "week", "title": "This week", "tone": "amber",
         "blurb": "Fired a rule, high-value, or approaching the service deadline.", "count": len(week), "items": week},
        {"key": "later", "title": "When you can", "tone": "slate",
         "blurb": "Routine open claims, progressing within SLA.", "count": len(later), "items": later},
    ]
    return {"persona": persona, "summary": summary, "worklist": worklist,
            "total_open": len(rows), "exposure": exposure, "buckets": buckets,
            "handlers": options, "selected": handler}


# ==========================================================================
# Phase 12 · Stage B2 — Create a claim (synchronous REAL scoring, ephemeral)
# --------------------------------------------------------------------------
# Scores the champion triage + reserve serving endpoints LIVE via ai_query on a
# feature vector built in-flight (no batch DLT), then runs the same R1-R7 rule
# engine inline. Created claims are EPHEMERAL — persisted only to the isolated
# app_sandbox_claims table (truncated by the Reset job); silver and the sacred
# heroes are never touched.
# ==========================================================================
_PERIL_ENC = {"home_escape_water": 0, "home_fire": 1, "home_storm": 2, "motor_tp": 3}
_CHANNEL_ENC = {"broker_email": 0, "digital": 1, "phone": 2}
_TRIAGE_ENC = {"escalate": 0, "pay_direct": 1, "refer_siu": 2}
_RESERVE_BRACKETS = ["LOW", "MEDIUM", "HIGH", "LARGE LOSS"]
_RESERVE_RANGE = {"LOW": "under £2,000", "MEDIUM": "£2,000–£10,000",
                  "HIGH": "£10,000–£50,000", "LARGE LOSS": "over £50,000"}
_model_eps: dict | None = None

SCENARIO_PRESETS = [
    {"key": "clean_motor", "label": "Clean motor knock", "peril_type": "motor_tp",
     "report_channel": "digital", "reported_amount": 900, "sum_insured": 24000,
     "fraud_score": 6, "prior_claims_12m": 0, "reporting_lag_days": 1, "policy_tenure_years": 4.0,
     "weather_risk_composite": 0.15, "at_fault": 0, "third_party_involved": 1, "flood_risk_score": 0.1,
     "hint": "Low value, prompt, clean history → expect pay & auto-close."},
    {"key": "late_suspicious", "label": "Late, suspicious motor", "peril_type": "motor_tp",
     "report_channel": "phone", "reported_amount": 8500, "sum_insured": 22000,
     "fraud_score": 78, "prior_claims_12m": 3, "reporting_lag_days": 25, "policy_tenure_years": 0.4,
     "weather_risk_composite": 0.2, "at_fault": 1, "third_party_involved": 1, "flood_risk_score": 0.1,
     "hint": "High fraud, repeat, reported late → expect SIU / escalate, rules fire."},
    {"key": "escape_water", "label": "Escape of water (home)", "peril_type": "home_escape_water",
     "report_channel": "digital", "reported_amount": 3200, "sum_insured": 350000,
     "fraud_score": 8, "prior_claims_12m": 0, "reporting_lag_days": 2, "policy_tenure_years": 6.0,
     "weather_risk_composite": 0.3, "at_fault": 0, "third_party_involved": 0, "flood_risk_score": 0.2,
     "hint": "Clean but above the £2,000 pay cap → expect a handler review."},
    {"key": "large_fire", "label": "Large home fire", "peril_type": "home_fire",
     "report_channel": "broker_email", "reported_amount": 85000, "sum_insured": 420000,
     "fraud_score": 10, "prior_claims_12m": 1, "reporting_lag_days": 3, "policy_tenure_years": 9.0,
     "weather_risk_composite": 0.25, "at_fault": 0, "third_party_involved": 0, "flood_risk_score": 0.15,
     "hint": "Major loss well over the pay cap → expect escalation for a handler."},
]
PRESET_FIELDS = ["peril_type", "report_channel", "reported_amount", "sum_insured",
                 "fraud_score", "prior_claims_12m", "reporting_lag_days", "policy_tenure_years",
                 "weather_risk_composite", "at_fault", "third_party_involved", "flood_risk_score"]


def _resolve_model_endpoints() -> dict:
    global _model_eps
    if _model_eps is not None:
        return _model_eps
    from server.sql import _client
    triage = reserve = None
    try:
        names = [e.name for e in _client().serving_endpoints.list()]
        triage = next((n for n in names if n.endswith("claims-workbench-triage")), None)
        reserve = next((n for n in names if n.endswith("claims-workbench-reserve")), None)
    except Exception as e:
        logger.warning("model endpoint resolution failed: %s", e)
    _model_eps = {"triage": triage, "reserve": reserve}
    return _model_eps


async def list_policies(limit: int = 8) -> list[dict]:
    """A few REAL policies from silver to attach a sandbox claim to (picker)."""
    return await execute_query(f"""
        SELECT policy_number, any_value(product) product,
               CAST(any_value(sum_insured) AS double) sum_insured,
               CAST(any_value(annual_premium) AS double) annual_premium,
               CAST(max(policy_tenure_years) AS double) policy_tenure_years
        FROM {_fq('silver_claims_enriched')}
        WHERE policy_number IS NOT NULL AND sum_insured IS NOT NULL
        GROUP BY policy_number ORDER BY policy_number LIMIT {int(limit)}
    """)


async def create_claim_scenario() -> dict:
    """Form metadata for the create-a-claim panel."""
    policies = await list_policies()
    return {"presets": SCENARIO_PRESETS, "policies": policies, "fields": PRESET_FIELDS}


async def _score_models(inp: dict) -> dict:
    """One SQL round-trip: ai_query the triage endpoint, derive the decision, then
    ai_query the reserve endpoint with the resulting triage encoding. Synchronous."""
    eps = await asyncio.to_thread(_resolve_model_endpoints)
    if not eps["triage"] or not eps["reserve"]:
        raise RuntimeError("triage/reserve serving endpoints not found")
    peril_e = _PERIL_ENC.get(inp["peril_type"], -1)
    chan_e = _CHANNEL_ENC.get(inp["report_channel"], -1)
    amt = float(inp["reported_amount"]); si = float(inp["sum_insured"]) or 1.0
    amt_log = math.log1p(amt); si_log = math.log1p(si)
    ratio = round(amt / si, 4) if si else 0.0
    is_hv = 1 if amt > 10000 else 0
    fraud = int(inp["fraud_score"]); prior = int(inp["prior_claims_12m"])
    lag = int(inp["reporting_lag_days"]); tenure = float(inp["policy_tenure_years"])
    weather = float(inp["weather_risk_composite"]); flood = float(inp["flood_risk_score"])
    at_fault = int(inp["at_fault"]); tp = int(inp["third_party_involved"])
    triage_struct = (
        f"named_struct('peril_type_encoded',{peril_e}.0,'report_channel_encoded',{chan_e}.0,"
        f"'reported_amount_log',{amt_log},'sum_insured_to_reported_ratio',{ratio},"
        f"'fraud_score',{fraud}.0,'prior_claims_12m',{prior}.0,'reporting_lag_days',{lag}.0,"
        f"'policy_tenure_years',{tenure},'weather_risk_composite',{weather},'is_high_value',{is_hv}.0,"
        f"'at_fault',{at_fault}.0,'third_party_involved',{tp}.0,'postcode_flood_risk',{flood})")
    # reserve uses the triage decision encoding (derived in-query), handler_grade=senior(1), days_open=0.
    reserve_struct = (
        "named_struct('peril_type_encoded',{pe}.0,'handler_grade_encoded',1.0,"
        "'reported_amount_log',{al},'fraud_score',{fr}.0,'prior_claims_12m',{pr}.0,"
        "'weather_risk_composite',{we},'days_open',0.0,'triage_decision_encoded',tde,"
        "'sum_insured_log',{sl})").format(pe=peril_e, al=amt_log, fr=fraud, pr=prior, we=weather, sl=si_log)
    sql = f"""
      WITH t AS (SELECT ai_query('{eps['triage']}', {triage_struct}, 'ARRAY<DOUBLE>') AS p),
      td AS (SELECT p,
               element_at(array('escalate','pay_direct','refer_siu'), CAST(array_position(p, array_max(p)) AS INT)) AS decision,
               round(array_max(p)*100, 1) AS confidence FROM t),
      r AS (SELECT decision, confidence,
               CAST(decision='escalate' AS INT)*0 + CAST(decision='pay_direct' AS INT)*1 + CAST(decision='refer_siu' AS INT)*2 AS tde
            FROM td)
      SELECT decision, confidence,
             element_at(array('LOW','MEDIUM','HIGH','LARGE LOSS'),
               CAST(ai_query('{eps['reserve']}',
                 {reserve_struct.replace('tde', '(SELECT tde FROM r)')}, 'DOUBLE') AS INT) + 1) AS bracket
      FROM r
    """
    rows = await execute_query(sql)
    out = rows[0] if rows else {}
    return {"decision": out.get("decision"), "confidence": float(out.get("confidence") or 0),
            "bracket": out.get("bracket"), "triage_endpoint": eps["triage"], "reserve_endpoint": eps["reserve"]}


async def _rule_params(peril_type: str) -> dict:
    """One SQL round-trip: risk-appetite band + rule thresholds + per-peril severity norm."""
    rows = await execute_query(f"""
        SELECT a.conf_threshold, a.amount_cap, a.fraud_floor,
               r.lag_limit, r.velocity_limit, r.ratio_ceiling, r.severity_mult,
               (SELECT CAST(avg(total_incurred) AS double) FROM {_fq('silver_claims_enriched')}
                 WHERE peril_type='{_esc(peril_type)}') AS peril_avg
        FROM (SELECT * FROM {_fq('auto_close_config')} WHERE config_key='default') a
        CROSS JOIN (SELECT * FROM {_fq('rule_config')} WHERE config_key='default') r
    """)
    return rows[0] if rows else {}


def _rule_engine(inp: dict, decision: str, confidence: float, params: dict) -> dict:
    """Replicate the 10_auto_close disposition (band + R1-R7) inline for one new claim.
    R6 (telematics) and R7 (image) pass — a brand-new claim has neither yet."""
    cfg = params or {}
    CONF = float(cfg.get("conf_threshold") or 85.0); CAP = float(cfg.get("amount_cap") or 2000.0)
    FLOOR = float(cfg.get("fraud_floor") or 20.0)
    LAG = float(cfg.get("lag_limit") or 14.0); VEL = float(cfg.get("velocity_limit") or 1.0)
    RATIOC = float(cfg.get("ratio_ceiling") or 0.9); SEVM = float(cfg.get("severity_mult") or 5.0)
    peril_avg = float(cfg["peril_avg"]) if cfg.get("peril_avg") is not None else None
    amt = float(inp["reported_amount"]); si = float(inp["sum_insured"]) or 1.0
    ratio = amt / si if si else 0.0
    fraud = int(inp["fraud_score"]); prior = int(inp["prior_claims_12m"]); lag = int(inp["reporting_lag_days"])

    checks = [
        ("band", "triage = pay_direct", decision == "pay_direct", f"triage={decision}"),
        ("band", f"confidence ≥ {CONF:.0f}%", confidence >= CONF, f"{confidence:.1f}%"),
        ("band", f"amount ≤ £{CAP:,.0f}", amt <= CAP, f"£{amt:,.0f}"),
        ("band", "FNOL data complete", True, "complete"),
        ("R1", f"fraud ≤ {FLOOR:.0f}", fraud <= FLOOR, f"fraud {fraud}"),
        ("R2", f"reporting-lag ≤ {LAG:.0f}d", lag <= LAG, f"{lag}d"),
        ("R3", f"prior-claims ≤ {VEL:.0f}", prior <= VEL, f"{prior} prior"),
        ("R4", "amount/sum-insured ok", ratio <= RATIOC, f"ratio {ratio:.3f}"),
        ("R5", "severity consistent", peril_avg is None or amt <= SEVM * peril_avg,
         f"£{amt:,.0f} vs norm £{(peril_avg or 0):,.0f}"),
        ("R6", "speed vs limit", True, "no telematics"),
        ("R7", "image severity vs reported", True, "no photo"),
    ]
    results = [{"code": c, "label": lab, "passed": bool(ok), "value": val} for c, lab, ok, val in checks]
    fired = [r["code"] for r in results if not r["passed"] and r["code"].startswith("R")]
    band_ok = all(r["passed"] for r in results if r["code"] == "band")
    auto_ok = all(r["passed"] for r in results)
    disposition = "auto_closed" if auto_ok else "escalated"
    return {"disposition": disposition, "auto_ok": auto_ok, "band_ok": band_ok,
            "fired_rules": fired, "checks": results,
            "thresholds": {"conf": CONF, "cap": CAP, "fraud_floor": FLOOR}}


async def _ensure_sandbox_table():
    await execute_query(f"""
        CREATE TABLE IF NOT EXISTS {_fq('app_sandbox_claims')} (
          claim_public_id STRING, created_ts TIMESTAMP, scenario STRING, policy_number STRING,
          peril_type STRING, reported_amount DOUBLE, sum_insured DOUBLE, fraud_score INT,
          prior_claims_12m INT, reporting_lag_days INT, model_decision STRING, model_confidence DOUBLE,
          reserve_bracket STRING, disposition STRING, fired_rules STRING
        ) USING DELTA
        COMMENT 'EPHEMERAL app-created sandbox claims (Phase 12 B2). Truncated by the Reset job. Never joined to silver/heroes.'
    """)


async def create_claim(inputs: dict) -> dict:
    """Synchronous real scoring of a new ephemeral claim + inline rule engine."""
    inp = {f: inputs.get(f) for f in PRESET_FIELDS}
    # coerce / default
    inp["peril_type"] = inp.get("peril_type") or "motor_tp"
    inp["report_channel"] = inp.get("report_channel") or "digital"
    for f, d in (("reported_amount", 1000), ("sum_insured", 25000), ("fraud_score", 10),
                 ("prior_claims_12m", 0), ("reporting_lag_days", 2), ("policy_tenure_years", 3.0),
                 ("weather_risk_composite", 0.2), ("at_fault", 0), ("third_party_involved", 0),
                 ("flood_risk_score", 0.1)):
        if inp.get(f) is None:
            inp[f] = d
    scored, params = await asyncio.gather(_score_models(inp), _rule_params(inp["peril_type"]))
    rules = _rule_engine(inp, scored["decision"], scored["confidence"], params)
    cid = "cc:sb-" + uuid.uuid4().hex[:6]
    # Persist OFF the critical path — the scored result returns immediately; the
    # ephemeral sandbox row is written in the background (the UI re-reads the
    # sandbox list a beat later). Keeps interactive scoring fast.
    async def _persist():
        try:
            await _ensure_sandbox_table()
            await execute_query(f"""
                INSERT INTO {_fq('app_sandbox_claims')} VALUES
                ('{cid}', current_timestamp(), '{_esc(str(inputs.get('scenario','custom')))}',
                 '{_esc(str(inputs.get('policy_number','') or ''))}', '{_esc(inp['peril_type'])}',
                 {float(inp['reported_amount'])}, {float(inp['sum_insured'])}, {int(inp['fraud_score'])},
                 {int(inp['prior_claims_12m'])}, {int(inp['reporting_lag_days'])},
                 '{_esc(scored['decision'] or '')}', {scored['confidence']},
                 '{_esc(scored['bracket'] or '')}', '{rules['disposition']}',
                 '{_esc(json.dumps(rules['fired_rules']))}')
            """)
        except Exception as e:
            logger.warning("sandbox persist failed (non-fatal): %s", e)
    asyncio.create_task(_persist())
    return {
        "claim_public_id": cid, "inputs": inp,
        "scenario": inputs.get("scenario", "custom"), "policy_number": inputs.get("policy_number"),
        "triage": {"decision": scored["decision"], "confidence": scored["confidence"],
                   "endpoint": scored["triage_endpoint"]},
        "reserve": {"bracket": scored["bracket"],
                    "range": _RESERVE_RANGE.get(scored["bracket"], ""),
                    "endpoint": scored["reserve_endpoint"]},
        "disposition": rules["disposition"], "fired_rules": rules["fired_rules"],
        "checks": rules["checks"], "thresholds": rules["thresholds"], "ephemeral": True,
    }


async def sandbox_claims(limit: int = 20) -> list[dict]:
    try:
        return await execute_query(f"""
            SELECT claim_public_id, cast(created_ts AS string) created_ts, scenario, peril_type,
                   reported_amount, model_decision, model_confidence, reserve_bracket,
                   disposition, fired_rules
            FROM {_fq('app_sandbox_claims')} ORDER BY created_ts DESC LIMIT {int(limit)}
        """)
    except Exception:
        return []


async def governance_inventory() -> dict:
    counts = {}
    for t in ("silver_claims_enriched", "agent_reasoning_log", "gold_claim_disposition", "gold_handler_decisions"):
        try:
            counts[t] = int((await execute_query(f"SELECT count(*) c FROM {_fq(t)}"))[0]["c"])
        except Exception:
            counts[t] = None
    return {"inventory": _INVENTORY, "counts": counts,
            "tiers": [
                {"tier": "PII", "rule": "postcode, names — masked unless in claims_workbench_pii_readers"},
                {"tier": "SECRET", "rule": "claim narrative / health — withheld unless claims_workbench_secret_readers; CMK-encrypted at rest"},
            ]}


# --------------------------------------------------------------------------
# Broker Portal — broker-scoped self-service book (helpline-deflection story).
# Each "signed-in" broker reads its own pre-filtered view (the mock row filter);
# the views expose broker-safe columns only — never fraud or handler fields.
# --------------------------------------------------------------------------
BROKER_VIEWS = {"BRK-001": "v_broker_aldgate_claims",
                "BRK-002": "v_broker_caldwell_claims",
                "BRK-003": "v_broker_northgate_claims"}


async def _broker_roster() -> list[dict]:
    return await execute_query(f"""
        SELECT b.broker_id, b.broker_name, b.producer_code, b.segment,
               b.contact_name, b.contact_email,
               count(c.claim_public_id) AS book_claims,
               sum(CASE WHEN c.claim_status IN ('open','under_investigation') THEN 1 ELSE 0 END) AS open_claims
        FROM {_fq('ref_broker')} b
        LEFT JOIN {_fq('gold_broker_claims')} c ON b.broker_id = c.broker_id
        GROUP BY b.broker_id, b.broker_name, b.producer_code, b.segment, b.contact_name, b.contact_email
        ORDER BY b.broker_id""")


async def broker_portal(broker: str | None = None) -> dict:
    brokers = await _broker_roster()
    view = BROKER_VIEWS.get(broker or "")
    if not view:
        return {"brokers": brokers}
    v = _fq(view)
    open_st = "claim_status IN ('open','under_investigation')"
    kpis_q = execute_query(f"""
        SELECT count(*) total,
          sum(CASE WHEN {open_st} THEN 1 ELSE 0 END) open,
          sum(CASE WHEN claim_status='under_investigation' THEN 1 ELSE 0 END) under_review,
          sum(CASE WHEN report_date >= date_sub(current_date(), 30) THEN 1 ELSE 0 END) new_30d,
          sum(CASE WHEN claim_status='settled' AND last_update >= date_sub(current_date(), 30) THEN 1 ELSE 0 END) settled_30d,
          round(avg(CASE WHEN {open_st} THEN days_open END), 1) avg_days_open,
          sum(CASE WHEN {open_st} THEN outstanding_estimate ELSE 0 END) outstanding_total
        FROM {v}""")
    claims_q = execute_query(f"""
        SELECT claim_public_id, claim_number, policy_number, client_name, product, peril_type,
               loss_cause, postcode_district, cast(loss_date AS string) loss_date,
               cast(report_date AS string) report_date, claim_status, stage, next_step,
               paid_amount, outstanding_estimate, cast(last_update AS string) last_update, days_open
        FROM {v} WHERE {open_st}
        ORDER BY last_update DESC, days_open LIMIT 60""")
    recent_q = execute_query(f"""
        SELECT claim_public_id, client_name, peril_type, stage, paid_amount,
               cast(last_update AS string) last_update, days_open
        FROM {v} WHERE claim_status IN ('settled','declined','withdrawn')
        ORDER BY last_update DESC LIMIT 10""")
    stage_q = execute_query(
        f"SELECT stage, count(*) n FROM {v} WHERE {open_st} GROUP BY stage ORDER BY n DESC")
    peril_q = execute_query(
        f"SELECT peril_type, count(*) n FROM {v} GROUP BY peril_type ORDER BY n DESC")
    ageing_q = execute_query(f"""
        SELECT CASE WHEN days_open <= 7 THEN '0–7 days' WHEN days_open <= 30 THEN '8–30 days'
                    WHEN days_open <= 90 THEN '31–90 days' ELSE '90+ days' END bucket, count(*) n
        FROM {v} WHERE {open_st} GROUP BY 1""")
    kpis, claims, recent, stage_mix, peril_mix, ageing = await asyncio.gather(
        kpis_q, claims_q, recent_q, stage_q, peril_q, ageing_q)

    def _links():
        from server.sql import _client
        host = _client().config.host.rstrip("/")
        gid = config.GENIE_SPACE_ID
        return {"genie_embed_url": f"{host}/embed/genie/rooms/{gid}" if gid else None,
                "genie_url": f"{host}/genie/rooms/{gid}" if gid else None,
                "view_url": f"{host}/explore/data/{config.CATALOG}/{config.SCHEMA}/{view}"}
    links = await asyncio.to_thread(_links)
    profile = next((b for b in brokers if b.get("broker_id") == broker), {})
    order = ["0–7 days", "8–30 days", "31–90 days", "90+ days"]
    ageing = sorted(ageing, key=lambda r: order.index(r["bucket"]) if r["bucket"] in order else 9)
    return {"brokers": brokers, "profile": profile, "view_name": f"{CAT}.{SCH}.{view}",
            "kpis": kpis[0] if kpis else {}, "claims": claims, "recent": recent,
            "stage_mix": stage_mix, "peril_mix": peril_mix, "ageing": ageing, **links}
