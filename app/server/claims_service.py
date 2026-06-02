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
import uuid

from utils import config
from utils.agent_cache import get_agent_response
from server.sql import execute_query

logger = logging.getLogger(__name__)
CAT, SCH = config.CATALOG, config.SCHEMA


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
async def get_panels(cid: str) -> dict:
    summary, triage, reserve, fraud, policy = await asyncio.gather(
        _fn("fn_claim_summary", cid), _fn("fn_triage_claim", cid), _fn("fn_reserve_claim", cid),
        _fn("fn_fraud_signals", cid), _fn("fn_policy_history", cid))
    extra_rows = await execute_query(f"""
        SELECT policy_number, weather_risk_composite, flood_risk_score, wind_risk_score,
               freeze_risk_score, prior_claims_12m, at_fault, reporting_lag_days
        FROM {_fq('silver_claims_enriched')} WHERE claim_public_id = '{_esc(cid)}'
    """)
    extra = extra_rows[0] if extra_rows else {}
    return {"claim_public_id": cid, "summary": summary, "triage": triage,
            "reserve": reserve, "fraud": fraud, "policy": policy, "extra": extra}


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


def _ingestion_sync() -> dict:
    import requests
    from server.sql import _client
    w = _client()
    host = w.config.host.rstrip("/")
    hdr = w.config._header_factory()
    pid = pname = state = None
    try:
        # DAB dev mode prefixes the name (e.g. "[dev user] claims_workbench_01_bronze_dlt"),
        # so match by substring rather than exact name.
        pipes = list(w.pipelines.list_pipelines(filter=f"name LIKE '%{PIPELINE_NAME}%'"))
        if not pipes:
            pipes = [p for p in w.pipelines.list_pipelines() if PIPELINE_NAME in (p.name or "")]
        if pipes:
            pid, pname = pipes[0].pipeline_id, pipes[0].name
            d = w.pipelines.get(pid)
            state = str(d.state).replace("PipelineState.", "") if d.state else None
    except Exception as e:
        logger.warning("pipeline lookup failed: %s", e)
    expectations = {}
    if pid:
        try:
            evs = requests.get(f"{host}/api/2.0/pipelines/{pid}/events?max_results=250",
                               headers=hdr, timeout=60).json().get("events", [])
            for e in evs:
                dq = (e.get("details", {}).get("flow_progress", {}) or {}).get("data_quality")
                if not dq:
                    continue
                for ex in dq.get("expectations", []) or []:
                    c = expectations.setdefault(ex.get("name"), {"passed": 0, "failed": 0})
                    c["passed"] += int(ex.get("passed_records") or 0)
                    c["failed"] += int(ex.get("failed_records") or 0)
        except Exception as e:
            logger.warning("event log failed: %s", e)
    exp_list = [{"name": k, **v} for k, v in sorted(expectations.items())]
    tp = sum(v["passed"] for v in expectations.values())
    tf = sum(v["failed"] for v in expectations.values())
    return {
        "pipeline_name": pname or PIPELINE_NAME,
        "pipeline_id": pid,
        "state": state,
        "pipeline_url": f"{host}/pipelines/{pid}" if pid else None,
        "expectations": exp_list,
        "pass_rate": round(100 * tp / max(tp + tf, 1), 2),
        "total_evaluated": tp + tf,
    }


async def ingestion_status() -> dict:
    out = await asyncio.to_thread(_ingestion_sync)
    try:
        qc = await execute_query(
            f"SELECT (SELECT count(*) FROM {_fq('bronze_quarantine_claims')}) AS claims, "
            f"(SELECT count(*) FROM {_fq('bronze_quarantine_fraud_signals')}) AS fraud")
        out["quarantined_claims"] = int(qc[0]["claims"])
        out["quarantined_fraud"] = int(qc[0]["fraud"])
    except Exception:
        out["quarantined_claims"] = out["quarantined_fraud"] = 0
    return out


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


def reset_available() -> bool:
    from server.sql import _client
    try:
        return next((j for j in _client().jobs.list(name=config.RESET_JOB_NAME)), None) is not None
    except Exception:
        return False
