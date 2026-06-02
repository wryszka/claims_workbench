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

# --- CCO framing constants (illustrative, documented) ---
HANDLER_COST_PER_CLAIM = 45.0     # £ fully-loaded handling cost avoided per auto-close
HANDLER_MINS_PER_CLAIM = 25       # handler-minutes freed per auto-close
PERIL_SLA = {"motor_tp": 30, "home_fire": 60}   # days; default 45
DEFAULT_SLA = 45
# Book targets for the RAG status on each tile.
TARGETS = {"pct_auto_closed": 15.0, "leakage_rate": 6.0, "avg_settle_days": 35.0,
           "fraud_refer_rate": 5.0, "sla_breach_pct": 10.0}


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
async def get_panels(cid: str) -> dict:
    summary, triage, reserve, fraud, policy, recovery = await asyncio.gather(
        _fn("fn_claim_summary", cid), _fn("fn_triage_claim", cid), _fn("fn_reserve_claim", cid),
        _fn("fn_fraud_signals", cid), _fn("fn_policy_history", cid), _fn("fn_recovery_signals", cid))
    extra_rows = await execute_query(f"""
        SELECT policy_number, weather_risk_composite, flood_risk_score, wind_risk_score,
               freeze_risk_score, prior_claims_12m, at_fault, reporting_lag_days
        FROM {_fq('silver_claims_enriched')} WHERE claim_public_id = '{_esc(cid)}'
    """)
    extra = extra_rows[0] if extra_rows else {}
    return {"claim_public_id": cid, "summary": summary, "triage": triage,
            "reserve": reserve, "fraud": fraud, "policy": policy, "recovery": recovery, "extra": extra}


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
          round(sum(CASE WHEN recovery_flag AND claim_status IN ('open','under_investigation') THEN recoverable_amount ELSE 0 END)) recoverable_total
        FROM {s}"""))[0]
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

    tiles = [
        {"key": "open", "label": "Open inventory", "value": int(base["open_inv"]), "fmt": "num",
         "sub": f"{int(base['investigating']):,} under investigation", "rag": "info", "worklist": "aged"},
        {"key": "sla", "label": "Past SLA", "value": past_sla, "fmt": "num",
         "sub": f"{sla_breach_pct}% of open · per-peril SLA", "rag": _rag(sla_breach_pct, TARGETS["sla_breach_pct"], True), "worklist": "aged"},
        {"key": "large", "label": "Large losses (open)", "value": int(base["large_losses"]), "fmt": "num",
         "sub": "over £50,000 — senior review", "rag": "info", "worklist": "large"},
        {"key": "reserves", "label": "Open reserves", "value": int(base["total_reserves"]), "fmt": "gbp",
         "sub": "outstanding liability", "rag": "info", "worklist": None},
        {"key": "leakage", "label": "Leakage rate", "value": base["leakage_rate"], "fmt": "pct",
         "sub": f"target ≤ {TARGETS['leakage_rate']}% of claims", "rag": _rag(base["leakage_rate"], TARGETS["leakage_rate"], True),
         "trend": trend("leakage_rate", base["leakage_rate"], True), "worklist": None},
        {"key": "settle", "label": "Avg settlement", "value": base["avg_settle_days"], "fmt": "days",
         "sub": f"target ≤ {TARGETS['avg_settle_days']} days", "rag": _rag(base["avg_settle_days"], TARGETS["avg_settle_days"], True),
         "trend": trend("avg_settle_days", base["avg_settle_days"], True), "worklist": None},
        {"key": "fraud", "label": "Fraud-refer rate", "value": base["fraud_refer_rate"], "fmt": "pct",
         "sub": "elevated signals → SIU", "rag": "info", "worklist": "high_fraud"},
        {"key": "closed", "label": "Settled", "value": int(base["closed"]), "fmt": "num", "sub": "lifetime", "rag": "info", "worklist": None},
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
                    "gap": int(float(rd["gap"])) if rd.get("gap") is not None else None},
        "sla": {"open_inv": int(base["open_inv"]), "past_sla": past_sla, "breach_pct": sla_breach_pct},
        "tiles": tiles,
    }


async def segment_auto_close(conf: float, cap: float, fraud: float) -> dict:
    """Re-segment % auto-closed LIVE for the slider — pure SQL over the stored
    raw decision inputs in gold_claim_disposition (no model re-score)."""
    d = _fq("gold_claim_disposition")
    r = (await execute_query(f"""
        SELECT count(*) total,
          sum(CASE WHEN model_decision='pay_direct' AND model_confidence >= {float(conf)}
                    AND total_incurred <= {float(cap)} AND fraud_score <= {float(fraud)}
                    AND data_complete THEN 1 ELSE 0 END) auto_closed
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


async def monday_brief() -> dict:
    """The Monday-morning brief — prioritised, money-framed, and each item links to a
    worklist you can act on. Grouped: needs you now / money on the table / risk."""
    t = await control_tower()
    sla, rec, res, hero = t["sla"], t["recovery"], t["reserve"], t["hero"]
    needs, money, risk = [], [], []
    if sla["past_sla"]:
        needs.append({"text": f"{sla['past_sla']:,} open claims are past their per-peril SLA ({sla['breach_pct']}% of open) — reallocate or chase.",
                      "action": "Open the SLA worklist", "worklist": "aged"})
    large = next((x["value"] for x in t["tiles"] if x["key"] == "large"), 0)
    if large:
        needs.append({"text": f"{large:,} large losses (over £50,000) are open — confirm reserves and senior ownership.",
                      "action": "Open large-loss worklist", "worklist": "large"})
    needs.append({"text": f"{hero['escalated']:,} claims escalated to handlers; {hero['pct_auto_closed']}% auto-closed straight-through, freeing ≈{hero['hours_freed']:,} handler-hours (≈£{hero['gbp_saved']:,}).",
                  "action": "Review auto-close appetite", "worklist": "autoclose"})
    if rec["total"]:
        money.append({"text": f"£{rec['total']:,} is recoverable across {rec['count']:,} open claims — recovery is chronically under-pursued.",
                      "action": "Open recovery worklist", "worklist": "recovery"})
    if res.get("dev_ratio") and res["dev_ratio"] > 1.1:
        money.append({"text": f"Escape-of-water is developing at {res['dev_ratio']}× initial reserve (~{round((res['dev_ratio']-1)*100)}% under-reserved) — a ≈£{abs(res['gap']):,} provision gap.",
                      "action": "See under-reserved claims", "worklist": "underreserved"})
    leak = next((x for x in t["tiles"] if x["key"] == "leakage"), {})
    if leak.get("rag") in ("amber", "red"):
        risk.append({"text": f"Leakage is {leak['value']}% (target ≤ {TARGETS['leakage_rate']}%){_trend_phrase(leak.get('trend'))}.",
                     "action": None, "worklist": None})
    fr = next((x for x in t["tiles"] if x["key"] == "fraud"), {})
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
    return {"summary": summary, "buckets": buckets, "queue": queue}


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
                'reasoning', reasoning)) j
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
    product = (c.get("product") or "home")
    docs = [{"name": d, "status": _doc_status(cid, d, auto)} for d in _DOC_SETS.get(product, _DOC_SETS["home"])]
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
        else:
            lc.append({"stage": "Awaiting handler decision", "when": None,
                       "detail": "No human decision logged yet — outstanding.", "status": "awaited"})
    if c.get("recovery_flag"):
        lc.append({"stage": "Recovery identified", "when": None,
                   "detail": f"Subrogation potential — up to {gbp_note(c.get('recoverable_amount'))} recoverable from the third party.", "status": "awaited"})
    if c.get("settlement_date"):
        lc.append({"stage": "Settled", "when": c["settlement_date"],
                   "detail": f"Closed in {c.get('days_to_settle')} days.", "status": "done"})

    actions = [{"actor": r["agent_name"], "detail": (r["reasoning_text"] or "")[:240]} for r in reasoning]
    gaps = [f"{d['name']} — {d['status']}" for d in docs if d["status"] != "received"]
    if not auto and not decisions:
        gaps.append("Handler decision outstanding")
    if c.get("recovery_flag"):
        gaps.append(f"Recovery not yet pursued ({gbp_note(c.get('recoverable_amount'))})")

    return {"found": True, "claim_public_id": cid,
            "claim": {k: c[k] for k in c if k != "description_text"},
            "disposition": disp.get("disposition"), "documents": docs,
            "doc_complete_pct": round(100 * received / len(docs)),
            "lifecycle": lc, "actions": actions, "gaps": gaps}


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
         "endpoint": "genie", "url": None},
    ]
    return {"supervisor": {"name": "Claims AI Supervisor", **ep(sup),
                           "desc": "Reads the question, classifies it against the specialist catalogue in a single Foundation-Model call, dispatches to the right one, synthesises the answer with source citations, and writes the routing decision to the audit log.",
                           "present": bool(config.ENDPOINT_SUPERVISOR)},
            "agents": agents, "genie": genie}


async def agent_roster() -> dict:
    return await asyncio.to_thread(_agent_roster_sync)


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
