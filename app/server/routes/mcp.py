"""MCP server — the Claims Intelligence Workbench exposed as callable tools.

MCP-first: build the capability once (the app route handlers), expose it as a
tool surface, and let the app UI, notebooks, and external agents — including the
Bricksurance control tower — all be clients of the one surface.

Every tool DELEGATES to the existing claims route handler, so it reuses the exact
logic AND the exact server-side gate (a gated action re-checks its rule in the
handler it calls — it cannot be bypassed here). Reads are idempotent; [action]
tools write through the governed handler.

Transport: JSON-RPC 2.0 over one POST (MCP streamable-HTTP), plus a GET manifest —
mirrors pricing-workbench-gen2/routes/mcp.py. Auth is whatever the Databricks App
already enforces in front of the container; no separate credential path.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Awaitable

from fastapi import APIRouter, HTTPException, Request

from server.routes import claims

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/mcp", tags=["mcp"])

PROTOCOL_VERSION = "2025-06-18"
SERVER_INFO = {"name": "bricksurance-claims-workbench", "version": "1.0.0"}


def _mk(name: str, desc: str, props: dict | None = None, required: list | None = None) -> dict:
    return {"name": name, "description": desc,
            "inputSchema": {"type": "object", "properties": props or {}, "required": required or []}}


async def _call(coro: Awaitable) -> dict:
    """Await a route handler; normalise to {"ok": ...}. An HTTPException (incl. a
    401/403 from a server-side gate) becomes a clean {"ok": False, "gated": ...}."""
    try:
        r = await coro
    except HTTPException as e:
        gated = e.status_code in (401, 403)
        return {"ok": False, **({"gated": True} if gated else {}), "error": f"{e.status_code}: {e.detail}"}
    except Exception as e:
        logger.warning("mcp claims delegate failed: %s", str(e)[:200])
        return {"ok": False, "error": str(e)[:200]}
    return r if isinstance(r, dict) else {"ok": True, "data": r}


class _AgentReq:
    """Minimal Request shim so a handler that reads request.headers (comms_approve
    reads x-forwarded-email for the approver) attributes the action to the agent."""
    def __init__(self, agent_id: str):
        self.headers = {"x-forwarded-email": f"agent:{agent_id}"}


_CID = {"cid": {"type": "string", "description": "Claim id (claim_public_id), e.g. CLM-2026-000001"}}


# ===========================================================================
# claim_ — per-claim reads (the handler's desk view of one claim)
# ===========================================================================
async def _t_claim_list(a, s, ag):         return await _call(claims.claims(int(a.get("limit") or 25)))
async def _t_claim_panels(a, s, ag):       return await _call(claims.panels(str(a.get("cid") or "")))
async def _t_claim_synthesis(a, s, ag):    return await _call(claims.synthesis(str(a.get("cid") or ""), a.get("use_cache")))
async def _t_claim_enrichment(a, s, ag):   return await _call(claims.enrichment(str(a.get("cid") or "")))
async def _t_claim_disposition(a, s, ag):  return await _call(claims.claim_disposition(str(a.get("cid") or "")))
async def _t_claim_reasoning(a, s, ag):    return await _call(claims.claim_reasoning(str(a.get("cid") or "")))
async def _t_claim_track(a, s, ag):        return await _call(claims.claim_track(str(a.get("cid") or "")))
async def _t_claim_package(a, s, ag):      return await _call(claims.claim_package(str(a.get("cid") or "")))
async def _t_claim_packages(a, s, ag):     return await _call(claims.claim_packages())
async def _t_claim_expert(a, s, ag):       return await _call(claims.claim_expert(str(a.get("cid") or ""), str(a.get("role") or "")))
async def _t_claim_reserve_adequacy(a, s, ag): return await _call(claims.claim_reserve_adequacy(str(a.get("cid") or "")))
async def _t_claim_vulnerability(a, s, ag):    return await _call(claims.claim_vulnerability(str(a.get("cid") or "")))
async def _t_claim_calls(a, s, ag):        return await _call(claims.claim_calls(str(a.get("cid") or "")))
async def _t_claim_comms(a, s, ag):        return await _call(claims.comms_history(str(a.get("cid") or "")))


# ===========================================================================
# ops_ — portfolio, control tower, worklists, handlers, fraud/trends
# ===========================================================================
async def _t_ops_control_tower(a, s, ag):  return await _call(claims.control_tower())
async def _t_ops_operations(a, s, ag):     return await _call(claims.operations())
async def _t_ops_monitoring_lens(a, s, ag):return await _call(claims.monitoring_lens())
async def _t_ops_monday_brief(a, s, ag):   return await _call(claims.monday_brief())
async def _t_ops_worklist(a, s, ag):       return await _call(claims.worklist(str(a.get("kind") or ""), int(a.get("limit") or 100)))
async def _t_ops_handlers(a, s, ag):       return await _call(claims.handlers())
async def _t_ops_handler_queue(a, s, ag):  return await _call(claims.handler_queue(a.get("handler")))
async def _t_ops_fraud(a, s, ag):          return await _call(claims.fraud())
async def _t_ops_trends(a, s, ag):         return await _call(claims.trends())
async def _t_ops_agents(a, s, ag):         return await _call(claims.agents())
async def _t_ops_experts(a, s, ag):        return await _call(claims.experts())
async def _t_ops_suppliers(a, s, ag):      return await _call(claims.suppliers())
async def _t_ops_decisions(a, s, ag):      return await _call(claims.decisions(int(a.get("limit") or 20)))
async def _t_ops_auto_close_config(a, s, ag):  return await _call(claims.auto_close_config())
async def _t_ops_auto_close_segment(a, s, ag):
    return await _call(claims.auto_close_segment(float(a.get("conf") or 85.0), float(a.get("cap") or 2000.0), float(a.get("fraud") or 0.0)))
async def _t_ops_rules(a, s, ag):          return await _call(claims.rules())


# ===========================================================================
# gov_ — governance, fair outcomes, vulnerability, QA, inventory
# ===========================================================================
async def _t_gov_summary(a, s, ag):        return await _call(claims.governance())
async def _t_gov_inventory(a, s, ag):      return await _call(claims.governance_inventory())
async def _t_gov_fair_outcomes(a, s, ag):  return await _call(claims.fair_outcomes())
async def _t_gov_vulnerability(a, s, ag):  return await _call(claims.governance_vulnerability())
async def _t_gov_qa(a, s, ag):             return await _call(claims.governance_qa())


# ===========================================================================
# ingest_ — Guidewire CDA ingestion, quality, quarantine, documents
# ===========================================================================
async def _t_ingest_summary(a, s, ag):     return await _call(claims.ingestion())
async def _t_ingest_quarantine(a, s, ag):  return await _call(claims.ingestion_quarantine(a.get("reason"), int(a.get("limit") or 25)))
async def _t_ingest_documents(a, s, ag):   return await _call(claims.ingestion_documents(int(a.get("limit") or 20)))
async def _t_ingest_profile(a, s, ag):     return await _call(claims.ingestion_profile())
async def _t_ingest_analytics(a, s, ag):   return await _call(claims.ingestion_analytics())
async def _t_ingest_sample(a, s, ag):      return await _call(claims.ingestion_sample(str(a.get("table") or ""), int(a.get("limit") or 8)))
async def _t_ingest_assets(a, s, ag):      return await _call(claims.ingestion_assets())
async def _t_ingest_dataset(a, s, ag):     return await _call(claims.ingestion_dataset(str(a.get("key") or "")))


# ===========================================================================
# broker_ — the broker portal view
# ===========================================================================
async def _t_broker_portal(a, s, ag):      return await _call(claims.broker_portal(a.get("broker")))


# ===========================================================================
# ai_ / act_ — grounded Q&A and the governed write actions
# ===========================================================================
async def _t_ai_ask(a, s, ag):
    return await _call(claims.ask(claims.AskIn(question=str(a.get("question") or ""), cid=a.get("cid"), use_cache=a.get("use_cache"))))

async def _t_act_record_decision(a, s, ag):
    return await _call(claims.decision(claims.DecisionIn(
        claim_public_id=str(a.get("cid") or a.get("claim_public_id") or ""),
        model_recommendation=str(a.get("model_recommendation") or ""),
        model_confidence=a.get("model_confidence"),
        handler_action=str(a.get("handler_action") or ""),
        override_flag=bool(a.get("override_flag") or False),
        override_reason=str(a.get("override_reason") or ""))))

async def _t_act_draft_comms(a, s, ag):
    return await _call(claims.comms_draft(claims.CommsDraftIn(
        cid=str(a.get("cid") or ""), comm_type=str(a.get("comm_type") or ""))))

async def _t_act_approve_comms(a, s, ag):
    return await _call(claims.comms_approve(
        claims.CommsApproveIn(comm_id=str(a.get("comm_id") or "")), _AgentReq(ag)))

async def _t_act_create_claim(a, s, ag):
    return await _call(claims.create_claim(claims.CreateClaimIn(**{
        k: a.get(k) for k in (
            "scenario", "policy_number", "peril_type", "report_channel", "reported_amount",
            "sum_insured", "fraud_score", "prior_claims_12m", "reporting_lag_days",
            "policy_tenure_years", "weather_risk_composite", "at_fault", "third_party_involved",
            "flood_risk_score") if a.get(k) is not None})))

async def _t_act_create_scenario(a, s, ag): return await _call(claims.create_claim_scenario())
async def _t_ops_sandbox_claims(a, s, ag):  return await _call(claims.sandbox_claims(int(a.get("limit") or 20)))


TOOL_SCHEMAS: list[dict[str, Any]] = [
    # claim_
    _mk("claim_list", "List recent claims (id, peril, amount, model recommendation, status).", {"limit": {"type": "integer"}}),
    _mk("claim_panels", "The full desk view for one claim — every panel the handler sees (facts, policy, model score, flags).", _CID, ["cid"]),
    _mk("claim_synthesis", "The AI synthesis for a claim — the grounded narrative and recommended action, with sources.", {**_CID, "use_cache": {"type": "boolean"}}, ["cid"]),
    _mk("claim_enrichment", "External + internal enrichment attached to a claim (weather, prior claims, policy context).", _CID, ["cid"]),
    _mk("claim_disposition", "The current disposition of a claim (auto-close / refer / decline) and why.", _CID, ["cid"]),
    _mk("claim_reasoning", "The step-by-step reasoning trace behind a claim's recommendation.", _CID, ["cid"]),
    _mk("claim_track", "The lifecycle track of a claim — first notice → settlement, with timestamps.", _CID, ["cid"]),
    _mk("claim_package", "The assembled decision package for a claim (evidence + reasoning + audit).", _CID, ["cid"]),
    _mk("claim_packages", "List assembled claim decision packages."),
    _mk("claim_expert", "A named specialist agent's opinion on a claim (role = adjuster/fraud/coverage/reserving/recovery/conduct/…).", {**_CID, "role": {"type": "string"}}, ["cid", "role"]),
    _mk("claim_reserve_adequacy", "Reserve-adequacy view for a claim (set reserve vs modelled expectation).", _CID, ["cid"]),
    _mk("claim_vulnerability", "Vulnerability / Consumer-Duty assessment for the policyholder on a claim.", _CID, ["cid"]),
    _mk("claim_calls", "Call/contact history and analysis for a claim.", _CID, ["cid"]),
    _mk("claim_comms", "Outbound-communication history for a claim.", _CID, ["cid"]),
    # ops_
    _mk("ops_control_tower", "The claims control-tower overview — portfolio KPIs, queues, SLA and auto-close posture."),
    _mk("ops_operations", "Operational metrics — throughput, cycle time, backlog."),
    _mk("ops_monitoring_lens", "The monitoring lens — drift, volumes, exception rates."),
    _mk("ops_monday_brief", "The Monday morning brief — what changed and what needs attention this week."),
    _mk("ops_worklist", "A worklist of claims by kind (e.g. referrals, high-value, fraud-flagged).", {"kind": {"type": "string"}, "limit": {"type": "integer"}}, ["kind"]),
    _mk("ops_handlers", "The handler roster and their load."),
    _mk("ops_handler_queue", "The queue for a specific handler (or all).", {"handler": {"type": "string"}}),
    _mk("ops_fraud", "The fraud lens — flagged claims and SIU referrals."),
    _mk("ops_trends", "Portfolio trends over time (frequency, severity, cause mix)."),
    _mk("ops_agents", "The registered AI agents in the workbench and their health."),
    _mk("ops_experts", "The specialist expert-agent roster (adjuster, fraud, coverage, reserving, …)."),
    _mk("ops_suppliers", "Supplier/repairer network view and performance."),
    _mk("ops_decisions", "Recent handler decisions (accept / override) with the audit trail.", {"limit": {"type": "integer"}}),
    _mk("ops_auto_close_config", "The current auto-close configuration (thresholds)."),
    _mk("ops_auto_close_segment", "Simulate the auto-close-eligible segment at given confidence / cap / fraud thresholds.", {"conf": {"type": "number"}, "cap": {"type": "number"}, "fraud": {"type": "number"}}),
    _mk("ops_rules", "The claims decisioning rule set."),
    _mk("ops_sandbox_claims", "List sandbox (agent/user-created) claims.", {"limit": {"type": "integer"}}),
    # gov_
    _mk("gov_summary", "Governance overview — audit posture, model governance, decision logging."),
    _mk("gov_inventory", "The governed-asset inventory (tables, models, agents) for claims."),
    _mk("gov_fair_outcomes", "The fair-outcomes monitor across protected/vulnerable cohorts."),
    _mk("gov_vulnerability", "The vulnerability-handling governance view (Consumer Duty)."),
    _mk("gov_qa", "The QA / decision-quality monitor."),
    # ingest_
    _mk("ingest_summary", "Guidewire CDA ingestion overview — feeds, freshness, volumes."),
    _mk("ingest_quarantine", "Quarantined records and why (optionally filter by reason).", {"reason": {"type": "string"}, "limit": {"type": "integer"}}),
    _mk("ingest_documents", "Ingested claim documents (FNOL forms, photos, reports).", {"limit": {"type": "integer"}}),
    _mk("ingest_profile", "Data-quality profile of the ingested claims data."),
    _mk("ingest_analytics", "Ingestion analytics — throughput and DQ trends."),
    _mk("ingest_sample", "Sample rows from a bronze/silver ingestion table.", {"table": {"type": "string"}, "limit": {"type": "integer"}}, ["table"]),
    _mk("ingest_assets", "The labelled ingestion assets (tables + provenance)."),
    _mk("ingest_dataset", "Details of one ingestion dataset by key.", {"key": {"type": "string"}}, ["key"]),
    # broker_
    _mk("broker_portal", "The broker-portal view (optionally for one broker).", {"broker": {"type": "string"}}),
    # ai_ / act_
    _mk("ai_ask", "Ask the grounded claims assistant a question (optionally about a specific claim). Answers only from workbench data.", {"question": {"type": "string"}, "cid": {"type": "string"}, "use_cache": {"type": "boolean"}}, ["question"]),
    _mk("act_record_decision", "[action] Record a handler decision on a claim (accept the model or override) — audit-logged. An override requires override_flag=true and an override_reason.",
        {**_CID, "model_recommendation": {"type": "string"}, "model_confidence": {"type": "number"}, "handler_action": {"type": "string", "enum": ["accept", "override"]}, "override_flag": {"type": "boolean"}, "override_reason": {"type": "string"}}, ["cid", "model_recommendation", "handler_action"]),
    _mk("act_draft_comms", "[action] Draft an outbound policyholder communication for a claim (does NOT send).", {**_CID, "comm_type": {"type": "string"}}, ["cid", "comm_type"]),
    _mk("act_approve_comms", "[gated] Approve a drafted communication (maker/checker; the approver is recorded from the caller identity).", {"comm_id": {"type": "string"}}, ["comm_id"]),
    _mk("act_create_claim", "[action] Create a sandbox claim from a scenario or explicit attributes (for what-if / demo).",
        {"scenario": {"type": "string"}, "policy_number": {"type": "string"}, "peril_type": {"type": "string"}, "report_channel": {"type": "string"}, "reported_amount": {"type": "number"}, "sum_insured": {"type": "number"}, "fraud_score": {"type": "integer"}, "prior_claims_12m": {"type": "integer"}, "reporting_lag_days": {"type": "integer"}, "policy_tenure_years": {"type": "number"}, "weather_risk_composite": {"type": "number"}, "at_fault": {"type": "integer"}, "third_party_involved": {"type": "integer"}, "flood_risk_score": {"type": "number"}}),
    _mk("act_create_scenario", "Generate a fresh sandbox-claim scenario (attributes you can then create)."),
]

TOOL_IMPLS: dict[str, Any] = {
    "claim_list": _t_claim_list, "claim_panels": _t_claim_panels, "claim_synthesis": _t_claim_synthesis,
    "claim_enrichment": _t_claim_enrichment, "claim_disposition": _t_claim_disposition,
    "claim_reasoning": _t_claim_reasoning, "claim_track": _t_claim_track, "claim_package": _t_claim_package,
    "claim_packages": _t_claim_packages, "claim_expert": _t_claim_expert,
    "claim_reserve_adequacy": _t_claim_reserve_adequacy, "claim_vulnerability": _t_claim_vulnerability,
    "claim_calls": _t_claim_calls, "claim_comms": _t_claim_comms,
    "ops_control_tower": _t_ops_control_tower, "ops_operations": _t_ops_operations,
    "ops_monitoring_lens": _t_ops_monitoring_lens, "ops_monday_brief": _t_ops_monday_brief,
    "ops_worklist": _t_ops_worklist, "ops_handlers": _t_ops_handlers, "ops_handler_queue": _t_ops_handler_queue,
    "ops_fraud": _t_ops_fraud, "ops_trends": _t_ops_trends, "ops_agents": _t_ops_agents,
    "ops_experts": _t_ops_experts, "ops_suppliers": _t_ops_suppliers, "ops_decisions": _t_ops_decisions,
    "ops_auto_close_config": _t_ops_auto_close_config, "ops_auto_close_segment": _t_ops_auto_close_segment,
    "ops_rules": _t_ops_rules, "ops_sandbox_claims": _t_ops_sandbox_claims,
    "gov_summary": _t_gov_summary, "gov_inventory": _t_gov_inventory, "gov_fair_outcomes": _t_gov_fair_outcomes,
    "gov_vulnerability": _t_gov_vulnerability, "gov_qa": _t_gov_qa,
    "ingest_summary": _t_ingest_summary, "ingest_quarantine": _t_ingest_quarantine,
    "ingest_documents": _t_ingest_documents, "ingest_profile": _t_ingest_profile,
    "ingest_analytics": _t_ingest_analytics, "ingest_sample": _t_ingest_sample,
    "ingest_assets": _t_ingest_assets, "ingest_dataset": _t_ingest_dataset,
    "broker_portal": _t_broker_portal,
    "ai_ask": _t_ai_ask, "act_record_decision": _t_act_record_decision,
    "act_draft_comms": _t_act_draft_comms, "act_approve_comms": _t_act_approve_comms,
    "act_create_claim": _t_act_create_claim, "act_create_scenario": _t_act_create_scenario,
}


def _ok(rpc_id, result):  return {"jsonrpc": "2.0", "id": rpc_id, "result": result}
def _err(rpc_id, code, m): return {"jsonrpc": "2.0", "id": rpc_id, "error": {"code": code, "message": m}}


@router.post("")
async def jsonrpc(request: Request) -> dict:
    try:
        body = await request.json()
    except Exception:
        return _err(None, -32700, "Parse error: body is not valid JSON")
    rpc_id = body.get("id"); method = body.get("method"); params = body.get("params") or {}
    agent_id = request.headers.get("user-agent", "unknown-agent")[:120]

    if method == "initialize":
        return _ok(rpc_id, {
            "protocolVersion": PROTOCOL_VERSION, "serverInfo": SERVER_INFO,
            "capabilities": {"tools": {}},
            "instructions": (
                "Claims Intelligence Workbench services for Bricksurance SE. Reads cover the "
                "control tower, per-claim desk view, ingestion, governance and specialists. "
                "act_* tools write through the same governed handlers the UI uses — recording a "
                "decision is audit-logged, an override needs a reason, and approving a "
                "communication is a separate maker/checker step. Never invent a figure.")})
    if method in ("notifications/initialized", "notifications/cancelled"):
        return _ok(rpc_id, {})
    if method == "tools/list":
        return _ok(rpc_id, {"tools": TOOL_SCHEMAS})
    if method == "tools/call":
        name = params.get("name"); args = params.get("arguments") or {}
        impl = TOOL_IMPLS.get(name)
        if impl is None:
            return _err(rpc_id, -32601, f"Unknown tool: {name}")
        session_id = str(args.get("session_id") or "").strip() or "mcp"
        try:
            payload = await impl(args, session_id, agent_id)
        except Exception as e:
            logger.exception("mcp tool %s failed", name)
            return _err(rpc_id, -32603, f"Tool execution failed: {str(e)[:200]}")
        return _ok(rpc_id, {
            "content": [{"type": "text", "text": json.dumps(payload, default=str)}],
            "structuredContent": payload,
            "isError": isinstance(payload, dict) and payload.get("ok") is False})
    return _err(rpc_id, -32601, f"Method not found: {method}")


@router.get("/manifest")
async def manifest() -> dict:
    return {"server": SERVER_INFO, "protocol_version": PROTOCOL_VERSION,
            "tools": [{"name": t["name"], "description": t["description"]} for t in TOOL_SCHEMAS]}
