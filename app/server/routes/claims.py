"""Claims AI API routes — thin wrappers over claims_service."""
import logging

from fastapi import APIRouter, Request, Response
from pydantic import BaseModel

from utils import config
from server import claims_service as svc
from server.sql import execute_query, _client

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")


@router.get("/claims")
async def claims(limit: int = 25):
    return await svc.list_claims(limit)


@router.get("/claim/panels")
async def panels(cid: str):
    return await svc.get_panels(cid)


@router.get("/claim/synthesis")
async def synthesis(cid: str, use_cache: bool | None = None):
    return await svc.get_synthesis(cid, use_cache)


class DecisionIn(BaseModel):
    claim_public_id: str
    model_recommendation: str
    model_confidence: float | None = None
    handler_action: str            # 'accept' | 'override'
    override_flag: bool = False
    override_reason: str = ""


@router.post("/decision")
async def decision(d: DecisionIn):
    return await svc.log_decision(
        d.claim_public_id, d.model_recommendation, d.model_confidence,
        d.handler_action, d.override_flag, d.override_reason)


@router.get("/decisions")
async def decisions(limit: int = 20):
    return await svc.recent_decisions(limit)


@router.get("/cache-mode")
async def cache_mode_get():
    try:
        n = (await execute_query(f"SELECT count(*) c FROM `{config.CATALOG}`.`{config.SCHEMA}`.cache_agent_responses"))[0]["c"]
    except Exception:
        n = 0
    return {"use_cache": svc.get_cache_mode(), "entries": int(n)}


class CacheMode(BaseModel):
    use_cache: bool


@router.post("/cache-mode")
async def cache_mode_set(m: CacheMode):
    return {"use_cache": svc.set_cache_mode(m.use_cache)}


@router.get("/ingestion")
async def ingestion():
    return await svc.ingestion_status()


@router.get("/ingestion/quarantine")
async def ingestion_quarantine(reason: str | None = None, limit: int = 25):
    return await svc.ingestion_quarantine(reason, limit)


@router.get("/ingestion/documents")
async def ingestion_documents(limit: int = 20):
    return await svc.ingestion_documents(limit)


@router.get("/ingestion/profile")
async def ingestion_profile():
    return await svc.ingestion_profile()


@router.get("/ingestion/analytics")
async def ingestion_analytics():
    return await svc.ingestion_analytics()


@router.get("/ingestion/sample")
async def ingestion_sample(table: str, limit: int = 8):
    return await svc.ingestion_sample(table, limit)


@router.get("/ingestion/assets")
async def ingestion_assets():
    return await svc.ingestion_assets()


@router.get("/ingestion/dataset")
async def ingestion_dataset(key: str):
    return await svc.ingestion_dataset(key)


@router.get("/claim/enrichment")
async def enrichment(cid: str):
    return await svc.enrichment(cid)


@router.get("/governance")
async def governance():
    return await svc.governance_links()


@router.get("/control-tower")
async def control_tower():
    return await svc.control_tower()


@router.get("/auto-close/config")
async def auto_close_config():
    return await svc.auto_close_config()


@router.get("/auto-close/segment")
async def auto_close_segment(conf: float = 85.0, cap: float = 2000.0, fraud: float = 20.0):
    return await svc.segment_auto_close(conf, cap, fraud)


@router.get("/rules")
async def rules():
    return await svc.rules()


@router.get("/monitoring-lens")
async def monitoring_lens():
    return await svc.monitoring_lens()


@router.get("/monday-brief")
async def monday_brief():
    return await svc.monday_brief()


@router.get("/worklist")
async def worklist(kind: str, limit: int = 100):
    return await svc.worklist(kind, limit)


@router.get("/handlers")
async def handlers():
    return await svc.handlers()


@router.get("/operations")
async def operations():
    return await svc.operations_view()


@router.get("/fraud")
async def fraud():
    return await svc.fraud_view()


@router.get("/trends")
async def trends():
    return await svc.trends()


class AskIn(BaseModel):
    question: str
    cid: str | None = None
    use_cache: bool | None = None


@router.post("/ask")
async def ask(a: AskIn):
    return await svc.ask(a.question, a.cid, a.use_cache)


@router.get("/claim/disposition")
async def claim_disposition(cid: str):
    return await svc.claim_disposition(cid)


@router.get("/claim/reasoning")
async def claim_reasoning(cid: str):
    return await svc.claim_reasoning(cid)


@router.get("/governance/inventory")
async def governance_inventory():
    return await svc.governance_inventory()


@router.get("/claim/track")
async def claim_track(cid: str):
    return await svc.claim_track(cid)


@router.get("/claim/packages")
async def claim_packages():
    return await svc.closure_packages()


@router.get("/claim/package")
async def claim_package(cid: str):
    res = await svc.claim_package_file(cid)
    if not res:
        return Response(status_code=404, content="No closure package for this claim.")
    data, file_name = res
    return Response(content=data, media_type="application/pdf",
                    headers={"Content-Disposition": f'inline; filename="{file_name}"'})


@router.get("/agents")
async def agents():
    return await svc.agent_roster()


@router.get("/experts")
async def experts():
    return {"experts": svc.EXPERTS}


@router.get("/claim/expert")
async def claim_expert(cid: str, role: str):
    return await svc.expert_opinion(cid, role)


@router.get("/governance/fair-outcomes")
async def fair_outcomes():
    return await svc.fair_outcomes()


@router.get("/broker/portal")
async def broker_portal(broker: str | None = None):
    return await svc.broker_portal(broker)


@router.get("/governance/vulnerability")
async def governance_vulnerability():
    return await svc.vulnerability_view()


@router.get("/governance/qa")
async def governance_qa():
    return await svc.qa_view()


@router.get("/claim/vulnerability")
async def claim_vulnerability(cid: str):
    return await svc.claim_vulnerability(cid)


@router.get("/claim/calls")
async def claim_calls(cid: str):
    return await svc.claim_calls(cid)


class CommsDraftIn(BaseModel):
    cid: str
    comm_type: str


@router.post("/claim/comms/draft")
async def comms_draft(c: CommsDraftIn):
    return await svc.draft_comms(c.cid, c.comm_type)


@router.get("/claim/comms")
async def comms_history(cid: str):
    return await svc.comms_history(cid)


class CommsApproveIn(BaseModel):
    comm_id: str


@router.post("/claim/comms/approve")
async def comms_approve(c: CommsApproveIn, request: Request):
    approver = request.headers.get("x-forwarded-email") or "handler (demo)"
    return await svc.approve_comms(c.comm_id, approver)


# --- Phase 12 Stage B1: Handler "My Queue" (persona: Sarah Chen) ---
@router.get("/handler/queue")
async def handler_queue(handler: str | None = None):
    return await svc.handler_queue(handler)


# --- Phase 12 Stage B2: Create a claim (synchronous real scoring, ephemeral) ---
@router.get("/create-claim/scenario")
async def create_claim_scenario():
    return await svc.create_claim_scenario()


class CreateClaimIn(BaseModel):
    scenario: str | None = "custom"
    policy_number: str | None = None
    peril_type: str | None = None
    report_channel: str | None = None
    reported_amount: float | None = None
    sum_insured: float | None = None
    fraud_score: int | None = None
    prior_claims_12m: int | None = None
    reporting_lag_days: int | None = None
    policy_tenure_years: float | None = None
    weather_risk_composite: float | None = None
    at_fault: int | None = None
    third_party_involved: int | None = None
    flood_risk_score: float | None = None


@router.post("/create-claim")
async def create_claim(c: CreateClaimIn):
    return await svc.create_claim(c.model_dump())


@router.get("/sandbox-claims")
async def sandbox_claims(limit: int = 20):
    return await svc.sandbox_claims(limit)


@router.get("/reset-status")
async def reset_status():
    import asyncio
    return {"available": await asyncio.to_thread(svc.reset_available)}


@router.get("/reset-run")
async def reset_run(run_id: int):
    import asyncio

    def _g():
        r = _client().jobs.get_run(run_id=run_id)
        st = r.state
        return {"life_cycle": str(st.life_cycle_state) if st and st.life_cycle_state else None,
                "result": str(st.result_state) if st and st.result_state else None}
    return await asyncio.to_thread(_g)


@router.post("/reset-demo")
async def reset_demo():
    """Trigger the Phase 9 reset job by name. Graceful if it doesn't exist yet."""
    try:
        import asyncio
        job = await asyncio.to_thread(svc.find_reset_job)
        if not job:
            return {"available": False, "message": f"Reset job '{config.RESET_JOB_NAME}' not found."}
        run = _client().jobs.run_now(job_id=job.job_id)
        return {"available": True, "triggered": True, "run_id": getattr(run, "run_id", None)}
    except Exception as e:
        return {"available": False, "message": f"Reset unavailable: {e}"}
