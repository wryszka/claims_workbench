"""Claims AI API routes — thin wrappers over claims_service."""
import logging

from fastapi import APIRouter
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


@router.get("/monitoring-lens")
async def monitoring_lens():
    return await svc.monitoring_lens()


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


@router.get("/agents")
async def agents():
    return await svc.agent_roster()


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
