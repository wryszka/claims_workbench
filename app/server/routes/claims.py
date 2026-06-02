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


@router.post("/reset-demo")
async def reset_demo():
    """Trigger the Phase 9 reset job by name. Graceful if it doesn't exist yet."""
    try:
        w = _client()
        job = next((j for j in w.jobs.list(name=config.RESET_JOB_NAME)), None)
        if not job:
            return {"available": False, "message": f"Reset job '{config.RESET_JOB_NAME}' not found (built in Phase 9)."}
        run = w.jobs.run_now(job_id=job.job_id)
        return {"available": True, "triggered": True, "run_id": getattr(run, "run_id", None)}
    except Exception as e:
        return {"available": False, "message": f"Reset unavailable: {e}"}
