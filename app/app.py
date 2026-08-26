import logging
import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from server.routes import claims
from server.routes import mcp
from utils import config

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

FRONTEND_DIR = Path(__file__).parent / "frontend" / "dist"

app = FastAPI(title="Claims Intelligence Workbench", version="1.0.0")
app.include_router(claims.router)
app.include_router(mcp.router)


def _host() -> str:
    host = os.getenv("DATABRICKS_HOST", "")
    if not host:
        try:
            from server.sql import _client
            host = _client().config.host
        except Exception:
            host = ""
    host = host.rstrip("/")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    return host


@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/config")
async def app_config():
    host = _host()
    gid = config.GENIE_SPACE_ID
    did = config.DASHBOARD_ID
    return {
        "workspace_host": host,
        "catalog": config.CATALOG,
        "schema": config.SCHEMA,
        "supervisor_present": bool(config.ENDPOINT_SUPERVISOR),
        "genie_space_id": gid,
        "genie_url": f"{host}/genie/rooms/{gid}" if (host and gid) else None,
        "genie_embed_url": f"{host}/embed/genie/rooms/{gid}" if (host and gid) else None,
        "dashboard_id": did,
        "dashboard_url": f"{host}/dashboardsv3/{did}" if (host and did) else None,
        "dashboard_embed_url": f"{host}/embed/dashboardsv3/{did}" if (host and did) else None,
    }


if FRONTEND_DIR.is_dir():
    # Only mount /assets when a built Vite bundle is present (the hand-written
    # fallback dist ships a single index.html with no assets/ dir).
    _assets = FRONTEND_DIR / "assets"
    if _assets.is_dir():
        app.mount("/assets", StaticFiles(directory=_assets), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        fp = FRONTEND_DIR / full_path
        if fp.is_file():
            return FileResponse(fp)
        return FileResponse(FRONTEND_DIR / "index.html")
