"""Async SQL executor for the Claims AI app — runs statements on the SQL
warehouse via the Databricks SDK (INLINE disposition; Apps egress is firewalled
from EXTERNAL_LINKS storage). Mirrors the pricing-workbench reference."""
import asyncio
import logging
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from utils.config import WAREHOUSE_ID

logger = logging.getLogger(__name__)
_wc: WorkspaceClient | None = None


def _client() -> WorkspaceClient:
    global _wc
    if _wc is None:
        import os
        if os.getenv("DATABRICKS_APP_NAME"):
            _wc = WorkspaceClient()
        else:
            _wc = WorkspaceClient(profile=os.getenv("DATABRICKS_PROFILE", "DEFAULT"))
    return _wc


def _execute_sync(sql: str) -> list[dict[str, Any]]:
    r = _client().statement_execution.execute_statement(
        statement=sql, warehouse_id=WAREHOUSE_ID, wait_timeout="50s")
    if r.status and r.status.state == StatementState.FAILED:
        raise RuntimeError(f"SQL failed: {r.status.error.message if r.status.error else 'unknown'}")
    if not (r.manifest and r.manifest.schema and r.manifest.schema.columns):
        return []
    cols = [c.name for c in r.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in (r.result.data_array or [])] if r.result else []


async def execute_query(sql: str) -> list[dict[str, Any]]:
    return await asyncio.to_thread(_execute_sync, sql)
