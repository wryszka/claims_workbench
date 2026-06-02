"""Cache-first wrapper for Claims AI agent/endpoint calls (Phase 6, Stage C).

Generic over ANY serving endpoint (sub-agents and the managed Supervisor):

    from utils.agent_cache import get_agent_response
    out = get_agent_response(agent_name, input_dict, use_cache=True)
    # out = {"cache": "hit|miss|bypass", "agent_name": ..., "response": <endpoint json>}

Behaviour:
  * cache_key = sha256(agent_name + canonical_json(input_dict))
  * use_cache=True  → HIT returns the stored response (fast, no agent run);
                      MISS calls the real endpoint, saves, returns.
  * use_cache=False → always calls the real endpoint (and refreshes the cache).
  * No TTL — cache entries live until overwritten.

Backed by the Delta table `{catalog}.{schema}.cache_agent_responses`. Reads/writes
go through a SQL warehouse; endpoint calls hit the model-serving invocations API.
Usable from notebooks and the Databricks App (uses the ambient WorkspaceClient auth).
"""
import hashlib
import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

from utils.config import USE_CACHE, CACHE_TABLE, WAREHOUSE_ID


def _w():
    return WorkspaceClient()


def cache_key(agent_name: str, input_dict: dict) -> str:
    canon = json.dumps(input_dict, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(f"{agent_name}|{canon}".encode()).hexdigest()


def _sql(statement: str, params=None):
    r = _w().statement_execution.execute_statement(
        statement=statement, warehouse_id=WAREHOUSE_ID,
        parameters=params or [], wait_timeout="50s")
    if r.status and r.status.state == StatementState.FAILED:
        raise RuntimeError(r.status.error.message if r.status.error else "SQL failed")
    if not (r.manifest and r.manifest.schema and r.manifest.schema.columns):
        return []
    cols = [c.name for c in r.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in (r.result.data_array or [])] if r.result else []


def _read(key: str):
    rows = _sql(
        f"SELECT response_json FROM {CACHE_TABLE} WHERE cache_key = :key "
        f"ORDER BY created_ts DESC LIMIT 1",
        [StatementParameterListItem(name="key", value=key)])
    return json.loads(rows[0]["response_json"]) if rows else None


def _save(key: str, agent_name: str, input_dict: dict, response: dict):
    _sql(
        f"""MERGE INTO {CACHE_TABLE} t USING (SELECT :key AS cache_key) s
            ON t.cache_key = s.cache_key
            WHEN MATCHED THEN UPDATE SET agent_name = :agent, input_json = :inp,
                 response_json = :out, created_ts = current_timestamp(), mode = 'real'
            WHEN NOT MATCHED THEN INSERT (cache_key, agent_name, input_json, response_json, created_ts, mode)
                 VALUES (:key, :agent, :inp, :out, current_timestamp(), 'real')""",
        [StatementParameterListItem(name="key", value=key),
         StatementParameterListItem(name="agent", value=agent_name),
         StatementParameterListItem(name="inp", value=json.dumps(input_dict)),
         StatementParameterListItem(name="out", value=json.dumps(response))])


def _call_endpoint(agent_name: str, input_dict: dict) -> dict:
    import requests
    w = _w()
    host = w.config.host.rstrip("/")
    hdr = w.config._header_factory()
    resp = requests.post(
        f"{host}/serving-endpoints/{agent_name}/invocations",
        headers={**hdr, "Content-Type": "application/json"},
        json=input_dict, timeout=300)
    resp.raise_for_status()
    return resp.json()


def get_agent_response(agent_name: str, input_dict: dict, use_cache: bool = None) -> dict:
    """Cache-first call to any agent/endpoint. Returns {cache, agent_name, response}."""
    if use_cache is None:
        use_cache = USE_CACHE
    key = cache_key(agent_name, input_dict)
    if use_cache:
        hit = _read(key)
        if hit is not None:
            return {"cache": "hit", "agent_name": agent_name, "response": hit}
    response = _call_endpoint(agent_name, input_dict)
    _save(key, agent_name, input_dict, response)
    return {"cache": ("bypass" if not use_cache else "miss"),
            "agent_name": agent_name, "response": response}
