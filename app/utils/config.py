"""Single source of config for the Claims AI cache layer (Phase 6, Stage C).

Read by both the app and notebooks. Override via environment variables.
`USE_CACHE` is the single switch that flips cache-first vs always-real.
"""
import os


def _flag(name: str, default: bool) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


# Cache-first by default. Set USE_CACHE=false to always call the real endpoints.
USE_CACHE = _flag("USE_CACHE", True)

CATALOG = os.environ.get("CLAIMS_CATALOG", "lr_serverless_aws_us_catalog")
SCHEMA = os.environ.get("CLAIMS_SCHEMA", "claims_workbench")
WAREHOUSE_ID = os.environ.get("CLAIMS_WAREHOUSE_ID", "ab79eced8207d29b")

CACHE_TABLE = f"{CATALOG}.{SCHEMA}.cache_agent_responses"

# Endpoint names (agents.deploy auto-names; override per workspace via env).
ENDPOINT_FRAUD = os.environ.get(
    "CLAIMS_EP_FRAUD", "agents_lr_serverless_aws_us_catalog-claims_workbench-agent_frau")
ENDPOINT_CONTEXT = os.environ.get(
    "CLAIMS_EP_CONTEXT", "agents_lr_serverless_aws_us_catalog-claims_workbench-agent_cont")
# Set once the managed Supervisor is created (RUNBOOK Stage C).
ENDPOINT_SUPERVISOR = os.environ.get("CLAIMS_EP_SUPERVISOR", "")
