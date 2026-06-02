"""Single source of config for the Claims AI app + cache layer.

Read by the app and notebooks. Override via environment variables (the app.yaml
sets them on the Databricks App). `USE_CACHE` is the single switch for cache-first
vs always-live. Catalog/schema/warehouse accept both the app-style env names
(CATALOG_NAME / SCHEMA_NAME / WAREHOUSE_ID) and the CLAIMS_* names used by notebooks.
"""
import os


def _env(*names, default=""):
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
    return default


def _flag(name: str, default: bool) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


# Cache-first by default. Set USE_CACHE=false to always call the real endpoints.
USE_CACHE = _flag("USE_CACHE", True)

CATALOG = _env("CATALOG_NAME", "CLAIMS_CATALOG", default="lr_serverless_aws_us_catalog")
SCHEMA = _env("SCHEMA_NAME", "CLAIMS_SCHEMA", default="claims_workbench")
WAREHOUSE_ID = _env("WAREHOUSE_ID", "CLAIMS_WAREHOUSE_ID", default="ab79eced8207d29b")

CACHE_TABLE = f"{CATALOG}.{SCHEMA}.cache_agent_responses"

# Agent serving endpoints (agents.deploy auto-names; override per workspace via env).
ENDPOINT_FRAUD = _env("CLAIMS_EP_FRAUD",
                      default="agents_lr_serverless_aws_us_catalog-claims_workbench-agent_frau")
ENDPOINT_CONTEXT = _env("CLAIMS_EP_CONTEXT",
                        default="agents_lr_serverless_aws_us_catalog-claims_workbench-agent_cont")
# Set once the managed Supervisor is created (RUNBOOK Stage C). When blank, the
# synthesis box falls back to the Context agent via the same cache wrapper.
ENDPOINT_SUPERVISOR = _env("CLAIMS_EP_SUPERVISOR", default="")

# Governance & Portfolio section (Stage B)
GENIE_SPACE_ID = _env("GENIE_SPACE_ID", default="01f15e4e509f1410b5596f5c90b20ca4")
DASHBOARD_ID = _env("DASHBOARD_ID", default="")

# Reset-demo job (built in Phase 9 — the app triggers it by name; absent = graceful).
RESET_JOB_NAME = _env("RESET_JOB_NAME", default="claims_workbench_99_reset_demo")
