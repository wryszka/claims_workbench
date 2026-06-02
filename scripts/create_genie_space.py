#!/usr/bin/env python3
"""Create the "Claims AI — Ask the Book" Genie space over the four gold tables.

Phase 6, Stage A. Uses the Genie REST API (POST /api/2.0/genie/spaces) — no UI.
Run with the Databricks CLI configured (profile via --profile or DEFAULT):

    python3 scripts/create_genie_space.py \
        --catalog lr_serverless_aws_us_catalog --warehouse-id <id> --profile DEFAULT

Notes (this API version):
  * data_sources.tables MUST be sorted by identifier.
  * serialized_space is a JSON *string* containing only data_sources on create;
    instructions + curated sample questions are added afterwards in the Genie UI
    (see RUNBOOK_AGENT_SETUP.md) — the create call rejects them inline.
Prints GENIE_SPACE_ID=<id> on success.
"""
import argparse, json, subprocess, sys

GOLD = sorted(["gold_reserve_development", "gold_settlement_performance",
               "gold_geo_clustering", "gold_handler_scorecard"])

# Phase 11 "Ask Pricing + Claims" — the joined cross-domain view (+ the gold book).
JOINED = sorted(["gold_policy_claims_joined", "gold_reserve_development",
                 "gold_settlement_performance", "gold_geo_clustering"])

SPACES = {
    "book": {
        "title": "Claims AI - Ask the Book",
        "description": "Ask portfolio/book-level analytics questions about Bricksurance SE claims: "
                       "reserve development, settlement speed by channel, geographic risk clustering, "
                       "and handler performance.",
        "tables": GOLD,
    },
    "joined": {
        "title": "Claims AI - Ask Pricing + Claims",
        "description": "Ask CROSS-DOMAIN questions spanning the claims book and the policy/pricing "
                       "population: loss ratio by product and peril, premium adequacy, leakage versus "
                       "premium, and recovery potential. For actuaries and pricing analysts.",
        "tables": JOINED,
    },
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", default="claims_workbench")
    ap.add_argument("--warehouse-id", required=True)
    ap.add_argument("--profile", default="DEFAULT")
    ap.add_argument("--space", default="book", choices=list(SPACES),
                    help="book = Ask the Book (Phase 6); joined = Ask Pricing + Claims (Phase 11)")
    a = ap.parse_args()

    spec = SPACES[a.space]
    user = json.loads(subprocess.run(
        ["databricks", "current-user", "me", "--profile", a.profile],
        capture_output=True, text=True).stdout)["userName"]
    tables = [{"identifier": f"{a.catalog}.{a.schema}.{t}"} for t in sorted(spec["tables"])]
    body = {
        "title": spec["title"],
        "description": spec["description"],
        "warehouse_id": a.warehouse_id,
        "parent_path": f"/Workspace/Users/{user}",
        "serialized_space": json.dumps({"version": 2, "data_sources": {"tables": tables}}),
    }
    out = subprocess.run(["databricks", "api", "post", "/api/2.0/genie/spaces",
                          "--profile", a.profile, "--json", json.dumps(body)],
                         capture_output=True, text=True)
    try:
        sid = json.loads(out.stdout)["space_id"]
    except Exception:
        print("Genie create failed:", (out.stdout or out.stderr)[:500]); sys.exit(1)
    print(f"GENIE_SPACE_ID={sid}")


if __name__ == "__main__":
    main()
