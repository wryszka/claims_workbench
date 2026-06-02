# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 98 · Full end-to-end smoke test
# MAGIC
# MAGIC **Phase 10.** The single full end-to-end test — the one time we exercise the
# MAGIC whole chain together and assert at every hop. Each step is captured into a
# MAGIC PASS/FAIL table (one row per step); any red step makes the job fail loudly at
# MAGIC the end with a clear message.
# MAGIC
# MAGIC Sequence: reset → landing/bronze → silver → gold (3 headlines) → features →
# MAGIC models+serving → UC-fn tools → sub-agents → cache-first behaviour → audit
# MAGIC write → dashboard/Genie reachability.
# MAGIC
# MAGIC > Catalog/schema/warehouse via widgets (portable). `run_reset=false` skips the
# MAGIC > 15-min reset re-run and just asserts date freshness (use after a morning
# MAGIC > reset); `run_reset=true` triggers `claims_workbench_99_reset_demo` first.

# COMMAND ----------

# MAGIC %pip install databricks-sdk requests mlflow --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, sys, json, time
from datetime import datetime

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema")
dbutils.widgets.text("warehouse_id", "ab79eced8207d29b", "SQL warehouse")
dbutils.widgets.dropdown("run_reset", "false", ["true", "false"], "Run reset job first?")
dbutils.widgets.text("fraud_endpoint", "agents_lr_serverless_aws_us_catalog-claims_workbench-agent_frau", "Fraud agent endpoint")
dbutils.widgets.text("context_endpoint", "agents_lr_serverless_aws_us_catalog-claims_workbench-agent_cont", "Context agent endpoint")
dbutils.widgets.text("supervisor_endpoint", "", "Supervisor endpoint (blank = fallback)")

CAT = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
SCH = dbutils.widgets.get("schema").strip() or "claims_workbench"
WID = dbutils.widgets.get("warehouse_id").strip()
RUN_RESET = dbutils.widgets.get("run_reset").strip().lower() == "true"
FRAUD_EP = dbutils.widgets.get("fraud_endpoint").strip()
CONTEXT_EP = dbutils.widgets.get("context_endpoint").strip()
SUPER_EP = dbutils.widgets.get("supervisor_endpoint").strip()
VIVID = "cc:900001"
RESET_JOB_NAME = "claims_workbench_99_reset_demo"

spark.sql(f"USE CATALOG `{CAT}`")
spark.sql(f"USE SCHEMA `{SCH}`")
print(f"catalog={CAT} schema={SCH} run_reset={RUN_RESET}")

# config.py reads these at import time → set before importing the cache wrapper.
os.environ["CATALOG_NAME"] = CAT
os.environ["SCHEMA_NAME"] = SCH
os.environ["WAREHOUSE_ID"] = WID
os.environ["DATABRICKS_APP_NAME"] = "smoke-test"

# COMMAND ----------

# Test harness: each check() appends a row; assertions raise inside a step and are
# caught so the whole table is produced, then we fail loudly at the end if any red.
RESULTS = []


def check(step, fn):
    t0 = time.time()
    try:
        msg = fn() or "ok"
        status = "PASS"
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        status = "FAIL"
    secs = round(time.time() - t0, 1)
    RESULTS.append({"step": step, "status": status, "secs": secs, "detail": str(msg)[:160]})
    flag = "✅" if status == "PASS" else "❌"
    print(f"{flag} [{step}] {status} ({secs}s) — {str(msg)[:160]}")
    return status == "PASS"


def fq(t):
    return f"`{CAT}`.`{SCH}`.{t}"


def one(sql):
    return spark.sql(sql).collect()[0]


def count(t, where=""):
    w = f" WHERE {where}" if where else ""
    return int(one(f"SELECT count(*) c FROM {fq(t)}{w}")["c"])

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
HOST = w.config.host.rstrip("/")
import requests
HDR = w.config._header_factory()

# Resolve the fraud/context agent endpoints by substring so the test is portable
# across workspaces (agents.deploy truncates the name on some, keeps it full on others;
# 'agent_frau' is a substring of both 'agent_frau' and 'agent_fraud').
try:
    _eps = [e.name for e in w.serving_endpoints.list()]
    FRAUD_EP = next((n for n in _eps if "agent_frau" in n), FRAUD_EP)
    CONTEXT_EP = next((n for n in _eps if "agent_cont" in n), CONTEXT_EP)
    print(f"resolved fraud={FRAUD_EP} context={CONTEXT_EP}")
except Exception as e:
    print(f"endpoint resolve note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Reset & date freshness

# COMMAND ----------

def step_reset():
    if RUN_RESET:
        # Resolve the (DAB dev-prefixed) reset job by substring, trigger, and wait.
        job = next((j for j in w.jobs.list(name=RESET_JOB_NAME)), None)
        if not job:
            job = next((j for j in w.jobs.list()
                        if RESET_JOB_NAME in ((j.settings.name if j.settings else "") or "")), None)
        assert job, f"reset job '{RESET_JOB_NAME}' not found"
        run = w.jobs.run_now(job_id=job.job_id)
        rid = run.run_id
        print(f"  triggered reset run {rid}; polling…")
        for _ in range(120):  # up to ~30 min
            st = w.jobs.get_run(run_id=rid).state
            if st and st.life_cycle_state and str(st.life_cycle_state) not in ("RunLifeCycleState.RUNNING", "RunLifeCycleState.PENDING", "RunLifeCycleState.QUEUED"):
                assert str(st.result_state) == "RunResultState.SUCCESS", f"reset run {rid} ended {st.result_state}"
                break
            time.sleep(15)
        else:
            raise AssertionError(f"reset run {rid} did not finish in time")
    r = one(f"SELECT max(report_date) mx, min(report_date) mn, current_date() cd, "
            f"datediff(current_date(), max(report_date)) lag FROM {fq('silver_claims_enriched')}")
    assert r["lag"] is not None and r["lag"] <= 5, f"max(report_date)={r['mx']} is {r['lag']}d stale"
    return f"max={r['mx']} min={r['mn']} current={r['cd']} (lag {r['lag']}d){' [reset re-run]' if RUN_RESET else ' [freshness only]'}"

check("1·reset+dates", step_reset)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Landing / Bronze (tables populated, expectations, quarantine)

# COMMAND ----------

LANDING = ["landing_gw_cc_claim", "landing_gw_cc_exposure", "landing_gw_cc_incident",
           "landing_gw_cc_contact", "landing_gw_pc_policy", "landing_fraud_signals", "landing_weather"]
BRONZE = ["bronze_gw_cc_claim", "bronze_gw_cc_exposure", "bronze_gw_cc_incident",
          "bronze_gw_cc_contact", "bronze_gw_pc_policy", "bronze_fraud_signals_raw", "bronze_weather_raw"]


def step_bronze():
    for t in LANDING:
        assert count(t) > 0, f"landing table {t} empty"
    for t in BRONZE:
        assert count(t) > 0, f"bronze table {t} empty"
    qc = count("bronze_quarantine_claims")
    assert qc > 0, "quarantine_claims empty (expected dropped invalid_loss_cause rows)"
    # Expectation metrics present in the pipeline event log.
    exp_total = 0
    pipes = [p for p in w.pipelines.list_pipelines() if "claims_workbench_01_bronze_dlt" in (p.name or "")]
    if pipes:
        pid = pipes[0].pipeline_id
        evs = requests.get(f"{HOST}/api/2.0/pipelines/{pid}/events?max_results=250",
                           headers=HDR, timeout=60).json().get("events", [])
        for e in evs:
            dq = (e.get("details", {}).get("flow_progress", {}) or {}).get("data_quality")
            if dq:
                for ex in dq.get("expectations", []) or []:
                    exp_total += int(ex.get("passed_records") or 0) + int(ex.get("failed_records") or 0)
    assert exp_total > 0, "no expectation metrics found in pipeline event log"
    return f"7 landing + 7 bronze populated; quarantine_claims={qc:,}; expectation records={exp_total:,}"

check("2·landing/bronze", step_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Silver (row count, no dupes, key columns non-null)

# COMMAND ----------

def step_silver():
    sc = count("silver_claims_enriched")
    bc = count("bronze_gw_cc_claim")
    assert sc >= 0.95 * bc, f"silver {sc:,} << bronze claims {bc:,}"
    dupes = one(f"SELECT count(*) - count(DISTINCT claim_public_id) d FROM {fq('silver_claims_enriched')}")["d"]
    assert dupes == 0, f"{dupes} duplicate claim_public_id in silver"
    nulls = one(f"""SELECT
        sum(CASE WHEN peril_type IS NULL THEN 1 ELSE 0 END) p,
        sum(CASE WHEN reporting_lag_days IS NULL THEN 1 ELSE 0 END) r,
        sum(CASE WHEN weather_risk_composite IS NULL THEN 1 ELSE 0 END) w
        FROM {fq('silver_claims_enriched')}""")
    assert (nulls["p"], nulls["r"], nulls["w"]) == (0, 0, 0), f"null key cols: {nulls}"
    return f"silver={sc:,} (bronze claims {bc:,}); 0 dupes; key cols 0 null"

check("3·silver", step_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Gold (5 tables + the three headline numbers in range)

# COMMAND ----------

GOLD = ["gold_reserve_development", "gold_settlement_performance", "gold_geo_clustering",
        "gold_handler_scorecard", "gold_handler_decisions"]


def step_gold():
    for t in GOLD:
        spark.table(fq(t))  # presence
    # HEADLINE 1 — EoW reserve development ratio (under-reserving): expect ~1.1–1.5
    eow = one(f"""SELECT round(sum(sum_ultimate_reserve)/sum(sum_initial_reserve), 3) r
        FROM {fq('gold_reserve_development')} WHERE peril_type='home_escape_water'""")["r"]
    assert 1.05 <= eow <= 1.6, f"EoW dev ratio {eow} out of range"
    # HEADLINE 2 — NW EoW clustering: NW districts higher per-1000 than non-NW
    nw = spark.sql(f"""SELECT is_nw, round(avg(claims_per_1000_policies),1) a FROM (
            SELECT *, postcode_district rlike '^(M|BL|OL|WN)[0-9]' is_nw
            FROM {fq('gold_geo_clustering')} WHERE peril_type='home_escape_water')
        GROUP BY is_nw""").collect()
    nwm = {r["is_nw"]: r["a"] for r in nw}
    assert nwm.get(True, 0) > nwm.get(False, 0), f"NW EoW not elevated: {nwm}"
    # HEADLINE 3 — digital settles faster than phone
    ch = spark.sql(f"""SELECT report_channel, round(avg(days_to_settle),1) d
        FROM {fq('silver_claims_enriched')} WHERE days_to_settle IS NOT NULL
        GROUP BY report_channel""").collect()
    chm = {r["report_channel"]: r["d"] for r in ch}
    assert chm.get("digital", 1e9) < chm.get("phone", 0), f"digital not faster: {chm}"
    pct = round(100 * (chm["phone"] - chm["digital"]) / chm["phone"])
    return (f"EoW dev ratio={eow}; NW EoW {nwm.get(True)} vs {nwm.get(False)}/1000; "
            f"digital {chm['digital']}d vs phone {chm['phone']}d ({pct}% faster)")

check("4·gold+headlines", step_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Features (both tables, PK unique, encoder persisted)

# COMMAND ----------

def step_features():
    for t in ["feature_triage", "feature_reserve"]:
        n = count(t)
        d = one(f"SELECT count(*)-count(DISTINCT claim_public_id) d FROM {fq(t)}")["d"]
        assert n > 0, f"{t} empty"
        assert d == 0, f"{t} PK not unique ({d} dupes)"
    enc = count("ref_feature_encodings")
    assert enc > 0, "ref_feature_encodings (encoder) not persisted"
    return f"feature_triage={count('feature_triage'):,}, feature_reserve={count('feature_reserve'):,}, encodings={enc}"

check("5·features", step_features)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Models (champion alias) + serving endpoints READY

# COMMAND ----------

def step_models():
    import mlflow
    mlflow.set_registry_uri("databricks-uc")
    mc = mlflow.MlflowClient()
    vers = {}
    for m in ["model_triage_classifier", "model_reserve_bracket"]:
        mv = mc.get_model_version_by_alias(f"{CAT}.{SCH}.{m}", "champion")
        vers[m] = mv.version
    # DAB dev mode prefixes serving-endpoint names (dev_<user>_…); match by substring.
    all_eps = list(w.serving_endpoints.list())
    ready = {}
    for suffix in ["claims-workbench-triage", "claims-workbench-reserve"]:
        e = next((x for x in all_eps if x.name == suffix), None) or \
            next((x for x in all_eps if suffix in (x.name or "")), None)
        assert e, f"serving endpoint matching '{suffix}' not found"
        st = w.serving_endpoints.get(e.name).state
        ready[e.name] = str(st.ready) if st else None
        assert st and "READY" in str(st.ready), f"endpoint {e.name} not READY ({ready[e.name]})"
    return f"champions {vers}; endpoints READY {list(ready)}"

check("6·models+serving", step_models)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — UC-function tools (all 5 for the vivid claim; triage → refer_siu)

# COMMAND ----------

UC_FNS = ["fn_claim_summary", "fn_triage_claim", "fn_reserve_claim", "fn_fraud_signals", "fn_policy_history"]


def step_uc_fns():
    out = {}
    for fn in UC_FNS:
        r = one(f"SELECT to_json({fq(fn)}('{VIVID}')) j")["j"]
        assert r and len(r) > 2, f"{fn} returned empty"
        out[fn] = json.loads(r)
    tri = out["fn_triage_claim"]
    assert tri.get("decision") == "refer_siu", f"triage decision={tri.get('decision')} (expected refer_siu)"
    conf = tri.get("confidence", 0)
    assert 50 <= conf <= 100, f"triage confidence {conf} not sensible"
    return f"5/5 functions ok; triage={tri['decision']} @ {conf}% | reserve={out['fn_reserve_claim'].get('bracket')}"

check("7·uc-functions", step_uc_fns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Sub-agents (fraud + context return sensible responses)

# COMMAND ----------

# Make the cache-first wrapper importable (synced under .../files/app).
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_app = "/Workspace" + os.path.dirname(_ctx.notebookPath().get()).replace("/notebooks", "/app")
if _app not in sys.path:
    sys.path.insert(0, _app)
from utils.agent_cache import get_agent_response, cache_key  # noqa: E402
from utils.config import CACHE_TABLE  # noqa: E402


def _agent_text(ep, prompt, use_cache):
    payload = {"messages": [{"role": "user", "content": prompt}],
               "custom_inputs": {"claim_public_id": VIVID}}
    out = get_agent_response(ep, payload, use_cache=use_cache)
    msgs = out.get("response", {}).get("messages", [])
    txt = msgs[-1].get("content", "") if msgs else ""
    return txt, out.get("cache")


def step_agents():
    ft, fc = _agent_text(FRAUD_EP, f"Assess the fraud risk for claim {VIVID}.", True)
    ct, cc = _agent_text(CONTEXT_EP, f"Give me the before-you-pick-up-the-phone brief for {VIVID}.", True)
    assert len(ft) > 40, f"fraud agent response too short ({len(ft)} chars)"
    assert len(ct) > 40, f"context agent response too short ({len(ct)} chars)"
    return f"fraud {len(ft)} chars ({ft[:1]}…, cache={fc}); context {len(ct)} chars (cache={cc})"

check("8·sub-agents", step_agents)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Cache-first behaviour (miss→save, hit→fast, USE_CACHE=False→live)

# COMMAND ----------

def step_cache():
    nonce = str(time.time())
    inp = {"messages": [{"role": "user", "content": f"Smoke fraud check {VIVID}."}],
           "custom_inputs": {"claim_public_id": VIVID, "_smoke_nonce": nonce}}
    k = cache_key(FRAUD_EP, inp)

    def timed(uc):
        t0 = time.time()
        o = get_agent_response(FRAUD_EP, inp, use_cache=uc)
        return o["cache"], round(time.time() - t0, 2)

    c1, t1 = timed(True)    # miss → real + save
    c2, t2 = timed(True)    # hit  → fast
    c3, t3 = timed(False)   # bypass → real
    assert c1 == "miss", f"call1 expected miss got {c1}"
    assert c2 == "hit", f"call2 expected hit got {c2}"
    assert t2 < t1, f"hit ({t2}s) not faster than miss ({t1}s)"
    assert c3 in ("miss", "bypass", "real"), f"call3 (use_cache=False) cache flag={c3}"
    return f"miss {t1}s → hit {t2}s → live(use_cache=False) {t3}s; cache rows={count('cache_agent_responses')}"

check("9·cache-first", step_cache)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Audit write (simulate app HITL decision; row lands; then cleaned)

# COMMAND ----------

def step_audit():
    did = "SMOKE-" + datetime.utcnow().strftime("%H%M%S")
    spark.sql(f"""INSERT INTO {fq('gold_handler_decisions')}
        (decision_id, claim_public_id, model_recommendation, model_confidence,
         handler_action, override_flag, override_reason, handler_id, decision_ts)
        VALUES ('{did}', '{VIVID}', 'refer_siu', 87.0, 'accept', false, '',
                'smoke-test', current_timestamp())""")
    row = spark.sql(f"SELECT * FROM {fq('gold_handler_decisions')} WHERE decision_id='{did}'").collect()
    assert len(row) == 1, "audit row did not land"
    r = row[0].asDict()
    for f in ["claim_public_id", "model_recommendation", "handler_action", "handler_id", "decision_ts"]:
        assert r.get(f) is not None, f"audit row missing {f}"
    # Clean up so the audit table stays empty for the live demo beat.
    spark.sql(f"DELETE FROM {fq('gold_handler_decisions')} WHERE decision_id='{did}'")
    return f"row {did} landed with all fields (claim={r['claim_public_id']}, action={r['handler_action']}); cleaned up"

check("10·audit-write", step_audit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 — Reachability (Lakeview dashboard + Genie space resolve)

# COMMAND ----------

def step_reach():
    # Dashboard — resolve "Claims Portfolio" by display name.
    dash = requests.get(f"{HOST}/api/2.0/lakeview/dashboards?page_size=200", headers=HDR, timeout=60).json()
    did = next((d.get("dashboard_id") for d in dash.get("dashboards", [])
                if "Claims Portfolio" in (d.get("display_name") or "")), None)
    assert did, "Claims Portfolio dashboard not found"
    # Genie space — find a Claims space on THIS workspace (portable), else the config id.
    from utils import config as cfg
    spaces = requests.get(f"{HOST}/api/2.0/genie/spaces?page_size=100", headers=HDR, timeout=60).json().get("spaces", [])
    gid = next((s.get("space_id") for s in spaces if "Claims" in (s.get("title") or "")), cfg.GENIE_SPACE_ID)
    g = requests.get(f"{HOST}/api/2.0/genie/spaces/{gid}", headers=HDR, timeout=60)
    assert g.status_code == 200, f"genie space {gid} not reachable (HTTP {g.status_code})"
    return f"dashboard {did[:8]}… resolved; genie space {gid[:8]}… reachable"

check("11·reachability", step_reach)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — PASS/FAIL table (one row per step)

# COMMAND ----------

import pandas as pd
df = pd.DataFrame(RESULTS)
n_pass = (df["status"] == "PASS").sum()
n_fail = (df["status"] == "FAIL").sum()
print("=" * 78)
print(f"  CLAIMS WORKBENCH SMOKE TEST — {n_pass} PASS / {n_fail} FAIL")
print("=" * 78)
print(df.to_string(index=False))
print("=" * 78)
display(df)

# COMMAND ----------

summary = {"pass": int(n_pass), "fail": int(n_fail),
           "results": RESULTS, "run_reset": RUN_RESET, "catalog": CAT, "schema": SCH}
if n_fail:
    failed = [r["step"] for r in RESULTS if r["status"] == "FAIL"]
    raise AssertionError(f"SMOKE TEST FAILED on {n_fail} step(s): {failed} — see table above.")
dbutils.notebook.exit(json.dumps(summary))
