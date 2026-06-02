# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 07 · Governance & Lineage
# MAGIC
# MAGIC **Bricksurance SE** — Phase 7. The trust story for a regulated insurer, every
# MAGIC item backed by **genuine** Unity Catalog / MLflow / DLT output (no slideware).
# MAGIC
# MAGIC > Synthetic demo — no real Guidewire integration, no real customer data.
# MAGIC
# MAGIC Sections: (1) end-to-end lineage, (2) PII masking, (3) tag-visibility fallback,
# MAGIC (4) model provenance / cards, (5) data-quality evidence + HITL audit trail.

# COMMAND ----------

# MAGIC %pip install mlflow databricks-sdk requests --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json, requests
import mlflow
from mlflow.tracking import MlflowClient
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema")
catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
fqn = f"{catalog}.{schema}"
mlflow.set_registry_uri("databricks-uc")
w = WorkspaceClient()
HOST = w.config.host.rstrip("/")
HDR = w.config._header_factory()
print(f"[target] {fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · End-to-end lineage
# MAGIC Uses the table-lineage REST API (same source as Catalog Explorer's Lineage
# MAGIC tab — near real-time). Model/endpoint hops are not table-lineage entities;
# MAGIC their provenance is MLflow (section 4) — stated honestly, not faked.

# COMMAND ----------

def upstreams(table_full_name):
    try:
        r = requests.get(f"{HOST}/api/2.0/lineage-tracking/table-lineage",
                         headers=HDR, json={"table_name": table_full_name, "include_entity_lineage": False},
                         timeout=60)
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}"
        ups = []
        for u in r.json().get("upstreams", []) or []:
            ti = u.get("tableInfo") or {}
            nm = ti.get("name")
            if nm:
                ups.append(f"{ti.get('catalog_name')}.{ti.get('schema_name')}.{nm}")
        return ups, None
    except Exception as e:
        return None, str(e)

CHAIN = ["landing_gw_cc_claim", "bronze_gw_cc_claim", "silver_claims_enriched",
         "feature_triage", "gold_handler_decisions"]
print("Table-lineage chain (vivid claim cc:900001 flows through these tables):\n")
for t in CHAIN:
    full = f"{fqn}.{t}"
    ups, err = upstreams(full)
    if err:
        print(f"  {t:<26} upstream lookup unavailable: {err}")
    elif ups:
        rel = [u.split(".")[-1] for u in ups if u.startswith(fqn)]
        print(f"  {t:<26} <- {rel if rel else ups}")
    else:
        print(f"  {t:<26} <- (no upstream tables captured — source/leaf or lineage latency)")

print("\nModel/endpoint hops (NOT table-lineage entities — provenance via MLflow, see section 4):")
print("  feature_triage -> model_triage_classifier (fe.create_training_set read; MLflow run)")
print("  model_triage_classifier @champion -> serving endpoint (Mosaic AI; UC model version)")
print("  endpoint decisions -> gold_handler_decisions (written by the Phase 8 app / HITL)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · PII protection — dynamic masking view
# MAGIC Column-mask via a dynamic view (works on any workspace — no governed tags).
# MAGIC Privileged group `claims_workbench_pii_readers` sees raw PII; everyone else
# MAGIC sees masked. Demonstrated masked (current user) vs unmasked (base table).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fqn}.v_claims_masked AS
SELECT
  s.claim_public_id, s.peril_type, s.total_incurred, s.claim_status, s.handler_grade,
  CASE WHEN is_account_group_member('claims_workbench_pii_readers')
       THEN s.postcode_district
       ELSE concat(regexp_extract(s.postcode_district, '^[A-Za-z]+', 0), ' ***') END AS postcode_district,
  CASE WHEN is_account_group_member('claims_workbench_pii_readers')
       THEN h.handler_name
       ELSE concat('handler_', substr(sha2(h.handler_name, 256), 1, 8)) END AS handler_name
FROM {fqn}.silver_claims_enriched s
LEFT JOIN {fqn}.ref_handlers h USING (handler_id)
""")
try:
    spark.sql(f"ALTER VIEW {fqn}.v_claims_masked SET TBLPROPERTIES "
              f"('project'='claims_workbench','layer'='gov','pii'='masked')")
except Exception as e:
    print(f"(view tblproperties note: {e})")

print("MASKED view (as the current, non-privileged user) — cc:900001:")
spark.sql(f"SELECT claim_public_id, postcode_district, handler_name, peril_type "
          f"FROM {fqn}.v_claims_masked WHERE claim_public_id='cc:900001'").show(truncate=False)

print("UNMASKED base data (privileged equivalent) — cc:900001:")
spark.sql(f"""
  SELECT s.claim_public_id, s.postcode_district, h.handler_name, s.peril_type
  FROM {fqn}.silver_claims_enriched s LEFT JOIN {fqn}.ref_handlers h USING (handler_id)
  WHERE s.claim_public_id='cc:900001'
""").show(truncate=False)

print("Intended grants (workspace-specific role setup):")
print("  GRANT SELECT ON VIEW {fqn}.v_claims_masked TO `claims_handlers`;  -- handlers use the masked view")
print("  Members of group `claims_workbench_pii_readers` (SIU / data-protection) see raw PII.")
print("  Base tables (silver/ref) restricted to data-engineering; handlers query only the view.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2b · SECRET sensitivity tier (Phase 11) — claim narrative / health detail
# MAGIC A tier ABOVE PII. The free-text claim description can carry health/injury detail
# MAGIC and other special-category data, so it is classed **Secret** and withheld from
# MAGIC everyone except the privileged group `claims_workbench_secret_readers` (SIU /
# MAGIC DPO). Secret data should sit on **Customer-Managed Keys (CMK)** — see
# MAGIC `GOVERNANCE_NOTES.md` for the CMK / Lakebase positioning.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fqn}.v_claims_secret AS
SELECT
  s.claim_public_id, s.peril_type, s.total_incurred, s.claim_status,
  -- Sensitivity tier of the narrative (drives who-sees-what in the governance page).
  'Secret' AS narrative_sensitivity,
  -- Derived health/injury signal over the narrative (itself Secret).
  CASE WHEN is_account_group_member('claims_workbench_secret_readers')
       THEN (lower(s.description_text) rlike '(injur|medical|hospital|whiplash|health|ambulance)')
       ELSE NULL END AS injury_mentioned,
  -- The claim narrative itself: Secret. Withheld unless privileged.
  CASE WHEN is_account_group_member('claims_workbench_secret_readers')
       THEN s.description_text
       ELSE '[SECRET — claim narrative withheld; CMK-protected]' END AS description_text
FROM {fqn}.silver_claims_enriched s
""")
try:
    spark.sql(f"ALTER VIEW {fqn}.v_claims_secret SET TBLPROPERTIES "
              f"('project'='claims_workbench','layer'='gov','sensitivity'='secret')")
except Exception as e:
    print(f"(view tblproperties note: {e})")

print("SECRET view (as the current, non-privileged user) — cc:900001 narrative withheld:")
spark.sql(f"SELECT claim_public_id, narrative_sensitivity, injury_mentioned, description_text "
          f"FROM {fqn}.v_claims_secret WHERE claim_public_id='cc:900001'").show(truncate=False)
print("Grants: GRANT SELECT ON VIEW {fqn}.v_claims_secret TO `claims_workbench_secret_readers`;")
print("  Secret tier (narrative / health) -> CMK-encrypted storage; tighter audit than PII.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Tag-visibility fallback (resolves the Phase 1 governed-tag gap)
# MAGIC The governed `project`/`owner` UC tags were blocked on this workspace. We set
# MAGIC `TBLPROPERTIES('project'='claims_workbench')` on every asset as a fallback that
# MAGIC always works, and list assets by it. The native-tag admin fix is in GOVERNANCE_NOTES.md.

# COMMAND ----------

objs = spark.sql(f"SHOW TABLES IN {fqn}").collect()
type_map = {r["table_name"]: r["table_type"] for r in spark.sql(f"""
    SELECT table_name, table_type FROM {catalog}.information_schema.tables WHERE table_schema='{schema}'
""").collect()}
applied, tagged = 0, []
for o in objs:
    name = o.tableName
    kind = "VIEW" if type_map.get(name) == "VIEW" else "TABLE"
    try:
        spark.sql(f"ALTER {kind} {fqn}.`{name}` SET TBLPROPERTIES ('project'='claims_workbench')")
        applied += 1
    except Exception as e:
        print(f"  could not set property on {name}: {str(e)[:80]}")

# List all assets carrying the fallback property
for o in objs:
    name = o.tableName
    try:
        props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {fqn}.`{name}`").collect()}
        if props.get("project") == "claims_workbench":
            tagged.append(name)
    except Exception:
        pass
print(f"Set project property on {applied} assets.")
print(f"Assets carrying project=claims_workbench ({len(tagged)}):")
for t in sorted(tagged):
    print(f"  {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Model provenance / model cards (genuine MLflow/UC metadata)

# COMMAND ----------

mc = MlflowClient(registry_uri="databricks-uc")

def model_card(model):
    name = f"{fqn}.{model}"
    print(f"\n=== {name} ===")
    try:
        champ = mc.get_model_version_by_alias(name, "champion")
    except Exception as e:
        print(f"  no @champion alias: {e}"); return
    print(f"  champion: v{champ.version}")
    print(f"  created:  {champ.creation_timestamp}")
    print(f"  source:   {champ.source}")
    print(f"  run_id:   {champ.run_id}")
    # Walk versions to surface the training run that carries validation metrics
    for v in sorted(mc.search_model_versions(f"name='{name}'"), key=lambda x: int(x.version)):
        try:
            run = mc.get_run(v.run_id) if v.run_id else None
        except Exception:
            run = None
        m = run.data.metrics if run else {}
        src = (run.data.tags.get("mlflow.source.name", "") if run else "")[-50:]
        acc = m.get("accuracy"); f1 = m.get("macro_f1")
        tag = " <- @champion" if v.version == champ.version else ""
        metricstr = (f"accuracy={acc:.4f} macroF1={f1:.4f}" if acc is not None else "no metrics on this version")
        print(f"    v{v.version}: {metricstr} | src={src}{tag}")

for model in ["model_triage_classifier", "model_reserve_bracket"]:
    model_card(model)

print("\nFeature->model provenance: Phase 5 training used fe.create_training_set over")
print(f"  {fqn}.feature_triage / feature_reserve (see notebooks/05_ml_models.py). The deployed")
print("  models are logged feature-vector (Phase 5 serving decision, no online store), so the")
print("  feature lineage is the training-run READ of the feature tables (shown via lineage in")
print("  section 1 / Catalog Explorer), not an embedded feature spec.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Data-quality evidence + HITL audit trail

# COMMAND ----------

# --- DLT expectation pass/fail from the Phase 1 pipeline event log ---
pid = None
for p in w.pipelines.list_pipelines(filter="name LIKE 'claims_workbench_01_bronze_dlt'"):
    pid = p.pipeline_id
    break
print(f"bronze DLT pipeline id: {pid}")
expectations = {}
if pid:
    try:
        evs = requests.get(f"{HOST}/api/2.0/pipelines/{pid}/events?max_results=250",
                           headers=HDR, timeout=60).json().get("events", [])
        for e in evs:
            dq = (e.get("details", {}).get("flow_progress", {}) or {}).get("data_quality")
            if not dq:
                continue
            for ex in dq.get("expectations", []) or []:
                k = ex.get("name")
                cur = expectations.setdefault(k, {"passed": 0, "failed": 0})
                cur["passed"] += int(ex.get("passed_records") or 0)
                cur["failed"] += int(ex.get("failed_records") or 0)
    except Exception as e:
        print(f"event log query note: {e}")

if expectations:
    tot_p = sum(v["passed"] for v in expectations.values())
    tot_f = sum(v["failed"] for v in expectations.values())
    rate = 100 * tot_p / max(tot_p + tot_f, 1)
    print("DLT data-quality expectations (Phase 1 bronze pipeline):")
    for k, v in sorted(expectations.items()):
        print(f"  {k:<22} passed={v['passed']:>7,} failed={v['failed']:>5,}")
    qn = spark.sql(f"SELECT (SELECT count(*) FROM {fqn}.bronze_quarantine_claims) + "
                   f"(SELECT count(*) FROM {fqn}.bronze_quarantine_fraud_signals) AS n").collect()[0]["n"]
    print(f"\n  >>> {rate:.2f}% of evaluated records passed their quality rules; "
          f"{qn:,} bad records quarantined (not silently dropped).")
else:
    print("No expectation metrics retrieved from the event log (run Phase 1 first / latency).")

# COMMAND ----------

# --- HITL audit trail: gold_handler_decisions ---
print("HITL audit trail — gold_handler_decisions (FCA / Consumer-Duty evidence):")
spark.sql(f"DESCRIBE {fqn}.gold_handler_decisions").show(truncate=False)
n = spark.table(f"{fqn}.gold_handler_decisions").count()
print(f"rows: {n} — empty by design until the Phase 8 app writes each decision")
print("Every HITL decision will record: model_recommendation, model_confidence, handler_action,")
print("override_flag, override_reason, handler_id, decision_ts — a complete, queryable audit of")
print("what the model advised, what the human did, and why — the regulator-facing accountability trail.")

# COMMAND ----------

print("Phase 7 governance checks complete.")
