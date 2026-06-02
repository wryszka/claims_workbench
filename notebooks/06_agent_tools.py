# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 06 · Agent UC-function tools
# MAGIC
# MAGIC **Phase 6, Stage A.** Creates the five Unity Catalog functions the Supervisor
# MAGIC Agent and sub-agents route over. Rich COMMENTs double as the routing
# MAGIC capability statements.
# MAGIC
# MAGIC - `fn_triage_claim`  — scores the triage endpoint (probabilities) → decision + confidence% + reasons
# MAGIC - `fn_reserve_claim` — scores the reserve endpoint → bracket + £ range + rationale
# MAGIC - `fn_fraud_signals` — raw governed fraud signals (no model)
# MAGIC - `fn_policy_history`— policy summary for the briefing
# MAGIC - `fn_claim_summary` — core enriched claim context
# MAGIC
# MAGIC Endpoint names are resolved at run time (DAB dev mode prefixes them), so this
# MAGIC is portable. UC functions don't support UC tags (`layer=agent` is recorded in
# MAGIC the function COMMENT instead — `ALTER FUNCTION ... SET TAGS` is unsupported in UC).

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema (fixed)")
catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
FQ = f"`{catalog}`.`{schema}`"
print(f"[target] {catalog}.{schema}")

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
names = [e.name for e in w.serving_endpoints.list()]
TRIAGE_EP = next(n for n in names if n.endswith("claims-workbench-triage"))
RESERVE_EP = next(n for n in names if n.endswith("claims-workbench-reserve"))
print(f"triage endpoint:  {TRIAGE_EP}")
print(f"reserve endpoint: {RESERVE_EP}")

TRIAGE_COLS = ["peril_type_encoded", "report_channel_encoded", "reported_amount_log",
               "sum_insured_to_reported_ratio", "fraud_score", "prior_claims_12m",
               "reporting_lag_days", "policy_tenure_years", "weather_risk_composite",
               "is_high_value", "at_fault", "third_party_involved", "postcode_flood_risk"]
RESERVE_COLS = ["peril_type_encoded", "handler_grade_encoded", "reported_amount_log",
                "fraud_score", "prior_claims_12m", "weather_risk_composite", "days_open",
                "triage_decision_encoded", "sum_insured_log"]


def agg_inner(cols, table):
    sel = ", ".join(f"any_value(CAST({c} AS DOUBLE)) AS {c}" for c in cols)
    return f"SELECT {sel} FROM {FQ}.{table} WHERE claim_public_id = p_claim_public_id"


def nstruct(cols):
    return "named_struct(" + ", ".join(f"'{c}', {c}" for c in cols) + ")"

# COMMAND ----------

# fn_triage_claim — aggregate the (deterministic) feature row first, then ai_query
# on that single row (ai_query is non-deterministic so cannot sit inside any_value).
spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.fn_triage_claim(p_claim_public_id STRING)
RETURNS STRUCT<decision STRING, confidence DOUBLE, top_reasons ARRAY<STRING>>
COMMENT 'Decide how to handle a new claim: pay directly, escalate, or refer to SIU. Scores the FNOL triage model for a given claim_public_id and returns the recommended decision, a confidence percentage, and 2-3 plain-English reasons. Use when a handler asks what to do with a specific claim.'
RETURN
  SELECT named_struct(
    'decision', element_at(array('escalate','pay_direct','refer_siu'), CAST(array_position(p, array_max(p)) AS INT)),
    'confidence', round(array_max(p) * 100, 1),
    'top_reasons', slice(array_compact(array(
        CASE WHEN fraud_score > 70 THEN concat('High fraud score (', CAST(fraud_score AS INT), '/100)') END,
        CASE WHEN prior_claims_12m >= 2 THEN concat(CAST(prior_claims_12m AS INT), ' prior claims in 12 months') END,
        CASE WHEN reporting_lag_days > 14 THEN concat('Reported ', CAST(reporting_lag_days AS INT), ' days after the incident') END,
        CASE WHEN is_high_value = 1 THEN 'High-value claim (over GBP 10,000)' END
    )), 1, 3)
  )
  FROM (
    SELECT ai_query('{TRIAGE_EP}', {nstruct(TRIAGE_COLS)}, 'ARRAY<DOUBLE>') AS p,
           fraud_score, prior_claims_12m, reporting_lag_days, is_high_value
    FROM ( {agg_inner(TRIAGE_COLS, 'feature_triage')} )
  )
""")

# fn_reserve_claim
spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.fn_reserve_claim(p_claim_public_id STRING)
RETURNS STRUCT<bracket STRING, estimated_range STRING, rationale STRING>
COMMENT 'Predict the financial reserve bracket for a claim (LOW, MEDIUM, HIGH, or LARGE LOSS) and an indicative GBP range. Scores the reserve model for a claim_public_id. Use to estimate how much money to set aside for a claim.'
RETURN
  SELECT named_struct(
    'bracket', b,
    'estimated_range', CASE b WHEN 'LOW' THEN 'under GBP 2,000' WHEN 'MEDIUM' THEN 'GBP 2,000 to 10,000'
                              WHEN 'HIGH' THEN 'GBP 10,000 to 50,000' ELSE 'over GBP 50,000' END,
    'rationale', concat('Predicted ', b, ' reserve from reported amount, peril, prior history and handler grade.')
  )
  FROM (
    SELECT element_at(array('LOW','MEDIUM','HIGH','LARGE LOSS'), CAST(idx AS INT) + 1) AS b
    FROM (
      SELECT CAST(ai_query('{RESERVE_EP}', {nstruct(RESERVE_COLS)}, 'DOUBLE') AS INT) AS idx
      FROM ( {agg_inner(RESERVE_COLS, 'feature_reserve')} )
    )
  )
""")

# fn_fraud_signals — no model call
spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.fn_fraud_signals(p_claim_public_id STRING)
RETURNS STRUCT<fraud_score INT, fraud_flag BOOLEAN, prior_claims_12m INT, days_since_incident INT, reporting_lag_days INT>
COMMENT 'Return the raw governed fraud signals for a claim (fraud score 0-100, fraud flag, prior claims in 12 months, days since incident, reporting lag in days) WITHOUT scoring any model - for an analyst or agent to reason over.'
RETURN
  SELECT named_struct('fraud_score', any_value(CAST(fraud_score AS INT)), 'fraud_flag', any_value(fraud_flag),
                      'prior_claims_12m', any_value(CAST(prior_claims_12m AS INT)),
                      'days_since_incident', any_value(CAST(days_since_incident AS INT)),
                      'reporting_lag_days', any_value(CAST(reporting_lag_days AS INT)))
  FROM {FQ}.silver_claims_enriched WHERE claim_public_id = p_claim_public_id
""")

# fn_policy_history
spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.fn_policy_history(p_claim_public_id STRING)
RETURNS STRUCT<product STRING, sum_insured DECIMAL(12,2), policy_tenure_years DOUBLE, annual_premium DECIMAL(12,2), prior_claims_12m INT>
COMMENT 'Summarise the policy behind a claim: product (motor or home), sum insured (GBP), policy tenure in years, annual premium (GBP) and the number of prior claims in the last 12 months. For building a handler briefing.'
RETURN
  SELECT named_struct('product', any_value(product), 'sum_insured', any_value(sum_insured),
                      'policy_tenure_years', any_value(policy_tenure_years),
                      'annual_premium', any_value(annual_premium),
                      'prior_claims_12m', any_value(CAST(prior_claims_12m AS INT)))
  FROM {FQ}.silver_claims_enriched WHERE claim_public_id = p_claim_public_id
""")

# fn_claim_summary
spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.fn_claim_summary(p_claim_public_id STRING)
RETURNS STRUCT<peril_type STRING, total_incurred DECIMAL(12,2), report_channel STRING, postcode_district STRING, incident_description STRING, claim_status STRING>
COMMENT 'Core enriched summary of a claim: peril type, total incurred (GBP), report channel, claimant postcode district, incident description and current claim status. Shared context for any claim question.'
RETURN
  SELECT named_struct('peril_type', any_value(peril_type), 'total_incurred', any_value(total_incurred),
                      'report_channel', any_value(report_channel), 'postcode_district', any_value(postcode_district),
                      'incident_description', any_value(description_text), 'claim_status', any_value(claim_status))
  FROM {FQ}.silver_claims_enriched WHERE claim_public_id = p_claim_public_id
""")

# fn_recovery_signals — subrogation / recovery potential (Phase 11, no model call)
spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.fn_recovery_signals(p_claim_public_id STRING)
RETURNS STRUCT<recovery_flag BOOLEAN, third_party_at_fault BOOLEAN, recoverable_amount DECIMAL(12,2), at_fault BOOLEAN, third_party_involved BOOLEAN, peril_type STRING>
COMMENT 'Assess recovery / subrogation potential for a claim: whether money can be recovered from a third party (e.g. a not-at-fault motor third-party loss), the recoverable GBP amount, and the fault signals behind it. Use to decide if a claim has recovery potential.'
RETURN
  SELECT named_struct('recovery_flag', any_value(recovery_flag),
                      'third_party_at_fault', any_value(third_party_at_fault),
                      'recoverable_amount', any_value(recoverable_amount),
                      'at_fault', any_value(at_fault),
                      'third_party_involved', any_value(third_party_involved),
                      'peril_type', any_value(peril_type))
  FROM {FQ}.silver_claims_enriched WHERE claim_public_id = p_claim_public_id
""")

# fn_decision_reasoning — the auto-close / triage disposition + full reasoning for a
# claim (reads the gold_claim_disposition audit). Feeds the Audit / Reasoning agent.
spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.fn_decision_reasoning(p_claim_public_id STRING)
RETURNS STRUCT<disposition STRING, model_decision STRING, model_confidence DOUBLE, total_incurred DECIMAL(12,2), fraud_score INT, data_complete BOOLEAN, rules_passed ARRAY<STRING>, rules_failed ARRAY<STRING>, reasoning STRING>
COMMENT 'Return the workflow disposition (auto_closed / escalated) for a claim with the full reasoning: which auto-close rules passed or failed, the contributing values, and the model confidence. Use to explain WHY a claim was auto-closed or escalated, for a regulator-readable audit.'
RETURN
  SELECT named_struct('disposition', any_value(disposition), 'model_decision', any_value(model_decision),
                      'model_confidence', any_value(model_confidence), 'total_incurred', any_value(total_incurred),
                      'fraud_score', any_value(CAST(fraud_score AS INT)), 'data_complete', any_value(data_complete),
                      'rules_passed', any_value(rules_passed), 'rules_failed', any_value(rules_failed),
                      'reasoning', any_value(reasoning))
  FROM {FQ}.gold_claim_disposition WHERE claim_public_id = p_claim_public_id
""")

print("7 UC functions created.")

# COMMAND ----------

# Smoke-test each for the vivid claims
import json
for fn in ["fn_triage_claim", "fn_reserve_claim", "fn_fraud_signals", "fn_policy_history",
           "fn_claim_summary", "fn_recovery_signals", "fn_decision_reasoning"]:
    for cid in ["cc:900001", "cc:900002"]:
        try:
            r = spark.sql(f"SELECT to_json({FQ}.{fn}('{cid}')) AS r").collect()[0]["r"]
            print(f"{fn}({cid}): {r}")
        except Exception as e:
            print(f"{fn}({cid}): [skipped — {str(e)[:80]}]")
