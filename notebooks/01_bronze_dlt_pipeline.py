# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 01 · Bronze DLT Pipeline
# MAGIC
# MAGIC **Bricksurance SE** — Phase 1. A Delta Live Tables (Lakeflow Declarative)
# MAGIC pipeline that reads the **landing zone** (a simulated Guidewire ClaimCenter
# MAGIC CDA drop, produced by Phase 0) and produces governed **bronze** tables with
# MAGIC data-quality expectations and quarantine.
# MAGIC
# MAGIC > **About this demo.** Synthetic data only — fictional company, policies and
# MAGIC > figures. No real Guidewire integration, no real customer data.
# MAGIC
# MAGIC **Pattern:** `landing_*` (raw CDA) → **`bronze_*` (governed)** → silver (later).
# MAGIC Landing is the *only* source — bronze is never re-read here.
# MAGIC
# MAGIC **Expectations demonstrate all three DLT behaviours**
# MAGIC - `expect` (track, retain): `valid_policy_number`, `valid_report_channel`
# MAGIC - `expect_or_drop` (drop bad rows): `valid_loss_cause`, `fraud_score_range`
# MAGIC - Dropped rows are also captured in `bronze_quarantine_*` so nothing is lost
# MAGIC   silently — the "where did the bad records go" moment.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# In a UC pipeline the default catalog/schema is the pipeline target, so
# currentCatalog() resolves correctly; a pipeline `configuration` override
# (source_catalog/source_schema) takes precedence if set. Mirrors Phase 0's
# "variable, else workspace current catalog" resolution.
CATALOG = spark.conf.get("source_catalog", "") or spark.catalog.currentCatalog()
SCHEMA = spark.conf.get("source_schema", "") or "claims_workbench"


def _landing(name):
    return f"`{CATALOG}`.`{SCHEMA}`.{name}"


def _read_landing_stream(name):
    # Landing tables are written in overwrite mode by Phase 0; skipChangeCommits
    # lets the stream consume the snapshot without failing on the rewrite.
    return (spark.readStream
            .option("skipChangeCommits", "true")
            .table(_landing(name)))


VALID_LOSS_CAUSES = "('vehcollision','waterdamage','windhail','fire')"
VALID_CHANNELS = "('digital','phone','broker_email')"

# --------------------------------------------------------------------------
# Bronze: Guidewire ClaimCenter — claim header (the expectation showcase)
# --------------------------------------------------------------------------
@dlt.table(
    name="bronze_gw_cc_claim",
    comment="Governed bronze Guidewire CC claim header (typed + quality-gated).",
    table_properties={"quality": "bronze", "layer": "bronze"},
)
@dlt.expect("valid_policy_number", "policy_number IS NOT NULL AND policy_number RLIKE '^BSE-'")
@dlt.expect_or_drop("valid_loss_cause", f"loss_cause IN {VALID_LOSS_CAUSES}")
@dlt.expect("valid_report_channel", f"report_channel IN {VALID_CHANNELS}")
def bronze_gw_cc_claim():
    return (
        _read_landing_stream("landing_gw_cc_claim")
        .withColumn("total_incurred", F.col("total_incurred").cast("decimal(12,2)"))
        .withColumn("loss_date", F.col("loss_date").cast("date"))
        .withColumn("report_date", F.col("report_date").cast("date"))
        .withColumn("cda_batch_ts", F.col("cda_batch_ts").cast("timestamp"))
        .withColumn("_bronze_ingested_at", F.current_timestamp())
    )


# --------------------------------------------------------------------------
# Bronze: Guidewire ClaimCenter — exposure / incident / contact
# --------------------------------------------------------------------------
@dlt.table(
    name="bronze_gw_cc_exposure",
    comment="Governed bronze Guidewire CC exposure (reserve / paid amounts typed).",
    table_properties={"quality": "bronze", "layer": "bronze"},
)
@dlt.expect("non_negative_amounts", "reserve_amount >= 0 AND paid_amount >= 0")
def bronze_gw_cc_exposure():
    return (
        _read_landing_stream("landing_gw_cc_exposure")
        .withColumn("reserve_amount", F.col("reserve_amount").cast("decimal(12,2)"))
        .withColumn("paid_amount", F.col("paid_amount").cast("decimal(12,2)"))
        .withColumn("_bronze_ingested_at", F.current_timestamp())
    )


@dlt.table(
    name="bronze_gw_cc_incident",
    comment="Governed bronze Guidewire CC incident detail.",
    table_properties={"quality": "bronze", "layer": "bronze"},
)
def bronze_gw_cc_incident():
    return (
        _read_landing_stream("landing_gw_cc_incident")
        .withColumn("_bronze_ingested_at", F.current_timestamp())
    )


@dlt.table(
    name="bronze_gw_cc_contact",
    comment="Governed bronze Guidewire CC contact.",
    table_properties={"quality": "bronze", "layer": "bronze"},
)
def bronze_gw_cc_contact():
    return (
        _read_landing_stream("landing_gw_cc_contact")
        .withColumn("_bronze_ingested_at", F.current_timestamp())
    )


# --------------------------------------------------------------------------
# Bronze: Guidewire PolicyCenter — policy
# --------------------------------------------------------------------------
@dlt.table(
    name="bronze_gw_pc_policy",
    comment="Governed bronze Guidewire PC policy (amounts / dates typed).",
    table_properties={"quality": "bronze", "layer": "bronze"},
)
@dlt.expect("valid_policy_dates", "expiry_date > effective_date")
def bronze_gw_pc_policy():
    return (
        _read_landing_stream("landing_gw_pc_policy")
        .withColumn("sum_insured", F.col("sum_insured").cast("decimal(12,2)"))
        .withColumn("annual_premium", F.col("annual_premium").cast("decimal(12,2)"))
        .withColumn("effective_date", F.col("effective_date").cast("date"))
        .withColumn("expiry_date", F.col("expiry_date").cast("date"))
        .withColumn("_bronze_ingested_at", F.current_timestamp())
    )


# --------------------------------------------------------------------------
# Bronze: enrichment feeds — fraud signals (range-gated) + weather
# --------------------------------------------------------------------------
@dlt.table(
    name="bronze_fraud_signals_raw",
    comment="Governed bronze fraud signals; out-of-range scores dropped & quarantined.",
    table_properties={"quality": "bronze", "layer": "bronze"},
)
@dlt.expect_or_drop("fraud_score_range", "fraud_score BETWEEN 0 AND 100")
def bronze_fraud_signals_raw():
    return (
        _read_landing_stream("landing_fraud_signals")
        .withColumn("fraud_score", F.col("fraud_score").cast("int"))
        .withColumn("prior_claims_12m", F.col("prior_claims_12m").cast("int"))
        .withColumn("_bronze_ingested_at", F.current_timestamp())
    )


@dlt.table(
    name="bronze_weather_raw",
    comment="Governed bronze weather risk feed (per postcode district).",
    table_properties={"quality": "bronze", "layer": "bronze"},
)
def bronze_weather_raw():
    return (
        _read_landing_stream("landing_weather")
        .withColumn("_bronze_ingested_at", F.current_timestamp())
    )


@dlt.table(
    name="bronze_telematics",
    comment="Governed bronze telematics (motor only) — Smart Claims `telematic` entity, "
            "extended with posted_speed_limit + harsh_braking. Feeds rule R6 (speed-vs-limit).",
    table_properties={"quality": "bronze", "layer": "bronze"},
)
@dlt.expect("non_negative_speed", "speed_at_incident >= 0 AND posted_speed_limit > 0")
def bronze_telematics():
    return (
        _read_landing_stream("landing_telematics")
        .withColumn("_bronze_ingested_at", F.current_timestamp())
    )


# --------------------------------------------------------------------------
# Quarantine — the rows the expect_or_drop rules removed, captured from LANDING
# so they are inspectable ("no claims data lost — quarantined, not silently
# dropped"). Read directly from landing, never from bronze output.
# --------------------------------------------------------------------------
@dlt.table(
    name="bronze_quarantine_claims",
    comment="Claims dropped by the bronze valid_loss_cause expectation.",
    table_properties={"layer": "bronze"},
)
def bronze_quarantine_claims():
    return (
        spark.read.table(_landing("landing_gw_cc_claim"))
        .filter(f"loss_cause IS NULL OR loss_cause NOT IN {VALID_LOSS_CAUSES}")
        .withColumn("quarantine_reason", F.lit("invalid_loss_cause"))
        .withColumn("_quarantined_at", F.current_timestamp())
    )


@dlt.table(
    name="bronze_quarantine_fraud_signals",
    comment="Fraud signals dropped by the bronze fraud_score_range expectation.",
    table_properties={"layer": "bronze"},
)
def bronze_quarantine_fraud_signals():
    return (
        spark.read.table(_landing("landing_fraud_signals"))
        .filter("fraud_score IS NULL OR fraud_score < 0 OR fraud_score > 100")
        .withColumn("quarantine_reason", F.lit("fraud_score_out_of_range"))
        .withColumn("_quarantined_at", F.current_timestamp())
    )
