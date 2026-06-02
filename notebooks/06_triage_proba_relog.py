# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 06 · Triage model: expose probabilities
# MAGIC
# MAGIC **Phase 6 prerequisite.** The Phase 5 triage endpoint serves the predicted
# MAGIC class only. The Phase 6 `fn_triage_claim` tool needs a **confidence %**, so
# MAGIC re-log the existing triage model to return class **probabilities** (no
# MAGIC retrain — loads the current @champion and re-logs with the sklearn pyfunc
# MAGIC `predict_proba`). A new version is registered and @champion moves to it; the
# MAGIC triage serving endpoint is then redeployed to this version.

# COMMAND ----------

# MAGIC %pip install lightgbm scikit-learn mlflow

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import mlflow
import mlflow.sklearn
import mlflow.lightgbm
from mlflow.tracking import MlflowClient
from mlflow.models import infer_signature

mlflow.set_registry_uri("databricks-uc")

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema (fixed)")
catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
name = f"{catalog}.{schema}.model_triage_classifier"
user = spark.sql("select current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{user}/claims_workbench_05_ml")

# Load the current champion (logged with the lightgbm flavor) → LGBMClassifier.
model = mlflow.lightgbm.load_model(f"models:/{name}@champion")

# Feature vector schema (drop the PK). Cast to float (UC decimals -> object).
X = spark.table(f"{catalog}.{schema}.feature_triage").drop("claim_public_id").limit(200).toPandas().astype(float)
proba = model.predict_proba(X.head(50))
sig = infer_signature(X.head(50), proba)
print("classes_ order:", list(model.classes_))

with mlflow.start_run(run_name="triage_proba"):
    # pyfunc_predict_fn='predict_proba' makes the served endpoint return the
    # per-class probability array; extra_pip_requirements ensures lightgbm loads.
    mlflow.sklearn.log_model(
        model, artifact_path="model",
        pyfunc_predict_fn="predict_proba",
        extra_pip_requirements=["lightgbm"],
        signature=sig, input_example=X.head(3),
        registered_model_name=name)

client = MlflowClient(registry_uri="databricks-uc")
version = max(int(v.version) for v in client.search_model_versions(f"name='{name}'"))
client.set_registered_model_alias(name, "champion", version)
print(f"triage proba model registered: {name} v{version} @champion")
dbutils.notebook.exit(str(version))
