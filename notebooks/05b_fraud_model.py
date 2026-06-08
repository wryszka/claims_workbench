# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 05b · Fraud detection model
# MAGIC
# MAGIC A genuine **fraud propensity model**: gradient-boosted classifier that predicts
# MAGIC `is_potential_fraud` from a claim's **behavioural features** (prior-claims velocity,
# MAGIC reporting lag, amount/ratio, peril, channel, tenure, third-party) — **excluding the
# MAGIC raw upstream fraud score**, so the model learns fraud risk from claim context rather
# MAGIC than re-thresholding the signal it's given. Logged to MLflow, registered in Unity
# MAGIC Catalog as `model_fraud_detection` @champion, with a model card the app surfaces.

# COMMAND ----------

# MAGIC %pip install lightgbm scikit-learn mlflow --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, json
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mlflow, mlflow.lightgbm
from mlflow.tracking import MlflowClient
from mlflow.models import infer_signature
from lightgbm import LGBMClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, precision_score, recall_score

mlflow.set_registry_uri("databricks-uc")
dbutils.widgets.text("catalog", "", "Catalog (blank = current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema")
catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
def tbl(n): return f"{catalog}.{schema}.{n}"
name = tbl("model_fraud_detection")
user = spark.sql("select current_user()").collect()[0][0]
exp = f"/Users/{user}/claims_workbench_05_ml"
mlflow.set_experiment(exp)
client = MlflowClient(registry_uri="databricks-uc")
TMP = "/tmp/cw_fraud"; os.makedirs(TMP, exist_ok=True)

LABELS = {
    "prior_claims_12m": "Prior claims (12m)", "reporting_lag_days": "Reporting lag (days)",
    "reported_amount_log": "Reported amount (log)", "sum_insured_to_reported_ratio": "Sum insured / reported",
    "peril_type_encoded": "Peril type", "report_channel_encoded": "Report channel",
    "policy_tenure_years": "Policy tenure (years)", "weather_risk_composite": "Weather risk",
    "is_high_value": "High value", "at_fault": "At fault", "third_party_involved": "Third party involved",
    "postcode_flood_risk": "Postcode flood risk",
}

# COMMAND ----------

# Features = behavioural columns from feature_triage; label from silver. Drop the raw
# fraud_score (the upstream signal) so the model is not just re-learning its threshold.
from pyspark.sql import functions as F
feat = spark.table(tbl("feature_triage")).drop("fraud_score")
lab = spark.table(tbl("silver_claims_enriched")).select(
    "claim_public_id", F.col("is_potential_fraud").cast("int").alias("y"))
pdf = feat.join(lab, "claim_public_id").toPandas()
y = pdf["y"].astype(int)
X = pdf.drop(columns=["claim_public_id", "y"]).astype(float)
feat_cols = list(X.columns)
base_rate = float(y.mean())
print(f"rows={len(X):,} features={len(feat_cols)} base_rate={base_rate:.4f}")

X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
model = LGBMClassifier(objective="binary", n_estimators=400, learning_rate=0.05,
                       num_leaves=48, subsample=0.8, colsample_bytree=0.8,
                       class_weight="balanced", random_state=42, n_jobs=-1)
model.fit(X_tr, y_tr)
proba = model.predict_proba(X_te)[:, 1]
pred = (proba >= 0.5).astype(int)
auc = float(roc_auc_score(y_te, proba))
prec = float(precision_score(y_te, pred, zero_division=0))
rec = float(recall_score(y_te, pred, zero_division=0))
print(f"AUC={auc:.4f} precision={prec:.3f} recall={rec:.3f}")

imp = pd.Series(model.feature_importances_, index=feat_cols).sort_values(ascending=True)
fig, ax = plt.subplots(figsize=(7, 5))
ax.barh([LABELS.get(c, c) for c in imp.index], imp.values, color="#1d4ed8")
ax.set_title("Fraud detection — feature importance (gain)"); plt.tight_layout()
fi_path = f"{TMP}/fraud_importance.png"; plt.savefig(fi_path); plt.close()
top = imp.sort_values(ascending=False).head(5)
top_features = [LABELS.get(k, k) for k in top.index]

# COMMAND ----------

with mlflow.start_run(run_name="fraud_detection") as run:
    mlflow.log_params({"algo": "lightgbm", "n_estimators": 400, "learning_rate": 0.05,
                       "objective": "binary", "class_weight": "balanced",
                       "excludes": "raw fraud_score (behavioural model)"})
    mlflow.log_metrics({"roc_auc": auc, "precision": prec, "recall": rec, "base_rate": base_rate})
    mlflow.log_artifact(fi_path)
    sig = infer_signature(X_tr.head(50), model.predict_proba(X_tr.head(50))[:, 1])
    mlflow.lightgbm.log_model(model, artifact_path="model", signature=sig,
                              input_example=X_tr.head(3), registered_model_name=name)
    run_id = run.info.run_id; exp_id = run.info.experiment_id

version = max(int(v.version) for v in client.search_model_versions(f"name='{name}'"))
client.set_registered_model_alias(name, "champion", version)
print(f"registered {name} v{version} @champion (run {run_id})")

# Model card the app reads (Fraud & SIU → Models tab).
spark.sql(f"DROP TABLE IF EXISTS {tbl('gold_fraud_model_card')}")
card = spark.createDataFrame(
    [(name, str(version), round(auc, 4), round(prec, 4), round(rec, 4), round(base_rate, 4),
      json.dumps(top_features), run_id, exp_id)],
    "model_name string, model_version string, auc double, precision_at double, recall_at double, "
    "base_rate double, top_features string, run_id string, experiment_id string")
card.write.mode("overwrite").saveAsTable(tbl("gold_fraud_model_card"))
dbutils.notebook.exit(json.dumps({"version": version, "auc": round(auc, 4), "run_id": run_id}))
