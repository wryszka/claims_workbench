# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Intelligence Workbench — 05 · ML Models
# MAGIC
# MAGIC **Bricksurance SE** — Phase 5. Two models trained via the **Feature Store**,
# MAGIC logged to MLflow, registered in **Unity Catalog**, and (in the deploy step)
# MAGIC served on **Mosaic AI Model Serving**.
# MAGIC
# MAGIC > **About this demo.** Synthetic data only — fictional company, policies and
# MAGIC > figures. No real Guidewire integration, no real customer data.
# MAGIC
# MAGIC - **Model A** — FNOL Triage Classifier (LightGBM, pay_direct/escalate/refer_siu)
# MAGIC - **Model B** — Reserve Bracket Classifier (XGBoost, low/medium/high/large_loss)
# MAGIC
# MAGIC **Training** uses the Feature Store (`fe.create_training_set` assembles the
# MAGIC label + feature join, recording lineage). Labels come from `silver_claims_enriched`.
# MAGIC
# MAGIC **Serving design — feature-vector contract, NO online store.** Real-time
# MAGIC `fe.log_model` serving-by-key requires an online feature store (Mosaic AI
# MAGIC auto-setup failed on this workspace, and the spec excludes an online/Lakebase
# MAGIC path). So the models are logged as plain `mlflow.sklearn` models that accept
# MAGIC the **feature vector** directly. The app/agent (Phase 6/8) does the cheap
# MAGIC by-`claim_public_id` lookup against the OFFLINE UC feature table, then calls
# MAGIC the endpoint with the resulting features. (On a workspace with online tables
# MAGIC enabled, swap `mlflow.sklearn.log_model` → `fe.log_model` for true by-key serving.)

# COMMAND ----------

# MAGIC %pip install lightgbm xgboost shap databricks-feature-engineering scikit-learn matplotlib

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys
import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import mlflow
import mlflow.lightgbm
import mlflow.xgboost
from mlflow.tracking import MlflowClient
from mlflow.models import infer_signature
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, confusion_matrix, ConfusionMatrixDisplay
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

mlflow.set_registry_uri("databricks-uc")

dbutils.widgets.text("catalog", "", "Catalog (blank = workspace current)")
dbutils.widgets.text("schema", "claims_workbench", "Schema (fixed)")
catalog = dbutils.widgets.get("catalog").strip() or spark.catalog.currentCatalog()
schema = dbutils.widgets.get("schema").strip() or "claims_workbench"
print(f"[target] {catalog}.{schema}")

def tbl(name):
    return f"{catalog}.{schema}.{name}"

fe = FeatureEngineeringClient()
client = MlflowClient(registry_uri="databricks-uc")
mlflow.set_experiment(f"/Users/{spark.sql('select current_user()').collect()[0][0]}/claims_workbench_05_ml")

TMP = "/tmp/cw_artifacts"
os.makedirs(TMP, exist_ok=True)

# Plain-English labels for plots
TRIAGE_LABELS = {
    "peril_type_encoded": "Peril type", "report_channel_encoded": "Report channel",
    "reported_amount_log": "Reported amount (log)", "sum_insured_to_reported_ratio": "Sum insured / reported",
    "fraud_score": "Fraud score", "prior_claims_12m": "Prior claims (12m)",
    "reporting_lag_days": "Reporting lag (days)", "policy_tenure_years": "Policy tenure (years)",
    "weather_risk_composite": "Weather risk", "is_high_value": "High value (>GBP 10k)",
    "at_fault": "At fault", "third_party_involved": "Third party involved",
    "postcode_flood_risk": "Postcode flood risk",
}
BRACKET_ORDER = ["low", "medium", "high", "large_loss"]
BRACKET_DISPLAY = {"low": "LOW", "medium": "MEDIUM", "high": "HIGH", "large_loss": "LARGE LOSS"}


def latest_version(name):
    vs = client.search_model_versions(f"name='{name}'")
    return max(int(v.version) for v in vs)


RESULTS = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model A — FNOL Triage Classifier (LightGBM)

# COMMAND ----------

from lightgbm import LGBMClassifier

silver = spark.table(tbl("silver_claims_enriched"))
labels_triage = silver.select("claim_public_id", "triage_decision")

ts_triage = fe.create_training_set(
    df=labels_triage,
    feature_lookups=[FeatureLookup(table_name=tbl("feature_triage"), lookup_key="claim_public_id")],
    label="triage_decision",
    exclude_columns=[],
)
pdf = ts_triage.load_df().toPandas()
ids = pdf["claim_public_id"]
y = pdf["triage_decision"]
# astype(float): UC decimal features arrive as Python Decimal (object) in pandas.
X = pdf.drop(columns=["claim_public_id", "triage_decision"]).astype(float)
feat_cols = list(X.columns)

X_tr, X_te, y_tr, y_te, id_tr, id_te = train_test_split(
    X, y, ids, test_size=0.2, random_state=42, stratify=y)

triage_model = LGBMClassifier(
    objective="multiclass", n_estimators=300, learning_rate=0.05,
    num_leaves=48, subsample=0.8, colsample_bytree=0.8, random_state=42, n_jobs=-1)
triage_model.fit(X_tr, y_tr)

pred = triage_model.predict(X_te)
acc = accuracy_score(y_te, pred)
mf1 = f1_score(y_te, pred, average="macro")
print(f"[Triage] accuracy={acc:.4f} macro-F1={mf1:.4f}")
RESULTS["triage_accuracy"] = round(float(acc), 4)
RESULTS["triage_macro_f1"] = round(float(mf1), 4)

# Confusion matrix artifact
classes = sorted(y.unique())
cm = confusion_matrix(y_te, pred, labels=classes)
fig, ax = plt.subplots(figsize=(6, 5))
ConfusionMatrixDisplay(cm, display_labels=classes).plot(ax=ax, cmap="Blues", colorbar=False)
ax.set_title("FNOL Triage — Confusion Matrix")
plt.tight_layout(); cm_path = f"{TMP}/triage_confusion.png"; plt.savefig(cm_path); plt.close()

# Feature importance artifact (plain-English labels)
imp = pd.Series(triage_model.feature_importances_, index=feat_cols).sort_values(ascending=True)
fig, ax = plt.subplots(figsize=(7, 6))
ax.barh([TRIAGE_LABELS.get(c, c) for c in imp.index], imp.values, color="#1e293b")
ax.set_xlabel("Feature importance (gain)"); ax.set_title("FNOL Triage — Feature Importance")
plt.tight_layout(); fi_path = f"{TMP}/triage_importance.png"; plt.savefig(fi_path); plt.close()

top5 = imp.sort_values(ascending=False).head(5)
RESULTS["triage_top5"] = {TRIAGE_LABELS.get(k, k): int(v) for k, v in top5.items()}
print("[Triage] top-5 feature importance:")
for k, v in top5.items():
    print(f"  {TRIAGE_LABELS.get(k, k):<26} {int(v)}")

with mlflow.start_run(run_name="triage_classifier") as run:
    mlflow.log_params({"algo": "lightgbm", "n_estimators": 300, "learning_rate": 0.05, "num_leaves": 48})
    mlflow.log_metrics({"accuracy": acc, "macro_f1": mf1})
    mlflow.log_artifact(cm_path); mlflow.log_artifact(fi_path)
    # Feature-vector contract (see serving note). Use the lightgbm flavor so the
    # serving env captures the `lightgbm` dependency (sklearn flavor misses it).
    sig_t = infer_signature(X_tr.head(50), triage_model.predict(X_tr.head(50)))
    mlflow.lightgbm.log_model(
        triage_model, artifact_path="model", signature=sig_t,
        input_example=X_tr.head(3),
        registered_model_name=tbl("model_triage_classifier"))

triage_version = latest_version(tbl("model_triage_classifier"))
client.set_registered_model_alias(tbl("model_triage_classifier"), "champion", triage_version)
RESULTS["triage_version"] = triage_version
print(f"[Triage] registered {tbl('model_triage_classifier')} v{triage_version} @champion")

# vivid claim confidence (from predict_proba — endpoints return the class only)
vivid_row = X[ids == "cc:900001"]
if len(vivid_row):
    proba = triage_model.predict_proba(vivid_row)[0]
    cls = triage_model.classes_
    vi = int(np.argmax(proba))
    RESULTS["triage_vivid"] = {"prediction": str(cls[vi]), "confidence_pct": round(float(proba[vi]) * 100, 1)}
    print(f"[Triage] vivid cc:900001 -> {cls[vi]} ({proba[vi]*100:.1f}% confidence)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model B — Reserve Bracket Classifier (XGBoost)
# MAGIC Trained on **closed/settled** claims only (ultimate_reserve known → valid label).

# COMMAND ----------

from xgboost import XGBClassifier
import shap

labels_reserve = (
    silver.where("claim_status IN ('settled','declined','withdrawn')")
    .select("claim_public_id", "reserve_bracket")
)
print(f"[Reserve] training on closed claims: {labels_reserve.count():,}")

ts_reserve = fe.create_training_set(
    df=labels_reserve,
    feature_lookups=[FeatureLookup(table_name=tbl("feature_reserve"), lookup_key="claim_public_id")],
    label="reserve_bracket",
    exclude_columns=[],
)
pdf_r = ts_reserve.load_df().toPandas()
ids_r = pdf_r["claim_public_id"]
b2i = {b: i for i, b in enumerate(BRACKET_ORDER)}
i2b = {i: b for b, i in b2i.items()}
y_r = pdf_r["reserve_bracket"].map(b2i)
X_r = pdf_r.drop(columns=["claim_public_id", "reserve_bracket"]).astype(float)
feat_cols_r = list(X_r.columns)

Xr_tr, Xr_te, yr_tr, yr_te, idr_tr, idr_te = train_test_split(
    X_r, y_r, ids_r, test_size=0.2, random_state=42, stratify=y_r)

reserve_model = XGBClassifier(
    objective="multi:softprob", num_class=len(BRACKET_ORDER), n_estimators=300,
    max_depth=6, learning_rate=0.08, subsample=0.8, colsample_bytree=0.8,
    random_state=42, n_jobs=-1, eval_metric="mlogloss")
reserve_model.fit(Xr_tr, yr_tr)

pred_r = reserve_model.predict(Xr_te)
acc_r = accuracy_score(yr_te, pred_r)
mf1_r = f1_score(yr_te, pred_r, average="macro")
print(f"[Reserve] accuracy={acc_r:.4f} macro-F1={mf1_r:.4f}")
RESULTS["reserve_accuracy"] = round(float(acc_r), 4)
RESULTS["reserve_macro_f1"] = round(float(mf1_r), 4)

# Confusion matrix artifact (plain-English bracket labels)
disp_labels = [BRACKET_DISPLAY[b] for b in BRACKET_ORDER]
cm_r = confusion_matrix(yr_te, pred_r, labels=list(range(len(BRACKET_ORDER))))
fig, ax = plt.subplots(figsize=(6, 5))
ConfusionMatrixDisplay(cm_r, display_labels=disp_labels).plot(ax=ax, cmap="Greens", colorbar=False)
ax.set_title("Reserve Bracket — Confusion Matrix"); plt.xticks(rotation=30)
plt.tight_layout(); cmr_path = f"{TMP}/reserve_confusion.png"; plt.savefig(cmr_path); plt.close()

RESERVE_LABELS = {
    "peril_type_encoded": "Peril type", "handler_grade_encoded": "Handler grade",
    "reported_amount_log": "Reported amount (log)", "fraud_score": "Fraud score",
    "prior_claims_12m": "Prior claims (12m)", "weather_risk_composite": "Weather risk",
    "days_open": "Days open", "triage_decision_encoded": "Triage decision",
    "sum_insured_log": "Sum insured (log)",
}

# SHAP summary artifact (best-effort). shap.TreeExplainer can be incompatible
# with multiclass XGBoost depending on installed versions (vector base_score) —
# fall back to a plain-English gain-importance plot so the model still registers.
shap_path = f"{TMP}/reserve_explainability.png"
try:
    samp = Xr_te.sample(min(2000, len(Xr_te)), random_state=42)
    explainer = shap.TreeExplainer(reserve_model)
    shap_values = explainer.shap_values(samp)
    plt.figure()
    shap.summary_plot(shap_values, samp, show=False, plot_size=(8, 6))
    plt.tight_layout(); plt.savefig(shap_path); plt.close()
    print("[Reserve] SHAP summary plot saved.")
except Exception as e:  # noqa: BLE001
    print(f"[Reserve] SHAP unavailable ({type(e).__name__}); using gain-importance fallback.")
    impr = pd.Series(reserve_model.feature_importances_, index=feat_cols_r).sort_values(ascending=True)
    fig, ax = plt.subplots(figsize=(7, 6))
    ax.barh([RESERVE_LABELS.get(c, c) for c in impr.index], impr.values, color="#047857")
    ax.set_xlabel("Feature importance (gain)"); ax.set_title("Reserve Bracket — Feature Importance")
    plt.tight_layout(); plt.savefig(shap_path); plt.close()

with mlflow.start_run(run_name="reserve_bracket") as run:
    mlflow.log_params({"algo": "xgboost", "n_estimators": 300, "max_depth": 6, "learning_rate": 0.08})
    mlflow.log_metrics({"accuracy": acc_r, "macro_f1": mf1_r})
    mlflow.log_artifact(cmr_path); mlflow.log_artifact(shap_path)
    sig_r = infer_signature(Xr_tr.head(50), reserve_model.predict(Xr_tr.head(50)))
    mlflow.xgboost.log_model(
        reserve_model, artifact_path="model", signature=sig_r,
        input_example=Xr_tr.head(3),
        registered_model_name=tbl("model_reserve_bracket"))

reserve_version = latest_version(tbl("model_reserve_bracket"))
client.set_registered_model_alias(tbl("model_reserve_bracket"), "champion", reserve_version)
RESULTS["reserve_version"] = reserve_version
print(f"[Reserve] registered {tbl('model_reserve_bracket')} v{reserve_version} @champion")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business story — model catches handler under-reserving on escape-of-water

# COMMAND ----------

# Compare the handler's INITIAL reserve bracket (from initial_reserve) to the
# model's predicted bracket, on home_escape_water test claims.
def bracket_of(col):
    return F.when(col < 2000, "low").when(col < 10000, "medium").when(col < 50000, "high").otherwise("large_loss")

from pyspark.sql import functions as F
silver_pdf = (
    silver.select("claim_public_id", "peril_type", "initial_reserve",
                  bracket_of(F.col("initial_reserve")).alias("initial_bracket"))
    .toPandas().set_index("claim_public_id")
)

test_pred_brackets = pd.Series([i2b[int(p)] for p in pred_r], index=idr_te.values)
rank = {b: i for i, b in enumerate(BRACKET_ORDER)}
comp = pd.DataFrame({"pred_bracket": test_pred_brackets})
comp = comp.join(silver_pdf, how="left")
eow = comp[comp["peril_type"] == "home_escape_water"].dropna(subset=["initial_bracket"])
upward = (eow["pred_bracket"].map(rank) > eow["initial_bracket"].map(rank)).mean() * 100
RESULTS["eow_reclassified_upward_pct"] = round(float(upward), 1)
print(f"[Reserve] EoW test claims: {len(eow):,}")
print(f">>> HEADLINE: the model reclassifies {upward:.1f}% of home_escape_water claims "
      f"UPWARD vs the handler's initial reserve bracket (catches systematic under-reserving).")

# Save the EoW reclassification comparison as an artifact
eow_summary = (
    eow.assign(initial=eow["initial_bracket"].map(BRACKET_DISPLAY),
               predicted=eow["pred_bracket"].map(BRACKET_DISPLAY))
    .groupby(["initial", "predicted"]).size().reset_index(name="claims")
)
eow_csv = f"{TMP}/eow_reserve_reclassification.csv"
eow_summary.to_csv(eow_csv, index=False)
with mlflow.start_run(run_name="reserve_business_story"):
    mlflow.log_metric("eow_reclassified_upward_pct", float(upward))
    mlflow.log_artifact(eow_csv)

# vivid claim reserve prediction confidence
vivid_r = X_r[ids_r == "cc:900001"]
if len(vivid_r):
    pr = reserve_model.predict_proba(vivid_r)[0]
    vi = int(np.argmax(pr))
    RESULTS["reserve_vivid"] = {"prediction": i2b[vi], "confidence_pct": round(float(pr[vi]) * 100, 1)}
    print(f"[Reserve] vivid cc:900001 -> {BRACKET_DISPLAY[i2b[vi]]} ({pr[vi]*100:.1f}% confidence)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary (for the deploy/serving step)

# COMMAND ----------

print(json.dumps(RESULTS, indent=2))
# Surface model versions for the serving endpoint resource.
dbutils.notebook.exit(json.dumps(RESULTS))
