# Databricks notebook source
# MAGIC %md
# MAGIC # Oakbrook Collections — Train & Register ML Models
# MAGIC
# MAGIC This notebook demonstrates the full MLflow workflow Ed's team asked about:
# MAGIC 1. **Feature Tables** — Read customer data from Unity Catalog
# MAGIC 2. **MLflow Experiment Tracking** — Train propensity-to-pay and best-time-to-contact models
# MAGIC 3. **MLflow Model Registry** — Register champion models with versioning
# MAGIC 4. **Model Serving** — Deploy to real-time endpoints (next step)
# MAGIC
# MAGIC > "Our collections team are interested in learning about feature tables, MLflow and model serving
# MAGIC > and how they could use it to develop, test and deploy new collections models
# MAGIC > (e.g. propensity to pay, best time of day to contact etc) and strategies." — Ed Ball

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read from Feature Tables (Unity Catalog)

# COMMAND ----------

CATALOG = "main"
SCHEMA = "oakbrook_collections"

# Read the Customer 360 Feature Table
df_customers = spark.table(f"{CATALOG}.{SCHEMA}.customer_360")
display(df_customers)

# COMMAND ----------

# Read payment history
df_payments = spark.table(f"{CATALOG}.{SCHEMA}.payment_history")
display(df_payments)

# COMMAND ----------

# Read Open Banking data
df_ob = spark.table(f"{CATALOG}.{SCHEMA}.open_banking_data")
display(df_ob)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Feature Engineering
# MAGIC
# MAGIC In production, this would use Databricks Feature Engineering with ~1,000 features.
# MAGIC For this demo, we compute features from the existing tables.

# COMMAND ----------

from pyspark.sql import functions as F

# Join customers with payment stats
df_payment_stats = df_payments.groupBy("customer_id").agg(
    F.count("*").alias("total_payments"),
    F.sum(F.when(F.col("status") == "Paid", 1).otherwise(0)).alias("paid_count"),
    F.sum(F.when(F.col("status").isin("Missed", "Failed"), 1).otherwise(0)).alias("missed_count"),
    F.sum("amount").alias("total_paid_amount"),
    F.max("date").alias("last_payment_date"),
)

# Join with Open Banking
df_features = (
    df_customers
    .join(df_payment_stats, "customer_id", "left")
    .join(df_ob.select("customer_id", "available_for_repayment", "income_stability", "gambling_transactions_30d"), "customer_id", "left")
)

display(df_features)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Train Propensity-to-Pay Model with MLflow

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report
import pandas as pd
import numpy as np

# Set experiment
mlflow.set_experiment(f"/Users/tingting.wan@databricks.com/oakbrook-collections-brain")

# Convert to pandas for sklearn
pdf = df_features.toPandas()

# Create target variable (simplified: high propensity if employed + kept promises > broken)
pdf["target_ptp"] = (
    (pdf["employment_status"].str.startswith("Employed").astype(int)) +
    (pdf["payment_promises_kept"] > pdf["payment_promises_broken"]).astype(int) +
    (pdf["direct_debit_active"].astype(int)) +
    (pdf["open_banking_connected"].astype(int))
) >= 2
pdf["target_ptp"] = pdf["target_ptp"].astype(int)

# Feature columns
feature_cols = [
    "credit_limit", "outstanding_balance", "months_in_arrears", "days_past_due",
    "monthly_income", "contact_attempts_30d", "payment_promises_broken",
    "payment_promises_kept", "age",
]

# Fill NaN
for col in feature_cols:
    pdf[col] = pd.to_numeric(pdf[col], errors="coerce").fillna(0)

X = pdf[feature_cols].values
y = pdf["target_ptp"].values

# COMMAND ----------

# Train with MLflow tracking
with mlflow.start_run(run_name="propensity_to_pay_v3.2.1") as run:
    # Log parameters
    mlflow.log_param("model_type", "GradientBoosting")
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 3)
    mlflow.log_param("feature_count", len(feature_cols))
    mlflow.log_param("features", str(feature_cols))
    mlflow.log_param("training_data", f"{CATALOG}.{SCHEMA}.customer_360")

    # Train model
    model = GradientBoostingClassifier(n_estimators=100, max_depth=3, random_state=42)
    model.fit(X, y)

    # Log metrics
    y_pred = model.predict(X)
    y_proba = model.predict_proba(X)[:, 1]

    mlflow.log_metric("accuracy", accuracy_score(y, y_pred))
    mlflow.log_metric("roc_auc", roc_auc_score(y, y_proba) if len(set(y)) > 1 else 0.0)
    mlflow.log_metric("training_samples", len(y))
    mlflow.log_metric("positive_rate", y.mean())

    # Log feature importances
    for feat, imp in zip(feature_cols, model.feature_importances_):
        mlflow.log_metric(f"importance_{feat}", imp)

    # Log model (without UC registration for speed — register separately)
    mlflow.sklearn.log_model(
        model,
        artifact_path="model",
        input_example=pd.DataFrame([X[0]], columns=feature_cols),
    )

    ptp_run_id = run.info.run_id
    print(f"PTP Model logged — Run ID: {ptp_run_id}")
    print(f"Accuracy: {accuracy_score(y, y_pred):.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Train Best-Time-to-Contact Model

# COMMAND ----------

# Simplified BTC model — predicts best contact window based on employment pattern
# In production: trained on historical contact success data

with mlflow.start_run(run_name="best_time_to_contact_v2.1.0") as run:
    mlflow.log_param("model_type", "GradientBoosting")
    mlflow.log_param("n_estimators", 50)
    mlflow.log_param("max_depth", 2)
    mlflow.log_param("training_data", f"{CATALOG}.{SCHEMA}.customer_360")
    mlflow.log_param("target", "optimal_contact_window")

    # Encode employment to contact window (simplified)
    employment_map = {
        "Employed - Full Time": 0,
        "Employed - Part Time": 1,
        "Self Employed": 2,
        "Unemployed": 3,
    }
    pdf["employment_encoded"] = pdf["employment_status"].map(employment_map).fillna(3)

    X_btc = pdf[["employment_encoded", "contact_attempts_30d", "age", "days_past_due"]].values
    y_btc = pdf["employment_encoded"].values  # Simplified target

    model_btc = GradientBoostingClassifier(n_estimators=50, max_depth=2, random_state=42)
    model_btc.fit(X_btc, y_btc)

    y_pred_btc = model_btc.predict(X_btc)
    mlflow.log_metric("accuracy", accuracy_score(y_btc, y_pred_btc))
    mlflow.log_metric("training_samples", len(y_btc))

    mlflow.sklearn.log_model(
        model_btc,
        artifact_path="model",
        input_example=pd.DataFrame([X_btc[0]], columns=["employment_encoded", "contact_attempts_30d", "age", "days_past_due"]),
    )

    btc_run_id = run.info.run_id
    print(f"BTC Model logged — Run ID: {btc_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Register Models in Unity Catalog
# MAGIC
# MAGIC This registers the trained models in the UC Model Registry.
# MAGIC From the Registry you can then deploy to Model Serving endpoints.

# COMMAND ----------

import mlflow

# Register PTP model
ptp_model_uri = f"runs:/{ptp_run_id}/model"
result = mlflow.register_model(ptp_model_uri, f"{CATALOG}.{SCHEMA}.propensity_to_pay")
print(f"✅ Registered propensity_to_pay: version {result.version}")

# Register BTC model
btc_model_uri = f"runs:/{btc_run_id}/model"
result = mlflow.register_model(btc_model_uri, f"{CATALOG}.{SCHEMA}.best_time_to_contact")
print(f"✅ Registered best_time_to_contact: version {result.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Models in Registry

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()

# List registered models
for rm in client.search_registered_models(filter_string=f"name LIKE '{CATALOG}.{SCHEMA}%'"):
    print(f"\nModel: {rm.name}")
    for v in rm.latest_versions:
        print(f"  Version {v.version}: status={v.status}, run_id={v.run_id[:8]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Register Feature Table
# MAGIC
# MAGIC This registers the customer_360 table as a proper Feature Table in Unity Catalog,
# MAGIC enabling automatic feature lookup during model serving.

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

try:
    fe.register_table(
        delta_table=f"{CATALOG}.{SCHEMA}.customer_360",
        primary_keys=["customer_id"],
        description="Oakbrook Collections Customer 360 — unified profile for AI collections agent. Sources: IceNet/Oracle (loan data), Zendesk (contacts), ClearScore (Open Banking).",
    )
    print(f"✅ Registered {CATALOG}.{SCHEMA}.customer_360 as Feature Table")
except Exception as e:
    print(f"Feature Table registration: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Log Agent Traces with MLflow 3.0
# MAGIC
# MAGIC This demonstrates MLflow Tracing — the same observability Langfuse provides,
# MAGIC but native on Databricks. Every agent decision is traced.

# COMMAND ----------

import mlflow

# Simulate an agent trace
with mlflow.start_span(name="collections_brain_agent", span_type="AGENT") as agent_span:
    agent_span.set_inputs({"customer_id": "C-10001", "query": "Full analysis with strategy recommendation"})

    # Simulate tool calls as child spans
    with mlflow.start_span(name="lookup_customer", span_type="TOOL") as span:
        span.set_inputs({"customer_id": "C-10001"})
        span.set_outputs({"name": "James Whitfield", "days_past_due": 67, "source": "Feature Table"})

    with mlflow.start_span(name="score_propensity_to_pay", span_type="LLM") as span:
        span.set_inputs({"customer_id": "C-10001", "model": "ptp-v3.2.1"})
        span.set_outputs({"score": 0.80, "band": "High"})

    with mlflow.start_span(name="score_best_time_to_contact", span_type="LLM") as span:
        span.set_inputs({"customer_id": "C-10001", "model": "btc-v2.1.0"})
        span.set_outputs({"day": "Wednesday", "time": "10:00-11:30", "channel": "SMS"})

    with mlflow.start_span(name="get_scorecard_segment", span_type="TOOL") as span:
        span.set_inputs({"customer_id": "C-10001", "propensity_band": "High"})
        span.set_outputs({"segment": "Late Arrears", "strategy": "Specialist Referral"})

    with mlflow.start_span(name="assess_vulnerability", span_type="TOOL") as span:
        span.set_inputs({"customer_id": "C-10001"})
        span.set_outputs({"risk_level": "Low", "fca_action_required": False})

    with mlflow.start_span(name="generate_communication", span_type="TOOL") as span:
        span.set_inputs({"customer_id": "C-10001", "tone": "friendly"})
        span.set_outputs({"channel": "SMS", "message_length": 180, "fca_compliant": True})

    agent_span.set_outputs({
        "strategy": "Specialist Referral — contact Wednesday 10:00 via SMS",
        "payment_plan": "£130/month for 22 months",
        "compliance_flags": ["Consumer Duty: outcome in customer best interest"],
        "model_versions": {"ptp": "v3.2.1", "btc": "v2.1.0"},
    })

print("✅ Agent trace logged to MLflow — check the Traces tab in the experiment UI")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You now have:
# MAGIC 1. ✅ **Feature Tables** in Unity Catalog (`main.oakbrook_collections.customer_360`)
# MAGIC 2. ✅ **MLflow Experiment** with training runs and metrics
# MAGIC 3. ✅ **Registered Models** in Model Registry (`propensity_to_pay` v1, `best_time_to_contact` v1)
# MAGIC 4. ✅ **Agent Traces** visible in the MLflow Traces tab
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **Model Serving:** Deploy registered models to real-time endpoints
# MAGIC - **Feature Table Lookup:** Configure auto-lookup so Model Serving pulls features at inference time
# MAGIC - **Agent Framework:** Wire the agent to call Model Serving endpoints instead of local scoring
# MAGIC - **A/B Testing:** Compare model versions via Model Serving traffic splitting
