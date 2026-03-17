# Databricks notebook source
# MAGIC %md
# MAGIC # Oakbrook Collections — Deploy to Model Serving
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Registers trained models from MLflow to Unity Catalog Model Registry
# MAGIC 2. Creates Model Serving endpoints for real-time inference
# MAGIC 3. Tests the endpoints with sample data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Register Models in Unity Catalog

# COMMAND ----------

import mlflow
from mlflow import MlflowClient

CATALOG = "main"
SCHEMA = "oakbrook_collections"

client = MlflowClient()

# Find the latest PTP and BTC runs
experiment = mlflow.get_experiment_by_name(
    "/Users/tingting.wan@databricks.com/oakbrook-collections-brain/02_train_and_register_models"
)

if experiment is None:
    # Try shared experiment
    experiment = mlflow.get_experiment_by_name("/Shared/oakbrook-collections-brain")

print(f"Using experiment: {experiment.name} (ID: {experiment.experiment_id})")

runs = client.search_runs(
    experiment_ids=[experiment.experiment_id],
    order_by=["start_time DESC"],
    max_results=10,
)

ptp_run = None
btc_run = None
for r in runs:
    if "propensity" in r.info.run_name.lower():
        ptp_run = r
    elif "best_time" in r.info.run_name.lower() or "btc" in r.info.run_name.lower():
        btc_run = r

print(f"PTP Run: {ptp_run.info.run_id if ptp_run else 'NOT FOUND'}")
print(f"BTC Run: {btc_run.info.run_id if btc_run else 'NOT FOUND'}")

# COMMAND ----------

# Register PTP model
if ptp_run:
    ptp_model_uri = f"runs:/{ptp_run.info.run_id}/model"
    try:
        result = mlflow.register_model(ptp_model_uri, f"{CATALOG}.{SCHEMA}.propensity_to_pay")
        print(f"✅ Registered propensity_to_pay v{result.version}")
    except Exception as e:
        print(f"PTP registration: {e}")
        # Try to get existing
        try:
            mv = client.get_latest_versions(f"{CATALOG}.{SCHEMA}.propensity_to_pay")
            print(f"Already registered: v{mv[0].version}")
        except:
            pass

# COMMAND ----------

# Register BTC model
if btc_run:
    btc_model_uri = f"runs:/{btc_run.info.run_id}/model"
    try:
        result = mlflow.register_model(btc_model_uri, f"{CATALOG}.{SCHEMA}.best_time_to_contact")
        print(f"✅ Registered best_time_to_contact v{result.version}")
    except Exception as e:
        print(f"BTC registration: {e}")
        try:
            mv = client.get_latest_versions(f"{CATALOG}.{SCHEMA}.best_time_to_contact")
            print(f"Already registered: v{mv[0].version}")
        except:
            pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Verify Models in Registry

# COMMAND ----------

for model_name in [f"{CATALOG}.{SCHEMA}.propensity_to_pay", f"{CATALOG}.{SCHEMA}.best_time_to_contact"]:
    try:
        versions = client.get_latest_versions(model_name)
        for v in versions:
            print(f"✅ {model_name} v{v.version}: status={v.status}, run_id={v.run_id[:12]}...")
    except Exception as e:
        print(f"❌ {model_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Model Serving Endpoints
# MAGIC
# MAGIC Deploy the registered models to real-time serving endpoints.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

# Create PTP serving endpoint
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

try:
    endpoint = w.serving_endpoints.create_and_wait(
        name="oakbrook-ptp-model",
        config=EndpointCoreConfigInput(
            served_entities=[
                ServedEntityInput(
                    entity_name=f"{CATALOG}.{SCHEMA}.propensity_to_pay",
                    entity_version="1",
                    workload_size="Small",
                    scale_to_zero_enabled=True,
                )
            ]
        ),
    )
    print(f"✅ PTP endpoint created: {endpoint.name} ({endpoint.state})")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"PTP endpoint already exists")
    else:
        print(f"PTP endpoint: {e}")

# COMMAND ----------

# Create BTC serving endpoint
try:
    endpoint = w.serving_endpoints.create_and_wait(
        name="oakbrook-btc-model",
        config=EndpointCoreConfigInput(
            served_entities=[
                ServedEntityInput(
                    entity_name=f"{CATALOG}.{SCHEMA}.best_time_to_contact",
                    entity_version="1",
                    workload_size="Small",
                    scale_to_zero_enabled=True,
                )
            ]
        ),
    )
    print(f"✅ BTC endpoint created: {endpoint.name} ({endpoint.state})")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"BTC endpoint already exists")
    else:
        print(f"BTC endpoint: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test the Serving Endpoints

# COMMAND ----------

import requests
import json

# Test PTP endpoint
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = spark.conf.get("spark.databricks.workspaceUrl")

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Test data - James Whitfield features
test_data = {
    "dataframe_records": [{
        "credit_limit": 3500,
        "outstanding_balance": 2847.32,
        "months_in_arrears": 2,
        "days_past_due": 67,
        "monthly_income": 2200,
        "contact_attempts_30d": 4,
        "payment_promises_broken": 1,
        "payment_promises_kept": 2,
        "age": 34,
    }]
}

try:
    response = requests.post(
        f"https://{host}/serving-endpoints/oakbrook-ptp-model/invocations",
        headers=headers,
        json=test_data,
    )
    print(f"PTP Response ({response.status_code}):")
    print(json.dumps(response.json(), indent=2))
except Exception as e:
    print(f"PTP test: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: List All Serving Endpoints

# COMMAND ----------

endpoints = w.serving_endpoints.list()
for ep in endpoints:
    if "oakbrook" in ep.name.lower():
        print(f"  {ep.name}: {ep.state.ready if ep.state else '?'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You now have:
# MAGIC 1. ✅ **Models registered** in Unity Catalog Model Registry
# MAGIC 2. ✅ **Serving endpoints** deployed for real-time inference
# MAGIC 3. ✅ **Tested** with sample customer data
# MAGIC
# MAGIC The Collections Brain app can now call these endpoints instead of local scoring.
# MAGIC
# MAGIC ### End-to-End Flow
# MAGIC ```
# MAGIC Feature Tables (UC) → Training (MLflow) → Registry (UC) → Serving (Endpoints) → Agent (App)
# MAGIC ```
