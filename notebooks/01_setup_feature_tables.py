# Databricks notebook source
# MAGIC %md
# MAGIC # Oakbrook Collections Brain — Feature Table Setup
# MAGIC
# MAGIC This notebook creates the Unity Catalog Feature Tables used by the Collections Brain agent.
# MAGIC Run this once to set up the data layer.

# COMMAND ----------

CATALOG = "main"
SCHEMA = "oakbrook_collections"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Customer 360 Feature Table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, ArrayType, DateType

customers_data = [
    ("C-10001", "James Whitfield", "Credit Card", 3500.0, 2847.32, 2, 67, "Medium", 2200.0, "Employed - Part Time", "SMS", 4, "2026-02-28", "No Answer", 1, 2, True, "Very Poor", False, "2026-01-15", "Nottingham", 34, "2024-03-15"),
    ("C-10002", "Sarah Mitchell", "Debt Consolidation Loan", 8000.0, 6234.50, 1, 34, "Low", 3100.0, "Employed - Full Time", "WhatsApp", 2, "2026-03-10", "Promise to Pay", 0, 1, True, "Poor", True, None, "Derby", 42, "2023-11-01"),
    ("C-10003", "Michael Torres", "Credit Card", 2000.0, 1987.44, 4, 126, "High", 1600.0, "Unemployed", "Email", 8, "2026-03-12", "Refused to Engage", 3, 0, False, "Very Poor", False, "2025-11-20", "Leicester", 28, "2024-08-22"),
    ("C-10004", "Emma Richardson", "Debt Consolidation Loan", 12000.0, 4521.80, 1, 18, "Low", 3800.0, "Employed - Full Time", "Phone", 1, "2026-03-14", "Engaged - Discussing Options", 0, 3, True, "Fair", True, None, "Birmingham", 51, "2022-06-10"),
    ("C-10005", "David Okonkwo", "Credit Card", 5000.0, 4890.15, 3, 91, "High", 1900.0, "Self Employed", "SMS", 6, "2026-03-08", "Partial Payment Made", 2, 1, False, "Very Poor", False, "2025-12-05", "Coventry", 37, "2024-01-20"),
]

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType()),
    StructField("product", StringType()),
    StructField("credit_limit", DoubleType()),
    StructField("outstanding_balance", DoubleType()),
    StructField("months_in_arrears", IntegerType()),
    StructField("days_past_due", IntegerType()),
    StructField("risk_segment", StringType()),
    StructField("monthly_income", DoubleType()),
    StructField("employment_status", StringType()),
    StructField("preferred_channel", StringType()),
    StructField("contact_attempts_30d", IntegerType()),
    StructField("last_contact_date", StringType()),
    StructField("last_contact_outcome", StringType()),
    StructField("payment_promises_broken", IntegerType()),
    StructField("payment_promises_kept", IntegerType()),
    StructField("open_banking_connected", BooleanType()),
    StructField("clearscore_band", StringType()),
    StructField("direct_debit_active", BooleanType()),
    StructField("direct_debit_cancelled_date", StringType()),
    StructField("location", StringType()),
    StructField("age", IntegerType()),
    StructField("account_open_date", StringType()),
])

df_customers = spark.createDataFrame(customers_data, customers_schema)
df_customers.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.customer_360")

print(f"Created {CATALOG}.{SCHEMA}.customer_360 with {df_customers.count()} rows")

# COMMAND ----------

# Register as Feature Table by adding primary key constraint
# This is the correct approach — ALTER TABLE with NOT NULL + PRIMARY KEY
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.customer_360 ALTER COLUMN customer_id SET NOT NULL")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.customer_360 ADD CONSTRAINT customer_360_pk PRIMARY KEY(customer_id)")
spark.sql(f"COMMENT ON TABLE {CATALOG}.{SCHEMA}.customer_360 IS 'Oakbrook Collections Customer 360 — unified customer profile for collections AI agent. Sources: IceNet/Oracle, Zendesk, ClearScore.'")
print(f"✅ Registered {CATALOG}.{SCHEMA}.customer_360 as Feature Table with PRIMARY KEY(customer_id)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Payment History Table

# COMMAND ----------

payment_data = [
    ("C-10001", "2025-12-01", 120.0, "Paid", "Direct Debit"),
    ("C-10001", "2026-01-01", 120.0, "Failed", "Direct Debit"),
    ("C-10001", "2026-01-15", 0.0, "DD Cancelled", "Direct Debit"),
    ("C-10001", "2026-02-01", 0.0, "Missed", "N/A"),
    ("C-10001", "2026-03-01", 0.0, "Missed", "N/A"),
    ("C-10002", "2025-12-15", 250.0, "Paid", "Bank Transfer"),
    ("C-10002", "2026-01-15", 250.0, "Paid", "Bank Transfer"),
    ("C-10002", "2026-02-15", 0.0, "Missed", "N/A"),
    ("C-10002", "2026-03-10", 0.0, "Promise to Pay (25 Mar)", "N/A"),
    ("C-10003", "2025-11-01", 80.0, "Paid", "Card"),
    ("C-10003", "2025-12-01", 0.0, "Missed", "N/A"),
    ("C-10003", "2026-01-01", 0.0, "Missed", "N/A"),
    ("C-10003", "2026-02-01", 0.0, "Missed", "N/A"),
    ("C-10003", "2026-03-01", 0.0, "Missed", "N/A"),
    ("C-10004", "2025-12-15", 400.0, "Paid", "Direct Debit"),
    ("C-10004", "2026-01-15", 400.0, "Paid", "Direct Debit"),
    ("C-10004", "2026-02-15", 400.0, "Paid", "Direct Debit"),
    ("C-10004", "2026-03-01", 0.0, "Late - Pending", "N/A"),
    ("C-10005", "2025-12-01", 150.0, "Paid", "Card"),
    ("C-10005", "2026-01-01", 0.0, "Missed", "N/A"),
    ("C-10005", "2026-02-01", 0.0, "Missed", "N/A"),
    ("C-10005", "2026-03-08", 50.0, "Partial Payment", "Card"),
]

payment_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("date", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("method", StringType()),
])

df_payments = spark.createDataFrame(payment_data, payment_schema)
df_payments.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.payment_history")

print(f"Created {CATALOG}.{SCHEMA}.payment_history with {df_payments.count()} rows")

# COMMAND ----------

# Register payment_history with primary key
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.payment_history ALTER COLUMN customer_id SET NOT NULL")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.payment_history ALTER COLUMN date SET NOT NULL")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.payment_history ADD CONSTRAINT payment_history_pk PRIMARY KEY(customer_id, date)")
print(f"✅ Registered {CATALOG}.{SCHEMA}.payment_history with PRIMARY KEY(customer_id, date)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Open Banking Data Table

# COMMAND ----------

ob_data = [
    ("C-10001", "ClearScore", "2026-03-14", 2180.0, 1450.0, 380.0, 350.0, 220.0, 130.0, 0, "Stable"),
    ("C-10002", "ClearScore", "2026-03-12", 3050.0, 1800.0, 520.0, 730.0, 180.0, 550.0, 0, "Stable"),
    ("C-10004", "ClearScore", "2026-03-15", 3780.0, 2100.0, 650.0, 1030.0, 350.0, 680.0, 0, "Stable"),
]

ob_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("provider", StringType()),
    StructField("last_sync", StringType()),
    StructField("monthly_income_verified", DoubleType()),
    StructField("monthly_essential_expenses", DoubleType()),
    StructField("monthly_discretionary_spend", DoubleType()),
    StructField("disposable_income", DoubleType()),
    StructField("other_credit_commitments", DoubleType()),
    StructField("available_for_repayment", DoubleType()),
    StructField("gambling_transactions_30d", IntegerType()),
    StructField("income_stability", StringType()),
])

df_ob = spark.createDataFrame(ob_data, ob_schema)
df_ob.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.open_banking_data")

print(f"Created {CATALOG}.{SCHEMA}.open_banking_data with {df_ob.count()} rows")

# COMMAND ----------

# Register open_banking_data with primary key
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.open_banking_data ALTER COLUMN customer_id SET NOT NULL")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.open_banking_data ADD CONSTRAINT open_banking_pk PRIMARY KEY(customer_id)")
print(f"✅ Registered {CATALOG}.{SCHEMA}.open_banking_data with PRIMARY KEY(customer_id)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Approval Queue Table (UC-backed governance layer)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.approval_queue (
    id STRING,
    customer_id STRING,
    customer_name STRING,
    channel STRING,
    tone STRING,
    message STRING,
    strategy_summary STRING,
    status STRING,
    submitted_at STRING,
    submitted_by STRING,
    reviewed_by STRING,
    reviewed_at STRING
) USING DELTA
""")

print(f"Created {CATALOG}.{SCHEMA}.approval_queue")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify all tables

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.customer_360"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.payment_history ORDER BY customer_id, date"))
