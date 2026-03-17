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
import random

random.seed(42)

# ---------- Original 5 customers (unchanged) ----------
customers_data = [
    ("C-10001", "James Whitfield", "Credit Card", 3500.0, 2847.32, 2, 67, "Medium", 2200.0, "Employed - Part Time", "SMS", 4, "2026-02-28", "No Answer", 1, 2, True, "Very Poor", False, "2026-01-15", "Nottingham", 34, "2024-03-15"),
    ("C-10002", "Sarah Mitchell", "Debt Consolidation Loan", 8000.0, 6234.50, 1, 34, "Low", 3100.0, "Employed - Full Time", "WhatsApp", 2, "2026-03-10", "Promise to Pay", 0, 1, True, "Poor", True, None, "Derby", 42, "2023-11-01"),
    ("C-10003", "Michael Torres", "Credit Card", 2000.0, 1987.44, 4, 126, "High", 1600.0, "Unemployed", "Email", 8, "2026-03-12", "Refused to Engage", 3, 0, False, "Very Poor", False, "2025-11-20", "Leicester", 28, "2024-08-22"),
    ("C-10004", "Emma Richardson", "Debt Consolidation Loan", 12000.0, 4521.80, 1, 18, "Low", 3800.0, "Employed - Full Time", "Phone", 1, "2026-03-14", "Engaged - Discussing Options", 0, 3, True, "Fair", True, None, "Birmingham", 51, "2022-06-10"),
    ("C-10005", "David Okonkwo", "Credit Card", 5000.0, 4890.15, 3, 91, "High", 1900.0, "Self Employed", "SMS", 6, "2026-03-08", "Partial Payment Made", 2, 1, False, "Very Poor", False, "2025-12-05", "Coventry", 37, "2024-01-20"),
]

# ---------- Generate 95 additional synthetic customers (C-10006 to C-10100) ----------
first_names_m = ["Oliver", "George", "Harry", "Jack", "Jacob", "Noah", "Charlie", "Thomas", "Oscar", "William",
                 "James", "Muhammad", "Henry", "Alfie", "Leo", "Freddie", "Archie", "Ethan", "Isaac", "Alexander",
                 "Joseph", "Edward", "Samuel", "Max", "Logan", "Lucas", "Daniel", "Theo", "Arthur", "Reuben",
                 "Mohammed", "Harvey", "Liam", "Adam", "Dylan", "Toby", "Jayden", "Connor", "Kai", "Riley",
                 "Nathan", "Ben", "Aaron", "Ryan", "Tyler", "Callum", "Luke", "Bradley"]
first_names_f = ["Olivia", "Amelia", "Isla", "Emily", "Ava", "Jessica", "Ella", "Mia", "Grace", "Sophia",
                 "Lily", "Chloe", "Freya", "Isabella", "Poppy", "Daisy", "Florence", "Evie", "Rosie", "Millie",
                 "Charlotte", "Ruby", "Alice", "Sienna", "Phoebe", "Harper", "Willow", "Eva", "Layla", "Amber",
                 "Imogen", "Hannah", "Lucy", "Ellie", "Maisie", "Holly", "Molly", "Georgia", "Zara", "Bethany",
                 "Jasmine", "Scarlett", "Katie", "Paige", "Abigail", "Lauren", "Harriet"]
surnames = ["Smith", "Jones", "Taylor", "Brown", "Williams", "Wilson", "Johnson", "Davies", "Patel", "Robinson",
            "Wright", "Thompson", "Evans", "Walker", "White", "Roberts", "Green", "Hall", "Wood", "Harris",
            "Martin", "Jackson", "Clarke", "Lewis", "Young", "Allen", "King", "Anderson", "Scott", "Baker",
            "Morris", "Mitchell", "Campbell", "Carter", "Phillips", "Collins", "Stewart", "Murphy", "Parker",
            "Cook", "Murray", "Ward", "Russell", "Hughes", "Bell", "Kelly", "Palmer", "Fox", "Chapman",
            "Marshall", "Gray", "Mason", "Price", "Bennett", "Griffiths", "Henderson", "Coleman", "Brooks",
            "Howard", "Peters", "Reed", "Simpson", "Butler", "Barnes", "Fisher", "Grant", "Shaw", "Hunt",
            "Dixon", "Gordon", "Rose", "Stone", "Harvey", "Mills", "Webb", "Ellis", "Cross", "Tucker",
            "Freeman", "Reynolds", "Knight", "Graham", "Newton", "Pearson", "Walsh", "Spencer", "Hart",
            "Ali", "Singh", "Hussain", "Begum", "Kaur", "Rahman", "Sharma"]

locations = ["Nottingham", "Derby", "Leicester", "Birmingham", "Coventry", "Sheffield", "Leeds", "Manchester"]
products = ["Credit Card", "Debt Consolidation Loan"]
employment_statuses = ["Employed - Full Time", "Employed - Part Time", "Self Employed", "Unemployed"]
employment_weights = [40, 25, 20, 15]
channels = ["WhatsApp", "SMS", "Email", "Phone"]
outcomes = ["Paid", "Promise to Pay", "No Answer", "Refused to Engage", "Partial Payment Made", "Engaged - Discussing Options"]
outcome_weights = [10, 25, 25, 15, 15, 10]
clearscore_bands = ["Very Poor", "Poor", "Fair", "Good"]

def generate_customer(cid_num):
    cid = f"C-{cid_num}"
    is_female = random.random() < 0.48
    first = random.choice(first_names_f if is_female else first_names_m)
    last = random.choice(surnames)
    name = f"{first} {last}"

    product = random.choices(products, weights=[60, 40], k=1)[0]
    credit_limit = round(random.randint(10, 150) * 100.0, 2)  # 1000-15000 in steps of 100
    balance_pct = random.uniform(0.40, 0.98)
    outstanding = round(credit_limit * balance_pct, 2)

    # months_in_arrears: weighted towards 1-3
    mia = random.choices([0, 1, 2, 3, 4, 5, 6], weights=[10, 20, 25, 20, 12, 8, 5], k=1)[0]
    dpd = mia * 30 + random.randint(-5, 5) if mia > 0 else 0
    dpd = max(0, min(dpd, 180))

    if mia <= 1:
        risk = "Low"
    elif mia <= 3:
        risk = "Medium"
    else:
        risk = "High"

    income = round(random.randint(120, 500) * 10.0, 2)  # 1200-5000
    employment = random.choices(employment_statuses, weights=employment_weights, k=1)[0]
    channel = random.choice(channels)
    contact_attempts = random.randint(0, 12)

    # last contact date in Feb-Mar 2026
    month = random.choice([2, 3])
    day = random.randint(1, 28 if month == 2 else 15)
    last_contact_date = f"2026-{month:02d}-{day:02d}"

    outcome = random.choices(outcomes, weights=outcome_weights, k=1)[0]

    # promises: high risk -> more broken
    if risk == "High":
        broken = random.randint(1, 4)
        kept = random.randint(0, 1)
    elif risk == "Medium":
        broken = random.randint(0, 2)
        kept = random.randint(0, 3)
    else:
        broken = random.randint(0, 1)
        kept = random.randint(1, 4)

    ob_connected = random.random() < 0.50
    cs_band = random.choices(clearscore_bands, weights=[25, 35, 25, 15], k=1)[0]
    dd_active = random.random() < 0.40

    if dd_active:
        dd_cancelled = None
    else:
        # some have cancellation dates
        if random.random() < 0.6:
            cm = random.choice([10, 11, 12, 1])
            cy = 2025 if cm >= 10 else 2026
            dd_cancelled = f"{cy}-{cm:02d}-{random.randint(1,28):02d}"
        else:
            dd_cancelled = None

    location = random.choice(locations)
    age = random.randint(22, 65)

    # account open date: 2020-2025
    ao_year = random.randint(2020, 2025)
    ao_month = random.randint(1, 12)
    ao_day = random.randint(1, 28)
    account_open = f"{ao_year}-{ao_month:02d}-{ao_day:02d}"

    return (cid, name, product, credit_limit, outstanding, mia, dpd, risk,
            income, employment, channel, contact_attempts, last_contact_date,
            outcome, broken, kept, ob_connected, cs_band, dd_active,
            dd_cancelled, location, age, account_open)

for i in range(10006, 10101):
    customers_data.append(generate_customer(i))

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

# ---------- Original 5 customers' payment data (unchanged) ----------
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

# ---------- Generate ~300 additional payment history rows for synthetic customers ----------
random.seed(42)
payment_statuses = ["Paid", "Missed", "Failed", "Partial Payment", "Late - Pending"]
payment_methods = ["Direct Debit", "Bank Transfer", "Card", "N/A"]
months_range = [
    ("2025-09-01", "2025-10-01", "2025-11-01", "2025-12-01", "2026-01-01", "2026-02-01", "2026-03-01"),
]

for cid_num in range(10006, 10101):
    cid = f"C-{cid_num}"
    # Find the customer's data to tailor payment history
    cust = [c for c in customers_data if c[0] == cid][0]
    risk = cust[7]
    mia = cust[5]
    credit_limit = cust[3]

    # Generate 2-5 payment records per customer
    num_records = random.randint(2, 5)
    base_payment = round(credit_limit * random.uniform(0.02, 0.08), 2)

    # Create dates going back from March 2026
    all_dates = ["2025-09-01", "2025-10-01", "2025-11-01", "2025-12-01", "2026-01-01", "2026-02-01", "2026-03-01"]
    # Pick the most recent num_records dates
    selected_dates = all_dates[-num_records:]

    for date in selected_dates:
        # Determine status based on risk and recency
        if risk == "Low":
            status_weights = [60, 15, 5, 10, 10]
        elif risk == "Medium":
            status_weights = [30, 30, 10, 15, 15]
        else:
            status_weights = [10, 45, 15, 15, 15]

        status = random.choices(payment_statuses, weights=status_weights, k=1)[0]

        if status == "Paid":
            amount = base_payment
            method = random.choice(["Direct Debit", "Bank Transfer", "Card"])
        elif status == "Partial Payment":
            amount = round(base_payment * random.uniform(0.2, 0.7), 2)
            method = random.choice(["Bank Transfer", "Card"])
        else:
            amount = 0.0
            method = "N/A"

        payment_data.append((cid, date, amount, status, method))

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

# ---------- Original 3 open banking records (unchanged) ----------
ob_data = [
    ("C-10001", "ClearScore", "2026-03-14", 2180.0, 1450.0, 380.0, 350.0, 220.0, 130.0, 0, "Stable"),
    ("C-10002", "ClearScore", "2026-03-12", 3050.0, 1800.0, 520.0, 730.0, 180.0, 550.0, 0, "Stable"),
    ("C-10004", "ClearScore", "2026-03-15", 3780.0, 2100.0, 650.0, 1030.0, 350.0, 680.0, 0, "Stable"),
]

# ---------- Generate ~50 open banking records for connected synthetic customers ----------
random.seed(42)
ob_customers = [c for c in customers_data if c[0] not in ("C-10001", "C-10002", "C-10003", "C-10004", "C-10005") and c[16] is True]
# Take up to 50
ob_customers = ob_customers[:50]

income_stability_options = ["Stable", "Variable", "Declining"]
income_stability_weights = [50, 35, 15]

for cust in ob_customers:
    cid = cust[0]
    income = cust[8]

    verified_income = round(income * random.uniform(0.90, 1.10), 2)
    essentials = round(verified_income * random.uniform(0.35, 0.55), 2)
    discretionary = round(verified_income * random.uniform(0.10, 0.25), 2)
    disposable = round(verified_income - essentials - discretionary, 2)
    other_credit = round(random.uniform(50, 500), 2)
    available = round(max(0, disposable - other_credit), 2)
    gambling = random.choices([0, 0, 0, 0, 0, 1, 2, 3, 5], k=1)[0]
    stability = random.choices(income_stability_options, weights=income_stability_weights, k=1)[0]

    sync_day = random.randint(1, 15)
    last_sync = f"2026-03-{sync_day:02d}"

    ob_data.append((cid, "ClearScore", last_sync, verified_income, essentials,
                    discretionary, disposable, other_credit, available,
                    gambling, stability))

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
# MAGIC ## 5. Evaluation Dataset (Held-Out Customers)
# MAGIC
# MAGIC 30 held-out customers (C-10101 to C-10130) with ground-truth labels for model evaluation.
# MAGIC These are NOT in the training data.

# COMMAND ----------

random.seed(99)  # different seed for eval data

eval_data = []
for cid_num in range(10101, 10131):
    cust = generate_customer(cid_num)
    # Generate label: 1 = paid, 0 = didn't pay
    # Logic: Low risk + employed + kept promises -> more likely to pay
    risk = cust[7]
    employment = cust[9]
    kept = cust[15]
    broken = cust[14]
    dd_active = cust[18]

    pay_score = 0
    if risk == "Low":
        pay_score += 3
    elif risk == "Medium":
        pay_score += 1
    if "Full Time" in employment:
        pay_score += 2
    elif "Part Time" in employment:
        pay_score += 1
    if kept > broken:
        pay_score += 2
    if dd_active:
        pay_score += 1

    # Probabilistic label based on score
    pay_prob = min(0.95, max(0.05, pay_score / 9.0))
    label = 1 if random.random() < pay_prob else 0

    eval_data.append(cust + (label,))

eval_schema = StructType([
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
    StructField("label", IntegerType()),
])

df_eval = spark.createDataFrame(eval_data, eval_schema)
df_eval.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.evaluation_dataset")

print(f"Created {CATALOG}.{SCHEMA}.evaluation_dataset with {df_eval.count()} rows")

# COMMAND ----------

# Register evaluation_dataset with primary key
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.evaluation_dataset ALTER COLUMN customer_id SET NOT NULL")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.evaluation_dataset ADD CONSTRAINT eval_dataset_pk PRIMARY KEY(customer_id)")
spark.sql(f"COMMENT ON TABLE {CATALOG}.{SCHEMA}.evaluation_dataset IS 'Held-out evaluation dataset with ground-truth labels. 30 customers (C-10101 to C-10130) not in training data. label: 1=paid, 0=did not pay.'")
print(f"✅ Registered {CATALOG}.{SCHEMA}.evaluation_dataset with PRIMARY KEY(customer_id)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify all tables

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.customer_360"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.payment_history ORDER BY customer_id, date"))
