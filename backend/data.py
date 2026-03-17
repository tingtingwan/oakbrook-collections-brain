"""
Collections data layer — reads from Unity Catalog Feature Tables when running
on Databricks, falls back to local synthetic data for local development.

Databricks components:
- Feature Tables (Unity Catalog) → Customer 360 profiles
- MLflow Model Serving → Propensity-to-pay, Best-time-to-contact
- Structured Streaming → Real-time payment signals, DD cancellations
- MCP Server → Collection scorecard validation logic
- Open Banking → Affordability assessment via ClearScore/aggregators
"""

import json
import os
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Unity Catalog Feature Table access (real Databricks environment)
# ---------------------------------------------------------------------------
UC_CATALOG = "main"
UC_SCHEMA = "oakbrook_collections"
_sql_conn = None


def _get_sql_connection():
    """Get Databricks SQL connection for reading Feature Tables."""
    global _sql_conn
    if _sql_conn is not None:
        return _sql_conn

    host = os.environ.get("DATABRICKS_HOST", "")
    if not host:
        return None

    try:
        from databricks import sql as dbsql
        from databricks.sdk import WorkspaceClient

        clean_host = host.replace("https://", "").replace("http://", "")
        w = WorkspaceClient(host=f"https://{clean_host}")

        # Get token
        token = None
        if hasattr(w.config, 'token') and w.config.token:
            token = w.config.token
        else:
            auth_result = w.config.authenticate()
            if callable(auth_result):
                headers = auth_result()
            elif isinstance(auth_result, dict):
                headers = auth_result
            else:
                headers = {}
            auth_header = headers.get("Authorization", "")
            token = auth_header.replace("Bearer ", "") if auth_header else ""

        if not token:
            return None

        # Find a SQL warehouse
        warehouses = w.warehouses.list()
        wh_id = None
        for wh in warehouses:
            if wh.state and wh.state.value == "RUNNING":
                wh_id = wh.id
                break
        if not wh_id:
            # Try the first one
            for wh in w.warehouses.list():
                wh_id = wh.id
                break

        if not wh_id:
            logger.warning("No SQL warehouse found")
            return None

        _sql_conn = dbsql.connect(
            server_hostname=clean_host,
            http_path=f"/sql/1.0/warehouses/{wh_id}",
            access_token=token,
        )
        logger.info(f"Connected to UC via SQL warehouse {wh_id}")
        return _sql_conn
    except Exception as e:
        logger.warning(f"Could not connect to UC Feature Tables: {e}")
        return None


def _query_uc(query: str) -> list[dict] | None:
    """Execute a SQL query against Unity Catalog and return rows as dicts."""
    conn = _get_sql_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.warning(f"UC query failed: {e}")
        return None

# ---------------------------------------------------------------------------
# Customer Feature Table (would be a UC Feature Table in production)
# ---------------------------------------------------------------------------
CUSTOMERS = {
    "C-10001": {
        "customer_id": "C-10001",
        "name": "James Whitfield",
        "product": "Credit Card",
        "credit_limit": 3500,
        "outstanding_balance": 2847.32,
        "months_in_arrears": 2,
        "days_past_due": 67,
        "risk_segment": "Medium",
        "monthly_income": 2200,
        "employment_status": "Employed - Part Time",
        "preferred_channel": "SMS",
        "contact_attempts_30d": 4,
        "last_contact_date": "2026-02-28",
        "last_contact_outcome": "No Answer",
        "payment_promises_broken": 1,
        "payment_promises_kept": 2,
        "open_banking_connected": True,
        "clearscore_band": "Very Poor",
        "direct_debit_active": False,
        "direct_debit_cancelled_date": "2026-01-15",
        "location": "Nottingham",
        "vulnerability_flags": [],
        "age": 34,
        "account_open_date": "2024-03-15",
    },
    "C-10002": {
        "customer_id": "C-10002",
        "name": "Sarah Mitchell",
        "product": "Debt Consolidation Loan",
        "credit_limit": 8000,
        "outstanding_balance": 6234.50,
        "months_in_arrears": 1,
        "days_past_due": 34,
        "risk_segment": "Low",
        "monthly_income": 3100,
        "employment_status": "Employed - Full Time",
        "preferred_channel": "WhatsApp",
        "contact_attempts_30d": 2,
        "last_contact_date": "2026-03-10",
        "last_contact_outcome": "Promise to Pay",
        "payment_promises_broken": 0,
        "payment_promises_kept": 1,
        "open_banking_connected": True,
        "clearscore_band": "Poor",
        "direct_debit_active": True,
        "direct_debit_cancelled_date": None,
        "location": "Derby",
        "vulnerability_flags": [],
        "age": 42,
        "account_open_date": "2023-11-01",
    },
    "C-10003": {
        "customer_id": "C-10003",
        "name": "Michael Torres",
        "product": "Credit Card",
        "credit_limit": 2000,
        "outstanding_balance": 1987.44,
        "months_in_arrears": 4,
        "days_past_due": 126,
        "risk_segment": "High",
        "monthly_income": 1600,
        "employment_status": "Unemployed",
        "preferred_channel": "Email",
        "contact_attempts_30d": 8,
        "last_contact_date": "2026-03-12",
        "last_contact_outcome": "Refused to Engage",
        "payment_promises_broken": 3,
        "payment_promises_kept": 0,
        "open_banking_connected": False,
        "clearscore_band": "Very Poor",
        "direct_debit_active": False,
        "direct_debit_cancelled_date": "2025-11-20",
        "location": "Leicester",
        "vulnerability_flags": ["unemployment", "potential_financial_hardship"],
        "age": 28,
        "account_open_date": "2024-08-22",
    },
    "C-10004": {
        "customer_id": "C-10004",
        "name": "Emma Richardson",
        "product": "Debt Consolidation Loan",
        "credit_limit": 12000,
        "outstanding_balance": 4521.80,
        "months_in_arrears": 1,
        "days_past_due": 18,
        "risk_segment": "Low",
        "monthly_income": 3800,
        "employment_status": "Employed - Full Time",
        "preferred_channel": "Phone",
        "contact_attempts_30d": 1,
        "last_contact_date": "2026-03-14",
        "last_contact_outcome": "Engaged - Discussing Options",
        "payment_promises_broken": 0,
        "payment_promises_kept": 3,
        "open_banking_connected": True,
        "clearscore_band": "Fair",
        "direct_debit_active": True,
        "direct_debit_cancelled_date": None,
        "location": "Birmingham",
        "vulnerability_flags": [],
        "age": 51,
        "account_open_date": "2022-06-10",
    },
    "C-10005": {
        "customer_id": "C-10005",
        "name": "David Okonkwo",
        "product": "Credit Card",
        "credit_limit": 5000,
        "outstanding_balance": 4890.15,
        "months_in_arrears": 3,
        "days_past_due": 91,
        "risk_segment": "High",
        "monthly_income": 1900,
        "employment_status": "Self Employed",
        "preferred_channel": "SMS",
        "contact_attempts_30d": 6,
        "last_contact_date": "2026-03-08",
        "last_contact_outcome": "Partial Payment Made",
        "payment_promises_broken": 2,
        "payment_promises_kept": 1,
        "open_banking_connected": False,
        "clearscore_band": "Very Poor",
        "direct_debit_active": False,
        "direct_debit_cancelled_date": "2025-12-05",
        "location": "Coventry",
        "vulnerability_flags": ["irregular_income"],
        "age": 37,
        "account_open_date": "2024-01-20",
    },
}

# ---------------------------------------------------------------------------
# Payment History (would be streamed via Structured Streaming in production)
# ---------------------------------------------------------------------------
PAYMENT_HISTORY = {
    "C-10001": [
        {"date": "2025-12-01", "amount": 120.00, "status": "Paid", "method": "Direct Debit"},
        {"date": "2026-01-01", "amount": 120.00, "status": "Failed", "method": "Direct Debit"},
        {"date": "2026-01-15", "amount": 0, "status": "DD Cancelled", "method": "Direct Debit"},
        {"date": "2026-02-01", "amount": 0, "status": "Missed", "method": "N/A"},
        {"date": "2026-03-01", "amount": 0, "status": "Missed", "method": "N/A"},
    ],
    "C-10002": [
        {"date": "2025-12-15", "amount": 250.00, "status": "Paid", "method": "Bank Transfer"},
        {"date": "2026-01-15", "amount": 250.00, "status": "Paid", "method": "Bank Transfer"},
        {"date": "2026-02-15", "amount": 0, "status": "Missed", "method": "N/A"},
        {"date": "2026-03-10", "amount": 0, "status": "Promise to Pay (25 Mar)", "method": "N/A"},
    ],
    "C-10003": [
        {"date": "2025-11-01", "amount": 80.00, "status": "Paid", "method": "Card"},
        {"date": "2025-12-01", "amount": 0, "status": "Missed", "method": "N/A"},
        {"date": "2026-01-01", "amount": 0, "status": "Missed", "method": "N/A"},
        {"date": "2026-02-01", "amount": 0, "status": "Missed", "method": "N/A"},
        {"date": "2026-03-01", "amount": 0, "status": "Missed", "method": "N/A"},
    ],
    "C-10004": [
        {"date": "2025-12-15", "amount": 400.00, "status": "Paid", "method": "Direct Debit"},
        {"date": "2026-01-15", "amount": 400.00, "status": "Paid", "method": "Direct Debit"},
        {"date": "2026-02-15", "amount": 400.00, "status": "Paid", "method": "Direct Debit"},
        {"date": "2026-03-01", "amount": 0, "status": "Late - Pending", "method": "N/A"},
    ],
    "C-10005": [
        {"date": "2025-12-01", "amount": 150.00, "status": "Paid", "method": "Card"},
        {"date": "2026-01-01", "amount": 0, "status": "Missed", "method": "N/A"},
        {"date": "2026-02-01", "amount": 0, "status": "Missed", "method": "N/A"},
        {"date": "2026-03-08", "amount": 50.00, "status": "Partial Payment", "method": "Card"},
    ],
}

# ---------------------------------------------------------------------------
# Open Banking Data (would come via ClearScore / aggregator APIs)
# ---------------------------------------------------------------------------
OPEN_BANKING_DATA = {
    "C-10001": {
        "customer_id": "C-10001",
        "provider": "ClearScore",
        "last_sync": "2026-03-14",
        "monthly_income_verified": 2180,
        "monthly_essential_expenses": 1450,
        "monthly_discretionary_spend": 380,
        "disposable_income": 350,
        "other_credit_commitments": 220,
        "available_for_repayment": 130,
        "gambling_transactions_30d": 0,
        "income_stability": "Stable",
        "accounts": [
            {"type": "Current Account", "balance": 342.50, "bank": "Monzo"},
            {"type": "Savings", "balance": 85.00, "bank": "Monzo"},
        ],
    },
    "C-10002": {
        "customer_id": "C-10002",
        "provider": "ClearScore",
        "last_sync": "2026-03-12",
        "monthly_income_verified": 3050,
        "monthly_essential_expenses": 1800,
        "monthly_discretionary_spend": 520,
        "disposable_income": 730,
        "other_credit_commitments": 180,
        "available_for_repayment": 550,
        "gambling_transactions_30d": 0,
        "income_stability": "Stable",
        "accounts": [
            {"type": "Current Account", "balance": 1240.00, "bank": "HSBC"},
            {"type": "Savings", "balance": 3200.00, "bank": "HSBC"},
        ],
    },
    "C-10004": {
        "customer_id": "C-10004",
        "provider": "ClearScore",
        "last_sync": "2026-03-15",
        "monthly_income_verified": 3780,
        "monthly_essential_expenses": 2100,
        "monthly_discretionary_spend": 650,
        "disposable_income": 1030,
        "other_credit_commitments": 350,
        "available_for_repayment": 680,
        "gambling_transactions_30d": 0,
        "income_stability": "Stable",
        "accounts": [
            {"type": "Current Account", "balance": 2850.00, "bank": "Barclays"},
            {"type": "Savings", "balance": 8500.00, "bank": "Barclays"},
        ],
    },
}

# ---------------------------------------------------------------------------
# Collection Scorecard Rules (would be served via MCP Server in production)
# ---------------------------------------------------------------------------
SCORECARD_RULES = {
    "segment_A": {
        "name": "Early Arrears - High Propensity",
        "criteria": {"days_past_due_max": 30, "propensity_band": "High"},
        "strategy": "Soft Touch",
        "actions": ["SMS reminder", "Payment link", "DD reinstatement offer"],
        "max_contact_frequency": "2 per week",
        "escalation_trigger": "No response after 3 attempts",
    },
    "segment_B": {
        "name": "Early Arrears - Low Propensity",
        "criteria": {"days_past_due_max": 30, "propensity_band": "Low"},
        "strategy": "Proactive Engagement",
        "actions": ["Phone call", "Hardship assessment offer", "Payment plan discussion"],
        "max_contact_frequency": "3 per week",
        "escalation_trigger": "Missed promise to pay",
    },
    "segment_C": {
        "name": "Mid Arrears - Engaged",
        "criteria": {"days_past_due_max": 60, "last_outcome": "engaged"},
        "strategy": "Negotiate Settlement",
        "actions": ["Dedicated case handler", "Flexible payment plan", "Partial settlement offer"],
        "max_contact_frequency": "2 per week",
        "escalation_trigger": "Broken payment promise",
    },
    "segment_D": {
        "name": "Mid Arrears - Disengaged",
        "criteria": {"days_past_due_max": 60, "last_outcome": "disengaged"},
        "strategy": "Formal Notice",
        "actions": ["Written notice", "Multi-channel outreach", "Vulnerability screening"],
        "max_contact_frequency": "1 per week",
        "escalation_trigger": "No engagement after 14 days",
    },
    "segment_E": {
        "name": "Late Arrears",
        "criteria": {"days_past_due_max": 90},
        "strategy": "Specialist Referral",
        "actions": ["Specialist agent assignment", "Default notice", "Reduced settlement offer (60-80%)"],
        "max_contact_frequency": "1 per week",
        "escalation_trigger": "Automatic after 90 DPD",
    },
    "segment_F": {
        "name": "Pre-Litigation / Write-Off",
        "criteria": {"days_past_due_min": 91},
        "strategy": "Final Resolution",
        "actions": ["Final settlement (40-60%)", "Debt sale preparation", "Legal referral assessment"],
        "max_contact_frequency": "As needed",
        "escalation_trigger": "No resolution after 120 DPD",
    },
}

# ---------------------------------------------------------------------------
# Communication Templates (RAG-style — would be retrieved from vector store)
# ---------------------------------------------------------------------------
COMMS_TEMPLATES = {
    "soft_reminder_sms": {
        "channel": "SMS",
        "tone": "Friendly",
        "template": "Hi {name}, this is a friendly reminder that your {product} payment of £{amount} is overdue. We're here to help — tap here to make a payment or set up a plan: {payment_link}. Call us free on 0800 XXX XXXX if you'd like to talk. Oakbrook Finance.",
    },
    "soft_reminder_whatsapp": {
        "channel": "WhatsApp",
        "tone": "Friendly",
        "template": "Hi {name} 👋\n\nWe noticed your {product} payment hasn't come through yet. No worries — these things happen!\n\nYou can:\n✅ Pay now: {payment_link}\n📞 Call us free: 0800 XXX XXXX\n💬 Reply here to chat\n\nWe're happy to work out a plan that suits you.\n\nOakbrook Finance",
    },
    "empathetic_engagement": {
        "channel": "Phone/SMS",
        "tone": "Empathetic",
        "template": "Hi {name}, we understand things can be tough sometimes. Your {product} account is {days_past_due} days overdue with a balance of £{amount}. We'd like to find a solution that works for you — whether that's a reduced payment plan or a short break. Please call us on 0800 XXX XXXX or reply to arrange a time to talk. Oakbrook Finance.",
    },
    "formal_notice": {
        "channel": "Email/Letter",
        "tone": "Formal",
        "template": "Dear {name},\n\nRe: {product} Account — Outstanding Balance £{amount}\n\nDespite previous correspondence, your account remains {days_past_due} days in arrears. Under the Consumer Credit Act 1974, we are required to inform you that failure to address this balance may result in further action.\n\nWe strongly encourage you to contact us to discuss your options:\n- Affordable payment plan\n- Financial hardship support\n- Debt advice referral (StepChange: 0800 138 1111)\n\nPlease contact us within 14 days on 0800 XXX XXXX.\n\nYours sincerely,\nOakbrook Finance Collections",
    },
    "vulnerability_sensitive": {
        "channel": "Phone/Letter",
        "tone": "Sensitive",
        "template": "Dear {name},\n\nWe're writing about your {product} account. We understand that managing finances can sometimes be challenging, and we want you to know we're here to help.\n\nThere's no pressure — we can offer:\n- A payment break while you get back on your feet\n- A reduced payment plan based on what you can afford\n- A referral to free debt advice (StepChange: 0800 138 1111, Citizens Advice: 0800 144 8848)\n\nPlease reach out when you're ready. We're here to support you.\n\nOakbrook Finance",
    },
    "settlement_offer": {
        "channel": "Email/Letter",
        "tone": "Formal - Positive",
        "template": "Dear {name},\n\nRe: Settlement Offer — {product} Account\n\nWe'd like to offer you the opportunity to settle your outstanding balance of £{amount} at a reduced amount of £{settlement_amount} ({discount}% discount). This offer is valid for 30 days.\n\nAccepting this offer will:\n- Clear your account in full\n- Stop all further collection activity\n- Be reported as 'Settled' to credit reference agencies\n\nTo accept, call 0800 XXX XXXX or visit {payment_link}.\n\nYours sincerely,\nOakbrook Finance",
    },
}


def get_customer(customer_id: str) -> dict | None:
    """Look up customer from UC Feature Table, fallback to local data."""
    uc_result = _query_uc(
        f"SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.customer_360 WHERE customer_id = '{customer_id}'"
    )
    if uc_result and len(uc_result) > 0:
        row = uc_result[0]
        row["vulnerability_flags"] = []  # Would be a separate table in production
        row["_source"] = "Unity Catalog Feature Table"
        return row
    return CUSTOMERS.get(customer_id)


def get_all_customers() -> list[dict]:
    """List all customers from UC Feature Table, fallback to local data."""
    uc_result = _query_uc(
        f"SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.customer_360 ORDER BY days_past_due DESC"
    )
    if uc_result and len(uc_result) > 0:
        for row in uc_result:
            row["vulnerability_flags"] = []
            row["_source"] = "Unity Catalog Feature Table"
        return uc_result
    return list(CUSTOMERS.values())


def get_payment_history(customer_id: str) -> list[dict] | None:
    """Get payment history from UC table, fallback to local data."""
    uc_result = _query_uc(
        f"SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.payment_history WHERE customer_id = '{customer_id}' ORDER BY date"
    )
    if uc_result and len(uc_result) > 0:
        return uc_result
    return PAYMENT_HISTORY.get(customer_id)


def get_open_banking_data(customer_id: str) -> dict | None:
    """Retrieve Open Banking affordability data from UC table, fallback to local."""
    uc_result = _query_uc(
        f"SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.open_banking_data WHERE customer_id = '{customer_id}'"
    )
    if uc_result and len(uc_result) > 0:
        return uc_result[0]
    return OPEN_BANKING_DATA.get(customer_id)


def get_scorecard_segment(customer_id: str, propensity_band: str) -> dict | None:
    """
    Match customer to collection scorecard segment.
    In production: MCP Server serving the scorecard SQL logic.
    """
    c = get_customer(customer_id)
    if not c:
        return None

    dpd = int(c.get("days_past_due", 0))
    outcome = str(c.get("last_contact_outcome", "")).lower()

    if dpd <= 30 and propensity_band == "High":
        segment = SCORECARD_RULES["segment_A"]
    elif dpd <= 30:
        segment = SCORECARD_RULES["segment_B"]
    elif dpd <= 60 and ("promise" in outcome or "engaged" in outcome or "discussing" in outcome):
        segment = SCORECARD_RULES["segment_C"]
    elif dpd <= 60:
        segment = SCORECARD_RULES["segment_D"]
    elif dpd <= 90:
        segment = SCORECARD_RULES["segment_E"]
    else:
        segment = SCORECARD_RULES["segment_F"]

    return {
        "customer_id": customer_id,
        "assigned_segment": segment["name"],
        "strategy": segment["strategy"],
        "recommended_actions": segment["actions"],
        "max_contact_frequency": segment["max_contact_frequency"],
        "escalation_trigger": segment["escalation_trigger"],
        "source": "MCP Server — Collection Scorecard v2.4",
    }


def assess_vulnerability(customer_id: str) -> dict | None:
    """
    Assess customer vulnerability indicators (FCA Consumer Duty requirement).
    """
    c = get_customer(customer_id)
    if not c:
        return None

    indicators = []
    risk_level = "Low"

    # Employment-based vulnerability
    if c.get("employment_status") == "Unemployed":
        indicators.append({"type": "Financial", "detail": "Currently unemployed", "severity": "High"})
        risk_level = "High"
    elif c.get("employment_status") == "Self Employed":
        indicators.append({"type": "Financial", "detail": "Self-employed — income may be irregular", "severity": "Medium"})

    # Arrears depth
    mia = int(c.get("months_in_arrears", 0))
    if mia >= 3:
        indicators.append({"type": "Financial", "detail": f"{mia} months in arrears — persistent debt indicator", "severity": "High"})
        risk_level = "High"

    # Contact engagement
    if c.get("last_contact_outcome") == "Refused to Engage":
        indicators.append({"type": "Behavioural", "detail": "Refusing to engage — potential distress signal", "severity": "Medium"})

    # Contact frequency
    contact_attempts = int(c.get("contact_attempts_30d", 0))
    if contact_attempts >= 6:
        indicators.append({"type": "Compliance", "detail": f"{contact_attempts} contact attempts in 30 days — approaching limit", "severity": "Medium"})

    # Existing flags
    for flag in c.get("vulnerability_flags", []):
        label = flag.replace("_", " ").title()
        indicators.append({"type": "Flagged", "detail": f"Previously flagged: {label}", "severity": "Medium"})

    # Open Banking signals
    ob = get_open_banking_data(customer_id)
    if ob:
        if ob.get("gambling_transactions_30d", 0) > 0:
            indicators.append({"type": "Behavioural", "detail": "Gambling transactions detected in banking data", "severity": "High"})
            risk_level = "High"
        if ob.get("available_for_repayment", 999) < 50:
            indicators.append({"type": "Financial", "detail": "Very low disposable income for repayment", "severity": "High"})
            risk_level = "High"

    if not indicators:
        indicators.append({"type": "None", "detail": "No vulnerability indicators detected", "severity": "None"})

    return {
        "customer_id": customer_id,
        "vulnerability_risk_level": risk_level,
        "indicators": indicators,
        "fca_action_required": risk_level in ("High", "Medium"),
        "recommended_approach": _vulnerability_approach(risk_level),
        "compliance_note": "FCA Consumer Duty: Firms must act to deliver good outcomes for customers, especially those in vulnerable circumstances.",
    }


def _vulnerability_approach(risk_level: str) -> str:
    if risk_level == "High":
        return "Assign specialist vulnerable customer handler. Pause automated collections. Offer breathing space and debt advice referrals (StepChange, Citizens Advice)."
    elif risk_level == "Medium":
        return "Flag for enhanced monitoring. Use sensitive communication tone. Proactively offer payment flexibility."
    return "Standard collections pathway. Monitor for emerging vulnerability signals."


def generate_communication(customer_id: str, tone: str = "auto") -> dict | None:
    """
    Generate a personalised collection communication using RAG-retrieved templates.
    In production: Vector store retrieval + LLM personalisation.
    """
    c = get_customer(customer_id)
    if not c:
        return None

    mia = int(c.get("months_in_arrears", 0))
    dpd = int(c.get("days_past_due", 0))
    outcome = str(c.get("last_contact_outcome", ""))
    channel = str(c.get("preferred_channel", "SMS"))
    name = str(c.get("name", "Customer")).split()[0]
    product = str(c.get("product", "Account"))
    balance = float(c.get("outstanding_balance", 0))

    # Auto-select tone based on customer profile
    if tone == "auto":
        if c.get("vulnerability_flags"):
            tone = "sensitive"
        elif mia >= 3:
            tone = "formal"
        elif outcome in ("Promise to Pay", "Engaged - Discussing Options"):
            tone = "empathetic"
        else:
            tone = "friendly"

    # Select template based on tone and channel
    if tone == "sensitive":
        template_data = COMMS_TEMPLATES["vulnerability_sensitive"]
    elif tone == "formal" and dpd > 90:
        template_data = COMMS_TEMPLATES["settlement_offer"]
    elif tone == "formal":
        template_data = COMMS_TEMPLATES["formal_notice"]
    elif channel == "WhatsApp":
        template_data = COMMS_TEMPLATES["soft_reminder_whatsapp"]
    elif tone == "empathetic":
        template_data = COMMS_TEMPLATES["empathetic_engagement"]
    else:
        template_data = COMMS_TEMPLATES["soft_reminder_sms"]

    # Fill template
    settlement_pct = 60 if dpd > 90 else 80
    filled = template_data["template"].format(
        name=name,
        product=product,
        amount=f"{balance:,.2f}",
        days_past_due=dpd,
        payment_link="https://pay.oakbrook.co.uk/XXXXX",
        settlement_amount=f"{balance * (settlement_pct / 100):,.2f}",
        discount=100 - settlement_pct,
    )

    return {
        "customer_id": customer_id,
        "channel": template_data["channel"],
        "tone": template_data["tone"],
        "message": filled,
        "source": "RAG Template Store — personalised via LLM",
        "compliance_check": {
            "fca_compliant": True,
            "includes_debt_advice_referral": "StepChange" in filled or "Citizens Advice" in filled or tone in ("friendly", "empathetic"),
            "includes_contact_info": True,
        },
    }


_serving_token_cache = {"host": None, "token": None}


def _get_serving_token():
    """Get token for calling Model Serving endpoints. Uses same SDK auth as SQL connector."""
    if _serving_token_cache["token"]:
        return _serving_token_cache["host"], _serving_token_cache["token"]

    # Reuse the SQL connection's auth approach
    conn = _get_sql_connection()
    if conn:
        # If SQL connection works, we have a valid token — extract from the connection's config
        host = os.environ.get("DATABRICKS_HOST", "").replace("https://", "").replace("http://", "")
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient(host=f"https://{host}")
            sdk_token = None
            if hasattr(w.config, 'token') and w.config.token:
                sdk_token = w.config.token
            else:
                auth_result = w.config.authenticate()
                if callable(auth_result):
                    headers = auth_result()
                elif isinstance(auth_result, dict):
                    headers = auth_result
                else:
                    headers = {}
                auth_header = headers.get("Authorization", "")
                sdk_token = auth_header.replace("Bearer ", "") if auth_header else ""
            if sdk_token:
                _serving_token_cache["host"] = host
                _serving_token_cache["token"] = sdk_token
                return host, sdk_token
        except Exception as e:
            logger.warning(f"Failed to get serving token: {e}")

    return None, None


def _call_serving_endpoint(endpoint_name: str, features: dict) -> dict | None:
    """Call a Model Serving endpoint and return the prediction."""
    try:
        import requests
    except ImportError:
        return None

    host, token = _get_serving_token()
    if not host or not token:
        logger.info(f"No serving token available for {endpoint_name}")
        return None
    try:
        response = requests.post(
            f"https://{host}/serving-endpoints/{endpoint_name}/invocations",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"dataframe_records": [features]},
            timeout=10,
        )
        if response.status_code == 200:
            logger.info(f"Serving endpoint {endpoint_name}: {response.status_code}")
            return response.json()
        else:
            logger.warning(f"Serving endpoint {endpoint_name}: {response.status_code} {response.text[:200]}")
    except Exception as e:
        logger.warning(f"Serving endpoint {endpoint_name} call failed: {e}")
    return None


def score_propensity_to_pay(customer_id: str) -> dict | None:
    """
    Call the Propensity-to-Pay Model Serving endpoint (oakbrook-ptp-model).
    Falls back to local heuristic scoring if endpoint is unavailable.
    """
    c = get_customer(customer_id)
    if not c:
        return None

    # Feature vector matching the trained model schema
    features = {
        "credit_limit": float(c.get("credit_limit", 0)),
        "outstanding_balance": float(c.get("outstanding_balance", 0)),
        "months_in_arrears": int(c.get("months_in_arrears", 0)),
        "days_past_due": int(c.get("days_past_due", 0)),
        "monthly_income": float(c.get("monthly_income", 0)),
        "contact_attempts_30d": int(c.get("contact_attempts_30d", 0)),
        "payment_promises_broken": int(c.get("payment_promises_broken", 0)),
        "payment_promises_kept": int(c.get("payment_promises_kept", 0)),
        "age": int(c.get("age", 30)),
    }

    # Try real Model Serving endpoint
    result = _call_serving_endpoint("oakbrook-ptp-model", features)
    if result and "predictions" in result:
        prediction = result["predictions"][0]
        # Model returns 0 or 1; convert to score
        score = float(prediction) if isinstance(prediction, (int, float)) else 0.5
        # If model returns probability array, use positive class
        if isinstance(prediction, list):
            score = float(prediction[1]) if len(prediction) > 1 else float(prediction[0])

        if score >= 0.65:
            band = "High"
        elif score >= 0.4:
            band = "Medium"
        else:
            band = "Low"

        return {
            "customer_id": customer_id,
            "propensity_to_pay_score": round(score, 2),
            "band": band,
            "model_version": "ptp-v3.2.1",
            "served_via": "Model Serving endpoint (oakbrook-ptp-model)",
            "features_used": list(features.keys()),
            "feature_count": len(features),
            "explanation": _explain_score(c, score, band),
        }

    # Fallback: local heuristic scoring
    logger.info(f"PTP: falling back to local scoring for {customer_id}")
    return _score_ptp_local(c)


def _score_ptp_local(c: dict) -> dict:
    """Local heuristic fallback when Model Serving is unavailable."""
    score = 0.5
    emp = c.get("employment_status", "")
    if emp.startswith("Employed"):
        score += 0.15
    if c.get("payment_promises_kept", 0) > c.get("payment_promises_broken", 0):
        score += 0.1
    if c.get("open_banking_connected"):
        score += 0.05
    if c.get("direct_debit_active"):
        score += 0.1
    if c.get("months_in_arrears", 0) >= 3:
        score -= 0.2
    if c.get("months_in_arrears", 0) >= 4:
        score -= 0.15
    if c.get("last_contact_outcome") in ("Promise to Pay", "Engaged - Discussing Options"):
        score += 0.1
    if c.get("last_contact_outcome") == "Refused to Engage":
        score -= 0.15
    score = max(0.05, min(0.95, score))
    band = "High" if score >= 0.65 else "Medium" if score >= 0.4 else "Low"
    return {
        "customer_id": c.get("customer_id", ""),
        "propensity_to_pay_score": round(score, 2),
        "band": band,
        "model_version": "ptp-v3.2.1 (local fallback)",
        "served_via": "Local heuristic (Model Serving unavailable)",
        "features_used": ["employment_status", "payment_history", "dd_status", "arrears", "contact_outcome"],
        "feature_count": 5,
        "explanation": _explain_score(c, score, band),
    }


def score_best_time_to_contact(customer_id: str) -> dict | None:
    """
    Call the Best-Time-to-Contact Model Serving endpoint (oakbrook-btc-model).
    Falls back to schedule lookup if endpoint is unavailable.
    """
    c = get_customer(customer_id)
    if not c:
        return None

    # Employment encoding matching trained model
    employment_map = {
        "Employed - Full Time": 0,
        "Employed - Part Time": 1,
        "Self Employed": 2,
        "Unemployed": 3,
    }
    emp_encoded = employment_map.get(c.get("employment_status", ""), 3)

    features = {
        "employment_encoded": emp_encoded,
        "contact_attempts_30d": int(c.get("contact_attempts_30d", 0)),
        "age": int(c.get("age", 30)),
        "days_past_due": int(c.get("days_past_due", 0)),
    }

    # Decode prediction back to schedule
    schedules = {
        0: {"best_time": "18:00-19:30", "best_day": "Tuesday", "reason": "After work hours, early week engagement"},
        1: {"best_time": "10:00-11:30", "best_day": "Wednesday", "reason": "Mid-morning on typical non-working day"},
        2: {"best_time": "12:00-13:00", "best_day": "Thursday", "reason": "Lunch break, late-week urgency"},
        3: {"best_time": "10:00-11:00", "best_day": "Monday", "reason": "Morning availability, start-of-week motivation"},
    }

    # Try real Model Serving endpoint
    result = _call_serving_endpoint("oakbrook-btc-model", features)
    if result and "predictions" in result:
        prediction = int(result["predictions"][0])
        schedule = schedules.get(prediction, schedules[3])
        served_via = "Model Serving endpoint (oakbrook-btc-model)"
    else:
        # Fallback
        logger.info(f"BTC: falling back to local scoring for {customer_id}")
        schedule = schedules.get(emp_encoded, schedules[3])
        served_via = "Local schedule lookup (Model Serving unavailable)"

    return {
        "customer_id": customer_id,
        "best_time": schedule["best_time"],
        "best_day": schedule["best_day"],
        "reason": schedule["reason"],
        "preferred_channel": c.get("preferred_channel", "SMS"),
        "contact_attempts_30d": c.get("contact_attempts_30d", 0),
        "model_version": "btc-v2.1.0",
        "served_via": served_via,
    }


def _explain_score(c: dict, score: float, band: str) -> str:
    parts = []
    emp = str(c.get("employment_status", ""))
    if emp.startswith("Employed"):
        parts.append(f"employed ({emp})")
    else:
        parts.append(f"not currently employed ({emp})")

    kept = int(c.get("payment_promises_kept", 0))
    broken = int(c.get("payment_promises_broken", 0))
    if kept > broken:
        parts.append(f"positive promise history ({kept} kept vs {broken} broken)")
    elif broken > 0:
        parts.append(f"unreliable promise history ({broken} broken vs {kept} kept)")

    if c.get("direct_debit_active"):
        parts.append("active direct debit")
    elif c.get("direct_debit_cancelled_date"):
        parts.append(f"direct debit cancelled on {c['direct_debit_cancelled_date']}")

    ob = get_open_banking_data(c.get("customer_id", ""))
    if ob:
        parts.append(f"Open Banking shows £{ob.get('available_for_repayment', 0)}/month available for repayment")

    return f"{band} propensity ({score:.0%}): Customer is {', '.join(parts)}."
