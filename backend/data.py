"""
Synthetic collections data modelled on Oakbrook Finance's non-prime lending domain.
Represents what would live in Feature Tables / Unity Catalog in production.

Databricks components demonstrated:
- Feature Tables (Unity Catalog) → Customer 360 profiles
- MLflow Model Serving → Propensity-to-pay, Best-time-to-contact
- Structured Streaming → Real-time payment signals, DD cancellations
- MCP Server → Collection scorecard validation logic
- Open Banking → Affordability assessment via ClearScore/aggregators
"""

import json

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
    return CUSTOMERS.get(customer_id)


def get_all_customers() -> list[dict]:
    return list(CUSTOMERS.values())


def get_payment_history(customer_id: str) -> list[dict] | None:
    return PAYMENT_HISTORY.get(customer_id)


def get_open_banking_data(customer_id: str) -> dict | None:
    """Retrieve Open Banking affordability data (ClearScore / aggregator)."""
    return OPEN_BANKING_DATA.get(customer_id)


def get_scorecard_segment(customer_id: str, propensity_band: str) -> dict | None:
    """
    Match customer to collection scorecard segment.
    In production: MCP Server serving the scorecard SQL logic.
    """
    c = CUSTOMERS.get(customer_id)
    if not c:
        return None

    dpd = c["days_past_due"]
    outcome = c["last_contact_outcome"].lower()

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
    c = CUSTOMERS.get(customer_id)
    if not c:
        return None

    indicators = []
    risk_level = "Low"

    # Employment-based vulnerability
    if c["employment_status"] == "Unemployed":
        indicators.append({"type": "Financial", "detail": "Currently unemployed", "severity": "High"})
        risk_level = "High"
    elif c["employment_status"] == "Self Employed":
        indicators.append({"type": "Financial", "detail": "Self-employed — income may be irregular", "severity": "Medium"})

    # Arrears depth
    if c["months_in_arrears"] >= 3:
        indicators.append({"type": "Financial", "detail": f"{c['months_in_arrears']} months in arrears — persistent debt indicator", "severity": "High"})
        risk_level = "High"

    # Contact engagement
    if c["last_contact_outcome"] == "Refused to Engage":
        indicators.append({"type": "Behavioural", "detail": "Refusing to engage — potential distress signal", "severity": "Medium"})

    # Contact frequency
    if c["contact_attempts_30d"] >= 6:
        indicators.append({"type": "Compliance", "detail": f"{c['contact_attempts_30d']} contact attempts in 30 days — approaching limit", "severity": "Medium"})

    # Existing flags
    for flag in c.get("vulnerability_flags", []):
        label = flag.replace("_", " ").title()
        indicators.append({"type": "Flagged", "detail": f"Previously flagged: {label}", "severity": "Medium"})

    # Open Banking signals
    ob = OPEN_BANKING_DATA.get(customer_id)
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
    c = CUSTOMERS.get(customer_id)
    if not c:
        return None

    # Auto-select tone based on customer profile
    if tone == "auto":
        if c.get("vulnerability_flags"):
            tone = "sensitive"
        elif c["months_in_arrears"] >= 3:
            tone = "formal"
        elif c["last_contact_outcome"] in ("Promise to Pay", "Engaged - Discussing Options"):
            tone = "empathetic"
        else:
            tone = "friendly"

    # Select template based on tone and channel
    channel = c["preferred_channel"]
    if tone == "sensitive":
        template_data = COMMS_TEMPLATES["vulnerability_sensitive"]
    elif tone == "formal" and c["days_past_due"] > 90:
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
    settlement_pct = 60 if c["days_past_due"] > 90 else 80
    filled = template_data["template"].format(
        name=c["name"].split()[0],
        product=c["product"],
        amount=f"{c['outstanding_balance']:,.2f}",
        days_past_due=c["days_past_due"],
        payment_link="https://pay.oakbrook.co.uk/XXXXX",
        settlement_amount=f"{c['outstanding_balance'] * (settlement_pct / 100):,.2f}",
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


def score_propensity_to_pay(customer_id: str) -> dict | None:
    """
    Simulates an MLflow Model Serving endpoint for propensity-to-pay.
    In production this would call a registered MLflow model via Model Serving.
    """
    c = CUSTOMERS.get(customer_id)
    if not c:
        return None

    score = 0.5
    if c["employment_status"].startswith("Employed"):
        score += 0.15
    if c["payment_promises_kept"] > c["payment_promises_broken"]:
        score += 0.1
    if c["open_banking_connected"]:
        score += 0.05
    if c["direct_debit_active"]:
        score += 0.1
    if c["months_in_arrears"] >= 3:
        score -= 0.2
    if c["months_in_arrears"] >= 4:
        score -= 0.15
    if c["last_contact_outcome"] in ("Promise to Pay", "Engaged - Discussing Options"):
        score += 0.1
    if c["last_contact_outcome"] == "Refused to Engage":
        score -= 0.15

    # Open Banking boost
    ob = OPEN_BANKING_DATA.get(customer_id)
    if ob:
        if ob["available_for_repayment"] > 200:
            score += 0.1
        elif ob["available_for_repayment"] < 50:
            score -= 0.1

    score = max(0.05, min(0.95, score))

    if score >= 0.65:
        band = "High"
    elif score >= 0.4:
        band = "Medium"
    else:
        band = "Low"

    features_used = [
        "employment_status", "payment_history", "direct_debit_status",
        "arrears_depth", "contact_outcome",
    ]
    if ob:
        features_used.extend(["open_banking_disposable_income", "open_banking_stability"])

    return {
        "customer_id": customer_id,
        "propensity_to_pay_score": round(score, 2),
        "band": band,
        "model_version": "ptp-v3.2.1",
        "served_via": "MLflow Model Serving (Feature Table lookup + real-time scoring)",
        "features_used": features_used,
        "feature_count": len(features_used),
        "explanation": _explain_score(c, score, band),
    }


def score_best_time_to_contact(customer_id: str) -> dict | None:
    """
    Simulates an MLflow Model Serving endpoint for best-time-to-contact.
    """
    c = CUSTOMERS.get(customer_id)
    if not c:
        return None

    schedules = {
        "Employed - Full Time": {"best_time": "18:00-19:30", "best_day": "Tuesday", "reason": "After work hours, early week engagement"},
        "Employed - Part Time": {"best_time": "10:00-11:30", "best_day": "Wednesday", "reason": "Mid-morning on typical non-working day"},
        "Self Employed": {"best_time": "12:00-13:00", "best_day": "Thursday", "reason": "Lunch break, late-week urgency"},
        "Unemployed": {"best_time": "10:00-11:00", "best_day": "Monday", "reason": "Morning availability, start-of-week motivation"},
    }

    schedule = schedules.get(c["employment_status"], schedules["Unemployed"])

    return {
        "customer_id": customer_id,
        "best_time": schedule["best_time"],
        "best_day": schedule["best_day"],
        "reason": schedule["reason"],
        "preferred_channel": c["preferred_channel"],
        "contact_attempts_30d": c["contact_attempts_30d"],
        "model_version": "btc-v2.1.0",
        "served_via": "MLflow Model Serving (behavioural pattern analysis)",
    }


def _explain_score(c: dict, score: float, band: str) -> str:
    parts = []
    if c["employment_status"].startswith("Employed"):
        parts.append(f"employed ({c['employment_status']})")
    else:
        parts.append(f"not currently employed ({c['employment_status']})")

    kept = c["payment_promises_kept"]
    broken = c["payment_promises_broken"]
    if kept > broken:
        parts.append(f"positive promise history ({kept} kept vs {broken} broken)")
    elif broken > 0:
        parts.append(f"unreliable promise history ({broken} broken vs {kept} kept)")

    if c["direct_debit_active"]:
        parts.append("active direct debit")
    elif c["direct_debit_cancelled_date"]:
        parts.append(f"direct debit cancelled on {c['direct_debit_cancelled_date']}")

    ob = OPEN_BANKING_DATA.get(c["customer_id"])
    if ob:
        parts.append(f"Open Banking shows £{ob['available_for_repayment']}/month available for repayment")

    return f"{band} propensity ({score:.0%}): Customer is {', '.join(parts)}."
