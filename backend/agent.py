"""
AI Collections Agent — demonstrates multi-agent orchestration for Oakbrook Finance.

Databricks capabilities demonstrated:
1. Feature Tables (Unity Catalog) → Customer 360 profiles
2. MLflow Model Serving → Propensity-to-pay, Best-time-to-contact scoring
3. MLflow 3.0 → Full traceability of agent decisions (trace panel)
4. Structured Streaming → Real-time payment signals, DD cancellations
5. MCP Server → Collection scorecard validation logic
6. Multi-Agent Supervisor → Combining Genie + scoring agent + action tools
7. RAG → Communication tone/template retrieval and personalisation
8. Open Banking → Affordability assessment via ClearScore
9. FCA Consumer Duty → Vulnerability assessment and compliance flags
"""

import json
import os
import logging
from openai import OpenAI

MLFLOW_AVAILABLE = False  # Using REST API tracing instead


def _log_agent_trace(user_query: str, trace: list[dict], response_text: str):
    """Log the full agent run as an MLflow trace via REST API."""
    try:
        import time
        import requests as _req

        host = os.environ.get("DATABRICKS_HOST", "").replace("https://", "").replace("http://", "")
        if not host:
            return

        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient(host=f"https://{host}")
        token = None
        if hasattr(w.config, 'token') and w.config.token:
            token = w.config.token
        else:
            auth_result = w.config.authenticate()
            headers = auth_result() if callable(auth_result) else auth_result if isinstance(auth_result, dict) else {}
            token = headers.get("Authorization", "").replace("Bearer ", "")

        if not token:
            return

        hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        ts_start = int(time.time() * 1000)

        # Build trace inputs/outputs
        tool_summary = " → ".join([t["tool"] for t in trace])
        inputs_json = json.dumps({
            "query": user_query[:200],
            "tool_count": len(trace),
        })
        outputs_json = json.dumps({
            "tools_called": tool_summary,
            "response_length": len(response_text),
            "response_preview": response_text[:200],
        })

        # Start trace
        start_resp = _req.post(
            f"https://{host}/api/2.0/mlflow/traces",
            headers=hdrs,
            json={
                "experiment_id": "1734200624343198",
                "timestamp_ms": ts_start,
                "status": "IN_PROGRESS",
                "request_metadata": [
                    {"key": "mlflow.traceName", "value": "collections_brain_agent"},
                    {"key": "mlflow.traceInputs", "value": inputs_json},
                ],
                "tags": [
                    {"key": "app", "value": "oakbrook-collections-brain"},
                    {"key": "tool_count", "value": str(len(trace))},
                    {"key": "tools", "value": tool_summary[:200]},
                ],
            },
            timeout=5,
        )
        if start_resp.status_code != 200:
            return

        trace_id = start_resp.json().get("trace_info", {}).get("request_id", "")
        if not trace_id:
            return

        # End trace
        ts_end = int(time.time() * 1000)
        _req.patch(
            f"https://{host}/api/2.0/mlflow/traces/{trace_id}",
            headers=hdrs,
            json={
                "status": "OK",
                "timestamp_ms": ts_end,
                "request_metadata": [
                    {"key": "mlflow.traceName", "value": "collections_brain_agent"},
                    {"key": "mlflow.traceInputs", "value": inputs_json},
                    {"key": "mlflow.traceOutputs", "value": outputs_json},
                ],
            },
            timeout=5,
        )
        logger.info(f"Agent trace logged: {trace_id} ({len(trace)} tool calls)")
    except Exception as e:
        logger.debug(f"Agent trace logging failed: {e}")

logger = logging.getLogger(__name__)

from backend.data import (
    get_customer,
    get_all_customers,
    get_payment_history,
    get_open_banking_data,
    get_scorecard_segment,
    assess_vulnerability,
    generate_communication,
    score_propensity_to_pay,
    score_best_time_to_contact,
)

# Tools exposed to the agent — each maps to a Databricks capability
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "lookup_customer",
            "description": "Look up a customer's full profile from the Customer 360 Feature Table (Unity Catalog). Returns demographics, account details, contact history, risk segment, and vulnerability flags.",
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {
                        "type": "string",
                        "description": "Customer ID (e.g. C-10001). If unknown, use list_customers to see all.",
                    }
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_customers",
            "description": "List all customers in the collections portfolio with summary info including risk segment, days past due, and balance.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_payment_history",
            "description": "Retrieve payment history for a customer from the Structured Streaming transaction pipeline. Shows payment dates, amounts, statuses (Paid/Missed/Failed/DD Cancelled), and methods.",
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {"type": "string", "description": "Customer ID"}
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "score_propensity_to_pay",
            "description": "Call the Propensity-to-Pay ML model served via MLflow Model Serving. Uses Feature Table features (employment, payment history, Open Banking data, DD status, arrears depth). Returns probability score, band (High/Medium/Low), model version, and explainability breakdown.",
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {"type": "string", "description": "Customer ID"}
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "score_best_time_to_contact",
            "description": "Call the Best-Time-to-Contact ML model served via MLflow Model Serving. Analyses behavioural patterns to recommend optimal day, time window, and channel.",
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {"type": "string", "description": "Customer ID"}
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_open_banking_data",
            "description": "Retrieve Open Banking affordability data for a customer (via ClearScore / aggregator integration). Shows verified income, expenses, disposable income, other credit commitments, and available-for-repayment figure. Only available for customers who have consented to Open Banking.",
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {"type": "string", "description": "Customer ID"}
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_scorecard_segment",
            "description": "Match customer to a collection scorecard segment via the MCP Server (validation logic). Returns the assigned segment, strategy, recommended actions, contact frequency limits, and escalation triggers. Requires propensity band from the PTP model.",
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {"type": "string", "description": "Customer ID"},
                    "propensity_band": {"type": "string", "description": "Propensity-to-pay band: High, Medium, or Low"},
                },
                "required": ["customer_id", "propensity_band"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "assess_vulnerability",
            "description": "Assess customer vulnerability indicators per FCA Consumer Duty requirements. Checks employment, arrears depth, engagement patterns, Open Banking signals (e.g. gambling), and existing flags. Returns risk level and recommended handling approach.",
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {"type": "string", "description": "Customer ID"}
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_communication",
            "description": "Generate a personalised collection communication using RAG-retrieved templates. Auto-selects tone (friendly/empathetic/formal/sensitive) based on customer profile, or accepts a specific tone. Returns the message, channel, and compliance checks.",
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {"type": "string", "description": "Customer ID"},
                    "tone": {
                        "type": "string",
                        "description": "Communication tone: 'auto' (recommended), 'friendly', 'empathetic', 'formal', or 'sensitive'",
                        "default": "auto",
                    },
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "recommend_strategy",
            "description": "Generate a comprehensive collections strategy for a customer. This is the Multi-Agent Supervisor function that orchestrates all other tools: runs PTP model, BTC model, scorecard, vulnerability assessment, and Open Banking check, then combines everything into an actionable plan with compliance flags and suggested payment plan.",
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {"type": "string", "description": "Customer ID"}
                },
                "required": ["customer_id"],
            },
        },
    },
]

SYSTEM_PROMPT = """You are the Oakbrook Collections Brain — an AI collections agent that helps the collections team make optimal, compliant decisions about customer outreach and recovery.

You have access to these tools, each powered by a different Databricks capability:

1. **lookup_customer** — Customer 360 Feature Table (Unity Catalog)
2. **list_customers** — Portfolio overview from Feature Tables
3. **get_payment_history** — Real-time transaction stream (Structured Streaming)
4. **score_propensity_to_pay** — ML model via MLflow Model Serving (~1,000 features in production)
5. **score_best_time_to_contact** — ML model via MLflow Model Serving
6. **get_open_banking_data** — ClearScore/aggregator affordability data
7. **get_scorecard_segment** — Collection scorecard via MCP Server (validation logic)
8. **assess_vulnerability** — FCA Consumer Duty vulnerability screening
9. **generate_communication** — Personalised comms via RAG template retrieval
10. **recommend_strategy** — Multi-Agent Supervisor (orchestrates all of the above)

**Architecture note:** In production, this agent runs as a Databricks Multi-Agent Supervisor that:
- Queries Genie for customer payment history and portfolio data
- Calls MLflow Model Serving endpoints for real-time scoring
- Uses MCP Server for scorecard validation logic
- Retrieves communication templates from a Vector Search index (RAG)
- Logs every decision via MLflow 3.0 Tracing for full audit trail

**Key principles:**
- Always start by looking up customer data before making recommendations
- Use ML models to score propensity and timing — never guess
- Check vulnerability indicators before any escalation action
- Flag FCA/Consumer Duty compliance considerations
- Explain reasoning clearly — every decision must be auditable (MLflow 3.0)
- Reference model versions in outputs for traceability
- When suggesting payment plans, use Open Banking data if available for affordability

You work for Oakbrook Finance (O6K), a UK non-prime lender in Nottingham. Products include credit cards and debt consolidation loans. Customers are in various stages of arrears. Your OKR is to achieve >20% uplift in Cure/Roll outcomes within 90 days while treating customers fairly.

Format responses clearly with sections, bullet points, and bold text. Always show model versions when presenting scores.
"""


def _execute_tool(name: str, arguments: dict) -> str:
    """Route tool calls to the appropriate data/model function."""
    # Wrap in MLflow span for tracing
    if MLFLOW_AVAILABLE:
        try:
            with mlflow.start_span(name=f"tool:{name}", span_type=SpanType.TOOL) as span:
                span.set_inputs(arguments)
                result = _execute_tool_inner(name, arguments)
                span.set_outputs({"result_length": len(result), "preview": result[:200]})
                return result
        except Exception:
            pass
    return _execute_tool_inner(name, arguments)


def _execute_tool_inner(name: str, arguments: dict) -> str:
    """Inner tool execution logic."""
    if name == "lookup_customer":
        result = get_customer(arguments["customer_id"])
        if not result:
            return json.dumps({"error": f"Customer {arguments['customer_id']} not found. Use list_customers to see available IDs."})
        return json.dumps(result)

    elif name == "list_customers":
        customers = get_all_customers()
        summary = []
        for c in customers:
            summary.append({
                "customer_id": c["customer_id"],
                "name": c["name"],
                "product": c["product"],
                "outstanding": c["outstanding_balance"],
                "days_past_due": c["days_past_due"],
                "risk_segment": c["risk_segment"],
                "months_in_arrears": c["months_in_arrears"],
                "last_contact_outcome": c["last_contact_outcome"],
                "vulnerability_flags": c.get("vulnerability_flags", []),
            })
        return json.dumps(summary)

    elif name == "get_payment_history":
        result = get_payment_history(arguments["customer_id"])
        if not result:
            return json.dumps({"error": "No payment history found."})
        return json.dumps(result)

    elif name == "score_propensity_to_pay":
        result = score_propensity_to_pay(arguments["customer_id"])
        if not result:
            return json.dumps({"error": "Customer not found."})
        return json.dumps(result)

    elif name == "score_best_time_to_contact":
        result = score_best_time_to_contact(arguments["customer_id"])
        if not result:
            return json.dumps({"error": "Customer not found."})
        return json.dumps(result)

    elif name == "get_open_banking_data":
        result = get_open_banking_data(arguments["customer_id"])
        if not result:
            return json.dumps({"error": "No Open Banking data — customer has not consented. Consider requesting consent for affordability assessment."})
        return json.dumps(result)

    elif name == "get_scorecard_segment":
        result = get_scorecard_segment(arguments["customer_id"], arguments["propensity_band"])
        if not result:
            return json.dumps({"error": "Customer not found."})
        return json.dumps(result)

    elif name == "assess_vulnerability":
        result = assess_vulnerability(arguments["customer_id"])
        if not result:
            return json.dumps({"error": "Customer not found."})
        return json.dumps(result)

    elif name == "generate_communication":
        tone = arguments.get("tone", "auto")
        result = generate_communication(arguments["customer_id"], tone)
        if not result:
            return json.dumps({"error": "Customer not found."})
        return json.dumps(result)

    elif name == "recommend_strategy":
        cid = arguments["customer_id"]
        customer = get_customer(cid)
        if not customer:
            return json.dumps({"error": "Customer not found."})
        ptp = score_propensity_to_pay(cid)
        btc = score_best_time_to_contact(cid)
        payments = get_payment_history(cid)
        ob = get_open_banking_data(cid)
        vuln = assess_vulnerability(cid)
        scorecard = get_scorecard_segment(cid, ptp["band"])

        strategy = _build_strategy(customer, ptp, btc, payments, ob, vuln, scorecard)
        return json.dumps(strategy)

    return json.dumps({"error": f"Unknown tool: {name}"})


def _build_strategy(customer: dict, ptp: dict, btc: dict, payments: list,
                    ob: dict | None, vuln: dict, scorecard: dict) -> dict:
    """Combine all model outputs with business rules to generate a comprehensive strategy."""
    cid = customer["customer_id"]
    dpd = customer["days_past_due"]

    # Channel recommendation based on tone
    if vuln["vulnerability_risk_level"] == "High":
        tone = "Sensitive — specialist handler required"
        urgency = "Low — do not pressure"
    elif ptp["band"] == "High":
        tone = "Supportive and solution-focused"
        urgency = "Standard"
    elif ptp["band"] == "Medium":
        tone = "Empathetic but clear on consequences"
        urgency = "Moderate"
    else:
        tone = "Formal, compliance-focused"
        urgency = "High"

    # Compliance flags
    compliance = []
    if vuln["vulnerability_risk_level"] in ("High", "Medium"):
        compliance.append(f"VULNERABILITY: {vuln['vulnerability_risk_level']} risk — {vuln['recommended_approach']}")
    if customer["months_in_arrears"] >= 3:
        compliance.append("FCA: Must assess vulnerability indicators before escalation")
    if not customer["open_banking_connected"]:
        compliance.append("Consider requesting Open Banking consent for affordability assessment")
    if customer["contact_attempts_30d"] >= 6:
        compliance.append("WARNING: Approaching contact frequency limit — risk of harassment claim")
    compliance.append("Consumer Duty: Ensure outcome is in customer's best interest")

    # Affordability-based payment plan
    balance = customer["outstanding_balance"]
    if ob:
        affordable_payment = min(ob["available_for_repayment"], round(ob["monthly_income_verified"] * 0.08, 2))
        affordability_source = "Open Banking verified"
    else:
        income = customer["monthly_income"]
        affordable_payment = round(income * 0.08, 2)
        affordability_source = "Estimated (no Open Banking)"

    # Estimated automation savings
    automation = {
        "manual_time_per_case_mins": 45,
        "ai_time_per_case_mins": 5,
        "time_saved_mins": 40,
        "at_portfolio_scale_monthly_hours": "~70 hours/month (OKR target)",
    }

    return {
        "customer_id": cid,
        "customer_name": customer["name"],
        "scorecard_segment": scorecard["assigned_segment"],
        "scorecard_strategy": scorecard["strategy"],
        "propensity_to_pay": {"band": ptp["band"], "score": ptp["propensity_to_pay_score"]},
        "vulnerability_assessment": {"risk_level": vuln["vulnerability_risk_level"], "indicators_count": len(vuln["indicators"])},
        "recommended_actions": scorecard["recommended_actions"],
        "contact_plan": {
            "channel": btc["preferred_channel"],
            "best_day": btc["best_day"],
            "best_time": btc["best_time"],
            "tone": tone,
            "urgency": urgency,
            "max_frequency": scorecard["max_contact_frequency"],
        },
        "suggested_payment_plan": {
            "outstanding_balance": balance,
            "affordable_monthly_payment": affordable_payment,
            "affordability_source": affordability_source,
            "months_to_clear": round(balance / affordable_payment) if affordable_payment > 0 else None,
        },
        "compliance_flags": compliance,
        "escalation_trigger": scorecard["escalation_trigger"],
        "automation_impact": automation,
        "model_versions": {
            "propensity_to_pay": ptp["model_version"],
            "best_time_to_contact": btc["model_version"],
            "scorecard": "MCP Server v2.4",
            "comms_templates": "RAG Vector Store v1.2",
        },
        "mlflow_trace_id": f"trace-{cid}-20260316-001",
    }


def get_llm_client():
    """Create OpenAI-compatible client for Databricks Model Serving."""
    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")

    if host and token:
        clean_host = host.replace("https://", "").replace("http://", "")
        return OpenAI(
            base_url=f"https://{clean_host}/serving-endpoints",
            api_key=token,
        ), "databricks-claude-sonnet-4-6"

    # Databricks Apps: use SDK default auth (service principal identity)
    if host:
        clean_host = host.replace("https://", "").replace("http://", "")
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient(host=f"https://{clean_host}")
            # Try multiple approaches to get the token
            sdk_token = None
            # Approach 1: config.token property
            if hasattr(w.config, 'token') and w.config.token:
                sdk_token = w.config.token
            # Approach 2: authenticate() may return headers dict or callable
            if not sdk_token:
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
                return OpenAI(
                    base_url=f"https://{clean_host}/serving-endpoints",
                    api_key=sdk_token,
                ), "databricks-claude-sonnet-4-6"
        except Exception:
            pass

    # Fallback for local dev
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if api_key:
        return OpenAI(api_key=api_key), "gpt-4o-mini"

    return OpenAI(
        base_url="https://416411475796958.8.gcp.databricks.com/serving-endpoints",
        api_key=os.environ.get("DATABRICKS_DEMO_TOKEN", "no-token"),
    ), "databricks-claude-sonnet-4-6"


async def run_agent(messages: list[dict]) -> dict:
    """
    Run the collections agent with tool-calling loop.
    Returns the final assistant message and tool call trace.
    Logs the full trace to MLflow via REST API.
    """
    result = await _run_agent_inner(messages)

    # Log trace to MLflow (fire-and-forget in background thread)
    import threading
    user_query = ""
    for m in reversed(messages):
        if m["role"] == "user":
            user_query = m["content"]
            break
    threading.Thread(target=_log_agent_trace, args=(user_query, result.get("trace", []), result.get("response", "")), daemon=True).start()

    return result


async def _run_agent_inner(messages: list[dict]) -> dict:
    client, model = get_llm_client()

    full_messages = [{"role": "system", "content": SYSTEM_PROMPT}] + messages

    trace = []
    max_iterations = 10

    for i in range(max_iterations):
        response = client.chat.completions.create(
            model=model,
            messages=full_messages,
            tools=TOOLS,
            tool_choice="auto",
        )

        choice = response.choices[0]

        if choice.finish_reason == "tool_calls" or (choice.message.tool_calls and len(choice.message.tool_calls) > 0):
            full_messages.append(choice.message)

            for tc in choice.message.tool_calls:
                fn_name = tc.function.name
                fn_args = json.loads(tc.function.arguments)

                trace.append({
                    "iteration": i + 1,
                    "tool": fn_name,
                    "arguments": fn_args,
                    "databricks_component": _tool_to_component(fn_name),
                })

                result = _execute_tool(fn_name, fn_args)
                trace[-1]["result_preview"] = result[:300] + "..." if len(result) > 300 else result

                full_messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result,
                })
        else:
            return {
                "response": choice.message.content,
                "trace": trace,
            }

    return {
        "response": "Agent reached maximum iterations. Please refine your question.",
        "trace": trace,
    }


async def run_agent_stream(messages: list[dict]):
    """
    Run the collections agent with streaming output.
    Uses the non-streaming run_agent for the full tool loop (reliable),
    then streams the final response character-by-character for UX.
    """
    client, model = get_llm_client()
    full_messages = [{"role": "system", "content": SYSTEM_PROMPT}] + messages

    trace = []
    max_iterations = 10

    # Run the full tool-calling loop non-streamed for reliability
    for i in range(max_iterations):
        response = client.chat.completions.create(
            model=model,
            messages=full_messages,
            tools=TOOLS,
            tool_choice="auto",
        )

        choice = response.choices[0]

        if choice.finish_reason == "tool_calls" or (choice.message.tool_calls and len(choice.message.tool_calls) > 0):
            # If the LLM also included text content alongside tool calls, add it to messages
            full_messages.append(choice.message)

            for tc in choice.message.tool_calls:
                fn_name = tc.function.name
                fn_args = json.loads(tc.function.arguments)

                trace_entry = {
                    "iteration": i + 1,
                    "tool": fn_name,
                    "arguments": fn_args,
                    "databricks_component": _tool_to_component(fn_name),
                }
                trace.append(trace_entry)

                # Yield tool call event so frontend shows real-time progress
                yield {"type": "tool_call", "data": trace_entry}

                result = _execute_tool(fn_name, fn_args)

                full_messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result,
                })
        else:
            # Final text response — stream it in chunks for progressive display
            final_text = choice.message.content or ""
            chunk_size = 12  # ~12 chars at a time for smooth streaming feel
            for j in range(0, len(final_text), chunk_size):
                yield {"type": "token", "data": final_text[j:j + chunk_size]}

            # Log trace to MLflow
            user_query = ""
            for m in reversed(messages):
                if m["role"] == "user":
                    user_query = m["content"]
                    break
            import threading
            threading.Thread(target=_log_agent_trace, args=(user_query, trace, final_text), daemon=True).start()

            yield {"type": "done", "data": final_text}
            return

    yield {"type": "done", "data": "Agent reached maximum iterations."}


def _tool_to_component(tool_name: str) -> str:
    """Map tool names to Databricks components for the trace UI."""
    mapping = {
        "lookup_customer": "Feature Tables (Unity Catalog)",
        "list_customers": "Feature Tables (Unity Catalog)",
        "get_payment_history": "Structured Streaming",
        "score_propensity_to_pay": "MLflow Model Serving",
        "score_best_time_to_contact": "MLflow Model Serving",
        "get_open_banking_data": "Open Banking (ClearScore)",
        "get_scorecard_segment": "MCP Server (Scorecard)",
        "assess_vulnerability": "FCA Vulnerability Engine",
        "generate_communication": "RAG (Vector Search)",
        "recommend_strategy": "Multi-Agent Supervisor",
    }
    return mapping.get(tool_name, "Unknown")
