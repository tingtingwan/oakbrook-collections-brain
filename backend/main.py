"""
FastAPI backend for the Oakbrook AI Collections Agent demo.
"""

import os
import json
import traceback
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel
from pathlib import Path

from backend.agent import run_agent, run_agent_stream
from backend.data import (
    get_customer, get_all_customers, get_payment_history,
    score_propensity_to_pay, score_best_time_to_contact,
    get_open_banking_data, get_scorecard_segment,
    assess_vulnerability, generate_communication,
)
from backend.memory import ConversationMemory
from backend.approvals import submit_approval, list_approvals as get_approvals, update_approval_status, get_next_id

app = FastAPI(title="Oakbrook Collections Brain")

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

# Conversation memory
memory = ConversationMemory()


# ---------------------------------------------------------------------------
# Individual tool endpoints (for step-by-step workbench UI)
# ---------------------------------------------------------------------------

@app.get("/api/customers")
async def list_customers():
    return get_all_customers()


@app.get("/api/customers/{customer_id}")
async def get_customer_detail(customer_id: str):
    c = get_customer(customer_id)
    if not c:
        return JSONResponse(status_code=404, content={"error": "Customer not found"})
    return c


@app.get("/api/customers/{customer_id}/payments")
async def get_customer_payments(customer_id: str):
    result = get_payment_history(customer_id)
    if not result:
        return JSONResponse(status_code=404, content={"error": "No payment history"})
    return result


@app.get("/api/customers/{customer_id}/ptp")
async def get_customer_ptp(customer_id: str):
    result = score_propensity_to_pay(customer_id)
    if not result:
        return JSONResponse(status_code=404, content={"error": "Customer not found"})
    return result


@app.get("/api/customers/{customer_id}/btc")
async def get_customer_btc(customer_id: str):
    result = score_best_time_to_contact(customer_id)
    if not result:
        return JSONResponse(status_code=404, content={"error": "Customer not found"})
    return result


@app.get("/api/customers/{customer_id}/open-banking")
async def get_customer_ob(customer_id: str):
    result = get_open_banking_data(customer_id)
    if not result:
        return JSONResponse(status_code=404, content={"error": "No Open Banking data — customer has not consented"})
    return result


@app.get("/api/customers/{customer_id}/vulnerability")
async def get_customer_vulnerability(customer_id: str):
    result = assess_vulnerability(customer_id)
    if not result:
        return JSONResponse(status_code=404, content={"error": "Customer not found"})
    return result


@app.get("/api/customers/{customer_id}/scorecard")
async def get_customer_scorecard(customer_id: str, propensity_band: str = "Medium"):
    result = get_scorecard_segment(customer_id, propensity_band)
    if not result:
        return JSONResponse(status_code=404, content={"error": "Customer not found"})
    return result


@app.get("/api/customers/{customer_id}/communication")
async def get_customer_comms(customer_id: str, tone: str = "auto"):
    result = generate_communication(customer_id, tone)
    if not result:
        return JSONResponse(status_code=404, content={"error": "Customer not found"})
    return result


# ---------------------------------------------------------------------------
# Approval queue (UC Delta table — human-in-the-loop)
# ---------------------------------------------------------------------------

class ApprovalRequest(BaseModel):
    customer_id: str
    customer_name: str
    channel: str
    tone: str
    message: str
    strategy_summary: str = ""


@app.post("/api/approvals")
async def submit_for_approval(req: ApprovalRequest):
    """Submit a generated communication to the approval queue (UC Delta table)."""
    entry = {
        "id": get_next_id(),
        "customer_id": req.customer_id,
        "customer_name": req.customer_name,
        "channel": req.channel,
        "tone": req.tone,
        "message": req.message,
        "strategy_summary": req.strategy_summary,
        "status": "Pending",
        "submitted_at": datetime.now().isoformat(),
        "submitted_by": "Collections Brain Agent",
        "reviewed_by": None,
        "reviewed_at": None,
    }
    return submit_approval(entry)


@app.get("/api/approvals")
async def list_approvals_endpoint():
    """List all pending and processed approvals from UC."""
    return get_approvals()


@app.post("/api/approvals/{approval_id}/approve")
async def approve_comms(approval_id: str):
    """Approve a communication in UC — ready to send to WhatsApp."""
    result = update_approval_status(approval_id, "Approved")
    if not result:
        return JSONResponse(status_code=404, content={"error": "Approval not found"})
    return result


@app.post("/api/approvals/{approval_id}/reject")
async def reject_comms(approval_id: str):
    """Reject a communication in UC."""
    result = update_approval_status(approval_id, "Rejected")
    if not result:
        return JSONResponse(status_code=404, content={"error": "Approval not found"})
    return result


# ---------------------------------------------------------------------------
# Chat endpoints (for ad-hoc questions)
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    messages: list[dict]
    session_id: str = "default"


@app.post("/api/chat")
async def chat(request: ChatRequest):
    try:
        history = memory.get_history(request.session_id)
        all_messages = history + request.messages
        result = await run_agent(all_messages)
        for msg in request.messages:
            memory.save_message(request.session_id, msg["role"], msg["content"])
        memory.save_message(request.session_id, "assistant", result["response"])
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e), "traceback": traceback.format_exc()})


@app.post("/api/chat/stream")
async def chat_stream(request: ChatRequest):
    try:
        history = memory.get_history(request.session_id)
        all_messages = history + request.messages

        async def event_generator():
            full_response = ""
            trace = []
            async for event in run_agent_stream(all_messages):
                if event["type"] == "tool_call":
                    trace.append(event["data"])
                    yield f"data: {json.dumps(event)}\n\n"
                elif event["type"] == "token":
                    full_response += event["data"]
                    yield f"data: {json.dumps(event)}\n\n"
                elif event["type"] == "done":
                    full_response = event.get("data", full_response)
                    yield f"data: {json.dumps({'type': 'done', 'data': {'trace': trace}})}\n\n"
            for msg in request.messages:
                memory.save_message(request.session_id, msg["role"], msg["content"])
            if full_response:
                memory.save_message(request.session_id, "assistant", full_response)

        return StreamingResponse(event_generator(), media_type="text/event-stream")
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


# ---------------------------------------------------------------------------
# Static assets
# ---------------------------------------------------------------------------

@app.get("/api/architecture")
async def serve_architecture():
    img_path = Path(__file__).parent.parent / "docs" / "oakbrook_architecture.png"
    if img_path.exists():
        return FileResponse(img_path, media_type="image/png")
    return JSONResponse(status_code=404, content={"error": "Architecture diagram not found"})


@app.get("/api/debug")
async def debug():
    host = os.environ.get("DATABRICKS_HOST", "NOT SET")
    token = os.environ.get("DATABRICKS_TOKEN", "NOT SET")
    return {"DATABRICKS_HOST": host, "DATABRICKS_TOKEN": "SET" if token != "NOT SET" else "NOT SET", "LAKEBASE_STATUS": memory.status(), "APPROVALS_COUNT": len(approval_queue)}


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    return FileResponse(FRONTEND_DIR / "index.html")
