"""
FastAPI backend for the Oakbrook AI Collections Agent demo.
"""

import os
import json
import traceback
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel
from pathlib import Path

from backend.agent import run_agent, run_agent_stream
from backend.data import get_all_customers
from backend.memory import ConversationMemory

app = FastAPI(title="Oakbrook Collections Brain")

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

# Conversation memory backed by Lakebase (PostgreSQL)
memory = ConversationMemory()


class ChatRequest(BaseModel):
    messages: list[dict]
    session_id: str = "default"


class ChatResponse(BaseModel):
    response: str
    trace: list[dict]


@app.get("/api/customers")
async def list_customers():
    """Return all customers for the portfolio overview."""
    return get_all_customers()


@app.get("/api/debug")
async def debug():
    """Debug endpoint to check environment."""
    host = os.environ.get("DATABRICKS_HOST", "NOT SET")
    token = os.environ.get("DATABRICKS_TOKEN", "NOT SET")
    sdk_token = "NOT AVAILABLE"
    sdk_error = None
    try:
        from databricks.sdk import WorkspaceClient
        clean_host = host.replace("https://", "").replace("http://", "") if host != "NOT SET" else ""
        w = WorkspaceClient(host=f"https://{clean_host}") if clean_host else WorkspaceClient()
        t = ""
        if hasattr(w.config, 'token') and w.config.token:
            t = w.config.token
        else:
            auth_result = w.config.authenticate()
            if callable(auth_result):
                headers = auth_result()
            elif isinstance(auth_result, dict):
                headers = auth_result
            else:
                headers = {}
            auth_header = headers.get("Authorization", "")
            t = auth_header.replace("Bearer ", "") if auth_header else ""
        sdk_token = f"{t[:20]}..." if t else "EMPTY"
    except Exception as e:
        sdk_error = str(e)
    return {
        "DATABRICKS_HOST": host,
        "DATABRICKS_TOKEN": f"{token[:20]}..." if token and token != "NOT SET" else "NOT SET",
        "SDK_TOKEN": sdk_token,
        "SDK_ERROR": sdk_error,
        "LAKEBASE_STATUS": memory.status(),
    }


@app.post("/api/chat")
async def chat(request: ChatRequest):
    """Send a message to the AI Collections Agent (non-streaming)."""
    try:
        # Load conversation history from Lakebase
        history = memory.get_history(request.session_id)
        all_messages = history + request.messages

        result = await run_agent(all_messages)

        # Save to Lakebase memory
        for msg in request.messages:
            memory.save_message(request.session_id, msg["role"], msg["content"])
        memory.save_message(request.session_id, "assistant", result["response"])

        return result
    except Exception as e:
        tb = traceback.format_exc()
        return JSONResponse(
            status_code=500,
            content={"detail": str(e), "traceback": tb},
        )


@app.post("/api/chat/stream")
async def chat_stream(request: ChatRequest):
    """Send a message to the AI Collections Agent with streaming response."""
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

            # Save to Lakebase memory
            for msg in request.messages:
                memory.save_message(request.session_id, msg["role"], msg["content"])
            if full_response:
                memory.save_message(request.session_id, "assistant", full_response)

        return StreamingResponse(event_generator(), media_type="text/event-stream")
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": str(e)},
        )


@app.get("/api/sessions/{session_id}/history")
async def get_session_history(session_id: str):
    """Retrieve conversation history from Lakebase."""
    return memory.get_history(session_id)


@app.delete("/api/sessions/{session_id}")
async def clear_session(session_id: str):
    """Clear conversation history for a session."""
    memory.clear_history(session_id)
    return {"status": "cleared"}


@app.get("/api/architecture")
async def serve_architecture():
    """Serve the architecture diagram."""
    img_path = Path(__file__).parent.parent / "docs" / "oakbrook_architecture.png"
    if img_path.exists():
        return FileResponse(img_path, media_type="image/png")
    return JSONResponse(status_code=404, content={"error": "Architecture diagram not found"})


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    """Serve the main UI."""
    return FileResponse(FRONTEND_DIR / "index.html")
