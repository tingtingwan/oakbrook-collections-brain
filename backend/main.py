"""
FastAPI backend for the Oakbrook AI Collections Agent demo.
"""

import os
import traceback
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from pydantic import BaseModel
from pathlib import Path

from backend.agent import run_agent
from backend.data import get_all_customers

app = FastAPI(title="Oakbrook Collections Brain")

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


class ChatRequest(BaseModel):
    messages: list[dict]


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
        # Try config.token first
        if hasattr(w.config, 'token') and w.config.token:
            t = w.config.token
        else:
            auth_result = w.config.authenticate()
            auth_type = type(auth_result).__name__
            if callable(auth_result):
                headers = auth_result()
            elif isinstance(auth_result, dict):
                headers = auth_result
            else:
                headers = {}
                sdk_error = f"authenticate() returned {auth_type}: {str(auth_result)[:100]}"
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
    }


@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Send a message to the AI Collections Agent."""
    try:
        result = await run_agent(request.messages)
        return result
    except Exception as e:
        tb = traceback.format_exc()
        return JSONResponse(
            status_code=500,
            content={"detail": str(e), "traceback": tb},
        )


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    """Serve the main UI."""
    return FileResponse(FRONTEND_DIR / "index.html")
