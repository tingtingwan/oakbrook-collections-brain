"""
FastAPI backend for the Oakbrook AI Collections Agent demo.
"""

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
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


@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Send a message to the AI Collections Agent."""
    result = await run_agent(request.messages)
    return result


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    """Serve the main UI."""
    return FileResponse(FRONTEND_DIR / "index.html")
