# Oakbrook Collections Brain

AI-powered collections agent demo for Oakbrook Finance, built as a Databricks App.

## What It Demonstrates

This demo showcases how Databricks capabilities combine into an intelligent collections agent:

| Capability | Databricks Component | What It Does |
|---|---|---|
| Customer 360 | **Feature Tables** (Unity Catalog) | Unified customer profile with demographics, account details, contact history |
| Propensity-to-Pay | **MLflow Model Serving** | Real-time ML scoring — likelihood of payment based on ~1,000 features |
| Best-Time-to-Contact | **MLflow Model Serving** | Optimal day/time/channel for outreach based on behavioural patterns |
| Payment Signals | **Structured Streaming** | Real-time events — DD cancellations, payment failures, partial payments |
| Collection Scorecard | **MCP Server** | Validation logic matching customers to strategy segments |
| Personalised Comms | **RAG** (Vector Search) | Tone-appropriate message generation from template store |
| Open Banking | **ClearScore Integration** | Verified affordability data for payment plan recommendations |
| Vulnerability Screening | **FCA Consumer Duty** | Automated vulnerability assessment before escalation |
| Agent Orchestration | **Multi-Agent Supervisor** | Combines all above into a single actionable strategy |
| Audit Trail | **MLflow 3.0 Tracing** | Every tool call logged — visible in the trace panel |

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Collections Brain UI                │
│            (Chat + Portfolio Sidebar)                │
├─────────────────────────────────────────────────────┤
│               FastAPI Backend                        │
│          ┌─────────────────────┐                    │
│          │  Agent Orchestrator  │ ← Claude Sonnet    │
│          │  (Tool-calling loop) │   via FMAPI        │
│          └──────┬──────────────┘                    │
│                 │                                    │
│    ┌────────────┼────────────────────────┐          │
│    ▼            ▼            ▼           ▼          │
│ Feature     MLflow       MCP Server   RAG           │
│ Tables      Model        (Scorecard)  (Comms        │
│ (Customer   Serving      Validation   Templates)    │
│  360)       (PTP, BTC)   Logic                      │
│    │            │            │           │          │
│    ▼            ▼            ▼           ▼          │
│ Unity       Model        Scorecard   Vector         │
│ Catalog     Registry     Rules       Search         │
│             + Serving                 Index          │
└─────────────────────────────────────────────────────┘
         ▲                        ▲
         │                        │
   Structured Streaming      Open Banking
   (Payment events,          (ClearScore
    DD cancellations)         affordability)
```

## Quick Start

### Deploy to Databricks (GCP)

```bash
# Authenticate
databricks auth login --host https://YOUR-WORKSPACE.gcp.databricks.com --profile e2-gcp

# Create the app
databricks apps create --profile e2-gcp --json '{"name": "oakbrook-collections-brain"}'

# Sync and deploy
databricks sync ./oakbrook-demo /Workspace/Users/YOU@databricks.com/apps/oakbrook-collections-brain --profile e2-gcp --watch=false
databricks apps deploy oakbrook-collections-brain --source-code-path /Workspace/Users/YOU@databricks.com/apps/oakbrook-collections-brain --profile e2-gcp
```

### Run Locally

```bash
cd oakbrook-demo
pip install -r requirements.txt
uvicorn backend.main:app --port 8000
# Open http://localhost:8000
```

Set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` env vars to use Databricks FMAPI, or `OPENAI_API_KEY` for OpenAI.

## Demo Walkthrough

1. **Portfolio Overview** — Sidebar shows all customers with risk segments, days past due, and balances
2. **Click a customer** — Pre-fills a prompt to run full analysis
3. **Agent runs tools** — Watch the trace panel to see Feature Table lookups, ML model calls, scorecard matching
4. **Strategy output** — Actionable plan with contact timing, tone, payment plan, and compliance flags
5. **Generate comms** — Ask it to draft a WhatsApp/SMS/email with auto-selected tone
6. **Vulnerability check** — FCA Consumer Duty screening before any escalation

### Sample Prompts

- "Show me the full portfolio and prioritise by risk"
- "Run a full analysis on C-10001 — models, scorecard, vulnerability, and strategy"
- "Check Open Banking affordability for C-10002 and draft a WhatsApp message"
- "Assess vulnerability for C-10003 and recommend a sensitive approach"
- "Generate a weekly contact plan for all customers"

## Synthetic Data

All customer data is synthetic and modelled on a UK non-prime lending scenario:

- **5 customers** across credit cards and debt consolidation loans
- **Payment histories** with realistic patterns (paid, missed, DD cancelled, partial)
- **Open Banking data** for 3 of 5 customers (ClearScore-style)
- **Vulnerability flags** on 2 customers
- **Collection scorecard** with 6 segments mapping to strategies

## File Structure

```
oakbrook-demo/
├── app.yaml              # Databricks App config
├── requirements.txt      # Python dependencies
├── backend/
│   ├── __init__.py
│   ├── main.py           # FastAPI routes
│   ├── agent.py          # AI agent with tool-calling loop
│   └── data.py           # Synthetic data + model scoring
└── frontend/
    └── index.html        # Chat UI with portfolio sidebar
```

## Production Roadmap

To move from demo to production, Oakbrook would:

1. **Feature Tables** → Materialise Customer 360 from Oracle (via Lakeflow Connect) + IceNet + Zendesk
2. **ML Models** → Train propensity-to-pay and best-time-to-contact on historical collections data, register in MLflow
3. **Model Serving** → Deploy models to real-time endpoints with Feature Table auto-lookup
4. **Structured Streaming** → Ingest payment events, DD cancellations from O6K transaction systems
5. **MCP Server** → Serve collection scorecard SQL logic as an MCP tool
6. **RAG** → Index approved communication templates in Vector Search
7. **Agent** → Wire up as a Databricks Agent with MLflow 3.0 tracing for full audit trail
