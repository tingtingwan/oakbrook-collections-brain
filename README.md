# AI Collections Brain

AI-powered collections agent built as a Databricks App. Demonstrates how Databricks capabilities combine into an intelligent, compliant collections workflow — from customer profiling through to personalised outreach.

## What It Demonstrates

| Capability | Databricks Component | What It Does |
|---|---|---|
| Customer 360 | **Feature Tables** (Unity Catalog) | Unified customer profile — reads from UC tables via SQL |
| Propensity-to-Pay | **MLflow Model Serving** | Real-time ML scoring — likelihood of payment based on ~1,000 features |
| Best-Time-to-Contact | **MLflow Model Serving** | Optimal day/time/channel for outreach based on behavioural patterns |
| Payment Signals | **Structured Streaming** | Real-time events — DD cancellations, payment failures, partial payments |
| Collection Scorecard | **MCP Server** | Validation logic matching customers to strategy segments |
| Personalised Comms | **RAG** (Vector Search) | Tone-appropriate message generation — auto-selects friendly/empathetic/formal/sensitive |
| Open Banking | **Aggregator Integration** | Verified affordability data for compliant payment plan recommendations |
| Vulnerability Screening | **FCA Consumer Duty** | Automated vulnerability assessment before any escalation action |
| Approval Queue | **Unity Catalog** (Delta) | Human-in-the-loop review — comms require manager approval before sending |
| Agent Orchestration | **FMAPI + Tool-Calling** | LLM orchestrates all tools via streaming tool-calling loop |
| Audit Trail | **MLflow 3.0 Tracing** | Real spans emitted for every agent decision — full regulatory audit trail |
| Agent Memory | **PostgreSQL** | Persistent conversation history across sessions |
| Output Channels | **WhatsApp / SMS / Email** | Generated comms delivered via preferred channel |

## Architecture

![Architecture Diagram](docs/oakbrook_architecture.png)

```
User Interface (Databricks App — FastAPI + SSE streaming)
    │
    ▼
AI Agent (LLM via FMAPI — tool-calling loop)
    │
    ├─→ ML Models (MLflow Model Serving)
    │     ├── Propensity-to-Pay
    │     └── Best-Time-to-Contact
    │
    ├─→ Data Lookup (UC Feature Tables + Structured Streaming)
    │     ├── Customer 360
    │     ├── Payment History
    │     └── Open Banking (affordability)
    │
    ├─→ Business Rules & Comms
    │     ├── Collection Scorecard (MCP Server)
    │     ├── Vulnerability Engine (FCA Consumer Duty)
    │     └── Comms Generator (RAG / Vector Search)
    │
    ├─→ Human-in-the-Loop
    │     └── Approval Queue (UC Delta table — manager review)
    │
    ├─→ Output Channels
    │     ├── WhatsApp (primary)
    │     ├── SMS
    │     └── Email (formal notices)
    │
    └─→ Observability + Memory
          ├── MLflow 3.0 Tracing (audit trail)
          ├── Unity Catalog (governance, lineage)
          └── Agent Memory (PostgreSQL)

Data Sources (Lakeflow Connect)
    ├── Loan Management System (CDC)
    ├── Payment Processor (CDC)
    ├── Customer Contacts (Streaming)
    ├── SFTP (Collections Providers)
    └── Open Banking API (affordability)
```

## Quick Start

### 1. Set Up Feature Tables

Run the setup notebook to create Unity Catalog tables with synthetic data:

```
notebooks/01_setup_feature_tables.py
```

This creates `customer_360`, `payment_history`, and `open_banking_data` tables in your configured catalog/schema.

### 2. Train and Deploy Models (Optional)

```
notebooks/02_train_and_register_models.py   # Train PTP + BTC models, register in MLflow
notebooks/03_deploy_model_serving.py        # Deploy to Model Serving endpoints
```

### 3. Deploy to Databricks

```bash
# Authenticate
databricks auth login --host https://YOUR-WORKSPACE.databricks.com --profile your-profile

# Create the app
databricks apps create --profile your-profile --json '{"name": "collections-brain"}'

# Sync and deploy
databricks sync . /Workspace/Users/YOU@company.com/apps/collections-brain --profile your-profile --watch=false
databricks apps deploy collections-brain \
  --source-code-path /Workspace/Users/YOU@company.com/apps/collections-brain \
  --profile your-profile
```

### 4. Run Locally

```bash
pip install -r requirements.txt
DATABRICKS_HOST=YOUR-WORKSPACE.databricks.com DATABRICKS_TOKEN=your-token \
  uvicorn backend.main:app --port 8000
# Open http://localhost:8000
```

## Demo Walkthrough

The app has three tabs:

### Agent Chat
1. **Portfolio sidebar** — Customers with risk segments, days past due, and balances
2. **Click a customer** — Pre-fills analysis prompt
3. **Streaming response** — Tool execution chips appear in real-time, then text streams token-by-token
4. **Agent trace** — Expandable panel shows every tool call with Databricks component mapping
5. **Generate comms** — Draft WhatsApp/SMS/email with auto-selected tone
6. **Submit for approval** — Comms go to the approval queue for manager review before sending

### Architecture
- System diagram showing all layers and data flows
- Data flow description from ingestion through to output channels

### How to Build This
- Component cards with cloud-specific documentation links
- Availability matrix for your region
- Production roadmap

### Sample Prompts

- "Show me the full portfolio and prioritise by risk"
- "Run a full analysis on C-10001 — models, scorecard, vulnerability, and strategy"
- "Check Open Banking affordability for C-10002 and draft a WhatsApp message"
- "Assess vulnerability for C-10003 and recommend a sensitive approach"
- "Generate a weekly contact plan for all customers"

## Data Layer

The app reads from **Unity Catalog Feature Tables** when running on Databricks, with local fallback for development:

| Table | Description |
|---|---|
| `customer_360` | Customer profiles — credit cards + debt consolidation loans |
| `payment_history` | Payment events (paid, missed, DD cancelled, partial) |
| `open_banking_data` | Affordability data for customers who have consented |
| `approval_queue` | Human-in-the-loop comms approval (UC Delta table) |

Additional in-memory data:
- **Collection scorecard** — Strategy segments mapping to actions (served via MCP Server pattern)
- **Communication templates** — Templates across tones (served via RAG pattern)
- **Vulnerability rules** — FCA Consumer Duty compliance logic

## Configuration

### app.yaml Resources

The Databricks App is configured with these resources in `app.yaml`:

| Resource | Type | Purpose |
|---|---|---|
| `serving_endpoint` | Serving Endpoint | LLM (FMAPI) for agent orchestration |
| `ptp_model` | Serving Endpoint | Propensity-to-Pay model |
| `btc_model` | Serving Endpoint | Best-Time-to-Contact model |

Update the endpoint names in `app.yaml` to match your workspace.

### Environment Variables

| Variable | Description |
|---|---|
| `DATABRICKS_HOST` | Workspace URL (auto-set when deployed as Databricks App) |
| `DATABRICKS_TOKEN` | Auth token (auto-set when deployed as Databricks App) |

## File Structure

```
├── app.yaml                  # Databricks App config (serving endpoint resources)
├── requirements.txt          # Python deps (fastapi, openai, mlflow, databricks-sdk)
├── backend/
│   ├── __init__.py
│   ├── main.py               # FastAPI routes (streaming + non-streaming chat, approvals)
│   ├── agent.py              # AI agent with tool-calling loop + MLflow 3.0 tracing
│   ├── data.py               # UC Feature Table reads (SQL connector) + local fallback
│   ├── memory.py             # Conversation memory (PostgreSQL / in-memory fallback)
│   └── approvals.py          # Approval queue (UC Delta table — human-in-the-loop)
├── frontend/
│   └── index.html            # 3-tab UI: Agent Chat, Architecture, How to Build This
├── notebooks/
│   ├── 01_setup_feature_tables.py       # Create UC tables with synthetic data
│   ├── 02_train_and_register_models.py  # Train and register PTP + BTC models
│   └── 03_deploy_model_serving.py       # Deploy models to serving endpoints
└── docs/
    ├── oakbrook_architecture.mmd        # Mermaid source
    ├── oakbrook_architecture.png        # Architecture diagram (PNG)
    └── oakbrook_architecture.svg        # Architecture diagram (SVG)
```

## Production Roadmap

To move from demo to production:

1. **Data Ingestion** — Materialise Customer 360 from loan management systems (Lakeflow Connect CDC). DLT pipelines for Bronze → Silver → Gold.
2. **Feature Engineering** — Create Feature Tables in UC with ~1,000 features from historical collections data. Include Open Banking signals, contact outcomes, behavioural patterns.
3. **Model Training** — Train propensity-to-pay and best-time-to-contact using MLflow experiment tracking. Register in MLflow Model Registry.
4. **Model Serving** — Deploy to real-time Model Serving endpoints with Feature Table auto-lookup. A/B testing between versions.
5. **Agent + Observability** — Build agent using FMAPI + tool-calling. MCP Server for scorecard. Vector Search for RAG comms. MLflow 3.0 tracing for full regulatory audit trail.
6. **Production App** — Databricks App with workspace SSO. Approval queue for human-in-the-loop governance. WhatsApp Business API for outreach delivery. PostgreSQL for session memory.
