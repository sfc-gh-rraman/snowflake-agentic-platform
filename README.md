# CoCo Healthcare Skills Orchestrator

> Agentic platform for deploying healthcare AI solutions on Snowflake in minutes, not months.

Built with **LangGraph** + **Snowflake Cortex** + **Langfuse** + **ReactFlow**

---

## What It Does

A natural language prompt like _"Analyze adverse events for our cardiovascular drug portfolio"_ triggers an end-to-end agentic pipeline that:

1. **Routes** to the correct healthcare scenario (Clinical Data, Drug Safety, Clinical Docs, ML Risk)
2. **Generates** an execution plan with skill assignments
3. **Gates** the plan for human review (approve/modify/reject steps)
4. **Executes** Snowflake objects (Dynamic Tables, Cortex Search, ML models, Cortex Agents)
5. **Traces** every step in Langfuse with full LLM observability

---

## Architecture

```
+-------------------------------------------------------------------+
|                        User Interface                              |
|   React + ReactFlow + Zustand + Vega-Lite                         |
|   (Scenario cards, DAG, Plan Gate modal, AI Chat, Skills Catalog) |
+-------------------------------------------------------------------+
        |                           |
        v                           v
+-------------------+   +-------------------+
|  FastAPI Backend  |   |   Nginx (8080)    |
|  uvicorn :8000    |<--|   reverse proxy   |
+-------------------+   +-------------------+
        |
        v
+-------------------------------------------------------------------+
|                  LangGraph Orchestration Engine                     |
|  StateGraph | MemorySaver | interrupt_before (Plan Gate)           |
|  Parallel execution | Checkpoint/resume                            |
+-------------------------------------------------------------------+
        |                           |
        v                           v
+-------------------+   +-------------------+
| Langfuse Tracing  |   | Snowflake Platform|
| Traces, Spans,    |   | Dynamic Tables    |
| Generations, Cost |   | Cortex Search     |
+-------------------+   | Cortex Agent      |
                        | Cortex Analyst    |
                        | ML Registry       |
                        | SPCS Services     |
                        +-------------------+
```

---

## Scenarios

| Scenario | Skills Used | What Gets Built |
|----------|------------|-----------------|
| **Clinical Data Warehouse** | Dynamic Tables, ML, Cortex Search, Cortex Analyst, React | Patient 360 view with risk model, search service, semantic model, dashboard |
| **Drug Safety Signal Detection** | FAERS data generation, PRR/ROR analysis, Streamlit | Pharmacovigilance pipeline with signal detection dashboard |
| **Clinical Document Intelligence** | Doc generation, NLP extraction, Cortex Search, Cortex Agent | Document search index and intelligence agent |

---

## Quick Start

### Prerequisites

- Snowflake account with **Cortex AI** enabled
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/index) (`snow`) installed
- Docker (for SPCS deployment)
- Node.js 18+ and Python 3.11+
- A named Snowflake connection configured (`snow connection add`)

### 1. Setup Snowflake Objects

```bash
# Run the setup SQL (creates database, schemas, compute pool, stages)
snow sql -f orchestrator/deploy/setup.sql -c my_snowflake
```

### 2. Deploy to SPCS (Recommended)

```bash
cd orchestrator
./deploy/deploy.sh
```

This will:
- Build the Docker image (frontend + backend + nginx)
- Push to Snowflake image repository
- Create the SPCS service
- Print the endpoint URL

**Customize deployment:**
```bash
DATABASE=MY_DB SCHEMA=MY_SCHEMA COMPUTE_POOL=MY_POOL CONNECTION=my_conn ./deploy/deploy.sh
```

### 3. Local Development

```bash
# Terminal 1: Backend
cd orchestrator/backend
pip install -r requirements.txt
SNOWFLAKE_CONNECTION_NAME=my_snowflake python -m uvicorn api.main:app \
  --host 0.0.0.0 --port 8000 --reload --reload-dir . \
  --app-dir /path/to/snowflake-agentic-platform/orchestrator/backend

# Terminal 2: Frontend
cd orchestrator/frontend
npm install
npm run dev
```

Open http://localhost:5173

---

## Plan Gate (Human-in-the-Loop)

After the plan phase completes, the orchestrator pauses and shows a **Plan Gate Modal**:

- Review all execution steps with skill badges
- Toggle individual steps on/off
- Approve to proceed or Reject to cancel
- Skipped steps are marked as `SKIPPED` in the DAG

The gate is powered by LangGraph's `interrupt_before` mechanism with checkpoint/resume.

---

## Skills Catalog (GitHub Integration)

The **Skills** tab in the right panel pulls the skill catalog from the companion GitHub repo:

```
https://github.com/Snowflake-Solutions/health-sciences-coco-skills-incubator
```

Set `GITHUB_TOKEN` env var for private repo access. Falls back to a built-in catalog if the API is unreachable.

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SNOWFLAKE_CONNECTION_NAME` | Named Snowflake connection | `my_snowflake` |
| `LANGFUSE_PUBLIC_KEY` | Langfuse public key (optional) | - |
| `LANGFUSE_SECRET_KEY` | Langfuse secret key (optional) | - |
| `LANGFUSE_HOST` | Langfuse host URL | `https://cloud.langfuse.com` |
| `GITHUB_TOKEN` | GitHub PAT for private skills repo | - |
| `SKILLS_GITHUB_REPO` | Skills repo `owner/name` | `Snowflake-Solutions/health-sciences-coco-skills-incubator` |

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/scenarios` | GET | List available scenarios |
| `/api/workflow` | GET | Current workflow state |
| `/api/workflow/start` | POST | Start workflow with scenario |
| `/api/workflow/start-nl` | POST | Start via natural language routing |
| `/api/workflow/approve` | POST | Approve/reject plan gate |
| `/api/workflow/approval-status` | GET | Check if awaiting approval |
| `/api/workflow/reset` | POST | Reset workflow |
| `/api/workflow/task/{id}` | GET | Task details |
| `/api/workflow/task/{id}/retry` | POST | Retry failed task |
| `/api/workflow/logs` | GET | Execution logs |
| `/api/workflow/langfuse` | GET | Langfuse trace info + costs |
| `/api/workflow/costs` | GET | Real Snowflake credit usage |
| `/api/workflow/data-freshness` | GET | Dynamic Table refresh status |
| `/api/skills/catalog` | GET | Skills from GitHub repo |
| `/api/chat/stream` | POST | Cortex Agent SSE chat |
| `/ws` | WebSocket | Real-time updates |

---

## Directory Structure

```
snowflake-agentic-platform/
+-- orchestrator/
|   +-- backend/
|   |   +-- api/              # FastAPI routes, WebSocket, chat SSE
|   |   +-- engine/           # LangGraph StateGraph, state management, Langfuse
|   |   +-- tasks/            # Scenario task implementations (SQL, Cortex calls)
|   |   +-- services/         # Cortex Agent client
|   |   +-- config.py         # Runtime configuration
|   |   +-- requirements.txt  # Python dependencies
|   +-- frontend/
|   |   +-- src/
|   |   |   +-- components/   # React components (18 total)
|   |   |   +-- hooks/        # useWebSocket
|   |   |   +-- stores/       # Zustand workflowStore
|   |   |   +-- types/        # TypeScript interfaces
|   |   +-- package.json
|   +-- deploy/
|   |   +-- Dockerfile        # Multi-stage (node build + python runtime + nginx)
|   |   +-- nginx.conf        # Reverse proxy config
|   |   +-- supervisord.conf  # Process manager (nginx + uvicorn)
|   |   +-- deploy.sh         # One-command SPCS deployment
|   |   +-- setup.sql         # Database/schema/compute pool setup
|   +-- cortex/
|   |   +-- health_semantic_model.yaml  # Cortex Analyst semantic model
+-- docs/
|   +-- EXECUTIVE_VISION.md   # Pitch deck (markdown slides)
|   +-- ARCHITECTURE.md       # Technical architecture
|   +-- DEMO_QA.md            # Stakeholder Q&A from demo
|   +-- DEVOPS.md             # Deployment operations guide
+-- .gitignore
+-- pyproject.toml
+-- README.md                 # This file
```

---

## Snowflake Objects Created

| Schema | Object | Type |
|--------|--------|------|
| `ORCHESTRATOR` | `HEALTH_ORCHESTRATOR` | SPCS Service |
| `ORCHESTRATOR` | `HEALTH_COPILOT_AGENT` | Cortex Agent |
| `ORCHESTRATOR` | `ORCHESTRATOR_DISPATCHER_AGENT` | Cortex Agent |
| `ORCHESTRATOR` | `DISPATCH_WORKFLOW_FN` | UDF (tool for dispatcher agent) |
| `FHIR_DEMO` | `PATIENT`, `OBSERVATION`, `CONDITION` | Tables |
| `ANALYTICS` | `PATIENT_ENRICHED`, `OBSERVATION_ENRICHED`, `CONDITION_ENRICHED`, `PATIENT_360` | Dynamic Tables |
| `ML` | `RISK_TRAINING_DATA`, `RISK_PREDICTIONS`, `PATIENT_RISK_CLASSIFIER` | Tables / ML Model |
| `CORTEX` | `CLINICAL_PATIENT_SEARCH` | Cortex Search Service |
| `CORTEX` | `SEMANTIC_MODELS` (stage) | Cortex Analyst Model |
| `DRUG_SAFETY` | `FAERS_DEMO`, `SIGNAL_DETECTION` | Tables |
| `CLINICAL_DOCS` | `DOCUMENT_REGISTRY`, `EXTRACTED_FIELDS`, `CLINICAL_DOC_SEARCH` | Tables / Search |

---

## Key Design Decisions

1. **Cortex Agent custom tools can ONLY call UDFs** (not stored procedures). The dispatcher agent uses `DISPATCH_WORKFLOW_FN` as a SQL UDF.
2. **LangGraph `interrupt_before`** powers the Plan Gate. The interrupt fires before the first execute-phase node.
3. **No readiness probe** in SPCS service spec (nginx + supervisord causes PENDING loop).
4. **SSE + WebSocket dual channel** for real-time updates. SSE for workflow state polling, WebSocket for task-level events.
5. **Langfuse is optional** but recommended. Set env vars to enable full tracing.

---

## Troubleshooting

```sql
-- Check service status
SHOW SERVICES LIKE 'HEALTH_ORCHESTRATOR' IN SCHEMA AGENTIC_PLATFORM.ORCHESTRATOR;

-- Get logs
SELECT SYSTEM$GET_SERVICE_LOGS('AGENTIC_PLATFORM.ORCHESTRATOR.HEALTH_ORCHESTRATOR', 0, 'orchestrator', 100);

-- Get endpoint URL
SHOW ENDPOINTS IN SERVICE AGENTIC_PLATFORM.ORCHESTRATOR.HEALTH_ORCHESTRATOR;

-- Restart service
ALTER SERVICE AGENTIC_PLATFORM.ORCHESTRATOR.HEALTH_ORCHESTRATOR SUSPEND;
ALTER SERVICE AGENTIC_PLATFORM.ORCHESTRATOR.HEALTH_ORCHESTRATOR RESUME;
```
