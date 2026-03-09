# Agentic Platform Orchestrator

Visual workflow orchestrator for deploying AI applications on Snowflake. Runs as an SPCS service inside your Snowflake account.

## Features

- **Visual DAG** - Real-time workflow visualization with ReactFlow
- **WebSocket Updates** - Live progress and log streaming
- **Langfuse Tracing** - Full LLM observability and cost tracking
- **Snowflake Persistence** - State saved to Snowflake tables
- **SPCS Deployment** - Runs inside your Snowflake account

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    SPCS Service                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   Nginx      │───▶│   FastAPI    │───▶│  Snowflake   │  │
│  │   (8080)     │    │   Backend    │    │  Session     │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│         │                   │                              │
│         │                   ▼                              │
│         │           ┌──────────────┐                       │
│         │           │   Langfuse   │                       │
│         │           │   Tracing    │                       │
│         │           └──────────────┘                       │
│         ▼                                                   │
│  ┌──────────────┐                                          │
│  │    React     │                                          │
│  │   Frontend   │                                          │
│  └──────────────┘                                          │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### Deploy to SPCS (Recommended)

```bash
# Using CLI
agentic-platform orchestrator

# Or using deploy script directly
cd orchestrator
./deploy/deploy.sh
```

### Local Development

```bash
# Backend
cd orchestrator/backend
pip install -r requirements.txt
uvicorn server:app --reload --port 8000

# Frontend (separate terminal)
cd orchestrator/frontend
npm install
npm run dev
```

Open http://localhost:5173

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SNOWFLAKE_CONNECTION_NAME` | Snowflake connection name | `default` |
| `LANGFUSE_PUBLIC_KEY` | Langfuse public key | - |
| `LANGFUSE_SECRET_KEY` | Langfuse secret key | - |
| `LANGFUSE_HOST` | Langfuse host URL | `https://cloud.langfuse.com` |

### Deploy Options

```bash
# Custom database/schema
DATABASE=MY_DB SCHEMA=MY_SCHEMA ./deploy/deploy.sh

# Custom compute pool
COMPUTE_POOL=MY_POOL ./deploy/deploy.sh

# Specific connection
CONNECTION=my_connection ./deploy/deploy.sh
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/workflow` | GET | Get current workflow state |
| `/api/workflow/start` | POST | Start workflow execution |
| `/api/workflow/reset` | POST | Reset workflow to initial state |
| `/api/workflow/task/{id}` | GET | Get task details |
| `/api/workflow/task/{id}/retry` | POST | Retry a failed task |
| `/api/workflow/logs` | GET | Get all execution logs |
| `/ws` | WebSocket | Real-time updates |
| `/health` | GET | Health check |

## Workflow Tasks

| Task | Phase | Description |
|------|-------|-------------|
| `scan_sources` | Discovery | Identify tables and schemas |
| `profile_schema` | Discovery | Analyze column statistics |
| `process_structured` | Preprocessing | Load Parquet/CSV data |
| `process_documents` | Preprocessing | Chunk documents |
| `deploy_search` | Cortex | Create Cortex Search service |
| `deploy_semantic` | Cortex | Create semantic model |
| `feature_engineering` | ML | Generate features |
| `train_models` | ML | Train ML models |
| `register_models` | ML | Register in ML Registry |
| `generate_app` | Deployment | Generate app code |
| `deploy_spcs` | Deployment | Deploy to SPCS |

## Langfuse Integration

All LangGraph execution is traced to Langfuse:

1. **Traces** - One trace per workflow execution
2. **Spans** - One span per task
3. **Events** - Log messages within tasks
4. **Metrics** - Duration, success/failure

View traces at: https://cloud.langfuse.com

## Directory Structure

```
orchestrator/
├── backend/
│   ├── api/           # FastAPI routes + WebSocket
│   ├── engine/        # Workflow executor + state
│   ├── tasks/         # Task implementations
│   └── server.py      # Entry point
├── frontend/
│   ├── src/
│   │   ├── components/  # React components
│   │   ├── hooks/       # WebSocket hook
│   │   ├── stores/      # Zustand store
│   │   └── types/       # TypeScript types
│   └── package.json
└── deploy/
    ├── Dockerfile       # Multi-stage build
    ├── nginx.conf       # Reverse proxy config
    ├── supervisord.conf # Process manager
    ├── deploy.sh        # SPCS deployment script
    └── setup.sql        # Database setup
```

## Troubleshooting

### Service won't start
```sql
SELECT SYSTEM$GET_SERVICE_LOGS('DB.SCHEMA.SERVICE', 0, 'orchestrator', 100);
```

### Check service status
```sql
SHOW SERVICES LIKE 'AGENTIC_ORCHESTRATOR';
DESCRIBE SERVICE DB.SCHEMA.AGENTIC_ORCHESTRATOR;
```

### Get endpoint URL
```sql
SHOW ENDPOINTS IN SERVICE DB.SCHEMA.AGENTIC_ORCHESTRATOR;
```
