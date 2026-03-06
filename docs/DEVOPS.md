# DevOps & Observability

## Observability Stack

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      OBSERVABILITY ARCHITECTURE                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  TRACING & DEBUGGING                                                        │
│  ├── LangSmith — LangGraph trace visualization, prompt debugging            │
│  ├── Snowflake CORTEX_CALL_LOGS — All Cortex API calls logged              │
│  └── Orchestrator UI — Real-time execution monitor                          │
│                                                                             │
│  ML EXPERIMENT TRACKING                                                     │
│  ├── Snowflake ML Registry — Model versioning & metadata                    │
│  └── LangSmith — Evaluation datasets for LLM quality                        │
│                                                                             │
│  STATE MANAGEMENT                                                           │
│  ├── LangGraph Checkpointer → Snowflake Tables                              │
│  ├── AGENT_EXECUTION_STATE — Per-agent state                                │
│  └── AGENT_ARTIFACTS — Created resources                                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## LangSmith Integration

### Setup

```bash
export LANGCHAIN_TRACING_V2=true
export LANGCHAIN_API_KEY=<your-key>
export LANGCHAIN_PROJECT=snowflake-agentic-platform
```

### Tracing Configuration

```python
from langsmith import traceable
from langgraph.graph import StateGraph

@traceable(run_type="chain", name="meta_agent_plan")
def generate_execution_plan(use_case: str, data_paths: list[str]):
    # Planning logic
    pass
```

### What Gets Traced

| Component | Trace Type | Captured Data |
|-----------|------------|---------------|
| Meta-Agent planning | chain | Use case, data analysis, generated plan |
| Sub-agent execution | chain | Input state, output state, duration |
| Cortex LLM calls | llm | Prompt, response, tokens, latency |
| Cortex Search queries | retriever | Query, results, scores |
| Validation checks | chain | Check type, pass/fail, errors |

## Testing Strategy

### Test Levels

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  UNIT TESTS (pytest)                                                        │
│  ├── Individual agent logic with mocked Snowflake                          │
│  ├── State transition validation                                           │
│  └── Tool function correctness                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│  INTEGRATION TESTS                                                          │
│  ├── Multi-agent chains with test data                                     │
│  ├── Checkpoint/resume functionality                                       │
│  └── Error recovery and retry logic                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│  LANGSMITH EVALUATIONS                                                      │
│  ├── LLM output quality (planning accuracy)                                │
│  ├── Code generation correctness                                           │
│  └── Semantic model validity                                               │
├─────────────────────────────────────────────────────────────────────────────┤
│  END-TO-END TESTS                                                           │
│  ├── Full pipeline with sample use cases                                   │
│  ├── Generated app functionality                                           │
│  └── SPCS deployment verification                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

### LangSmith Evaluation Datasets

Create evaluation datasets for critical LLM tasks:

| Dataset | Purpose | Metrics |
|---------|---------|---------|
| `use_case_parsing` | Validate use case → intent extraction | Accuracy, completeness |
| `plan_generation` | Validate execution plan correctness | Valid DAG, agent selection |
| `code_generation` | Validate generated code quality | Syntax valid, functional |
| `semantic_model` | Validate semantic model generation | Query accuracy |

## CI/CD Pipeline

### GitHub Actions Workflow

```yaml
# .github/workflows/test.yml
name: Test
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install -e ".[dev]"
      - run: pytest tests/unit -v
      - run: pytest tests/integration -v --snowflake-connection=${{ secrets.SF_CONNECTION }}
```

```yaml
# .github/workflows/deploy-dev.yml
name: Deploy to DEV
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Build container
        run: docker build -t agentic-platform:${{ github.sha }} .
      - name: Push to Snowflake
        run: |
          snow spcs image-registry login
          docker tag agentic-platform:${{ github.sha }} <registry>/agentic-platform:dev
          docker push <registry>/agentic-platform:dev
      - name: Deploy service
        run: snow spcs service create-or-update --spec deployment/service_spec_dev.yaml
```

### Deployment Environments

| Environment | Schema | Trigger | Purpose |
|-------------|--------|---------|---------|
| DEV | `AGENTIC_DB.DEV` | Push to main | Development testing |
| STAGING | `AGENTIC_DB.STAGING` | Tag `rc-*` | Pre-release validation |
| PROD | `AGENTIC_DB.PROD` | Tag `v*.*.*` | Production |

## Agent Versioning

### Git Tag Strategy

```
v1.0.0          # Major release
v1.1.0          # New agent added
v1.1.1          # Bug fix
rc-1.2.0        # Release candidate
```

### Agent Registry Versioning

Each agent in the registry has a version:

```yaml
agent_id: parquet_processor
version: "1.2.0"
compatible_platform: ">=1.0.0"
```

## Monitoring & Alerts

### Key Metrics

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| Execution duration | LangSmith | >30 min |
| LLM error rate | CORTEX_CALL_LOGS | >5% |
| Validation failures | AGENT_PHASE_STATE | >3 retries |
| Checkpoint failures | LANGGRAPH_CHECKPOINTS | Any |

### Snowflake Queries for Monitoring

```sql
-- Active executions
SELECT plan_id, status, DATEDIFF('minute', created_at, CURRENT_TIMESTAMP()) as duration_min
FROM AGENT_EXECUTION_PLANS
WHERE status = 'running';

-- Failed phases (last 24h)
SELECT phase_name, COUNT(*) as failures, AVG(retry_count) as avg_retries
FROM AGENT_PHASE_STATE
WHERE status = 'failed' AND started_at > DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY phase_name;

-- Cortex call costs
SELECT call_type, model, SUM(input_tokens + output_tokens) as total_tokens
FROM CORTEX_CALL_LOGS
WHERE created_at > DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY call_type, model;
```
