# Work Execution Plan - Snowflake Agentic Platform

## Overview

This execution plan maps directly to `docs/ARCHITECTURE.md` and defines how Cortex Code sub-agents will divide and execute the implementation work.

---

## Phase 1: Foundation Layer (Week 1)

### 1.1 State Management Infrastructure
**Sub-Agent**: `generalPurpose` (readonly=false)  
**Task Division**: Sequential (dependencies exist)

| Task | Description | Output Artifacts |
|------|-------------|------------------|
| 1.1.1 | Create Snowflake database with medallion schemas (RAW, CURATED, ML, ANALYTICS) | `src/state/schema_setup.sql` |
| 1.1.2 | Create state tables: AGENT_EXECUTION_PLANS, AGENT_PHASE_STATE, AGENT_EXECUTION_STATE, LANGGRAPH_CHECKPOINTS, AGENT_ARTIFACTS, CORTEX_CALL_LOGS | `src/state/tables.sql` |
| 1.1.3 | Implement LangGraph checkpoint saver for Snowflake | `src/state/snowflake_checkpoint.py` |
| 1.1.4 | Create state accessor utilities (read/write/update) | `src/state/state_manager.py` |

**Validation Criteria**:
- [ ] All 6 state tables exist and are queryable
- [ ] Checkpoint save/load round-trips successfully
- [ ] State manager CRUD operations work

---

### 1.2 Agent Registry (Capability Catalog)
**Sub-Agent**: `generalPurpose` (readonly=false)  
**Task Division**: Parallel after schema

| Task | Description | Output Artifacts |
|------|-------------|------------------|
| 1.2.1 | Define agent capability YAML schema | `src/agents/registry/schema.yaml` |
| 1.2.2 | Create agent capability chunk table | `src/agents/registry/tables.sql` |
| 1.2.3 | Create Cortex Search service over agent capabilities | `src/agents/registry/search_service.sql` |
| 1.2.4 | Implement registry query tool for meta-agent | `src/agents/registry/registry_query.py` |
| 1.2.5 | Seed initial agent definitions (parquet_processor, document_chunker, ml_model_builder, app_code_generator) | `src/agents/registry/seed_agents.py` |

**Agent Capability Schema** (from ARCHITECTURE.md):
```yaml
agent_id: string
name: string
version: string
description: string
capabilities:
  - capability_id: string
    name: string
    input_types: [string]
    output_types: [string]
    constraints: object
dependencies:
  - agent_id: string
    relationship: string
triggers:
  - condition: string
    priority: number
```

**Validation Criteria**:
- [ ] Cortex Search returns relevant agents for "process parquet files"
- [ ] Agent definitions contain all required fields
- [ ] Dependencies correctly linked

---

## Phase 2: Meta-Agent (Planner) (Week 2)

### 2.1 Meta-Agent Core
**Sub-Agent**: `generalPurpose` (readonly=false)  
**Task Division**: Sequential (LangGraph graph construction)

| Task | Description | Output Artifacts |
|------|-------------|------------------|
| 2.1.1 | Define LangGraph state schema (TypedDict) | `src/meta_agent/state.py` |
| 2.1.2 | Implement use_case_parser tool | `src/meta_agent/tools/use_case_parser.py` |
| 2.1.3 | Implement data_scanner tool | `src/meta_agent/tools/data_scanner.py` |
| 2.1.4 | Implement agent_registry_query tool | `src/meta_agent/tools/agent_registry_query.py` |
| 2.1.5 | Implement plan_generator tool | `src/meta_agent/tools/plan_generator.py` |
| 2.1.6 | Build LangGraph graph with nodes and edges | `src/meta_agent/graph.py` |
| 2.1.7 | Add human-in-the-loop approval node | `src/meta_agent/approval.py` |

**LangGraph State** (from ARCHITECTURE.md):
```python
class MetaAgentState(TypedDict):
    use_case_description: str
    data_assets: List[DataAsset]
    parsed_requirements: ParsedRequirements
    data_profile: DataProfile
    available_agents: List[AgentCapability]
    execution_plan: ExecutionPlan
    approval_status: Literal["pending", "approved", "rejected"]
    execution_state: ExecutionState
```

**Graph Flow**:
```
START → parse_use_case → scan_data → query_registry → generate_plan → human_approval → execute_plan → END
```

**Validation Criteria**:
- [ ] LangGraph compiles without errors
- [ ] Tools return structured outputs
- [ ] Execution plan JSON matches schema in ARCHITECTURE.md
- [ ] Checkpoints persist to Snowflake

---

## Phase 3: Discovery Agents (Week 2-3)

### 3.1 File Scanner Agent
**Sub-Agent**: `explore` (readonly=true for analysis)  
**Task Division**: Parallel with Schema Profiler

| Task | Description | Output Artifacts |
|------|-------------|------------------|
| 3.1.1 | Implement stage file listing (Snowflake stages) | `src/agents/discovery/file_scanner.py` |
| 3.1.2 | Implement file type detection (parquet, csv, pdf, json) | (same file) |
| 3.1.3 | Generate file inventory with metadata | (same file) |
| 3.1.4 | Write discovered files to AGENT_ARTIFACTS | (same file) |

### 3.2 Schema Profiler Agent
**Sub-Agent**: `explore` (readonly=true)

| Task | Description | Output Artifacts |
|------|-------------|------------------|
| 3.2.1 | Implement parquet schema inference | `src/agents/discovery/schema_profiler.py` |
| 3.2.2 | Implement column statistics (nulls, uniques, ranges) | (same file) |
| 3.2.3 | Detect data types and semantic types | (same file) |
| 3.2.4 | Generate data profile report | (same file) |

**Validation Criteria**:
- [ ] Scanner detects all file types in test stage
- [ ] Profiler generates accurate statistics
- [ ] Results written to state tables

---

## Phase 4: Preprocessing Agents (Week 3)

### 4.1 Parquet Processor Agent
**Sub-Agent**: `generalPurpose` (readonly=false)  
**Task Division**: Sequential (state machine)

**State Machine** (from ARCHITECTURE.md):
```
SCAN → SCHEMA_INFER → PROFILE → QUALITY_CHECK → TRANSFORM → LOAD
```

| Task | Description | Sub-State |
|------|-------------|-----------|
| 4.1.1 | Implement SCAN state - list parquet files | `src/agents/preprocessing/parquet_processor.py` |
| 4.1.2 | Implement SCHEMA_INFER state - read schemas | (same file) |
| 4.1.3 | Implement PROFILE state - statistics | (same file) |
| 4.1.4 | Implement QUALITY_CHECK state - basic validation | (same file) |
| 4.1.5 | Implement TRANSFORM state - type coercion, renaming | (same file) |
| 4.1.6 | Implement LOAD state - COPY INTO tables | (same file) |
| 4.1.7 | Build LangGraph sub-graph for state machine | `src/agents/preprocessing/parquet_graph.py` |

**Key Patterns** (from drilling_ops):
```python
# Parquet → Table via Snowpark
df = session.read.parquet(f"@{stage}/{file}")
df.write.mode("overwrite").save_as_table(f"{schema}.{table}")
```

### 4.2 Document Chunker Agent
**Sub-Agent**: `generalPurpose` (readonly=false)  
**Task Division**: Sequential (state machine)

**State Machine** (from ARCHITECTURE.md):
```
EXTRACT → ANALYZE_STRUCTURE → CHUNK → ENRICH_METADATA → LOAD_CHUNKS
```

| Task | Description | Sub-State |
|------|-------------|-----------|
| 4.2.1 | Implement EXTRACT state - PDF text extraction | `src/agents/preprocessing/document_chunker.py` |
| 4.2.2 | Implement ANALYZE_STRUCTURE state - detect sections | (same file) |
| 4.2.3 | Implement CHUNK state - semantic chunking | (same file) |
| 4.2.4 | Implement ENRICH_METADATA state - add context | (same file) |
| 4.2.5 | Implement LOAD_CHUNKS state - insert to chunk table | (same file) |
| 4.2.6 | Build LangGraph sub-graph | `src/agents/preprocessing/document_graph.py` |

**Chunk Table Schema** (from drilling_ops):
```sql
CREATE TABLE DOCUMENT_CHUNKS (
    chunk_id NUMBER AUTOINCREMENT PRIMARY KEY,
    source_file VARCHAR(500),
    document_type VARCHAR(100),
    chunk VARCHAR(16000),
    metadata VARIANT,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

**Validation Criteria**:
- [ ] Parquet files load to tables with correct types
- [ ] PDFs chunk to <16K char segments
- [ ] Metadata preserved through pipeline

---

## Phase 5: Validation Framework (Week 3-4)

### 5.1 Validation Agent
**Sub-Agent**: `generalPurpose` (readonly=false)  
**Task Division**: Parallel (independent check suites)

**Check Suites** (from ARCHITECTURE.md):

| Suite | Checks | Task |
|-------|--------|------|
| Completeness | Required columns, min row count, coverage | 5.1.1 |
| Schema | Type consistency, naming conventions | 5.1.2 |
| Quality | Null ratios, outliers, duplicates | 5.1.3 |
| Semantic | Business rule validation | 5.1.4 |
| ML-Specific | Feature distributions, label balance | 5.1.5 |

| Task | Description | Output Artifacts |
|------|-------------|------------------|
| 5.1.1 | Implement completeness validator | `src/agents/validation/completeness.py` |
| 5.1.2 | Implement schema validator | `src/agents/validation/schema.py` |
| 5.1.3 | Implement quality validator | `src/agents/validation/quality.py` |
| 5.1.4 | Implement semantic validator (Cortex LLM) | `src/agents/validation/semantic.py` |
| 5.1.5 | Implement ML validator | `src/agents/validation/ml_specific.py` |
| 5.1.6 | Build validation orchestrator with retry logic | `src/agents/validation/orchestrator.py` |

**Failure Handling**:
```
FAIL → LOG_ERROR → CHECK_RETRY_COUNT → 
  (count < max) → APPLY_FIX → RETRY
  (count >= max) → ESCALATE_TO_HUMAN
```

**Validation Criteria**:
- [ ] Each suite runs independently
- [ ] Retry logic works (test with intentional failures)
- [ ] Human escalation triggers correctly

---

## Phase 6: ML Pipeline (Week 4)

### 6.1 Feature Store Builder
**Sub-Agent**: `generalPurpose` (readonly=false)

| Task | Description | Output Artifacts |
|------|-------------|------------------|
| 6.1.1 | Implement feature discovery from data profile | `src/agents/ml/feature_store.py` |
| 6.1.2 | Implement window function feature engineering | (same file) |
| 6.1.3 | Create feature registry table | (same file) |

**Window Function Patterns** (from drilling_ops):
```python
window_spec = Window.partition_by("ENTITY_ID").order_by("TIMESTAMP").rows_between(-60, 0)
df = df.with_column("ROLLING_AVG", F.avg("VALUE").over(window_spec))
df = df.with_column("LAG_1", F.lag("VALUE", 1).over(window_spec))
```

### 6.2 ML Model Builder Agent
**Sub-Agent**: `generalPurpose` (readonly=false)  
**Task Division**: Sequential (state machine)

**State Machine** (from ARCHITECTURE.md):
```
TASK_CLASSIFICATION → FEATURE_SELECTION → TRAINING → EVALUATION → REGISTRATION → EXPLAINABILITY
```

| Task | Description | Sub-State |
|------|-------------|-----------|
| 6.2.1 | Implement TASK_CLASSIFICATION - detect regression/classification/clustering | `src/agents/ml/model_builder.py` |
| 6.2.2 | Implement FEATURE_SELECTION - correlation, importance | (same file) |
| 6.2.3 | Implement TRAINING - Snowpark ML pipelines | (same file) |
| 6.2.4 | Implement EVALUATION - metrics computation | (same file) |
| 6.2.5 | Implement REGISTRATION - log to ML Registry | (same file) |
| 6.2.6 | Implement EXPLAINABILITY - SHAP values | (same file) |
| 6.2.7 | Build LangGraph sub-graph | `src/agents/ml/model_graph.py` |

**Registry Pattern** (from drilling_ops):
```python
from snowflake.ml.registry import Registry

registry = Registry(session=session, database_name="DB", schema_name="ML")
model_version = registry.log_model(
    model_name="MODEL_NAME",
    version_name="v1",
    model=pipeline,
    sample_input_data=train_df.limit(100),
    metrics={"accuracy": 0.95, "f1": 0.93}
)
```

**SHAP Pattern** (from drilling_ops):
```python
import shap
explainer = shap.TreeExplainer(model.get_sklearn_object())
shap_values = explainer.shap_values(X_test.to_pandas())
```

**Validation Criteria**:
- [ ] Task classification correct for test datasets
- [ ] Models register to Snowflake ML Registry
- [ ] SHAP values compute without errors

---

## Phase 7: Search & Semantic (Week 4-5)

### 7.1 Cortex Search Builder
**Sub-Agent**: `generalPurpose` (readonly=false)

| Task | Description | Output Artifacts |
|------|-------------|------------------|
| 7.1.1 | Generate chunk table DDL from document profile | `src/agents/search/search_builder.py` |
| 7.1.2 | Create Cortex Search service | (same file) |
| 7.1.3 | Implement search query wrapper | (same file) |
| 7.1.4 | Register search service in AGENT_ARTIFACTS | (same file) |

**Search Service Pattern**:
```sql
CREATE OR REPLACE CORTEX SEARCH SERVICE {service_name}
    ON chunk
    ATTRIBUTES {filter_columns}
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 hour'
    EMBEDDING_MODEL = 'snowflake-arctic-embed-m-v1.5'
AS (SELECT * FROM {chunk_table});
```

### 7.2 Semantic Model Generator
**Sub-Agent**: `generalPurpose` (readonly=false)

| Task | Description | Output Artifacts |
|------|-------------|------------------|
| 7.2.1 | Profile tables for dimensions and facts | `src/agents/semantic/model_generator.py` |
| 7.2.2 | Generate semantic model YAML | (same file) |
| 7.2.3 | Generate verified queries | (same file) |
| 7.2.4 | Create semantic view | (same file) |

**Semantic YAML Structure** (from ARCHITECTURE.md):
```yaml
name: {model_name}
tables:
  - name: {table_name}
    base_table:
      database: {db}
      schema: {schema}
      table: {physical_table}
    dimensions:
      - name: {dim_name}
        expr: {column}
        data_type: {type}
    facts:
      - name: {fact_name}
        expr: {column}
        data_type: {type}
```

**Validation Criteria**:
- [ ] Search service returns relevant results
- [ ] Semantic model passes Cortex Analyst validation
- [ ] Verified queries execute successfully

---

## Phase 8: App Code Generation (Week 5-6)

### 8.1 App Code Generator Agent
**Sub-Agent**: `generalPurpose` (readonly=false)  
**Task Division**: Sequential (state machine)

**State Machine** (from ARCHITECTURE.md):
```
APP_SPEC_GENERATION → REACT_CODE_GENERATION → FASTAPI_CODE_GENERATION → DEPLOYMENT_CONFIG_GENERATION → TEST_AND_VALIDATE
```

| Task | Description | Sub-State |
|------|-------------|-----------|
| 8.1.1 | Implement APP_SPEC_GENERATION - requirements to spec | `src/agents/app_generation/code_generator.py` |
| 8.1.2 | Implement REACT_CODE_GENERATION - App.tsx, pages, components, hooks | (same file) |
| 8.1.3 | Implement FASTAPI_CODE_GENERATION - main.py, routes, Pydantic models | (same file) |
| 8.1.4 | Implement DEPLOYMENT_CONFIG_GENERATION - Dockerfile, nginx, SPCS spec | (same file) |
| 8.1.5 | Implement TEST_AND_VALIDATE - syntax check, type check | (same file) |
| 8.1.6 | Build LangGraph sub-graph | `src/agents/app_generation/app_graph.py` |

**Generated App Spec** (from ARCHITECTURE.md):
```json
{
  "app_name": "string",
  "pages": [
    {
      "name": "string",
      "route": "string",
      "components": ["DataTable", "Chart", "SearchBox"],
      "data_sources": [
        {"type": "sql", "query": "string"},
        {"type": "ml_model", "model_name": "string"},
        {"type": "cortex_search", "service_name": "string"}
      ]
    }
  ],
  "api_endpoints": [...],
  "auth_config": {...}
}
```

**Key Constraint**: Generated code, NOT templated. Each app is unique based on requirements.

**Validation Criteria**:
- [ ] Generated React compiles (npm run build)
- [ ] Generated FastAPI type-checks (mypy)
- [ ] SPCS spec is valid YAML

---

## Phase 9: Deployment (Week 6)

### 9.1 SPCS Deployer Agent
**Sub-Agent**: `generalPurpose` (readonly=false)

| Task | Description | Output Artifacts |
|------|-------------|------------------|
| 9.1.1 | Build Docker image | `src/agents/deployment/spcs_deployer.py` |
| 9.1.2 | Push to Snowflake Image Repository | (same file) |
| 9.1.3 | Create SPCS Service | (same file) |
| 9.1.4 | Wait for endpoint availability | (same file) |
| 9.1.5 | Run health check | (same file) |
| 9.1.6 | Register deployed service in AGENT_ARTIFACTS | (same file) |

**SPCS Commands**:
```bash
# Build and push
docker build -t {image_name}:{tag} .
docker tag {image_name}:{tag} {repo_url}/{image_name}:{tag}
docker push {repo_url}/{image_name}:{tag}

# Create service (via SQL)
CREATE SERVICE {service_name}
  IN COMPUTE POOL {pool}
  FROM SPECIFICATION $$...$$
```

**Validation Criteria**:
- [ ] Image pushes successfully
- [ ] Service reaches READY state
- [ ] Health endpoint returns 200

---

## Phase 10: Observability & Testing (Continuous)

### 10.1 LangSmith Integration
**Sub-Agent**: `generalPurpose` (readonly=false)

| Task | Description | Output Artifacts |
|------|-------------|------------------|
| 10.1.1 | Configure LangSmith tracing | `src/observability/langsmith_config.py` |
| 10.1.2 | Implement dual logging (LangSmith + CORTEX_CALL_LOGS) | `src/observability/dual_logger.py` |
| 10.1.3 | Create observability dashboards | `src/observability/dashboards.sql` |

### 10.2 Test Framework
**Sub-Agent**: `generalPurpose` (readonly=false)

| Task | Description | Output Artifacts |
|------|-------------|------------------|
| 10.2.1 | Unit tests for each agent | `tests/unit/` |
| 10.2.2 | Integration tests for agent chains | `tests/integration/` |
| 10.2.3 | LangSmith evaluation datasets | `tests/evals/` |
| 10.2.4 | E2E test harness | `tests/e2e/` |

---

## Phase 11: Orchestrator UI (Week 6-7)

### 11.1 Orchestrator App
**Sub-Agent**: `generalPurpose` (readonly=false)

| Task | Description | Output Artifacts |
|------|-------------|------------------|
| 11.1.1 | Adapt drilling_ops orchestrator frontend (React) | `src/app/frontend/` |
| 11.1.2 | Adapt drilling_ops orchestrator backend (FastAPI) | `src/app/backend/` |
| 11.1.3 | Replace hardcoded tasks with dynamic plan execution | (same files) |
| 11.1.4 | Add plan visualization (DAG view) | `src/app/frontend/src/components/PlanViewer.tsx` |
| 11.1.5 | Add human approval interface | `src/app/frontend/src/components/ApprovalDialog.tsx` |

**Key Adaptation**: The drilling_ops orchestrator is a working template. Adapt it to:
- Accept dynamic execution plans from Meta-Agent
- Display arbitrary agent DAGs (not fixed workflow)
- Support human-in-the-loop approval

---

## Sub-Agent Assignment Summary

| Sub-Agent Type | Tasks | When to Use |
|----------------|-------|-------------|
| `explore` (readonly) | 3.1, 3.2 | Discovery, analysis, profiling |
| `generalPurpose` (readonly=false) | All others | Code generation, file creation, SQL execution |

---

## Sub-Agent Delegation Strategy

### Delegation Model

Cortex Code uses the `runSubagent` tool to delegate tasks. Two agent types:

| Agent Type | `readonly` | Capabilities |
|------------|------------|--------------|
| `explore` | `true` | Read files, query Snowflake, analyze data - cannot modify |
| `generalPurpose` | `false` | Full access: create files, execute DDL, deploy services |

### Prompt Engineering for Sub-Agents

Each sub-agent receives a **detailed prompt** with:

1. **Context** - What phase we're in, what was completed before
2. **Task description** - Exactly what to build
3. **File paths** - Where to write output artifacts
4. **Patterns reference** - Point to skills for code patterns
5. **Return requirements** - What to report back

### Example Delegation Prompts

**Phase 1.1 - State Management**:
```
runSubagent(
  prompt="""
  CONTEXT: Building Snowflake Agentic Platform. Phase 1.1 - State Management.
  
  TASK: Create state management infrastructure for LangGraph agents.
  
  STEPS:
  1. Create database with medallion schemas (RAW, CURATED, ML, ANALYTICS)
  2. Create 6 state tables per ARCHITECTURE.md:
     - AGENT_EXECUTION_PLANS (plan_id, use_case, plan_json, status, timestamps)
     - AGENT_PHASE_STATE (phase_id, plan_id, phase_name, status, input/output)
     - AGENT_EXECUTION_STATE (execution_id, phase_id, agent_name, sub_state, error)
     - LANGGRAPH_CHECKPOINTS (checkpoint_id, thread_id, checkpoint_data, timestamp)
     - AGENT_ARTIFACTS (artifact_id, plan_id, artifact_type, reference, metadata)
     - CORTEX_CALL_LOGS (log_id, plan_id, model, prompt, response, tokens, latency)
  3. Implement LangGraph checkpoint saver using Snowflake tables
  4. Create state_manager.py with CRUD operations
  
  OUTPUT FILES:
  - /Users/rraman/.../snowflake-agentic-platform/src/state/schema_setup.sql
  - /Users/rraman/.../snowflake-agentic-platform/src/state/tables.sql
  - /Users/rraman/.../snowflake-agentic-platform/src/state/snowflake_checkpoint.py
  - /Users/rraman/.../snowflake-agentic-platform/src/state/state_manager.py
  
  PATTERNS: Reference /Users/rraman/.cortex/skills/snowflake-ai-apps/ for Snowpark patterns
  
  RETURN: List of created files, table names, any errors encountered
  """,
  description="Create state tables",
  readonly=False
)
```

**Phase 4.1 - Parquet Processor (parallel with 4.2)**:
```
runSubagent(
  prompt="""
  CONTEXT: Phase 4.1 - Building Parquet Processor agent with LangGraph state machine.
  
  TASK: Implement 6-state processor: SCAN → SCHEMA_INFER → PROFILE → QUALITY_CHECK → TRANSFORM → LOAD
  
  STATE MACHINE:
  - SCAN: List parquet files in Snowflake stage, return file inventory
  - SCHEMA_INFER: Read parquet schemas via Snowpark, detect column types
  - PROFILE: Compute statistics (nulls, uniques, ranges) per column
  - QUALITY_CHECK: Validate against thresholds, flag issues
  - TRANSFORM: Apply type coercion, rename columns to conventions
  - LOAD: COPY INTO Snowflake tables, return table references
  
  LANGGRAPH PATTERN:
  ```python
  from langgraph.graph import StateGraph
  from typing import TypedDict, Literal
  
  class ParquetState(TypedDict):
      stage_path: str
      files: List[FileInfo]
      schemas: Dict[str, Schema]
      profile: DataProfile
      quality_issues: List[Issue]
      tables_created: List[str]
      current_state: Literal["SCAN", "SCHEMA_INFER", ...]
  
  graph = StateGraph(ParquetState)
  graph.add_node("scan", scan_files)
  graph.add_node("schema_infer", infer_schemas)
  # ... add all nodes and edges
  ```
  
  SNOWPARK PATTERNS (from drilling_ops):
  ```python
  df = session.read.parquet(f"@{stage}/{file}")
  df.write.mode("overwrite").save_as_table(f"{schema}.{table}")
  ```
  
  OUTPUT FILES:
  - src/agents/preprocessing/parquet_processor.py (state functions)
  - src/agents/preprocessing/parquet_graph.py (LangGraph definition)
  
  RETURN: Files created, example state transition log
  """,
  description="Build parquet processor",
  readonly=False
)
```

**Phase 4.2 - Document Chunker (parallel with 4.1)**:
```
runSubagent(
  prompt="""
  CONTEXT: Phase 4.2 - Building Document Chunker agent (parallel with Parquet Processor).
  
  TASK: Implement 5-state chunker: EXTRACT → ANALYZE_STRUCTURE → CHUNK → ENRICH_METADATA → LOAD_CHUNKS
  
  STATE MACHINE:
  - EXTRACT: PDF text extraction (PyMuPDF or similar)
  - ANALYZE_STRUCTURE: Detect sections, headers, tables
  - CHUNK: Semantic chunking with overlap, max 16K chars
  - ENRICH_METADATA: Add source file, page, section context
  - LOAD_CHUNKS: Insert to DOCUMENT_CHUNKS table
  
  CHUNK TABLE (create if not exists):
  ```sql
  CREATE TABLE DOCUMENT_CHUNKS (
      chunk_id NUMBER AUTOINCREMENT PRIMARY KEY,
      source_file VARCHAR(500),
      document_type VARCHAR(100),
      page_number INTEGER,
      section_header VARCHAR(500),
      chunk VARCHAR(16000),
      metadata VARIANT,
      created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
  );
  ```
  
  OUTPUT FILES:
  - src/agents/preprocessing/document_chunker.py
  - src/agents/preprocessing/document_graph.py
  
  RETURN: Files created, chunk count from test run
  """,
  description="Build document chunker",
  readonly=False
)
```

### Parallel Delegation Pattern

When tasks have no dependencies, launch multiple sub-agents in the **same message**:

```python
# Phase 4: Preprocessing (3 parallel sub-agents)
runSubagent(prompt="..parquet processor..", description="Build parquet processor", readonly=False)
runSubagent(prompt="..document chunker..", description="Build document chunker", readonly=False)
runSubagent(prompt="..validation framework..", description="Build validation", readonly=False)
# All three execute concurrently, I wait for all to complete
```

### Sequential Delegation Pattern

When tasks depend on previous outputs:

```python
# Step 1: Create state tables (must complete first)
result1 = runSubagent(prompt="..state tables..", ...)

# Step 2: Now create meta-agent (needs state tables)
result2 = runSubagent(prompt=f"""
  CONTEXT: State tables created: {result1.tables}
  TASK: Build meta-agent using these tables for checkpointing...
""", ...)
```

### Skills Reference in Prompts

All sub-agents should reference the skills directory for patterns:

```
PATTERNS LOCATION: /Users/rraman/.cortex/skills/snowflake-ai-apps/
- cortex-search/SKILL.md - Cortex Search service creation
- snowpark-ml/SKILL.md - ML training and registry
- references/cortex-llm-patterns.md - Cortex LLM calls
- references/spcs-auth-patterns.md - SPCS authentication
- references/snowpark-dataframe-patterns.md - Window functions, transforms
- references/semantic-model-patterns.md - Semantic YAML structure
```

### Return Requirements

Every sub-agent prompt ends with explicit return requirements:

```
RETURN:
- List of files created (absolute paths)
- Snowflake objects created (tables, services, models)
- Any errors or warnings encountered
- Test results if applicable
- Artifacts registered to AGENT_ARTIFACTS table
```

### Error Handling

If a sub-agent fails:
1. Read the error from sub-agent response
2. Determine if retryable (transient) or requires human input
3. For retryable: adjust prompt with error context, re-delegate
4. For blocking: escalate to user with context

```python
result = runSubagent(prompt="...", ...)
if "error" in result.lower():
    # Retry with error context
    result = runSubagent(prompt=f"""
      PREVIOUS ATTEMPT FAILED: {result}
      
      RETRY TASK: [same task with additional guidance]
      
      AVOID: [specific error pattern]
    """, ...)
```

---

## Parallelization Strategy

### Parallel Execution Groups

**Group 1** (can run in parallel):
- 1.1 State Management
- 1.2 Agent Registry

**Group 2** (after Group 1):
- 2.1 Meta-Agent Core

**Group 3** (after Group 2, parallel):
- 3.1 File Scanner
- 3.2 Schema Profiler

**Group 4** (after Group 3, parallel):
- 4.1 Parquet Processor
- 4.2 Document Chunker
- 5.1 Validation Framework (starts with schema for others)

**Group 5** (after Group 4):
- 6.1 Feature Store
- 6.2 ML Model Builder

**Group 6** (after Group 4, parallel with Group 5):
- 7.1 Cortex Search Builder
- 7.2 Semantic Model Generator

**Group 7** (after Groups 5 & 6):
- 8.1 App Code Generator

**Group 8** (after Group 7):
- 9.1 SPCS Deployer

**Continuous**:
- 10.1 Observability
- 10.2 Testing

**Final**:
- 11.1 Orchestrator UI

---

## Dependency Graph

```
                    ┌─────────────────┐
                    │  1.1 State Mgmt │
                    └────────┬────────┘
                             │
                    ┌────────┴────────┐
                    │ 1.2 Agent Reg   │
                    └────────┬────────┘
                             │
                    ┌────────┴────────┐
                    │ 2.1 Meta-Agent  │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
    ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
    │ 3.1 Scanner │  │ 3.2 Profiler│  │ 5.1 Validtn │
    └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
           │                │                │
           └────────┬───────┘                │
                    ▼                        │
         ┌─────────────────┐                 │
         │ 4.1 Parquet     │◄────────────────┘
         │ 4.2 Document    │
         └────────┬────────┘
                  │
       ┌──────────┼──────────┐
       ▼          ▼          ▼
┌───────────┐ ┌───────────┐ ┌───────────┐
│ 6.1 Feat  │ │ 7.1 Search│ │ 7.2 Semant│
│ 6.2 ML    │ └─────┬─────┘ └─────┬─────┘
└─────┬─────┘       │             │
      │             └──────┬──────┘
      └────────────────────┼──────────────────┐
                           ▼                  │
                  ┌─────────────────┐         │
                  │ 8.1 App CodeGen │◄────────┘
                  └────────┬────────┘
                           │
                  ┌────────┴────────┐
                  │ 9.1 SPCS Deploy │
                  └────────┬────────┘
                           │
                  ┌────────┴────────┐
                  │ 11.1 Orchestratr│
                  └─────────────────┘
```

---

## Estimated Timeline

| Week | Phases | Deliverables |
|------|--------|--------------|
| 1 | 1.1, 1.2 | State tables, Agent Registry with Cortex Search |
| 2 | 2.1, 3.1, 3.2 | Meta-Agent MVP, Discovery agents |
| 3 | 4.1, 4.2, 5.1 | Preprocessing agents, Validation framework |
| 4 | 6.1, 6.2, 7.1, 7.2 | ML pipeline, Search & Semantic agents |
| 5-6 | 8.1, 9.1 | Code generation, SPCS deployment |
| 6-7 | 10.x, 11.1 | Observability, Orchestrator UI |

---

## Success Criteria (End State)

1. **Meta-Agent** parses "I need to analyze drilling reports for equipment failures" and generates correct execution plan
2. **Agent Registry** returns relevant agents via Cortex Search
3. **Preprocessing** ingests parquet and PDF files correctly
4. **Validation** catches data quality issues with retry
5. **ML Builder** trains model and registers to Snowflake ML Registry
6. **Search Builder** creates working Cortex Search service
7. **Semantic Generator** creates valid semantic model
8. **Code Generator** produces deployable React + FastAPI app
9. **SPCS Deployer** deploys app to accessible endpoint
10. **Orchestrator** visualizes entire pipeline with human approval

---

## Reference Implementation Mapping

| drilling_ops Component | Agentic Platform Equivalent |
|------------------------|---------------------------|
| `orchestrator/backend/engine/` | `src/meta_agent/` + `src/state/` |
| `orchestrator/backend/tasks/` | `src/agents/*/` |
| `orchestrator/frontend/` | `src/app/frontend/` (adapted) |
| `cortex/deploy_agent.sql` | `src/agents/search/` + `src/agents/semantic/` |
| `notebooks/*.py` | `src/agents/ml/` |
| `copilot/` | Generated by `src/agents/app_generation/` |

---

*Document generated: 2026-03-08*  
*Corresponds to: docs/ARCHITECTURE.md*
