# CoCo Healthcare Skills Orchestrator -- Comprehensive Gap Analysis

**Date:** March 26, 2026
**Author:** Cortex Code (AI-assisted analysis)
**Scope:** Full-stack review of `orchestrator/` (backend engine, API, tasks, frontend, deployment)
**Reference Implementation:** `construction_capital_delivery` copilot (Cortex Agent + Search + Analyst patterns)

---

## Executive Summary

25+ gaps identified across 6 categories. The most critical gaps prevent a production-quality demo: flat DAG layout, no real-time streaming, chat uses custom tool-calling instead of native Cortex Agent, and fake cost estimates. A 3-tier improvement plan prioritizes demo-stopping issues first.

---

## CATEGORY 1 -- Architecture & Engine

### GAP 1.1: No Parallel Node Execution
- **File:** `backend/engine/graph.py` -- `_build_graph()` uses sequential `add_edge()` chains
- **Impact:** All tasks execute sequentially even when logically independent (e.g., `build_dynamic_tables` and `setup_faers_data` could run in parallel)
- **Fix:** Use LangGraph `Send()` API or fan-out/fan-in pattern to parallelize independent phase tasks

### GAP 1.2: Retry-from-Checkpoint Stubbed
- **File:** `backend/api/routes.py:174` -- `POST /api/workflow/task/{task_id}/retry` returns `{"status": "not_implemented"}`
- **Impact:** If a task fails mid-workflow, user must restart entire workflow
- **Fix:** Implement `MemorySaver` checkpoint retrieval + selective re-execution from the failed node using LangGraph's `interrupt` + `Command(resume=...)` pattern

### GAP 1.3: No Human-in-the-Loop Approval Gate
- **File:** `backend/engine/graph.py` -- `interrupt_before` parameter not used in `StateGraph` compilation
- **Impact:** Sensitive tasks (e.g., `train_risk_model`, `run_signal_detection`) execute without user approval
- **Fix:** Add `interrupt_before` on high-risk nodes, implement `POST /api/workflow/approve` endpoint that calls `Command(resume=...)`

### GAP 1.4: Global Mutable State Singleton
- **File:** `backend/api/routes.py:15-20` -- `_engine: Optional[LangGraphWorkflowState] = None`
- **Impact:** Concurrent API requests share a single workflow state; second user overwrites first user's workflow
- **Fix:** Use session-keyed engine instances (dict of `session_id -> LangGraphWorkflowState`) or per-request instantiation

### GAP 1.5: Error Propagation Stops Entire Graph
- **File:** `backend/engine/graph.py` -- `_execute_task()` catches exceptions and sets `task.status = "failed"` but the conditional edge logic skips all remaining tasks in the same phase
- **Impact:** One failed task blocks all downstream phases even if other tasks in the phase succeeded
- **Fix:** Add a `continue_on_error` flag per phase; only halt if a critical task fails

---

## CATEGORY 2 -- Frontend UX

### GAP 2.1: DAG Layout is Flat Grid, Not Hierarchical
- **File:** `frontend/src/components/WorkflowGraph.tsx:120` -- `x = 230 + taskIndex * 240` (flat row per phase)
- **Impact:** DAG does not visually represent parent-child dependencies; all tasks in a phase are a flat row
- **Fix:** Integrate `dagre` or `@dagrejs/dagre` for automatic hierarchical DAG layout via ReactFlow's `useNodesInitialized` + Dagre `layout()`

### GAP 2.2: No Real-Time Streaming (1s Polling Only)
- **File:** `frontend/src/App.tsx:80` -- `setInterval(() => fetchWorkflow(), 1000)`
- **Impact:** 1-second polling creates unnecessary API load and visible lag. WebSocket messages are sent by the backend (`backend/api/routes.py:200`) but the frontend never opens a WebSocket connection
- **Fix:** Replace polling with `EventSource` (SSE) or WebSocket on frontend. Backend already has WS support at `/api/ws`

### GAP 2.3: No Animated Task Transitions
- **File:** `frontend/src/components/WorkflowGraph.tsx` -- No CSS transitions or animation on node status changes
- **Impact:** Tasks jump from "pending" to "running" to "completed" with no visual feedback
- **Fix:** Add CSS `@keyframes` pulse for "running" state, confetti/checkmark animation for "completed", shake for "failed"

### GAP 2.4: Scenario Cards Take 40% of Viewport
- **File:** `frontend/src/components/ScenarioSelector.tsx` -- Full-width cards with large descriptions
- **Impact:** Reduces usable screen real estate for the actual workflow
- **Fix:** Collapse to compact chip-style selector after workflow starts; use a dropdown or horizontal pill bar

### GAP 2.5: No Dark/Light Theme Toggle
- **Impact:** Demo may not look good on projector screens with bright ambient light
- **Fix:** Add Tailwind dark mode toggle using `class` strategy + zustand state

### GAP 2.6: No Mobile/Responsive Layout
- **Impact:** Demo unusable on tablets/phones
- **Fix:** Add Tailwind responsive breakpoints for sidebar collapse on `md:` and below

---

## CATEGORY 3 -- Observability & Intelligence

### GAP 3.1: Cost Tracker Uses Fake Heuristics
- **File:** `frontend/src/components/CostTracker.tsx:20-35` -- Hardcoded `creditsPerSec` values (e.g., DT=0.003, ML=0.01)
- **Impact:** Cost numbers are fabricated; not based on actual Snowflake credit usage
- **Fix:** Query `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` for actual credit consumption per query; aggregate by task

### GAP 3.2: No Query-Level Tracing
- **File:** `backend/engine/langfuse_integration.py` -- Traces are at the task level, not individual SQL query level
- **Impact:** Cannot drill into which specific SQL query was slow or expensive within a task
- **Fix:** Wrap each `snowflake_sql_execute` call in a Langfuse `span` with SQL text, duration, and row count

### GAP 3.3: No Time-Series Charts in Dashboard
- **File:** `frontend/src/components/ObservabilityDashboard.tsx` -- Only KPI cards and static bar/pie charts
- **Impact:** No temporal trends visible (e.g., task duration over time, LLM token usage trend)
- **Fix:** Add Recharts `LineChart` with mock or real time-series data from Langfuse

### GAP 3.4: No Alerting or Anomaly Detection
- **Impact:** No proactive notification when a task is taking unusually long or credit burn is high
- **Fix:** Add threshold-based alerts (e.g., task duration > 2x average triggers a warning badge)

---

## CATEGORY 4 -- Agent & Chat (Cortex Agent + Cortex Search)

> **DIRECTIVE:** Replace custom tool-calling chat implementation with native **Cortex Agent REST API** + **Cortex Search** service patterns, as demonstrated in the `construction_capital_delivery` reference application.

### GAP 4.1: Replace Custom Tool-Calling with Cortex Agent REST API

**Current state:** `backend/api/chat.py` implements a hand-coded two-pass LLM workflow:
1. First `CORTEX.COMPLETE()` call with tool definitions to decide which tool to call
2. Manual `_execute_tool()` dispatches to 4 hardcoded tools (`query_patients`, `query_drug_signals`, `search_documents`, `get_workflow_status`)
3. Second `CORTEX.COMPLETE()` call to synthesize the final answer
4. SQL injection vulnerability: `_execute_tool()` directly interpolates LLM-generated SQL WHERE clauses

**Target state:** Deploy a named **Cortex Agent** in Snowflake (e.g., `AGENTIC_PLATFORM.ORCHESTRATOR.HEALTH_COPILOT_AGENT`) and call it via the REST API:

```
POST /api/v2/databases/{db}/schemas/{schema}/agents/{name}:run
Headers: Authorization: Bearer {oauth_token}
Body: { "messages": [...], "stream": true }
```

**Reference pattern** (from `cortex_agent_client.py`):
- `CortexAgentClient` class with `run_agent_stream()` method
- OAuth authentication via SPCS token file (`/snowflake/session/token`)
- httpx `AsyncClient.stream("POST", ...)` for SSE streaming
- Message format: `{"role": "user", "content": [{"type": "text", "text": "..."}]}`
- `thread_id` for multi-turn conversation continuity

**Benefits over current approach:**
- Eliminates SQL injection (agent generates safe SQL via Cortex Analyst semantic model)
- Eliminates hand-coded tool routing (agent natively decides which tools to use)
- Multi-step ReAct reasoning built in (agent can chain multiple tool calls)
- Multi-turn conversation via `thread_id`
- Streaming response with thinking steps

**Implementation steps:**
1. Create `backend/services/cortex_agent_client.py` modeled on reference
2. Deploy Cortex Agent in Snowflake with tools (see GAP 4.2-4.4 for tool configuration)
3. Add `POST /api/chat/stream` SSE endpoint (see GAP 4.5)
4. Delete or archive `backend/api/chat.py`

### GAP 4.2: Add Cortex Analyst as Agent Tool (Text-to-SQL)

**Current state:** Chat tools use hand-written SQL with f-string interpolation. No semantic model.

**Target state:** Create a **semantic model YAML** for the healthcare data (similar to `capital_semantic_model.yaml`) and attach it as a **Cortex Analyst** tool on the agent.

**Semantic model should cover:**
- `FHIR_DEMO.PATIENT` -- Patient demographics (12 rows)
- `FHIR_DEMO.OBSERVATION` -- Vitals and lab results (17 rows)
- `FHIR_DEMO.CONDITION` -- Diagnoses (12 rows)
- `ANALYTICS.PATIENT_360` -- Enriched patient view (Dynamic Table)
- `DRUG_SAFETY.FAERS_DEMO` -- FDA adverse event reports (2000 rows)
- `DRUG_SAFETY.SIGNAL_DETECTION` -- Drug safety signals (40 rows)
- `ML.RISK_PREDICTIONS` -- Patient risk scores

**Agent tool configuration:**
```
Tool: data_analyst (Cortex Analyst)
Semantic Model: @AGENTIC_PLATFORM.CORTEX.SEMANTIC_MODELS/health_semantic_model.yaml
```

**Reference pattern** (from `capital_semantic_model.yaml`):
- Tables with `dimensions`, `time_dimensions`, `facts`, `metrics`
- `relationships` section for JOINs
- `verified_queries` for common questions with pre-written SQL
- `custom_instructions` for business context

### GAP 4.3: Add Cortex Search as Agent Tool (Document Search)

**Current state:** `search_documents` tool in `chat.py` uses `ILIKE '%{query}%'` against `CLINICAL_DOCS.DOCUMENT_REGISTRY.TITLE`

**Target state:** Use the **existing** Cortex Search service `AGENTIC_PLATFORM.CORTEX.CLINICAL_PATIENT_SEARCH` (already created by the orchestrator's `create_cortex_search` task) as a **Cortex Search** tool on the agent.

**Agent tool configuration:**
```
Tool: clinical_search (Cortex Search)
Service: AGENTIC_PLATFORM.CORTEX.CLINICAL_PATIENT_SEARCH
Max Results: 10
```

**Additional search service (if needed):**
```
Tool: doc_search (Cortex Search)
Service: AGENTIC_PLATFORM.CLINICAL_DOCS.DOC_SEARCH_CORPUS
Max Results: 5
```

**Benefits:**
- Semantic search (embedding-based) instead of keyword ILIKE
- Relevance ranking and scoring built in
- No SQL injection risk

### GAP 4.4: Handle SSE Streaming Events in Backend

**Current state:** `POST /api/chat` returns a single JSON response after both LLM calls complete. No streaming.

**Target state:** `POST /api/chat/stream` returns `StreamingResponse` with SSE events.

**Reference pattern** (from `main.py`):
```python
@app.post("/api/chat/stream")
async def chat_stream(message: ChatMessage):
    async def event_generator():
        agent = get_cortex_agent_client()
        yield f"data: {json.dumps({'type': 'thinking', 'title': 'Planning', 'content': 'Analyzing...'})}\n\n"
        async for event in agent.run_agent(message.message):
            yield f"data: {json.dumps(event)}\n\n"
            await asyncio.sleep(0.01)
        yield "data: [DONE]\n\n"
    
    return StreamingResponse(event_generator(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"})
```

**SSE event types to handle:**
| Event Type | Description | Frontend Action |
|---|---|---|
| `response.output_text.delta` | User-visible answer text | Append to message content |
| `response.thinking.delta` | Agent reasoning | Show in collapsible thinking panel |
| `response.status` | Planning/reasoning status | Show as thinking step badge |
| `response.tool_result` | SQL query + data results | Show SQL in expandable code block |
| `response.tool_result.status` | Tool execution status | Update thinking step status |
| `response.chart` | Vega-Lite chart specification | Render inline chart |
| `response.done` | Stream complete | Finalize message |

### GAP 4.5: Update Frontend Chat Component for SSE Streaming

**Current state:** `frontend/src/components/CortexChat.tsx` -- Simple `fetch POST`, waits for full response, renders plain text. Has `simulateResponse()` fallback. No markdown rendering.

**Target state:** Full SSE streaming with thinking steps, tool results, SQL preview, Vega charts, and markdown rendering.

**Reference pattern** (from `Chat.tsx`):

**Key frontend features to adopt:**
1. **SSE Reader:** `response.body.getReader()` + `TextDecoder` + buffer-based event parsing
2. **Thinking Steps Panel:** Collapsible section with status icons (spinner/checkmark) per step
3. **SQL Preview:** Expandable code block when `tool_result` contains SQL
4. **Markdown Rendering:** `ReactMarkdown` for formatted assistant responses
5. **Vega Charts:** `VegaChart` component using `vega-embed` for agent-generated visualizations
6. **Suggested Questions:** Pre-populated question chips for healthcare domain
7. **Fallback:** If streaming fails, fall back to `/api/chat` non-streaming endpoint

**TypeScript interfaces to add:**
```typescript
interface ThinkingStep {
  id: string
  title: string
  content: string
  status: 'pending' | 'in_progress' | 'completed'
  sql?: string
}

interface ChartSpec {
  $schema?: string
  data?: unknown
  mark?: unknown
  encoding?: unknown
  [key: string]: unknown
}

interface Message {
  id: string
  role: 'user' | 'assistant'
  content: string
  thinkingSteps?: ThinkingStep[]
  isStreaming?: boolean
  chartSpec?: ChartSpec
  // ...
}
```

### GAP 4.6: Deploy Cortex Agent in Snowflake

**Current state:** No Cortex Agent exists in the `AGENTIC_PLATFORM` database.

**Target state:** Create `AGENTIC_PLATFORM.ORCHESTRATOR.HEALTH_COPILOT_AGENT` with:
- **Model:** `mistral-large2` or `llama3.1-70b`
- **Instructions:** Healthcare-domain system prompt
- **Tools:**
  - `data_analyst` (Cortex Analyst) -- semantic model for structured queries
  - `clinical_search` (Cortex Search) -- semantic document search
  - `doc_search` (Cortex Search) -- clinical document search (optional)

**Reference pattern** (from `deploy_agent.py`):
- Verify prerequisites (semantic model uploaded, search services available, data loaded)
- Deploy via Snowsight UI (AI & ML > Cortex Agents > Create Agent)
- Or use SQL: `CREATE CORTEX AGENT ...`

---

## CATEGORY 5 -- Data Pipeline

### GAP 5.1: No Data Freshness Monitoring
- **Impact:** Dynamic Tables may lag without notification; user sees stale data
- **Fix:** Query `INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY` and display last-refresh timestamp in Artifact Explorer

### GAP 5.2: Clinical Document Generation Too Slow
- **File:** `backend/tasks/` -- Document generation takes ~5 min for 18 docs due to sequential LLM calls
- **Impact:** Scenario 3 ("Clinical Intelligence") demo drags during doc generation
- **Fix:** Batch document generation with parallel `CORTEX.COMPLETE()` calls (fire 6 at a time)

### GAP 5.3: No Data Preview in Artifact Explorer
- **File:** `frontend/src/components/ArtifactExplorer.tsx` -- Shows object names but no data preview
- **Impact:** User must go to Snowsight to see actual data
- **Fix:** Add "Preview" button that calls `SELECT * FROM {table} LIMIT 5` and shows inline table

---

## CATEGORY 6 -- Deployment & Production

### GAP 6.1: Python Version Mismatch
- **File:** `deploy/Dockerfile` -- Uses Python 3.11-slim; local Mac uses Python 3.14
- **Impact:** Potential compatibility issues between local dev and SPCS container
- **Fix:** Pin Python 3.11 everywhere or upgrade Dockerfile to 3.12+

### GAP 6.2: No Environment Configuration
- **Files:** Multiple -- Hardcoded `AGENTIC_PLATFORM`, `COMPUTE_WH`, schema names throughout
- **Impact:** Cannot deploy to a different database/warehouse without code changes
- **Fix:** Use environment variables with defaults: `SNOWFLAKE_DATABASE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_SCHEMA`

### GAP 6.3: Static Health Endpoint
- **File:** `backend/api/main.py:30` -- `/health` returns `{"status": "ok"}` without checking Snowflake connectivity
- **Impact:** Service reports healthy even when Snowflake connection is broken
- **Fix:** Add Snowflake connectivity check: `SELECT 1` with 5s timeout; return degraded status if unreachable

---

## Improvement Priority Tiers

### TIER 1 -- Demo Stoppers (Immediate Impact)
| # | Improvement | GAP | Effort |
|---|---|---|---|
| 1 | Auto-layout DAG with Dagre | 2.1 | 2h |
| 2 | Deploy Cortex Agent + replace custom chat | 4.1, 4.6 | 4h |
| 3 | SSE streaming in backend + frontend | 4.4, 4.5 | 3h |
| 4 | Create healthcare semantic model YAML | 4.2 | 2h |
| 5 | Wire Cortex Search as agent tool | 4.3 | 1h |
| 6 | Node animations (pulse, glow, confetti) | 2.3 | 1h |
| 7 | Compact scenario selector | 2.4 | 1h |

### TIER 2 -- Impressive (Differentiated Demo)
| # | Improvement | GAP | Effort |
|---|---|---|---|
| 8 | Markdown + Vega charts in chat | 4.5 | 2h |
| 9 | WebSocket/SSE for workflow state | 2.2 | 2h |
| 10 | Parallel task execution | 1.1 | 3h |
| 11 | Resume from checkpoint | 1.2 | 3h |
| 12 | Time-series observability charts | 3.3 | 2h |
| 13 | Data preview in artifact explorer | 5.3 | 1h |

### TIER 3 -- Polish (Production-Ready)
| # | Improvement | GAP | Effort |
|---|---|---|---|
| 14 | Real cost tracking from QUERY_HISTORY | 3.1 | 3h |
| 15 | Human-in-the-loop approval gate | 1.3 | 3h |
| 16 | Query-level Langfuse tracing | 3.2 | 2h |
| 17 | Session-keyed engine instances | 1.4 | 2h |
| 18 | Environment configuration | 6.2 | 1h |
| 19 | Health endpoint with SF check | 6.3 | 0.5h |
| 20 | Data freshness monitoring | 5.1 | 1h |

---

## Cortex Agent Architecture (Target State)

```
User (Frontend Chat)
  |
  | POST /api/chat/stream
  | Content-Type: application/json
  v
FastAPI Backend (event_generator)
  |
  | POST /api/v2/databases/AGENTIC_PLATFORM/schemas/ORCHESTRATOR/agents/HEALTH_COPILOT_AGENT:run
  | Authorization: Bearer {oauth_token}
  | Accept: text/event-stream
  | Body: { "messages": [...], "stream": true, "thread_id": "..." }
  v
Cortex Agent (HEALTH_COPILOT_AGENT)
  |-- Tool: data_analyst (Cortex Analyst)
  |     \-- Semantic Model: @CORTEX.SEMANTIC_MODELS/health_semantic_model.yaml
  |         Tables: PATIENT, OBSERVATION, CONDITION, PATIENT_360, FAERS_DEMO, SIGNAL_DETECTION
  |
  |-- Tool: clinical_search (Cortex Search)
  |     \-- Service: CORTEX.CLINICAL_PATIENT_SEARCH
  |
  |-- Tool: doc_search (Cortex Search)
        \-- Service: CLINICAL_DOCS.DOC_SEARCH_CORPUS
```

**SSE Event Flow:**
```
Frontend                    Backend                     Cortex Agent
   |                           |                              |
   |-- POST /api/chat/stream ->|                              |
   |                           |-- POST agents/:run --------->|
   |                           |                              |
   |<-- SSE: thinking ---------|<-- response.thinking.delta --|
   |<-- SSE: status -----------|<-- response.status ---------|
   |<-- SSE: tool_result ------|<-- response.tool_result -----|
   |<-- SSE: text --------------|<-- response.output_text.delta|
   |<-- SSE: chart ------------|<-- response.chart -----------|
   |<-- SSE: [DONE] -----------|<-- response.done ------------|
```
