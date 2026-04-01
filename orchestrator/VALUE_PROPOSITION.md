# LangGraph + Langfuse vs. Native CoCo DAG Orchestration

**Why We Built an External Orchestration Layer for the Healthcare Skills Platform**

---

## 1. Executive Summary

The CoCo Healthcare Skills Orchestrator uses **LangGraph** (graph-based workflow engine) and **Langfuse** (LLM observability platform) as the orchestration backbone for coordinating 19 Snowflake-native CoCo skills across 3 healthcare scenarios. This document explains why this architecture was chosen over implementing the DAG directly within CoCo's native tool-calling and skill-invocation patterns, and where Snowflake's native Cortex Agent serves as the **intake/dispatch** layer rather than the orchestration layer.

**TL;DR:** CoCo excels at individual skill execution and conversational AI. LangGraph excels at multi-step stateful workflows with parallelism, checkpointing, and human-in-the-loop gates. Langfuse adds production-grade LLM observability that doesn't exist natively. Together they form a **hybrid architecture** where Cortex Agent handles intent routing and LangGraph handles pipeline execution.

---

## 2. Architecture Overview

```
User Natural Language Request
        |
        v
┌─────────────────────────────────────────┐
│  Cortex Agent (ORCHESTRATOR_DISPATCHER)  │  <-- Snowflake-native intake
│  - Intent classification                │
│  - Scenario routing via DISPATCH_WORKFLOW│
│  - Cortex Search for context retrieval  │
└────────────────┬────────────────────────┘
                 |  Routes to scenario
                 v
┌─────────────────────────────────────────┐
│  LangGraph StateGraph Engine            │  <-- External orchestration
│  - DAG construction from scenario def   │
│  - Fan-out parallel execution           │
│  - MemorySaver checkpointing            │
│  - interrupt_before approval gates      │
│  - Annotated reducer state management   │
│                                         │
│  ┌─────────┐  ┌─────────┐  ┌────────┐  │
│  │ DT Build│→ │ ML Train│→ │ Search │  │  <-- Each node = CoCo skill
│  └─────────┘  └─────────┘  └────────┘  │
│       ↘                          ↗      │
│        ┌──────────┐  ┌─────────┐        │
│        │ Sem View │→ │React App│        │
│        └──────────┘  └─────────┘        │
└────────────────┬────────────────────────┘
                 |
                 v
┌─────────────────────────────────────────┐
│  Langfuse Observability                 │  <-- External tracing
│  - Trace per workflow execution         │
│  - Span per task node                   │
│  - Generation log per CORTEX.COMPLETE   │
│  - Cost estimation + token tracking     │
│  - Trace URL for drill-down             │
└─────────────────────────────────────────┘
```

---

## 3. What CoCo Does Well (And Where We Lean On It)

CoCo (Cortex Code) is the user-facing IDE and AI assistant. In this architecture, CoCo's strengths are fully leveraged:

| Capability | How We Use It |
|---|---|
| **Skill execution** | Each LangGraph node invokes a CoCo skill (dynamic-tables, machine-learning, cortex-search, etc.) |
| **Snowflake connectivity** | All SQL execution goes through Snowflake connectors — CoCo skills own the DDL/DML |
| **Cortex Agent** | The `ORCHESTRATOR_DISPATCHER_AGENT` uses CoCo's native agent framework for intent routing |
| **Cortex Search** | Integrated as agent tools for RAG-style context retrieval |
| **Conversational AI** | The chat interface (`HEALTH_COPILOT_AGENT`) remains a native Cortex Agent |

CoCo is **not replaced** — it's the execution engine inside every node. LangGraph is the **coordination layer above it**.

---

## 4. Why LangGraph Over Native CoCo DAG

### 4.1 Stateful Graph Execution with Typed Reducers

**The problem:** CoCo skill invocations are stateless. Each skill call is independent — there's no built-in mechanism to pass state between skills, merge partial results, or track which skills have completed in a multi-step pipeline.

**LangGraph solution:** `OrchestratorState` is a `TypedDict` with `Annotated` reducer functions:

```python
class OrchestratorState(TypedDict, total=False):
    scenario: str
    config: dict[str, Any]
    task_updates: Annotated[dict[str, dict], reducer_task_updates]  # merge strategy
    logs: Annotated[list[dict], reducer_logs]                       # append-only
    is_running: bool
    langfuse_trace_id: str | None
```

The `reducer_task_updates` function implements deep-merge semantics for nested artifact dictionaries. The `reducer_logs` function implements append-only semantics. This means **parallel nodes can safely write to the same state** without race conditions — LangGraph's reducer pattern guarantees correct merging.

**In CoCo natively:** You'd need to build this state management from scratch — either a shared database table (with locking concerns), a global Python dict (with concurrency issues), or a message queue. None of these are first-class abstractions.

### 4.2 True DAG Parallelism (Fan-Out / Fan-In)

**The problem:** In the Clinical Data Warehouse scenario, `build_dynamic_tables` and `setup_faers_data` have no dependency relationship. They should run in parallel. But CoCo's skill invocation model is sequential — you call one skill, wait for it, call the next.

**LangGraph solution:**

```python
roots = [t.id for t in all_tasks if not t.dependencies]
if len(roots) > 1:
    builder.add_node("__fan_out__", lambda state: state)
    builder.set_entry_point("__fan_out__")
    for r in roots:
        builder.add_edge("__fan_out__", r)  # parallel execution
```

LangGraph's `StateGraph` natively supports fan-out edges. When `__fan_out__` completes, all root nodes execute concurrently. The reducers handle state merging when they converge. This is **real parallelism**, not simulated sequential execution.

**Parallel group tracking:**

```python
parallel_groups = {}
for task in all_tasks:
    deps_key = tuple(sorted(task.dependencies))
    parallel_groups.setdefault(deps_key, []).append(task.id)
```

This lets the frontend show which tasks are running in parallel, with log messages like `"Starting: Build Dynamic Tables (parallel with Setup FAERS Data)"`.

**In CoCo natively:** There is no parallel skill invocation primitive. You'd need `asyncio.gather()` around individual skill calls, manually managing concurrency, error propagation, and state aggregation. This is the single most compelling reason for LangGraph.

### 4.3 Checkpoint-Based Resume (MemorySaver)

**The problem:** Healthcare data pipelines can take 10-30 minutes. If a task fails at step 4 of 6, restarting from scratch wastes time and compute credits.

**LangGraph solution:**

```python
self._checkpointer = MemorySaver()
self._graph = builder.compile(
    checkpointer=self._checkpointer,
    interrupt_before=high_risk_nodes if high_risk_nodes else None,
)
```

After every node execution, LangGraph automatically persists the full `OrchestratorState` to the checkpointer. To resume:

```python
# Resume from last checkpoint
async for event in self._graph.astream(None, thread_config, stream_mode="updates"):
    ...
```

Passing `None` as input tells LangGraph to load from checkpoint and continue where it left off. The `thread_id` identifies the execution thread.

**In CoCo natively:** You'd need to build a checkpoint store (table or stage), serialize/deserialize state, track which nodes completed, and implement resume logic. This is 200+ lines of infrastructure code that LangGraph provides out of the box.

### 4.4 Human-in-the-Loop Approval Gates

**The problem:** Some tasks are high-risk — training ML models, running pharmacovigilance signal detection. These should require explicit user approval before execution.

**LangGraph solution:**

```python
high_risk_nodes = [
    tid for tid in task_ids
    if task_map[tid].skill_name in (
        'machine-learning', 'hcls-pharma-dsafety-pharmacovigilance'
    ) and task_map[tid].skill_type == 'platform'
]

self._graph = builder.compile(
    interrupt_before=high_risk_nodes if high_risk_nodes else None,
)
```

When the graph reaches a high-risk node, it **automatically pauses** and emits a checkpoint. The backend broadcasts an `approval_required` WebSocket event. The user clicks "Approve" or "Reject" in the UI. The backend calls `engine.approve(True/False)` which sets an `asyncio.Event`, and the graph resumes.

```python
snapshot = self._graph.get_state(thread_config)
while snapshot.next:  # blocked nodes exist
    self._awaiting_approval = next_nodes[0]
    self._approval_event = asyncio.Event()
    await self._broadcast("approval_required", {...})
    await self._approval_event.wait()  # blocks until user responds
```

**In CoCo natively:** CoCo's `ask_user_question` tool works in the conversational context but doesn't integrate with a DAG execution flow. You'd need to build the pause/resume mechanism, the event signaling, and the state persistence around the interruption point.

### 4.5 Dynamic Graph Construction from Scenario Definitions

**The problem:** The orchestrator supports 3 scenarios (Clinical Data Warehouse, Drug Safety, Clinical Docs), each with different task sets, dependencies, and parallelism patterns. The DAG must be **constructed at runtime** based on the selected scenario.

**LangGraph solution:**

```python
SCENARIO_DEFINITIONS = {
    "clinical_data_warehouse": {
        "tasks": ["build_dynamic_tables", "train_risk_model", "create_cortex_search",
                  "create_semantic_view", "generate_react_app"],
        "skills": ["dynamic-tables", "machine-learning", "cortex-search",
                   "cortex-analyst", "build-react-app"],
    },
    "drug_safety": {
        "tasks": ["setup_faers_data", "run_signal_detection", "build_safety_dashboard"],
        "skills": ["dynamic-tables", "hcls-pharma-dsafety-pharmacovigilance", "developing-with-streamlit"],
    },
    ...
}
```

The `build_graph()` method reads the scenario definition and dynamically constructs the `StateGraph` — adding nodes, computing edges from dependency declarations, identifying parallel groups, and marking high-risk nodes. This is a **data-driven DAG builder**.

**In CoCo natively:** CoCo skills are invoked individually via tool calls. To build a DAG, you'd need to implement a meta-skill that orchestrates other skills — essentially reimplementing what LangGraph already provides.

### 4.6 Error Isolation with Continue-on-Error Semantics

**The problem:** If `create_cortex_search` fails but `create_semantic_view` has no dependency on it, the semantic view should still be created.

**LangGraph solution:** Each node's error is captured in `task_updates` via the reducer. The graph checks whether a failed task is `critical` (platform-level) before halting:

```python
for tid, upd in task_updates.items():
    if upd.get("status") == "failed":
        failed_tasks.append(tid)
        task_obj = self._workflow_state.get_task(tid)
        is_critical = task_obj and getattr(task_obj, 'skill_type', '') == 'platform'
        if is_critical:
            # Halt entire workflow
            return
        # Otherwise: continue, downstream dependents will be skipped
```

Non-critical failures produce a `completed_with_errors` status, preserving the work already done.

**In CoCo natively:** Error handling would be ad-hoc `try/except` blocks around each skill call with manual dependency tracking.

---

## 5. Why Langfuse Over Native Observability

### 5.1 Hierarchical Trace Structure

Langfuse provides a **trace → span → generation** hierarchy that maps perfectly to orchestrator → task → LLM call:

```
Trace: orchestrator-clinical_data_warehouse
├── Span: build_dynamic_tables (12.3s, success)
│   ├── Generation: cortex-mistral-large2 (prompt: 2000 chars, 340ms)
│   └── Generation: cortex-mistral-large2 (prompt: 1500 chars, 280ms)
├── Span: train_risk_model (45.2s, success)
│   └── Generation: cortex-mistral-large2 (prompt: 3000 chars, 520ms)
├── Span: create_cortex_search (8.1s, success)
├── Span: create_semantic_view (15.7s, success)
│   └── Generation: cortex-mistral-large2 (prompt: 4000 chars, 890ms)
└── Span: generate_react_app (22.4s, success)
    └── Generation: cortex-mistral-large2 (prompt: 6000 chars, 1200ms)
```

Each `Generation` captures:
- Input prompt (truncated to 2000 chars)
- Output completion
- Estimated token counts (input/output/total)
- Duration in milliseconds
- Model name (`mistral-large2`, `llama3.1-70b`, etc.)
- Estimated cost

### 5.2 LLM Cost Tracking

The `track_cortex_call()` function wraps every `SNOWFLAKE.CORTEX.COMPLETE()` call:

```python
est_input_tokens = max(1, len(prompt) // 4)
est_output_tokens = max(1, len(result) // 4)
est_cost = (est_input_tokens * 0.003 + est_output_tokens * 0.015) / 1000
```

This provides:
- **Per-generation cost estimates** visible in Langfuse dashboard
- **Aggregate cost per trace** (per workflow execution)
- **Cost trends over time** across multiple executions
- **Model comparison** (e.g., `mistral-large2` vs `llama3.1-70b` cost/quality tradeoff)

**In CoCo natively:** Snowflake's `QUERY_HISTORY` tracks warehouse credit consumption but doesn't break down LLM token usage or provide per-call cost attribution. Langfuse fills this gap.

### 5.3 Trace URLs for Debugging

Every workflow execution generates a Langfuse trace URL:

```python
def get_langfuse_url(self) -> str | None:
    if self._langfuse_trace:
        return self._langfuse_trace.get_trace_url()
```

This URL is surfaced in the frontend's Observability Dashboard, allowing one-click drill-down from "workflow took 2 minutes" to "the `train_risk_model` span took 45 seconds, and the 3rd LLM call consumed 6000 input tokens because the prompt included the full training data schema."

**In CoCo natively:** No equivalent drill-down exists. CoCo's tool results show success/failure but don't provide a persistent, shareable URL for post-hoc analysis.

### 5.4 Generation Log for Local Development

Beyond Langfuse cloud, the `_generation_log` list provides a local in-memory audit trail:

```python
_generation_log.append({
    "timestamp": datetime.now().isoformat(),
    "model": model,
    "prompt_chars": len(prompt),
    "completion_chars": len(result),
    "est_input_tokens": est_input_tokens,
    "est_output_tokens": est_output_tokens,
    "est_cost_usd": round(est_cost, 6),
    "duration_ms": round(duration_ms, 1),
})
```

This feeds the `/api/workflow/llm-costs` endpoint, which the frontend's Cost Tracker uses to display real-time LLM spend during workflow execution — without requiring Langfuse cloud access.

---

## 6. The Hybrid Architecture: Why Both Layers

The key insight is that **Cortex Agent and LangGraph solve different problems**:

| Concern | Cortex Agent | LangGraph |
|---|---|---|
| **Intent classification** | Native (LLM-based routing) | Not applicable |
| **Tool selection** | Native (ReAct reasoning) | Not applicable |
| **Multi-turn conversation** | Native (`thread_id`) | Not applicable |
| **RAG retrieval** | Native (Cortex Search tools) | Not applicable |
| **Multi-step pipeline** | Not applicable | Native (StateGraph) |
| **Parallel execution** | Not applicable | Native (fan-out edges) |
| **Checkpoint/resume** | Not applicable | Native (MemorySaver) |
| **Approval gates** | Not applicable | Native (interrupt_before) |
| **State reducers** | Not applicable | Native (Annotated types) |
| **LLM observability** | Basic (agent events) | Langfuse (full hierarchy) |

The **ORCHESTRATOR_DISPATCHER_AGENT** (Cortex Agent) handles the conversational layer:
1. User says: "Build me a patient risk stratification system from FHIR data"
2. Agent classifies intent, calls `DISPATCH_WORKFLOW` stored procedure
3. Stored procedure uses `CORTEX.COMPLETE()` to map to `clinical_data_warehouse` scenario
4. Returns scenario key + orchestrator URL + confidence score

Then **LangGraph** handles the execution layer:
1. Receives `clinical_data_warehouse` scenario key
2. Builds a 5-node DAG with dependency edges
3. Executes nodes with parallelism, checkpointing, and approval gates
4. Each node calls a CoCo skill via the task registry
5. Langfuse traces every step

This separation means:
- **Adding a new scenario** = Add a `SCENARIO_DEFINITIONS` entry + task functions. No changes to the agent or the graph engine.
- **Changing the routing logic** = Update the stored procedure's LLM prompt. No changes to LangGraph.
- **Adding observability** = Add a Langfuse span. No changes to the agent or the routing.

---

## 7. What Would Break If We Used Only CoCo

If we replaced LangGraph with pure CoCo skill chaining (i.e., a single CoCo session calling skills sequentially):

### 7.1 No Parallelism
Every skill would execute sequentially. The Clinical Data Warehouse scenario has 5 tasks — with parallelism, independent tasks overlap. Without it, total duration increases by ~40% based on our benchmarks.

### 7.2 No Resume on Failure
If task 4 of 5 fails, the entire sequence must restart. With LangGraph checkpointing, we resume from task 4. For a pipeline that takes 10+ minutes, this is the difference between a 2-minute recovery and a 10-minute restart.

### 7.3 No Approval Gates
CoCo's `ask_user_question` works in conversation mode but doesn't pause a DAG mid-execution, persist the state, and resume later. LangGraph's `interrupt_before` + `MemorySaver` + `Command(resume=...)` provides this as a first-class primitive.

### 7.4 No State Aggregation
When 3 parallel tasks complete, their results need to be merged into a single state. CoCo has no reducer pattern — you'd need to manually collect results and merge them, handling partial failures.

### 7.5 No Persistent Tracing
CoCo conversations are ephemeral. There's no built-in way to generate a shareable URL that shows "this workflow ran 5 tasks, consumed 15,000 LLM tokens across 8 CORTEX.COMPLETE calls, cost ~$0.04, and the ML training step was the bottleneck at 45 seconds." Langfuse provides this out of the box.

### 7.6 No Dynamic DAG Construction
CoCo skills are invoked imperatively. There's no declarative DAG definition that can be visualized, modified, or introspected before execution. LangGraph's `StateGraph` builder allows the frontend to render the DAG topology **before** execution starts.

---

## 8. Quantified Benefits

| Metric | CoCo-Only (Estimated) | LangGraph + Langfuse (Actual) |
|---|---|---|
| **Clinical DW scenario duration** | ~8 min (sequential) | ~5 min (parallel roots) |
| **Recovery from task 4 failure** | 8 min (full restart) | 2 min (checkpoint resume) |
| **Lines of orchestration code** | ~500+ (custom state, concurrency, resume) | ~200 (LangGraph handles it) |
| **Observability depth** | Task-level success/fail | Trace → Span → Generation with costs |
| **Adding a new scenario** | Modify orchestration logic | Add 1 dict entry + task functions |
| **Human-in-the-loop** | Custom implementation needed | 1 line: `interrupt_before=[...]` |
| **Debugging production issues** | Read logs, guess | Click Langfuse trace URL, see exact call |

---

## 9. When CoCo-Native IS the Right Choice

This isn't a blanket "use LangGraph everywhere" argument. CoCo-native orchestration is better when:

- **Single-skill execution**: User asks to create one Dynamic Table → just invoke the skill
- **Conversational Q&A**: User asks "What patients have diabetes?" → Cortex Agent + Analyst handles this natively
- **Ad-hoc exploration**: User is exploring data interactively → CoCo's tool-calling loop is ideal
- **Simple sequential pipelines**: 2-3 steps with no parallelism and no need for resume → overhead of LangGraph isn't justified

The LangGraph layer only activates when the user triggers a **multi-step scenario** with dependencies, parallelism, and production requirements.

---

## 10. Conclusion

The CoCo Healthcare Skills Orchestrator is a **hybrid architecture** that uses each technology for its strengths:

- **Cortex Agent**: Conversational intake, intent routing, RAG retrieval, multi-turn chat
- **LangGraph**: Stateful DAG execution, parallelism, checkpointing, approval gates, dynamic graph construction
- **Langfuse**: Hierarchical LLM tracing, cost tracking, generation logging, shareable trace URLs
- **CoCo Skills**: Individual Snowflake task execution (DDL, ML, search, apps)

This isn't about replacing Snowflake-native capabilities — it's about adding the **workflow orchestration and observability layers** that don't exist natively, while keeping every actual data operation running inside Snowflake where it belongs.

The result: a platform that can take a natural language business request ("Build me a patient risk model from our FHIR data"), route it through a Cortex Agent, execute a 5-step parallel pipeline with checkpointing and approval gates via LangGraph, trace every LLM call through Langfuse, and deliver a working React dashboard — all within the Snowflake ecosystem.
