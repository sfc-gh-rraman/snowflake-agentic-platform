"""API routes for the Health Sciences Orchestrator — LangGraph-native."""

import asyncio
from typing import Any

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ..engine import get_engine, reset_engine
from .websocket import get_manager

router = APIRouter()


class WorkflowConfig(BaseModel):
    user_request: str = "Validate FHIR data quality, apply HIPAA governance, and build analytics view"
    database: str = "AGENTIC_PLATFORM"
    fhir_schema: str = "FHIR_DEMO"
    scenario: str | None = None


class NLRouteRequest(BaseModel):
    prompt: str


class ApprovalRequest(BaseModel):
    approved: bool
    modifications: dict[str, Any] | None = None
    skip_tasks: list[str] | None = None


@router.get("/scenarios")
async def list_scenarios():
    from ..tasks import SCENARIO_DEFINITIONS
    return {
        key: {
            "name": val["name"],
            "description": val["description"],
            "skills": val["skills"],
            "task_count": len(val["tasks"]),
        }
        for key, val in SCENARIO_DEFINITIONS.items()
    }


@router.get("/workflow")
async def get_workflow():
    engine = get_engine()
    return engine.to_dict()


@router.get("/workflow/stream")
async def workflow_stream():
    import json as _json

    async def event_generator():
        last_hash = ""
        while True:
            engine = get_engine()
            state = engine.to_dict()
            state_str = _json.dumps(state, default=str, sort_keys=True)
            current_hash = str(hash(state_str))
            if current_hash != last_hash:
                last_hash = current_hash
                yield f"data: {state_str}\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/workflow/start")
async def start_workflow(config: WorkflowConfig):
    engine = get_engine()
    if engine.is_running:
        raise HTTPException(status_code=400, detail="Workflow already running")

    reset_engine()
    engine = get_engine()

    scenario = config.scenario or "clinical_data_warehouse"
    manager = get_manager()

    asyncio.create_task(
        engine.run(scenario, config.model_dump(), websocket_manager=manager)
    )

    return {
        "status": "started",
        "scenario": scenario,
        "message": f"LangGraph orchestrator started ({scenario})",
    }


@router.post("/workflow/route")
async def route_nl_request(request: NLRouteRequest):
    from ..tasks.scenario_tasks import SCENARIO_DEFINITIONS, _cortex_complete, _get_connection
    import json as _json

    conn = _get_connection()
    scenarios_text = "\n".join(
        f"- {k}: {v['description']} (skills: {', '.join(v['skills'])})"
        for k, v in SCENARIO_DEFINITIONS.items()
    )

    routing = _cortex_complete(
        conn,
        f"""You are a healthcare data platform orchestrator. Route this user request to the best scenario.

User Request: {request.prompt}

Available Scenarios:
{scenarios_text}

Respond in JSON with:
- scenario: the scenario key (clinical_data_warehouse, drug_safety, or clinical_docs)
- confidence: HIGH, MEDIUM, or LOW
- reasoning: 1 sentence why this scenario matches

JSON only, no markdown fences.""",
    )
    conn.close()

    try:
        cleaned = routing.strip()
        if cleaned.startswith("```"):
            cleaned = "\n".join(cleaned.split("\n")[1:])
        if cleaned.endswith("```"):
            cleaned = "\n".join(cleaned.split("\n")[:-1])
        cleaned = cleaned.strip()
        result = _json.loads(cleaned)
        scenario = result.get("scenario", "clinical_data_warehouse")
        confidence = result.get("confidence", "MEDIUM")
        reasoning = result.get("reasoning", "")
    except (ValueError, KeyError):
        scenario = "clinical_data_warehouse"
        confidence = "LOW"
        reasoning = routing[:200]

    scenario_def = SCENARIO_DEFINITIONS.get(scenario, SCENARIO_DEFINITIONS["clinical_data_warehouse"])
    return {
        "scenario": scenario,
        "scenario_name": scenario_def["name"],
        "confidence": confidence,
        "reasoning": reasoning,
        "skills": scenario_def["skills"],
        "tasks": scenario_def["tasks"],
    }


@router.post("/workflow/start-nl")
async def start_workflow_nl(request: NLRouteRequest):
    engine = get_engine()
    if engine.is_running:
        raise HTTPException(status_code=400, detail="Workflow already running")

    route_result = await route_nl_request(request)
    scenario = route_result["scenario"]

    reset_engine()
    engine = get_engine()
    manager = get_manager()

    config = {
        "user_request": request.prompt,
        "scenario": scenario,
        "database": "AGENTIC_PLATFORM",
        "fhir_schema": "FHIR_DEMO",
        "routed_confidence": route_result["confidence"],
        "routed_reasoning": route_result["reasoning"],
    }

    asyncio.create_task(engine.run(scenario, config, websocket_manager=manager))

    return {
        "status": "started",
        "scenario": scenario,
        "scenario_name": route_result["scenario_name"],
        "confidence": route_result["confidence"],
        "reasoning": route_result["reasoning"],
        "message": f"LangGraph orchestrator started via NL routing → {route_result['scenario_name']}",
    }


@router.post("/workflow/reset")
async def reset_workflow():
    engine = get_engine()
    if engine.is_running:
        raise HTTPException(status_code=400, detail="Cannot reset while running")
    reset_engine()
    return {"status": "reset", "message": "Workflow reset to initial state"}


@router.get("/workflow/task/{task_id}")
async def get_task(task_id: str):
    engine = get_engine()
    task = engine.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    return {
        "id": task.id,
        "name": task.name,
        "status": task.status.value,
        "progress": task.progress,
        "duration": task.duration,
        "error": task.error,
        "skill_name": task.skill_name,
        "skill_type": task.skill_type,
        "preflight_status": task.preflight_status,
        "governance": task.governance,
        "logs": [
            {"timestamp": log.timestamp, "level": log.level, "message": log.message}
            for log in task.logs
        ],
        "artifacts": task.artifacts,
    }


@router.post("/workflow/task/{task_id}/retry")
async def retry_task(task_id: str):
    engine = get_engine()
    task = engine.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    if engine.is_running:
        raise HTTPException(status_code=400, detail="Cannot retry while workflow is running")

    from ..tasks import SCENARIO_TASK_REGISTRY
    from ..tasks.workflow_tasks import TASK_REGISTRY

    all_fns = {**TASK_REGISTRY, **SCENARIO_TASK_REGISTRY}
    task_fn = all_fns.get(task_id)
    if not task_fn:
        raise HTTPException(status_code=400, detail=f"No implementation found for task {task_id}")

    engine.update_task(task_id, status="running", progress=0, error=None)
    manager = get_manager()

    import time
    from datetime import datetime

    async def run_retry():
        start = time.time()
        try:
            async def log_fn(msg):
                engine.add_log(task_id, "info", msg)
                await manager.broadcast({"type": "task_log", "payload": {"taskId": task_id, "message": msg}})

            async def progress_fn(p):
                engine.update_task(task_id, progress=p)

            config = engine.config or {}
            result = await task_fn(log=log_fn, progress=progress_fn, config=config if isinstance(config, dict) else {})
            duration = time.time() - start
            artifacts = result if isinstance(result, dict) else {}
            engine.update_task(task_id, status="success", progress=100, duration=duration, artifacts=artifacts)
        except Exception as e:
            duration = time.time() - start
            engine.update_task(task_id, status="failed", error=str(e), duration=duration)

    asyncio.create_task(run_retry())
    return {"status": "retrying", "task_id": task_id}


@router.get("/workflow/logs")
async def get_all_logs(limit: int = 100):
    engine = get_engine()
    all_logs = []
    for phase in engine.phases:
        for task in phase.tasks:
            for log in task.logs:
                all_logs.append({
                    "timestamp": log.timestamp,
                    "taskId": task.id,
                    "taskName": task.name,
                    "skillName": task.skill_name,
                    "level": log.level,
                    "message": log.message,
                })
    all_logs.sort(key=lambda x: x["timestamp"])
    return all_logs[-limit:]


@router.post("/workflow/approve")
async def approve_plan(request: ApprovalRequest):
    engine = get_engine()
    if not engine.awaiting_approval:
        raise HTTPException(status_code=400, detail="No task awaiting approval")
    task_id = engine.awaiting_approval
    if request.skip_tasks:
        from ..engine.state import TaskStatus
        for skip_id in request.skip_tasks:
            engine.update_task(skip_id, status=TaskStatus.SKIPPED)
    engine.approve(request.approved)
    return {
        "status": "approved" if request.approved else "rejected",
        "task_id": task_id,
        "skipped": request.skip_tasks or [],
        "message": f"Task '{task_id}' {'approved' if request.approved else 'rejected'}",
    }


@router.get("/workflow/approval-status")
async def get_approval_status():
    engine = get_engine()
    awaiting = engine.awaiting_approval
    if awaiting:
        task = engine.get_task(awaiting)
        execute_phase = next(
            (p for p in engine.phases if p.id == "execute"), None
        )
        pending_tasks = []
        if execute_phase:
            for t in execute_phase.tasks:
                pending_tasks.append({
                    "id": t.id,
                    "name": t.name,
                    "description": t.description,
                    "skill_name": t.skill_name,
                    "skill_type": t.skill_type,
                    "dependencies": t.dependencies,
                    "enabled": t.status.value != "skipped",
                })
        return {
            "awaiting": True,
            "task_id": awaiting,
            "task_name": task.name if task else awaiting,
            "pending_tasks": pending_tasks,
        }
    return {"awaiting": False}


class ChatRequest(BaseModel):
    message: str
    history: list[dict[str, Any]] | None = None


@router.post("/chat")
async def chat_endpoint(req: ChatRequest):
    from .chat import chat
    return chat(req.message, req.history)


@router.post("/chat/stream")
async def chat_stream_endpoint(req: ChatRequest):
    from .chat import chat_stream

    async def event_generator():
        async for chunk in chat_stream(req.message, req.history):
            yield chunk

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/skills/catalog")
async def get_skills_catalog():
    import os
    import httpx
    import re

    repo = os.environ.get(
        "SKILLS_GITHUB_REPO",
        "Snowflake-Solutions/health-sciences-coco-skills-incubator",
    )
    token = os.environ.get("GITHUB_TOKEN", "")
    branch = os.environ.get("SKILLS_BRANCH", "main")

    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    skills = []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            tree_url = f"https://api.github.com/repos/{repo}/git/trees/{branch}?recursive=1"
            resp = await client.get(tree_url, headers=headers)
            resp.raise_for_status()
            tree = resp.json()

            skill_files = [
                item["path"]
                for item in tree.get("tree", [])
                if item["path"].endswith("SKILL.md") and item["type"] == "blob"
            ]

            for path in skill_files[:30]:
                raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{path}"
                file_resp = await client.get(raw_url, headers=headers)
                if file_resp.status_code != 200:
                    continue

                content = file_resp.text
                name_match = re.search(r'^#\s+(.+)', content, re.MULTILINE)
                desc_match = re.search(r'^>\s*(.+)', content, re.MULTILINE)
                category_match = re.search(r'\*\*Category\*\*[:\s]*(.+)', content, re.IGNORECASE)
                triggers_match = re.search(r'\*\*Triggers?\*\*[:\s]*(.+)', content, re.IGNORECASE)

                folder = "/".join(path.split("/")[:-1])
                skills.append({
                    "path": path,
                    "folder": folder,
                    "name": name_match.group(1).strip() if name_match else folder.split("/")[-1],
                    "description": desc_match.group(1).strip() if desc_match else "",
                    "category": category_match.group(1).strip() if category_match else "uncategorized",
                    "triggers": triggers_match.group(1).strip() if triggers_match else "",
                    "repo": repo,
                    "url": f"https://github.com/{repo}/tree/{branch}/{folder}",
                })

    except Exception as e:
        return {
            "source": "error",
            "error": str(e)[:300],
            "repo": repo,
            "skills": _fallback_skills_catalog(),
        }

    return {"source": "github", "repo": repo, "branch": branch, "skills": skills}


def _fallback_skills_catalog():
    return [
        {
            "path": "skills/provider/clinical-data/fhir/SKILL.md",
            "folder": "skills/provider/clinical-data/fhir",
            "name": "FHIR Data Pipeline",
            "description": "Build FHIR-compliant clinical data pipelines with Dynamic Tables",
            "category": "Provider > Clinical Data",
            "triggers": "FHIR, patient data, clinical data warehouse",
            "repo": "Snowflake-Solutions/health-sciences-coco-skills-incubator",
            "url": "https://github.com/Snowflake-Solutions/health-sciences-coco-skills-incubator/tree/main/skills/provider/clinical-data/fhir",
        },
        {
            "path": "skills/provider/clinical-data/clinical-docs/SKILL.md",
            "folder": "skills/provider/clinical-data/clinical-docs",
            "name": "Clinical Document Intelligence",
            "description": "NLP extraction, document search, and analytics over clinical documents",
            "category": "Provider > Clinical Data",
            "triggers": "clinical docs, document intelligence, NLP extraction",
            "repo": "Snowflake-Solutions/health-sciences-coco-skills-incubator",
            "url": "https://github.com/Snowflake-Solutions/health-sciences-coco-skills-incubator/tree/main/skills/provider/clinical-data/clinical-docs",
        },
        {
            "path": "skills/pharma/drug-safety/pharmacovigilance/SKILL.md",
            "folder": "skills/pharma/drug-safety/pharmacovigilance",
            "name": "Pharmacovigilance Signal Detection",
            "description": "FDA FAERS adverse event analysis with PRR/ROR signal detection",
            "category": "Pharma > Drug Safety",
            "triggers": "FAERS, adverse events, pharmacovigilance, drug safety",
            "repo": "Snowflake-Solutions/health-sciences-coco-skills-incubator",
            "url": "https://github.com/Snowflake-Solutions/health-sciences-coco-skills-incubator/tree/main/skills/pharma/drug-safety/pharmacovigilance",
        },
        {
            "path": "skills/cross-industry/validation/SKILL.md",
            "folder": "skills/cross-industry/validation",
            "name": "Data Validation & Quality",
            "description": "Completeness, schema, and semantic data quality checks",
            "category": "Cross-Industry",
            "triggers": "data quality, validation, completeness check",
            "repo": "Snowflake-Solutions/health-sciences-coco-skills-incubator",
            "url": "https://github.com/Snowflake-Solutions/health-sciences-coco-skills-incubator/tree/main/skills/cross-industry/validation",
        },
        {
            "path": "skills/cross-industry/cortex-agent-chat/SKILL.md",
            "folder": "skills/cross-industry/cortex-agent-chat",
            "name": "Cortex Agent Chat",
            "description": "Reusable Cortex Agent chat with SSE streaming and thinking steps",
            "category": "Cross-Industry",
            "triggers": "cortex agent, chat, copilot, Q&A",
            "repo": "Snowflake-Solutions/health-sciences-coco-skills-incubator",
            "url": "https://github.com/Snowflake-Solutions/health-sciences-coco-skills-incubator/tree/main/skills/cross-industry/cortex-agent-chat",
        },
    ]


@router.get("/workflow/langfuse")
async def get_langfuse_info():
    engine = get_engine()
    from ..engine.langfuse_integration import get_generation_log, get_trace_url
    from ..tasks.scenario_tasks import get_sql_trace_log
    gen_log = get_generation_log()
    sql_log = get_sql_trace_log()
    total_cost = sum(g.get("est_cost_usd", 0) for g in gen_log)
    total_tokens = sum(g.get("est_input_tokens", 0) + g.get("est_output_tokens", 0) for g in gen_log)
    return {
        "trace_url": get_trace_url() or engine.get_langfuse_url(),
        "checkpoint": engine.get_checkpoint() is not None,
        "generations": len(gen_log),
        "total_tokens": total_tokens,
        "total_cost_usd": round(total_cost, 6),
        "generation_log": gen_log[-20:],
        "sql_traces": sql_log[-50:],
        "total_sql_queries": len(sql_log),
    }


@router.get("/workflow/data-freshness")
async def get_data_freshness():
    import os
    import snowflake.connector

    try:
        if os.path.exists("/snowflake/session/token"):
            with open("/snowflake/session/token") as f:
                token = f.read().strip()
            conn = snowflake.connector.connect(
                host=os.environ.get("SNOWFLAKE_HOST", ""),
                account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
                authenticator="oauth",
                token=token,
            )
        else:
            conn_name = os.environ.get("SNOWFLAKE_CONNECTION_NAME", "my_snowflake")
            conn = snowflake.connector.connect(connection_name=conn_name)

        cur = conn.cursor()
        cur.execute("""
            SELECT
                NAME,
                SCHEMA_NAME,
                TARGET_LAG,
                LAST_REFRESH_TIME,
                REFRESH_STATUS,
                DATA_TIMESTAMP,
                DATEDIFF('minute', LAST_REFRESH_TIME, CURRENT_TIMESTAMP()) AS minutes_since_refresh
            FROM AGENTIC_PLATFORM.INFORMATION_SCHEMA.DYNAMIC_TABLES
            ORDER BY LAST_REFRESH_TIME DESC NULLS LAST
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        tables = [dict(zip(cols, r)) for r in rows]

        cur.close()
        conn.close()
        return {"source": "INFORMATION_SCHEMA", "dynamic_tables": tables}
    except Exception as e:
        return {"source": "error", "error": str(e)[:300], "dynamic_tables": []}


@router.get("/workflow/data-preview/{schema_name}/{table_name}")
async def get_data_preview(schema_name: str, table_name: str, limit: int = 5):
    import os
    import re
    import snowflake.connector

    if not re.match(r'^[A-Z_][A-Z0-9_]*$', schema_name) or not re.match(r'^[A-Z_][A-Z0-9_]*$', table_name):
        raise HTTPException(status_code=400, detail="Invalid object name")

    try:
        if os.path.exists("/snowflake/session/token"):
            with open("/snowflake/session/token") as f:
                token = f.read().strip()
            conn = snowflake.connector.connect(
                host=os.environ.get("SNOWFLAKE_HOST", ""),
                account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
                authenticator="oauth",
                token=token,
            )
        else:
            conn_name = os.environ.get("SNOWFLAKE_CONNECTION_NAME", "my_snowflake")
            conn = snowflake.connector.connect(connection_name=conn_name)

        cur = conn.cursor()
        safe_limit = min(max(1, limit), 20)
        cur.execute(f"SELECT * FROM AGENTIC_PLATFORM.{schema_name}.{table_name} LIMIT {safe_limit}")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        data = [dict(zip(cols, [str(v) if v is not None else None for v in r])) for r in rows]

        cur.close()
        conn.close()
        return {"schema": schema_name, "table": table_name, "columns": cols, "rows": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)[:300])


@router.get("/workflow/costs")
async def get_real_costs():
    import os
    import snowflake.connector

    try:
        if os.path.exists("/snowflake/session/token"):
            with open("/snowflake/session/token") as f:
                token = f.read().strip()
            conn = snowflake.connector.connect(
                host=os.environ.get("SNOWFLAKE_HOST", ""),
                account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
                authenticator="oauth",
                token=token,
            )
        else:
            conn_name = os.environ.get("SNOWFLAKE_CONNECTION_NAME", "my_snowflake")
            conn = snowflake.connector.connect(connection_name=conn_name)

        cur = conn.cursor()
        cur.execute("""
            SELECT
                QUERY_TYPE,
                COUNT(*) AS query_count,
                SUM(CREDITS_USED_CLOUD_SERVICES) AS cloud_credits,
                SUM(TOTAL_ELAPSED_TIME) / 1000 AS total_seconds,
                AVG(TOTAL_ELAPSED_TIME) / 1000 AS avg_seconds,
                MAX(END_TIME) AS last_query_time
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE DATABASE_NAME = 'AGENTIC_PLATFORM'
              AND START_TIME >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
              AND EXECUTION_STATUS = 'SUCCESS'
            GROUP BY QUERY_TYPE
            ORDER BY cloud_credits DESC NULLS LAST
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        query_costs = [dict(zip(cols, r)) for r in rows]

        cur.execute("""
            SELECT
                WAREHOUSE_NAME,
                SUM(CREDITS_USED) AS credits_used,
                SUM(CREDITS_USED_COMPUTE) AS compute_credits,
                SUM(CREDITS_USED_CLOUD_SERVICES) AS cloud_credits,
                COUNT(*) AS query_count
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
            GROUP BY WAREHOUSE_NAME
        """)
        rows2 = cur.fetchall()
        cols2 = [d[0] for d in cur.description]
        warehouse_costs = [dict(zip(cols2, r)) for r in rows2]

        cur.close()
        conn.close()

        total_credits = sum(float(w.get("CREDITS_USED", 0) or 0) for w in warehouse_costs)
        total_queries = sum(int(q.get("QUERY_COUNT", 0) or 0) for q in query_costs)

        return {
            "source": "QUERY_HISTORY",
            "total_credits": round(total_credits, 6),
            "total_queries": total_queries,
            "query_costs": query_costs,
            "warehouse_costs": warehouse_costs,
        }
    except Exception as e:
        return {
            "source": "error",
            "error": str(e)[:300],
            "total_credits": 0,
            "total_queries": 0,
            "query_costs": [],
            "warehouse_costs": [],
        }


websocket_router = APIRouter()


@websocket_router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    manager = get_manager()
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        manager.disconnect(websocket)
