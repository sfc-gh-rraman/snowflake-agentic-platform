"""Plan management routes with dynamic execution."""

import json
import uuid
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from ..db import execute_non_query, execute_query

router = APIRouter()


class CreatePlanRequest(BaseModel):
    use_case: str
    data_paths: list[str] = []


class ApproveRequest(BaseModel):
    approved: bool


class ExecutionPlan(BaseModel):
    id: str
    use_case_summary: str
    detected_domain: str
    phases: list[dict]
    status: str
    created_at: str
    updated_at: str | None = None


class Phase(BaseModel):
    phase_id: str
    phase_name: str
    agents: list[str]
    parallel: bool
    checkpoint: bool
    depends_on: list[str] = []
    status: str
    retry_count: int = 0
    started_at: str | None = None
    completed_at: str | None = None
    error_message: str | None = None


class Artifact(BaseModel):
    artifact_id: str
    artifact_type: str
    artifact_name: str
    artifact_location: str
    created_at: str


@router.get("")
async def list_plans():
    rows = execute_query("""
        SELECT
            plan_id, use_case_description, detected_domain,
            execution_plan, status, created_at, updated_at
        FROM AGENTIC_PLATFORM.STATE.EXECUTION_PLANS
        ORDER BY created_at DESC
        LIMIT 100
    """)

    plans = []
    for row in rows:
        plan_data = json.loads(row.get("EXECUTION_PLAN", "{}")) if row.get("EXECUTION_PLAN") else {}
        plans.append(
            {
                "id": row["PLAN_ID"],
                "use_case_summary": row.get("USE_CASE_DESCRIPTION", ""),
                "detected_domain": row.get("DETECTED_DOMAIN", "general"),
                "phases": plan_data.get("phases", []),
                "status": row.get("STATUS", "pending"),
                "created_at": str(row.get("CREATED_AT", "")),
                "updated_at": str(row.get("UPDATED_AT", "")) if row.get("UPDATED_AT") else None,
            }
        )

    return {"plans": plans}


@router.get("/{plan_id}")
async def get_plan(plan_id: str):
    rows = execute_query(
        """
        SELECT
            plan_id, use_case_description, detected_domain,
            execution_plan, status, created_at, updated_at
        FROM AGENTIC_PLATFORM.STATE.EXECUTION_PLANS
        WHERE plan_id = %s
    """,
        (plan_id,),
    )

    if not rows:
        raise HTTPException(status_code=404, detail="Plan not found")

    row = rows[0]
    plan_data = json.loads(row.get("EXECUTION_PLAN", "{}")) if row.get("EXECUTION_PLAN") else {}

    return {
        "id": row["PLAN_ID"],
        "use_case_summary": row.get("USE_CASE_DESCRIPTION", ""),
        "detected_domain": row.get("DETECTED_DOMAIN", "general"),
        "phases": plan_data.get("phases", []),
        "status": row.get("STATUS", "pending"),
        "created_at": str(row.get("CREATED_AT", "")),
        "updated_at": str(row.get("UPDATED_AT", "")) if row.get("UPDATED_AT") else None,
    }


@router.post("")
async def create_plan(request: CreatePlanRequest):
    from src.agents.meta_agent import MetaAgent

    plan_id = str(uuid.uuid4())

    meta_agent = MetaAgent()
    result = meta_agent.generate_plan(request.use_case, request.data_paths)

    execution_plan = {
        "phases": result.get("phases", []),
        "data_paths": request.data_paths,
    }

    execute_non_query(
        """
        INSERT INTO AGENTIC_PLATFORM.STATE.EXECUTION_PLANS
        (plan_id, use_case_description, detected_domain, execution_plan, status, created_at)
        VALUES (%s, %s, %s, %s, 'pending', CURRENT_TIMESTAMP())
    """,
        (
            plan_id,
            result.get("use_case_summary", request.use_case),
            result.get("detected_domain", "general"),
            json.dumps(execution_plan),
        ),
    )

    for i, phase in enumerate(result.get("phases", [])):
        phase_id = f"{plan_id}-phase-{i}"
        execute_non_query(
            """
            INSERT INTO AGENTIC_PLATFORM.STATE.PHASE_STATE
            (phase_id, plan_id, phase_name, status, retry_count)
            VALUES (%s, %s, %s, 'pending', 0)
        """,
            (phase_id, plan_id, phase.get("phase_name", f"Phase {i}")),
        )

    return {
        "id": plan_id,
        "use_case_summary": result.get("use_case_summary", request.use_case),
        "detected_domain": result.get("detected_domain", "general"),
        "phases": result.get("phases", []),
        "status": "pending",
        "created_at": datetime.utcnow().isoformat(),
    }


@router.post("/{plan_id}/approve")
async def approve_plan(plan_id: str, request: ApproveRequest):
    rows = execute_query(
        """
        SELECT status FROM AGENTIC_PLATFORM.STATE.EXECUTION_PLANS
        WHERE plan_id = %s
    """,
        (plan_id,),
    )

    if not rows:
        raise HTTPException(status_code=404, detail="Plan not found")

    new_status = "approved" if request.approved else "rejected"

    execute_non_query(
        """
        UPDATE AGENTIC_PLATFORM.STATE.EXECUTION_PLANS
        SET status = %s, updated_at = CURRENT_TIMESTAMP()
        WHERE plan_id = %s
    """,
        (new_status, plan_id),
    )

    return {"status": new_status}


def execute_plan_phases(plan_id: str):
    """Background task to execute plan phases dynamically."""
    from src.agents.app_generation.app_graph import run_app_pipeline
    from src.agents.ml.model_graph import run_ml_pipeline
    from src.agents.preprocessing.document_graph import run_document_pipeline
    from src.agents.preprocessing.parquet_graph import run_parquet_pipeline

    agent_runners = {
        "parquet_processor": run_parquet_pipeline,
        "document_chunker": run_document_pipeline,
        "model_builder": run_ml_pipeline,
        "app_code_generator": run_app_pipeline,
    }

    rows = execute_query(
        """
        SELECT execution_plan FROM AGENTIC_PLATFORM.STATE.EXECUTION_PLANS
        WHERE plan_id = %s
    """,
        (plan_id,),
    )

    if not rows:
        return

    plan_data = json.loads(rows[0].get("EXECUTION_PLAN", "{}"))
    phases = plan_data.get("phases", [])

    execute_non_query(
        """
        UPDATE AGENTIC_PLATFORM.STATE.EXECUTION_PLANS
        SET status = 'running', updated_at = CURRENT_TIMESTAMP()
        WHERE plan_id = %s
    """,
        (plan_id,),
    )

    try:
        for i, phase in enumerate(phases):
            phase_id = f"{plan_id}-phase-{i}"
            phase.get("phase_name", f"Phase {i}")

            execute_non_query(
                """
                UPDATE AGENTIC_PLATFORM.STATE.PHASE_STATE
                SET status = 'running', started_at = CURRENT_TIMESTAMP()
                WHERE phase_id = %s
            """,
                (phase_id,),
            )

            try:
                agents = phase.get("agents", [])
                for agent in agents:
                    agent_name = agent if isinstance(agent, str) else agent.get("agent", "")

                    if agent_name in agent_runners:
                        config = agent.get("config", {}) if isinstance(agent, dict) else {}
                        agent_runners[agent_name](**config, thread_id=f"{plan_id}-{agent_name}")

                execute_non_query(
                    """
                    UPDATE AGENTIC_PLATFORM.STATE.PHASE_STATE
                    SET status = 'completed', completed_at = CURRENT_TIMESTAMP()
                    WHERE phase_id = %s
                """,
                    (phase_id,),
                )

            except Exception as e:
                execute_non_query(
                    """
                    UPDATE AGENTIC_PLATFORM.STATE.PHASE_STATE
                    SET status = 'failed', error_message = %s, completed_at = CURRENT_TIMESTAMP()
                    WHERE phase_id = %s
                """,
                    (str(e), phase_id),
                )
                raise

        execute_non_query(
            """
            UPDATE AGENTIC_PLATFORM.STATE.EXECUTION_PLANS
            SET status = 'completed', updated_at = CURRENT_TIMESTAMP()
            WHERE plan_id = %s
        """,
            (plan_id,),
        )

    except Exception:
        execute_non_query(
            """
            UPDATE AGENTIC_PLATFORM.STATE.EXECUTION_PLANS
            SET status = 'failed', updated_at = CURRENT_TIMESTAMP()
            WHERE plan_id = %s
        """,
            (plan_id,),
        )


@router.post("/{plan_id}/execute")
async def execute_plan(plan_id: str, background_tasks: BackgroundTasks):
    rows = execute_query(
        """
        SELECT status FROM AGENTIC_PLATFORM.STATE.EXECUTION_PLANS
        WHERE plan_id = %s
    """,
        (plan_id,),
    )

    if not rows:
        raise HTTPException(status_code=404, detail="Plan not found")

    if rows[0]["STATUS"] not in ("pending", "approved"):
        raise HTTPException(status_code=400, detail="Plan cannot be executed in current state")

    background_tasks.add_task(execute_plan_phases, plan_id)

    return {"status": "executing"}


@router.get("/{plan_id}/phases")
async def get_phases(plan_id: str):
    rows = execute_query(
        """
        SELECT
            phase_id, plan_id, phase_name, status,
            retry_count, error_message, started_at, completed_at
        FROM AGENTIC_PLATFORM.STATE.PHASE_STATE
        WHERE plan_id = %s
        ORDER BY phase_id
    """,
        (plan_id,),
    )

    plan_rows = execute_query(
        """
        SELECT execution_plan FROM AGENTIC_PLATFORM.STATE.EXECUTION_PLANS
        WHERE plan_id = %s
    """,
        (plan_id,),
    )

    plan_data = json.loads(plan_rows[0].get("EXECUTION_PLAN", "{}")) if plan_rows else {}
    phase_details = {p.get("phase_name"): p for p in plan_data.get("phases", [])}

    phases = []
    for row in rows:
        phase_name = row.get("PHASE_NAME", "")
        details = phase_details.get(phase_name, {})

        phases.append(
            {
                "phase_id": row["PHASE_ID"],
                "phase_name": phase_name,
                "agents": details.get("agents", []),
                "parallel": details.get("parallel", False),
                "checkpoint": details.get("checkpoint", False),
                "depends_on": details.get("depends_on", []),
                "status": row.get("STATUS", "pending"),
                "retry_count": row.get("RETRY_COUNT", 0),
                "error_message": row.get("ERROR_MESSAGE"),
                "started_at": str(row.get("STARTED_AT", "")) if row.get("STARTED_AT") else None,
                "completed_at": str(row.get("COMPLETED_AT", ""))
                if row.get("COMPLETED_AT")
                else None,
            }
        )

    return {"phases": phases}


@router.get("/{plan_id}/artifacts")
async def get_artifacts(plan_id: str):
    rows = execute_query(
        """
        SELECT
            artifact_id, artifact_type, artifact_name,
            artifact_location, created_at
        FROM AGENTIC_PLATFORM.STATE.ARTIFACTS
        WHERE plan_id = %s
        ORDER BY created_at DESC
    """,
        (plan_id,),
    )

    artifacts = [
        {
            "artifact_id": row["ARTIFACT_ID"],
            "artifact_type": row["ARTIFACT_TYPE"],
            "artifact_name": row["ARTIFACT_NAME"],
            "artifact_location": row["ARTIFACT_LOCATION"],
            "created_at": str(row["CREATED_AT"]),
        }
        for row in rows
    ]

    return {"artifacts": artifacts}
