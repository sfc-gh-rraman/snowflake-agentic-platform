"""API routes for the orchestrator."""

import asyncio
from typing import Any

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from ..engine import WorkflowExecutor, get_executor, set_executor, workflow_state
from .websocket import get_manager

router = APIRouter()


class WorkflowConfig(BaseModel):
    use_case: str | None = None
    tables: list[str] = []
    documents: list[str] = []
    ml_task: str | None = None
    output_dir: str = "/tmp/generated"
    user_id: str = "anonymous"


class ApprovalRequest(BaseModel):
    approved: bool
    modifications: dict[str, Any] | None = None


@router.get("/workflow")
async def get_workflow():
    return workflow_state.to_dict()


@router.post("/workflow/start")
async def start_workflow(config: WorkflowConfig):
    if workflow_state.is_running:
        raise HTTPException(status_code=400, detail="Workflow already running")

    workflow_state.reset()

    manager = get_manager()
    executor = WorkflowExecutor(websocket_manager=manager)
    set_executor(executor)

    asyncio.create_task(executor.run_all(config.model_dump()))

    return {"status": "started", "message": "Workflow execution started"}


@router.post("/workflow/reset")
async def reset_workflow():
    if workflow_state.is_running:
        raise HTTPException(status_code=400, detail="Cannot reset while running")

    workflow_state.reset()
    return {"status": "reset", "message": "Workflow reset to initial state"}


@router.get("/workflow/task/{task_id}")
async def get_task(task_id: str):
    task = workflow_state.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    return {
        "id": task.id,
        "name": task.name,
        "status": task.status.value,
        "progress": task.progress,
        "duration": task.duration,
        "error": task.error,
        "logs": [
            {"timestamp": log.timestamp, "level": log.level, "message": log.message}
            for log in task.logs
        ],
        "artifacts": task.artifacts,
    }


@router.post("/workflow/task/{task_id}/retry")
async def retry_task(task_id: str):
    task = workflow_state.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

    if workflow_state.is_running:
        raise HTTPException(status_code=400, detail="Cannot retry while workflow is running")

    executor = get_executor()
    asyncio.create_task(executor.run_task(task_id))

    return {"status": "retrying", "task_id": task_id}


@router.get("/workflow/logs")
async def get_all_logs(limit: int = 100):
    all_logs = []
    for phase in workflow_state.phases:
        for task in phase.tasks:
            for log in task.logs:
                all_logs.append(
                    {
                        "timestamp": log.timestamp,
                        "taskId": task.id,
                        "taskName": task.name,
                        "level": log.level,
                        "message": log.message,
                    }
                )
    all_logs.sort(key=lambda x: x["timestamp"])
    return all_logs[-limit:]


@router.post("/workflow/approve")
async def approve_plan(request: ApprovalRequest):
    if request.approved:
        return {"status": "approved", "message": "Plan approved, continuing execution"}
    else:
        return {"status": "rejected", "message": "Plan rejected"}


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
