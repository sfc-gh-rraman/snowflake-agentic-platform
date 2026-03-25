"""Workflow executor for the Health Sciences Orchestrator."""

import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

from .state import TaskStatus, workflow_state


class WorkflowExecutor:
    def __init__(self, websocket_manager=None):
        self.manager = websocket_manager
        self.task_functions: dict[str, Callable] = {}

    def register_task(self, task_id: str, fn: Callable):
        self.task_functions[task_id] = fn

    async def broadcast(self, message_type: str, payload: dict[str, Any]):
        if self.manager:
            await self.manager.broadcast(
                {
                    "type": message_type,
                    "payload": payload,
                    "timestamp": datetime.now().isoformat(),
                }
            )

    async def log(self, task_id: str, level: str, message: str):
        workflow_state.add_log(task_id, level, message)
        await self.broadcast(
            "task_log",
            {
                "taskId": task_id,
                "level": level,
                "message": message,
            },
        )

    async def update_progress(self, task_id: str, progress: int):
        workflow_state.update_task(task_id, progress=progress)
        await self.broadcast(
            "task_progress",
            {
                "taskId": task_id,
                "progress": progress,
            },
        )

    async def run_task(self, task_id: str):
        task = workflow_state.get_task(task_id)
        if not task:
            raise ValueError(f"Task not found: {task_id}")

        for dep_id in task.dependencies:
            dep_task = workflow_state.get_task(dep_id)
            if dep_task and dep_task.status != TaskStatus.SUCCESS:
                await self.run_task(dep_id)

        await self._execute_task(task_id)

    async def run_all(self, config: dict[str, Any] | None = None):
        workflow_state.is_running = True
        workflow_state.start_time = datetime.now()
        workflow_state.config = config or {}

        try:
            execution_order = self._get_execution_order()

            for task_id in execution_order:
                task = workflow_state.get_task(task_id)
                if task and task.status == TaskStatus.PENDING:
                    deps_ready = all(
                        workflow_state.get_task(d).status == TaskStatus.SUCCESS
                        for d in task.dependencies
                        if workflow_state.get_task(d)
                    )
                    if deps_ready:
                        await self._execute_task(task_id)

                        if task.status == TaskStatus.FAILED:
                            await self.log(
                                "orchestrator",
                                "error",
                                f"Workflow stopped due to task failure: {task_id}",
                            )
                            break

            await self.broadcast(
                "workflow_completed",
                {
                    "status": "completed",
                    "duration": (datetime.now() - workflow_state.start_time).total_seconds(),
                },
            )

        except Exception as e:
            await self.log("orchestrator", "error", f"Workflow failed: {str(e)}")
        finally:
            workflow_state.is_running = False
            workflow_state.end_time = datetime.now()

    async def _execute_task(self, task_id: str):
        task = workflow_state.get_task(task_id)
        if not task:
            return

        task_fn = self.task_functions.get(task_id)
        if not task_fn:
            await self.log(task_id, "warning", f"No implementation for task: {task_id}, skipping")
            workflow_state.update_task(task_id, status=TaskStatus.SKIPPED)
            return

        workflow_state.update_task(task_id, status=TaskStatus.RUNNING, progress=0)
        await self.broadcast("task_started", {"taskId": task_id})
        await self.log(task_id, "info", f"Starting: {task.name}")

        start_time = time.time()

        try:
            result = await task_fn(
                log=lambda msg: self.log(task_id, "info", msg),
                progress=lambda p: self.update_progress(task_id, p),
                config=workflow_state.config,
            )

            duration = time.time() - start_time
            workflow_state.update_task(
                task_id,
                status=TaskStatus.SUCCESS,
                progress=100,
                duration=duration,
                artifacts=result if isinstance(result, dict) else {},
            )
            await self.broadcast(
                "task_completed",
                {
                    "taskId": task_id,
                    "status": "success",
                    "duration": duration,
                },
            )
            await self.log(task_id, "success", f"Completed in {duration:.1f}s")

        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            workflow_state.update_task(
                task_id,
                status=TaskStatus.FAILED,
                error=error_msg,
                duration=duration,
            )
            await self.broadcast(
                "task_completed",
                {
                    "taskId": task_id,
                    "status": "failed",
                    "error": error_msg,
                    "duration": duration,
                },
            )
            await self.log(task_id, "error", f"Failed: {error_msg}")

    def _get_execution_order(self) -> list[str]:
        all_tasks = workflow_state.get_all_tasks()
        task_map = {t.id: t for t in all_tasks}

        order = []
        visited = set()

        def visit(task_id: str):
            if task_id in visited:
                return
            visited.add(task_id)

            task = task_map.get(task_id)
            if task:
                for dep_id in task.dependencies:
                    visit(dep_id)
                order.append(task_id)

        for task in all_tasks:
            visit(task.id)

        return order


_executor: WorkflowExecutor | None = None


def get_executor() -> WorkflowExecutor:
    global _executor
    if _executor is None:
        _executor = WorkflowExecutor()
    return _executor


def set_executor(executor: WorkflowExecutor):
    global _executor
    _executor = executor
