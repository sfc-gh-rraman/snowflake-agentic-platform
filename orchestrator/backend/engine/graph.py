"""LangGraph-native workflow engine for the Health Sciences Orchestrator.

Replaces the custom WorkflowExecutor with a LangGraph StateGraph.
Each scenario task becomes a graph node. Dependencies become edges.
Langfuse tracing instruments every node execution.
"""

import asyncio
import copy
import time
import uuid
from datetime import datetime
from typing import Annotated, Any, TypedDict

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph
from langgraph.types import Command, interrupt

from .state import (
    SCENARIO_WORKFLOWS,
    Phase,
    PhaseStatus,
    Task,
    TaskLog,
    TaskStatus,
    WorkflowState,
)


def _merge_task_updates(existing: dict, new: dict) -> dict:
    merged = {**existing}
    for k, v in new.items():
        if k == "artifacts" and k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = {**merged[k], **v}
        elif k == "logs" and k in merged and isinstance(merged[k], list) and isinstance(v, list):
            merged[k] = merged[k] + v
        else:
            merged[k] = v
    return merged


def reducer_task_updates(
    existing: dict[str, dict], new: dict[str, dict]
) -> dict[str, dict]:
    merged = {**existing}
    for task_id, updates in new.items():
        if task_id in merged:
            merged[task_id] = _merge_task_updates(merged[task_id], updates)
        else:
            merged[task_id] = updates
    return merged


def reducer_logs(existing: list[dict], new: list[dict]) -> list[dict]:
    return existing + new


class OrchestratorState(TypedDict, total=False):
    scenario: str
    config: dict[str, Any]
    task_updates: Annotated[dict[str, dict], reducer_task_updates]
    logs: Annotated[list[dict], reducer_logs]
    is_running: bool
    start_time: str | None
    end_time: str | None
    error: str | None
    current_node: str | None
    langfuse_trace_id: str | None


class LangGraphWorkflowState:
    def __init__(self):
        self._workflow_state = WorkflowState()
        self._graph = None
        self._checkpointer = MemorySaver()
        self._thread_id = None
        self._websocket_manager = None
        self._langfuse = None
        self._langfuse_trace = None
        self._parallel_groups: dict = {}
        self._awaiting_approval: str | None = None
        self._approval_event: asyncio.Event | None = None
        self._approval_result: bool = True

    @property
    def workflow_state(self) -> WorkflowState:
        return self._workflow_state

    def init_langfuse(self):
        try:
            import os
            pk = os.environ.get("LANGFUSE_PUBLIC_KEY")
            sk = os.environ.get("LANGFUSE_SECRET_KEY")
            host = os.environ.get("LANGFUSE_HOST", "https://cloud.langfuse.com")
            if pk and sk:
                from langfuse import Langfuse
                self._langfuse = Langfuse(
                    public_key=pk,
                    secret_key=sk,
                    host=host,
                )
                return True
        except Exception:
            pass
        return False

    def _create_trace(self, scenario: str, config: dict):
        if self._langfuse:
            self._langfuse_trace = self._langfuse.trace(
                name=f"orchestrator-{scenario}",
                metadata={
                    "scenario": scenario,
                    "user_request": config.get("user_request", ""),
                },
                tags=["orchestrator", scenario],
            )
            return self._langfuse_trace.id
        return None

    def _create_span(self, task_id: str, task_name: str):
        if self._langfuse_trace:
            return self._langfuse_trace.span(
                name=task_id,
                metadata={"task_name": task_name},
            )
        return None

    def _end_span(self, span, status: str, duration: float, artifacts: dict | None = None, error: str | None = None):
        if span:
            span.end(
                metadata={
                    "status": status,
                    "duration_seconds": round(duration, 2),
                    "artifacts": artifacts or {},
                    "error": error,
                },
                level="ERROR" if error else "DEFAULT",
                status_message=error if error else "success",
            )

    def _flush_langfuse(self):
        if self._langfuse:
            try:
                self._langfuse.flush()
            except Exception:
                pass

    def build_graph(self, scenario_key: str, task_registry: dict):
        self._workflow_state.reset()
        if scenario_key in SCENARIO_WORKFLOWS:
            self._workflow_state.initialize_scenario(scenario_key)
        self._thread_id = f"thread-{uuid.uuid4().hex[:8]}"

        all_tasks = self._workflow_state.get_all_tasks()
        task_ids = [t.id for t in all_tasks]
        task_map = {t.id: t for t in all_tasks}

        dep_map = {}
        for t in all_tasks:
            dep_map[t.id] = t.dependencies

        builder = StateGraph(OrchestratorState)

        for task in all_tasks:
            task_fn = task_registry.get(task.id)
            node_fn = self._make_node(task.id, task.name, task_fn)
            builder.add_node(task.id, node_fn)

        roots = [t.id for t in all_tasks if not t.dependencies]
        if len(roots) == 1:
            builder.set_entry_point(roots[0])
        elif len(roots) > 1:
            builder.add_node("__fan_out__", lambda state: state)
            builder.set_entry_point("__fan_out__")
            for r in roots:
                builder.add_edge("__fan_out__", r)

        dependents = {}
        for t in all_tasks:
            for dep in t.dependencies:
                dependents.setdefault(dep, []).append(t.id)

        for task in all_tasks:
            children = dependents.get(task.id, [])
            if not children:
                builder.add_edge(task.id, END)
            elif len(children) == 1:
                builder.add_edge(task.id, children[0])
            else:
                for child in children:
                    builder.add_edge(task.id, child)

        parallel_groups = {}
        for task in all_tasks:
            deps_key = tuple(sorted(task.dependencies))
            parallel_groups.setdefault(deps_key, []).append(task.id)
        self._parallel_groups = {
            k: v for k, v in parallel_groups.items() if len(v) > 1
        }

        phase_ids = [p.id for p in self._workflow_state.phases]
        execute_idx = next((i for i, pid in enumerate(phase_ids) if pid == "execute"), -1)
        gate_nodes = []
        if execute_idx >= 0:
            execute_phase = self._workflow_state.phases[execute_idx]
            gate_nodes = [t.id for t in execute_phase.tasks if not any(
                d in [et.id for et in execute_phase.tasks] for d in t.dependencies
            ) or all(
                d not in [et.id for et in execute_phase.tasks] for d in t.dependencies
            )]
            if not gate_nodes:
                gate_nodes = [execute_phase.tasks[0].id] if execute_phase.tasks else []

        self._graph = builder.compile(
            checkpointer=self._checkpointer,
            interrupt_before=gate_nodes if gate_nodes else None,
        )
        return self._graph

    def _make_node(self, task_id: str, task_name: str, task_fn):
        engine = self

        async def node(state: OrchestratorState) -> dict:
            logs = []
            start = time.time()
            span = engine._create_span(task_id, task_name)

            engine._workflow_state.update_task(task_id, status=TaskStatus.RUNNING, progress=0)
            await engine._broadcast("task_started", {"taskId": task_id})

            parallel_peers = []
            for _deps_key, peer_ids in getattr(engine, '_parallel_groups', {}).items():
                if task_id in peer_ids:
                    parallel_peers = [p for p in peer_ids if p != task_id]
                    break
            par_note = f" (parallel with {', '.join(parallel_peers)})" if parallel_peers else ""

            logs.append({
                "timestamp": datetime.now().isoformat(),
                "taskId": task_id,
                "level": "info",
                "message": f"Starting: {task_name}{par_note}",
            })

            if not task_fn:
                duration = time.time() - start
                engine._workflow_state.update_task(
                    task_id, status=TaskStatus.SKIPPED, duration=duration
                )
                engine._end_span(span, "skipped", duration)
                return {
                    "task_updates": {task_id: {"status": "skipped", "duration": duration}},
                    "logs": logs,
                    "current_node": task_id,
                }

            try:
                async def log_fn(msg):
                    engine._workflow_state.add_log(task_id, "info", msg)
                    logs.append({
                        "timestamp": datetime.now().isoformat(),
                        "taskId": task_id,
                        "level": "info",
                        "message": msg,
                    })
                    await engine._broadcast("task_log", {
                        "taskId": task_id,
                        "level": "info",
                        "message": msg,
                    })

                async def progress_fn(p):
                    engine._workflow_state.update_task(task_id, progress=p)
                    await engine._broadcast("task_progress", {
                        "taskId": task_id,
                        "progress": p,
                    })

                config = state.get("config", {})
                result = await task_fn(
                    log=log_fn,
                    progress=progress_fn,
                    config=config,
                )

                duration = time.time() - start
                artifacts = result if isinstance(result, dict) else {}

                engine._workflow_state.update_task(
                    task_id,
                    status=TaskStatus.SUCCESS,
                    progress=100,
                    duration=duration,
                    artifacts=artifacts,
                )
                await engine._broadcast("task_completed", {
                    "taskId": task_id,
                    "status": "success",
                    "duration": duration,
                })

                logs.append({
                    "timestamp": datetime.now().isoformat(),
                    "taskId": task_id,
                    "level": "success",
                    "message": f"Completed in {duration:.1f}s",
                })
                engine._end_span(span, "success", duration, artifacts=artifacts)

                return {
                    "task_updates": {task_id: {
                        "status": "success",
                        "duration": duration,
                        "artifacts": artifacts,
                    }},
                    "logs": logs,
                    "current_node": task_id,
                }

            except Exception as e:
                duration = time.time() - start
                error_msg = str(e)

                engine._workflow_state.update_task(
                    task_id,
                    status=TaskStatus.FAILED,
                    error=error_msg,
                    duration=duration,
                )
                await engine._broadcast("task_completed", {
                    "taskId": task_id,
                    "status": "failed",
                    "error": error_msg,
                    "duration": duration,
                })

                logs.append({
                    "timestamp": datetime.now().isoformat(),
                    "taskId": task_id,
                    "level": "error",
                    "message": f"Failed: {error_msg}",
                })
                engine._end_span(span, "failed", duration, error=error_msg)

                return {
                    "task_updates": {task_id: {
                        "status": "failed",
                        "error": error_msg,
                        "duration": duration,
                    }},
                    "logs": logs,
                    "current_node": task_id,
                    "error": error_msg,
                }

        return node

    async def _broadcast(self, message_type: str, payload: dict):
        if self._websocket_manager:
            await self._websocket_manager.broadcast({
                "type": message_type,
                "payload": payload,
                "timestamp": datetime.now().isoformat(),
            })

    async def run(self, scenario: str, config: dict, websocket_manager=None):
        self._websocket_manager = websocket_manager
        self._workflow_state.is_running = True
        self._workflow_state.start_time = datetime.now()
        self._workflow_state.config = config

        self.init_langfuse()
        trace_id = self._create_trace(scenario, config)

        from ..tasks import SCENARIO_TASK_REGISTRY
        from ..tasks.workflow_tasks import TASK_REGISTRY

        all_fns = {**TASK_REGISTRY, **SCENARIO_TASK_REGISTRY}
        self.build_graph(scenario, all_fns)

        initial_state: OrchestratorState = {
            "scenario": scenario,
            "config": config,
            "task_updates": {},
            "logs": [],
            "is_running": True,
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "error": None,
            "current_node": None,
            "langfuse_trace_id": trace_id,
        }

        try:
            thread_config = {"configurable": {"thread_id": self._thread_id}}

            failed_tasks = []

            async def _stream_graph(input_val):
                async for ev in self._graph.astream(
                    input_val, thread_config, stream_mode="updates"
                ):
                    yield ev

            async for event in _stream_graph(initial_state):
                for node_name, node_output in event.items():
                    if node_output and isinstance(node_output, dict):
                        task_updates = node_output.get("task_updates", {})
                        for tid, upd in task_updates.items():
                            if upd.get("status") == "failed":
                                failed_tasks.append(tid)
                                task_obj = self._workflow_state.get_task(tid)
                                is_critical = task_obj and getattr(task_obj, 'skill_type', '') == 'platform'
                                if is_critical:
                                    self._workflow_state.is_running = False
                                    self._workflow_state.end_time = datetime.now()
                                    await self._broadcast("workflow_completed", {
                                        "status": "failed",
                                        "failed_task": tid,
                                        "error": upd.get("error", "Unknown"),
                                        "duration": (datetime.now() - self._workflow_state.start_time).total_seconds(),
                                    })
                                    self._flush_langfuse()
                                    return

            snapshot = self._graph.get_state(thread_config)
            while snapshot.next:
                next_nodes = list(snapshot.next)
                self._awaiting_approval = next_nodes[0]
                self._approval_event = asyncio.Event()

                execute_phase = next(
                    (p for p in self._workflow_state.phases if p.id == "execute"), None
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
                            "enabled": True,
                        })

                await self._broadcast("approval_required", {
                    "task_id": self._awaiting_approval,
                    "task_name": (self._workflow_state.get_task(self._awaiting_approval) or type('', (), {'name': self._awaiting_approval})).name,
                    "message": "Execution plan ready for review. Approve to proceed or modify steps.",
                    "pending_tasks": pending_tasks,
                })

                await self._approval_event.wait()
                self._awaiting_approval = None
                self._approval_event = None

                if not self._approval_result:
                    for nid in next_nodes:
                        self._workflow_state.update_task(nid, status=TaskStatus.SKIPPED)
                    break

                async for event in self._graph.astream(
                    None, thread_config, stream_mode="updates"
                ):
                    for node_name, node_output in event.items():
                        if node_output and isinstance(node_output, dict):
                            task_updates = node_output.get("task_updates", {})
                            for tid, upd in task_updates.items():
                                if upd.get("status") == "failed":
                                    failed_tasks.append(tid)
                                    task_obj = self._workflow_state.get_task(tid)
                                    is_critical = task_obj and getattr(task_obj, 'skill_type', '') == 'platform'
                                    if is_critical:
                                        self._workflow_state.is_running = False
                                        self._workflow_state.end_time = datetime.now()
                                        await self._broadcast("workflow_completed", {
                                            "status": "failed",
                                            "failed_task": tid,
                                        })
                                        self._flush_langfuse()
                                        return
                snapshot = self._graph.get_state(thread_config)

            final_status = "completed_with_errors" if failed_tasks else "completed"
            await self._broadcast("workflow_completed", {
                "status": final_status,
                "duration": (datetime.now() - self._workflow_state.start_time).total_seconds(),
                "failed_tasks": failed_tasks,
            })

        except Exception as e:
            self._workflow_state.add_log("orchestrator", "error", f"Workflow failed: {str(e)}")
        finally:
            self._workflow_state.is_running = False
            self._workflow_state.end_time = datetime.now()
            self._flush_langfuse()

    def to_dict(self) -> dict:
        return self._workflow_state.to_dict()

    def get_task(self, task_id: str):
        return self._workflow_state.get_task(task_id)

    def get_all_tasks(self):
        return self._workflow_state.get_all_tasks()

    def reset(self):
        self._workflow_state.reset()
        self._thread_id = None
        self._graph = None
        self._langfuse_trace = None
        self._awaiting_approval = None
        self._approval_event = None
        self._approval_result = True

    @property
    def is_running(self):
        return self._workflow_state.is_running

    @is_running.setter
    def is_running(self, val):
        self._workflow_state.is_running = val

    @property
    def phases(self):
        return self._workflow_state.phases

    @property
    def config(self):
        return self._workflow_state.config

    @property
    def start_time(self):
        return self._workflow_state.start_time

    @property
    def end_time(self):
        return self._workflow_state.end_time

    @property
    def active_scenario(self):
        return self._workflow_state.active_scenario

    def initialize_scenario(self, scenario_key: str):
        self._workflow_state.initialize_scenario(scenario_key)

    def add_log(self, task_id: str, level: str, message: str):
        self._workflow_state.add_log(task_id, level, message)

    def update_task(self, task_id: str, **kwargs):
        self._workflow_state.update_task(task_id, **kwargs)

    def get_langfuse_url(self) -> str | None:
        if self._langfuse_trace:
            return self._langfuse_trace.get_trace_url()
        return None

    def get_checkpoint(self) -> dict | None:
        if self._checkpointer and self._thread_id:
            try:
                config = {"configurable": {"thread_id": self._thread_id}}
                return self._checkpointer.get(config)
            except Exception:
                pass
        return None

    @property
    def awaiting_approval(self) -> str | None:
        return self._awaiting_approval

    def approve(self, approved: bool = True):
        self._approval_result = approved
        if self._approval_event:
            self._approval_event.set()


_engines: dict[str, LangGraphWorkflowState] = {}
_default_session = "default"


def get_engine(session_id: str | None = None) -> LangGraphWorkflowState:
    sid = session_id or _default_session
    if sid not in _engines:
        _engines[sid] = LangGraphWorkflowState()
    return _engines[sid]


def reset_engine(session_id: str | None = None):
    sid = session_id or _default_session
    if sid in _engines:
        _engines[sid].reset()
    _engines[sid] = LangGraphWorkflowState()
