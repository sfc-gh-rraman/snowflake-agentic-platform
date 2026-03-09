"""Workflow state management with Snowflake persistence."""

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class TaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class PhaseStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class TaskLog:
    timestamp: str
    level: str
    message: str


@dataclass
class Task:
    id: str
    name: str
    description: str
    phase: str
    status: TaskStatus = TaskStatus.PENDING
    progress: int = 0
    duration: float | None = None
    error: str | None = None
    dependencies: list[str] = field(default_factory=list)
    logs: list[TaskLog] = field(default_factory=list)
    artifacts: dict[str, Any] = field(default_factory=dict)


@dataclass
class Phase:
    id: str
    name: str
    description: str
    status: PhaseStatus = PhaseStatus.PENDING
    tasks: list[Task] = field(default_factory=list)


class WorkflowState:
    def __init__(self):
        self.phases: list[Phase] = []
        self.is_running: bool = False
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None
        self.config: dict[str, Any] = {}
        self.plan_id: str | None = None
        self._initialize_default_workflow()

    def _initialize_default_workflow(self):
        self.phases = [
            Phase(
                id="discovery",
                name="Data Discovery",
                description="Scan and profile data sources",
                tasks=[
                    Task(
                        id="scan_sources",
                        name="Scan Data Sources",
                        description="Identify tables, files, and schemas",
                        phase="discovery",
                    ),
                    Task(
                        id="profile_schema",
                        name="Profile Schema",
                        description="Analyze column types and statistics",
                        phase="discovery",
                        dependencies=["scan_sources"],
                    ),
                ],
            ),
            Phase(
                id="preprocessing",
                name="Data Preprocessing",
                description="Transform and prepare data",
                tasks=[
                    Task(
                        id="process_structured",
                        name="Process Structured Data",
                        description="Load and transform Parquet/CSV files",
                        phase="preprocessing",
                        dependencies=["profile_schema"],
                    ),
                    Task(
                        id="process_documents",
                        name="Process Documents",
                        description="Chunk and enrich unstructured documents",
                        phase="preprocessing",
                        dependencies=["profile_schema"],
                    ),
                ],
            ),
            Phase(
                id="cortex_services",
                name="Cortex Services",
                description="Deploy Cortex Search and Analyst",
                tasks=[
                    Task(
                        id="deploy_search",
                        name="Deploy Cortex Search",
                        description="Create search service over documents",
                        phase="cortex_services",
                        dependencies=["process_documents"],
                    ),
                    Task(
                        id="deploy_semantic",
                        name="Deploy Semantic Model",
                        description="Create semantic model for Analyst",
                        phase="cortex_services",
                        dependencies=["process_structured"],
                    ),
                ],
            ),
            Phase(
                id="ml_models",
                name="ML Models",
                description="Train and register ML models",
                tasks=[
                    Task(
                        id="feature_engineering",
                        name="Feature Engineering",
                        description="Generate features from data",
                        phase="ml_models",
                        dependencies=["process_structured"],
                    ),
                    Task(
                        id="train_models",
                        name="Train Models",
                        description="Train and evaluate ML models",
                        phase="ml_models",
                        dependencies=["feature_engineering"],
                    ),
                    Task(
                        id="register_models",
                        name="Register Models",
                        description="Register models in ML Registry",
                        phase="ml_models",
                        dependencies=["train_models"],
                    ),
                ],
            ),
            Phase(
                id="deployment",
                name="App Deployment",
                description="Generate and deploy application",
                tasks=[
                    Task(
                        id="generate_app",
                        name="Generate App Code",
                        description="Generate React + FastAPI application",
                        phase="deployment",
                        dependencies=[
                            "deploy_search",
                            "deploy_semantic",
                            "register_models",
                        ],
                    ),
                    Task(
                        id="deploy_spcs",
                        name="Deploy to SPCS",
                        description="Deploy application to Snowpark Container Services",
                        phase="deployment",
                        dependencies=["generate_app"],
                    ),
                ],
            ),
        ]

    def reset(self):
        self._initialize_default_workflow()
        self.is_running = False
        self.start_time = None
        self.end_time = None
        self.config = {}

    def get_task(self, task_id: str) -> Task | None:
        for phase in self.phases:
            for task in phase.tasks:
                if task.id == task_id:
                    return task
        return None

    def get_all_tasks(self) -> list[Task]:
        tasks = []
        for phase in self.phases:
            tasks.extend(phase.tasks)
        return tasks

    def update_task(
        self,
        task_id: str,
        status: TaskStatus | None = None,
        progress: int | None = None,
        duration: float | None = None,
        error: str | None = None,
        artifacts: dict | None = None,
    ):
        task = self.get_task(task_id)
        if task:
            if status is not None:
                task.status = status
            if progress is not None:
                task.progress = progress
            if duration is not None:
                task.duration = duration
            if error is not None:
                task.error = error
            if artifacts is not None:
                task.artifacts.update(artifacts)
            self._update_phase_status(task.phase)

    def _update_phase_status(self, phase_id: str):
        for phase in self.phases:
            if phase.id == phase_id:
                statuses = [t.status for t in phase.tasks]
                if all(s == TaskStatus.SUCCESS for s in statuses):
                    phase.status = PhaseStatus.COMPLETED
                elif any(s == TaskStatus.FAILED for s in statuses):
                    phase.status = PhaseStatus.FAILED
                elif any(s == TaskStatus.RUNNING for s in statuses):
                    phase.status = PhaseStatus.RUNNING
                break

    def add_log(self, task_id: str, level: str, message: str):
        task = self.get_task(task_id)
        if task:
            task.logs.append(
                TaskLog(
                    timestamp=datetime.now().isoformat(),
                    level=level,
                    message=message,
                )
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "phases": [
                {
                    "id": p.id,
                    "name": p.name,
                    "description": p.description,
                    "status": p.status.value,
                    "tasks": [
                        {
                            "id": t.id,
                            "name": t.name,
                            "description": t.description,
                            "status": t.status.value,
                            "progress": t.progress,
                            "duration": t.duration,
                            "error": t.error,
                            "dependencies": t.dependencies,
                            "logs": [
                                {
                                    "timestamp": log.timestamp,
                                    "level": log.level,
                                    "message": log.message,
                                }
                                for log in t.logs
                            ],
                            "artifacts": t.artifacts,
                        }
                        for t in p.tasks
                    ],
                }
                for p in self.phases
            ],
            "is_running": self.is_running,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "config": self.config,
            "plan_id": self.plan_id,
        }

    def persist_to_snowflake(self, session) -> bool:
        """Persist workflow state to Snowflake table."""
        try:
            state_json = json.dumps(self.to_dict())
            escaped = state_json.replace("'", "''")
            session.sql(
                f"""
                MERGE INTO AGENTIC_PLATFORM.ORCHESTRATOR.WORKFLOW_STATE t
                USING (SELECT '{self.plan_id or "default"}' as plan_id) s
                ON t.plan_id = s.plan_id
                WHEN MATCHED THEN UPDATE SET
                    state_json = '{escaped}',
                    updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (plan_id, state_json, updated_at)
                    VALUES (s.plan_id, '{escaped}', CURRENT_TIMESTAMP())
            """
            ).collect()
            return True
        except Exception as e:
            print(f"Failed to persist state: {e}")
            return False

    def load_from_snowflake(self, session, plan_id: str) -> bool:
        """Load workflow state from Snowflake table."""
        try:
            result = session.sql(
                f"""
                SELECT state_json FROM AGENTIC_PLATFORM.ORCHESTRATOR.WORKFLOW_STATE
                WHERE plan_id = '{plan_id}'
            """
            ).collect()
            if result:
                state_data = json.loads(result[0]["STATE_JSON"])
                self._load_from_dict(state_data)
                return True
            return False
        except Exception as e:
            print(f"Failed to load state: {e}")
            return False

    def _load_from_dict(self, data: dict[str, Any]):
        self.is_running = data.get("is_running", False)
        self.plan_id = data.get("plan_id")
        self.config = data.get("config", {})
        if data.get("start_time"):
            self.start_time = datetime.fromisoformat(data["start_time"])
        if data.get("end_time"):
            self.end_time = datetime.fromisoformat(data["end_time"])


workflow_state = WorkflowState()
