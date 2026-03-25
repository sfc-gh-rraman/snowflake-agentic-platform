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
    skill_name: str | None = None
    skill_type: str | None = None
    preflight_status: str | None = None
    governance: dict[str, Any] | None = None


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
        self.session_id: str | None = None
        self.detected_domain: str | None = None
        self._initialize_default_workflow()

    def _initialize_default_workflow(self):
        self.phases = [
            Phase(
                id="preflight",
                name="Preflight Check",
                description="Verify Snowflake dependencies and data availability",
                tasks=[
                    Task(
                        id="check_fhir_tables",
                        name="Verify FHIR Tables",
                        description="Check Patient, Observation, Condition tables exist",
                        phase="preflight",
                        skill_name="preflight-checker",
                        skill_type="infrastructure",
                    ),
                    Task(
                        id="check_observability",
                        name="Verify Observability",
                        description="Check execution log tables exist",
                        phase="preflight",
                        skill_name="preflight-checker",
                        skill_type="infrastructure",
                    ),
                    Task(
                        id="check_cke",
                        name="Check CKE Availability",
                        description="Probe PubMed/ClinicalTrials Marketplace listings",
                        phase="preflight",
                        skill_name="preflight-checker",
                        skill_type="infrastructure",
                        dependencies=["check_fhir_tables"],
                    ),
                ],
            ),
            Phase(
                id="plan",
                name="Plan Generation",
                description="Detect domain and build execution plan",
                tasks=[
                    Task(
                        id="detect_domain",
                        name="Detect Domain",
                        description="Route request to Provider > Clinical Data Management",
                        phase="plan",
                        skill_name="orchestrator",
                        skill_type="routing",
                        dependencies=["check_fhir_tables", "check_observability"],
                    ),
                    Task(
                        id="generate_plan",
                        name="Generate Plan",
                        description="Build 5-step execution plan with skill assignments",
                        phase="plan",
                        skill_name="orchestrator",
                        skill_type="routing",
                        dependencies=["detect_domain"],
                    ),
                    Task(
                        id="approve_plan",
                        name="Approve Plan",
                        description="Auto-approve plan (demo mode)",
                        phase="plan",
                        skill_name="orchestrator",
                        skill_type="routing",
                        dependencies=["generate_plan"],
                    ),
                ],
            ),
            Phase(
                id="execute",
                name="Skill Execution",
                description="Execute healthcare and platform skills",
                tasks=[
                    Task(
                        id="verify_fhir",
                        name="Verify FHIR Data",
                        description="Check FHIR tables are loaded and structured correctly",
                        phase="execute",
                        skill_name="hcls-provider-cdata-fhir",
                        skill_type="standalone",
                        dependencies=["approve_plan"],
                    ),
                    Task(
                        id="validate_quality",
                        name="Validate Data Quality",
                        description="Run completeness, schema, and semantic validation",
                        phase="execute",
                        skill_name="hcls-cross-validation",
                        skill_type="standalone",
                        dependencies=["verify_fhir"],
                    ),
                    Task(
                        id="verify_governance",
                        name="Verify PHI Masking",
                        description="Check HIPAA masking policies via IS_ROLE_IN_SESSION()",
                        phase="execute",
                        skill_name="data-governance",
                        skill_type="platform",
                        dependencies=["validate_quality"],
                    ),
                    Task(
                        id="post_governance_check",
                        name="Post-Governance Check",
                        description="Verify masking is active on PHI columns",
                        phase="execute",
                        skill_name="hcls-cross-validation",
                        skill_type="standalone",
                        dependencies=["verify_governance"],
                    ),
                    Task(
                        id="create_analytics",
                        name="Create Analytics View",
                        description="Join Patient + Observation + Condition into analytics view",
                        phase="execute",
                        skill_name="semantic-view",
                        skill_type="platform",
                        dependencies=["post_governance_check"],
                    ),
                ],
            ),
            Phase(
                id="summary",
                name="Execution Summary",
                description="Log results and produce final report",
                tasks=[
                    Task(
                        id="log_results",
                        name="Log to Observability",
                        description="Write execution logs to Snowflake observability tables",
                        phase="summary",
                        skill_name="observability",
                        skill_type="infrastructure",
                        dependencies=["create_analytics"],
                    ),
                    Task(
                        id="final_report",
                        name="Generate Report",
                        description="Produce validation summary and governance audit",
                        phase="summary",
                        skill_name="orchestrator",
                        skill_type="routing",
                        dependencies=["log_results"],
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
        self.session_id = None
        self.detected_domain = None

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
        preflight_status: str | None = None,
        governance: dict | None = None,
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
            if preflight_status is not None:
                task.preflight_status = preflight_status
            if governance is not None:
                task.governance = governance
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
                            "skill_name": t.skill_name,
                            "skill_type": t.skill_type,
                            "preflight_status": t.preflight_status,
                            "governance": t.governance,
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
            "session_id": self.session_id,
            "detected_domain": self.detected_domain,
        }


workflow_state = WorkflowState()
