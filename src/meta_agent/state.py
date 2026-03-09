"""State definition for Meta-Agent LangGraph."""

from typing import Any, Dict, List, Literal, Optional, TypedDict
from dataclasses import dataclass, field
from enum import Enum


class DataType(str, Enum):
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    PDF = "pdf"
    DOCX = "docx"
    TXT = "txt"
    TABLE = "table"
    UNKNOWN = "unknown"


class TaskType(str, Enum):
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    CLUSTERING = "clustering"
    RAG = "rag"
    ANALYTICS = "analytics"
    SEARCH = "search"


@dataclass
class DataAsset:
    name: str
    location: str
    data_type: DataType
    size_bytes: Optional[int] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    schema: Optional[Dict[str, str]] = None
    metadata: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "location": self.location,
            "data_type": self.data_type.value,
            "size_bytes": self.size_bytes,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "schema": self.schema,
            "metadata": self.metadata,
        }


@dataclass
class ParsedRequirements:
    primary_task: TaskType
    secondary_tasks: List[TaskType] = field(default_factory=list)
    target_variable: Optional[str] = None
    search_enabled: bool = False
    analytics_enabled: bool = False
    ml_enabled: bool = False
    app_type: Optional[str] = None
    deployment_target: str = "spcs"
    entities: List[str] = field(default_factory=list)
    key_features: List[str] = field(default_factory=list)
    constraints: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "primary_task": self.primary_task.value,
            "secondary_tasks": [t.value for t in self.secondary_tasks],
            "target_variable": self.target_variable,
            "search_enabled": self.search_enabled,
            "analytics_enabled": self.analytics_enabled,
            "ml_enabled": self.ml_enabled,
            "app_type": self.app_type,
            "deployment_target": self.deployment_target,
            "entities": self.entities,
            "key_features": self.key_features,
            "constraints": self.constraints,
        }


@dataclass
class DataProfile:
    total_assets: int
    structured_count: int
    unstructured_count: int
    total_rows: int
    total_size_bytes: int
    has_labeled_data: bool
    potential_target_columns: List[str]
    potential_features: List[str]
    text_content_detected: bool
    profiles: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_assets": self.total_assets,
            "structured_count": self.structured_count,
            "unstructured_count": self.unstructured_count,
            "total_rows": self.total_rows,
            "total_size_bytes": self.total_size_bytes,
            "has_labeled_data": self.has_labeled_data,
            "potential_target_columns": self.potential_target_columns,
            "potential_features": self.potential_features,
            "text_content_detected": self.text_content_detected,
            "profiles": self.profiles,
        }


@dataclass
class AgentCapability:
    agent_id: str
    name: str
    capability_name: str
    input_types: List[str]
    output_types: List[str]
    priority: int = 50

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "name": self.name,
            "capability_name": self.capability_name,
            "input_types": self.input_types,
            "output_types": self.output_types,
            "priority": self.priority,
        }


@dataclass
class ExecutionPhase:
    phase_id: str
    phase_name: str
    phase_order: int
    agent_id: str
    agent_name: str
    config: Dict[str, Any]
    depends_on: List[str] = field(default_factory=list)
    expected_outputs: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "phase_id": self.phase_id,
            "phase_name": self.phase_name,
            "phase_order": self.phase_order,
            "agent_id": self.agent_id,
            "agent_name": self.agent_name,
            "config": self.config,
            "depends_on": self.depends_on,
            "expected_outputs": self.expected_outputs,
        }


@dataclass
class ExecutionPlan:
    plan_id: str
    name: str
    description: str
    phases: List[ExecutionPhase]
    estimated_duration_minutes: int
    total_phases: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "name": self.name,
            "description": self.description,
            "phases": [p.to_dict() for p in self.phases],
            "estimated_duration_minutes": self.estimated_duration_minutes,
            "total_phases": self.total_phases,
        }


class MetaAgentState(TypedDict):
    """LangGraph state for Meta-Agent."""
    
    use_case_description: str
    data_locations: List[str]
    
    data_assets: List[Dict[str, Any]]
    parsed_requirements: Optional[Dict[str, Any]]
    data_profile: Optional[Dict[str, Any]]
    available_agents: List[Dict[str, Any]]
    execution_plan: Optional[Dict[str, Any]]
    
    approval_status: Literal["pending", "approved", "rejected"]
    approval_feedback: Optional[str]
    
    current_phase: str
    error: Optional[str]
    messages: List[Dict[str, Any]]
    
    use_case_config: Optional[Dict[str, Any]]
    generated_ddls: Optional[str]
    generated_app: Optional[str]
