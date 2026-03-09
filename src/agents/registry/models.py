"""Data models for Agent Registry."""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class AgentCategory(StrEnum):
    DISCOVERY = "discovery"
    PREPROCESSING = "preprocessing"
    VALIDATION = "validation"
    ML = "ml"
    SEARCH = "search"
    SEMANTIC = "semantic"
    APP_GENERATION = "app_generation"
    DEPLOYMENT = "deployment"
    OBSERVABILITY = "observability"


class DependencyRelationship(StrEnum):
    REQUIRES = "requires"
    OPTIONAL = "optional"
    CONFLICTS = "conflicts"


@dataclass
class AgentCapability:
    capability_id: str
    name: str
    input_types: list[str]
    output_types: list[str]
    description: str | None = None
    constraints: dict[str, Any] | None = None


@dataclass
class AgentDependency:
    agent_id: str
    relationship: DependencyRelationship
    description: str | None = None


@dataclass
class AgentTrigger:
    condition: str
    priority: int
    description: str | None = None


@dataclass
class StateMachine:
    states: list[str]
    initial_state: str
    transitions: list[dict[str, str]]


@dataclass
class AgentDefinition:
    agent_id: str
    name: str
    version: str
    description: str
    category: AgentCategory
    capabilities: list[AgentCapability]
    dependencies: list[AgentDependency] = field(default_factory=list)
    triggers: list[AgentTrigger] = field(default_factory=list)
    config_schema: dict[str, Any] | None = None
    state_machine: StateMachine | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "category": self.category.value,
            "capabilities": [
                {
                    "capability_id": c.capability_id,
                    "name": c.name,
                    "description": c.description,
                    "input_types": c.input_types,
                    "output_types": c.output_types,
                    "constraints": c.constraints,
                }
                for c in self.capabilities
            ],
            "dependencies": [
                {
                    "agent_id": d.agent_id,
                    "relationship": d.relationship.value,
                    "description": d.description,
                }
                for d in self.dependencies
            ],
            "triggers": [
                {
                    "condition": t.condition,
                    "priority": t.priority,
                    "description": t.description,
                }
                for t in self.triggers
            ],
            "config_schema": self.config_schema,
            "state_machine": {
                "states": self.state_machine.states,
                "initial_state": self.state_machine.initial_state,
                "transitions": self.state_machine.transitions,
            }
            if self.state_machine
            else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AgentDefinition":
        capabilities = [
            AgentCapability(
                capability_id=c["capability_id"],
                name=c["name"],
                description=c.get("description"),
                input_types=c["input_types"],
                output_types=c["output_types"],
                constraints=c.get("constraints"),
            )
            for c in data.get("capabilities", [])
        ]

        dependencies = [
            AgentDependency(
                agent_id=d["agent_id"],
                relationship=DependencyRelationship(d["relationship"]),
                description=d.get("description"),
            )
            for d in data.get("dependencies", [])
        ]

        triggers = [
            AgentTrigger(
                condition=t["condition"],
                priority=t["priority"],
                description=t.get("description"),
            )
            for t in data.get("triggers", [])
        ]

        state_machine = None
        if data.get("state_machine"):
            sm = data["state_machine"]
            state_machine = StateMachine(
                states=sm["states"],
                initial_state=sm["initial_state"],
                transitions=sm["transitions"],
            )

        return cls(
            agent_id=data["agent_id"],
            name=data["name"],
            version=data["version"],
            description=data["description"],
            category=AgentCategory(data["category"]),
            capabilities=capabilities,
            dependencies=dependencies,
            triggers=triggers,
            config_schema=data.get("config_schema"),
            state_machine=state_machine,
        )


@dataclass
class AgentSearchResult:
    agent_id: str
    agent_name: str
    capability_name: str
    score: float
    input_types: list[str]
    output_types: list[str]
    full_definition: dict[str, Any]
