"""Agent registry query tool - finds agents by capability."""

from typing import Any

from ...agents.registry import AgentRegistryQuery
from ..state import AgentCapability


class AgentRegistryQueryTool:
    """Query the agent registry to find capable agents for the task."""

    def __init__(
        self,
        connection_name: str | None = None,
        database: str = "AGENTIC_PLATFORM",
    ):
        self.registry = AgentRegistryQuery(
            connection_name=connection_name,
            database=database,
        )

    def find_agents(
        self,
        requirements: dict[str, Any],
        data_profile: dict[str, Any],
    ) -> list[AgentCapability]:
        capabilities = []

        if data_profile.get("structured_count", 0) > 0:
            parquet_results = self.registry.search_agents(
                query="process parquet files ingest data",
                input_type="parquet",
                limit=3,
            )
            for r in parquet_results:
                capabilities.append(
                    AgentCapability(
                        agent_id=r.agent_id,
                        name=r.agent_name,
                        capability_name=r.capability_name,
                        input_types=r.input_types,
                        output_types=r.output_types,
                        priority=80,
                    )
                )

        if data_profile.get("unstructured_count", 0) > 0 or data_profile.get(
            "text_content_detected"
        ):
            doc_results = self.registry.search_agents(
                query="chunk documents extract pdf text",
                input_type="pdf",
                limit=3,
            )
            for r in doc_results:
                capabilities.append(
                    AgentCapability(
                        agent_id=r.agent_id,
                        name=r.agent_name,
                        capability_name=r.capability_name,
                        input_types=r.input_types,
                        output_types=r.output_types,
                        priority=75,
                    )
                )

        if requirements.get("ml_enabled") or data_profile.get("has_labeled_data"):
            ml_results = self.registry.search_agents(
                query="train model classification regression",
                category="ml",
                limit=3,
            )
            for r in ml_results:
                capabilities.append(
                    AgentCapability(
                        agent_id=r.agent_id,
                        name=r.agent_name,
                        capability_name=r.capability_name,
                        input_types=r.input_types,
                        output_types=r.output_types,
                        priority=70,
                    )
                )

        if requirements.get("search_enabled") or data_profile.get("text_content_detected"):
            search_results = self.registry.search_agents(
                query="cortex search service create",
                category="search",
                limit=3,
            )
            for r in search_results:
                capabilities.append(
                    AgentCapability(
                        agent_id=r.agent_id,
                        name=r.agent_name,
                        capability_name=r.capability_name,
                        input_types=r.input_types,
                        output_types=r.output_types,
                        priority=65,
                    )
                )

        if requirements.get("analytics_enabled"):
            semantic_results = self.registry.search_agents(
                query="semantic model generate analytics",
                category="semantic",
                limit=3,
            )
            for r in semantic_results:
                capabilities.append(
                    AgentCapability(
                        agent_id=r.agent_id,
                        name=r.agent_name,
                        capability_name=r.capability_name,
                        input_types=r.input_types,
                        output_types=r.output_types,
                        priority=60,
                    )
                )

        if requirements.get("app_type"):
            app_results = self.registry.search_agents(
                query="generate app code react fastapi",
                category="app_generation",
                limit=3,
            )
            for r in app_results:
                capabilities.append(
                    AgentCapability(
                        agent_id=r.agent_id,
                        name=r.agent_name,
                        capability_name=r.capability_name,
                        input_types=r.input_types,
                        output_types=r.output_types,
                        priority=50,
                    )
                )

        seen = set()
        unique_capabilities = []
        for cap in capabilities:
            key = (cap.agent_id, cap.capability_name)
            if key not in seen:
                seen.add(key)
                unique_capabilities.append(cap)

        return sorted(unique_capabilities, key=lambda x: -x.priority)


def query_registry(state: dict[str, Any]) -> dict[str, Any]:
    """LangGraph node function to query agent registry."""
    tool = AgentRegistryQueryTool()

    requirements = state.get("parsed_requirements", {})
    data_profile = state.get("data_profile", {})

    capabilities = tool.find_agents(requirements, data_profile)

    return {
        "available_agents": [c.to_dict() for c in capabilities],
        "current_phase": "generate_plan",
        "messages": state.get("messages", [])
        + [
            {
                "role": "system",
                "content": f"Found {len(capabilities)} capable agents for this task",
            }
        ],
    }
