"""LangGraph definition for Meta-Agent.

This module orchestrates the meta-agent workflow that takes a UseCaseConfig
and generates a complete AI application.
"""

import json
from pathlib import Path
from typing import Any

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, StateGraph

from .approval import execute_plan, human_approval, should_wait_for_approval
from .state import MetaAgentState
from .tools import generate_plan, parse_use_case, query_registry, scan_data


def create_meta_agent_graph(
    checkpointer: BaseCheckpointSaver | None = None,
) -> StateGraph:
    """Create the Meta-Agent LangGraph.

    Flow:
    START → parse_use_case → scan_data → query_registry → generate_plan
          → human_approval → execute_plan → END

    Args:
        checkpointer: Optional checkpoint saver for persistence

    Returns:
        Compiled LangGraph
    """
    workflow = StateGraph(MetaAgentState)

    workflow.add_node("parse_use_case", parse_use_case)
    workflow.add_node("scan_data", scan_data)
    workflow.add_node("query_registry", query_registry)
    workflow.add_node("generate_plan", generate_plan)
    workflow.add_node("human_approval", human_approval)
    workflow.add_node("execute_plan", execute_plan)

    workflow.set_entry_point("parse_use_case")

    workflow.add_edge("parse_use_case", "scan_data")
    workflow.add_edge("scan_data", "query_registry")
    workflow.add_edge("query_registry", "generate_plan")
    workflow.add_edge("generate_plan", "human_approval")

    workflow.add_conditional_edges(
        "human_approval",
        should_wait_for_approval,
        {
            "execute_plan": "execute_plan",
            "await_approval": END,
            "end": END,
        },
    )

    workflow.add_edge("execute_plan", END)

    return workflow.compile(checkpointer=checkpointer)


def run_meta_agent(
    use_case_description: str,
    data_locations: list | None = None,
    checkpointer: BaseCheckpointSaver | None = None,
    auto_approve: bool = False,
) -> dict[str, Any]:
    """Run the meta-agent to generate an execution plan.

    Args:
        use_case_description: Natural language description of the use case
        data_locations: List of stage paths or table names to scan
        checkpointer: Optional checkpoint saver
        auto_approve: If True, automatically approve the plan

    Returns:
        Final state containing the execution plan
    """
    graph = create_meta_agent_graph(checkpointer=checkpointer)

    initial_state: MetaAgentState = {
        "use_case_description": use_case_description,
        "data_locations": data_locations or [],
        "data_assets": [],
        "parsed_requirements": None,
        "data_profile": None,
        "available_agents": [],
        "execution_plan": None,
        "approval_status": "approved" if auto_approve else "pending",
        "approval_feedback": None,
        "current_phase": "start",
        "error": None,
        "messages": [],
    }

    config = {"configurable": {"thread_id": "meta-agent-run"}}

    result = graph.invoke(initial_state, config)

    return result


def run_from_config(
    config_path: str | Path,
    checkpointer: BaseCheckpointSaver | None = None,
    auto_approve: bool = False,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    """Run the meta-agent from a UseCaseConfig YAML file.

    This is the primary entry point for running the platform from a configuration.

    Args:
        config_path: Path to the UseCaseConfig YAML file
        checkpointer: Optional checkpoint saver for persistence
        auto_approve: If True, automatically approve the plan
        output_dir: Directory to write generated artifacts

    Returns:
        Final state containing the execution plan and generated artifacts
    """
    from src.config import load_use_case_yaml
    from src.generators import generate_app, generate_ddls

    config = load_use_case_yaml(config_path)

    use_case_description = _build_description_from_config(config)

    data_locations = []
    for asset in config.data.structured:
        data_locations.append(asset.location)
    for asset in config.data.unstructured:
        data_locations.append(asset.location)

    graph = create_meta_agent_graph(checkpointer=checkpointer)

    initial_state: MetaAgentState = {
        "use_case_description": use_case_description,
        "data_locations": data_locations,
        "data_assets": [],
        "parsed_requirements": _extract_requirements(config),
        "data_profile": _extract_data_profile(config),
        "available_agents": [a.name for a in config.agents],
        "execution_plan": _build_execution_plan(config),
        "approval_status": "approved" if auto_approve else "pending",
        "approval_feedback": None,
        "current_phase": "start",
        "error": None,
        "messages": [],
        "use_case_config": config.model_dump(),
    }

    thread_id = f"meta-agent-{config.domain.name.lower().replace(' ', '-')}"
    graph_config = {"configurable": {"thread_id": thread_id}}

    result = graph.invoke(initial_state, graph_config)

    if output_dir and result.get("approval_status") == "approved":
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        ddl_dir = output_dir / "ddl"
        generate_ddls(config, ddl_dir)
        result["generated_ddls"] = str(ddl_dir)

        generate_app(config, output_dir)
        result["generated_app"] = str(output_dir)

    return result


def _build_description_from_config(config) -> str:
    """Build a natural language description from UseCaseConfig."""
    parts = [
        f"Domain: {config.domain.name} ({config.domain.industry.value})",
        f"Description: {config.domain.description}",
        "",
        "Personas:",
    ]

    for persona in config.personas:
        parts.append(f"  - {persona.role}: {', '.join(persona.needs[:3])}")

    parts.extend(["", "Data Assets:"])
    for asset in config.data.structured:
        parts.append(f"  - {asset.name} ({asset.data_type.value}): {asset.location}")
    for asset in config.data.unstructured:
        parts.append(f"  - {asset.name} (documents): {asset.location}")

    if config.ml_models:
        parts.extend(["", "ML Requirements:"])
        for model in config.ml_models:
            parts.append(f"  - {model.name}: {model.task.value} on {model.target}")

    return "\n".join(parts)


def _extract_requirements(config) -> dict[str, Any]:
    """Extract structured requirements from UseCaseConfig."""
    return {
        "domain": config.domain.name,
        "industry": config.domain.industry.value,
        "personas": [{"role": p.role, "needs": p.needs} for p in config.personas],
        "agents_needed": [
            {"name": a.name, "type": a.agent_type.value, "purpose": a.purpose}
            for a in config.agents
        ],
        "ml_models": [
            {"name": m.name, "task": m.task.value, "target": m.target} for m in config.ml_models
        ],
        "app_features": {
            "pages": [p.name for p in config.app.pages],
            "real_time": config.app.real_time.enabled,
            "deployment_target": config.app.deployment.target,
        },
    }


def _extract_data_profile(config) -> dict[str, Any]:
    """Extract data profile from UseCaseConfig."""
    return {
        "database": config.snowflake.database,
        "schemas": {
            "raw": config.snowflake.raw_schema,
            "curated": config.snowflake.curated_schema,
            "ml": config.snowflake.ml_schema,
            "docs": config.snowflake.docs_schema,
            "cortex": config.snowflake.cortex_schema,
            "orchestrator": config.snowflake.orchestrator_schema,
        },
        "structured_assets": [
            {
                "name": a.name,
                "location": a.location,
                "type": a.data_type.value,
                "entity_column": a.entity_column,
                "time_column": a.time_column,
                "measures": a.measures,
                "label_column": a.label_column,
            }
            for a in config.data.structured
        ],
        "unstructured_assets": [
            {
                "name": a.name,
                "location": a.location,
                "doc_type": a.doc_type.value,
                "chunk_size": a.chunk_size,
            }
            for a in config.data.unstructured
        ],
    }


def _build_execution_plan(config) -> dict[str, Any]:
    """Build execution plan from UseCaseConfig."""
    phases = []

    phases.append(
        {
            "phase": "infrastructure",
            "name": "Create Snowflake Infrastructure",
            "steps": [
                {"action": "create_database", "target": config.snowflake.database},
                {
                    "action": "create_schemas",
                    "schemas": [
                        config.snowflake.raw_schema,
                        config.snowflake.curated_schema,
                        config.snowflake.ml_schema,
                        config.snowflake.docs_schema,
                        config.snowflake.cortex_schema,
                        config.snowflake.orchestrator_schema,
                    ],
                },
                {
                    "action": "create_stages",
                    "count": len(config.data.structured) + len(config.data.unstructured),
                },
                {"action": "create_tables", "count": len(config.data.structured)},
            ],
        }
    )

    if config.data.unstructured:
        phases.append(
            {
                "phase": "document_processing",
                "name": "Process Documents for RAG",
                "steps": [
                    {
                        "action": "chunk_documents",
                        "assets": [a.name for a in config.data.unstructured],
                    },
                    {"action": "create_embeddings"},
                    {
                        "action": "create_search_services",
                        "services": [s.name for s in config.cortex_services.search],
                    },
                ],
            }
        )

    if config.ml_models:
        phases.append(
            {
                "phase": "ml_models",
                "name": "Train ML Models",
                "steps": [
                    {
                        "action": "train_model",
                        "model": m.name,
                        "task": m.task.value,
                        "target": m.target,
                    }
                    for m in config.ml_models
                ],
            }
        )

    phases.append(
        {
            "phase": "app_generation",
            "name": "Generate Application Code",
            "steps": [
                {"action": "generate_backend", "framework": "fastapi"},
                {"action": "generate_frontend", "framework": "react"},
                {"action": "generate_spcs_config"},
            ],
        }
    )

    phases.append(
        {
            "phase": "deployment",
            "name": "Deploy to SPCS",
            "steps": [
                {"action": "build_docker_image"},
                {"action": "push_to_registry"},
                {"action": "create_service", "compute_pool": config.app.deployment.compute_pool},
            ],
        }
    )

    return {
        "use_case": config.domain.name,
        "total_phases": len(phases),
        "phases": phases,
    }


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--config":
        config_path = sys.argv[2] if len(sys.argv) > 2 else "config/templates/drilling_ops.yaml"
        output_dir = Path(sys.argv[3]) if len(sys.argv) > 3 else Path("./generated_app")

        print(f"Running meta-agent from config: {config_path}")
        result = run_from_config(
            config_path=config_path,
            auto_approve=True,
            output_dir=output_dir,
        )

        print("\nExecution Plan:")
        if result.get("execution_plan"):
            print(json.dumps(result["execution_plan"], indent=2))

        if result.get("generated_app"):
            print(f"\nGenerated app at: {result['generated_app']}")
    else:
        result = run_meta_agent(
            use_case_description="I need to analyze drilling reports for equipment failures and predict maintenance needs",
            data_locations=["@RAW.DATA_STAGE", "@RAW.DOCUMENTS_STAGE"],
            auto_approve=True,
        )

        print("Execution Plan:")
        if result.get("execution_plan"):
            print(json.dumps(result["execution_plan"], indent=2))
        else:
            print("No plan generated")
            print(f"Error: {result.get('error')}")
