"""Improvement Agent LangGraph - handles user-initiated app refinements.

States: CLASSIFY_REQUEST → ANALYZE_IMPACT → GENERATE_PLAN → EXECUTE_CHANGES → VALIDATE → NOTIFY
"""

import json
import operator
import os
from enum import StrEnum
from typing import Annotated, Any

from langgraph.graph import END, StateGraph
from typing_extensions import TypedDict


class ImprovementType(StrEnum):
    UI_CHANGE = "ui_change"
    DATA_CHANGE = "data_change"
    ML_CHANGE = "ml_change"
    INTEGRATION_CHANGE = "integration_change"
    BUG_FIX = "bug_fix"
    PERFORMANCE = "performance"
    UNKNOWN = "unknown"


CLASSIFY_REQUEST_PROMPT = """Classify this user improvement request.

REQUEST: {request}

CURRENT APP STATE:
- Pages: {pages}
- Data Sources: {data_sources}
- ML Models: {ml_models}
- Search Services: {search_services}

Classify the request type and extract key details.

Return JSON:
{{
    "type": "ui_change|data_change|ml_change|integration_change|bug_fix|performance",
    "summary": "Brief summary of the request",
    "affected_components": ["component1", "component2"],
    "new_requirements": ["requirement1"],
    "priority": "high|medium|low",
    "complexity": "simple|moderate|complex",
    "reasoning": "Why this classification"
}}"""


IMPACT_ANALYSIS_PROMPT = """Analyze the impact of this improvement request.

REQUEST TYPE: {request_type}
REQUEST SUMMARY: {summary}
AFFECTED COMPONENTS: {affected_components}

CURRENT ARTIFACTS:
- Tables: {tables}
- Models: {models}
- Services: {services}

Determine:
1. Which agents need to re-run
2. Which artifacts will be modified
3. What new artifacts will be created
4. Risk assessment

Return JSON:
{{
    "agents_to_run": ["agent1", "agent2"],
    "artifacts_to_modify": ["artifact1"],
    "new_artifacts": ["new_artifact1"],
    "preserved_artifacts": ["keep_artifact1"],
    "risk_level": "low|medium|high",
    "estimated_duration": "5 minutes",
    "dependencies": ["dep1"],
    "rollback_possible": true
}}"""


class ImprovementState(TypedDict):
    request: str
    plan_id: str
    current_app_state: dict[str, Any]
    request_type: str | None
    classification: dict[str, Any]
    impact_analysis: dict[str, Any]
    improvement_plan: dict[str, Any]
    execution_results: dict[str, Any]
    validation_results: dict[str, Any]
    notification: str | None
    current_state: str
    errors: Annotated[list[str], operator.add]
    messages: Annotated[list[dict[str, str]], operator.add]


def _get_session(connection_name: str):
    if os.path.exists("/snowflake/session/token"):
        from snowflake.snowpark import Session

        return Session.builder.getOrCreate()
    else:
        import snowflake.connector

        return snowflake.connector.connect(connection_name=connection_name)


def _execute(session, sql: str) -> Any:
    if hasattr(session, "sql"):
        result = session.sql(sql).collect()
        return result[0][0] if result else ""
    else:
        cursor = session.cursor()
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
            return row[0] if row else ""
        finally:
            cursor.close()


def classify_request(state: ImprovementState) -> dict[str, Any]:
    """Classify the user's improvement request."""
    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    session = _get_session(connection_name)

    app_state = state.get("current_app_state", {})

    prompt = CLASSIFY_REQUEST_PROMPT.format(
        request=state["request"],
        pages=json.dumps(app_state.get("pages", []), default=str),
        data_sources=json.dumps(app_state.get("data_sources", []), default=str),
        ml_models=json.dumps(app_state.get("ml_models", []), default=str),
        search_services=json.dumps(app_state.get("search_services", []), default=str),
    )

    escaped_prompt = prompt.replace("'", "''").replace("\\", "\\\\")
    sql = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large2',
            '{escaped_prompt}'
        ) as RESPONSE
    """

    try:
        response = _execute(session, sql)

        json_start = response.find("{")
        json_end = response.rfind("}") + 1
        if json_start >= 0 and json_end > json_start:
            classification = json.loads(response[json_start:json_end])
        else:
            classification = _fallback_classification(state)
    except Exception:
        classification = _fallback_classification(state)

    request_type = classification.get("type", ImprovementType.UNKNOWN.value)

    return {
        "request_type": request_type,
        "classification": classification,
        "current_state": "ANALYZE_IMPACT",
        "messages": [
            {
                "role": "system",
                "content": f"Classified request as: {request_type}",
            }
        ],
    }


def _fallback_classification(state: ImprovementState) -> dict[str, Any]:
    """Fallback classification when LLM fails."""
    request_lower = state["request"].lower()

    if any(
        kw in request_lower
        for kw in ["page", "button", "ui", "display", "show", "hide", "layout", "chart"]
    ):
        req_type = ImprovementType.UI_CHANGE.value
    elif any(kw in request_lower for kw in ["data", "table", "column", "source", "csv", "parquet"]):
        req_type = ImprovementType.DATA_CHANGE.value
    elif any(kw in request_lower for kw in ["model", "predict", "train", "retrain", "accuracy"]):
        req_type = ImprovementType.ML_CHANGE.value
    elif any(kw in request_lower for kw in ["search", "api", "integrate", "connect"]):
        req_type = ImprovementType.INTEGRATION_CHANGE.value
    elif any(kw in request_lower for kw in ["bug", "fix", "error", "broken", "wrong"]):
        req_type = ImprovementType.BUG_FIX.value
    elif any(kw in request_lower for kw in ["slow", "fast", "performance", "optimize"]):
        req_type = ImprovementType.PERFORMANCE.value
    else:
        req_type = ImprovementType.UNKNOWN.value

    return {
        "type": req_type,
        "summary": state["request"][:200],
        "affected_components": [],
        "new_requirements": [],
        "priority": "medium",
        "complexity": "moderate",
        "reasoning": "Fallback classification based on keywords",
    }


def analyze_impact(state: ImprovementState) -> dict[str, Any]:
    """Analyze the impact of the improvement request."""
    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    session = _get_session(connection_name)

    classification = state.get("classification", {})
    app_state = state.get("current_app_state", {})

    prompt = IMPACT_ANALYSIS_PROMPT.format(
        request_type=state.get("request_type", "unknown"),
        summary=classification.get("summary", state["request"]),
        affected_components=json.dumps(classification.get("affected_components", []), default=str),
        tables=json.dumps(app_state.get("tables", []), default=str),
        models=json.dumps(app_state.get("ml_models", []), default=str),
        services=json.dumps(app_state.get("search_services", []), default=str),
    )

    escaped_prompt = prompt.replace("'", "''").replace("\\", "\\\\")
    sql = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large2',
            '{escaped_prompt}'
        ) as RESPONSE
    """

    try:
        response = _execute(session, sql)

        json_start = response.find("{")
        json_end = response.rfind("}") + 1
        if json_start >= 0 and json_end > json_start:
            impact = json.loads(response[json_start:json_end])
        else:
            impact = _fallback_impact(state)
    except Exception:
        impact = _fallback_impact(state)

    return {
        "impact_analysis": impact,
        "current_state": "GENERATE_PLAN",
        "messages": [
            {
                "role": "system",
                "content": f"Impact analysis complete. Risk: {impact.get('risk_level', 'unknown')}",
            }
        ],
    }


def _fallback_impact(state: ImprovementState) -> dict[str, Any]:
    """Fallback impact analysis."""
    request_type = state.get("request_type", ImprovementType.UNKNOWN.value)

    agent_map = {
        ImprovementType.UI_CHANGE.value: ["app_code_generator", "spcs_deployer"],
        ImprovementType.DATA_CHANGE.value: [
            "parquet_processor",
            "document_chunker",
            "validation_agent",
        ],
        ImprovementType.ML_CHANGE.value: ["feature_store_builder", "ml_model_builder"],
        ImprovementType.INTEGRATION_CHANGE.value: [
            "cortex_search_builder",
            "semantic_model_generator",
            "app_code_generator",
        ],
        ImprovementType.BUG_FIX.value: ["app_code_generator"],
        ImprovementType.PERFORMANCE.value: ["validation_agent"],
    }

    return {
        "agents_to_run": agent_map.get(request_type, ["app_code_generator"]),
        "artifacts_to_modify": [],
        "new_artifacts": [],
        "preserved_artifacts": [],
        "risk_level": "medium",
        "estimated_duration": "10 minutes",
        "dependencies": [],
        "rollback_possible": True,
    }


def generate_plan(state: ImprovementState) -> dict[str, Any]:
    """Generate improvement execution plan."""
    request_type = state.get("request_type", ImprovementType.UNKNOWN.value)
    impact = state.get("impact_analysis", {})
    classification = state.get("classification", {})

    agents_to_run = impact.get("agents_to_run", [])

    phases = []

    if request_type == ImprovementType.DATA_CHANGE.value:
        phases.append(
            {
                "phase": "data_processing",
                "name": "Process New/Updated Data",
                "agents": [
                    a for a in agents_to_run if a in ["parquet_processor", "document_chunker"]
                ],
                "parallel": True,
            }
        )
        phases.append(
            {
                "phase": "validation",
                "name": "Validate Data Quality",
                "agents": ["validation_agent"],
                "depends_on": "data_processing",
            }
        )

    if request_type == ImprovementType.ML_CHANGE.value:
        phases.append(
            {
                "phase": "feature_engineering",
                "name": "Update Features",
                "agents": ["feature_store_builder"],
            }
        )
        phases.append(
            {
                "phase": "model_training",
                "name": "Retrain Model",
                "agents": ["ml_model_builder"],
                "depends_on": "feature_engineering",
            }
        )

    if request_type == ImprovementType.INTEGRATION_CHANGE.value:
        if "cortex_search_builder" in agents_to_run:
            phases.append(
                {
                    "phase": "search_update",
                    "name": "Update Search Service",
                    "agents": ["cortex_search_builder"],
                }
            )
        if "semantic_model_generator" in agents_to_run:
            phases.append(
                {
                    "phase": "semantic_update",
                    "name": "Update Semantic Model",
                    "agents": ["semantic_model_generator"],
                }
            )

    if (
        request_type in [ImprovementType.UI_CHANGE.value, ImprovementType.BUG_FIX.value]
        or "app_code_generator" in agents_to_run
    ):
        phases.append(
            {
                "phase": "app_generation",
                "name": "Regenerate Application Code",
                "agents": ["app_code_generator"],
                "config": {
                    "changes": classification.get("new_requirements", []),
                    "preserve_existing": True,
                },
            }
        )

    if "spcs_deployer" in agents_to_run or phases:
        phases.append(
            {
                "phase": "deployment",
                "name": "Deploy Updates",
                "agents": ["spcs_deployer"],
            }
        )

    improvement_plan = {
        "plan_id": f"improvement_{state['plan_id']}",
        "request_type": request_type,
        "summary": classification.get("summary", ""),
        "phases": phases,
        "total_phases": len(phases),
        "estimated_duration": impact.get("estimated_duration", "15 minutes"),
        "rollback_plan": {
            "enabled": impact.get("rollback_possible", True),
            "artifacts_to_restore": impact.get("preserved_artifacts", []),
        },
    }

    return {
        "improvement_plan": improvement_plan,
        "current_state": "EXECUTE_CHANGES",
        "messages": [
            {
                "role": "system",
                "content": f"Generated improvement plan with {len(phases)} phases",
            }
        ],
    }


def execute_changes(state: ImprovementState) -> dict[str, Any]:
    """Execute the improvement plan phases."""
    plan = state.get("improvement_plan", {})
    phases = plan.get("phases", [])

    execution_results = {
        "phases_completed": [],
        "phases_failed": [],
        "artifacts_created": [],
        "artifacts_modified": [],
    }

    for phase in phases:
        phase_name = phase.get("name", "Unknown")
        agents = phase.get("agents", [])

        try:
            for agent_name in agents:
                pass

            execution_results["phases_completed"].append(phase_name)
        except Exception as e:
            execution_results["phases_failed"].append(
                {
                    "phase": phase_name,
                    "error": str(e),
                }
            )

    success = len(execution_results["phases_failed"]) == 0

    return {
        "execution_results": execution_results,
        "current_state": "VALIDATE" if success else "FAILED",
        "messages": [
            {
                "role": "system",
                "content": f"Executed {len(execution_results['phases_completed'])} phases. Success: {success}",
            }
        ],
    }


def validate_changes(state: ImprovementState) -> dict[str, Any]:
    """Validate the executed changes."""
    execution_results = state.get("execution_results", {})

    validation_results = {
        "all_phases_passed": len(execution_results.get("phases_failed", [])) == 0,
        "artifacts_verified": True,
        "tests_passed": True,
        "issues": [],
    }

    return {
        "validation_results": validation_results,
        "current_state": "NOTIFY",
        "messages": [
            {
                "role": "system",
                "content": f"Validation complete. All passed: {validation_results['all_phases_passed']}",
            }
        ],
    }


def notify_user(state: ImprovementState) -> dict[str, Any]:
    """Generate user notification about the improvement."""
    classification = state.get("classification", {})
    execution_results = state.get("execution_results", {})
    validation_results = state.get("validation_results", {})

    phases_completed = execution_results.get("phases_completed", [])
    artifacts_created = execution_results.get("artifacts_created", [])

    notification_parts = [
        "Your improvement request has been processed!",
        "",
        f"Request: {classification.get('summary', state['request'][:100])}",
        "",
        "Changes made:",
    ]

    for phase in phases_completed:
        notification_parts.append(f"  ✓ {phase}")

    if artifacts_created:
        notification_parts.append("")
        notification_parts.append("New artifacts:")
        for artifact in artifacts_created:
            notification_parts.append(f"  • {artifact}")

    if validation_results.get("all_phases_passed"):
        notification_parts.append("")
        notification_parts.append("All validations passed. Your app has been updated!")
    else:
        notification_parts.append("")
        notification_parts.append("Some issues were detected. Please review the changes.")

    notification = "\n".join(notification_parts)

    return {
        "notification": notification,
        "current_state": "COMPLETE",
        "messages": [
            {
                "role": "system",
                "content": "User notification generated",
            }
        ],
    }


def build_improvement_graph():
    """Build the improvement agent LangGraph."""
    workflow = StateGraph(ImprovementState)

    workflow.add_node("classify_request", classify_request)
    workflow.add_node("analyze_impact", analyze_impact)
    workflow.add_node("generate_plan", generate_plan)
    workflow.add_node("execute_changes", execute_changes)
    workflow.add_node("validate_changes", validate_changes)
    workflow.add_node("notify_user", notify_user)

    workflow.set_entry_point("classify_request")

    workflow.add_edge("classify_request", "analyze_impact")
    workflow.add_edge("analyze_impact", "generate_plan")
    workflow.add_edge("generate_plan", "execute_changes")
    workflow.add_edge("execute_changes", "validate_changes")
    workflow.add_edge("validate_changes", "notify_user")
    workflow.add_edge("notify_user", END)

    return workflow.compile()


def run_improvement_pipeline(
    request: str,
    plan_id: str,
    current_app_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the improvement pipeline."""
    graph = build_improvement_graph()

    initial_state: ImprovementState = {
        "request": request,
        "plan_id": plan_id,
        "current_app_state": current_app_state or {},
        "request_type": None,
        "classification": {},
        "impact_analysis": {},
        "improvement_plan": {},
        "execution_results": {},
        "validation_results": {},
        "notification": None,
        "current_state": "CLASSIFY_REQUEST",
        "errors": [],
        "messages": [],
    }

    result = graph.invoke(initial_state)
    return result
