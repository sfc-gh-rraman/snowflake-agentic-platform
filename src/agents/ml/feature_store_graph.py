"""Feature Store LangGraph - automated feature engineering pipeline.

States: DISCOVER → ANALYZE → ENGINEER → MATERIALIZE → VALIDATE
"""

import json
import operator
import os
from typing import Annotated, Any

from langgraph.graph import END, StateGraph
from typing_extensions import TypedDict

from .feature_store import FeatureDefinition, FeatureStore

FEATURE_ENGINEERING_PROMPT = """Analyze this table and recommend feature engineering strategies.

TABLE: {table_name}
COLUMNS: {columns}
SAMPLE DATA: {sample_data}

ML TASK: {ml_task}
TARGET COLUMN: {target_column}

Based on the data, recommend:
1. Which columns should have lag features (for time series)
2. Which columns should have window aggregations
3. Which timestamp columns need temporal features
4. Any interaction features between columns
5. Categorical encoding strategy

Return JSON:
{{
    "lag_columns": [
        {{"column": "col_name", "partition_by": "entity_col", "order_by": "time_col", "lags": [1, 7, 14]}}
    ],
    "window_columns": [
        {{"column": "col_name", "partition_by": "entity_col", "order_by": "time_col", "windows": [7, 14, 30]}}
    ],
    "temporal_columns": ["timestamp_col1"],
    "interactions": [
        {{"columns": ["col1", "col2"], "operation": "multiply"}}
    ],
    "categorical_columns": ["cat_col1"],
    "reasoning": "Brief explanation"
}}"""


class FeatureStoreState(TypedDict):
    source_table: str
    target_column: str | None
    ml_task: str | None
    database: str
    schema: str
    entity_column: str | None
    time_column: str | None
    discovered_features: Annotated[list[dict[str, Any]], operator.add]
    analysis: dict[str, Any]
    engineered_features: Annotated[list[dict[str, Any]], operator.add]
    feature_table: str | None
    feature_stats: dict[str, Any]
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


def _execute_query(session, sql: str) -> list[dict]:
    if hasattr(session, "sql"):
        result = session.sql(sql).collect()
        return [dict(row.asDict()) for row in result]
    else:
        cursor = session.cursor()
        try:
            cursor.execute(sql)
            if cursor.description:
                columns = [col[0] for col in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            return []
        finally:
            cursor.close()


def discover_features(state: FeatureStoreState) -> dict[str, Any]:
    """Discover existing features from source table."""
    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    store = FeatureStore(
        connection_name=connection_name,
        database=state["database"],
        schema=state["schema"],
    )

    try:
        features = store.discover_features(state["source_table"])

        return {
            "discovered_features": [f.to_dict() for f in features],
            "current_state": "ANALYZE",
            "messages": [
                {
                    "role": "system",
                    "content": f"Discovered {len(features)} features from {state['source_table']}",
                }
            ],
        }
    except Exception as e:
        return {
            "errors": [f"Feature discovery failed: {str(e)}"],
            "current_state": "FAILED",
        }


def analyze_for_engineering(state: FeatureStoreState) -> dict[str, Any]:
    """Analyze table to determine optimal feature engineering strategies."""
    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    session = _get_session(connection_name)

    columns = state.get("discovered_features", [])

    sample_sql = f"SELECT * FROM {state['source_table']} LIMIT 5"
    try:
        sample_data = _execute_query(session, sample_sql)
    except Exception:
        sample_data = []

    prompt = FEATURE_ENGINEERING_PROMPT.format(
        table_name=state["source_table"],
        columns=json.dumps(columns[:20], default=str),
        sample_data=json.dumps(sample_data[:3], default=str),
        ml_task=state.get("ml_task", "general"),
        target_column=state.get("target_column", "unknown"),
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
            analysis = json.loads(response[json_start:json_end])
        else:
            analysis = _default_analysis(state)
    except Exception:
        analysis = _default_analysis(state)

    return {
        "analysis": analysis,
        "current_state": "ENGINEER",
        "messages": [
            {
                "role": "system",
                "content": "Analyzed table for feature engineering opportunities",
            }
        ],
    }


def _default_analysis(state: FeatureStoreState) -> dict[str, Any]:
    """Generate default analysis when LLM fails."""
    numeric_cols = [
        f["name"] for f in state.get("discovered_features", []) if f.get("category") == "numeric"
    ]
    temporal_cols = [
        f["name"] for f in state.get("discovered_features", []) if f.get("category") == "temporal"
    ]
    categorical_cols = [
        f["name"]
        for f in state.get("discovered_features", [])
        if f.get("category") == "categorical"
    ]

    entity_col = state.get("entity_column") or (
        next(
            (
                c
                for c in [f["name"] for f in state.get("discovered_features", [])]
                if "id" in c.lower()
            ),
            None,
        )
    )
    time_col = state.get("time_column") or (temporal_cols[0] if temporal_cols else None)

    analysis = {
        "lag_columns": [],
        "window_columns": [],
        "temporal_columns": temporal_cols[:3],
        "interactions": [],
        "categorical_columns": categorical_cols[:5],
        "reasoning": "Default analysis based on column types",
    }

    if entity_col and time_col:
        for col in numeric_cols[:3]:
            analysis["lag_columns"].append(
                {
                    "column": col,
                    "partition_by": entity_col,
                    "order_by": time_col,
                    "lags": [1, 7, 14],
                }
            )
            analysis["window_columns"].append(
                {
                    "column": col,
                    "partition_by": entity_col,
                    "order_by": time_col,
                    "windows": [7, 14, 30],
                }
            )

    return analysis


def engineer_features(state: FeatureStoreState) -> dict[str, Any]:
    """Generate engineered features based on analysis."""
    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    store = FeatureStore(
        connection_name=connection_name,
        database=state["database"],
        schema=state["schema"],
    )

    analysis = state.get("analysis", {})
    all_features = []

    for lag_spec in analysis.get("lag_columns", []):
        features = store.create_lag_features(
            value_column=lag_spec["column"],
            partition_column=lag_spec["partition_by"],
            order_column=lag_spec["order_by"],
            lags=lag_spec.get("lags", [1, 7, 14]),
        )
        all_features.extend(features)

    for window_spec in analysis.get("window_columns", []):
        features = store.create_window_features(
            table_name=state["source_table"],
            value_column=window_spec["column"],
            partition_column=window_spec["partition_by"],
            order_column=window_spec["order_by"],
            windows=window_spec.get("windows", [7, 14, 30]),
        )
        all_features.extend(features)

    for temporal_col in analysis.get("temporal_columns", []):
        features = store.create_temporal_features(temporal_col)
        all_features.extend(features)

    for interaction in analysis.get("interactions", []):
        cols = interaction.get("columns", [])
        op = interaction.get("operation", "multiply")
        if len(cols) >= 2:
            if op == "multiply":
                expr = f'"{cols[0]}" * "{cols[1]}"'
            elif op == "divide":
                expr = f'"{cols[0]}" / NULLIF("{cols[1]}", 0)'
            elif op == "add":
                expr = f'"{cols[0]}" + "{cols[1]}"'
            elif op == "subtract":
                expr = f'"{cols[0]}" - "{cols[1]}"'
            else:
                expr = f'"{cols[0]}" * "{cols[1]}"'

            all_features.append(
                FeatureDefinition(
                    name=f"{cols[0]}_{op}_{cols[1]}".upper(),
                    expression=expr,
                    data_type="FLOAT",
                    description=f"Interaction: {cols[0]} {op} {cols[1]}",
                    category="numeric",
                    is_derived=True,
                    source_columns=cols,
                )
            )

    return {
        "engineered_features": [f.to_dict() if hasattr(f, "to_dict") else f for f in all_features],
        "current_state": "MATERIALIZE",
        "messages": [
            {
                "role": "system",
                "content": f"Engineered {len(all_features)} features",
            }
        ],
    }


def materialize_features(state: FeatureStoreState) -> dict[str, Any]:
    """Materialize features into a feature table."""
    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    store = FeatureStore(
        connection_name=connection_name,
        database=state["database"],
        schema=state["schema"],
    )

    features = [
        FeatureDefinition(**f) if isinstance(f, dict) else f
        for f in state.get("engineered_features", [])
    ]

    if not features:
        return {
            "feature_table": state["source_table"],
            "current_state": "VALIDATE",
            "messages": [{"role": "system", "content": "No derived features to materialize"}],
        }

    source_parts = state["source_table"].split(".")
    table_name = source_parts[-1] if source_parts else "FEATURES"
    output_table = f"{state['database']}.{state['schema']}.{table_name}_FEATURES"

    try:
        result = store.materialize_feature_table(
            source_table=state["source_table"],
            features=features,
            output_table=output_table,
            include_source_columns=True,
        )

        return {
            "feature_table": result,
            "current_state": "VALIDATE",
            "messages": [
                {
                    "role": "system",
                    "content": f"Materialized feature table: {result}",
                }
            ],
        }
    except Exception as e:
        return {
            "errors": [f"Failed to materialize features: {str(e)}"],
            "current_state": "FAILED",
        }


def validate_features(state: FeatureStoreState) -> dict[str, Any]:
    """Validate materialized feature table."""
    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    store = FeatureStore(
        connection_name=connection_name,
        database=state["database"],
        schema=state["schema"],
    )

    feature_table = state.get("feature_table")
    if not feature_table:
        return {
            "current_state": "COMPLETE",
            "messages": [{"role": "system", "content": "No feature table to validate"}],
        }

    engineered_names = [
        f["name"] for f in state.get("engineered_features", []) if f.get("is_derived")
    ]

    try:
        stats = store.get_feature_stats(feature_table, engineered_names[:10])

        issues = []
        for feature_name, feature_stats in stats.items():
            if feature_stats.get("error"):
                issues.append(f"Could not compute stats for {feature_name}")
            elif feature_stats.get("std") == 0:
                issues.append(f"Feature {feature_name} has zero variance")

        return {
            "feature_stats": stats,
            "current_state": "COMPLETE",
            "errors": issues if issues else [],
            "messages": [
                {
                    "role": "system",
                    "content": f"Validated {len(stats)} features. Issues: {len(issues)}",
                }
            ],
        }
    except Exception as e:
        return {
            "errors": [f"Validation failed: {str(e)}"],
            "current_state": "COMPLETE",
        }


def route_after_state(state: FeatureStoreState) -> str:
    """Route based on current state."""
    current = state.get("current_state", "")
    if current == "FAILED" or current == "COMPLETE":
        return END

    state_map = {
        "DISCOVER": "analyze",
        "ANALYZE": "engineer",
        "ENGINEER": "materialize",
        "MATERIALIZE": "validate",
        "VALIDATE": END,
    }
    return state_map.get(current, END)


def build_feature_store_graph():
    """Build the feature store LangGraph."""
    workflow = StateGraph(FeatureStoreState)

    workflow.add_node("discover", discover_features)
    workflow.add_node("analyze", analyze_for_engineering)
    workflow.add_node("engineer", engineer_features)
    workflow.add_node("materialize", materialize_features)
    workflow.add_node("validate", validate_features)

    workflow.set_entry_point("discover")

    workflow.add_edge("discover", "analyze")
    workflow.add_edge("analyze", "engineer")
    workflow.add_edge("engineer", "materialize")
    workflow.add_edge("materialize", "validate")
    workflow.add_edge("validate", END)

    return workflow.compile()


def run_feature_store_pipeline(
    source_table: str,
    database: str = "AGENTIC_PLATFORM",
    schema: str = "ML",
    target_column: str | None = None,
    ml_task: str | None = None,
    entity_column: str | None = None,
    time_column: str | None = None,
) -> dict[str, Any]:
    """Run the feature store pipeline."""
    graph = build_feature_store_graph()

    initial_state: FeatureStoreState = {
        "source_table": source_table,
        "target_column": target_column,
        "ml_task": ml_task,
        "database": database,
        "schema": schema,
        "entity_column": entity_column,
        "time_column": time_column,
        "discovered_features": [],
        "analysis": {},
        "engineered_features": [],
        "feature_table": None,
        "feature_stats": {},
        "current_state": "DISCOVER",
        "errors": [],
        "messages": [],
    }

    result = graph.invoke(initial_state)
    return result
