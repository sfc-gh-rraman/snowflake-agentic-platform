"""Semantic Model LangGraph - automated semantic model creation pipeline.

States: ANALYZE_TABLE → CLASSIFY_COLUMNS → GENERATE_YAML → CREATE_VERIFIED_QUERIES → VALIDATE → COMPLETE
"""

import json
import operator
import os
from typing import Annotated, Any

from langgraph.graph import END, StateGraph
from typing_extensions import TypedDict

VERIFIED_QUERY_PROMPT = """Generate verified queries for this semantic model.

TABLE: {table_name}
DIMENSIONS: {dimensions}
FACTS: {facts}
BUSINESS CONTEXT: {context}

Generate 5-8 natural language questions and their SQL answers.
Focus on common business questions for this domain.

Return JSON array:
[
    {{"name": "query_name", "question": "What is the total X?", "sql": "SELECT SUM(X) FROM TABLE"}},
    ...
]"""


class SemanticGraphState(TypedDict):
    source_table: str
    model_name: str
    business_context: str
    database: str
    schema: str
    columns: list[dict[str, str]]
    sample_data: list[dict[str, Any]]
    dimensions: list[dict[str, str]]
    facts: list[dict[str, str]]
    yaml_content: str | None
    verified_queries: list[dict[str, str]]
    validation_result: dict[str, Any] | None
    stage_path: str | None
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


def analyze_table(state: SemanticGraphState) -> dict[str, Any]:
    """Analyze source table structure."""
    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    session = _get_session(connection_name)

    table_name = state["source_table"]
    parts = table_name.split(".")
    if len(parts) >= 3:
        db, schema, table = parts[0], parts[1], parts[-1]
    elif len(parts) == 2:
        db, schema, table = state["database"], parts[0], parts[1]
    else:
        db, schema, table = state["database"], state["schema"], parts[0]

    col_sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM {db}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
    """

    try:
        columns = _execute_query(session, col_sql)

        if not columns:
            return {
                "errors": [f"Table {table_name} not found"],
                "current_state": "FAILED",
            }

        sample_sql = f"SELECT * FROM {table_name} LIMIT 5"
        sample_data = _execute_query(session, sample_sql)

        return {
            "columns": columns,
            "sample_data": sample_data,
            "current_state": "CLASSIFY_COLUMNS",
            "messages": [
                {
                    "role": "system",
                    "content": f"Analyzed table with {len(columns)} columns",
                }
            ],
        }
    except Exception as e:
        return {
            "errors": [f"Failed to analyze table: {str(e)}"],
            "current_state": "FAILED",
        }


def classify_columns(state: SemanticGraphState) -> dict[str, Any]:
    """Classify columns into dimensions and facts."""
    columns = state.get("columns", [])
    dimensions = []
    facts = []

    for col in columns:
        col_name = col.get("COLUMN_NAME", "")
        data_type = col.get("DATA_TYPE", "")
        col_lower = col_name.lower()

        is_date = any(t in data_type.upper() for t in ["DATE", "TIME", "TIMESTAMP"])
        is_string = any(t in data_type.upper() for t in ["VARCHAR", "STRING", "TEXT"])
        is_numeric = any(
            t in data_type.upper() for t in ["NUMBER", "FLOAT", "INT", "DOUBLE", "DECIMAL"]
        )

        is_id = any(kw in col_lower for kw in ["_id", "id_", "key", "code"])
        is_category = any(
            kw in col_lower
            for kw in ["type", "status", "category", "class", "name", "region", "country"]
        )
        is_measure = any(
            kw in col_lower
            for kw in [
                "amount",
                "count",
                "total",
                "sum",
                "avg",
                "price",
                "cost",
                "value",
                "quantity",
                "rate",
                "score",
            ]
        )

        if is_date or is_id or is_category or (is_string and not is_measure):
            dimensions.append(
                {
                    "name": col_name,
                    "expr": f'"{col_name}"',
                    "data_type": data_type,
                    "description": f"{col_name} dimension",
                }
            )
        elif is_numeric and (is_measure or not is_id):
            facts.append(
                {
                    "name": col_name,
                    "expr": f'"{col_name}"',
                    "data_type": data_type,
                    "description": f"{col_name} measure",
                }
            )
        else:
            dimensions.append(
                {
                    "name": col_name,
                    "expr": f'"{col_name}"',
                    "data_type": data_type,
                    "description": f"{col_name}",
                }
            )

    return {
        "dimensions": dimensions[:15],
        "facts": facts[:10],
        "current_state": "GENERATE_YAML",
        "messages": [
            {
                "role": "system",
                "content": f"Classified {len(dimensions)} dimensions and {len(facts)} facts",
            }
        ],
    }


def generate_yaml(state: SemanticGraphState) -> dict[str, Any]:
    """Generate semantic model YAML."""
    model_name = state["model_name"]
    table_name = state["source_table"]
    dimensions = state.get("dimensions", [])
    facts = state.get("facts", [])

    yaml_lines = [
        f"name: {model_name}",
        f"description: Semantic model for {table_name}",
        "",
        "tables:",
        f"  - name: {table_name.split('.')[-1]}",
        f"    base_table: {table_name}",
    ]

    if dimensions:
        yaml_lines.append("    dimensions:")
        for dim in dimensions:
            yaml_lines.extend(
                [
                    f"      - name: {dim['name']}",
                    f"        expr: {dim['expr']}",
                    f"        data_type: {dim['data_type']}",
                    f"        description: {dim['description']}",
                ]
            )

    if facts:
        yaml_lines.append("    facts:")
        for fact in facts:
            yaml_lines.extend(
                [
                    f"      - name: {fact['name']}",
                    f"        expr: {fact['expr']}",
                    f"        data_type: {fact['data_type']}",
                    f"        description: {fact['description']}",
                ]
            )

    yaml_content = "\n".join(yaml_lines)

    return {
        "yaml_content": yaml_content,
        "current_state": "CREATE_VERIFIED_QUERIES",
        "messages": [
            {
                "role": "system",
                "content": "Generated semantic model YAML",
            }
        ],
    }


def create_verified_queries(state: SemanticGraphState) -> dict[str, Any]:
    """Generate verified queries using LLM."""
    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    session = _get_session(connection_name)

    dimensions = state.get("dimensions", [])
    facts = state.get("facts", [])
    table_name = state["source_table"]

    prompt = VERIFIED_QUERY_PROMPT.format(
        table_name=table_name,
        dimensions=json.dumps([d["name"] for d in dimensions[:10]], default=str),
        facts=json.dumps([f["name"] for f in facts[:5]], default=str),
        context=state.get("business_context", "General analytics"),
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

        json_start = response.find("[")
        json_end = response.rfind("]") + 1
        if json_start >= 0 and json_end > json_start:
            verified_queries = json.loads(response[json_start:json_end])
        else:
            verified_queries = _default_verified_queries(state)
    except Exception:
        verified_queries = _default_verified_queries(state)

    yaml_content = state.get("yaml_content", "")
    if verified_queries:
        yaml_content += "\n\nverified_queries:"
        for vq in verified_queries[:8]:
            yaml_content += f"\n  - name: {vq.get('name', 'query')}"
            yaml_content += f'\n    question: "{vq.get("question", "")}"'
            yaml_content += f'\n    sql: "{vq.get("sql", "")}"'

    return {
        "verified_queries": verified_queries,
        "yaml_content": yaml_content,
        "current_state": "VALIDATE",
        "messages": [
            {
                "role": "system",
                "content": f"Created {len(verified_queries)} verified queries",
            }
        ],
    }


def _default_verified_queries(state: SemanticGraphState) -> list[dict[str, str]]:
    """Generate default verified queries."""
    table_name = state["source_table"]
    facts = state.get("facts", [])
    dimensions = state.get("dimensions", [])

    queries = [
        {
            "name": "total_count",
            "question": "How many records are in the table?",
            "sql": f"SELECT COUNT(*) FROM {table_name}",
        }
    ]

    if facts:
        first_fact = facts[0]["name"]
        queries.append(
            {
                "name": f"total_{first_fact.lower()}",
                "question": f"What is the total {first_fact}?",
                "sql": f'SELECT SUM("{first_fact}") FROM {table_name}',
            }
        )
        queries.append(
            {
                "name": f"avg_{first_fact.lower()}",
                "question": f"What is the average {first_fact}?",
                "sql": f'SELECT AVG("{first_fact}") FROM {table_name}',
            }
        )

    date_dims = [
        d for d in dimensions if any(t in d.get("data_type", "").upper() for t in ["DATE", "TIME"])
    ]
    if date_dims and facts:
        date_col = date_dims[0]["name"]
        fact_col = facts[0]["name"]
        queries.append(
            {
                "name": "daily_trend",
                "question": f"What is the daily trend of {fact_col}?",
                "sql": f'SELECT DATE_TRUNC(\'day\', "{date_col}") as day, SUM("{fact_col}") as total FROM {table_name} GROUP BY 1 ORDER BY 1',
            }
        )

    return queries


def validate_model(state: SemanticGraphState) -> dict[str, Any]:
    """Validate the semantic model."""
    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    session = _get_session(connection_name)

    verified_queries = state.get("verified_queries", [])
    validation_results = {"valid_queries": 0, "invalid_queries": 0, "errors": []}

    for vq in verified_queries[:3]:
        sql = vq.get("sql", "")
        if not sql:
            continue

        try:
            explain_sql = f"EXPLAIN {sql}"
            _execute(session, explain_sql)
            validation_results["valid_queries"] += 1
        except Exception as e:
            validation_results["invalid_queries"] += 1
            validation_results["errors"].append(f"Query '{vq.get('name')}' invalid: {str(e)[:100]}")

    return {
        "validation_result": validation_results,
        "current_state": "COMPLETE",
        "messages": [
            {
                "role": "system",
                "content": f"Validated {validation_results['valid_queries']} queries, {validation_results['invalid_queries']} invalid",
            }
        ],
    }


def build_semantic_graph():
    """Build the semantic model creation LangGraph."""
    workflow = StateGraph(SemanticGraphState)

    workflow.add_node("analyze_table", analyze_table)
    workflow.add_node("classify_columns", classify_columns)
    workflow.add_node("generate_yaml", generate_yaml)
    workflow.add_node("create_verified_queries", create_verified_queries)
    workflow.add_node("validate", validate_model)

    workflow.set_entry_point("analyze_table")

    workflow.add_edge("analyze_table", "classify_columns")
    workflow.add_edge("classify_columns", "generate_yaml")
    workflow.add_edge("generate_yaml", "create_verified_queries")
    workflow.add_edge("create_verified_queries", "validate")
    workflow.add_edge("validate", END)

    return workflow.compile()


def run_semantic_pipeline(
    source_table: str,
    model_name: str,
    database: str = "AGENTIC_PLATFORM",
    schema: str = "ANALYTICS",
    business_context: str = "General analytics",
) -> dict[str, Any]:
    """Run the semantic model creation pipeline."""
    graph = build_semantic_graph()

    initial_state: SemanticGraphState = {
        "source_table": source_table,
        "model_name": model_name,
        "business_context": business_context,
        "database": database,
        "schema": schema,
        "columns": [],
        "sample_data": [],
        "dimensions": [],
        "facts": [],
        "yaml_content": None,
        "verified_queries": [],
        "validation_result": None,
        "stage_path": None,
        "current_state": "ANALYZE_TABLE",
        "errors": [],
        "messages": [],
    }

    result = graph.invoke(initial_state)
    return result
