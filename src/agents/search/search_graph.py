"""Cortex Search Service LangGraph - automated search service creation pipeline.

States: VALIDATE_SOURCE → CONFIGURE → CREATE_SERVICE → TEST_SERVICE → COMPLETE
"""

import json
import operator
import os
from typing import Annotated, Any, Dict, List, Optional

from langgraph.graph import StateGraph, END
from typing_extensions import TypedDict

from .search_builder import CortexSearchBuilder


SEARCH_CONFIG_PROMPT = """Analyze this chunk table and recommend optimal Cortex Search configuration.

TABLE: {table_name}
COLUMNS: {columns}
SAMPLE DATA: {sample}

Recommend:
1. Best column for search (contains the main text content)
2. Attribute columns for filtering
3. Target lag setting
4. Embedding model

Return JSON:
{{
    "search_column": "COLUMN_NAME",
    "attribute_columns": ["col1", "col2"],
    "target_lag": "1 hour",
    "embedding_model": "snowflake-arctic-embed-m-v1.5",
    "reasoning": "Brief explanation"
}}"""


class SearchGraphState(TypedDict):
    source_table: str
    service_name: str
    database: str
    schema: str
    columns: List[Dict[str, str]]
    sample_data: List[Dict[str, Any]]
    search_column: Optional[str]
    attribute_columns: Optional[List[str]]
    target_lag: str
    embedding_model: str
    search_service: Optional[str]
    test_results: Optional[Dict[str, Any]]
    current_state: str
    errors: Annotated[List[str], operator.add]
    messages: Annotated[List[Dict[str, str]], operator.add]


def _get_session(connection_name: str):
    if os.path.exists("/snowflake/session/token"):
        from snowflake.snowpark import Session
        return Session.builder.getOrCreate()
    else:
        import snowflake.connector
        return snowflake.connector.connect(connection_name=connection_name)


def _execute_query(session, sql: str) -> List[Dict]:
    if hasattr(session, 'sql'):
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
    if hasattr(session, 'sql'):
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


def validate_source(state: SearchGraphState) -> Dict[str, Any]:
    """Validate source table exists and get column info."""
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
                "errors": [f"Table {table_name} not found or has no columns"],
                "current_state": "FAILED",
            }

        sample_sql = f"SELECT * FROM {table_name} LIMIT 3"
        sample_data = _execute_query(session, sample_sql)

        return {
            "columns": columns,
            "sample_data": sample_data,
            "current_state": "CONFIGURE",
            "messages": [{
                "role": "system",
                "content": f"Validated source table with {len(columns)} columns",
            }],
        }
    except Exception as e:
        return {
            "errors": [f"Failed to validate source: {str(e)}"],
            "current_state": "FAILED",
        }


def configure_service(state: SearchGraphState) -> Dict[str, Any]:
    """Configure search service parameters using LLM analysis."""
    if state.get("search_column"):
        return {
            "current_state": "CREATE_SERVICE",
            "messages": [{"role": "system", "content": "Using provided configuration"}],
        }

    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    session = _get_session(connection_name)

    prompt = SEARCH_CONFIG_PROMPT.format(
        table_name=state["source_table"],
        columns=json.dumps(state.get("columns", []), default=str),
        sample=json.dumps(state.get("sample_data", [])[:2], default=str),
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
        
        json_start = response.find('{')
        json_end = response.rfind('}') + 1
        if json_start >= 0 and json_end > json_start:
            config = json.loads(response[json_start:json_end])
        else:
            config = _default_config(state)
    except Exception:
        config = _default_config(state)

    return {
        "search_column": config.get("search_column"),
        "attribute_columns": config.get("attribute_columns", []),
        "target_lag": config.get("target_lag", "1 hour"),
        "embedding_model": config.get("embedding_model", "snowflake-arctic-embed-m-v1.5"),
        "current_state": "CREATE_SERVICE",
        "messages": [{
            "role": "system",
            "content": f"Configured search on column: {config.get('search_column')}",
        }],
    }


def _default_config(state: SearchGraphState) -> Dict[str, Any]:
    """Generate default configuration when LLM fails."""
    columns = state.get("columns", [])
    
    text_columns = [
        c["COLUMN_NAME"] for c in columns
        if any(t in c.get("DATA_TYPE", "").upper() for t in ["VARCHAR", "STRING", "TEXT"])
    ]
    
    search_col = None
    for candidate in ["CHUNK", "CONTENT", "TEXT", "BODY", "DOCUMENT"]:
        for col in text_columns:
            if candidate in col.upper():
                search_col = col
                break
        if search_col:
            break
    
    if not search_col and text_columns:
        search_col = max(text_columns, key=len) if text_columns else text_columns[0]

    attr_cols = [c for c in text_columns if c != search_col][:5]

    return {
        "search_column": search_col,
        "attribute_columns": attr_cols,
        "target_lag": "1 hour",
        "embedding_model": "snowflake-arctic-embed-m-v1.5",
        "reasoning": "Default configuration based on column analysis",
    }


def create_service(state: SearchGraphState) -> Dict[str, Any]:
    """Create the Cortex Search service."""
    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    builder = CortexSearchBuilder(
        connection_name=connection_name,
        database=state["database"],
        schema=state["schema"],
    )

    search_column = state.get("search_column")
    if not search_column:
        return {
            "errors": ["No search column configured"],
            "current_state": "FAILED",
        }

    try:
        service_ref = builder.create_search_service(
            service_name=state["service_name"],
            source_table=state["source_table"],
            search_column=search_column,
            attribute_columns=state.get("attribute_columns"),
            target_lag=state.get("target_lag", "1 hour"),
            embedding_model=state.get("embedding_model", "snowflake-arctic-embed-m-v1.5"),
        )

        return {
            "search_service": service_ref,
            "current_state": "TEST_SERVICE",
            "messages": [{
                "role": "system",
                "content": f"Created Cortex Search service: {service_ref}",
            }],
        }
    except Exception as e:
        return {
            "errors": [f"Failed to create search service: {str(e)}"],
            "current_state": "FAILED",
        }


def test_service(state: SearchGraphState) -> Dict[str, Any]:
    """Test the created search service with sample queries."""
    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    builder = CortexSearchBuilder(
        connection_name=connection_name,
        database=state["database"],
        schema=state["schema"],
    )

    service_name = state.get("search_service")
    if not service_name:
        return {
            "current_state": "COMPLETE",
            "messages": [{"role": "system", "content": "No service to test"}],
        }

    test_queries = ["test", "example", "recent"]
    test_results = {"queries": [], "success": True, "errors": []}

    for query in test_queries:
        try:
            results = builder.search(
                service_name=service_name,
                query=query,
                limit=3,
            )
            
            test_results["queries"].append({
                "query": query,
                "result_count": len(results),
                "has_results": len(results) > 0,
            })
        except Exception as e:
            test_results["errors"].append(f"Query '{query}' failed: {str(e)}")
            test_results["success"] = False

    return {
        "test_results": test_results,
        "current_state": "COMPLETE",
        "messages": [{
            "role": "system",
            "content": f"Tested search service. Success: {test_results['success']}",
        }],
    }


def build_search_graph():
    """Build the search service creation LangGraph."""
    workflow = StateGraph(SearchGraphState)

    workflow.add_node("validate_source", validate_source)
    workflow.add_node("configure", configure_service)
    workflow.add_node("create_service", create_service)
    workflow.add_node("test_service", test_service)

    workflow.set_entry_point("validate_source")

    workflow.add_edge("validate_source", "configure")
    workflow.add_edge("configure", "create_service")
    workflow.add_edge("create_service", "test_service")
    workflow.add_edge("test_service", END)

    return workflow.compile()


def run_search_pipeline(
    source_table: str,
    service_name: str,
    database: str = "AGENTIC_PLATFORM",
    schema: str = "CORTEX",
    search_column: Optional[str] = None,
    attribute_columns: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Run the search service creation pipeline."""
    graph = build_search_graph()

    initial_state: SearchGraphState = {
        "source_table": source_table,
        "service_name": service_name,
        "database": database,
        "schema": schema,
        "columns": [],
        "sample_data": [],
        "search_column": search_column,
        "attribute_columns": attribute_columns,
        "target_lag": "1 hour",
        "embedding_model": "snowflake-arctic-embed-m-v1.5",
        "search_service": None,
        "test_results": None,
        "current_state": "VALIDATE_SOURCE",
        "errors": [],
        "messages": [],
    }

    result = graph.invoke(initial_state)
    return result
