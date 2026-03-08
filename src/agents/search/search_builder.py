"""Cortex Search Builder agent - creates search services from chunk tables."""

import os
from typing import Any, Dict, List, Optional


class CortexSearchBuilder:
    """Build Cortex Search services from document chunks."""

    def __init__(
        self,
        connection_name: Optional[str] = None,
        database: str = "AGENTIC_PLATFORM",
        schema: str = "ANALYTICS",
        warehouse: str = "COMPUTE_WH",
    ):
        self.connection_name = connection_name or os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self._session = None

    @property
    def session(self):
        if self._session is None:
            self._session = self._create_session()
        return self._session

    def _create_session(self):
        if os.path.exists("/snowflake/session/token"):
            from snowflake.snowpark import Session
            return Session.builder.getOrCreate()
        else:
            import snowflake.connector
            conn = snowflake.connector.connect(connection_name=self.connection_name)
            return conn

    def _execute(self, sql: str) -> List[Dict]:
        if hasattr(self.session, 'sql'):
            result = self.session.sql(sql).collect()
            return [dict(row.asDict()) for row in result]
        else:
            cursor = self.session.cursor()
            try:
                cursor.execute(sql)
                if cursor.description:
                    columns = [col[0] for col in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
                return []
            finally:
                cursor.close()

    def _get_table_columns(self, table_name: str) -> List[Dict[str, str]]:
        parts = table_name.split('.')
        if len(parts) >= 3:
            db, schema, table = parts[0], parts[1], parts[-1]
        elif len(parts) == 2:
            db, schema, table = self.database, parts[0], parts[1]
        else:
            db, schema, table = self.database, self.schema, parts[0]

        sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM {db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        """
        return self._execute(sql)

    def create_search_service(
        self,
        service_name: str,
        source_table: str,
        search_column: str,
        attribute_columns: Optional[List[str]] = None,
        target_lag: str = "1 hour",
        embedding_model: str = "snowflake-arctic-embed-m-v1.5",
    ) -> str:
        full_service = f"{self.database}.{self.schema}.{service_name}"
        
        columns = self._get_table_columns(source_table)
        available_cols = {c.get("COLUMN_NAME", "").upper() for c in columns}

        if search_column.upper() not in available_cols:
            raise ValueError(f"Search column '{search_column}' not found in table")

        if attribute_columns:
            valid_attrs = [c for c in attribute_columns if c.upper() in available_cols]
        else:
            string_cols = [c.get("COLUMN_NAME") for c in columns 
                         if 'VARCHAR' in c.get("DATA_TYPE", "").upper() 
                         or 'STRING' in c.get("DATA_TYPE", "").upper()]
            valid_attrs = [c for c in string_cols if c.upper() != search_column.upper()][:5]

        attr_clause = f"ATTRIBUTES {', '.join(valid_attrs)}" if valid_attrs else ""

        sql = f"""
            CREATE OR REPLACE CORTEX SEARCH SERVICE {full_service}
                ON {search_column}
                {attr_clause}
                WAREHOUSE = {self.warehouse}
                TARGET_LAG = '{target_lag}'
                EMBEDDING_MODEL = '{embedding_model}'
            AS (
                SELECT * FROM {source_table}
            )
        """

        try:
            self._execute(sql)
            return full_service
        except Exception as e:
            raise RuntimeError(f"Failed to create search service: {e}")

    def search(
        self,
        service_name: str,
        query: str,
        columns: Optional[List[str]] = None,
        filter_dict: Optional[Dict[str, Any]] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        import json
        
        if '.' not in service_name:
            service_name = f"{self.database}.{self.schema}.{service_name}"

        columns = columns or []
        
        search_spec = {
            "query": query,
            "columns": columns,
            "filter": filter_dict or {},
            "limit": limit,
        }

        search_json = json.dumps(search_spec).replace("'", "''")

        sql = f"""
            SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                '{service_name}',
                '{search_json}'
            ) as RESULT
        """

        try:
            results = self._execute(sql)
            if not results:
                return []

            result_data = results[0].get("RESULT")
            if isinstance(result_data, str):
                result_data = json.loads(result_data)

            return result_data.get("results", [])
        except Exception as e:
            return [{"error": str(e)}]

    def list_services(self) -> List[Dict[str, Any]]:
        sql = f"""
            SHOW CORTEX SEARCH SERVICES IN SCHEMA {self.database}.{self.schema}
        """
        try:
            return self._execute(sql)
        except Exception:
            return []


def build_search_service(state: Dict[str, Any]) -> Dict[str, Any]:
    """LangGraph node function for building Cortex Search service."""
    builder = CortexSearchBuilder()

    service_name = state.get("service_name", "DOCUMENT_SEARCH")
    source_table = state.get("chunk_table")
    search_column = state.get("search_column", "CHUNK")
    attribute_columns = state.get("attribute_columns")

    if not source_table:
        return {
            "search_result": {"error": "chunk_table required"},
            "current_state": "FAILED",
        }

    try:
        service_ref = builder.create_search_service(
            service_name=service_name,
            source_table=source_table,
            search_column=search_column,
            attribute_columns=attribute_columns,
        )

        return {
            "search_result": {
                "status": "success",
                "service": service_ref,
            },
            "search_service": service_ref,
            "current_state": "COMPLETE",
            "messages": state.get("messages", []) + [{
                "role": "system",
                "content": f"Created Cortex Search service: {service_ref}",
            }],
        }
    except Exception as e:
        return {
            "search_result": {"error": str(e)},
            "current_state": "FAILED",
        }
