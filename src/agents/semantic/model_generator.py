"""Semantic Model Generator agent - creates semantic models from table profiles."""

import os
from typing import Any, Dict, List, Optional
import json


SEMANTIC_GEN_PROMPT = """Generate a semantic model YAML for Cortex Analyst.

TABLE: {table_name}
COLUMNS: {columns}
SAMPLE DATA: {sample_data}

Business Context: {context}

Create a semantic model with:
1. Meaningful dimension columns (categorical, temporal)
2. Fact/measure columns (numeric)
3. 3-5 verified queries for common questions

Return valid YAML:
```yaml
name: {model_name}
tables:
  - name: TABLE_NAME
    base_table: {base_table}
    dimensions:
      - name: COLUMN_NAME
        expr: "COLUMN_NAME"
        data_type: VARCHAR
        description: Description
    facts:
      - name: COLUMN_NAME
        expr: "COLUMN_NAME"
        data_type: NUMBER
        description: Description
verified_queries:
  - name: query_name
    question: "Natural language question?"
    sql: "SELECT ... FROM TABLE_NAME WHERE ..."
```

Return ONLY the YAML content."""


class SemanticModelGenerator:
    """Generate semantic model YAML from table profiles."""

    def __init__(
        self,
        connection_name: Optional[str] = None,
        database: str = "AGENTIC_PLATFORM",
        schema: str = "ANALYTICS",
        model: str = "mistral-large2",
    ):
        self.connection_name = connection_name or os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        self.database = database
        self.schema = schema
        self.model = model
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

    def _execute(self, sql: str) -> Any:
        if hasattr(self.session, 'sql'):
            result = self.session.sql(sql).collect()
            return result[0][0] if result else ""
        else:
            cursor = self.session.cursor()
            try:
                cursor.execute(sql)
                row = cursor.fetchone()
                return row[0] if row else ""
            finally:
                cursor.close()

    def _execute_query(self, sql: str) -> List[Dict]:
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

    def _escape(self, text: str) -> str:
        return text.replace("'", "''").replace("\\", "\\\\")

    def _get_table_info(self, table_name: str) -> Dict[str, Any]:
        parts = table_name.split('.')
        table = parts[-1]

        col_sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM {'.'.join(parts[:-1]) if len(parts) > 1 else parts[0]}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table}'
        """
        columns = self._execute_query(col_sql)

        sample_sql = f"SELECT * FROM {table_name} LIMIT 5"
        try:
            sample = self._execute_query(sample_sql)
        except Exception:
            sample = []

        return {
            "columns": columns,
            "sample_data": sample[:3],
        }

    def _classify_columns(
        self,
        columns: List[Dict[str, str]],
    ) -> Dict[str, List[Dict[str, str]]]:
        dimensions = []
        facts = []

        for col in columns:
            col_name = col.get("COLUMN_NAME", "")
            data_type = col.get("DATA_TYPE", "")
            col_lower = col_name.lower()

            is_date = any(t in data_type.upper() for t in ['DATE', 'TIME', 'TIMESTAMP'])
            is_string = any(t in data_type.upper() for t in ['VARCHAR', 'STRING', 'TEXT'])
            is_numeric = any(t in data_type.upper() for t in ['NUMBER', 'FLOAT', 'INT', 'DOUBLE', 'DECIMAL'])

            is_id = any(kw in col_lower for kw in ['_id', 'id_', 'key', 'code'])
            is_category = any(kw in col_lower for kw in ['type', 'status', 'category', 'class', 'name', 'region'])
            is_measure = any(kw in col_lower for kw in ['amount', 'count', 'total', 'sum', 'avg', 'price', 'cost', 'value', 'quantity'])

            if is_date or is_id or is_category or (is_string and not is_measure):
                dimensions.append(col)
            elif is_numeric and (is_measure or not is_id):
                facts.append(col)
            elif is_numeric:
                dimensions.append(col)
            else:
                dimensions.append(col)

        return {"dimensions": dimensions, "facts": facts}

    def generate_yaml(
        self,
        table_name: str,
        model_name: str,
        business_context: str = "General analytics",
        use_llm: bool = True,
    ) -> str:
        info = self._get_table_info(table_name)
        classified = self._classify_columns(info["columns"])

        if use_llm:
            prompt = SEMANTIC_GEN_PROMPT.format(
                table_name=table_name,
                columns=json.dumps(info["columns"], default=str),
                sample_data=json.dumps(info["sample_data"][:2], default=str),
                context=business_context,
                model_name=model_name,
                base_table=table_name,
            )

            escaped_prompt = self._escape(prompt)
            sql = f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                    '{self.model}',
                    '{escaped_prompt}'
                ) as RESPONSE
            """

            try:
                response = self._execute(sql)
                yaml_start = response.find('```yaml')
                yaml_end = response.rfind('```')
                if yaml_start >= 0 and yaml_end > yaml_start:
                    return response[yaml_start + 7:yaml_end].strip()
                elif 'name:' in response:
                    return response.strip()
            except Exception:
                pass

        yaml_parts = [f"name: {model_name}", "tables:", f"  - name: {table_name.split('.')[-1]}", f"    base_table: {table_name}"]

        if classified["dimensions"]:
            yaml_parts.append("    dimensions:")
            for dim in classified["dimensions"][:10]:
                col_name = dim.get("COLUMN_NAME")
                data_type = dim.get("DATA_TYPE")
                yaml_parts.extend([
                    f"      - name: {col_name}",
                    f'        expr: "{col_name}"',
                    f"        data_type: {data_type}",
                    f"        description: {col_name} dimension",
                ])

        if classified["facts"]:
            yaml_parts.append("    facts:")
            for fact in classified["facts"][:10]:
                col_name = fact.get("COLUMN_NAME")
                data_type = fact.get("DATA_TYPE")
                yaml_parts.extend([
                    f"      - name: {col_name}",
                    f'        expr: "{col_name}"',
                    f"        data_type: {data_type}",
                    f"        description: {col_name} measure",
                ])

        yaml_parts.append("verified_queries:")
        yaml_parts.extend([
            "  - name: total_count",
            '    question: "How many total records are there?"',
            f'    sql: "SELECT COUNT(*) FROM {table_name}"',
        ])

        if classified["facts"]:
            first_fact = classified["facts"][0].get("COLUMN_NAME")
            yaml_parts.extend([
                "  - name: sum_measure",
                f'    question: "What is the total {first_fact}?"',
                f'    sql: "SELECT SUM({first_fact}) FROM {table_name}"',
            ])

        return "\n".join(yaml_parts)

    def create_semantic_view(
        self,
        view_name: str,
        yaml_content: str,
        stage_path: Optional[str] = None,
    ) -> str:
        if stage_path:
            full_stage = f"@{self.database}.{self.schema}.{stage_path}"
        else:
            stage_name = f"{view_name}_STAGE"
            self._execute_query(f"""
                CREATE STAGE IF NOT EXISTS {self.database}.{self.schema}.{stage_name}
            """)
            full_stage = f"@{self.database}.{self.schema}.{stage_name}"

        yaml_escaped = yaml_content.replace("'", "''")

        return {
            "view_name": view_name,
            "yaml_content": yaml_content,
            "stage": full_stage,
            "status": "yaml_generated",
        }


def generate_semantic_model(state: Dict[str, Any]) -> Dict[str, Any]:
    """LangGraph node function for semantic model generation."""
    generator = SemanticModelGenerator()

    table_name = state.get("source_table")
    model_name = state.get("model_name", "AUTO_SEMANTIC_MODEL")
    business_context = state.get("business_context", "General analytics")

    if not table_name:
        return {
            "semantic_result": {"error": "source_table required"},
            "current_state": "FAILED",
        }

    try:
        yaml_content = generator.generate_yaml(
            table_name=table_name,
            model_name=model_name,
            business_context=business_context,
        )

        return {
            "semantic_result": {
                "status": "success",
                "yaml": yaml_content,
            },
            "semantic_yaml": yaml_content,
            "current_state": "COMPLETE",
            "messages": state.get("messages", []) + [{
                "role": "system",
                "content": f"Generated semantic model: {model_name}",
            }],
        }
    except Exception as e:
        return {
            "semantic_result": {"error": str(e)},
            "current_state": "FAILED",
        }
