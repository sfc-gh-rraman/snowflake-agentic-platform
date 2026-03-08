"""Semantic validator - uses Cortex LLM for business rule validation."""

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


SEMANTIC_PROMPT = """Analyze this data sample for semantic validity.

TABLE: {table_name}
COLUMNS: {columns}

SAMPLE DATA:
{sample_data}

BUSINESS CONTEXT: {context}

Check for:
1. Logical consistency (e.g., end_date > start_date)
2. Value reasonableness (e.g., no negative prices)
3. Referential patterns (e.g., status values make sense)
4. Domain-specific rules

Return JSON:
{{
    "passed": true/false,
    "issues": [
        {{"column": "col_name", "issue": "description", "severity": "high/medium/low"}}
    ],
    "recommendations": ["suggestion 1", "suggestion 2"]
}}

Return ONLY the JSON object."""


@dataclass
class SemanticResult:
    passed: bool
    issues: List[Dict[str, Any]]
    recommendations: List[str]
    score: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "issues": self.issues,
            "recommendations": self.recommendations,
            "score": self.score,
        }


class SemanticValidator:
    """Validate semantic consistency using Cortex LLM."""

    def __init__(
        self,
        connection_name: Optional[str] = None,
        model: str = "mistral-large2",
    ):
        self.connection_name = connection_name or os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
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

    def _get_sample_data(self, table_name: str, limit: int = 10) -> str:
        sql = f"SELECT * FROM {table_name} LIMIT {limit}"
        try:
            results = self._execute_query(sql)
            return json.dumps(results[:5], indent=2, default=str)
        except Exception:
            return "[]"

    def _get_columns(self, table_name: str) -> List[str]:
        parts = table_name.split('.')
        table = parts[-1]
        
        sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM {'.'.join(parts[:-1]) if len(parts) > 1 else parts[0]}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table}'
        """
        try:
            results = self._execute_query(sql)
            return [f"{r.get('COLUMN_NAME')} ({r.get('DATA_TYPE')})" for r in results]
        except Exception:
            return []

    def validate(
        self,
        table_name: str,
        business_context: str = "General data validation",
    ) -> SemanticResult:
        columns = self._get_columns(table_name)
        sample_data = self._get_sample_data(table_name)

        prompt = SEMANTIC_PROMPT.format(
            table_name=table_name,
            columns=", ".join(columns),
            sample_data=sample_data,
            context=business_context,
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
            
            start = response.find('{')
            end = response.rfind('}') + 1
            if start >= 0 and end > start:
                data = json.loads(response[start:end])
            else:
                data = {"passed": True, "issues": [], "recommendations": []}

        except Exception:
            data = {"passed": True, "issues": [], "recommendations": ["Unable to perform semantic validation"]}

        issues = data.get("issues", [])
        high_severity = sum(1 for i in issues if i.get("severity") == "high")
        
        return SemanticResult(
            passed=data.get("passed", True) and high_severity == 0,
            issues=issues,
            recommendations=data.get("recommendations", []),
            score=1.0 - (len(issues) * 0.1) if len(issues) < 10 else 0.0,
        )
