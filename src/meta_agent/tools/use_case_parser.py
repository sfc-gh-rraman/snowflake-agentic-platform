"""Use case parser tool - extracts structured requirements from natural language."""

import json
import os
from typing import Any, Dict, Optional

from ..state import ParsedRequirements, TaskType


PARSE_PROMPT = """You are an expert at analyzing AI application requirements.

Given the following use case description, extract structured requirements:

USE CASE:
{use_case}

Analyze and return a JSON object with these fields:
{{
    "primary_task": "classification|regression|clustering|rag|analytics|search",
    "secondary_tasks": ["list of additional task types"],
    "target_variable": "column name if ML task, null otherwise",
    "search_enabled": true/false,
    "analytics_enabled": true/false,
    "ml_enabled": true/false,
    "app_type": "copilot|dashboard|api|null",
    "deployment_target": "spcs",
    "entities": ["key business entities mentioned"],
    "key_features": ["important features or requirements"],
    "constraints": {{"any": "constraints mentioned"}}
}}

Return ONLY the JSON object, no explanation."""


class UseCaseParser:
    """Parse natural language use cases into structured requirements."""

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

    def _execute(self, sql: str) -> str:
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

    def _escape(self, text: str) -> str:
        return text.replace("'", "''").replace("\\", "\\\\")

    def parse(self, use_case_description: str) -> ParsedRequirements:
        prompt = PARSE_PROMPT.format(use_case=use_case_description)
        escaped_prompt = self._escape(prompt)

        sql = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{self.model}',
                '{escaped_prompt}'
            ) as RESPONSE
        """

        response = self._execute(sql)
        
        try:
            start = response.find('{')
            end = response.rfind('}') + 1
            if start >= 0 and end > start:
                json_str = response[start:end]
                data = json.loads(json_str)
            else:
                data = self._default_requirements()
        except json.JSONDecodeError:
            data = self._default_requirements()

        return ParsedRequirements(
            primary_task=TaskType(data.get("primary_task", "analytics")),
            secondary_tasks=[TaskType(t) for t in data.get("secondary_tasks", []) if t in [e.value for e in TaskType]],
            target_variable=data.get("target_variable"),
            search_enabled=data.get("search_enabled", False),
            analytics_enabled=data.get("analytics_enabled", False),
            ml_enabled=data.get("ml_enabled", False),
            app_type=data.get("app_type"),
            deployment_target=data.get("deployment_target", "spcs"),
            entities=data.get("entities", []),
            key_features=data.get("key_features", []),
            constraints=data.get("constraints", {}),
        )

    def _default_requirements(self) -> Dict[str, Any]:
        return {
            "primary_task": "analytics",
            "secondary_tasks": [],
            "target_variable": None,
            "search_enabled": False,
            "analytics_enabled": True,
            "ml_enabled": False,
            "app_type": "dashboard",
            "deployment_target": "spcs",
            "entities": [],
            "key_features": [],
            "constraints": {},
        }


def parse_use_case(state: Dict[str, Any]) -> Dict[str, Any]:
    """LangGraph node function to parse use case."""
    parser = UseCaseParser()
    requirements = parser.parse(state["use_case_description"])
    
    return {
        "parsed_requirements": requirements.to_dict(),
        "current_phase": "scan_data",
        "messages": state.get("messages", []) + [{
            "role": "system",
            "content": f"Parsed requirements: {requirements.primary_task.value} task detected",
        }],
    }
