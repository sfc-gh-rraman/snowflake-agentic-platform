"""Plan generator tool - creates execution plan from requirements and agents."""

import json
import os
from typing import Any
from uuid import uuid4

from ..state import ExecutionPhase, ExecutionPlan

PLAN_PROMPT = """You are an expert at planning AI application workflows.

Given the following requirements and available agents, create an execution plan:

REQUIREMENTS:
{requirements}

DATA PROFILE:
{data_profile}

AVAILABLE AGENTS:
{agents}

Create an execution plan with phases in the correct order. Consider dependencies.

Return a JSON object:
{{
    "name": "Plan name",
    "description": "What this plan does",
    "phases": [
        {{
            "phase_name": "Phase name",
            "phase_order": 1,
            "agent_id": "agent_id from available agents",
            "agent_name": "Agent name",
            "config": {{}},
            "depends_on": [],
            "expected_outputs": ["table", "model", etc]
        }}
    ],
    "estimated_duration_minutes": 30
}}

Rules:
1. preprocessing agents before ML agents
2. ML agents before search/semantic agents
3. app_generation after all data processing
4. deployment last

Return ONLY the JSON object."""


class PlanGenerator:
    """Generate execution plans from requirements and agent capabilities."""

    def __init__(
        self,
        connection_name: str | None = None,
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
        if hasattr(self.session, "sql"):
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

    def generate(
        self,
        requirements: dict[str, Any],
        data_profile: dict[str, Any],
        available_agents: list[dict[str, Any]],
    ) -> ExecutionPlan:
        prompt = PLAN_PROMPT.format(
            requirements=json.dumps(requirements, indent=2),
            data_profile=json.dumps(data_profile, indent=2),
            agents=json.dumps(available_agents, indent=2),
        )
        escaped_prompt = self._escape(prompt)

        sql = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{self.model}',
                '{escaped_prompt}'
            ) as RESPONSE
        """

        response = self._execute(sql)

        try:
            start = response.find("{")
            end = response.rfind("}") + 1
            if start >= 0 and end > start:
                json_str = response[start:end]
                data = json.loads(json_str)
            else:
                data = self._default_plan(available_agents)
        except json.JSONDecodeError:
            data = self._default_plan(available_agents)

        plan_id = str(uuid4())
        phases = []
        for p in data.get("phases", []):
            phases.append(
                ExecutionPhase(
                    phase_id=str(uuid4()),
                    phase_name=p.get("phase_name", "Unknown"),
                    phase_order=p.get("phase_order", 0),
                    agent_id=p.get("agent_id", ""),
                    agent_name=p.get("agent_name", ""),
                    config=p.get("config", {}),
                    depends_on=p.get("depends_on", []),
                    expected_outputs=p.get("expected_outputs", []),
                )
            )

        return ExecutionPlan(
            plan_id=plan_id,
            name=data.get("name", "Generated Plan"),
            description=data.get("description", "Auto-generated execution plan"),
            phases=phases,
            estimated_duration_minutes=data.get("estimated_duration_minutes", 30),
            total_phases=len(phases),
        )

    def _default_plan(self, available_agents: list[dict[str, Any]]) -> dict[str, Any]:
        phases = []
        order = 1

        agent_ids = {a.get("agent_id") for a in available_agents}

        if "parquet_processor" in agent_ids:
            phases.append(
                {
                    "phase_name": "Data Ingestion",
                    "phase_order": order,
                    "agent_id": "parquet_processor",
                    "agent_name": "Parquet Processor",
                    "config": {},
                    "depends_on": [],
                    "expected_outputs": ["table"],
                }
            )
            order += 1

        if "document_chunker" in agent_ids:
            phases.append(
                {
                    "phase_name": "Document Processing",
                    "phase_order": order,
                    "agent_id": "document_chunker",
                    "agent_name": "Document Chunker",
                    "config": {},
                    "depends_on": [],
                    "expected_outputs": ["chunk_table"],
                }
            )
            order += 1

        if "ml_model_builder" in agent_ids:
            phases.append(
                {
                    "phase_name": "ML Training",
                    "phase_order": order,
                    "agent_id": "ml_model_builder",
                    "agent_name": "ML Model Builder",
                    "config": {},
                    "depends_on": ["parquet_processor"] if "parquet_processor" in agent_ids else [],
                    "expected_outputs": ["ml_model"],
                }
            )
            order += 1

        if "cortex_search_builder" in agent_ids:
            phases.append(
                {
                    "phase_name": "Search Setup",
                    "phase_order": order,
                    "agent_id": "cortex_search_builder",
                    "agent_name": "Cortex Search Builder",
                    "config": {},
                    "depends_on": ["document_chunker"] if "document_chunker" in agent_ids else [],
                    "expected_outputs": ["cortex_search_service"],
                }
            )
            order += 1

        if "semantic_model_generator" in agent_ids:
            phases.append(
                {
                    "phase_name": "Semantic Model",
                    "phase_order": order,
                    "agent_id": "semantic_model_generator",
                    "agent_name": "Semantic Model Generator",
                    "config": {},
                    "depends_on": ["parquet_processor"] if "parquet_processor" in agent_ids else [],
                    "expected_outputs": ["semantic_model"],
                }
            )
            order += 1

        if "app_code_generator" in agent_ids:
            phases.append(
                {
                    "phase_name": "App Generation",
                    "phase_order": order,
                    "agent_id": "app_code_generator",
                    "agent_name": "App Code Generator",
                    "config": {},
                    "depends_on": [
                        p["agent_id"] for p in phases if p["agent_id"] != "app_code_generator"
                    ],
                    "expected_outputs": ["generated_code"],
                }
            )
            order += 1

        return {
            "name": "Default Execution Plan",
            "description": "Auto-generated plan based on available agents",
            "phases": phases,
            "estimated_duration_minutes": len(phases) * 5,
        }


def generate_plan(state: dict[str, Any]) -> dict[str, Any]:
    """LangGraph node function to generate execution plan."""
    generator = PlanGenerator()

    plan = generator.generate(
        requirements=state.get("parsed_requirements", {}),
        data_profile=state.get("data_profile", {}),
        available_agents=state.get("available_agents", []),
    )

    return {
        "execution_plan": plan.to_dict(),
        "current_phase": "human_approval",
        "messages": state.get("messages", [])
        + [
            {
                "role": "system",
                "content": f"Generated plan '{plan.name}' with {plan.total_phases} phases",
            }
        ],
    }
