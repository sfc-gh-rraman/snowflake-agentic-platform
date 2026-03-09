"""Cortex Agent Builder - creates and manages Snowflake Cortex Agents.

Cortex Agents orchestrate tools including:
- Cortex Search (document retrieval)
- Cortex Analyst (semantic SQL)
- Model inference (ML predictions)
- Custom Python functions
"""

import json
import os
from typing import Any

AGENT_SYSTEM_PROMPT_TEMPLATE = """You are an AI assistant for {domain}.

Your capabilities:
{capabilities}

Guidelines:
- Be helpful and concise
- Use the appropriate tool for each question
- For data questions, use cortex_analyst
- For document/report questions, use cortex_search
- For predictions, use model_inference
- Always cite your sources

{additional_instructions}"""


class CortexAgentBuilder:
    """Build and manage Snowflake Cortex Agents."""

    def __init__(
        self,
        connection_name: str | None = None,
        database: str = "AGENTIC_PLATFORM",
        schema: str = "CORTEX",
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

            return snowflake.connector.connect(connection_name=self.connection_name)

    def _execute(self, sql: str) -> list[dict]:
        if hasattr(self.session, "sql"):
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

    def create_agent(
        self,
        agent_name: str,
        domain: str,
        search_services: list[str] | None = None,
        semantic_models: list[str] | None = None,
        ml_functions: list[str] | None = None,
        model: str = "claude-3-5-sonnet",
        additional_instructions: str = "",
    ) -> dict[str, Any]:
        """Create a Cortex Agent with configured tools."""

        tools = []
        capabilities = []

        if search_services:
            for service in search_services:
                tools.append(
                    {
                        "type": "cortex_search",
                        "service": service,
                    }
                )
                capabilities.append(f"- Search documents using {service}")

        if semantic_models:
            for model_ref in semantic_models:
                tools.append(
                    {
                        "type": "cortex_analyst",
                        "semantic_model": model_ref,
                    }
                )
                capabilities.append(f"- Query data using {model_ref}")

        if ml_functions:
            for func in ml_functions:
                tools.append(
                    {
                        "type": "function",
                        "function": func,
                    }
                )
                capabilities.append(f"- Make predictions using {func}")

        system_prompt = AGENT_SYSTEM_PROMPT_TEMPLATE.format(
            domain=domain,
            capabilities="\n".join(capabilities) if capabilities else "- General assistance",
            additional_instructions=additional_instructions,
        )

        agent_config = {
            "name": agent_name,
            "model": model,
            "system_prompt": system_prompt,
            "tools": tools,
            "domain": domain,
        }

        full_name = f"{self.database}.{self.schema}.{agent_name}"

        return {
            "agent_name": full_name,
            "config": agent_config,
            "status": "configured",
        }

    def create_agent_sql(
        self,
        agent_name: str,
        search_service: str | None = None,
        semantic_model_stage: str | None = None,
    ) -> str:
        """Generate SQL to create a Cortex Agent (when GA)."""

        tools_sql = []

        if search_service:
            tools_sql.append(f"""
                TOOL cortex_search_{agent_name}
                TYPE = cortex_search
                SERVICE = {search_service}""")

        if semantic_model_stage:
            tools_sql.append(f"""
                TOOL cortex_analyst_{agent_name}
                TYPE = cortex_analyst
                SEMANTIC_MODEL = {semantic_model_stage}""")

        tools_definition = "\n".join(tools_sql) if tools_sql else ""

        sql = f"""
-- Cortex Agent DDL (Preview syntax - may change)
CREATE OR REPLACE CORTEX AGENT {self.database}.{self.schema}.{agent_name}
    MODEL = 'claude-3-5-sonnet'
    WAREHOUSE = {self.warehouse}
    {tools_definition}
;
"""
        return sql

    def invoke_agent(
        self,
        agent_config: dict[str, Any],
        message: str,
        conversation_history: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        """Invoke a Cortex Agent with a message.

        Since Cortex Agent is in preview, this uses Cortex Complete with
        tool simulation.
        """
        system_prompt = agent_config.get("system_prompt", "You are a helpful assistant.")
        tools = agent_config.get("tools", [])
        model = agent_config.get("model", "mistral-large2")

        if model.startswith("claude"):
            model = "mistral-large2"

        messages_text = ""
        if conversation_history:
            for msg in conversation_history[-5:]:
                role = msg.get("role", "user")
                content = msg.get("content", "")
                messages_text += f"\n{role.upper()}: {content}"

        tool_descriptions = []
        for tool in tools:
            tool_type = tool.get("type", "")
            if tool_type == "cortex_search":
                service = tool.get("service", "")
                tool_descriptions.append(
                    f"- cortex_search({service}): Search documents for relevant information"
                )
            elif tool_type == "cortex_analyst":
                semantic_model = tool.get("semantic_model", "")
                tool_descriptions.append(
                    f"- cortex_analyst({semantic_model}): Query structured data using natural language"
                )
            elif tool_type == "function":
                func = tool.get("function", "")
                tool_descriptions.append(f"- {func}(): Call ML function for predictions")

        full_prompt = f"""{system_prompt}

Available tools:
{chr(10).join(tool_descriptions) if tool_descriptions else "No tools configured"}

Conversation:
{messages_text}

USER: {message}

Respond helpfully. If you need to use a tool, indicate which tool and why."""

        escaped_prompt = full_prompt.replace("'", "''").replace("\\", "\\\\")
        sql = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{model}',
                '{escaped_prompt}'
            ) as RESPONSE
        """

        try:
            results = self._execute(sql)
            if results:
                response = results[0].get("RESPONSE", "")
                return {
                    "response": response,
                    "model": model,
                    "tools_available": len(tools),
                    "status": "success",
                }
            return {"response": "", "status": "empty"}
        except Exception as e:
            return {"response": "", "status": "error", "error": str(e)}

    def create_agent_service_function(
        self,
        agent_name: str,
        agent_config: dict[str, Any],
    ) -> str:
        """Create a UDF that wraps the agent for easy invocation."""

        config_json = json.dumps(agent_config).replace("'", "''")

        sql = f"""
CREATE OR REPLACE FUNCTION {self.database}.{self.schema}.INVOKE_{agent_name}(message VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'invoke_agent'
AS
$$
import json
import _snowflake

def invoke_agent(message: str) -> dict:
    config = json.loads('''{config_json}''')

    system_prompt = config.get("system_prompt", "You are a helpful assistant.")
    model = "mistral-large2"

    full_prompt = f"{{system_prompt}}\\n\\nUSER: {{message}}"

    session = _snowflake.session()
    result = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{{model}}', $${{full_prompt}}$$) as RESPONSE").collect()  # noqa: F821

    if result:
        return {{"response": result[0]["RESPONSE"], "status": "success"}}
    return {{"response": "", "status": "empty"}}
$$;
"""
        return sql


def create_app_agent(
    agent_name: str,
    domain: str,
    search_service: str | None = None,
    semantic_model: str | None = None,
    ml_function: str | None = None,
    database: str = "AGENTIC_PLATFORM",
    schema: str = "CORTEX",
) -> dict[str, Any]:
    """High-level function to create an agent for a generated app."""
    builder = CortexAgentBuilder(database=database, schema=schema)

    search_services = [search_service] if search_service else None
    semantic_models = [semantic_model] if semantic_model else None
    ml_functions = [ml_function] if ml_function else None

    return builder.create_agent(
        agent_name=agent_name,
        domain=domain,
        search_services=search_services,
        semantic_models=semantic_models,
        ml_functions=ml_functions,
    )


def invoke_app_agent(
    agent_config: dict[str, Any],
    message: str,
    conversation_history: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Invoke an agent from a generated app."""
    builder = CortexAgentBuilder()
    return builder.invoke_agent(agent_config, message, conversation_history)
