"""Agent Registry Query - Search for agents by capability using Cortex Search."""

import json
import os

from .models import AgentDefinition, AgentSearchResult


class AgentRegistryQuery:
    """Query agent registry using Cortex Search for capability matching."""

    def __init__(
        self,
        connection_name: str | None = None,
        database: str = "AGENTIC_PLATFORM",
        schema: str = "REGISTRY",
        search_service: str = "AGENT_CAPABILITY_SEARCH",
    ):
        self.connection_name = connection_name or os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        self.database = database
        self.schema = schema
        self.search_service = search_service
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

    def _escape(self, value: str) -> str:
        if value is None:
            return "NULL"
        return str(value).replace("'", "''").replace('"', '\\"')

    def search_agents(
        self,
        query: str,
        input_type: str | None = None,
        output_type: str | None = None,
        category: str | None = None,
        limit: int = 10,
    ) -> list[AgentSearchResult]:
        service = f"{self.database}.{self.schema}.{self.search_service}"

        filter_parts = []
        if input_type:
            filter_parts.append(f'"input_types": "{self._escape(input_type)}"')
        if output_type:
            filter_parts.append(f'"output_types": "{self._escape(output_type)}"')
        if category:
            filter_parts.append(f'"category": "{self._escape(category)}"')

        filter_json = "{" + ", ".join(filter_parts) + "}" if filter_parts else "{}"

        search_query = {
            "query": self._escape(query),
            "columns": [
                "chunk_text",
                "agent_name",
                "capability_name",
                "input_types",
                "output_types",
                "full_definition",
            ],
            "filter": json.loads(filter_json) if filter_json != "{}" else {},
            "limit": limit,
        }

        search_json = json.dumps(search_query).replace("'", "''")

        sql = f"""
            SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                '{service}',
                '{search_json}'
            ) as RESULT
        """

        results = self._execute(sql)
        if not results:
            return []

        result_data = results[0].get("RESULT")
        if isinstance(result_data, str):
            result_data = json.loads(result_data)

        search_results = []
        for item in result_data.get("results", []):
            search_results.append(
                AgentSearchResult(
                    agent_id=item.get("agent_id", ""),
                    agent_name=item.get("agent_name", ""),
                    capability_name=item.get("capability_name", ""),
                    score=item.get("score", 0.0),
                    input_types=item.get("input_types", []),
                    output_types=item.get("output_types", []),
                    full_definition=item.get("full_definition", {}),
                )
            )

        return search_results

    def get_agent(self, agent_id: str) -> AgentDefinition | None:
        sql = f"""
            SELECT definition
            FROM {self.database}.{self.schema}.AGENT_DEFINITIONS
            WHERE agent_id = '{self._escape(agent_id)}' AND is_active = TRUE
        """
        results = self._execute(sql)
        if not results:
            return None

        definition_data = results[0].get("DEFINITION")
        if isinstance(definition_data, str):
            definition_data = json.loads(definition_data)

        return AgentDefinition.from_dict(definition_data)

    def list_agents(
        self, category: str | None = None, active_only: bool = True
    ) -> list[AgentDefinition]:
        where_clauses = []
        if active_only:
            where_clauses.append("is_active = TRUE")
        if category:
            where_clauses.append(f"category = '{self._escape(category)}'")

        where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

        sql = f"""
            SELECT definition
            FROM {self.database}.{self.schema}.AGENT_DEFINITIONS
            WHERE {where_sql}
            ORDER BY category, name
        """
        results = self._execute(sql)

        agents = []
        for row in results:
            definition_data = row.get("DEFINITION")
            if isinstance(definition_data, str):
                definition_data = json.loads(definition_data)
            agents.append(AgentDefinition.from_dict(definition_data))

        return agents

    def register_agent(self, agent: AgentDefinition) -> str:
        definition_json = json.dumps(agent.to_dict()).replace("'", "''")

        sql = f"""
            INSERT INTO {self.database}.{self.schema}.AGENT_DEFINITIONS
            (agent_id, name, version, description, category, definition)
            VALUES (
                '{self._escape(agent.agent_id)}',
                '{self._escape(agent.name)}',
                '{self._escape(agent.version)}',
                '{self._escape(agent.description)}',
                '{self._escape(agent.category.value)}',
                PARSE_JSON('{definition_json}')
            )
            ON CONFLICT (agent_id) DO UPDATE SET
                name = EXCLUDED.name,
                version = EXCLUDED.version,
                description = EXCLUDED.description,
                category = EXCLUDED.category,
                definition = EXCLUDED.definition,
                updated_at = CURRENT_TIMESTAMP()
        """
        self._execute(sql)
        self._index_capabilities(agent)
        return agent.agent_id

    def _index_capabilities(self, agent: AgentDefinition) -> None:
        self._execute(f"""
            DELETE FROM {self.database}.{self.schema}.AGENT_CAPABILITY_CHUNKS
            WHERE agent_id = '{self._escape(agent.agent_id)}'
        """)

        for capability in agent.capabilities:
            chunk_text = f"""
            Agent: {agent.name}
            Category: {agent.category.value}
            Description: {agent.description}

            Capability: {capability.name}
            {capability.description or ""}

            Inputs: {", ".join(capability.input_types)}
            Outputs: {", ".join(capability.output_types)}
            """

            definition_json = json.dumps(agent.to_dict()).replace("'", "''")
            input_array = "[" + ", ".join(f"'{t}'" for t in capability.input_types) + "]"
            output_array = "[" + ", ".join(f"'{t}'" for t in capability.output_types) + "]"

            sql = f"""
                INSERT INTO {self.database}.{self.schema}.AGENT_CAPABILITY_CHUNKS
                (agent_id, agent_name, agent_version, category, capability_id, capability_name,
                 chunk_text, input_types, output_types, priority, full_definition)
                VALUES (
                    '{self._escape(agent.agent_id)}',
                    '{self._escape(agent.name)}',
                    '{self._escape(agent.version)}',
                    '{self._escape(agent.category.value)}',
                    '{self._escape(capability.capability_id)}',
                    '{self._escape(capability.name)}',
                    '{self._escape(chunk_text)}',
                    ARRAY_CONSTRUCT{input_array.replace("[", "(").replace("]", ")")},
                    ARRAY_CONSTRUCT{output_array.replace("[", "(").replace("]", ")")},
                    50,
                    PARSE_JSON('{definition_json}')
                )
            """
            self._execute(sql)

    def find_agents_for_task(
        self,
        task_description: str,
        input_types: list[str],
        output_types: list[str],
    ) -> list[AgentSearchResult]:
        query = f"{task_description} input:{' '.join(input_types)} output:{' '.join(output_types)}"
        return self.search_agents(query, limit=5)
