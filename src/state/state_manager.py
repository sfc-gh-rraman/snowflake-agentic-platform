"""State manager for agent execution state in Snowflake.

Provides CRUD operations for execution plans, phases, agent states, and artifacts.
"""

import json
import os
from enum import StrEnum
from typing import Any
from uuid import uuid4


class PlanStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PhaseStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class AgentStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class ArtifactType(StrEnum):
    TABLE = "table"
    VIEW = "view"
    STAGE = "stage"
    FILE = "file"
    ML_MODEL = "ml_model"
    ML_MODEL_VERSION = "ml_model_version"
    FEATURE_STORE = "feature_store"
    CORTEX_SEARCH_SERVICE = "cortex_search_service"
    SEMANTIC_MODEL = "semantic_model"
    SEMANTIC_VIEW = "semantic_view"
    SPCS_SERVICE = "spcs_service"
    SPCS_IMAGE = "spcs_image"
    COMPUTE_POOL = "compute_pool"
    CORTEX_AGENT = "cortex_agent"
    APP_SPEC = "app_spec"
    GENERATED_CODE = "generated_code"


class StateManager:
    """Manages agent execution state in Snowflake."""

    def __init__(
        self,
        connection_name: str | None = None,
        database: str = "AGENTIC_PLATFORM",
        schema: str = "STATE",
    ):
        self.connection_name = connection_name or os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        self.database = database
        self.schema = schema
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
        return f"'{str(value).replace(chr(39), chr(39) + chr(39))}'"

    def _json_escape(self, obj: Any) -> str:
        if obj is None:
            return "NULL"
        json_str = json.dumps(obj).replace("'", "''")
        return f"PARSE_JSON('{json_str}')"

    def _table(self, name: str) -> str:
        return f"{self.database}.{self.schema}.{name}"

    def create_execution_plan(
        self,
        use_case_description: str,
        execution_plan: dict[str, Any],
        parsed_requirements: dict | None = None,
        data_assets: list[dict] | None = None,
    ) -> str:
        plan_id = str(uuid4())
        sql = f"""
            INSERT INTO {self._table("AGENT_EXECUTION_PLANS")}
            (plan_id, use_case_description, parsed_requirements, data_assets, execution_plan)
            VALUES (
                '{plan_id}',
                {self._escape(use_case_description)},
                {self._json_escape(parsed_requirements)},
                {self._json_escape(data_assets)},
                {self._json_escape(execution_plan)}
            )
        """
        self._execute(sql)
        return plan_id

    def get_execution_plan(self, plan_id: str) -> dict | None:
        sql = f"""
            SELECT * FROM {self._table("AGENT_EXECUTION_PLANS")}
            WHERE plan_id = '{plan_id}'
        """
        results = self._execute(sql)
        return results[0] if results else None

    def update_plan_status(
        self,
        plan_id: str,
        status: PlanStatus,
        error_message: str | None = None,
    ) -> None:
        completed_at = (
            "CURRENT_TIMESTAMP()" if status in [PlanStatus.COMPLETED, PlanStatus.FAILED] else "NULL"
        )
        sql = f"""
            UPDATE {self._table("AGENT_EXECUTION_PLANS")}
            SET status = '{status.value}',
                error_message = {self._escape(error_message)},
                completed_at = {completed_at},
                updated_at = CURRENT_TIMESTAMP()
            WHERE plan_id = '{plan_id}'
        """
        self._execute(sql)

    def approve_plan(self, plan_id: str, approved_by: str) -> None:
        sql = f"""
            UPDATE {self._table("AGENT_EXECUTION_PLANS")}
            SET approval_status = 'approved',
                approved_by = {self._escape(approved_by)},
                approved_at = CURRENT_TIMESTAMP(),
                status = 'approved',
                updated_at = CURRENT_TIMESTAMP()
            WHERE plan_id = '{plan_id}'
        """
        self._execute(sql)

    def reject_plan(self, plan_id: str, approved_by: str) -> None:
        sql = f"""
            UPDATE {self._table("AGENT_EXECUTION_PLANS")}
            SET approval_status = 'rejected',
                approved_by = {self._escape(approved_by)},
                approved_at = CURRENT_TIMESTAMP(),
                status = 'cancelled',
                updated_at = CURRENT_TIMESTAMP()
            WHERE plan_id = '{plan_id}'
        """
        self._execute(sql)

    def create_phase(
        self,
        plan_id: str,
        phase_name: str,
        phase_order: int,
        config: dict | None = None,
    ) -> str:
        phase_id = str(uuid4())
        sql = f"""
            INSERT INTO {self._table("AGENT_PHASE_STATE")}
            (phase_id, plan_id, phase_name, phase_order, config)
            VALUES (
                '{phase_id}',
                '{plan_id}',
                {self._escape(phase_name)},
                {phase_order},
                {self._json_escape(config)}
            )
        """
        self._execute(sql)
        return phase_id

    def get_phase(self, phase_id: str) -> dict | None:
        sql = f"""
            SELECT * FROM {self._table("AGENT_PHASE_STATE")}
            WHERE phase_id = '{phase_id}'
        """
        results = self._execute(sql)
        return results[0] if results else None

    def get_phases_for_plan(self, plan_id: str) -> list[dict]:
        sql = f"""
            SELECT * FROM {self._table("AGENT_PHASE_STATE")}
            WHERE plan_id = '{plan_id}'
            ORDER BY phase_order
        """
        return self._execute(sql)

    def update_phase_status(
        self,
        phase_id: str,
        status: PhaseStatus,
        output_artifacts: dict | None = None,
        error_message: str | None = None,
    ) -> None:
        started_at = (
            "COALESCE(started_at, CURRENT_TIMESTAMP())"
            if status == PhaseStatus.RUNNING
            else "started_at"
        )
        completed_at = (
            "CURRENT_TIMESTAMP()"
            if status in [PhaseStatus.COMPLETED, PhaseStatus.FAILED]
            else "NULL"
        )

        sql = f"""
            UPDATE {self._table("AGENT_PHASE_STATE")}
            SET status = '{status.value}',
                output_artifacts = COALESCE({self._json_escape(output_artifacts)}, output_artifacts),
                error_message = {self._escape(error_message)},
                started_at = {started_at},
                completed_at = {completed_at},
                updated_at = CURRENT_TIMESTAMP()
            WHERE phase_id = '{phase_id}'
        """
        self._execute(sql)

    def create_agent_execution(
        self,
        phase_id: str,
        plan_id: str,
        agent_name: str,
        agent_version: str | None = None,
        sub_state: str | None = None,
        state_data: dict | None = None,
    ) -> str:
        execution_id = str(uuid4())
        sql = f"""
            INSERT INTO {self._table("AGENT_EXECUTION_STATE")}
            (execution_id, phase_id, plan_id, agent_name, agent_version, sub_state, state_data)
            VALUES (
                '{execution_id}',
                '{phase_id}',
                '{plan_id}',
                {self._escape(agent_name)},
                {self._escape(agent_version)},
                {self._escape(sub_state)},
                {self._json_escape(state_data)}
            )
        """
        self._execute(sql)
        return execution_id

    def update_agent_state(
        self,
        execution_id: str,
        sub_state: str | None = None,
        state_data: dict | None = None,
        status: AgentStatus | None = None,
        error_message: str | None = None,
        error_details: dict | None = None,
    ) -> None:
        updates = ["updated_at = CURRENT_TIMESTAMP()"]

        if sub_state is not None:
            updates.append(f"sub_state = {self._escape(sub_state)}")
        if state_data is not None:
            updates.append(f"state_data = {self._json_escape(state_data)}")
        if status is not None:
            updates.append(f"status = '{status.value}'")
            if status == AgentStatus.RUNNING:
                updates.append("started_at = COALESCE(started_at, CURRENT_TIMESTAMP())")
            elif status in [AgentStatus.COMPLETED, AgentStatus.FAILED]:
                updates.append("completed_at = CURRENT_TIMESTAMP()")
            elif status == AgentStatus.RETRYING:
                updates.append("retry_count = retry_count + 1")
        if error_message is not None:
            updates.append(f"error_message = {self._escape(error_message)}")
        if error_details is not None:
            updates.append(f"error_details = {self._json_escape(error_details)}")

        sql = f"""
            UPDATE {self._table("AGENT_EXECUTION_STATE")}
            SET {", ".join(updates)}
            WHERE execution_id = '{execution_id}'
        """
        self._execute(sql)

    def get_agent_execution(self, execution_id: str) -> dict | None:
        sql = f"""
            SELECT * FROM {self._table("AGENT_EXECUTION_STATE")}
            WHERE execution_id = '{execution_id}'
        """
        results = self._execute(sql)
        return results[0] if results else None

    def register_artifact(
        self,
        artifact_type: ArtifactType,
        artifact_name: str,
        artifact_reference: str,
        plan_id: str | None = None,
        phase_id: str | None = None,
        metadata: dict | None = None,
        created_by_agent: str | None = None,
    ) -> str:
        artifact_id = str(uuid4())
        sql = f"""
            INSERT INTO {self._table("AGENT_ARTIFACTS")}
            (artifact_id, plan_id, phase_id, artifact_type, artifact_name, artifact_reference, metadata, created_by_agent)
            VALUES (
                '{artifact_id}',
                {self._escape(plan_id) if plan_id else "NULL"},
                {self._escape(phase_id) if phase_id else "NULL"},
                '{artifact_type.value}',
                {self._escape(artifact_name)},
                {self._escape(artifact_reference)},
                {self._json_escape(metadata)},
                {self._escape(created_by_agent)}
            )
        """
        self._execute(sql)
        return artifact_id

    def get_artifacts_by_type(
        self, artifact_type: ArtifactType, plan_id: str | None = None
    ) -> list[dict]:
        where_clause = f"artifact_type = '{artifact_type.value}' AND status = 'active'"
        if plan_id:
            where_clause += f" AND plan_id = '{plan_id}'"

        sql = f"""
            SELECT * FROM {self._table("AGENT_ARTIFACTS")}
            WHERE {where_clause}
            ORDER BY created_at DESC
        """
        return self._execute(sql)

    def log_cortex_call(
        self,
        call_type: str,
        model_name: str,
        prompt_text: str,
        response_text: str,
        latency_ms: int,
        plan_id: str | None = None,
        phase_id: str | None = None,
        execution_id: str | None = None,
        prompt_tokens: int | None = None,
        response_tokens: int | None = None,
        status: str = "success",
        error_message: str | None = None,
        metadata: dict | None = None,
    ) -> str:
        log_id = str(uuid4())
        total_tokens = (
            (prompt_tokens or 0) + (response_tokens or 0)
            if prompt_tokens or response_tokens
            else None
        )

        sql = f"""
            INSERT INTO {self._table("CORTEX_CALL_LOGS")}
            (log_id, plan_id, phase_id, execution_id, call_type, model_name,
             prompt_text, prompt_tokens, response_text, response_tokens, total_tokens,
             latency_ms, status, error_message, metadata)
            VALUES (
                '{log_id}',
                {self._escape(plan_id) if plan_id else "NULL"},
                {self._escape(phase_id) if phase_id else "NULL"},
                {self._escape(execution_id) if execution_id else "NULL"},
                {self._escape(call_type)},
                {self._escape(model_name)},
                {self._escape(prompt_text[:10000] if prompt_text else None)},
                {prompt_tokens or "NULL"},
                {self._escape(response_text[:10000] if response_text else None)},
                {response_tokens or "NULL"},
                {total_tokens or "NULL"},
                {latency_ms},
                {self._escape(status)},
                {self._escape(error_message)},
                {self._json_escape(metadata)}
            )
        """
        self._execute(sql)
        return log_id
