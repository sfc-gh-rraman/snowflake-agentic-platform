#!/usr/bin/env python3
"""Execution logger for Health Sciences orchestrator.

Logs orchestrator plans, skill executions, and governance actions to Snowflake
tables for auditability and regulatory compliance (HIPAA).

Tables must exist first — run scripts/setup_observability_tables.sql.

Usage:
    import os
    import snowflake.connector
    from shared.observability.logger import ExecutionLogger

    conn = snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default"
    )
    logger = ExecutionLogger(conn, database="MY_DB", schema="OBSERVABILITY")
    plan_id = logger.log_plan_start("sess-1", "Build imaging pipeline", "Provider", [...])
"""

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass
class PlanEntry:
    session_id: str
    plan_id: str
    user_request: str
    detected_domain: str
    plan_steps: list[dict]
    plan_approved: bool = False
    status: str = "PENDING"


@dataclass
class SkillEntry:
    session_id: str
    plan_id: str
    step_number: int
    skill_name: str
    skill_type: str = "standalone"
    input_context: Optional[dict] = None
    artifacts_produced: Optional[dict] = None
    governance_applied: Optional[dict] = None
    preflight_status: str = "SKIPPED"
    status: str = "PENDING"
    error_message: str = ""


@dataclass
class GovernanceEntry:
    session_id: str
    skill_name: str
    governance_action: str
    target_object: str
    policy_type: str
    policy_definition: Optional[dict] = None


class ExecutionLogger:
    def __init__(self, conn=None, database: str = "", schema: str = "OBSERVABILITY"):
        self._conn = conn
        self._database = database
        self._schema = schema
        self._prefix = f"{database}.{schema}" if database else schema

    def _execute(self, sql: str, params: tuple = ()):
        if self._conn is None:
            return
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params)
        finally:
            cur.close()

    def _to_variant(self, obj: Any) -> Optional[str]:
        if obj is None:
            return None
        return json.dumps(obj)

    def log_plan_start(
        self,
        session_id: str,
        user_request: str,
        detected_domain: str,
        plan_steps: list[dict],
    ) -> str:
        plan_id = str(uuid.uuid4())[:8]
        self._execute(
            f"""INSERT INTO {self._prefix}.ORCHESTRATOR_EXECUTION_LOG
                (session_id, plan_id, user_request, detected_domain, plan_steps, status)
                SELECT %s, %s, %s, %s, PARSE_JSON(%s), 'PENDING'""",
            (session_id, plan_id, user_request, detected_domain,
             self._to_variant(plan_steps)),
        )
        return plan_id

    def log_plan_approved(self, session_id: str, plan_id: str):
        self._execute(
            f"""UPDATE {self._prefix}.ORCHESTRATOR_EXECUTION_LOG
                SET plan_approved = TRUE, status = 'IN_PROGRESS'
                WHERE session_id = %s AND plan_id = %s""",
            (session_id, plan_id),
        )

    def log_plan_complete(self, session_id: str, plan_id: str, status: str = "COMPLETED"):
        self._execute(
            f"""UPDATE {self._prefix}.ORCHESTRATOR_EXECUTION_LOG
                SET status = %s, completed_at = CURRENT_TIMESTAMP()
                WHERE session_id = %s AND plan_id = %s""",
            (status, session_id, plan_id),
        )

    def log_skill_start(
        self,
        session_id: str,
        plan_id: str,
        step_number: int,
        skill_name: str,
        skill_type: str = "standalone",
        input_context: Optional[dict] = None,
        preflight_status: str = "SKIPPED",
    ):
        self._execute(
            f"""INSERT INTO {self._prefix}.SKILL_EXECUTION_LOG
                (session_id, plan_id, step_number, skill_name, skill_type,
                 input_context, preflight_status, status)
                SELECT %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, 'IN_PROGRESS'""",
            (session_id, plan_id, step_number, skill_name, skill_type,
             self._to_variant(input_context), preflight_status),
        )

    def log_skill_complete(
        self,
        session_id: str,
        plan_id: str,
        step_number: int,
        artifacts: Optional[dict] = None,
        governance: Optional[dict] = None,
        status: str = "COMPLETED",
        error_message: str = "",
    ):
        self._execute(
            f"""UPDATE {self._prefix}.SKILL_EXECUTION_LOG
                SET status = %s,
                    completed_at = CURRENT_TIMESTAMP(),
                    artifacts_produced = PARSE_JSON(%s),
                    governance_applied = PARSE_JSON(%s),
                    error_message = %s
                WHERE session_id = %s AND plan_id = %s AND step_number = %s""",
            (status, self._to_variant(artifacts), self._to_variant(governance),
             error_message, session_id, plan_id, step_number),
        )

    def log_governance_action(
        self,
        session_id: str,
        skill_name: str,
        governance_action: str,
        target_object: str,
        policy_type: str,
        policy_definition: Optional[dict] = None,
    ):
        self._execute(
            f"""INSERT INTO {self._prefix}.GOVERNANCE_AUDIT_LOG
                (session_id, skill_name, governance_action, target_object,
                 policy_type, policy_definition)
                SELECT %s, %s, %s, %s, %s, PARSE_JSON(%s)""",
            (session_id, skill_name, governance_action, target_object,
             policy_type, self._to_variant(policy_definition)),
        )

    def get_plan_status(self, session_id: str, plan_id: str) -> Optional[dict]:
        if self._conn is None:
            return None
        cur = self._conn.cursor()
        try:
            cur.execute(
                f"""SELECT status, plan_approved, started_at, completed_at
                    FROM {self._prefix}.ORCHESTRATOR_EXECUTION_LOG
                    WHERE session_id = %s AND plan_id = %s""",
                (session_id, plan_id),
            )
            row = cur.fetchone()
            if row:
                return {
                    "status": row[0],
                    "plan_approved": row[1],
                    "started_at": str(row[2]),
                    "completed_at": str(row[3]) if row[3] else None,
                }
            return None
        finally:
            cur.close()

    def get_skill_steps(self, session_id: str, plan_id: str) -> list[dict]:
        if self._conn is None:
            return []
        cur = self._conn.cursor()
        try:
            cur.execute(
                f"""SELECT step_number, skill_name, skill_type, status,
                           preflight_status, error_message,
                           started_at, completed_at
                    FROM {self._prefix}.SKILL_EXECUTION_LOG
                    WHERE session_id = %s AND plan_id = %s
                    ORDER BY step_number""",
                (session_id, plan_id),
            )
            return [
                {
                    "step_number": row[0],
                    "skill_name": row[1],
                    "skill_type": row[2],
                    "status": row[3],
                    "preflight_status": row[4],
                    "error_message": row[5],
                    "started_at": str(row[6]),
                    "completed_at": str(row[7]) if row[7] else None,
                }
                for row in cur.fetchall()
            ]
        finally:
            cur.close()
