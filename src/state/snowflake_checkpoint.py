"""LangGraph checkpoint saver for Snowflake.

Persists LangGraph checkpoints to Snowflake tables for execution resumability.
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, Iterator, Optional, Tuple
from uuid import uuid4

from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)

from src.config import get_settings


class SnowflakeCheckpointSaver(BaseCheckpointSaver):
    """Checkpoint saver that persists to Snowflake LANGGRAPH_CHECKPOINTS table."""

    def __init__(
        self,
        connection_name: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        table: str = "LANGGRAPH_CHECKPOINTS",
    ):
        super().__init__()
        settings = get_settings()
        self.connection_name = connection_name or settings.connection_name
        self.database = database or settings.database
        self.schema = schema or settings.orchestrator_schema
        self.table = table
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

    def _execute(self, sql: str, params: Optional[Dict] = None) -> list:
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

    def _get_full_table_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"

    def get_tuple(self, config: Dict[str, Any]) -> Optional[CheckpointTuple]:
        thread_id = config["configurable"]["thread_id"]
        checkpoint_id = config["configurable"].get("checkpoint_id")

        table = self._get_full_table_name()
        
        if checkpoint_id:
            sql = f"""
                SELECT checkpoint_id, thread_id, parent_checkpoint_id, 
                       checkpoint_data, metadata, created_at
                FROM {table}
                WHERE thread_id = '{thread_id}' AND checkpoint_id = '{checkpoint_id}'
            """
        else:
            sql = f"""
                SELECT checkpoint_id, thread_id, parent_checkpoint_id,
                       checkpoint_data, metadata, created_at
                FROM {table}
                WHERE thread_id = '{thread_id}'
                ORDER BY created_at DESC
                LIMIT 1
            """

        results = self._execute(sql)
        if not results:
            return None

        row = results[0]
        checkpoint_data = row["CHECKPOINT_DATA"]
        if isinstance(checkpoint_data, str):
            checkpoint_data = json.loads(checkpoint_data)

        metadata_data = row.get("METADATA", {})
        if isinstance(metadata_data, str):
            metadata_data = json.loads(metadata_data)

        return CheckpointTuple(
            config={
                "configurable": {
                    "thread_id": row["THREAD_ID"],
                    "checkpoint_id": row["CHECKPOINT_ID"],
                }
            },
            checkpoint=Checkpoint(
                v=checkpoint_data.get("v", 1),
                id=row["CHECKPOINT_ID"],
                ts=checkpoint_data.get("ts", row["CREATED_AT"].isoformat() if row.get("CREATED_AT") else datetime.utcnow().isoformat()),
                channel_values=checkpoint_data.get("channel_values", {}),
                channel_versions=checkpoint_data.get("channel_versions", {}),
                versions_seen=checkpoint_data.get("versions_seen", {}),
            ),
            metadata=CheckpointMetadata(**metadata_data) if metadata_data else None,
            parent_config={
                "configurable": {
                    "thread_id": row["THREAD_ID"],
                    "checkpoint_id": row["PARENT_CHECKPOINT_ID"],
                }
            } if row.get("PARENT_CHECKPOINT_ID") else None,
        )

    def list(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        table = self._get_full_table_name()
        
        where_clauses = []
        if config and "configurable" in config:
            thread_id = config["configurable"].get("thread_id")
            if thread_id:
                where_clauses.append(f"thread_id = '{thread_id}'")

        if before and "configurable" in before:
            before_id = before["configurable"].get("checkpoint_id")
            if before_id:
                where_clauses.append(f"created_at < (SELECT created_at FROM {table} WHERE checkpoint_id = '{before_id}')")

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        limit_sql = f"LIMIT {limit}" if limit else ""

        sql = f"""
            SELECT checkpoint_id, thread_id, parent_checkpoint_id,
                   checkpoint_data, metadata, created_at
            FROM {table}
            WHERE {where_sql}
            ORDER BY created_at DESC
            {limit_sql}
        """

        results = self._execute(sql)
        for row in results:
            checkpoint_data = row["CHECKPOINT_DATA"]
            if isinstance(checkpoint_data, str):
                checkpoint_data = json.loads(checkpoint_data)

            metadata_data = row.get("METADATA", {})
            if isinstance(metadata_data, str):
                metadata_data = json.loads(metadata_data)

            yield CheckpointTuple(
                config={
                    "configurable": {
                        "thread_id": row["THREAD_ID"],
                        "checkpoint_id": row["CHECKPOINT_ID"],
                    }
                },
                checkpoint=Checkpoint(
                    v=checkpoint_data.get("v", 1),
                    id=row["CHECKPOINT_ID"],
                    ts=checkpoint_data.get("ts", ""),
                    channel_values=checkpoint_data.get("channel_values", {}),
                    channel_versions=checkpoint_data.get("channel_versions", {}),
                    versions_seen=checkpoint_data.get("versions_seen", {}),
                ),
                metadata=CheckpointMetadata(**metadata_data) if metadata_data else None,
                parent_config={
                    "configurable": {
                        "thread_id": row["THREAD_ID"],
                        "checkpoint_id": row["PARENT_CHECKPOINT_ID"],
                    }
                } if row.get("PARENT_CHECKPOINT_ID") else None,
            )

    def put(
        self,
        config: Dict[str, Any],
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
    ) -> Dict[str, Any]:
        thread_id = config["configurable"]["thread_id"]
        parent_checkpoint_id = config["configurable"].get("checkpoint_id")
        checkpoint_id = str(uuid4())

        checkpoint_data = {
            "v": checkpoint.get("v", 1),
            "ts": checkpoint.get("ts", datetime.utcnow().isoformat()),
            "channel_values": checkpoint.get("channel_values", {}),
            "channel_versions": checkpoint.get("channel_versions", {}),
            "versions_seen": checkpoint.get("versions_seen", {}),
        }

        metadata_dict = dict(metadata) if metadata else {}

        table = self._get_full_table_name()
        checkpoint_json = json.dumps(checkpoint_data).replace("'", "''")
        metadata_json = json.dumps(metadata_dict).replace("'", "''")
        parent_sql = f"'{parent_checkpoint_id}'" if parent_checkpoint_id else "NULL"

        sql = f"""
            INSERT INTO {table} (checkpoint_id, thread_id, parent_checkpoint_id, checkpoint_data, metadata)
            VALUES ('{checkpoint_id}', '{thread_id}', {parent_sql}, PARSE_JSON('{checkpoint_json}'), PARSE_JSON('{metadata_json}'))
        """

        self._execute(sql)

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_id": checkpoint_id,
            }
        }

    def put_writes(
        self,
        config: Dict[str, Any],
        writes: list,
        task_id: str,
    ) -> None:
        pass
