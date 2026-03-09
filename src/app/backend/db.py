"""Database connection utilities for Snowflake."""

import os
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any


def get_connection():
    if os.path.exists("/snowflake/session/token"):
        import snowflake.connector

        with open("/snowflake/session/token") as f:
            token = f.read()
        return snowflake.connector.connect(
            host=os.environ.get("SNOWFLAKE_HOST"),
            account=os.environ.get("SNOWFLAKE_ACCOUNT"),
            authenticator="oauth",
            token=token,
        )

    import snowflake.connector

    return snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    )


@contextmanager
def get_cursor() -> Generator:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
        conn.close()


def execute_query(sql: str, params: tuple = None) -> list[dict[str, Any]]:
    with get_cursor() as cursor:
        cursor.execute(sql, params)
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        return []


def execute_non_query(sql: str, params: tuple = None) -> None:
    with get_cursor() as cursor:
        cursor.execute(sql, params)
