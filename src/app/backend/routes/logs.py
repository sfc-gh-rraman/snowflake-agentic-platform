"""Logging routes for Cortex calls."""

from fastapi import APIRouter

from ..db import execute_query

router = APIRouter()


@router.get("/cortex")
async def get_cortex_logs(plan_id: str | None = None, limit: int = 100):
    if plan_id:
        rows = execute_query(
            """
            SELECT
                call_id, call_type, model, input_tokens,
                output_tokens, latency_ms, created_at
            FROM AGENTIC_PLATFORM.STATE.CORTEX_CALL_LOGS
            WHERE plan_id = %s
            ORDER BY created_at DESC
            LIMIT %s
        """,
            (plan_id, limit),
        )
    else:
        rows = execute_query(
            """
            SELECT
                call_id, call_type, model, input_tokens,
                output_tokens, latency_ms, created_at
            FROM AGENTIC_PLATFORM.STATE.CORTEX_CALL_LOGS
            ORDER BY created_at DESC
            LIMIT %s
        """,
            (limit,),
        )

    logs = [
        {
            "call_id": row["CALL_ID"],
            "call_type": row["CALL_TYPE"],
            "model": row["MODEL"],
            "input_tokens": row["INPUT_TOKENS"],
            "output_tokens": row["OUTPUT_TOKENS"],
            "latency_ms": row["LATENCY_MS"],
            "created_at": str(row["CREATED_AT"]),
        }
        for row in rows
    ]

    return {"logs": logs}
