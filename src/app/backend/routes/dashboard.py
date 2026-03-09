"""Dashboard metrics routes."""

from fastapi import APIRouter

from ..db import execute_query

router = APIRouter()


@router.get("/metrics")
async def get_metrics():
    rows = execute_query("""
        SELECT
            (SELECT COUNT(*) FROM AGENTIC_PLATFORM.STATE.EXECUTION_PLANS WHERE status = 'running') AS active_executions,
            (SELECT COUNT(*) FROM AGENTIC_PLATFORM.STATE.EXECUTION_PLANS WHERE status = 'completed' AND created_at > DATEADD('day', -1, CURRENT_TIMESTAMP())) AS completed_24h,
            (SELECT COUNT(*) FROM AGENTIC_PLATFORM.STATE.EXECUTION_PLANS WHERE status = 'failed' AND created_at > DATEADD('day', -1, CURRENT_TIMESTAMP())) AS failed_24h,
            (SELECT COALESCE(SUM(input_tokens + output_tokens), 0) FROM AGENTIC_PLATFORM.STATE.CORTEX_CALL_LOGS WHERE created_at > DATEADD('day', -1, CURRENT_TIMESTAMP())) AS total_tokens_24h,
            (SELECT COUNT(*) FROM AGENTIC_PLATFORM.STATE.ARTIFACTS WHERE created_at > DATEADD('day', -1, CURRENT_TIMESTAMP())) AS artifacts_created_24h,
            (SELECT COUNT(*) FROM AGENTIC_PLATFORM.STATE.PHASE_STATE WHERE status = 'failed' AND started_at > DATEADD('day', -1, CURRENT_TIMESTAMP())) AS phase_failures_24h
    """)

    if rows:
        row = rows[0]
        return {
            "active_executions": row.get("ACTIVE_EXECUTIONS", 0) or 0,
            "completed_24h": row.get("COMPLETED_24H", 0) or 0,
            "failed_24h": row.get("FAILED_24H", 0) or 0,
            "total_tokens_24h": row.get("TOTAL_TOKENS_24H", 0) or 0,
            "artifacts_created_24h": row.get("ARTIFACTS_CREATED_24H", 0) or 0,
            "phase_failures_24h": row.get("PHASE_FAILURES_24H", 0) or 0,
        }

    return {
        "active_executions": 0,
        "completed_24h": 0,
        "failed_24h": 0,
        "total_tokens_24h": 0,
        "artifacts_created_24h": 0,
        "phase_failures_24h": 0,
    }
