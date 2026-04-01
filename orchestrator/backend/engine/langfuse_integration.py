"""Langfuse observability integration for the orchestrator.

Provides:
- Trace/span management for workflow execution
- LLM generation tracking for Cortex AI calls
- Cost estimation for Snowflake Cortex usage
"""

import os
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any

_langfuse_client = None
_current_trace = None


def get_langfuse():
    global _langfuse_client
    if _langfuse_client is not None:
        return _langfuse_client

    pk = os.environ.get("LANGFUSE_PUBLIC_KEY")
    sk = os.environ.get("LANGFUSE_SECRET_KEY")
    host = os.environ.get("LANGFUSE_HOST", "https://cloud.langfuse.com")

    if pk and sk:
        try:
            from langfuse import Langfuse
            _langfuse_client = Langfuse(
                public_key=pk,
                secret_key=sk,
                host=host,
            )
        except Exception:
            pass
    return _langfuse_client


def create_trace(name: str, metadata: dict | None = None, tags: list[str] | None = None):
    global _current_trace
    lf = get_langfuse()
    if lf:
        _current_trace = lf.trace(
            name=name,
            metadata=metadata or {},
            tags=tags or [],
        )
        return _current_trace
    return None


def get_current_trace():
    return _current_trace


def create_span(name: str, metadata: dict | None = None, parent=None):
    trace = parent or _current_trace
    if trace:
        return trace.span(name=name, metadata=metadata or {})
    return None


def end_span(span, status: str = "success", duration: float = 0, metadata: dict | None = None, error: str | None = None):
    if span:
        span.end(
            metadata={
                "status": status,
                "duration_seconds": round(duration, 2),
                **(metadata or {}),
            },
            level="ERROR" if error else "DEFAULT",
            status_message=error if error else status,
        )


def log_generation(
    name: str,
    model: str,
    prompt: str,
    completion: str,
    duration_ms: float,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    parent=None,
    metadata: dict | None = None,
):
    trace = parent or _current_trace
    if trace:
        est_input = input_tokens or max(1, len(prompt) // 4)
        est_output = output_tokens or max(1, len(completion) // 4)

        trace.generation(
            name=name,
            model=model,
            input=prompt[:2000],
            output=completion[:2000],
            usage={
                "input": est_input,
                "output": est_output,
                "total": est_input + est_output,
                "unit": "TOKENS",
            },
            metadata={
                "duration_ms": round(duration_ms, 1),
                "provider": "snowflake_cortex",
                **(metadata or {}),
            },
            level="DEFAULT",
            status_message="success",
        )


def flush():
    lf = get_langfuse()
    if lf:
        try:
            lf.flush()
        except Exception:
            pass


def get_trace_url() -> str | None:
    if _current_trace:
        try:
            return _current_trace.get_trace_url()
        except Exception:
            pass
    return None


_generation_log: list[dict] = []


def get_generation_log() -> list[dict]:
    return _generation_log


def clear_generation_log():
    global _generation_log
    _generation_log = []


def track_cortex_call(conn, prompt: str, model: str = "mistral-large2", parent_span=None) -> str:
    safe = prompt.replace("'", "''")
    start = time.time()

    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{safe}')"
        )
        rows = cur.fetchall()
        result = rows[0][0].strip() if rows else ""
    finally:
        cur.close()

    duration_ms = (time.time() - start) * 1000

    est_input_tokens = max(1, len(prompt) // 4)
    est_output_tokens = max(1, len(result) // 4)
    est_cost = (est_input_tokens * 0.003 + est_output_tokens * 0.015) / 1000

    _generation_log.append({
        "timestamp": datetime.now().isoformat(),
        "model": model,
        "prompt_chars": len(prompt),
        "completion_chars": len(result),
        "est_input_tokens": est_input_tokens,
        "est_output_tokens": est_output_tokens,
        "est_cost_usd": round(est_cost, 6),
        "duration_ms": round(duration_ms, 1),
    })

    log_generation(
        name=f"cortex-{model}",
        model=model,
        prompt=prompt,
        completion=result,
        duration_ms=duration_ms,
        input_tokens=est_input_tokens,
        output_tokens=est_output_tokens,
        parent=parent_span,
    )

    return result
