"""Cortex Chat — now backed by the Cortex Agent REST API with SSE streaming.

Provides two endpoints:
  - chat() — non-streaming fallback (returns full JSON)
  - chat_stream() — SSE streaming via Cortex Agent
"""

import asyncio
import json
import os
from typing import Any, AsyncIterator

from ..services.cortex_agent_client import get_cortex_agent_client
from ..engine.langfuse_integration import log_generation


async def chat_stream(message: str, history: list[dict] | None = None) -> AsyncIterator[str]:
    agent = get_cortex_agent_client()

    yield f"data: {json.dumps({'type': 'thinking', 'title': 'Analyzing your question', 'content': 'Connecting to Cortex Agent...'})}\n\n"
    await asyncio.sleep(0.01)

    messages = []
    if history:
        for h in history[-6:]:
            messages.append({"role": h.get("role", "user"), "content": h.get("content", "")})
    messages.append({"role": "user", "content": message})

    full_text = ""
    try:
        async for event in agent.run_agent_stream(messages):
            event_type = event.get("type", "raw")

            if event_type == "text":
                content = event.get("content", "")
                full_text += content
                yield f"data: {json.dumps({'type': 'text', 'content': content})}\n\n"

            elif event_type == "tool_result":
                payload = {
                    'type': 'tool_result',
                    'tool_name': event.get('tool_name', ''),
                    'content': str(event.get('content', ''))[:2000],
                }
                if event.get('sql'):
                    payload['sql'] = event['sql']
                if event.get('data'):
                    payload['data'] = event['data'][:50]
                if event.get('error'):
                    payload['error'] = event['error']
                yield f"data: {json.dumps(payload)}\n\n"

            elif event_type == "status":
                yield f"data: {json.dumps({'type': 'status', 'title': event.get('title', event.get('message', 'Processing')), 'status': event.get('status', '')})}\n\n"

            elif event_type == "tool_status":
                yield f"data: {json.dumps({'type': 'tool_status', 'title': event.get('title', ''), 'status': event.get('status', '')})}\n\n"

            elif event_type == "chart":
                yield f"data: {json.dumps({'type': 'chart', 'chart_spec': event.get('chart_spec', event.get('spec', {}))})}\n\n"

            elif event_type == "error":
                yield f"data: {json.dumps({'type': 'error', 'content': event.get('content', 'Unknown error')})}\n\n"

            elif event_type == "done":
                break

            elif event_type == "raw":
                data = event.get("data", {})
                if isinstance(data, dict):
                    delta = data.get("delta", {})
                    content_parts = delta.get("content", [])
                    if isinstance(content_parts, list):
                        for part in content_parts:
                            ptype = part.get("type", "")
                            if ptype == "text":
                                txt = part.get("text", "")
                                full_text += txt
                                yield f"data: {json.dumps({'type': 'text', 'content': txt})}\n\n"
                            elif ptype == "tool_results":
                                yield f"data: {json.dumps({'type': 'tool_result', 'tool_name': part.get('tool_name', ''), 'sql': part.get('sql', ''), 'content': str(part.get('content', ''))[:2000]})}\n\n"

            await asyncio.sleep(0.01)

    except Exception as e:
        yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"

    log_generation(
        name="cortex-agent-chat",
        model="cortex-agent",
        prompt=message,
        completion=full_text[:500],
        duration_ms=0,
    )

    yield "data: [DONE]\n\n"


def chat(message: str, history: list[dict] | None = None) -> dict:
    """Non-streaming fallback using Cortex COMPLETE for backward compatibility."""
    import snowflake.connector

    DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "AGENTIC_PLATFORM")
    CORTEX_MODEL = os.environ.get("CORTEX_MODEL", "mistral-large2")

    if os.path.exists("/snowflake/session/token"):
        with open("/snowflake/session/token") as f:
            token = f.read().strip()
        conn = snowflake.connector.connect(
            host=os.environ.get("SNOWFLAKE_HOST", ""),
            account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
            authenticator="oauth",
            token=token,
            database=DATABASE,
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        )
    else:
        conn_name = os.environ.get("SNOWFLAKE_CONNECTION_NAME", "my_snowflake")
        conn = snowflake.connector.connect(connection_name=conn_name)
        cur = conn.cursor()
        cur.execute(f"USE DATABASE {DATABASE}")
        cur.execute(f"USE WAREHOUSE {os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')}")
        cur.close()

    from ..engine.langfuse_integration import track_cortex_call

    history_text = ""
    if history:
        for h in history[-4:]:
            role = h.get("role", "user")
            content = h.get("content", "")[:200]
            history_text += f"\n{role}: {content}"

    prompt = f"""You are a healthcare data assistant for Snowflake's AGENTIC_PLATFORM database.
Answer concisely about patients, drug safety, and clinical documents.

Conversation history:{history_text}

User: {message}

Keep answers concise (3-5 sentences). If you don't have specific data, explain what tables are available.
Response:"""

    response = track_cortex_call(conn, prompt, model=CORTEX_MODEL)
    conn.close()

    return {
        "response": response,
        "tool_called": False,
        "tool_name": None,
        "tool_result": None,
    }
