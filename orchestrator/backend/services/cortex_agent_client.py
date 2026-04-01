"""Cortex Agent REST API client with SSE streaming support.

Calls the Cortex Agent via:
  POST /api/v2/databases/{db}/schemas/{schema}/agents/{name}:run

Supports both SPCS (OAuth token file) and local (JWT/token-based) auth.
"""

import json
import os
import time
from typing import AsyncIterator

import httpx
import snowflake.connector


DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "AGENTIC_PLATFORM")

STATUS_TITLES = {
    "planning": "Planning analysis approach",
    "reasoning_agent_start": "Starting analysis",
    "reasoning_agent_stop": "Analysis complete",
    "reevaluating_plan": "Refining approach",
    "streaming_analyst_results": "Running SQL query",
}
SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "ORCHESTRATOR")
AGENT_NAME = os.environ.get("CORTEX_AGENT_NAME", "HEALTH_COPILOT_AGENT")
WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")


class CortexAgentClient:
    def __init__(self):
        self._token: str | None = None
        self._token_expiry: float = 0
        self._account_url: str | None = None
        self._auth_type: str = "SNOWFLAKE_TOKEN"
        self._conn: snowflake.connector.SnowflakeConnection | None = None

    def _is_spcs(self) -> bool:
        return os.path.isfile("/snowflake/session/token")

    def _get_token(self) -> str:
        if self._token and time.time() < self._token_expiry and self._conn:
            try:
                self._conn.cursor().execute("SELECT 1")
                return self._token
            except Exception:
                pass

        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

        if self._is_spcs():
            with open("/snowflake/session/token") as f:
                self._token = f.read().strip()
            self._token_expiry = time.time() + 300
            return self._token

        conn = snowflake.connector.connect(
            connection_name=os.environ.get("SNOWFLAKE_CONNECTION_NAME", "my_snowflake")
        )
        self._conn = conn

        self._token = conn.rest.token
        self._token_expiry = time.time() + 540
        self._auth_type = 'SNOWFLAKE_TOKEN'

        host = conn.host
        if host and '.' in host:
            host = host.replace('_', '-')
            self._account_url = f"https://{host}"
        else:
            account = conn.account.replace('_', '-')
            self._account_url = f"https://{account}.snowflakecomputing.com"

        return self._token

    def _get_base_url(self) -> str:
        if self._is_spcs():
            return "https://localhost"

        if self._account_url:
            return self._account_url

        self._get_token()
        if self._account_url:
            return self._account_url

        raise RuntimeError("Could not determine Snowflake account URL")

    def _get_agent_url(self) -> str:
        base = self._get_base_url()
        db = DATABASE
        schema = SCHEMA
        agent = AGENT_NAME
        return f"{base}/api/v2/databases/{db}/schemas/{schema}/agents/{agent}:run"

    def _format_messages(self, messages: list[dict]) -> list[dict]:
        formatted = []
        for msg in messages:
            content = msg.get("content", "")
            if isinstance(content, str):
                content = [{"type": "text", "text": content}]
            formatted.append({
                "role": msg.get("role", "user"),
                "content": content,
            })
        return formatted

    def _parse_sse_event(self, event_str: str) -> dict | None:
        event_type = None
        data_parts = []

        for line in event_str.strip().split("\n"):
            if line.startswith("event:"):
                event_type = line[6:].strip()
            elif line.startswith("data:"):
                data_parts.append(line[5:].strip())
            elif line.startswith(":"):
                continue

        if not data_parts:
            return None

        data_str = "\n".join(data_parts)
        if data_str == "[DONE]":
            return {"type": "done"}

        try:
            data = json.loads(data_str)
        except json.JSONDecodeError:
            return {"type": "text", "content": data_str}

        if event_type:
            data["_event_type"] = event_type

        return self._normalize_event(data, event_type)

    def _normalize_event(self, data: dict, event_type: str | None) -> dict:
        if event_type == "response.text.delta" or event_type == "response.output_text.delta":
            return {"type": "text", "content": data.get("text", "")}

        if event_type == "response.thinking.delta":
            return {"type": "thinking", "content": data.get("text", "")}

        if event_type == "response.status":
            status_key = data.get("status", "")
            title = STATUS_TITLES.get(status_key, data.get("message", status_key))
            return {
                "type": "status",
                "title": title,
                "status": status_key,
            }

        if event_type == "response.tool_result":
            content_list = data.get("content", [])
            result = {
                "type": "tool_result",
                "tool_name": data.get("name", data.get("tool_name", "")),
            }
            for item in content_list if isinstance(content_list, list) else []:
                if isinstance(item, dict):
                    if "json" in item:
                        json_data = item["json"]
                        if isinstance(json_data, dict):
                            if "sql" in json_data:
                                result["sql"] = json_data["sql"]
                            if "data" in json_data:
                                result["data"] = json_data["data"]
                            if "error" in json_data:
                                err = json_data["error"]
                                result["error"] = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                        result.setdefault("content", json.dumps(json_data)[:2000])
                    elif "text" in item:
                        result["content"] = item["text"]
                elif isinstance(item, str):
                    result["content"] = item
            return result if len(result) > 1 else None

        if event_type == "response.chart":
            chart_spec = data.get("chart_spec", {})
            if isinstance(chart_spec, str):
                try:
                    chart_spec = json.loads(chart_spec)
                except json.JSONDecodeError:
                    return None
            return {"type": "chart", "chart_spec": chart_spec} if chart_spec else None

        if event_type == "response.tool_use.delta" or event_type == "response.tool_use":
            return {
                "type": "tool_use",
                "tool_name": data.get("name", ""),
                "input": data.get("input", {}),
            }

        if event_type == "response.tool_result.status":
            return {
                "type": "tool_status",
                "title": data.get("message", ""),
                "status": data.get("status", ""),
            }

        if event_type == "response.suggested_queries":
            return {"type": "raw", "data": data}

        if event_type in ("response.done", "response.text", "response"):
            return {"type": "raw", "data": data}

        delta = data.get("delta", {})
        if event_type == "message.delta" or "delta" in data:
            content_parts = delta.get("content", [])
            for part in content_parts if isinstance(content_parts, list) else []:
                ptype = part.get("type", "")
                if ptype == "tool_results":
                    return {
                        "type": "tool_result",
                        "tool_name": part.get("tool_name", ""),
                        "content": part.get("content", ""),
                        "sql": part.get("sql", ""),
                        "data": part.get("data"),
                    }
                if ptype == "text":
                    return {"type": "text", "content": part.get("text", "")}
                if ptype == "chart":
                    return {"type": "chart", "spec": part.get("chart_spec", part)}

        if "status" in data and isinstance(data.get("status"), str):
            return {
                "type": "status",
                "status": data["status"],
                "message": data.get("message", ""),
            }

        if "content" in data and isinstance(data["content"], str):
            return {"type": "text", "content": data["content"]}

        return {"type": "raw", "data": data}

    async def run_agent_stream(
        self,
        messages: list[dict],
        thread_id: str | None = None,
    ) -> AsyncIterator[dict]:
        token = self._get_token()
        url = self._get_agent_url()

        headers = {
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        }

        if self._is_spcs():
            headers["Authorization"] = f"Bearer {token}"
            headers["X-Snowflake-Authorization-Token-Type"] = "OAUTH"
        else:
            headers["Authorization"] = f'Snowflake Token="{token}"'

        body: dict = {
            "messages": self._format_messages(messages),
            "stream": True,
        }
        if thread_id:
            body["thread_id"] = thread_id

        async with httpx.AsyncClient(timeout=120.0, verify=not self._is_spcs()) as client:
            async with client.stream("POST", url, headers=headers, json=body) as response:
                if response.status_code != 200:
                    error_text = ""
                    async for chunk in response.aiter_text():
                        error_text += chunk
                    yield {
                        "type": "error",
                        "content": f"Agent API error {response.status_code}: {error_text[:500]}",
                    }
                    return

                buffer = ""
                async for chunk in response.aiter_text():
                    buffer += chunk
                    while "\n\n" in buffer:
                        event_str, buffer = buffer.split("\n\n", 1)
                        if event_str.strip():
                            event = self._parse_sse_event(event_str)
                            if event:
                                yield event

    async def run_agent(
        self,
        message: str,
        history: list[dict] | None = None,
        thread_id: str | None = None,
    ) -> AsyncIterator[dict]:
        messages = []
        if history:
            messages.extend(history)
        messages.append({"role": "user", "content": message})

        async for event in self.run_agent_stream(messages, thread_id=thread_id):
            yield event


_client: CortexAgentClient | None = None


def get_cortex_agent_client() -> CortexAgentClient:
    global _client
    if _client is None:
        _client = CortexAgentClient()
    return _client
