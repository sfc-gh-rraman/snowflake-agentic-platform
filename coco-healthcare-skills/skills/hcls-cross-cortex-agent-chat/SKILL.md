---
name: hcls-cross-cortex-agent-chat
description: Build an AI chat interface powered by Snowflake Cortex Agent REST API with SSE streaming, thinking steps, SQL display, Vega-Lite visualizations, and data table previews. Use when building any Cortex Agent-backed chat UI in a React + FastAPI application deployed to SPCS. Triggers include cortex agent chat, AI chat, SSE streaming chat, mission control chat, agent REST API, thinking steps, vega chart, tool result display, SQL preview.
platform_affinities:
  - cortex-agent
  - build-react-app
  - deploy-to-spcs
  - developing-with-streamlit
---

# Cortex Agent Chat Pattern

Build a production-grade AI chat interface backed by Snowflake Cortex Agent REST API with SSE streaming. This pattern was extracted from the ATLAS Capital Delivery reference application (construction_capital_delivery) and is reusable across any Snowflake-native AI application.

## Architecture

```
Frontend (React)                    Backend (FastAPI)                  Snowflake
─────────────────                   ──────────────────                 ─────────
Chat.tsx                            /api/chat/stream                  Cortex Agent
  │ POST {message}                    │ event_generator()               │ tools: analyst,
  │                                   │   CortexAgentClient               search, generic
  │◄── SSE: thinking ────────────────│◄── response.thinking.delta ────│
  │◄── SSE: status ──────────────────│◄── response.status ────────────│
  │◄── SSE: tool_result (SQL) ───────│◄── response.tool_result ───────│
  │◄── SSE: text ────────────────────│◄── response.output_text.delta ─│
  │◄── SSE: chart (Vega-Lite) ──────│◄── response.chart ─────────────│
  │◄── SSE: [DONE] ─────────────────│◄── response.done ──────────────│
```

## When to Use This Skill

- Building a chat UI over a Cortex Agent (named agent in Snowflake)
- Adding AI Q&A to any Snowflake-native application
- Deploying agent-backed chat to SPCS
- Adding SQL display, chart rendering, or thinking-step UX to existing chat

## SSE Event Types

The Cortex Agent REST API produces these SSE events. The backend normalizes them into 7 frontend event types:

| Cortex Agent SSE Event | Normalized Type | Frontend Action |
|---|---|---|
| `response.output_text.delta` / `response.text.delta` | `text` | Append to visible message content (ReactMarkdown) |
| `response.thinking.delta` | `thinking` | Add to collapsible thinking panel |
| `response.status` | `status` | Add thinking step badge (spinner → checkmark) |
| `response.tool_result.status` | `tool_status` | Tool execution status badge |
| `response.tool_result` | `tool_result` | SQL code block + data table preview |
| `response.chart` | `chart` | Render Vega-Lite visualization inline |
| `response.done` | `done` | Finalize message, collapse thinking |

### Status titles mapping

```python
status_titles = {
    "planning": "Planning analysis approach",
    "reasoning_agent_start": "Starting analysis",
    "reasoning_agent_stop": "Analysis complete",
    "reevaluating_plan": "Refining approach",
    "streaming_analyst_results": "Running SQL query",
}
```

## Backend Implementation

### 1. CortexAgentClient (Python — `backend/services/cortex_agent_client.py`)

Dual-mode authentication: SPCS (OAuth token file) and local (session token).

```python
class CortexAgentClient:
    def __init__(self):
        self.database = os.environ.get("SNOWFLAKE_DATABASE", "MY_DB")
        self.schema = "MY_SCHEMA"
        self.agent_name = "MY_AGENT"
        self._token = None
        self._conn = None  # keep alive for session token

    def _is_spcs(self) -> bool:
        return os.path.isfile("/snowflake/session/token")

    def _get_token(self) -> str:
        # Reuse cached token if connection still alive
        if self._token and self._conn:
            try:
                self._conn.cursor().execute("SELECT 1")
                return self._token
            except Exception:
                pass

        if self._is_spcs():
            with open("/snowflake/session/token") as f:
                self._token = f.read().strip()
            return self._token

        # Local: use connector session token (keep connection alive!)
        conn = snowflake.connector.connect(
            connection_name=os.environ.get("SNOWFLAKE_CONNECTION_NAME", "default")
        )
        self._conn = conn  # CRITICAL: do not close — token dies with connection
        self._token = conn.rest.token

        # Fix hostname for SSL (underscore → dash)
        host = conn.host.replace('_', '-')
        self._account_url = f"https://{host}"
        return self._token

    async def run_agent_stream(self, messages, thread_id=None):
        token = self._get_token()
        url = f"{self._get_base_url()}/api/v2/databases/{self.database}/schemas/{self.schema}/agents/{self.agent_name}:run"

        headers = {"Content-Type": "application/json", "Accept": "text/event-stream"}
        if self._is_spcs():
            headers["Authorization"] = f"Bearer {token}"
            headers["X-Snowflake-Authorization-Token-Type"] = "OAUTH"
        else:
            headers["Authorization"] = f'Snowflake Token="{token}"'

        body = {
            "messages": self._format_messages(messages),  # content must be [{type: text, text: ...}]
            "stream": True,
        }
        if thread_id:
            body["thread_id"] = thread_id

        async with httpx.AsyncClient(timeout=120.0, verify=not self._is_spcs()) as client:
            async with client.stream("POST", url, headers=headers, json=body) as response:
                buffer = ""
                async for chunk in response.aiter_text():
                    buffer += chunk
                    while "\n\n" in buffer:
                        event_str, buffer = buffer.split("\n\n", 1)
                        event = self._parse_sse_event(event_str)
                        if event:
                            yield event
```

**Critical auth notes:**
- SPCS: `Bearer {token}` + `X-Snowflake-Authorization-Token-Type: OAUTH`
- Local: `Snowflake Token="{token}"` (NOT Bearer)
- `conn.host` returns underscores → must `.replace('_', '-')` for SSL cert match
- Session token invalidates when connection closes → keep `self._conn` alive

### 2. SSE Event Parsing (Backend)

```python
def _parse_sse_event(self, event_str: str) -> dict | None:
    event_type = None
    data_parts = []
    for line in event_str.strip().split("\n"):
        if line.startswith("event:"):
            event_type = line[6:].strip()
        elif line.startswith("data:"):
            data_parts.append(line[5:].strip())

    data_str = "\n".join(data_parts)
    if data_str == "[DONE]":
        return {"type": "done"}
    data = json.loads(data_str)
    return self._normalize_event(data, event_type)

def _normalize_event(self, data: dict, event_type: str | None) -> dict:
    if event_type in ("response.output_text.delta", "response.text.delta"):
        return {"type": "text", "content": data.get("text", "")}

    if event_type == "response.thinking.delta":
        return {"type": "thinking", "title": "Reasoning", "content": data.get("text", "")}

    if event_type == "response.status":
        title = status_titles.get(data.get("status", ""), data.get("message", ""))
        return {"type": "status", "title": title, "status": data.get("status", "")}

    if event_type == "response.tool_result.status":
        return {"type": "tool_status", "title": data.get("message", ""), "status": data.get("status", "")}

    if event_type == "response.tool_result":
        content = data.get("content", [])
        result = {"type": "tool_result"}
        for item in content if isinstance(content, list) else []:
            if isinstance(item, dict):
                if "json" in item:
                    json_data = item["json"]
                    if "sql" in json_data:
                        result["sql"] = json_data["sql"]
                    if "data" in json_data:
                        result["data"] = json_data["data"]
                    if "error" in json_data:
                        result["error"] = json_data["error"].get("message", str(json_data["error"]))
                if "text" in item:
                    result["content"] = item["text"]
        return result if len(result) > 1 else None

    if event_type == "response.chart":
        chart_spec = data.get("chart_spec", {})
        if isinstance(chart_spec, str):
            chart_spec = json.loads(chart_spec)
        return {"type": "chart", "chart_spec": chart_spec} if chart_spec else None

    if event_type == "response.done":
        return {"type": "done"}
    return None
```

### 3. FastAPI SSE Endpoint

```python
@app.post("/api/chat/stream")
async def chat_stream(message: ChatMessage):
    async def event_generator():
        agent = get_cortex_agent_client()
        yield f"data: {json.dumps({'type': 'thinking', 'title': 'Planning', 'content': 'Analyzing...'})}\n\n"
        async for event in agent.run_agent(message.message):
            yield f"data: {json.dumps(event)}\n\n"
            await asyncio.sleep(0.01)
        yield "data: [DONE]\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )
```

### 4. Non-Streaming Fallback

Always provide `POST /api/chat` as a fallback when the agent SSE endpoint fails.

## Frontend Implementation

### 1. TypeScript Interfaces

```typescript
interface ThinkingStep {
  id: string
  title: string
  content: string
  status: 'pending' | 'in_progress' | 'completed'
  sql?: string
}

interface ChartSpec {
  $schema?: string
  data?: unknown
  mark?: unknown
  encoding?: unknown
  [key: string]: unknown
}

interface Message {
  id: string
  role: 'user' | 'assistant'
  content: string
  sources?: string[]
  timestamp: Date
  thinkingSteps?: ThinkingStep[]
  isStreaming?: boolean
  chartSpec?: ChartSpec
  toolResults?: { tool_name: string; sql?: string; content: string; data?: Record<string, unknown>[] }[]
}
```

### 2. SSE Stream Reader (Chat.tsx)

```typescript
const reader = response.body.getReader()
const decoder = new TextDecoder()
let buffer = ''
let fullContent = ''
const thinkingSteps: ThinkingStep[] = []

while (true) {
  const { done, value } = await reader.read()
  if (done) break

  buffer += decoder.decode(value, { stream: true })
  const events = buffer.split('\n\n')
  buffer = events.pop() || ''

  for (const eventStr of events) {
    if (!eventStr.trim() || !eventStr.startsWith('data:')) continue
    const dataStr = eventStr.replace('data:', '').trim()
    if (dataStr === '[DONE]') continue

    const event = JSON.parse(dataStr)

    if (event.type === 'text') {
      fullContent += event.content || ''
      // Update message content
    } else if (event.type === 'thinking') {
      // Add to thinking steps panel
    } else if (event.type === 'status' || event.type === 'tool_status') {
      // Add thinking step badge
    } else if (event.type === 'tool_result') {
      // Show SQL in expandable code block
      if (event.sql) {
        thinkingSteps.push({
          id: `sql-${Date.now()}`,
          title: 'SQL Executed',
          content: event.error || 'Query completed',
          status: event.error ? 'pending' : 'completed',
          sql: event.sql,
        })
      }
    } else if (event.type === 'chart') {
      // Set chartSpec on message → renders VegaChart inline
    } else if (event.type === 'error') {
      fullContent += `\n\n⚠️ ${event.content}`
    }
  }
}

// After streaming: mark all steps completed, auto-collapse thinking after 2s
thinkingSteps.forEach(s => s.status = 'completed')
setTimeout(() => setShowThinking(prev => ({ ...prev, [assistantId]: false })), 2000)
```

### 3. VegaChart Component

```typescript
// Install: npm install vega-embed vega vega-lite

import { useEffect, useRef } from 'react'
import embed from 'vega-embed'

export function VegaChart({ spec, className = '' }: { spec: Record<string, unknown>; className?: string }) {
  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!containerRef.current || !spec) return
    const themedSpec = {
      ...spec,
      background: 'transparent',
      config: {
        ...(spec.config as Record<string, unknown> || {}),
        view: { stroke: 'transparent' },
        axis: {
          domainColor: '#475569', gridColor: '#334155',
          tickColor: '#475569', labelColor: '#94a3b8', titleColor: '#e2e8f0',
        },
        legend: { labelColor: '#94a3b8', titleColor: '#e2e8f0' },
        title: { color: '#e2e8f0' },
      },
    }
    embed(containerRef.current, themedSpec as never, { actions: false, theme: 'dark', renderer: 'svg' })
      .catch(console.error)
    return () => { if (containerRef.current) containerRef.current.innerHTML = '' }
  }, [spec])

  return <div ref={containerRef} className={`vega-chart bg-slate-900/50 rounded-lg p-4 ${className}`} />
}
```

### 4. Thinking Steps Panel

```tsx
{message.thinkingSteps?.length > 0 && (
  <div className="mb-2">
    <button onClick={() => toggleThinking(message.id)} className="flex items-center gap-2 text-xs text-slate-400">
      {message.isStreaming ? <Loader2 size={12} className="animate-spin" /> : <CheckCircle size={12} className="text-green-400" />}
      <span>Thinking steps ({message.thinkingSteps.length})</span>
      {showThinking[message.id] ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
    </button>
    {showThinking[message.id] && (
      <div className="bg-slate-900/50 border border-slate-700/50 rounded-lg p-3 space-y-2">
        {message.thinkingSteps.map(step => (
          <div key={step.id} className="flex items-start gap-2">
            {step.status === 'completed' ? <CheckCircle size={14} className="text-green-400" /> : <Loader2 size={14} className="animate-spin text-blue-400" />}
            <div className="flex-1">
              <div className="text-xs font-medium text-slate-300">{step.title}</div>
              {step.sql && (
                <div className="mt-1 p-2 bg-slate-800 rounded text-xs font-mono text-slate-400">
                  <div className="flex items-center gap-1 text-blue-400 mb-1"><Code size={10} /><span>SQL</span></div>
                  <pre className="whitespace-pre-wrap">{step.sql.slice(0, 200)}...</pre>
                </div>
              )}
            </div>
          </div>
        ))}
      </div>
    )}
  </div>
)}
```

### 5. Data Table Preview

```tsx
function DataTablePreview({ data, maxRows = 5 }: { data: Record<string, unknown>[]; maxRows?: number }) {
  if (!data?.length) return null
  const columns = Object.keys(data[0])
  const rows = data.slice(0, maxRows)
  return (
    <div className="mt-3 overflow-hidden rounded-lg border border-slate-700">
      <table className="w-full text-xs">
        <thead><tr className="bg-slate-800">
          {columns.map(col => <th key={col} className="px-3 py-2 text-left text-slate-400">{col}</th>)}
        </tr></thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className="border-t border-slate-700/50">
              {columns.map(col => <td key={col} className="px-3 py-2 text-slate-300">{String(row[col] ?? '-')}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
      {data.length > maxRows && <div className="px-3 py-2 bg-slate-800/50 text-xs text-slate-500 text-center">Showing {maxRows} of {data.length} rows</div>}
    </div>
  )
}
```

## SPCS Deployment

### Nginx config for SSE

```nginx
location /api/chat/stream {
    proxy_pass http://127.0.0.1:8000/api/chat/stream;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header Connection '';
    proxy_buffering off;          # CRITICAL for SSE
    proxy_cache off;
    chunked_transfer_encoding off;
    proxy_read_timeout 300s;
    proxy_set_header Sf-Context-Current-User-Token $http_sf_context_current_user_token;
}
```

### Dockerfile pattern

Multi-stage: Node (frontend build) → Python (runtime with nginx + supervisord).

### Environment variables

| Variable | SPCS Value | Local Value |
|---|---|---|
| `SNOWFLAKE_DATABASE` | Set in service spec | `AGENTIC_PLATFORM` |
| `SNOWFLAKE_HOST` | Auto-set by SPCS | Not needed (from connector) |
| `SNOWFLAKE_ACCOUNT` | Auto-set by SPCS | Not needed (from connector) |
| `SNOWFLAKE_CONNECTION_NAME` | Not used | `my_snowflake` |

## NPM Dependencies

```json
{
  "react-markdown": "^9.0.0",
  "vega": "^5.30.0",
  "vega-lite": "^5.21.0",
  "vega-embed": "^6.26.0",
  "lucide-react": "^0.400.0"
}
```

## Python Dependencies

```
httpx>=0.28.0
snowflake-connector-python>=3.6.0
fastapi>=0.109.0
uvicorn>=0.27.0
```

## Reference Implementation

Source: `construction_capital_delivery/copilot/` (ATLAS Capital Delivery)

| File | Purpose |
|---|---|
| `frontend/src/components/Chat.tsx` | Full SSE chat with 7 event types |
| `frontend/src/components/VegaChart.tsx` | Vega-Lite renderer with dark theme |
| `frontend/src/components/AIThinking.tsx` | ThinkingSteps + QueryExecution + DataTablePreview |
| `backend/api/main.py` | FastAPI with `/api/chat/stream` SSE endpoint |
| `backend/services/cortex_agent_client.py` | Cortex Agent REST client (SPCS auth) |
