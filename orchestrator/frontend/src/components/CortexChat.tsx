import { useState, useRef, useEffect, useCallback } from 'react';
import { useWorkflowStore } from '../stores/workflowStore';
import ReactMarkdown from 'react-markdown';
import { VegaChart } from './VegaChart';
import {
  MessageSquare,
  Send,
  Loader2,
  Trash2,
  Bot,
  User,
  ChevronDown,
  ChevronUp,
  Database,
  Code,
  CheckCircle,
  BarChart3,
  Sparkles,
} from 'lucide-react';

interface ThinkingStep {
  id: string;
  title: string;
  content: string;
  status: 'pending' | 'in_progress' | 'completed';
  sql?: string;
}

interface ChartSpec {
  $schema?: string;
  data?: unknown;
  mark?: unknown;
  encoding?: unknown;
  [key: string]: unknown;
}

interface ToolResult {
  tool_name: string;
  sql?: string;
  content: string;
  data?: Record<string, unknown>[];
}

interface ChatMsg {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
  source?: string;
  thinkingSteps?: ThinkingStep[];
  isStreaming?: boolean;
  toolResults?: ToolResult[];
  chartSpec?: ChartSpec;
  sources?: string[];
}

function DataTablePreview({ data, maxRows = 5 }: { data: Record<string, unknown>[]; maxRows?: number }) {
  if (!data?.length) return null;
  const columns = Object.keys(data[0]);
  const rows = data.slice(0, maxRows);
  return (
    <div className="mt-2 overflow-hidden rounded-lg border border-slate-700">
      <div className="overflow-x-auto">
        <table className="w-full text-[10px]">
          <thead>
            <tr className="bg-slate-800">
              {columns.map((col) => (
                <th key={col} className="px-2 py-1.5 text-left text-slate-400 font-medium whitespace-nowrap">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr key={i} className="border-t border-slate-700/50 hover:bg-slate-800/50">
                {columns.map((col) => (
                  <td key={col} className="px-2 py-1.5 text-slate-300 whitespace-nowrap">
                    {String(row[col] ?? '-')}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {data.length > maxRows && (
        <div className="px-2 py-1 bg-slate-800/50 text-[9px] text-slate-500 text-center">
          Showing {maxRows} of {data.length} rows
        </div>
      )}
    </div>
  );
}

export function CortexChat() {
  const { activeScenario } = useWorkflowStore();
  const [messages, setMessages] = useState<ChatMsg[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [showThinking, setShowThinking] = useState<Record<string, boolean>>({});
  const scrollRef = useRef<HTMLDivElement>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  const toggleThinking = (msgId: string) => {
    setShowThinking((prev) => ({ ...prev, [msgId]: !prev[msgId] }));
  };

  const sendMessage = useCallback(async () => {
    if (!input.trim() || loading) return;
    const userMsg: ChatMsg = {
      id: `msg-${Date.now()}`,
      role: 'user',
      content: input.trim(),
      timestamp: new Date().toISOString(),
    };

    const assistantId = `msg-${Date.now() + 1}`;
    const assistantMsg: ChatMsg = {
      id: assistantId,
      role: 'assistant',
      content: '',
      timestamp: new Date().toISOString(),
      source: 'cortex-agent',
      thinkingSteps: [],
      isStreaming: true,
      toolResults: [],
    };

    setMessages((prev) => [...prev, userMsg, assistantMsg]);
    setInput('');
    setLoading(true);
    setShowThinking((prev) => ({ ...prev, [assistantId]: true }));

    const history = messages.slice(-6).map((m) => ({ role: m.role, content: m.content }));

    try {
      abortRef.current = new AbortController();
      const res = await fetch('/api/chat/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: userMsg.content, history }),
        signal: abortRef.current.signal,
      });

      if (!res.ok || !res.body) {
        throw new Error('Streaming not available');
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let fullContent = '';
      const thinkingSteps: ThinkingStep[] = [];
      const toolResults: ToolResult[] = [];
      let chartSpec: ChartSpec | undefined;
      let sources: string[] = [];
      let stepCounter = 0;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });

        const events = buffer.split('\n\n');
        buffer = events.pop() || '';

        for (const eventStr of events) {
          if (!eventStr.trim() || !eventStr.startsWith('data:')) continue;
          const dataStr = eventStr.replace('data:', '').trim();
          if (dataStr === '[DONE]') continue;

          try {
            const event = JSON.parse(dataStr);

            if (event.type === 'text') {
              fullContent += event.content || '';
              setMessages((prev) =>
                prev.map((m) =>
                  m.id === assistantId ? { ...m, content: fullContent } : m
                )
              );
            } else if (event.type === 'thinking') {
              const text = event.content || '';
              if (text.length > 10) {
                if (thinkingSteps.length > 0) {
                  thinkingSteps[thinkingSteps.length - 1].status = 'completed';
                }
                stepCounter++;
                thinkingSteps.push({
                  id: `step-${stepCounter}`,
                  title: event.title || 'Reasoning',
                  content: text.slice(0, 200) + (text.length > 200 ? '...' : ''),
                  status: 'in_progress',
                });
                if (thinkingSteps.length > 8) thinkingSteps.shift();
                setMessages((prev) =>
                  prev.map((m) =>
                    m.id === assistantId ? { ...m, thinkingSteps: [...thinkingSteps] } : m
                  )
                );
              }
            } else if (event.type === 'status' || event.type === 'tool_status') {
              if (thinkingSteps.length > 0) {
                thinkingSteps[thinkingSteps.length - 1].status = 'completed';
              }
              stepCounter++;
              thinkingSteps.push({
                id: `status-${stepCounter}`,
                title: event.title || event.status || 'Processing',
                content: '',
                status: 'in_progress',
              });
              setMessages((prev) =>
                prev.map((m) =>
                  m.id === assistantId ? { ...m, thinkingSteps: [...thinkingSteps] } : m
                )
              );
            } else if (event.type === 'tool_result') {
              const tr: ToolResult = {
                tool_name: event.tool_name || '',
                sql: event.sql,
                content: event.content || '',
                data: event.data,
              };
              toolResults.push(tr);

              if (event.sql) {
                stepCounter++;
                thinkingSteps.push({
                  id: `sql-${stepCounter}`,
                  title: 'SQL Executed',
                  content: event.error || 'Query completed',
                  status: event.error ? 'pending' : 'completed',
                  sql: event.sql,
                });
              }

              setMessages((prev) =>
                prev.map((m) =>
                  m.id === assistantId
                    ? { ...m, toolResults: [...toolResults], thinkingSteps: [...thinkingSteps] }
                    : m
                )
              );
            } else if (event.type === 'chart') {
              chartSpec = event.chart_spec || event.spec;
              if (chartSpec) {
                setMessages((prev) =>
                  prev.map((m) =>
                    m.id === assistantId ? { ...m, chartSpec } : m
                  )
                );
              }
            } else if (event.type === 'sources') {
              sources = event.sources || [];
            } else if (event.type === 'error') {
              fullContent += `\n\n⚠️ ${event.content || 'An error occurred'}`;
              setMessages((prev) =>
                prev.map((m) =>
                  m.id === assistantId ? { ...m, content: fullContent } : m
                )
              );
            }
          } catch {
            // skip malformed
          }
        }
      }

      thinkingSteps.forEach((s) => (s.status = 'completed'));
      setMessages((prev) =>
        prev.map((m) =>
          m.id === assistantId
            ? {
                ...m,
                content: fullContent || 'I processed your request.',
                thinkingSteps: [...thinkingSteps],
                toolResults: [...toolResults],
                sources,
                isStreaming: false,
              }
            : m
        )
      );

      setTimeout(() => {
        setShowThinking((prev) => ({ ...prev, [assistantId]: false }));
      }, 2000);
    } catch (err: unknown) {
      if (err instanceof Error && err.name === 'AbortError') return;

      try {
        const fallback = await fetch('/api/chat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ message: input.trim(), history }),
        });
        const data = await fallback.json();
        setMessages((prev) =>
          prev.map((m) =>
            m.id === assistantId
              ? { ...m, content: data.response || 'No response', isStreaming: false, source: 'cortex-fallback' }
              : m
          )
        );
      } catch {
        setMessages((prev) =>
          prev.map((m) =>
            m.id === assistantId
              ? { ...m, content: 'Connection error. Please try again.', isStreaming: false }
              : m
          )
        );
      }
    } finally {
      setLoading(false);
      abortRef.current = null;
    }
  }, [input, loading, messages]);

  const clearChat = () => {
    if (abortRef.current) abortRef.current.abort();
    setMessages([]);
    setLoading(false);
  };

  const suggestions = getSuggestions(activeScenario);

  return (
    <div className="h-full flex flex-col">
      <div className="flex items-center justify-between px-4 py-3 border-b border-slate-800">
        <div className="flex items-center gap-2">
          <MessageSquare className="w-4 h-4 text-cyan-400" />
          <span className="text-sm font-semibold text-slate-200">Cortex AI Chat</span>
          <span className="text-[9px] px-1.5 py-0.5 rounded-full bg-cyan-900/50 text-cyan-400">
            Agent
          </span>
        </div>
        {messages.length > 0 && (
          <button onClick={clearChat} className="p-1 hover:bg-slate-800 rounded">
            <Trash2 className="w-3.5 h-3.5 text-slate-500" />
          </button>
        )}
      </div>

      <div ref={scrollRef} className="flex-1 overflow-y-auto p-3 space-y-3">
        {messages.length === 0 ? (
          <div className="space-y-3">
            <p className="text-xs text-slate-500 text-center py-2">
              Ask your healthcare data questions — powered by Cortex Agent
            </p>
            <div className="flex items-center gap-2 mb-2 px-1 text-[10px] text-slate-600">
              <Sparkles className="w-3 h-3" />
              <span>Suggested questions</span>
            </div>
            <div className="flex flex-wrap gap-1.5">
              {suggestions.map((s, i) => (
                <button
                  key={i}
                  onClick={() => setInput(s)}
                  className="px-3 py-1.5 bg-slate-800/50 hover:bg-slate-700/50 rounded-full text-[10px] text-slate-400 hover:text-white transition-colors"
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        ) : (
          messages.map((msg) => (
            <div key={msg.id} className={`flex gap-2 ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
              {msg.role === 'assistant' && (
                <div className="flex-shrink-0 w-6 h-6 rounded-lg bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center mt-1">
                  <Bot className="w-3.5 h-3.5 text-white" />
                </div>
              )}
              <div className={`max-w-[85%] ${msg.role === 'user' ? '' : 'space-y-1'}`}>
                {msg.role === 'assistant' && msg.thinkingSteps && msg.thinkingSteps.length > 0 && (
                  <div className="mb-1">
                    <button
                      onClick={() => toggleThinking(msg.id)}
                      className="flex items-center gap-2 text-[10px] text-slate-400 hover:text-slate-300 transition-colors"
                    >
                      {msg.isStreaming ? (
                        <Loader2 className="w-3 h-3 animate-spin text-cyan-400" />
                      ) : (
                        <CheckCircle className="w-3 h-3 text-green-400" />
                      )}
                      <span>Thinking steps ({msg.thinkingSteps.length})</span>
                      {showThinking[msg.id] ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
                    </button>

                    {showThinking[msg.id] && (
                      <div className="mt-1 bg-slate-900/50 border border-slate-700/50 rounded-lg p-2 space-y-1.5">
                        {msg.thinkingSteps.map((step) => (
                          <div key={step.id} className="flex items-start gap-1.5">
                            {step.status === 'completed' ? (
                              <CheckCircle className="w-3 h-3 text-green-400 mt-0.5 flex-shrink-0" />
                            ) : step.status === 'in_progress' ? (
                              <Loader2 className="w-3 h-3 animate-spin text-cyan-400 mt-0.5 flex-shrink-0" />
                            ) : (
                              <div className="w-3 h-3 rounded-full border border-slate-600 mt-0.5 flex-shrink-0" />
                            )}
                            <div className="flex-1 min-w-0">
                              <div className="text-[10px] font-medium text-slate-300">{step.title}</div>
                              {step.content && (
                                <div className="text-[9px] text-slate-500">{step.content}</div>
                              )}
                              {step.sql && (
                                <div className="mt-1 p-1.5 bg-slate-800 rounded text-[9px] font-mono text-slate-400 overflow-x-auto">
                                  <div className="flex items-center gap-1 text-cyan-400 mb-0.5">
                                    <Code className="w-2.5 h-2.5" />
                                    <span>SQL</span>
                                  </div>
                                  <pre className="whitespace-pre-wrap break-all">
                                    {step.sql.length > 200 ? step.sql.slice(0, 200) + '...' : step.sql}
                                  </pre>
                                </div>
                              )}
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}

                <div
                  className={`px-3 py-2 rounded-lg text-xs ${
                    msg.role === 'user'
                      ? 'bg-cyan-900/50 text-cyan-100'
                      : 'bg-slate-800 border border-slate-700/50'
                  }`}
                >
                  {msg.role === 'assistant' ? (
                    <div className="prose prose-invert prose-xs max-w-none [&_p]:my-1 [&_ul]:my-1 [&_table]:text-xs [&_th]:text-left [&_th]:pr-3 [&_td]:pr-3">
                      {msg.content ? (
                        <ReactMarkdown>{msg.content}</ReactMarkdown>
                      ) : msg.isStreaming ? (
                        <span className="inline-flex items-center gap-1 text-slate-500">
                          <Loader2 className="w-3 h-3 animate-spin" /> Processing...
                        </span>
                      ) : null}

                      {msg.chartSpec && (
                        <div className="mt-3">
                          <div className="flex items-center gap-2 mb-2 text-[10px] text-cyan-400">
                            <BarChart3 className="w-3 h-3" />
                            <span>Generated Visualization</span>
                          </div>
                          <VegaChart spec={msg.chartSpec} className="w-full" />
                        </div>
                      )}

                      {msg.toolResults && msg.toolResults.length > 0 && msg.toolResults.some((tr) => tr.data?.length) && (
                        <div className="mt-2">
                          {msg.toolResults
                            .filter((tr) => tr.data?.length)
                            .map((tr, i) => (
                              <div key={i}>
                                <div className="flex items-center gap-1.5 text-[9px] text-slate-500 mb-1">
                                  <Database className="w-3 h-3" />
                                  <span>{tr.tool_name || 'Query Result'}</span>
                                </div>
                                <DataTablePreview data={tr.data!} />
                              </div>
                            ))}
                        </div>
                      )}
                    </div>
                  ) : (
                    <p className="whitespace-pre-wrap">{msg.content}</p>
                  )}
                </div>

                {msg.role === 'assistant' && msg.sources && msg.sources.length > 0 && (
                  <div className="mt-1 flex flex-wrap items-center gap-1.5 text-[9px] text-slate-500">
                    <Database className="w-2.5 h-2.5" />
                    <span>Sources:</span>
                    {msg.sources.map((source, i) => (
                      <span key={i} className="px-1.5 py-0.5 bg-slate-800 rounded text-slate-400">
                        {source}
                      </span>
                    ))}
                  </div>
                )}

                {msg.source && !msg.sources?.length && (
                  <span className="text-[9px] text-slate-600 mt-0.5 block px-1">via {msg.source}</span>
                )}
              </div>
              {msg.role === 'user' && (
                <div className="flex-shrink-0 w-6 h-6 rounded-lg bg-slate-700 flex items-center justify-center mt-1">
                  <User className="w-3.5 h-3.5 text-slate-300" />
                </div>
              )}
            </div>
          ))
        )}
      </div>

      <div className="p-3 border-t border-slate-800">
        <form
          onSubmit={(e) => {
            e.preventDefault();
            sendMessage();
          }}
          className="flex items-center gap-2 bg-slate-800 rounded-lg px-3 py-2"
        >
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask about your healthcare data..."
            className="flex-1 bg-transparent text-xs text-white placeholder:text-slate-500 outline-none"
            disabled={loading}
          />
          <button
            type="submit"
            disabled={loading || !input.trim()}
            className="p-1 hover:bg-slate-700 rounded disabled:opacity-50"
          >
            {loading ? (
              <Loader2 className="w-3.5 h-3.5 text-cyan-400 animate-spin" />
            ) : (
              <Send className="w-3.5 h-3.5 text-cyan-400" />
            )}
          </button>
        </form>
      </div>
    </div>
  );
}

function getSuggestions(scenario: string | null): string[] {
  switch (scenario) {
    case 'clinical_data_warehouse':
      return [
        'How many patients are high risk?',
        'Show me the patient 360 summary',
        'What conditions are most common?',
        'List patients by risk score',
      ];
    case 'drug_safety':
      return [
        'What are the top safety signals?',
        'Show drugs with highest PRR scores',
        'How many adverse events were reported?',
        'List signals for cardiac events',
      ];
    case 'clinical_docs':
      return [
        'Find documents mentioning diabetes',
        'How many documents have PHI?',
        'Show me discharge summaries',
        'What patient conditions were extracted?',
      ];
    default:
      return [
        'How many patients are in the system?',
        'What drug safety signals were detected?',
        'Show me high-risk patients',
        'Summarize adverse event reports',
      ];
  }
}
