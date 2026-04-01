import { useEffect, useState, useMemo } from 'react';
import { useWorkflowStore } from '../stores/workflowStore';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar } from 'recharts';
import {
  BarChart3,
  Activity,
  CheckCircle,
  XCircle,
  Clock,
  AlertTriangle,
  Info,
  Loader2,
  Cpu,
  DollarSign,
  TrendingUp,
} from 'lucide-react';

const statusConfig: Record<string, { color: string; bg: string; icon: typeof CheckCircle }> = {
  success: { color: 'text-green-400', bg: 'bg-green-500', icon: CheckCircle },
  failed: { color: 'text-red-400', bg: 'bg-red-500', icon: XCircle },
  running: { color: 'text-cyan-400', bg: 'bg-cyan-500', icon: Loader2 },
  pending: { color: 'text-slate-400', bg: 'bg-slate-500', icon: Clock },
  skipped: { color: 'text-yellow-400', bg: 'bg-yellow-500', icon: AlertTriangle },
};

const logConfig: Record<string, { color: string; bg: string }> = {
  info: { color: 'text-blue-400', bg: 'bg-blue-500' },
  success: { color: 'text-green-400', bg: 'bg-green-500' },
  error: { color: 'text-red-400', bg: 'bg-red-500' },
  warning: { color: 'text-yellow-400', bg: 'bg-yellow-500' },
};

export function ObservabilityDashboard() {
  const { phases, logs, observabilityData, computeObservability } = useWorkflowStore();
  const [langfuseData, setLangfuseData] = useState<{
    generations: number;
    total_tokens: number;
    total_cost_usd: number;
    trace_url: string | null;
    generation_log: Array<{
      model: string;
      prompt_chars: number;
      completion_chars: number;
      est_input_tokens: number;
      est_output_tokens: number;
      est_cost_usd: number;
      duration_ms: number;
    }>;
  }>({ generations: 0, total_tokens: 0, total_cost_usd: 0, trace_url: null, generation_log: [] });

  useEffect(() => {
    computeObservability();
  }, [phases, logs, computeObservability]);

  useEffect(() => {
    const fetchLangfuse = async () => {
      try {
        const res = await fetch('/api/workflow/langfuse');
        if (res.ok) setLangfuseData(await res.json());
      } catch {}
    };
    fetchLangfuse();
    const interval = setInterval(fetchLangfuse, 5000);
    return () => clearInterval(interval);
  }, []);

  const { taskDurations, statusCounts, logLevelCounts } = observabilityData;
  const totalTasks = Object.values(statusCounts).reduce((s, v) => s + v, 0);
  const totalLogs = Object.values(logLevelCounts).reduce((s, v) => s + v, 0);
  const maxDuration = taskDurations.reduce((m, t) => Math.max(m, t.duration), 1);
  const avgDuration = taskDurations.length > 0
    ? taskDurations.reduce((s, t) => s + t.duration, 0) / taskDurations.length
    : 0;

  const alerts = useMemo(() => {
    const result: { type: 'warning' | 'critical'; message: string; taskId?: string }[] = [];
    if (avgDuration > 0) {
      for (const td of taskDurations) {
        if (td.duration > avgDuration * 2.5) {
          result.push({
            type: 'critical',
            message: `"${td.name}" took ${td.duration.toFixed(1)}s (${(td.duration / avgDuration).toFixed(1)}x avg)`,
            taskId: td.id,
          });
        } else if (td.duration > avgDuration * 1.8) {
          result.push({
            type: 'warning',
            message: `"${td.name}" took ${td.duration.toFixed(1)}s (${(td.duration / avgDuration).toFixed(1)}x avg)`,
            taskId: td.id,
          });
        }
      }
    }
    if (langfuseData.total_cost_usd > 0.05) {
      result.push({
        type: 'warning',
        message: `Cortex AI cost: $${langfuseData.total_cost_usd.toFixed(4)} — monitor for budget`,
      });
    }
    if ((statusCounts.failed || 0) > 0) {
      result.push({
        type: 'critical',
        message: `${statusCounts.failed} task(s) failed`,
      });
    }
    return result;
  }, [taskDurations, avgDuration, langfuseData.total_cost_usd, statusCounts]);

  const timeSeriesData = useMemo(() => {
    const allTasks = phases.flatMap((p) => p.tasks);
    const completedTasks = allTasks.filter((t) => t.duration !== null && t.status === 'success');
    return completedTasks.map((t, idx) => ({
      name: t.name.length > 12 ? t.name.slice(0, 12) + '…' : t.name,
      duration: t.duration || 0,
      index: idx,
    }));
  }, [phases]);

  const tokenTimeSeries = useMemo(() => {
    return langfuseData.generation_log.map((g, idx) => ({
      name: `Call ${idx + 1}`,
      tokens: (g.est_input_tokens || 0) + (g.est_output_tokens || 0),
      cost: g.est_cost_usd * 1000,
      duration: g.duration_ms,
    }));
  }, [langfuseData.generation_log]);

  return (
    <div className="h-full flex flex-col bg-slate-950 p-4 overflow-y-auto">
      <div className="flex items-center gap-2 mb-4">
        <BarChart3 className="w-5 h-5 text-cyan-400" />
        <h2 className="text-lg font-semibold text-white">Observability Dashboard</h2>
      </div>

      <div className="grid grid-cols-4 gap-3 mb-6">
        <div className="bg-slate-800 rounded-lg p-3">
          <span className="text-[10px] text-slate-500 uppercase">Total Tasks</span>
          <p className="text-2xl font-bold text-white">{totalTasks}</p>
        </div>
        <div className="bg-slate-800 rounded-lg p-3">
          <span className="text-[10px] text-slate-500 uppercase">Success Rate</span>
          <p className="text-2xl font-bold text-green-400">
            {totalTasks > 0 ? Math.round(((statusCounts.success || 0) / totalTasks) * 100) : 0}%
          </p>
        </div>
        <div className="bg-slate-800 rounded-lg p-3">
          <span className="text-[10px] text-slate-500 uppercase">Avg Duration</span>
          <p className="text-2xl font-bold text-cyan-400">{avgDuration.toFixed(1)}s</p>
        </div>
        <div className="bg-slate-800 rounded-lg p-3">
          <span className="text-[10px] text-slate-500 uppercase">Alerts</span>
          <p className={`text-2xl font-bold ${alerts.length > 0 ? 'text-amber-400' : 'text-green-400'}`}>{alerts.length}</p>
        </div>
      </div>

      {alerts.length > 0 && (
        <div className="bg-amber-900/20 border border-amber-800/50 rounded-lg p-3 mb-4 space-y-1.5">
          <h3 className="text-xs font-semibold text-amber-300 flex items-center gap-1.5">
            <AlertTriangle className="w-3 h-3" />
            Active Alerts ({alerts.length})
          </h3>
          {alerts.map((a, i) => (
            <div key={i} className={`flex items-center gap-2 text-xs ${a.type === 'critical' ? 'text-red-400' : 'text-amber-400'}`}>
              <span className={`w-1.5 h-1.5 rounded-full ${a.type === 'critical' ? 'bg-red-500' : 'bg-amber-500'}`} />
              {a.message}
            </div>
          ))}
        </div>
      )}

      {timeSeriesData.length > 0 && (
        <div className="bg-slate-900 rounded-lg p-4 border border-slate-800 mb-4">
          <h3 className="text-xs font-semibold text-slate-300 mb-3 flex items-center gap-1.5">
            <TrendingUp className="w-3 h-3 text-cyan-400" />
            Task Duration Timeline
          </h3>
          <ResponsiveContainer width="100%" height={160}>
            <BarChart data={timeSeriesData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="name" tick={{ fontSize: 9, fill: '#94a3b8' }} />
              <YAxis tick={{ fontSize: 9, fill: '#94a3b8' }} unit="s" />
              <Tooltip
                contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', fontSize: 11 }}
                labelStyle={{ color: '#e2e8f0' }}
              />
              <Bar dataKey="duration" fill="#06b6d4" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {tokenTimeSeries.length > 0 && (
        <div className="bg-slate-900 rounded-lg p-4 border border-slate-800 mb-4">
          <h3 className="text-xs font-semibold text-slate-300 mb-3 flex items-center gap-1.5">
            <Cpu className="w-3 h-3 text-amber-400" />
            LLM Token Usage Over Time
          </h3>
          <ResponsiveContainer width="100%" height={140}>
            <LineChart data={tokenTimeSeries}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="name" tick={{ fontSize: 9, fill: '#94a3b8' }} />
              <YAxis tick={{ fontSize: 9, fill: '#94a3b8' }} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', fontSize: 11 }}
              />
              <Line type="monotone" dataKey="tokens" stroke="#f59e0b" strokeWidth={2} dot={{ r: 3 }} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      <div className="grid grid-cols-2 gap-4 mb-6">
        <div className="bg-slate-900 rounded-lg p-4 border border-slate-800">
          <h3 className="text-xs font-semibold text-slate-300 mb-3 flex items-center gap-1.5">
            <Activity className="w-3 h-3 text-cyan-400" />
            Task Status Distribution
          </h3>
          <div className="space-y-2">
            {Object.entries(statusCounts).map(([status, count]) => {
              const cfg = statusConfig[status] || statusConfig.pending;
              const pct = totalTasks > 0 ? (count / totalTasks) * 100 : 0;
              const Icon = cfg.icon;
              return (
                <div key={status} className="flex items-center gap-2">
                  <Icon className={`w-3 h-3 ${cfg.color}`} />
                  <span className="text-xs text-slate-400 w-16 capitalize">{status}</span>
                  <div className="flex-1 h-4 bg-slate-800 rounded-full overflow-hidden">
                    <div
                      className={`h-full ${cfg.bg} rounded-full transition-all duration-500`}
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                  <span className="text-xs text-slate-300 w-8 text-right">{count}</span>
                </div>
              );
            })}
          </div>
        </div>

        <div className="bg-slate-900 rounded-lg p-4 border border-slate-800">
          <h3 className="text-xs font-semibold text-slate-300 mb-3 flex items-center gap-1.5">
            <Info className="w-3 h-3 text-blue-400" />
            Log Level Distribution
          </h3>
          <div className="space-y-2">
            {Object.entries(logLevelCounts).map(([level, count]) => {
              const cfg = logConfig[level] || logConfig.info;
              const pct = totalLogs > 0 ? (count / totalLogs) * 100 : 0;
              return (
                <div key={level} className="flex items-center gap-2">
                  <div className={`w-3 h-3 rounded ${cfg.bg}`} />
                  <span className="text-xs text-slate-400 w-16 capitalize">{level}</span>
                  <div className="flex-1 h-4 bg-slate-800 rounded-full overflow-hidden">
                    <div
                      className={`h-full ${cfg.bg} rounded-full transition-all duration-500`}
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                  <span className="text-xs text-slate-300 w-8 text-right">{count}</span>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      <div className="bg-slate-900 rounded-lg p-4 border border-slate-800">
        <h3 className="text-xs font-semibold text-slate-300 mb-3 flex items-center gap-1.5">
          <Clock className="w-3 h-3 text-purple-400" />
          Task Duration Chart
        </h3>
        {taskDurations.length > 0 ? (
          <div className="space-y-1.5">
            {taskDurations
              .sort((a, b) => b.duration - a.duration)
              .map((t) => {
                const pct = (t.duration / maxDuration) * 100;
                const cfg = statusConfig[t.status] || statusConfig.pending;
                return (
                  <div key={t.id} className="flex items-center gap-2">
                    <span className="text-[10px] text-slate-400 w-36 truncate text-right">
                      {t.name}
                    </span>
                    <div className="flex-1 h-5 bg-slate-800 rounded overflow-hidden">
                      <div
                        className={`h-full ${cfg.bg} rounded transition-all duration-500 flex items-center px-1.5`}
                        style={{ width: `${Math.max(pct, 5)}%` }}
                      >
                        <span className="text-[9px] text-white font-medium whitespace-nowrap">
                          {t.duration.toFixed(1)}s
                        </span>
                      </div>
                    </div>
                  </div>
                );
              })}
          </div>
        ) : (
          <p className="text-xs text-slate-500 text-center py-4">
            Execute a scenario to see duration metrics
          </p>
        )}
      </div>

      <div className="bg-slate-900 rounded-lg p-4 border border-slate-800 mt-4">
        <h3 className="text-xs font-semibold text-slate-300 mb-3 flex items-center gap-1.5">
          <Cpu className="w-3 h-3 text-amber-400" />
          Cortex AI / Langfuse Observability
        </h3>
        <div className="grid grid-cols-3 gap-3 mb-3">
          <div className="bg-slate-800 rounded p-2">
            <span className="text-[10px] text-slate-500 uppercase">LLM Calls</span>
            <p className="text-lg font-bold text-amber-400">{langfuseData.generations}</p>
          </div>
          <div className="bg-slate-800 rounded p-2">
            <span className="text-[10px] text-slate-500 uppercase">Total Tokens</span>
            <p className="text-lg font-bold text-white">{langfuseData.total_tokens.toLocaleString()}</p>
          </div>
          <div className="bg-slate-800 rounded p-2">
            <span className="text-[10px] text-slate-500 uppercase flex items-center gap-1">
              <DollarSign className="w-2.5 h-2.5" /> Est. Cost
            </span>
            <p className="text-lg font-bold text-green-400">
              ${langfuseData.total_cost_usd.toFixed(4)}
            </p>
          </div>
        </div>
        {langfuseData.generation_log.length > 0 ? (
          <div className="space-y-1">
            {langfuseData.generation_log.map((g, i) => (
              <div key={i} className="flex items-center gap-2 text-[10px]">
                <span className="text-amber-400 w-24 truncate">{g.model}</span>
                <span className="text-slate-500">{g.prompt_chars}ch→{g.completion_chars}ch</span>
                <span className="text-slate-400">{g.duration_ms.toFixed(0)}ms</span>
                <span className="text-green-400 ml-auto">${g.est_cost_usd.toFixed(5)}</span>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-xs text-slate-500 text-center py-2">
            No Cortex AI calls tracked yet
          </p>
        )}
        {langfuseData.trace_url && (
          <a
            href={langfuseData.trace_url}
            target="_blank"
            rel="noopener noreferrer"
            className="mt-2 block text-xs text-cyan-400 hover:text-cyan-300 underline"
          >
            View trace in Langfuse →
          </a>
        )}
      </div>
    </div>
  );
}
