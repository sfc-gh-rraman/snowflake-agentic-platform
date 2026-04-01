import { useEffect, useState } from 'react';
import { useWorkflowStore } from '../stores/workflowStore';
import { DollarSign, TrendingUp, Zap, Clock, Database, RefreshCw } from 'lucide-react';

interface RealCostData {
  source: string;
  total_credits: number;
  total_queries: number;
  query_costs: Array<{
    QUERY_TYPE: string;
    QUERY_COUNT: number;
    CLOUD_CREDITS: number;
    TOTAL_SECONDS: number;
    AVG_SECONDS: number;
  }>;
  warehouse_costs: Array<{
    WAREHOUSE_NAME: string;
    CREDITS_USED: number;
    COMPUTE_CREDITS: number;
    CLOUD_CREDITS: number;
    QUERY_COUNT: number;
  }>;
  error?: string;
}

export function CostTracker() {
  const { phases } = useWorkflowStore();
  const [realCosts, setRealCosts] = useState<RealCostData | null>(null);
  const [loading, setLoading] = useState(false);

  const fetchRealCosts = async () => {
    setLoading(true);
    try {
      const res = await fetch('/api/workflow/costs');
      if (res.ok) setRealCosts(await res.json());
    } catch {}
    setLoading(false);
  };

  useEffect(() => {
    fetchRealCosts();
    const interval = setInterval(fetchRealCosts, 30000);
    return () => clearInterval(interval);
  }, []);

  const allTasks = phases.flatMap((p) => p.tasks);
  const completed = allTasks.filter((t) => t.status === 'success' && t.duration);
  const totalDuration = completed.reduce((s, t) => s + (t.duration || 0), 0);

  const totalCredits = realCosts?.total_credits ?? 0;
  const totalQueries = realCosts?.total_queries ?? completed.length;
  const isReal = realCosts?.source === 'QUERY_HISTORY';

  return (
    <div className="p-4 space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <DollarSign className="w-4 h-4 text-green-400" />
          <span className="text-sm font-semibold text-white">Cost & Credit Tracker</span>
          {isReal && (
            <span className="text-[9px] px-1.5 py-0.5 rounded-full bg-green-900/50 text-green-400">LIVE</span>
          )}
        </div>
        <button onClick={fetchRealCosts} disabled={loading} className="p-1 hover:bg-slate-800 rounded">
          <RefreshCw className={`w-3.5 h-3.5 text-slate-500 ${loading ? 'animate-spin' : ''}`} />
        </button>
      </div>

      <div className="grid grid-cols-3 gap-3">
        <div className="bg-slate-800 rounded-lg p-3">
          <div className="flex items-center gap-1.5 mb-1">
            <Zap className="w-3 h-3 text-amber-400" />
            <span className="text-[10px] text-slate-500 uppercase">Credits Used</span>
          </div>
          <span className="text-lg font-bold text-white">{totalCredits.toFixed(4)}</span>
          <span className="text-[10px] text-slate-500 block">~${(totalCredits * 3).toFixed(2)}</span>
        </div>
        <div className="bg-slate-800 rounded-lg p-3">
          <div className="flex items-center gap-1.5 mb-1">
            <TrendingUp className="w-3 h-3 text-cyan-400" />
            <span className="text-[10px] text-slate-500 uppercase">Queries</span>
          </div>
          <span className="text-lg font-bold text-white">{totalQueries}</span>
        </div>
        <div className="bg-slate-800 rounded-lg p-3">
          <div className="flex items-center gap-1.5 mb-1">
            <Clock className="w-3 h-3 text-purple-400" />
            <span className="text-[10px] text-slate-500 uppercase">Compute Time</span>
          </div>
          <span className="text-lg font-bold text-white">{totalDuration.toFixed(1)}s</span>
        </div>
      </div>

      {realCosts && realCosts.warehouse_costs && realCosts.warehouse_costs.length > 0 && (
        <div className="space-y-1">
          <h3 className="text-[10px] text-slate-500 uppercase tracking-wider px-2 flex items-center gap-1">
            <Database className="w-3 h-3" /> Warehouse Credits (Last 2h)
          </h3>
          {realCosts.warehouse_costs.map((wh) => (
            <div
              key={wh.WAREHOUSE_NAME}
              className="flex items-center justify-between px-2 py-1.5 bg-slate-800/30 rounded hover:bg-slate-800/60"
            >
              <span className="text-xs text-slate-300">{wh.WAREHOUSE_NAME}</span>
              <div className="flex items-center gap-3">
                <span className="text-xs text-green-300">{(wh.CREDITS_USED || 0).toFixed(4)} cr</span>
                <span className="text-xs text-slate-500">{wh.QUERY_COUNT} queries</span>
              </div>
            </div>
          ))}
        </div>
      )}

      {realCosts && realCosts.query_costs && realCosts.query_costs.length > 0 && (
        <div className="space-y-1">
          <h3 className="text-[10px] text-slate-500 uppercase tracking-wider px-2">
            Query Types (Last 2h)
          </h3>
          {realCosts.query_costs.slice(0, 8).map((q) => (
            <div
              key={q.QUERY_TYPE}
              className="flex items-center justify-between px-2 py-1.5 bg-slate-800/30 rounded hover:bg-slate-800/60"
            >
              <span className="text-xs text-slate-300 truncate max-w-[120px]">{q.QUERY_TYPE}</span>
              <div className="flex items-center gap-3">
                <span className="text-xs text-slate-400">{q.QUERY_COUNT} queries</span>
                <span className="text-xs text-slate-500">{(q.AVG_SECONDS || 0).toFixed(1)}s avg</span>
              </div>
            </div>
          ))}
        </div>
      )}

      {(!realCosts || realCosts.source === 'error') && completed.length > 0 && (
        <div className="space-y-1">
          <div className="grid grid-cols-4 gap-2 text-[10px] text-slate-500 uppercase tracking-wider px-2">
            <span>Task</span>
            <span className="text-right">Est. Credits</span>
            <span className="text-right">Queries</span>
            <span className="text-right">Duration</span>
          </div>
          {completed.map((t) => {
            let creditsPerSec = 0.00056;
            if (t.skill_type === 'platform') creditsPerSec = 0.0012;
            if (t.skill_name?.includes('cortex') || t.skill_name?.includes('ml')) creditsPerSec = 0.0025;
            const credits = (t.duration || 0) * creditsPerSec;
            return (
              <div
                key={t.id}
                className="grid grid-cols-4 gap-2 items-center px-2 py-1.5 bg-slate-800/30 rounded hover:bg-slate-800/60"
              >
                <span className="text-xs text-slate-300 truncate">{t.name}</span>
                <span className="text-xs text-green-300 text-right">{credits.toFixed(4)}</span>
                <span className="text-xs text-slate-400 text-right">{Math.max(1, Math.floor((t.duration || 0) / 3))}</span>
                <span className="text-xs text-slate-400 text-right">{(t.duration || 0).toFixed(1)}s</span>
              </div>
            );
          })}
        </div>
      )}

      <div className="text-[10px] text-slate-600 border-t border-slate-800 pt-2">
        {isReal
          ? '* Live data from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY (2h window)'
          : '* Cost data from Snowflake (may take up to 45min to populate in ACCOUNT_USAGE)'}
      </div>
    </div>
  );
}
