import { useMemo } from 'react';
import { useWorkflowStore } from '../stores/workflowStore';
import { Clock, CheckCircle, XCircle, Loader2 } from 'lucide-react';

const statusBarColors: Record<string, string> = {
  success: 'bg-green-500',
  failed: 'bg-red-500',
  running: 'bg-cyan-500',
  pending: 'bg-slate-600',
  skipped: 'bg-yellow-500',
};

export function TimelineView() {
  const { phases } = useWorkflowStore();

  const { tasks, maxDuration } = useMemo(() => {
    const allTasks = phases.flatMap((p) =>
      p.tasks.map((t) => ({ ...t, phaseName: p.name }))
    );
    const completedTasks = allTasks.filter(
      (t) => t.duration !== null && t.duration > 0
    );
    const max = completedTasks.reduce(
      (m, t) => Math.max(m, t.duration || 0),
      1
    );
    return { tasks: allTasks, maxDuration: max };
  }, [phases]);

  const totalDuration = tasks.reduce((sum, t) => sum + (t.duration || 0), 0);
  const completedCount = tasks.filter((t) => t.status === 'success').length;

  let cumulativeStart = 0;
  const tasksWithOffset = tasks.map((t) => {
    const offset = cumulativeStart;
    cumulativeStart += t.duration || 0;
    return { ...t, offset };
  });

  return (
    <div className="h-full flex flex-col bg-slate-950 p-4 overflow-y-auto">
      <div className="flex items-center gap-2 mb-4">
        <Clock className="w-5 h-5 text-cyan-400" />
        <h2 className="text-lg font-semibold text-white">Execution Timeline</h2>
        <span className="text-xs text-slate-500 ml-auto">
          {completedCount}/{tasks.length} tasks &middot; {totalDuration.toFixed(1)}s total
        </span>
      </div>

      <div className="flex gap-4 mb-4">
        {['success', 'running', 'failed', 'pending'].map((s) => (
          <div key={s} className="flex items-center gap-1.5">
            <div className={`w-3 h-3 rounded ${statusBarColors[s]}`} />
            <span className="text-[10px] text-slate-400 capitalize">{s}</span>
          </div>
        ))}
      </div>

      <div className="space-y-1.5 flex-1">
        {tasksWithOffset.map((task) => {
          const widthPct =
            task.duration && maxDuration > 0
              ? Math.max((task.duration / maxDuration) * 100, 4)
              : 4;

          return (
            <div key={task.id} className="flex items-center gap-3 group">
              <div className="w-40 shrink-0 text-right">
                <span className="text-xs text-slate-400 truncate block">{task.name}</span>
                <span className="text-[10px] text-slate-600">{task.phaseName}</span>
              </div>

              <div className="flex-1 h-7 bg-slate-900 rounded-md overflow-hidden relative">
                <div
                  className={`h-full ${statusBarColors[task.status]} rounded-md transition-all duration-500 flex items-center px-2`}
                  style={{ width: `${widthPct}%` }}
                >
                  {task.duration !== null && (
                    <span className="text-[10px] text-white font-medium whitespace-nowrap">
                      {task.duration.toFixed(1)}s
                    </span>
                  )}
                  {task.status === 'running' && (
                    <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/10 to-transparent animate-pulse" />
                  )}
                </div>
              </div>

              <div className="w-6 shrink-0">
                {task.status === 'success' && <CheckCircle className="w-4 h-4 text-green-400" />}
                {task.status === 'failed' && <XCircle className="w-4 h-4 text-red-400" />}
                {task.status === 'running' && <Loader2 className="w-4 h-4 text-cyan-400 animate-spin" />}
              </div>
            </div>
          );
        })}
      </div>

      {tasks.every((t) => t.status === 'pending') && (
        <div className="flex-1 flex items-center justify-center">
          <p className="text-sm text-slate-500">Execute a scenario to see the timeline</p>
        </div>
      )}
    </div>
  );
}
