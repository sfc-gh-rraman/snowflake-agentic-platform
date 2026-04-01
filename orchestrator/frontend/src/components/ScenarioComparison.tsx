import { useWorkflowStore } from '../stores/workflowStore';
import { ComparisonResult } from '../types/workflow';
import {
  PlayCircle,
  Loader2,
  CheckCircle,
  XCircle,
  Clock,
  GitCompare,
} from 'lucide-react';

export function ScenarioComparison() {
  const {
    scenarios,
    comparisonResults,
    comparisonRunning,
    setComparisonResults,
    setComparisonRunning,
    isRunning,
  } = useWorkflowStore();

  const runAllScenarios = async () => {
    if (comparisonRunning || isRunning) return;
    setComparisonRunning(true);

    const keys = Object.keys(scenarios);
    const results: ComparisonResult[] = keys.map((key) => ({
      scenario: key,
      scenario_name: scenarios[key].name,
      status: 'pending',
      task_count: scenarios[key].task_count,
      completed_tasks: 0,
      total_duration: null,
      artifacts_created: 0,
    }));
    setComparisonResults(results);

    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      setComparisonResults(
        results.map((r, idx) =>
          idx === i ? { ...r, status: 'running' } : r
        )
      );

      try {
        const startTime = Date.now();
        const response = await fetch('/api/workflow/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            user_request: `Execute ${scenarios[key].name} pipeline`,
            database: 'AGENTIC_PLATFORM',
            fhir_schema: 'FHIR_DEMO',
            scenario: key,
          }),
        });

        if (!response.ok) throw new Error('Failed to start');

        let done = false;
        while (!done) {
          await new Promise((r) => setTimeout(r, 2000));
          const statusRes = await fetch('/api/workflow');
          const statusData = await statusRes.json();
          if (!statusData.is_running) {
            done = true;
            const elapsed = (Date.now() - startTime) / 1000;
            const allTasks = (statusData.phases || []).flatMap(
              (p: { tasks: { status: string; artifacts: Record<string, unknown> }[] }) => p.tasks
            );
            const completed = allTasks.filter(
              (t: { status: string }) => t.status === 'success'
            ).length;
            const artifactCount = allTasks.reduce(
              (sum: number, t: { artifacts: Record<string, unknown> }) =>
                sum + Object.keys(t.artifacts || {}).length,
              0
            );
            const hasFailed = allTasks.some(
              (t: { status: string }) => t.status === 'failed'
            );
            results[i] = {
              ...results[i],
              status: hasFailed ? 'failed' : 'completed',
              completed_tasks: completed,
              total_duration: elapsed,
              artifacts_created: artifactCount,
            };
            setComparisonResults([...results]);
          }
        }

        if (i < keys.length - 1) {
          await fetch('/api/workflow/reset', { method: 'POST' });
          await new Promise((r) => setTimeout(r, 1000));
        }
      } catch {
        results[i] = { ...results[i], status: 'failed' };
        setComparisonResults([...results]);
      }
    }

    setComparisonRunning(false);
  };

  const statusIcon = (s: string) => {
    switch (s) {
      case 'completed':
        return <CheckCircle className="w-4 h-4 text-green-400" />;
      case 'failed':
        return <XCircle className="w-4 h-4 text-red-400" />;
      case 'running':
        return <Loader2 className="w-4 h-4 text-cyan-400 animate-spin" />;
      default:
        return <Clock className="w-4 h-4 text-slate-500" />;
    }
  };

  return (
    <div className="p-4">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <GitCompare className="w-4 h-4 text-cyan-400" />
          <span className="text-sm font-semibold text-white">Scenario Comparison</span>
        </div>
        <button
          onClick={runAllScenarios}
          disabled={comparisonRunning || isRunning}
          className="flex items-center gap-2 px-3 py-1.5 bg-cyan-600 hover:bg-cyan-500 disabled:opacity-50 text-white text-xs rounded-lg"
        >
          {comparisonRunning ? (
            <Loader2 className="w-3 h-3 animate-spin" />
          ) : (
            <PlayCircle className="w-3 h-3" />
          )}
          Run All Scenarios
        </button>
      </div>

      {comparisonResults.length > 0 && (
        <div className="space-y-2">
          <div className="grid grid-cols-5 gap-2 text-[10px] text-slate-500 uppercase tracking-wider px-3">
            <span>Scenario</span>
            <span>Status</span>
            <span>Tasks</span>
            <span>Duration</span>
            <span>Artifacts</span>
          </div>
          {comparisonResults.map((r) => (
            <div
              key={r.scenario}
              className="grid grid-cols-5 gap-2 items-center px-3 py-2 bg-slate-800/50 rounded-lg"
            >
              <span className="text-xs text-white font-medium truncate">{r.scenario_name}</span>
              <div className="flex items-center gap-1">
                {statusIcon(r.status)}
                <span className="text-xs text-slate-300 capitalize">{r.status}</span>
              </div>
              <span className="text-xs text-slate-300">
                {r.completed_tasks}/{r.task_count}
              </span>
              <span className="text-xs text-slate-300">
                {r.total_duration ? `${r.total_duration.toFixed(1)}s` : '-'}
              </span>
              <span className="text-xs text-slate-300">{r.artifacts_created || '-'}</span>
            </div>
          ))}
        </div>
      )}

      {comparisonResults.length === 0 && (
        <p className="text-xs text-slate-500 text-center py-6">
          Click "Run All Scenarios" to compare all 3 healthcare pipelines side-by-side
        </p>
      )}
    </div>
  );
}
