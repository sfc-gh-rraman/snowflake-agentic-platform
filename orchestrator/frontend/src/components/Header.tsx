import { Play, RotateCcw, HeartPulse } from 'lucide-react';
import { useWorkflowStore } from '../stores/workflowStore';

export function Header() {
  const { isRunning } = useWorkflowStore();

  const startWorkflow = async () => {
    try {
      const response = await fetch('/api/workflow/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          user_request:
            'Validate FHIR data quality, apply HIPAA governance, and build patient analytics view',
          database: 'AGENTIC_PLATFORM',
          fhir_schema: 'FHIR_DEMO',
        }),
      });
      if (!response.ok) {
        console.error('Failed to start workflow');
      }
    } catch (e) {
      console.error('Error starting workflow:', e);
    }
  };

  const resetWorkflow = async () => {
    try {
      await fetch('/api/workflow/reset', { method: 'POST' });
      window.location.reload();
    } catch (e) {
      console.error('Error resetting workflow:', e);
    }
  };

  return (
    <header className="h-20 bg-slate-900 border-b border-slate-800 px-6 flex items-center justify-between">
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2">
          <HeartPulse className="w-8 h-8 text-cyan-400" />
          <div>
            <h1 className="text-xl font-bold text-white">Health Sciences Orchestrator</h1>
            <p className="text-xs text-slate-400">
              CoCo Skills Platform &middot; FHIR &middot; HIPAA Governance
            </p>
          </div>
        </div>
      </div>

      <div className="flex items-center gap-3">
        <button
          onClick={resetWorkflow}
          disabled={isRunning}
          className="flex items-center gap-2 px-4 py-2 bg-slate-700 hover:bg-slate-600 disabled:opacity-50 disabled:cursor-not-allowed text-white rounded-lg transition-colors"
        >
          <RotateCcw className="w-4 h-4" />
          Reset
        </button>

        <button
          onClick={startWorkflow}
          disabled={isRunning}
          className="flex items-center gap-2 px-4 py-2 bg-cyan-600 hover:bg-cyan-500 disabled:opacity-50 disabled:cursor-not-allowed text-white rounded-lg transition-colors"
        >
          <Play className="w-4 h-4" />
          {isRunning ? 'Running...' : 'Execute Plan'}
        </button>
      </div>
    </header>
  );
}
