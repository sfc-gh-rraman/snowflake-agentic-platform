import { useState } from 'react';
import { Play, RotateCcw, HeartPulse, Send, Loader2, Sparkles, Sun, Moon } from 'lucide-react';
import { useWorkflowStore } from '../stores/workflowStore';

export function Header() {
  const { isRunning, activeScenario, scenarios, nlInput, setNlInput, nlLoading, setNlLoading, setActiveScenario, theme, toggleTheme } = useWorkflowStore();
  const [showNlBar, setShowNlBar] = useState(false);

  const startWorkflow = async () => {
    try {
      const body: Record<string, string> = {
        user_request: activeScenario
          ? `Execute ${scenarios[activeScenario]?.name || activeScenario} pipeline`
          : 'Validate FHIR data quality, apply HIPAA governance, and build patient analytics view',
        database: 'AGENTIC_PLATFORM',
        fhir_schema: 'FHIR_DEMO',
      };
      if (activeScenario) {
        body.scenario = activeScenario;
      }

      const response = await fetch('/api/workflow/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
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

  const handleNlSubmit = async () => {
    if (!nlInput.trim() || nlLoading || isRunning) return;
    setNlLoading(true);

    try {
      const res = await fetch('/api/workflow/start-nl', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt: nlInput.trim() }),
      });

      if (res.ok) {
        const data = await res.json();
        if (data.scenario) {
          setActiveScenario(data.scenario);
        }
      }
    } catch (e) {
      console.error('NL routing failed:', e);
    } finally {
      setNlLoading(false);
      setNlInput('');
      setShowNlBar(false);
    }
  };

  const scenarioName = activeScenario
    ? scenarios[activeScenario]?.name
    : null;

  return (
    <header className="bg-slate-900 border-b border-slate-800 px-6">
      <div className="h-16 flex items-center justify-between">
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2">
            <HeartPulse className="w-8 h-8 text-cyan-400" />
            <div>
              <h1 className="text-xl font-bold text-white">Health Sciences Orchestrator</h1>
              <p className="text-xs text-slate-400">
                CoCo Skills Platform
                {scenarioName ? (
                  <span className="text-cyan-400"> &middot; {scenarioName}</span>
                ) : (
                  <> &middot; Select a scenario below</>
                )}
              </p>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-3">
          <button
            onClick={toggleTheme}
            className="p-2 bg-slate-800 hover:bg-slate-700 dark:bg-slate-800 dark:hover:bg-slate-700 rounded-lg transition-colors"
            title={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
          >
            {theme === 'dark' ? (
              <Sun className="w-4 h-4 text-amber-400" />
            ) : (
              <Moon className="w-4 h-4 text-slate-300" />
            )}
          </button>

          <button
            onClick={() => setShowNlBar(!showNlBar)}
            className="flex items-center gap-2 px-3 py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg transition-colors text-xs"
          >
            <Sparkles className="w-3.5 h-3.5 text-amber-400" />
            AI Route
          </button>

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
      </div>

      {showNlBar && (
        <div className="pb-3">
          <div className="flex items-center gap-2 bg-slate-800 border border-slate-700 rounded-lg px-4 py-2">
            <Sparkles className="w-4 h-4 text-amber-400 shrink-0" />
            <input
              type="text"
              value={nlInput}
              onChange={(e) => setNlInput(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleNlSubmit()}
              placeholder="Describe what you want to do... e.g. 'Analyze drug safety signals from FAERS data'"
              className="flex-1 bg-transparent text-sm text-white placeholder:text-slate-500 outline-none"
              disabled={nlLoading || isRunning}
            />
            <button
              onClick={handleNlSubmit}
              disabled={nlLoading || isRunning || !nlInput.trim()}
              className="p-1.5 hover:bg-slate-700 rounded disabled:opacity-50"
            >
              {nlLoading ? (
                <Loader2 className="w-4 h-4 text-cyan-400 animate-spin" />
              ) : (
                <Send className="w-4 h-4 text-cyan-400" />
              )}
            </button>
          </div>
          <div className="flex gap-2 mt-2">
            {[
              { label: 'Build clinical data warehouse with FHIR', scenario: 'clinical_data_warehouse' },
              { label: 'Run drug safety signal detection', scenario: 'drug_safety' },
              { label: 'Process clinical documents with AI', scenario: 'clinical_docs' },
            ].map((hint) => (
              <button
                key={hint.scenario}
                onClick={() => {
                  setNlInput(hint.label);
                  setActiveScenario(hint.scenario);
                  setShowNlBar(false);
                }}
                className="text-[10px] px-2 py-1 bg-slate-800/50 border border-slate-700/50 rounded text-slate-400 hover:text-white hover:border-slate-600 transition-colors"
              >
                {hint.label}
              </button>
            ))}
          </div>
        </div>
      )}
    </header>
  );
}
