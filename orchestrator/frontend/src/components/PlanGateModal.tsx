import { useWorkflowStore } from '../stores/workflowStore';
import {
  Shield,
  ToggleLeft,
  ToggleRight,
  Play,
  Ban,
  Loader2,
  Database,
  Brain,
  Search,
  BarChart3,
  FileText,
  Cpu,
  Sparkles,
  Activity,
  Zap,
} from 'lucide-react';

const SKILL_ICONS: Record<string, typeof Cpu> = {
  'dynamic-tables': Database,
  'machine-learning': Brain,
  'cortex-search': Search,
  'cortex-analyst': BarChart3,
  'build-react-app': Cpu,
  'hcls-pharma-dsafety-pharmacovigilance': Shield,
  'cortex-ai-functions': Sparkles,
  'streamlit': BarChart3,
  'hcls-provider-cdata-clinical-docs': FileText,
  'cortex-agent': Brain,
  'semantic-view': BarChart3,
  'data-governance': Shield,
  'hcls-cross-validation': Activity,
  'hcls-provider-cdata-fhir': Database,
  'observability': Activity,
  'preflight-checker': Zap,
  'orchestrator': Sparkles,
};

const SKILL_TYPE_COLORS: Record<string, string> = {
  standalone: 'bg-purple-500/20 text-purple-300 border-purple-500/30',
  platform: 'bg-blue-500/20 text-blue-300 border-blue-500/30',
  infrastructure: 'bg-slate-500/20 text-slate-300 border-slate-500/30',
  routing: 'bg-amber-500/20 text-amber-300 border-amber-500/30',
};

export function PlanGateModal() {
  const {
    planGateOpen,
    planGateTasks,
    planGateApproving,
    toggleGateTask,
    approvePlan,
    rejectPlan,
  } = useWorkflowStore();

  if (!planGateOpen) return null;

  const enabledCount = planGateTasks.filter((t) => t.enabled).length;
  const totalCount = planGateTasks.length;
  const skipTasks = planGateTasks.filter((t) => !t.enabled).map((t) => t.id);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="bg-slate-900 border border-slate-700 rounded-2xl shadow-2xl w-[600px] max-h-[80vh] flex flex-col overflow-hidden">
        <div className="px-6 py-4 border-b border-slate-800 bg-gradient-to-r from-cyan-950/50 to-slate-900">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl bg-cyan-500/20 flex items-center justify-center">
              <Shield className="w-5 h-5 text-cyan-400" />
            </div>
            <div>
              <h2 className="text-lg font-semibold text-white">Execution Plan Gate</h2>
              <p className="text-xs text-slate-400">
                Review and approve the execution plan before proceeding
              </p>
            </div>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto px-6 py-4">
          <div className="flex items-center justify-between mb-4">
            <span className="text-xs text-slate-400 uppercase tracking-wider font-medium">
              Execution Steps
            </span>
            <span className="text-xs text-slate-500">
              {enabledCount}/{totalCount} enabled
            </span>
          </div>

          <div className="space-y-2">
            {planGateTasks.map((task, idx) => {
              const SkillIcon = SKILL_ICONS[task.skill_name || ''] || Cpu;
              const typeColor = SKILL_TYPE_COLORS[task.skill_type || ''] || 'bg-slate-500/20 text-slate-300 border-slate-500/30';

              return (
                <div
                  key={task.id}
                  className={`
                    relative rounded-xl border p-4 transition-all duration-200 cursor-pointer
                    ${task.enabled
                      ? 'bg-slate-800/80 border-slate-700 hover:border-cyan-700/50'
                      : 'bg-slate-900/50 border-slate-800/50 opacity-50'
                    }
                  `}
                  onClick={() => toggleGateTask(task.id)}
                >
                  <div className="flex items-start gap-3">
                    <div className="flex-shrink-0 mt-0.5">
                      <div className={`
                        w-8 h-8 rounded-lg flex items-center justify-center text-xs font-bold
                        ${task.enabled ? 'bg-cyan-500/20 text-cyan-300' : 'bg-slate-800 text-slate-600'}
                      `}>
                        {idx + 1}
                      </div>
                    </div>

                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-1">
                        <span className={`font-medium text-sm ${task.enabled ? 'text-white' : 'text-slate-500'}`}>
                          {task.name}
                        </span>
                        {task.skill_type && (
                          <span className={`text-[9px] px-1.5 py-0.5 rounded-full border ${typeColor}`}>
                            {task.skill_type}
                          </span>
                        )}
                      </div>
                      <p className={`text-xs mb-2 ${task.enabled ? 'text-slate-400' : 'text-slate-600'}`}>
                        {task.description}
                      </p>
                      {task.skill_name && (
                        <div className="flex items-center gap-1.5">
                          <SkillIcon className={`w-3 h-3 ${task.enabled ? 'text-slate-400' : 'text-slate-600'}`} />
                          <span className={`text-[10px] ${task.enabled ? 'text-slate-500' : 'text-slate-700'}`}>
                            {task.skill_name}
                          </span>
                        </div>
                      )}
                    </div>

                    <div className="flex-shrink-0">
                      {task.enabled ? (
                        <ToggleRight className="w-6 h-6 text-cyan-400" />
                      ) : (
                        <ToggleLeft className="w-6 h-6 text-slate-600" />
                      )}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        <div className="px-6 py-4 border-t border-slate-800 bg-slate-900/80">
          <div className="flex items-center justify-between">
            <button
              onClick={rejectPlan}
              disabled={planGateApproving}
              className="flex items-center gap-2 px-4 py-2 rounded-lg border border-red-800/50 bg-red-950/30 text-red-400 hover:bg-red-900/40 hover:text-red-300 transition-colors text-sm disabled:opacity-50"
            >
              <Ban className="w-4 h-4" />
              Reject
            </button>
            <div className="flex items-center gap-3">
              {skipTasks.length > 0 && (
                <span className="text-xs text-amber-400">
                  {skipTasks.length} step{skipTasks.length > 1 ? 's' : ''} will be skipped
                </span>
              )}
              <button
                onClick={() => approvePlan(skipTasks)}
                disabled={planGateApproving || enabledCount === 0}
                className="flex items-center gap-2 px-5 py-2 rounded-lg bg-gradient-to-r from-cyan-600 to-cyan-500 text-white font-medium hover:from-cyan-500 hover:to-cyan-400 transition-all text-sm disabled:opacity-50 shadow-lg shadow-cyan-500/20"
              >
                {planGateApproving ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Play className="w-4 h-4" />
                )}
                {planGateApproving ? 'Approving...' : `Execute ${enabledCount} Steps`}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
