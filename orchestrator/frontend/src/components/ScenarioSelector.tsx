import { useEffect, useState } from 'react';
import {
  Database,
  Shield,
  FileText,
  ChevronRight,
  ChevronDown,
  Cpu,
  Brain,
  Search,
  BarChart3,
  Sparkles,
} from 'lucide-react';
import { useWorkflowStore } from '../stores/workflowStore';

const SCENARIO_ICONS: Record<string, typeof Database> = {
  clinical_data_warehouse: Database,
  drug_safety: Shield,
  clinical_docs: FileText,
};

const SCENARIO_COLORS: Record<string, string> = {
  clinical_data_warehouse: 'from-cyan-500/20 to-blue-500/20 border-cyan-500/40 hover:border-cyan-400',
  drug_safety: 'from-amber-500/20 to-red-500/20 border-amber-500/40 hover:border-amber-400',
  clinical_docs: 'from-violet-500/20 to-purple-500/20 border-violet-500/40 hover:border-violet-400',
};

const SCENARIO_ACCENT: Record<string, string> = {
  clinical_data_warehouse: 'text-cyan-400',
  drug_safety: 'text-amber-400',
  clinical_docs: 'text-violet-400',
};

const PILL_COLORS: Record<string, string> = {
  clinical_data_warehouse: 'bg-cyan-900/60 border-cyan-600 text-cyan-300 hover:bg-cyan-800/60',
  drug_safety: 'bg-amber-900/60 border-amber-600 text-amber-300 hover:bg-amber-800/60',
  clinical_docs: 'bg-violet-900/60 border-violet-600 text-violet-300 hover:bg-violet-800/60',
};

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
};

export function ScenarioSelector() {
  const { scenarios, activeScenario, isRunning, setActiveScenario, setScenarios } =
    useWorkflowStore();
  const [expanded, setExpanded] = useState(true);

  useEffect(() => {
    fetch('/api/scenarios')
      .then((res) => res.json())
      .then((data) => setScenarios(data))
      .catch(console.error);
  }, [setScenarios]);

  useEffect(() => {
    if (isRunning || activeScenario) setExpanded(false);
  }, [isRunning, activeScenario]);

  const scenarioKeys = Object.keys(scenarios);
  if (scenarioKeys.length === 0) return null;

  if (!expanded) {
    return (
      <div className="px-4 py-2 bg-slate-900/50 border-b border-slate-800 flex items-center gap-2">
        <button onClick={() => setExpanded(true)} className="p-0.5 hover:bg-slate-800 rounded">
          <ChevronRight className="w-3.5 h-3.5 text-slate-500" />
        </button>
        <Sparkles className="w-3 h-3 text-slate-500" />
        <span className="text-[10px] text-slate-500 uppercase tracking-wider mr-2">Scenarios</span>
        <div className="flex items-center gap-1.5">
          {scenarioKeys.map((key) => {
            const scenario = scenarios[key];
            const Icon = SCENARIO_ICONS[key] || Database;
            const isActive = activeScenario === key;
            const pillColor = PILL_COLORS[key] || 'bg-slate-800 border-slate-600 text-slate-300';
            return (
              <button
                key={key}
                onClick={() => !isRunning && setActiveScenario(isActive ? null : key)}
                disabled={isRunning}
                className={`
                  inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full border text-xs transition-all
                  ${isActive ? `${pillColor} ring-1 ring-white/20` : 'bg-slate-800/50 border-slate-700 text-slate-400 hover:text-slate-200'}
                  ${isRunning ? 'cursor-not-allowed opacity-50' : 'cursor-pointer'}
                `}
              >
                <Icon className="w-3 h-3" />
                <span className="font-medium">{scenario.name.replace(/^CoCo\s*/, '')}</span>
                <span className="text-[9px] opacity-60">{scenario.task_count}</span>
              </button>
            );
          })}
        </div>
      </div>
    );
  }

  return (
    <div className="px-6 py-4 bg-slate-900/50 border-b border-slate-800">
      <div className="flex items-center gap-2 mb-3">
        <button onClick={() => setExpanded(false)} className="p-0.5 hover:bg-slate-800 rounded">
          <ChevronDown className="w-3.5 h-3.5 text-slate-500" />
        </button>
        <Sparkles className="w-4 h-4 text-slate-400" />
        <span className="text-xs font-medium text-slate-400 uppercase tracking-wider">
          CoCo Healthcare Scenario Cards
        </span>
      </div>
      <div className="grid grid-cols-3 gap-4">
        {scenarioKeys.map((key) => {
          const scenario = scenarios[key];
          const Icon = SCENARIO_ICONS[key] || Database;
          const colorClass = SCENARIO_COLORS[key] || 'from-slate-500/20 to-slate-600/20 border-slate-500/40';
          const accentClass = SCENARIO_ACCENT[key] || 'text-slate-400';
          const isActive = activeScenario === key;

          return (
            <button
              key={key}
              onClick={() => !isRunning && setActiveScenario(isActive ? null : key)}
              disabled={isRunning}
              className={`
                relative p-4 rounded-xl border text-left transition-all duration-200
                bg-gradient-to-br ${colorClass}
                ${isActive ? 'ring-2 ring-white/30 scale-[1.02]' : 'opacity-80 hover:opacity-100'}
                ${isRunning ? 'cursor-not-allowed opacity-50' : 'cursor-pointer'}
              `}
            >
              <div className="flex items-start justify-between mb-2">
                <Icon className={`w-6 h-6 ${accentClass}`} />
                {isActive && (
                  <span className="text-[10px] font-bold uppercase tracking-wider px-2 py-0.5 rounded-full bg-white/10 text-white">
                    Selected
                  </span>
                )}
              </div>
              <h3 className="text-sm font-semibold text-white mb-1">{scenario.name}</h3>
              <p className="text-xs text-slate-400 mb-3 line-clamp-2">{scenario.description}</p>
              <div className="flex flex-wrap gap-1.5">
                {scenario.skills.slice(0, 4).map((skill) => {
                  const SkillIcon = SKILL_ICONS[skill] || Cpu;
                  return (
                    <span
                      key={skill}
                      className="inline-flex items-center gap-1 text-[10px] px-1.5 py-0.5 rounded bg-black/30 text-slate-300"
                    >
                      <SkillIcon className="w-2.5 h-2.5" />
                      {skill.split('-').slice(-1)[0]}
                    </span>
                  );
                })}
                {scenario.skills.length > 4 && (
                  <span className="text-[10px] text-slate-500">
                    +{scenario.skills.length - 4} more
                  </span>
                )}
              </div>
              <div className="mt-2 flex items-center gap-1 text-[10px] text-slate-500">
                <ChevronRight className="w-3 h-3" />
                {scenario.task_count} tasks
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}
