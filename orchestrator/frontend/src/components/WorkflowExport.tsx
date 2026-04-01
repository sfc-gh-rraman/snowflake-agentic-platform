import { useState } from 'react';
import { useWorkflowStore } from '../stores/workflowStore';
import { Download, Check, FileJson, FileText, ClipboardCopy } from 'lucide-react';

export function WorkflowExport() {
  const { phases, activeScenario, scenarios, logs } = useWorkflowStore();
  const [copied, setCopied] = useState(false);
  const [format, setFormat] = useState<'json' | 'yaml'>('json');

  const generateTemplate = () => {
    const allTasks = phases.flatMap((p) => p.tasks);
    const template = {
      template_version: '1.0',
      generated_at: new Date().toISOString(),
      scenario: activeScenario,
      scenario_name: activeScenario ? scenarios[activeScenario]?.name : 'Custom',
      database: 'AGENTIC_PLATFORM',
      phases: phases.map((p) => ({
        id: p.id,
        name: p.name,
        status: p.status,
        tasks: p.tasks.map((t) => ({
          id: t.id,
          name: t.name,
          skill_name: t.skill_name,
          skill_type: t.skill_type,
          status: t.status,
          duration: t.duration,
          dependencies: t.dependencies,
          artifacts: t.artifacts,
        })),
      })),
      summary: {
        total_tasks: allTasks.length,
        completed: allTasks.filter((t) => t.status === 'success').length,
        failed: allTasks.filter((t) => t.status === 'failed').length,
        total_duration: allTasks.reduce((s, t) => s + (t.duration || 0), 0),
        total_logs: logs.length,
      },
    };
    return template;
  };

  const toYaml = (obj: unknown, indent = 0): string => {
    const pad = '  '.repeat(indent);
    if (obj === null || obj === undefined) return `${pad}null\n`;
    if (typeof obj === 'string') return `${pad}"${obj}"\n`;
    if (typeof obj === 'number' || typeof obj === 'boolean') return `${pad}${obj}\n`;
    if (Array.isArray(obj)) {
      if (obj.length === 0) return `${pad}[]\n`;
      return obj.map((item) => {
        if (typeof item === 'object' && item !== null) {
          const lines = toYaml(item, indent + 1).split('\n').filter(Boolean);
          return `${pad}- ${lines[0].trim()}\n${lines.slice(1).map((l) => `${pad}  ${l.trim()}\n`).join('')}`;
        }
        return `${pad}- ${item}\n`;
      }).join('');
    }
    if (typeof obj === 'object') {
      return Object.entries(obj as Record<string, unknown>)
        .map(([key, val]) => {
          if (typeof val === 'object' && val !== null) {
            return `${pad}${key}:\n${toYaml(val, indent + 1)}`;
          }
          return `${pad}${key}: ${val === null ? 'null' : typeof val === 'string' ? `"${val}"` : val}\n`;
        })
        .join('');
    }
    return '';
  };

  const getContent = () => {
    const template = generateTemplate();
    if (format === 'json') return JSON.stringify(template, null, 2);
    return toYaml(template);
  };

  const handleCopy = () => {
    navigator.clipboard.writeText(getContent());
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleDownload = () => {
    const content = getContent();
    const blob = new Blob([content], { type: format === 'json' ? 'application/json' : 'text/yaml' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `workflow-${activeScenario || 'template'}-${Date.now()}.${format}`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const hasData = phases.length > 0;

  return (
    <div className="p-4 space-y-3">
      <div className="flex items-center gap-2">
        <Download className="w-4 h-4 text-cyan-400" />
        <span className="text-sm font-semibold text-white">Export Workflow Template</span>
      </div>

      <div className="flex gap-2">
        <button
          onClick={() => setFormat('json')}
          className={`flex items-center gap-1.5 px-3 py-1.5 rounded text-xs transition-colors ${
            format === 'json' ? 'bg-cyan-900/50 text-cyan-300 border border-cyan-700' : 'bg-slate-800 text-slate-400 border border-slate-700'
          }`}
        >
          <FileJson className="w-3 h-3" />
          JSON
        </button>
        <button
          onClick={() => setFormat('yaml')}
          className={`flex items-center gap-1.5 px-3 py-1.5 rounded text-xs transition-colors ${
            format === 'yaml' ? 'bg-cyan-900/50 text-cyan-300 border border-cyan-700' : 'bg-slate-800 text-slate-400 border border-slate-700'
          }`}
        >
          <FileText className="w-3 h-3" />
          YAML
        </button>
      </div>

      {hasData ? (
        <>
          <div className="bg-slate-800 rounded-lg p-3 max-h-[300px] overflow-y-auto">
            <pre className="text-[10px] text-slate-300 font-mono whitespace-pre-wrap">
              {getContent().slice(0, 3000)}
              {getContent().length > 3000 && '\n... (truncated in preview)'}
            </pre>
          </div>

          <div className="flex gap-2">
            <button
              onClick={handleCopy}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-slate-700 hover:bg-slate-600 text-white text-xs rounded-lg"
            >
              {copied ? (
                <Check className="w-3 h-3 text-green-400" />
              ) : (
                <ClipboardCopy className="w-3 h-3" />
              )}
              {copied ? 'Copied!' : 'Copy'}
            </button>
            <button
              onClick={handleDownload}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-cyan-600 hover:bg-cyan-500 text-white text-xs rounded-lg"
            >
              <Download className="w-3 h-3" />
              Download .{format}
            </button>
          </div>
        </>
      ) : (
        <p className="text-xs text-slate-500 text-center py-4">
          Run a scenario to generate an exportable template
        </p>
      )}
    </div>
  );
}
