import { useEffect } from 'react';
import { useWorkflowStore } from '../stores/workflowStore';
import { ArrowRight, Plus, Minus, Equal, GitCompare } from 'lucide-react';

const objectTypes = ['Dynamic Tables', 'Tables', 'Views', 'Search Services', 'ML Models', 'Documents'];

function extractCounts(artifacts: Record<string, unknown>): Record<string, number> {
  const counts: Record<string, number> = {};
  objectTypes.forEach((t) => (counts[t] = 0));

  if (artifacts.dynamic_tables && Array.isArray(artifacts.dynamic_tables)) {
    counts['Dynamic Tables'] = (artifacts.dynamic_tables as string[]).length;
  }
  if (artifacts.tables_created && Array.isArray(artifacts.tables_created)) {
    counts['Tables'] = (artifacts.tables_created as string[]).length;
  }
  if (artifacts.table_name && !artifacts.tables_created) {
    counts['Tables'] = 1;
  }
  if (artifacts.views && Array.isArray(artifacts.views)) {
    counts['Views'] = (artifacts.views as string[]).length;
  }
  if (artifacts.search_service) {
    counts['Search Services'] = 1;
  }
  if (artifacts.model_name) {
    counts['ML Models'] = 1;
  }
  if (artifacts.doc_count && typeof artifacts.doc_count === 'number') {
    counts['Documents'] = artifacts.doc_count as number;
  }

  return counts;
}

export function TaskDiffPanel() {
  const { phases, taskDiffs, setTaskDiffs } = useWorkflowStore();

  useEffect(() => {
    const allTasks = phases.flatMap((p) => p.tasks);
    const diffs = allTasks
      .filter((t) => t.status === 'success' && Object.keys(t.artifacts).length > 0)
      .map((t) => {
        const before: Record<string, number> = {};
        objectTypes.forEach((ot) => (before[ot] = 0));
        const after = extractCounts(t.artifacts);
        return {
          task_id: t.id,
          task_name: t.name,
          before,
          after,
        };
      });
    setTaskDiffs(diffs);
  }, [phases, setTaskDiffs]);

  return (
    <div className="p-4 space-y-3">
      <div className="flex items-center gap-2">
        <GitCompare className="w-4 h-4 text-cyan-400" />
        <span className="text-sm font-semibold text-white">Before / After Diff</span>
      </div>

      {taskDiffs.length === 0 ? (
        <p className="text-xs text-slate-500 text-center py-4">
          Run a scenario to see object changes
        </p>
      ) : (
        <div className="space-y-3">
          {taskDiffs.map((diff) => {
            const hasChanges = objectTypes.some(
              (ot) => diff.after[ot] > 0
            );
            if (!hasChanges) return null;

            return (
              <div key={diff.task_id} className="bg-slate-800/50 rounded-lg p-3">
                <span className="text-xs font-medium text-white block mb-2">
                  {diff.task_name}
                </span>
                <div className="space-y-1">
                  {objectTypes.map((ot) => {
                    const before = diff.before[ot] || 0;
                    const after = diff.after[ot] || 0;
                    if (before === 0 && after === 0) return null;
                    const delta = after - before;

                    return (
                      <div
                        key={ot}
                        className="flex items-center gap-2 text-xs"
                      >
                        <span className="w-28 text-slate-500">{ot}</span>
                        <span className="text-slate-400 w-6 text-right">{before}</span>
                        <ArrowRight className="w-3 h-3 text-slate-600" />
                        <span className="text-white w-6">{after}</span>
                        {delta > 0 && (
                          <span className="flex items-center gap-0.5 text-green-400">
                            <Plus className="w-3 h-3" />
                            {delta}
                          </span>
                        )}
                        {delta < 0 && (
                          <span className="flex items-center gap-0.5 text-red-400">
                            <Minus className="w-3 h-3" />
                            {Math.abs(delta)}
                          </span>
                        )}
                        {delta === 0 && after > 0 && (
                          <span className="flex items-center gap-0.5 text-slate-500">
                            <Equal className="w-3 h-3" />
                          </span>
                        )}
                      </div>
                    );
                  })}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
