import { useWorkflowStore } from '../stores/workflowStore';
import { X, RefreshCw } from 'lucide-react';

export function LogModal() {
  const { phases, selectedTaskId, showLogModal, setShowLogModal } = useWorkflowStore();

  if (!showLogModal || !selectedTaskId) return null;

  const task = phases
    .flatMap((p) => p.tasks)
    .find((t) => t.id === selectedTaskId);

  if (!task) return null;

  const retryTask = async () => {
    try {
      await fetch(`/api/workflow/task/${task.id}/retry`, { method: 'POST' });
      setShowLogModal(false);
    } catch (e) {
      console.error('Error retrying task:', e);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50">
      <div className="bg-slate-900 rounded-xl border border-slate-700 w-[700px] max-h-[80vh] flex flex-col">
        <div className="flex items-center justify-between px-6 py-4 border-b border-slate-700">
          <div>
            <h2 className="text-lg font-semibold text-white">{task.name}</h2>
            <p className="text-sm text-slate-400">{task.description}</p>
          </div>
          <button
            onClick={() => setShowLogModal(false)}
            className="p-2 hover:bg-slate-800 rounded-lg transition-colors"
          >
            <X className="w-5 h-5 text-slate-400" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto p-4">
          <div className="grid grid-cols-3 gap-4 mb-4">
            <div className="bg-slate-800 rounded-lg p-3">
              <span className="text-xs text-slate-500">Status</span>
              <p className="text-sm font-medium text-white capitalize">{task.status}</p>
            </div>
            <div className="bg-slate-800 rounded-lg p-3">
              <span className="text-xs text-slate-500">Progress</span>
              <p className="text-sm font-medium text-white">{task.progress}%</p>
            </div>
            <div className="bg-slate-800 rounded-lg p-3">
              <span className="text-xs text-slate-500">Duration</span>
              <p className="text-sm font-medium text-white">
                {task.duration ? `${task.duration.toFixed(1)}s` : '-'}
              </p>
            </div>
          </div>

          {task.error && (
            <div className="bg-red-900/30 border border-red-800 rounded-lg p-3 mb-4">
              <span className="text-xs text-red-400 font-medium">Error</span>
              <p className="text-sm text-red-300 mt-1">{task.error}</p>
            </div>
          )}

          <div className="bg-slate-800 rounded-lg p-3">
            <span className="text-xs text-slate-500 mb-2 block">Task Logs</span>
            <div className="space-y-1 font-mono text-xs max-h-[300px] overflow-y-auto">
              {task.logs.length === 0 ? (
                <p className="text-slate-500">No logs for this task</p>
              ) : (
                task.logs.map((log, i) => (
                  <div key={i} className="flex gap-2">
                    <span className="text-slate-600">
                      {new Date(log.timestamp).toLocaleTimeString()}
                    </span>
                    <span
                      className={
                        log.level === 'error'
                          ? 'text-red-400'
                          : log.level === 'success'
                          ? 'text-green-400'
                          : 'text-slate-300'
                      }
                    >
                      {log.message}
                    </span>
                  </div>
                ))
              )}
            </div>
          </div>

          {Object.keys(task.artifacts).length > 0 && (
            <div className="bg-slate-800 rounded-lg p-3 mt-4">
              <span className="text-xs text-slate-500 mb-2 block">Artifacts</span>
              <pre className="text-xs text-slate-300 overflow-x-auto">
                {JSON.stringify(task.artifacts, null, 2)}
              </pre>
            </div>
          )}
        </div>

        <div className="px-6 py-4 border-t border-slate-700 flex justify-end gap-3">
          {task.status === 'failed' && (
            <button
              onClick={retryTask}
              className="flex items-center gap-2 px-4 py-2 bg-cyan-600 hover:bg-cyan-500 text-white rounded-lg transition-colors"
            >
              <RefreshCw className="w-4 h-4" />
              Retry Task
            </button>
          )}
          <button
            onClick={() => setShowLogModal(false)}
            className="px-4 py-2 bg-slate-700 hover:bg-slate-600 text-white rounded-lg transition-colors"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
