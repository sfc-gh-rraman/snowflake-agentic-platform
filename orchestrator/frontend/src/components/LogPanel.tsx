import { useEffect, useRef } from 'react';
import { useWorkflowStore } from '../stores/workflowStore';
import { Info, CheckCircle, XCircle, AlertTriangle } from 'lucide-react';

const levelIcons = {
  info: <Info className="w-3 h-3 text-blue-400" />,
  success: <CheckCircle className="w-3 h-3 text-green-400" />,
  error: <XCircle className="w-3 h-3 text-red-400" />,
  warning: <AlertTriangle className="w-3 h-3 text-yellow-400" />,
};

const levelColors = {
  info: 'text-slate-300',
  success: 'text-green-300',
  error: 'text-red-300',
  warning: 'text-yellow-300',
};

export function LogPanel() {
  const { logs } = useWorkflowStore();
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [logs]);

  const formatTime = (timestamp: string) => {
    try {
      return new Date(timestamp).toLocaleTimeString();
    } catch {
      return '';
    }
  };

  return (
    <div className="h-full flex flex-col bg-slate-900">
      <div className="px-4 py-3 border-b border-slate-800">
        <h2 className="text-sm font-semibold text-slate-200">Execution Logs</h2>
        <p className="text-xs text-slate-500">{logs.length} entries</p>
      </div>
      
      <div
        ref={scrollRef}
        className="flex-1 overflow-y-auto p-2 space-y-1 font-mono text-xs"
      >
        {logs.length === 0 ? (
          <div className="text-slate-500 text-center py-8">
            No logs yet. Start the workflow to see execution logs.
          </div>
        ) : (
          logs.map((log, index) => (
            <div
              key={index}
              className="flex items-start gap-2 px-2 py-1 rounded hover:bg-slate-800/50"
            >
              <span className="text-slate-600 shrink-0">
                {formatTime(log.timestamp)}
              </span>
              <span className="shrink-0">{levelIcons[log.level]}</span>
              <span className="text-cyan-400 shrink-0">[{log.taskId}]</span>
              <span className={levelColors[log.level]}>{log.message}</span>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
