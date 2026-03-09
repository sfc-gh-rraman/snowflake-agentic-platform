import { useEffect, useRef } from 'react';
import { Header, WorkflowGraph, LogPanel, LogModal } from './components';
import { useWebSocket } from './hooks/useWebSocket';
import { useWorkflowStore } from './stores/workflowStore';
import { TaskLog } from './types/workflow';

function App() {
  useWebSocket();
  const pollRef = useRef<number | null>(null);
  const lastLogCountRef = useRef<number>(0);

  useEffect(() => {
    const poll = async () => {
      try {
        const res = await fetch('/api/workflow');
        if (res.ok) {
          const data = await res.json();
          const store = useWorkflowStore.getState();

          if (data.phases) {
            store.setPhases(data.phases);

            const allLogs: TaskLog[] = [];
            for (const phase of data.phases) {
              for (const task of phase.tasks) {
                if (task.logs && task.logs.length > 0) {
                  for (const log of task.logs) {
                    allLogs.push({
                      timestamp: log.timestamp,
                      taskId: task.id,
                      level: log.level || 'info',
                      message: log.message,
                    });
                  }
                }
              }
            }

            allLogs.sort(
              (a, b) =>
                new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
            );

            if (allLogs.length < lastLogCountRef.current) {
              lastLogCountRef.current = 0;
              store.clearLogs();
            }

            if (allLogs.length > lastLogCountRef.current) {
              store.clearLogs();
              for (const log of allLogs) {
                store.addLog(log);
              }
              lastLogCountRef.current = allLogs.length;
            }
          }

          if (data.is_running && !store.isRunning) {
            store.startWorkflow();
          } else if (!data.is_running && store.isRunning) {
            store.stopWorkflow();
          }
        }
      } catch (e) {
        console.error('[POLL] Error:', e);
      }
    };

    poll();
    pollRef.current = window.setInterval(poll, 1000);

    return () => {
      if (pollRef.current) {
        window.clearInterval(pollRef.current);
      }
    };
  }, []);

  return (
    <div className="h-screen w-screen flex flex-col bg-slate-950 overflow-hidden">
      <Header />
      <div className="flex-1 flex" style={{ height: 'calc(100vh - 80px)' }}>
        <div className="flex-1 border-r border-slate-800">
          <WorkflowGraph />
        </div>
        <div className="w-[500px] flex-shrink-0">
          <LogPanel />
        </div>
      </div>
      <LogModal />
    </div>
  );
}

export default App;
