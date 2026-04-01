import { useEffect, useRef } from 'react';
import {
  Header,
  ScenarioSelector,
  WorkflowGraph,
  LogPanel,
  LogModal,
  ArtifactExplorer,
  ScenarioComparison,
  TimelineView,
  CostTracker,
  LineageGraph,
  TaskDiffPanel,
  CortexChat,
  RoleBasedView,
  WorkflowExport,
  ObservabilityDashboard,
  PlanGateModal,
  SkillCatalog,
} from './components';
import { useWebSocket } from './hooks/useWebSocket';
import { useWorkflowStore } from './stores/workflowStore';
import { TaskLog, ActiveTab, RightPanelTab } from './types/workflow';
import {
  GitBranch,
  Clock,
  Network,
  BarChart3,
  ScrollText,
  Package,
  DollarSign,
  MessageSquare,
  GitCompare,
  Download,
  Blocks,
} from 'lucide-react';

const mainTabs: { id: ActiveTab; label: string; icon: typeof GitBranch }[] = [
  { id: 'workflow', label: 'Workflow', icon: GitBranch },
  { id: 'timeline', label: 'Timeline', icon: Clock },
  { id: 'lineage', label: 'Lineage', icon: Network },
  { id: 'observability', label: 'Observability', icon: BarChart3 },
];

const rightTabs: { id: RightPanelTab; label: string; icon: typeof ScrollText }[] = [
  { id: 'logs', label: 'Logs', icon: ScrollText },
  { id: 'artifacts', label: 'Artifacts', icon: Package },
  { id: 'costs', label: 'Costs', icon: DollarSign },
  { id: 'chat', label: 'AI Chat', icon: MessageSquare },
  { id: 'diff', label: 'Diff', icon: GitCompare },
  { id: 'export', label: 'Export', icon: Download },
  { id: 'skills', label: 'Skills', icon: Blocks },
];

function App() {
  useWebSocket();
  const eventSourceRef = useRef<EventSource | null>(null);
  const lastLogCountRef = useRef<number>(0);
  const pollFallbackRef = useRef<number | null>(null);

  useEffect(() => {
    const connectSSE = () => {
      const es = new EventSource('/api/workflow/stream');
      eventSourceRef.current = es;

      es.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
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

          if (data.active_scenario && !store.activeScenario) {
            store.setActiveScenario(data.active_scenario);
          }

          if (data.is_running && !store.isRunning) {
            store.startWorkflow();
          } else if (!data.is_running && store.isRunning) {
            store.stopWorkflow();
          }
        } catch (e) {
          console.error('[SSE] Parse error:', e);
        }
      };

      es.onerror = () => {
        es.close();
        eventSourceRef.current = null;
        if (!pollFallbackRef.current) {
          pollFallbackRef.current = window.setInterval(pollFallback, 1500);
        }
        setTimeout(connectSSE, 3000);
      };
    };

    const pollFallback = async () => {
      try {
        const res = await fetch('/api/workflow');
        if (res.ok) {
          const data = await res.json();
          const store = useWorkflowStore.getState();
          if (data.phases) store.setPhases(data.phases);
          if (data.is_running && !store.isRunning) store.startWorkflow();
          else if (!data.is_running && store.isRunning) store.stopWorkflow();
        }
      } catch {}
    };

    connectSSE();

    const approvalPoll = setInterval(async () => {
      try {
        const store = useWorkflowStore.getState();
        if (!store.isRunning || store.planGateOpen) return;
        const res = await fetch('/api/workflow/approval-status');
        if (!res.ok) return;
        const data = await res.json();
        if (data.awaiting && !store.planGateOpen) {
          store.openPlanGate(data.task_id, data.pending_tasks || []);
        }
      } catch {}
    }, 2000);

    return () => {
      if (eventSourceRef.current) eventSourceRef.current.close();
      if (pollFallbackRef.current) window.clearInterval(pollFallbackRef.current);
      clearInterval(approvalPoll);
    };
  }, []);

  const { activeTab, setActiveTab, rightPanelTab, setRightPanelTab } = useWorkflowStore();

  const renderMainContent = () => {
    switch (activeTab) {
      case 'timeline':
        return <TimelineView />;
      case 'lineage':
        return <LineageGraph />;
      case 'observability':
        return <ObservabilityDashboard />;
      default:
        return <WorkflowGraph />;
    }
  };

  const renderRightPanel = () => {
    switch (rightPanelTab) {
      case 'artifacts':
        return (
          <div className="h-full flex flex-col bg-slate-900">
            <ScenarioComparison />
            <div className="border-t border-slate-800 flex-1 overflow-y-auto">
              <RoleBasedView />
            </div>
          </div>
        );
      case 'costs':
        return (
          <div className="h-full overflow-y-auto bg-slate-900">
            <CostTracker />
          </div>
        );
      case 'chat':
        return (
          <div className="h-full bg-slate-900">
            <CortexChat />
          </div>
        );
      case 'diff':
        return (
          <div className="h-full overflow-y-auto bg-slate-900">
            <TaskDiffPanel />
          </div>
        );
      case 'export':
        return (
          <div className="h-full overflow-y-auto bg-slate-900">
            <WorkflowExport />
          </div>
        );
      case 'skills':
        return (
          <div className="h-full bg-slate-900">
            <SkillCatalog />
          </div>
        );
      default:
        return <LogPanel />;
    }
  };

  return (
    <div className="h-screen w-screen flex flex-col bg-slate-950 overflow-hidden">
      <Header />
      <ScenarioSelector />

      <div className="flex items-center gap-1 px-4 py-1.5 bg-slate-900/80 border-b border-slate-800 overflow-x-auto">
        <div className="flex items-center gap-1 flex-1">
          {mainTabs.map((tab) => {
            const Icon = tab.icon;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded text-xs transition-colors ${
                  activeTab === tab.id
                    ? 'bg-cyan-900/50 text-cyan-300 border border-cyan-800'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800'
                }`}
              >
                <Icon className="w-3 h-3" />
                {tab.label}
              </button>
            );
          })}
        </div>
        <div className="w-px h-5 bg-slate-700 mx-2" />
        <div className="flex items-center gap-1">
          {rightTabs.map((tab) => {
            const Icon = tab.icon;
            return (
              <button
                key={tab.id}
                onClick={() => setRightPanelTab(tab.id)}
                className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded text-xs transition-colors ${
                  rightPanelTab === tab.id
                    ? 'bg-slate-700 text-white'
                    : 'text-slate-500 hover:text-slate-300 hover:bg-slate-800'
                }`}
              >
                <Icon className="w-3 h-3" />
                {tab.label}
              </button>
            );
          })}
        </div>
      </div>

      <div className="flex-1 flex min-h-0">
        <div className="flex-1 border-r border-slate-800">
          {renderMainContent()}
        </div>
        <div className="w-[500px] flex-shrink-0">
          {renderRightPanel()}
        </div>
      </div>

      <ArtifactExplorer />
      <LogModal />
      <PlanGateModal />
    </div>
  );
}

export default App;
