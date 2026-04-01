import { create } from 'zustand';
import {
  Phase,
  Scenario,
  TaskLog,
  SnowflakeArtifact,
  LineageNode,
  LineageEdge,
  CostEntry,
  ChatMessage,
  ComparisonResult,
  TaskDiff,
  ActiveTab,
  RightPanelTab,
  PendingGateTask,
} from '../types/workflow';

interface WorkflowStore {
  phases: Phase[];
  logs: TaskLog[];
  isRunning: boolean;
  deployedUrl: string | null;
  selectedTaskId: string | null;
  showLogModal: boolean;
  activeScenario: string | null;
  scenarios: Record<string, Scenario>;

  activeTab: ActiveTab;
  rightPanelTab: RightPanelTab;
  artifacts: SnowflakeArtifact[];
  artifactsPanelOpen: boolean;
  lineageNodes: LineageNode[];
  lineageEdges: LineageEdge[];
  costEntries: CostEntry[];
  chatMessages: ChatMessage[];
  chatLoading: boolean;
  comparisonResults: ComparisonResult[];
  comparisonRunning: boolean;
  taskDiffs: TaskDiff[];
  nlInput: string;
  nlLoading: boolean;
  selectedRole: string;
  theme: 'dark' | 'light';
  planGateOpen: boolean;
  planGateTaskId: string | null;
  planGateTasks: PendingGateTask[];
  planGateApproving: boolean;
  observabilityData: { taskDurations: { id: string; name: string; duration: number; status: string }[]; statusCounts: Record<string, number>; logLevelCounts: Record<string, number> };

  setPhases: (phases: Phase[]) => void;
  addLog: (log: TaskLog) => void;
  clearLogs: () => void;
  startWorkflow: () => void;
  stopWorkflow: () => void;
  setDeployedUrl: (url: string | null) => void;
  setSelectedTask: (taskId: string | null) => void;
  setShowLogModal: (show: boolean) => void;
  setActiveScenario: (scenario: string | null) => void;
  setScenarios: (scenarios: Record<string, Scenario>) => void;
  reset: () => void;

  setActiveTab: (tab: ActiveTab) => void;
  setRightPanelTab: (tab: RightPanelTab) => void;
  setArtifacts: (artifacts: SnowflakeArtifact[]) => void;
  toggleArtifactsPanel: () => void;
  setLineage: (nodes: LineageNode[], edges: LineageEdge[]) => void;
  setCostEntries: (entries: CostEntry[]) => void;
  addChatMessage: (msg: ChatMessage) => void;
  setChatLoading: (loading: boolean) => void;
  clearChat: () => void;
  setComparisonResults: (results: ComparisonResult[]) => void;
  setComparisonRunning: (running: boolean) => void;
  setTaskDiffs: (diffs: TaskDiff[]) => void;
  setNlInput: (input: string) => void;
  setNlLoading: (loading: boolean) => void;
  setSelectedRole: (role: string) => void;
  toggleTheme: () => void;
  computeObservability: () => void;
  openPlanGate: (taskId: string, tasks: PendingGateTask[]) => void;
  closePlanGate: () => void;
  toggleGateTask: (taskId: string) => void;
  setPlanGateApproving: (v: boolean) => void;
  approvePlan: (skipTasks: string[]) => Promise<void>;
  rejectPlan: () => Promise<void>;
}

export const useWorkflowStore = create<WorkflowStore>((set, get) => ({
  phases: [],
  logs: [],
  isRunning: false,
  deployedUrl: null,
  selectedTaskId: null,
  showLogModal: false,
  activeScenario: null,
  scenarios: {},

  activeTab: 'workflow',
  rightPanelTab: 'logs',
  artifacts: [],
  artifactsPanelOpen: false,
  lineageNodes: [],
  lineageEdges: [],
  costEntries: [],
  chatMessages: [],
  chatLoading: false,
  comparisonResults: [],
  comparisonRunning: false,
  taskDiffs: [],
  nlInput: '',
  nlLoading: false,
  selectedRole: 'ACCOUNTADMIN',
  theme: 'dark',
  planGateOpen: false,
  planGateTaskId: null,
  planGateTasks: [],
  planGateApproving: false,
  observabilityData: { taskDurations: [], statusCounts: {}, logLevelCounts: {} },

  setPhases: (phases) => set({ phases }),

  addLog: (log) => set((state) => ({
    logs: [...state.logs, log].slice(-500)
  })),

  clearLogs: () => set({ logs: [] }),

  startWorkflow: () => set({ isRunning: true }),

  stopWorkflow: () => set({ isRunning: false }),

  setDeployedUrl: (url) => set({ deployedUrl: url }),

  setSelectedTask: (taskId) => set({ selectedTaskId: taskId }),

  setShowLogModal: (show) => set({ showLogModal: show }),

  setActiveScenario: (scenario) => set({ activeScenario: scenario }),

  setScenarios: (scenarios) => set({ scenarios }),

  reset: () => set({
    phases: [],
    logs: [],
    isRunning: false,
    deployedUrl: null,
    selectedTaskId: null,
    showLogModal: false,
    activeScenario: null,
    artifacts: [],
    lineageNodes: [],
    lineageEdges: [],
    costEntries: [],
    taskDiffs: [],
    comparisonResults: [],
    chatMessages: [],
    observabilityData: { taskDurations: [], statusCounts: {}, logLevelCounts: {} },
  }),

  setActiveTab: (tab) => set({ activeTab: tab }),
  setRightPanelTab: (tab) => set({ rightPanelTab: tab }),
  setArtifacts: (artifacts) => set({ artifacts }),
  toggleArtifactsPanel: () => set((s) => ({ artifactsPanelOpen: !s.artifactsPanelOpen })),
  setLineage: (nodes, edges) => set({ lineageNodes: nodes, lineageEdges: edges }),
  setCostEntries: (entries) => set({ costEntries: entries }),
  addChatMessage: (msg) => set((s) => ({ chatMessages: [...s.chatMessages, msg] })),
  setChatLoading: (loading) => set({ chatLoading: loading }),
  clearChat: () => set({ chatMessages: [] }),
  setComparisonResults: (results) => set({ comparisonResults: results }),
  setComparisonRunning: (running) => set({ comparisonRunning: running }),
  setTaskDiffs: (diffs) => set({ taskDiffs: diffs }),
  setNlInput: (input) => set({ nlInput: input }),
  setNlLoading: (loading) => set({ nlLoading: loading }),
  setSelectedRole: (role) => set({ selectedRole: role }),
  toggleTheme: () => set((s) => {
    const next = s.theme === 'dark' ? 'light' : 'dark';
    if (next === 'dark') {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
    return { theme: next };
  }),
  openPlanGate: (taskId, tasks) => set({ planGateOpen: true, planGateTaskId: taskId, planGateTasks: tasks, planGateApproving: false }),
  closePlanGate: () => set({ planGateOpen: false, planGateTaskId: null, planGateTasks: [], planGateApproving: false }),
  toggleGateTask: (taskId) => set((s) => ({
    planGateTasks: s.planGateTasks.map((t) => t.id === taskId ? { ...t, enabled: !t.enabled } : t),
  })),
  setPlanGateApproving: (v) => set({ planGateApproving: v }),
  approvePlan: async (skipTasks) => {
    set({ planGateApproving: true });
    try {
      await fetch('/api/workflow/approve', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ approved: true, skip_tasks: skipTasks }),
      });
      set({ planGateOpen: false, planGateTaskId: null, planGateTasks: [], planGateApproving: false });
    } catch {
      set({ planGateApproving: false });
    }
  },
  rejectPlan: async () => {
    set({ planGateApproving: true });
    try {
      await fetch('/api/workflow/approve', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ approved: false }),
      });
      set({ planGateOpen: false, planGateTaskId: null, planGateTasks: [], planGateApproving: false });
    } catch {
      set({ planGateApproving: false });
    }
  },
  computeObservability: () => {
    const { phases, logs } = get();
    const allTasks = phases.flatMap((p) => p.tasks);
    const taskDurations = allTasks
      .filter((t) => t.duration !== null)
      .map((t) => ({ id: t.id, name: t.name, duration: t.duration!, status: t.status }));
    const statusCounts: Record<string, number> = {};
    allTasks.forEach((t) => { statusCounts[t.status] = (statusCounts[t.status] || 0) + 1; });
    const logLevelCounts: Record<string, number> = {};
    logs.forEach((l) => { logLevelCounts[l.level] = (logLevelCounts[l.level] || 0) + 1; });
    set({ observabilityData: { taskDurations, statusCounts, logLevelCounts } });
  },
}));
