import { create } from 'zustand';
import { Phase, TaskLog } from '../types/workflow';

interface WorkflowStore {
  phases: Phase[];
  logs: TaskLog[];
  isRunning: boolean;
  deployedUrl: string | null;
  selectedTaskId: string | null;
  showLogModal: boolean;
  
  setPhases: (phases: Phase[]) => void;
  addLog: (log: TaskLog) => void;
  clearLogs: () => void;
  startWorkflow: () => void;
  stopWorkflow: () => void;
  setDeployedUrl: (url: string | null) => void;
  setSelectedTask: (taskId: string | null) => void;
  setShowLogModal: (show: boolean) => void;
  reset: () => void;
}

export const useWorkflowStore = create<WorkflowStore>((set) => ({
  phases: [],
  logs: [],
  isRunning: false,
  deployedUrl: null,
  selectedTaskId: null,
  showLogModal: false,
  
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
  
  reset: () => set({
    phases: [],
    logs: [],
    isRunning: false,
    deployedUrl: null,
    selectedTaskId: null,
    showLogModal: false,
  }),
}));
