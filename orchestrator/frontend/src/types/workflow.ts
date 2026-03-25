export interface TaskLog {
  timestamp: string;
  taskId: string;
  taskName?: string;
  skillName?: string;
  level: 'info' | 'success' | 'error' | 'warning';
  message: string;
}

export interface Task {
  id: string;
  name: string;
  description: string;
  status: 'pending' | 'running' | 'success' | 'failed' | 'skipped';
  progress: number;
  duration: number | null;
  error: string | null;
  dependencies: string[];
  logs: TaskLog[];
  artifacts: Record<string, unknown>;
  skill_name: string | null;
  skill_type: string | null;
  preflight_status: string | null;
  governance: Record<string, unknown> | null;
}

export interface Phase {
  id: string;
  name: string;
  description: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  tasks: Task[];
}

export interface WorkflowState {
  phases: Phase[];
  is_running: boolean;
  start_time: string | null;
  end_time: string | null;
  config: Record<string, unknown>;
  plan_id: string | null;
  session_id: string | null;
  detected_domain: string | null;
}

export interface WebSocketMessage {
  type: string;
  payload: Record<string, unknown>;
  timestamp?: string;
}
