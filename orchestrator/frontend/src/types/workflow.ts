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
  active_scenario: string | null;
}

export interface Scenario {
  name: string;
  description: string;
  skills: string[];
  task_count: number;
}

export interface WebSocketMessage {
  type: string;
  payload: Record<string, unknown>;
  timestamp?: string;
}

export interface SnowflakeArtifact {
  schema: string;
  name: string;
  type: string;
  row_count: number | null;
  bytes: number | null;
  created_on: string | null;
  task_id: string | null;
}

export interface LineageNode {
  id: string;
  label: string;
  type: 'raw' | 'enriched' | 'ml' | 'search' | 'analyst' | 'app';
  schema: string;
}

export interface LineageEdge {
  source: string;
  target: string;
  label?: string;
}

export interface CostEntry {
  task_id: string;
  task_name: string;
  credits: number;
  query_count: number;
  duration_ms: number;
}

export interface ChatMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
  source?: string;
}

export interface ComparisonResult {
  scenario: string;
  scenario_name: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  task_count: number;
  completed_tasks: number;
  total_duration: number | null;
  artifacts_created: number;
}

export interface TaskDiff {
  task_id: string;
  task_name: string;
  before: Record<string, number>;
  after: Record<string, number>;
}

export interface ObservabilityMetric {
  timestamp: string;
  task_id: string;
  metric_type: string;
  value: number;
}

export interface PendingGateTask {
  id: string;
  name: string;
  description: string;
  skill_name: string | null;
  skill_type: string | null;
  dependencies: string[];
  enabled: boolean;
}

export type ActiveTab = 'workflow' | 'timeline' | 'lineage' | 'observability';
export type RightPanelTab = 'logs' | 'artifacts' | 'costs' | 'chat' | 'diff' | 'export' | 'skills';
