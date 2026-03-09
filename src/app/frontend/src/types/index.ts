export interface ExecutionPlan {
  id: string;
  use_case_summary: string;
  detected_domain: string;
  phases: Phase[];
  status: 'pending' | 'running' | 'completed' | 'failed';
  created_at: string;
  updated_at?: string;
}

export interface Phase {
  phase_id: string;
  phase_name: string;
  agents: string[] | AgentConfig[];
  parallel: boolean;
  checkpoint: boolean;
  depends_on?: string[];
  status: 'pending' | 'running' | 'completed' | 'failed' | 'retrying';
  retry_count?: number;
  started_at?: string;
  completed_at?: string;
  error_message?: string;
}

export interface AgentConfig {
  agent: string;
  input?: string;
  config?: Record<string, unknown>;
}

export interface Artifact {
  artifact_id: string;
  artifact_type: string;
  artifact_name: string;
  artifact_location: string;
  created_at: string;
}

export interface CortexCallLog {
  call_id: string;
  call_type: string;
  model: string;
  input_tokens: number;
  output_tokens: number;
  latency_ms: number;
  created_at: string;
}

export interface DashboardMetrics {
  active_executions: number;
  completed_24h: number;
  failed_24h: number;
  total_tokens_24h: number;
  artifacts_created_24h: number;
  phase_failures_24h: number;
}

export interface ApprovalRequest {
  plan_id: string;
  execution_plan: ExecutionPlan;
  requires_approval: boolean;
  approval_status: 'pending' | 'approved' | 'rejected';
}
