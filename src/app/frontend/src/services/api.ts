const API_BASE = '/api';

async function fetchJson<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${url}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  });
  
  if (!response.ok) {
    throw new Error(`API error: ${response.status} ${response.statusText}`);
  }
  
  return response.json();
}

export const api = {
  getExecutionPlans: () => 
    fetchJson<{ plans: import('../types').ExecutionPlan[] }>('/plans'),
  
  getExecutionPlan: (planId: string) => 
    fetchJson<import('../types').ExecutionPlan>(`/plans/${planId}`),
  
  createExecutionPlan: (useCase: string, dataPaths: string[]) =>
    fetchJson<import('../types').ExecutionPlan>('/plans', {
      method: 'POST',
      body: JSON.stringify({ use_case: useCase, data_paths: dataPaths }),
    }),
  
  approvePlan: (planId: string, approved: boolean) =>
    fetchJson<{ status: string }>(`/plans/${planId}/approve`, {
      method: 'POST',
      body: JSON.stringify({ approved }),
    }),
  
  executePlan: (planId: string) =>
    fetchJson<{ status: string }>(`/plans/${planId}/execute`, {
      method: 'POST',
    }),
  
  getPhaseStatus: (planId: string) =>
    fetchJson<{ phases: import('../types').Phase[] }>(`/plans/${planId}/phases`),
  
  getArtifacts: (planId: string) =>
    fetchJson<{ artifacts: import('../types').Artifact[] }>(`/plans/${planId}/artifacts`),
  
  getDashboardMetrics: () =>
    fetchJson<import('../types').DashboardMetrics>('/dashboard/metrics'),
  
  getCortexLogs: (planId?: string) =>
    fetchJson<{ logs: import('../types').CortexCallLog[] }>(
      planId ? `/logs/cortex?plan_id=${planId}` : '/logs/cortex'
    ),
};

export default api;
