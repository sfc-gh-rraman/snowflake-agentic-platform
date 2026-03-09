import { useState, useEffect, useCallback } from 'react';
import api from '../services/api';
import type { ExecutionPlan, Phase, DashboardMetrics, Artifact } from '../types';

export function useExecutionPlans() {
  const [plans, setPlans] = useState<ExecutionPlan[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      setLoading(true);
      const response = await api.getExecutionPlans();
      setPlans(response.plans);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch plans');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  return { plans, loading, error, refresh };
}

export function useExecutionPlan(planId: string | undefined) {
  const [plan, setPlan] = useState<ExecutionPlan | null>(null);
  const [phases, setPhases] = useState<Phase[]>([]);
  const [artifacts, setArtifacts] = useState<Artifact[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    if (!planId) return;
    
    try {
      setLoading(true);
      const [planData, phasesData, artifactsData] = await Promise.all([
        api.getExecutionPlan(planId),
        api.getPhaseStatus(planId),
        api.getArtifacts(planId),
      ]);
      setPlan(planData);
      setPhases(phasesData.phases);
      setArtifacts(artifactsData.artifacts);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch plan');
    } finally {
      setLoading(false);
    }
  }, [planId]);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 5000);
    return () => clearInterval(interval);
  }, [refresh]);

  return { plan, phases, artifacts, loading, error, refresh };
}

export function useDashboardMetrics() {
  const [metrics, setMetrics] = useState<DashboardMetrics | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      setLoading(true);
      const data = await api.getDashboardMetrics();
      setMetrics(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch metrics');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 10000);
    return () => clearInterval(interval);
  }, [refresh]);

  return { metrics, loading, error, refresh };
}
