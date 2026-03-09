import React from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  Grid,
  Paper,
  Typography,
  Card,
  CardContent,
  CardActionArea,
  CircularProgress,
  Chip,
} from '@mui/material';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import TokenIcon from '@mui/icons-material/Token';
import StorageIcon from '@mui/icons-material/Storage';
import WarningIcon from '@mui/icons-material/Warning';

import { useDashboardMetrics, useExecutionPlans } from '../hooks/useExecutionPlan';

const MetricCard: React.FC<{
  title: string;
  value: number | string;
  icon: React.ReactNode;
  color?: string;
}> = ({ title, value, icon, color = 'primary.main' }) => (
  <Paper sx={{ p: 2 }}>
    <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
      <Box>
        <Typography variant="body2" color="text.secondary">
          {title}
        </Typography>
        <Typography variant="h4" sx={{ color }}>
          {value}
        </Typography>
      </Box>
      <Box sx={{ color }}>{icon}</Box>
    </Box>
  </Paper>
);

const statusColors: Record<string, string> = {
  running: '#29B5E8',
  completed: '#4CAF50',
  failed: '#f44336',
  pending: '#FFC107',
};

const Dashboard: React.FC = () => {
  const navigate = useNavigate();
  const { metrics, loading: metricsLoading } = useDashboardMetrics();
  const { plans, loading: plansLoading } = useExecutionPlans();

  const activePlans = plans.filter((p) => p.status === 'running');

  if (metricsLoading || plansLoading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', mt: 4 }}>
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Dashboard
      </Typography>

      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12} sm={6} md={2}>
          <MetricCard
            title="Active Executions"
            value={metrics?.active_executions ?? 0}
            icon={<PlayArrowIcon fontSize="large" />}
            color="#29B5E8"
          />
        </Grid>
        <Grid item xs={12} sm={6} md={2}>
          <MetricCard
            title="Completed (24h)"
            value={metrics?.completed_24h ?? 0}
            icon={<CheckCircleIcon fontSize="large" />}
            color="#4CAF50"
          />
        </Grid>
        <Grid item xs={12} sm={6} md={2}>
          <MetricCard
            title="Failed (24h)"
            value={metrics?.failed_24h ?? 0}
            icon={<ErrorIcon fontSize="large" />}
            color="#f44336"
          />
        </Grid>
        <Grid item xs={12} sm={6} md={2}>
          <MetricCard
            title="Tokens Used (24h)"
            value={metrics?.total_tokens_24h?.toLocaleString() ?? 0}
            icon={<TokenIcon fontSize="large" />}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={2}>
          <MetricCard
            title="Artifacts (24h)"
            value={metrics?.artifacts_created_24h ?? 0}
            icon={<StorageIcon fontSize="large" />}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={2}>
          <MetricCard
            title="Phase Failures"
            value={metrics?.phase_failures_24h ?? 0}
            icon={<WarningIcon fontSize="large" />}
            color="#FFC107"
          />
        </Grid>
      </Grid>

      <Typography variant="h5" gutterBottom>
        Active Executions
      </Typography>

      {activePlans.length === 0 ? (
        <Paper sx={{ p: 3, textAlign: 'center' }}>
          <Typography color="text.secondary">No active executions</Typography>
        </Paper>
      ) : (
        <Grid container spacing={2}>
          {activePlans.map((plan) => (
            <Grid item xs={12} md={6} lg={4} key={plan.id}>
              <Card>
                <CardActionArea onClick={() => navigate(`/plan/${plan.id}`)}>
                  <CardContent>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 1 }}>
                      <Typography variant="subtitle2" color="text.secondary">
                        {plan.id}
                      </Typography>
                      <Chip
                        label={plan.status}
                        size="small"
                        sx={{ backgroundColor: statusColors[plan.status] }}
                      />
                    </Box>
                    <Typography variant="body1" gutterBottom>
                      {plan.use_case_summary}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Domain: {plan.detected_domain}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Phases: {plan.phases.length}
                    </Typography>
                  </CardContent>
                </CardActionArea>
              </Card>
            </Grid>
          ))}
        </Grid>
      )}
    </Box>
  );
};

export default Dashboard;
