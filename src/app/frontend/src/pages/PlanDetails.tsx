import React from 'react';
import { useParams } from 'react-router-dom';
import {
  Box,
  Grid,
  Paper,
  Typography,
  CircularProgress,
  Alert,
  Chip,
  List,
  ListItem,
  ListItemText,
  Divider,
} from '@mui/material';

import { useExecutionPlan } from '../hooks/useExecutionPlan';
import PlanViewer from '../components/PlanViewer';
import ApprovalDialog from '../components/ApprovalDialog';

const statusColors: Record<string, string> = {
  running: '#29B5E8',
  completed: '#4CAF50',
  failed: '#f44336',
  pending: '#9e9e9e',
  retrying: '#FFC107',
};

const PlanDetails: React.FC = () => {
  const { planId } = useParams<{ planId: string }>();
  const { plan, phases, artifacts, loading, error, refresh } = useExecutionPlan(planId);
  const [approvalOpen, setApprovalOpen] = React.useState(false);

  if (loading && !plan) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', mt: 4 }}>
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return <Alert severity="error">{error}</Alert>;
  }

  if (!plan) {
    return <Alert severity="warning">Plan not found</Alert>;
  }

  const needsApproval = plan.status === 'pending';

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Box>
          <Typography variant="h4">{plan.use_case_summary}</Typography>
          <Typography variant="body2" color="text.secondary">
            Plan ID: {plan.id}
          </Typography>
        </Box>
        <Chip
          label={plan.status.toUpperCase()}
          sx={{ backgroundColor: statusColors[plan.status], color: 'white', fontWeight: 'bold' }}
        />
      </Box>

      {needsApproval && (
        <Alert
          severity="info"
          sx={{ mb: 3 }}
          action={
            <Chip
              label="Review & Approve"
              onClick={() => setApprovalOpen(true)}
              sx={{ cursor: 'pointer' }}
            />
          }
        >
          This execution plan requires your approval before proceeding.
        </Alert>
      )}

      <Grid container spacing={3}>
        <Grid item xs={12} lg={8}>
          <Paper sx={{ p: 2, mb: 3 }}>
            <Typography variant="h6" gutterBottom>
              Execution DAG
            </Typography>
            <Box sx={{ height: 400 }}>
              <PlanViewer phases={phases} />
            </Box>
          </Paper>

          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Phase Status
            </Typography>
            <List>
              {phases.map((phase, index) => (
                <React.Fragment key={phase.phase_id}>
                  {index > 0 && <Divider />}
                  <ListItem>
                    <ListItemText
                      primary={
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          {phase.phase_name}
                          <Chip
                            label={phase.status}
                            size="small"
                            sx={{ backgroundColor: statusColors[phase.status] }}
                          />
                          {phase.parallel && <Chip label="Parallel" size="small" variant="outlined" />}
                        </Box>
                      }
                      secondary={
                        <>
                          Agents: {Array.isArray(phase.agents) ? phase.agents.map(a => typeof a === 'string' ? a : a.agent).join(', ') : 'N/A'}
                          {phase.error_message && (
                            <Typography variant="body2" color="error">
                              Error: {phase.error_message}
                            </Typography>
                          )}
                          {phase.retry_count && phase.retry_count > 0 && (
                            <Typography variant="body2" color="warning.main">
                              Retries: {phase.retry_count}
                            </Typography>
                          )}
                        </>
                      }
                    />
                  </ListItem>
                </React.Fragment>
              ))}
            </List>
          </Paper>
        </Grid>

        <Grid item xs={12} lg={4}>
          <Paper sx={{ p: 2, mb: 3 }}>
            <Typography variant="h6" gutterBottom>
              Details
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Domain
            </Typography>
            <Typography variant="body1" gutterBottom>
              {plan.detected_domain}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Created
            </Typography>
            <Typography variant="body1" gutterBottom>
              {new Date(plan.created_at).toLocaleString()}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Total Phases
            </Typography>
            <Typography variant="body1">{plan.phases.length}</Typography>
          </Paper>

          <Paper sx={{ p: 2 }}>
            <Typography variant="h6" gutterBottom>
              Artifacts ({artifacts.length})
            </Typography>
            {artifacts.length === 0 ? (
              <Typography variant="body2" color="text.secondary">
                No artifacts yet
              </Typography>
            ) : (
              <List dense>
                {artifacts.map((artifact) => (
                  <ListItem key={artifact.artifact_id}>
                    <ListItemText
                      primary={artifact.artifact_name}
                      secondary={`${artifact.artifact_type} • ${artifact.artifact_location}`}
                    />
                  </ListItem>
                ))}
              </List>
            )}
          </Paper>
        </Grid>
      </Grid>

      <ApprovalDialog
        open={approvalOpen}
        onClose={() => setApprovalOpen(false)}
        plan={plan}
        onApprove={() => {
          setApprovalOpen(false);
          refresh();
        }}
      />
    </Box>
  );
};

export default PlanDetails;
