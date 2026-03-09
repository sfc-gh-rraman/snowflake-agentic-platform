import React, { useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Box,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Divider,
  Alert,
  CircularProgress,
  Chip,
} from '@mui/material';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import StorageIcon from '@mui/icons-material/Storage';
import PsychologyIcon from '@mui/icons-material/Psychology';
import SearchIcon from '@mui/icons-material/Search';
import CodeIcon from '@mui/icons-material/Code';

import type { ExecutionPlan } from '../types';
import api from '../services/api';

interface ApprovalDialogProps {
  open: boolean;
  onClose: () => void;
  plan: ExecutionPlan;
  onApprove: () => void;
}

const agentIcons: Record<string, React.ReactNode> = {
  parquet_processor: <StorageIcon />,
  document_chunker: <StorageIcon />,
  model_builder: <PsychologyIcon />,
  search_service_creator: <SearchIcon />,
  semantic_model_generator: <CodeIcon />,
  app_code_generator: <CodeIcon />,
};

const ApprovalDialog: React.FC<ApprovalDialogProps> = ({
  open,
  onClose,
  plan,
  onApprove,
}) => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleApprove = async () => {
    setLoading(true);
    setError(null);
    try {
      await api.approvePlan(plan.id, true);
      await api.executePlan(plan.id);
      onApprove();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to approve plan');
    } finally {
      setLoading(false);
    }
  };

  const handleReject = async () => {
    setLoading(true);
    setError(null);
    try {
      await api.approvePlan(plan.id, false);
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to reject plan');
    } finally {
      setLoading(false);
    }
  };

  const allAgents = new Set<string>();
  plan.phases.forEach((phase) => {
    phase.agents.forEach((agent) => {
      const agentName = typeof agent === 'string' ? agent : agent.agent;
      allAgents.add(agentName);
    });
  });

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <CheckCircleIcon color="primary" />
          Review Execution Plan
        </Box>
      </DialogTitle>
      
      <DialogContent dividers>
        <Typography variant="h6" gutterBottom>
          Use Case
        </Typography>
        <Typography variant="body1" sx={{ mb: 3 }}>
          {plan.use_case_summary}
        </Typography>

        <Divider sx={{ my: 2 }} />

        <Typography variant="h6" gutterBottom>
          Detected Domain
        </Typography>
        <Chip label={plan.detected_domain} sx={{ mb: 3 }} />

        <Divider sx={{ my: 2 }} />

        <Typography variant="h6" gutterBottom>
          Execution Phases ({plan.phases.length})
        </Typography>
        <List dense>
          {plan.phases.map((phase, index) => (
            <ListItem key={phase.phase_id}>
              <ListItemIcon>
                <Box
                  sx={{
                    width: 24,
                    height: 24,
                    borderRadius: '50%',
                    backgroundColor: 'primary.main',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    fontSize: 12,
                    fontWeight: 'bold',
                  }}
                >
                  {index + 1}
                </Box>
              </ListItemIcon>
              <ListItemText
                primary={
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    {phase.phase_name}
                    {phase.parallel && (
                      <Chip label="Parallel" size="small" variant="outlined" />
                    )}
                    {phase.checkpoint && (
                      <Chip label="Checkpoint" size="small" color="secondary" />
                    )}
                  </Box>
                }
                secondary={
                  <>
                    Agents:{' '}
                    {phase.agents
                      .map((a) => (typeof a === 'string' ? a : a.agent))
                      .join(', ')}
                  </>
                }
              />
            </ListItem>
          ))}
        </List>

        <Divider sx={{ my: 2 }} />

        <Typography variant="h6" gutterBottom>
          Agents Involved
        </Typography>
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
          {Array.from(allAgents).map((agent) => (
            <Chip
              key={agent}
              icon={agentIcons[agent] || <CodeIcon />}
              label={agent.replace(/_/g, ' ')}
              variant="outlined"
            />
          ))}
        </Box>

        <Alert severity="info" sx={{ mt: 3 }}>
          By approving this plan, the system will execute all phases sequentially.
          You can monitor progress from the plan details page.
        </Alert>

        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}
      </DialogContent>

      <DialogActions>
        <Button onClick={onClose} disabled={loading}>
          Cancel
        </Button>
        <Button
          onClick={handleReject}
          color="error"
          disabled={loading}
        >
          Reject
        </Button>
        <Button
          onClick={handleApprove}
          variant="contained"
          disabled={loading}
          startIcon={loading ? <CircularProgress size={16} /> : <CheckCircleIcon />}
        >
          {loading ? 'Processing...' : 'Approve & Execute'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default ApprovalDialog;
