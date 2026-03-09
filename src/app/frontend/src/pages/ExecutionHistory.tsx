import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  Paper,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
  CircularProgress,
  TablePagination,
  TextField,
  InputAdornment,
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';

import { useExecutionPlans } from '../hooks/useExecutionPlan';

const statusColors: Record<string, string> = {
  running: '#29B5E8',
  completed: '#4CAF50',
  failed: '#f44336',
  pending: '#9e9e9e',
};

const ExecutionHistory: React.FC = () => {
  const navigate = useNavigate();
  const { plans, loading } = useExecutionPlans();
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [searchTerm, setSearchTerm] = useState('');

  const filteredPlans = plans.filter(
    (plan) =>
      plan.use_case_summary.toLowerCase().includes(searchTerm.toLowerCase()) ||
      plan.detected_domain.toLowerCase().includes(searchTerm.toLowerCase()) ||
      plan.id.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const paginatedPlans = filteredPlans.slice(
    page * rowsPerPage,
    page * rowsPerPage + rowsPerPage
  );

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', mt: 4 }}>
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Execution History
      </Typography>

      <Paper sx={{ p: 2, mb: 3 }}>
        <TextField
          fullWidth
          placeholder="Search by use case, domain, or ID..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <SearchIcon />
              </InputAdornment>
            ),
          }}
        />
      </Paper>

      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Plan ID</TableCell>
              <TableCell>Use Case</TableCell>
              <TableCell>Domain</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Phases</TableCell>
              <TableCell>Created</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {paginatedPlans.map((plan) => (
              <TableRow
                key={plan.id}
                hover
                onClick={() => navigate(`/plan/${plan.id}`)}
                sx={{ cursor: 'pointer' }}
              >
                <TableCell>
                  <Typography variant="body2" fontFamily="monospace">
                    {plan.id.slice(0, 8)}...
                  </Typography>
                </TableCell>
                <TableCell>
                  <Typography variant="body2" noWrap sx={{ maxWidth: 300 }}>
                    {plan.use_case_summary}
                  </Typography>
                </TableCell>
                <TableCell>
                  <Chip label={plan.detected_domain} size="small" variant="outlined" />
                </TableCell>
                <TableCell>
                  <Chip
                    label={plan.status}
                    size="small"
                    sx={{ backgroundColor: statusColors[plan.status], color: 'white' }}
                  />
                </TableCell>
                <TableCell>{plan.phases.length}</TableCell>
                <TableCell>
                  <Typography variant="body2">
                    {new Date(plan.created_at).toLocaleString()}
                  </Typography>
                </TableCell>
              </TableRow>
            ))}
            {paginatedPlans.length === 0 && (
              <TableRow>
                <TableCell colSpan={6} align="center">
                  <Typography color="text.secondary">No executions found</Typography>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
        <TablePagination
          rowsPerPageOptions={[5, 10, 25]}
          component="div"
          count={filteredPlans.length}
          rowsPerPage={rowsPerPage}
          page={page}
          onPageChange={(_, newPage) => setPage(newPage)}
          onRowsPerPageChange={(e) => {
            setRowsPerPage(parseInt(e.target.value, 10));
            setPage(0);
          }}
        />
      </TableContainer>
    </Box>
  );
};

export default ExecutionHistory;
