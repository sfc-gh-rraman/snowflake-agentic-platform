import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  Paper,
  Typography,
  TextField,
  Button,
  Alert,
  CircularProgress,
  Chip,
  Stack,
} from '@mui/material';
import SendIcon from '@mui/icons-material/Send';

import api from '../services/api';

const exampleUseCases = [
  "I have sensor data and daily reports. Need to predict equipment failures and search historical incidents.",
  "Analyze customer transactions to detect fraud patterns and build a risk scoring model.",
  "Process support tickets and build a knowledge base for automated FAQ responses.",
];

const NewPlan: React.FC = () => {
  const navigate = useNavigate();
  const [useCase, setUseCase] = useState('');
  const [dataPaths, setDataPaths] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!useCase.trim()) {
      setError('Please enter a use case description');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const paths = dataPaths
        .split('\n')
        .map((p) => p.trim())
        .filter((p) => p.length > 0);
      
      const plan = await api.createExecutionPlan(useCase, paths);
      navigate(`/plan/${plan.id}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create execution plan');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        New Execution
      </Typography>
      <Typography variant="body1" color="text.secondary" sx={{ mb: 3 }}>
        Describe your use case and the Meta-Agent will generate an execution plan.
      </Typography>

      <Paper sx={{ p: 3, mb: 3 }}>
        <form onSubmit={handleSubmit}>
          <Typography variant="subtitle1" gutterBottom>
            Use Case Description
          </Typography>
          <TextField
            fullWidth
            multiline
            rows={4}
            placeholder="Describe what you want to accomplish with your data..."
            value={useCase}
            onChange={(e) => setUseCase(e.target.value)}
            sx={{ mb: 3 }}
          />

          <Typography variant="subtitle1" gutterBottom>
            Data Paths (one per line)
          </Typography>
          <TextField
            fullWidth
            multiline
            rows={3}
            placeholder="@RAW.DATA_STAGE/sensor_readings.parquet&#10;@RAW.DOCUMENTS_STAGE/reports/*.pdf"
            value={dataPaths}
            onChange={(e) => setDataPaths(e.target.value)}
            sx={{ mb: 3 }}
          />

          {error && (
            <Alert severity="error" sx={{ mb: 2 }}>
              {error}
            </Alert>
          )}

          <Button
            type="submit"
            variant="contained"
            size="large"
            disabled={loading}
            endIcon={loading ? <CircularProgress size={20} /> : <SendIcon />}
          >
            {loading ? 'Generating Plan...' : 'Generate Execution Plan'}
          </Button>
        </form>
      </Paper>

      <Paper sx={{ p: 3 }}>
        <Typography variant="subtitle1" gutterBottom>
          Example Use Cases
        </Typography>
        <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
          {exampleUseCases.map((example, index) => (
            <Chip
              key={index}
              label={example.slice(0, 60) + '...'}
              onClick={() => setUseCase(example)}
              sx={{ cursor: 'pointer', mb: 1 }}
            />
          ))}
        </Stack>
      </Paper>
    </Box>
  );
};

export default NewPlan;
