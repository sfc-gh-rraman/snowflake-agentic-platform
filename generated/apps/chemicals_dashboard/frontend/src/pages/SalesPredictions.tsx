import React, { useState, useEffect } from 'react';
import { Box, Container, Typography, Paper, CircularProgress } from '@mui/material';

interface SalesPredictionsProps {
  title?: string;
}

export const SalesPredictions: React.FC<SalesPredictionsProps> = ({ title = "Sales Predictions" }) => {
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch('/api/sales-predictions');
        if (!response.ok) throw new Error('Failed to fetch');
        const result = await response.json();
        setData(result.data || []);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Unknown error');
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, []);

  if (loading) return <CircularProgress />;
  if (error) return <Typography color="error">{error}</Typography>;

  return (
    <Container maxWidth="lg">
      <Paper sx={ { p: 3, mt: 2 } }>
        <Typography variant="h5" gutterBottom>{title}</Typography>
        <Box sx={{ mt: 2 }}>
          {data.map((row, idx) => (
            <Paper key={idx} sx={{ p: 1, mb: 1 }}>
              {JSON.stringify(row)}
            </Paper>
          ))}
        </Box>
        <Box sx={{ height: 300 }}>
          <Typography>Chart visualization placeholder</Typography>
        </Box>
      </Paper>
    </Container>
  );
};

export default SalesPredictions;
