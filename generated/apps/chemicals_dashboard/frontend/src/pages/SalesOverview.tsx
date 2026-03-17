import React, { useState, useEffect } from 'react';
import { Box, Container, Typography, Paper, CircularProgress } from '@mui/material';

interface SalesOverviewProps {
  title?: string;
}

export const SalesOverview: React.FC<SalesOverviewProps> = ({ title = "Sales Overview" }) => {
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch('/api/sales-overview');
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
        <Box sx={{ mb: 2 }}>
          <input type="text" placeholder="Search..." style={{ width: '100%', padding: 8 }} />
        </Box>
        <Box sx={{ height: 300 }}>
          <Typography>Chart visualization placeholder</Typography>
        </Box>
      </Paper>
    </Container>
  );
};

export default SalesOverview;
