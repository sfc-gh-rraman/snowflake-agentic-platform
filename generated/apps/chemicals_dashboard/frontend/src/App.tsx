import React from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { ThemeProvider, createTheme, CssBaseline } from '@mui/material';
import AppBar from '@mui/material/AppBar';
import Toolbar from '@mui/material/Toolbar';
import Typography from '@mui/material/Typography';

const theme = createTheme({
  palette: {
    mode: 'light',
    primary: { main: '#1976d2' },
  },
});

const App: React.FC = () => {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <BrowserRouter>
        <AppBar position="static">
          <Toolbar>
            <Typography variant="h6">Chemical Sales Analytics Dashboard</Typography>
          </Toolbar>
        </AppBar>
        <Routes>
          <Route path="/sales-overview" element={<SalesOverview />} />
          <Route path="/sales-predictions" element={<SalesPredictions />} />
          <Route path="/market-intelligence" element={<MarketIntelligence />} />
        </Routes>
      </BrowserRouter>
    </ThemeProvider>
  );
};

export default App;
