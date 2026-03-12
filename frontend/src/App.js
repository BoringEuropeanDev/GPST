import React from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import './styles/global.css';
import Header from './components/Header';
import Footer from './components/Footer';
import TickerList from './pages/TickerList';
import StockDetail from './pages/StockDetail';
import MarketOverview from './components/MarketOverview';

function App() {
  return (
    <BrowserRouter>
      <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
        <Header />
        <main style={{ flex: 1 }}>
          <Routes>
            <Route path="/" element={<TickerList />} />
            <Route path="/stock/:ticker" element={<StockDetail />} />
          </Routes>
        </main>
        <Footer />
      </div>
    </BrowserRouter>
  );
}

export default App;
