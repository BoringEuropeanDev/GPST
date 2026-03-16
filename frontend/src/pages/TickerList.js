import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { stocksAPI } from '../utils/api';
import { formatPrice } from '../utils/format';

const TICKERS_PER_PAGE = 50;

function SearchBar({ value, onChange }) {
  return (
    <div className="search-bar">
      <svg className="search-icon" width="16" height="16" viewBox="0 0 24 24" fill="none"
        stroke="currentColor" strokeWidth="2" strokeLinecap="round">
        <circle cx="11" cy="11" r="7" />
        <path d="M21 21l-4.35-4.35" />
      </svg>
      <input
        type="text"
        placeholder="Search ticker or company..."
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="search-input"
      />
      {value && (
        <button className="search-clear" onClick={() => onChange('')}>✕</button>
      )}
    </div>
  );
}

function StatsStrip({ stocks, total, lastUpdated }) {
  const up   = stocks.filter(s => s.change_pct > 0).length;
  const down = stocks.filter(s => s.change_pct < 0).length;

  return (
    <div className="stats-strip">
      <span>{total} instruments</span>
      <span className="stat-up">{up} up</span>
      <span className="stat-down">{down} down</span>
      {lastUpdated && (
        <span style={{ marginLeft: 'auto' }}>
          Updated {lastUpdated.toLocaleTimeString()}
        </span>
      )}
    </div>
  );
}

function SortHeader({ label, sortKey, currentSort, currentDir, onSort, align }) {
  const active = currentSort === sortKey;
  return (
    <th
      className={`table-th ${align === 'right' ? 'text-right' : ''}`}
      onClick={() => onSort(sortKey)}
    >
      {label}
      {active && (
        <span className="sort-indicator">
          {currentDir === 'asc' ? '▲' : '▼'}
        </span>
      )}
    </th>
  );
}

const TickerList = () => {
  const navigate = useNavigate();
  const [stocks,      setStocks]      = useState([]);
  const [loading,     setLoading]     = useState(true);
  const [error,       setError]       = useState(null);
  const [page,        setPage]        = useState(1);
  const [totalPages,  setTotalPages]  = useState(1);
  const [total,       setTotal]       = useState(0);
  const [sortField,   setSortField]   = useState('market_cap');
  const [sortDir,     setSortDir]     = useState('desc');
  const [lastUpdated, setLastUpdated] = useState(null);
  const [search,      setSearch]      = useState('');

  const loadStocks = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await stocksAPI.list(page, TICKERS_PER_PAGE);
      setStocks(res.data.stocks || []);
      setTotalPages(res.data.total_pages || 1);
      setTotal(res.data.total || 0);
      setLastUpdated(new Date());
    } catch (e) {
      console.error('Failed to load stocks:', e);
      setError('Unable to reach the backend. Please try again in a moment.');
    }
    setLoading(false);
  }, [page]);

  useEffect(() => { loadStocks(); }, [loadStocks]);
  useEffect(() => {
    const interval = setInterval(loadStocks, 30000);
    return () => clearInterval(interval);
  }, [loadStocks]);

  const handleSort = (field) => {
    if (sortField === field) setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    else { setSortField(field); setSortDir('desc'); }
  };

  const filtered = useMemo(() => {
    let result = stocks;

    // Client-side search filter
    if (search.trim()) {
      const q = search.toLowerCase();
      result = result.filter(s =>
        s.ticker?.toLowerCase().includes(q) ||
        (s.name || '').toLowerCase().includes(q)
      );
    }

    // Sort
    result = [...result].sort((a, b) => {
      let av = a[sortField], bv = b[sortField];
      if (av == null) return 1;
      if (bv == null) return -1;
      return sortDir === 'asc' ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
    });

    return result;
  }, [stocks, search, sortField, sortDir]);

  return (
    <div className="ticker-list">
      <SearchBar value={search} onChange={setSearch} />
      <StatsStrip stocks={stocks} total={total} lastUpdated={lastUpdated} />

      {/* Error banner */}
      {error && (
        <div className="error-banner">⚠ {error}</div>
      )}

      <div className="table-wrapper">
        <table className="stock-table">
          <thead>
            <tr>
              <SortHeader label="TICKER" sortKey="ticker"
                currentSort={sortField} currentDir={sortDir} onSort={handleSort} />
              <SortHeader label="COMPANY" sortKey="name"
                currentSort={sortField} currentDir={sortDir} onSort={handleSort} />
              <SortHeader label="PRICE" sortKey="current_price"
                currentSort={sortField} currentDir={sortDir} onSort={handleSort} align="right" />
              <SortHeader label="CHANGE" sortKey="change_pct"
                currentSort={sortField} currentDir={sortDir} onSort={handleSort} align="right" />
            </tr>
          </thead>
          <tbody>
            {loading ? (
              [...Array(15)].map((_, i) => (
                <tr key={i} className="stock-row">
                  <td className="cell-ticker"><div className="skeleton" style={{ height: 14, width: 60 }} /></td>
                  <td className="cell-company"><div className="skeleton" style={{ height: 14, width: 160 }} /></td>
                  <td className="cell-price"><div className="skeleton" style={{ height: 14, width: 80 }} /></td>
                  <td className="cell-change"><div className="skeleton" style={{ height: 14, width: 70 }} /></td>
                </tr>
              ))
            ) : (
              filtered.map((stock) => {
                const change  = stock.change_pct;
                const isUp    = change > 0;
                const isDown  = change < 0;

                return (
                  <tr
                    key={stock.ticker}
                    className="stock-row"
                    onClick={() => navigate(`/stock/${stock.ticker}`)}
                  >
                    <td className="cell-ticker">
                      {stock.ticker}
                      {stock.exchange && (
                        <span className="exchange-badge">{stock.exchange}</span>
                      )}
                    </td>
                    <td className="cell-company">
                      {stock.name || '—'}
                    </td>
                    <td className="cell-price">
                      {stock.current_price
                        ? formatPrice(stock.current_price, stock.currency)
                        : '—'}
                    </td>
                    <td className="cell-change">
                      {change != null ? (
                        <span className={`change-badge ${isUp ? 'up' : isDown ? 'down' : 'flat'}`}>
                          {isUp ? '▲' : isDown ? '▼' : '•'} {Math.abs(change).toFixed(2)}%
                        </span>
                      ) : '—'}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>

        {!loading && filtered.length === 0 && (
          <div className="empty-state">
            {search ? `No instruments match "${search}"` : 'No instruments available'}
          </div>
        )}
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="pagination">
          <span className="page-info">
            Showing {(page - 1) * TICKERS_PER_PAGE + 1}–{Math.min(page * TICKERS_PER_PAGE, total)} of {total}
          </span>
          <div className="page-buttons">
            <button
              className="page-btn"
              disabled={page === 1}
              onClick={() => setPage(p => Math.max(1, p - 1))}
            >
              ← PREV
            </button>
            {[...Array(Math.min(totalPages, 5))].map((_, i) => (
              <button
                key={i + 1}
                className={`page-btn ${page === i + 1 ? 'active' : ''}`}
                onClick={() => setPage(i + 1)}
              >
                {i + 1}
              </button>
            ))}
            <button
              className="page-btn"
              disabled={page >= totalPages}
              onClick={() => setPage(p => Math.min(totalPages, p + 1))}
            >
              NEXT →
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

export default TickerList;
