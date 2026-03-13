import React, { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { stocksAPI } from '../utils/api';
import { formatPrice, formatMarketCap, formatVolume } from '../utils/format';
import MarketOverview from '../components/MarketOverview';

const TICKERS_PER_PAGE = 50;

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

  const sorted = [...stocks].sort((a, b) => {
    let av = a[sortField], bv = b[sortField];
    if (av == null) return 1;
    if (bv == null) return -1;
    return sortDir === 'asc' ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
  });

  const SortIcon = ({ field }) => {
    if (sortField !== field) return <span style={{ color: 'var(--text-muted)', marginLeft: 4 }}>⇅</span>;
    return <span style={{ color: 'var(--accent-green)', marginLeft: 4 }}>{sortDir === 'asc' ? '↑' : '↓'}</span>;
  };

  return (
    <div>
      <MarketOverview />
      <div style={styles.container}>
        <div style={styles.topBar}>
          <div style={styles.topBarLeft}>
            <div style={styles.terminalTitle}>
              <span style={{ color: 'var(--accent-green)' }}>$</span>
              <span style={{ marginLeft: 8 }}>list --global --all-exchanges</span>
              <span className="cursor" style={{ marginLeft: 4 }} />
            </div>
            <div style={styles.topBarMeta}>
              {total} instruments · {lastUpdated ? `Updated ${lastUpdated.toLocaleTimeString()}` : ''}
              <span style={{ color: 'var(--text-muted)', marginLeft: 12, fontSize: 10 }}>
                ⓘ Sort applies to current page only
              </span>
            </div>
          </div>
          <div style={styles.refreshBtn} onClick={loadStocks}>↻ REFRESH</div>
        </div>

        {/* Error banner */}
        {error && (
          <div style={{ background: 'rgba(255,71,87,0.08)', border: '1px solid rgba(255,71,87,0.3)', borderRadius: 'var(--radius)', padding: '10px 16px', marginBottom: 16, fontSize: 12, color: 'var(--accent-red)', fontFamily: 'var(--font-mono)' }}>
            ⚠ {error}
          </div>
        )}

        <div style={styles.tableWrapper}>
          <table style={styles.table}>
            <thead>
              <tr style={styles.headerRow}>
                <th style={{ ...styles.th, width: 40 }}>#</th>
                <th style={{ ...styles.th, cursor: 'pointer' }} onClick={() => handleSort('ticker')}>
                  TICKER <SortIcon field="ticker" />
                </th>
                <th style={{ ...styles.th, textAlign: 'left' }}>COMPANY</th>
                <th style={{ ...styles.th, cursor: 'pointer' }} onClick={() => handleSort('current_price')}>
                  PRICE <SortIcon field="current_price" />
                </th>
                <th style={{ ...styles.th, cursor: 'pointer' }} onClick={() => handleSort('change_pct')}>
                  CHANGE <SortIcon field="change_pct" />
                </th>
                <th style={{ ...styles.th, cursor: 'pointer' }} onClick={() => handleSort('market_cap')}>
                  MKT CAP <SortIcon field="market_cap" />
                </th>
                <th style={styles.th}>SECTOR</th>
                <th style={styles.th}>ACTION</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                [...Array(15)].map((_, i) => (
                  <tr key={i} style={styles.row}>
                    {[...Array(8)].map((_, j) => (
                      <td key={j} style={styles.td}>
                        <div className="skeleton" style={{ height: 14, width: j === 2 ? 160 : 80 }} />
                      </td>
                    ))}
                  </tr>
                ))
              ) : (
                sorted.map((stock, idx) => {
                  const change      = stock.change_pct;
                  const changeColor = change > 0 ? 'var(--accent-green)' : change < 0 ? 'var(--accent-red)' : 'var(--text-secondary)';
                  const rowNum      = (page - 1) * TICKERS_PER_PAGE + idx + 1;
                  return (
                    <tr
                      key={stock.ticker}
                      style={styles.row}
                      onMouseEnter={e => e.currentTarget.style.background = 'var(--bg-card-hover)'}
                      onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
                    >
                      <td style={{ ...styles.td, color: 'var(--text-muted)', fontSize: 11 }}>{rowNum}</td>
                      <td style={styles.td}>
                        <div style={styles.tickerCell}>
                          <span style={styles.tickerSymbol}>{stock.ticker}</span>
                          {stock.exchange && <span style={styles.exchangeBadge}>{stock.exchange}</span>}
                        </div>
                      </td>
                      <td style={{ ...styles.td, textAlign: 'left', maxWidth: 220 }}>
                        <span style={styles.companyName}>{stock.name || '—'}</span>
                      </td>
                      <td style={styles.td}>
                        <span style={styles.price}>
                          {stock.current_price
                            ? formatPrice(stock.current_price, stock.currency)
                            : <span style={{ color: 'var(--text-muted)' }}>—</span>}
                        </span>
                      </td>
                      <td style={styles.td}>
                        {change != null ? (
                          <div style={{ ...styles.changeBadge, borderColor: changeColor + '44', color: changeColor }}>
                            {change > 0 ? '▲' : change < 0 ? '▼' : '●'} {Math.abs(change).toFixed(2)}%
                          </div>
                        ) : <span style={{ color: 'var(--text-muted)' }}>—</span>}
                      </td>
                      <td style={{ ...styles.td, color: 'var(--text-secondary)' }}>
                        {formatMarketCap(stock.market_cap)}
                      </td>
                      <td style={styles.td}>
                        {stock.sector
                          ? <span style={styles.sectorTag}>{stock.sector}</span>
                          : <span style={{ color: 'var(--text-muted)' }}>—</span>}
                      </td>
                      <td style={styles.td}>
                        <button
                          style={styles.viewBtn}
                          onClick={() => navigate(`/stock/${stock.ticker}`)}
                          onMouseEnter={e => { e.target.style.background = 'var(--accent-green)'; e.target.style.color = 'var(--bg-primary)'; }}
                          onMouseLeave={e => { e.target.style.background = 'transparent'; e.target.style.color = 'var(--accent-green)'; }}
                        >
                          VIEW MORE →
                        </button>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>

        <div style={styles.pagination}>
          <span style={styles.pageInfo}>
            Showing {(page - 1) * TICKERS_PER_PAGE + 1}–{Math.min(page * TICKERS_PER_PAGE, total)} of {total}
          </span>
          <div style={styles.pageButtons}>
            <button style={styles.pageBtn} disabled={page === 1} onClick={() => setPage(p => Math.max(1, p - 1))}>← PREV</button>
            {[...Array(Math.min(totalPages, 5))].map((_, i) => (
              <button
                key={i + 1}
                style={{ ...styles.pageBtn, ...(page === i + 1 ? styles.pageBtnActive : {}) }}
                onClick={() => setPage(i + 1)}
              >{i + 1}</button>
            ))}
            <button style={styles.pageBtn} disabled={page >= totalPages} onClick={() => setPage(p => Math.min(totalPages, p + 1))}>NEXT →</button>
          </div>
        </div>

        <div style={styles.footerDisclaimer}>
          ⚠️ DISCLAIMER: All data displayed is for informational purposes only. This is not financial advice.
          Market data may be delayed. Always conduct your own research and consult a licensed financial advisor.
        </div>
      </div>
    </div>
  );
};

const styles = {
  container:       { padding: '24px', maxWidth: 1400, margin: '0 auto' },
  topBar:          { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 16 },
  topBarLeft:      {},
  terminalTitle:   { fontFamily: 'var(--font-mono)', fontSize: 14, color: 'var(--text-secondary)', marginBottom: 4 },
  topBarMeta:      { fontSize: 11, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' },
  refreshBtn:      { background: 'transparent', border: '1px solid var(--border)', color: 'var(--text-secondary)', padding: '6px 12px', borderRadius: 'var(--radius)', fontSize: 11, cursor: 'pointer', fontFamily: 'var(--font-mono)', letterSpacing: '0.1em' },
  tableWrapper:    { overflowX: 'auto', border: '1px solid var(--border)', borderRadius: 'var(--radius-lg)' },
  table:           { width: '100%', borderCollapse: 'collapse', tableLayout: 'fixed' },
  headerRow:       { background: 'var(--bg-elevated)', borderBottom: '2px solid var(--border)' },
  th:              { padding: '10px 14px', fontSize: 9, color: 'var(--text-muted)', letterSpacing: '0.15em', fontFamily: 'var(--font-mono)', textAlign: 'right', whiteSpace: 'nowrap', userSelect: 'none' },
  row:             { borderBottom: '1px solid rgba(30,42,66,0.5)', transition: 'background 0.1s' },
  td:              { padding: '10px 14px', textAlign: 'right', fontSize: 12, fontFamily: 'var(--font-mono)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' },
  tickerCell:      { display: 'flex', alignItems: 'center', gap: 6, justifyContent: 'flex-end' },
  tickerSymbol:    { color: 'var(--accent-cyan)', fontWeight: 700, fontSize: 13, letterSpacing: '0.05em' },
  exchangeBadge:   { fontSize: 8, color: 'var(--text-muted)', border: '1px solid var(--border)', borderRadius: 2, padding: '1px 3px', letterSpacing: '0.1em' },
  companyName:     { color: 'var(--text-primary)', fontSize: 12, display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 200, textAlign: 'left' },
  price:           { color: 'var(--text-primary)', fontWeight: 500, fontSize: 13 },
  changeBadge:     { display: 'inline-block', fontSize: 11, padding: '2px 8px', border: '1px solid', borderRadius: 2, fontWeight: 500 },
  sectorTag:       { fontSize: 9, color: 'var(--accent-purple)', border: '1px solid rgba(139,92,246,0.3)', borderRadius: 2, padding: '2px 6px', letterSpacing: '0.05em', display: 'inline-block', maxWidth: 120, overflow: 'hidden', textOverflow: 'ellipsis' },
  viewBtn:         { background: 'transparent', border: '1px solid var(--accent-green)', color: 'var(--accent-green)', padding: '4px 10px', borderRadius: 'var(--radius)', fontSize: 10, letterSpacing: '0.1em', cursor: 'pointer', fontFamily: 'var(--font-mono)', fontWeight: 700, whiteSpace: 'nowrap' },
  pagination:      { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 16, padding: '12px 0', borderTop: '1px solid var(--border)' },
  pageInfo:        { fontSize: 11, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' },
  pageButtons:     { display: 'flex', gap: 4 },
  pageBtn:         { background: 'transparent', border: '1px solid var(--border)', color: 'var(--text-secondary)', padding: '4px 10px', borderRadius: 'var(--radius)', fontSize: 10, cursor: 'pointer', fontFamily: 'var(--font-mono)', letterSpacing: '0.05em' },
  pageBtnActive:   { background: 'var(--accent-green)', borderColor: 'var(--accent-green)', color: 'var(--bg-primary)', fontWeight: 700 },
  footerDisclaimer:{ marginTop: 24, padding: '12px 16px', background: 'rgba(245,166,35,0.05)', border: '1px solid rgba(245,166,35,0.2)', borderRadius: 'var(--radius)', fontSize: 10, color: 'var(--accent-amber)', fontFamily: 'var(--font-mono)', letterSpacing: '0.02em', lineHeight: 1.6 },
};

export default TickerList;
