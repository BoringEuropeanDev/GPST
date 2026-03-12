import React, { useState, useEffect } from 'react';

const Header = () => {
  const [time, setTime] = useState(new Date());
  const [marketStatus, setMarketStatus] = useState('CLOSED');

  useEffect(() => {
    const timer = setInterval(() => {
      const now = new Date();
      setTime(now);
      const et = new Date(now.toLocaleString("en-US", { timeZone: "America/New_York" }));
      const day = et.getDay();
      const totalMins = et.getHours() * 60 + et.getMinutes();
      if (day >= 1 && day <= 5 && totalMins >= 570 && totalMins < 960) {
        setMarketStatus('OPEN');
      } else if (day >= 1 && day <= 5 && ((totalMins >= 240 && totalMins < 570) || (totalMins >= 960 && totalMins < 1200))) {
        setMarketStatus('PRE/POST');
      } else {
        setMarketStatus('CLOSED');
      }
    }, 1000);
    return () => clearInterval(timer);
  }, []);

  const formatTime = (date) =>
    date.toLocaleTimeString('en-US', { hour12: false, timeZone: 'America/New_York' });

  const statusColor =
    marketStatus === 'OPEN' ? 'var(--accent-green)' :
    marketStatus === 'PRE/POST' ? 'var(--accent-amber)' :
    'var(--accent-red)';

  return (
    <header style={styles.header}>
      <div style={styles.inner}>
        <div style={styles.logo}>
          <div style={styles.logoMark}>
            <div style={styles.logoGrid}>
              {[...Array(9)].map((_, i) => (
                <div key={i} style={{ ...styles.logoCell, opacity: [0,2,4,6,8].includes(i) ? 1 : 0.3 }} />
              ))}
            </div>
          </div>
          <div>
            <div style={styles.logoText}>GPST</div>
            <div style={styles.logoSub}>GLOBAL PREDICTIVE STOCK TERMINAL</div>
          </div>
        </div>

        <div style={styles.centerStats}>
          <div style={styles.statItem}>
            <span style={styles.statLabel}>INDICES</span>
            <span style={styles.statVal}>LIVE</span>
          </div>
          <div style={styles.divider} />
          <div style={styles.statItem}>
            <span style={styles.statLabel}>TICKERS</span>
            <span style={styles.statVal}>80+</span>
          </div>
          <div style={styles.divider} />
          <div style={styles.statItem}>
            <span style={styles.statLabel}>ENGINE</span>
            <span style={{ ...styles.statVal, color: 'var(--accent-purple)' }}>ML v1.0</span>
          </div>
        </div>

        <div style={styles.rightSection}>
          <div style={styles.marketStatus}>
            <div style={{ ...styles.statusDot, background: statusColor, boxShadow: `0 0 8px ${statusColor}` }} />
            <div>
              <div style={{ ...styles.statusLabel, color: statusColor }}>NYSE {marketStatus}</div>
              <div style={styles.statusTime}>{formatTime(time)} ET</div>
            </div>
          </div>
          <div style={styles.disclaimer}>⚠ NOT FINANCIAL ADVICE</div>
        </div>
      </div>
      <div style={styles.terminalBar}>
        <span style={{ color: 'var(--accent-green)', opacity: 0.7 }}>▶</span>
        <span style={{ marginLeft: 8, color: 'var(--text-muted)' }}>
          sys://gpst.terminal v1.0.0 — All predictions are informational only.
          <span style={{ color: 'var(--accent-amber)' }}> NOT FINANCIAL ADVICE.</span>
        </span>
        <span className="cursor" style={{ marginLeft: 4 }} />
      </div>
    </header>
  );
};

const styles = {
  header: { background: 'var(--bg-secondary)', borderBottom: '1px solid var(--border)', position: 'sticky', top: 0, zIndex: 100 },
  inner: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '12px 24px', gap: 16, flexWrap: 'wrap' },
  logo: { display: 'flex', alignItems: 'center', gap: 12 },
  logoMark: { width: 32, height: 32 },
  logoGrid: { display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 2, width: '100%', height: '100%' },
  logoCell: { background: 'var(--accent-green)', borderRadius: 1 },
  logoText: { fontFamily: 'var(--font-display)', fontSize: 22, fontWeight: 800, color: 'var(--text-primary)', letterSpacing: '0.1em', lineHeight: 1 },
  logoSub: { fontSize: 8, color: 'var(--text-muted)', letterSpacing: '0.15em', fontFamily: 'var(--font-mono)' },
  centerStats: { display: 'flex', alignItems: 'center', gap: 16, background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 'var(--radius)', padding: '6px 16px' },
  statItem: { display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 2 },
  statLabel: { fontSize: 9, color: 'var(--text-muted)', letterSpacing: '0.1em' },
  statVal: { fontSize: 12, color: 'var(--accent-cyan)', fontWeight: 700 },
  divider: { width: 1, height: 24, background: 'var(--border)' },
  rightSection: { display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 4 },
  marketStatus: { display: 'flex', alignItems: 'center', gap: 8 },
  statusDot: { width: 8, height: 8, borderRadius: '50%' },
  statusLabel: { fontSize: 11, fontWeight: 700, letterSpacing: '0.05em', lineHeight: 1.2 },
  statusTime: { fontSize: 10, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' },
  disclaimer: { fontSize: 9, color: 'var(--accent-amber)', letterSpacing: '0.05em', opacity: 0.8 },
  terminalBar: { background: 'var(--bg-primary)', borderTop: '1px solid var(--border)', padding: '3px 24px', fontSize: 10, fontFamily: 'var(--font-mono)', color: 'var(--text-muted)', display: 'flex', alignItems: 'center', overflow: 'hidden', whiteSpace: 'nowrap' },
};

export default Header;
