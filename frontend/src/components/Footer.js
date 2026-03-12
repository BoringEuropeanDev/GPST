import React from 'react';

const Footer = () => (
  <footer style={styles.footer}>
    <div style={styles.inner}>
      <div style={styles.left}>
        <span style={{ color: 'var(--accent-green)', fontWeight: 700 }}>GPST</span>
        <span style={styles.sep}>|</span>
        <span>Global Predictive Stock Terminal</span>
        <span style={styles.sep}>|</span>
        <span>Open Source · Free APIs Only</span>
      </div>
      <div style={styles.right}>
        <span style={styles.disclaimer}>⚠️ NOT FINANCIAL ADVICE — For informational purposes only</span>
      </div>
    </div>
    <div style={styles.bottom}>
      Data: Yahoo Finance · Alpha Vantage · FRED · GDELT · World Bank · NewsAPI · Mediastack
    </div>
  </footer>
);

const styles = {
  footer: { background: 'var(--bg-secondary)', borderTop: '1px solid var(--border)', padding: '12px 24px', marginTop: 40 },
  inner: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: 11, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)', flexWrap: 'wrap', gap: 8 },
  left: { display: 'flex', alignItems: 'center', gap: 8 },
  right: {},
  sep: { color: 'var(--border)', margin: '0 4px' },
  disclaimer: { color: 'var(--accent-amber)', fontSize: 10 },
  bottom: { marginTop: 6, fontSize: 9, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)', letterSpacing: '0.05em' },
};

export default Footer;
