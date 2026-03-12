import React, { useState, useEffect } from 'react';
import { sectorsAPI } from '../utils/api';

const MarketOverview = () => {
  const [sectors, setSectors] = useState({});
  const [indicators, setIndicators] = useState({});

  useEffect(() => {
    const load = async () => {
      try {
        const [sectorRes, indRes] = await Promise.allSettled([
          sectorsAPI.getSectors(),
          sectorsAPI.getEconomicIndicators(),
        ]);
        if (sectorRes.status === 'fulfilled') setSectors(sectorRes.value.data.sectors || {});
        if (indRes.status === 'fulfilled') setIndicators(indRes.value.data.indicators || {});
      } catch (e) {}
    };
    load();
    const interval = setInterval(load, 5 * 60 * 1000);
    return () => clearInterval(interval);
  }, []);

  const sectorItems = Object.entries(sectors).slice(0, 6);
  const indicatorItems = Object.entries(indicators).slice(0, 4);

  return (
    <div style={styles.container}>
      <div style={styles.scrollContainer}>
        <div style={styles.track}>
          <div style={styles.label}>SECTORS</div>
          {sectorItems.map(([name, data]) => (
            <div key={name} style={styles.item}>
              <span style={styles.itemName}>{name.split(' ')[0].toUpperCase()}</span>
              <span style={{
                ...styles.itemValue,
                color: data.change_pct > 0 ? 'var(--accent-green)' :
                       data.change_pct < 0 ? 'var(--accent-red)' : 'var(--text-secondary)'
              }}>
                {data.change_pct > 0 ? '▲' : data.change_pct < 0 ? '▼' : '●'}
                {' '}{Math.abs(data.change_pct || 0).toFixed(2)}%
              </span>
            </div>
          ))}
          <div style={styles.separator} />
          <div style={styles.label}>MACRO</div>
          {indicatorItems.map(([id, data]) => (
            <div key={id} style={styles.item}>
              <span style={styles.itemName}>{id}</span>
              <span style={styles.itemValue}>{data.latest?.toFixed(2) ?? '—'}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

const styles = {
  container: { background: 'var(--bg-secondary)', borderBottom: '1px solid var(--border)', overflow: 'hidden' },
  scrollContainer: { overflowX: 'auto', scrollbarWidth: 'none', msOverflowStyle: 'none' },
  track: { display: 'flex', alignItems: 'center', padding: '6px 24px', whiteSpace: 'nowrap', minWidth: 'max-content' },
  label: { fontSize: 9, color: 'var(--text-muted)', letterSpacing: '0.15em', marginRight: 12, fontFamily: 'var(--font-mono)' },
  item: { display: 'flex', alignItems: 'center', gap: 6, padding: '2px 12px', borderRight: '1px solid var(--border)' },
  itemName: { fontSize: 10, color: 'var(--text-secondary)', letterSpacing: '0.05em' },
  itemValue: { fontSize: 11, fontFamily: 'var(--font-mono)', fontWeight: 500 },
  separator: { width: 1, height: 20, background: 'var(--border-active)', margin: '0 16px' },
};

export default MarketOverview;
