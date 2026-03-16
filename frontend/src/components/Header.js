import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';

/**
 * GPST Logo — 2×2 grid with opacity gradient
 * Use as both in-page logo and favicon source.
 *
 * For favicon, add to public/index.html <head>:
 * <link rel="icon" type="image/svg+xml" href="/favicon.svg" />
 */
function Logo({ size = 28 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none">
      <rect x="2" y="2" width="12" height="12" rx="2.5" fill="#00D4AA" />
      <rect x="18" y="2" width="12" height="12" rx="2.5" fill="#00D4AA" opacity=".55" />
      <rect x="2" y="18" width="12" height="12" rx="2.5" fill="#00D4AA" opacity=".55" />
      <rect x="18" y="18" width="12" height="12" rx="2.5" fill="#00D4AA" opacity=".25" />
    </svg>
  );
}

function MarketStatus() {
  const [time, setTime] = useState(new Date());

  useEffect(() => {
    const interval = setInterval(() => setTime(new Date()), 30000);
    return () => clearInterval(interval);
  }, []);

  // Simple NYSE check: weekdays 9:30-16:00 ET
  const et = new Date(time.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const day = et.getDay();
  const hours = et.getHours() + et.getMinutes() / 60;
  const isOpen = day >= 1 && day <= 5 && hours >= 9.5 && hours < 16;

  return (
    <div className="market-status">
      <div className={`market-dot ${isOpen ? 'open' : ''}`} />
      <span>{isOpen ? 'MARKET OPEN' : 'MARKET CLOSED'}</span>
    </div>
  );
}

export default function Header() {
  return (
    <header className="gpst-header">
      <Link to="/" className="gpst-brand">
        <Logo size={28} />
        <span className="gpst-wordmark">GPST</span>
        <span className="gpst-version">v2.0</span>
      </Link>
      <MarketStatus />
    </header>
  );
}

export { Logo };
