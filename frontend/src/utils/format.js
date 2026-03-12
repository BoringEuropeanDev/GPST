export const formatPrice = (price, currency = 'USD') => {
  if (price == null) return '—';
  return new Intl.NumberFormat('en-US', {
    style: 'currency', currency,
    minimumFractionDigits: 2,
    maximumFractionDigits: price < 1 ? 4 : 2,
  }).format(price);
};

export const formatChange = (pct) => {
  if (pct == null) return '—';
  return `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`;
};

export const formatMarketCap = (cap) => {
  if (cap == null) return '—';
  if (cap >= 1e12) return `$${(cap / 1e12).toFixed(2)}T`;
  if (cap >= 1e9) return `$${(cap / 1e9).toFixed(2)}B`;
  if (cap >= 1e6) return `$${(cap / 1e6).toFixed(2)}M`;
  return `$${cap.toFixed(0)}`;
};

export const formatVolume = (vol) => {
  if (vol == null) return '—';
  if (vol >= 1e9) return `${(vol / 1e9).toFixed(2)}B`;
  if (vol >= 1e6) return `${(vol / 1e6).toFixed(2)}M`;
  if (vol >= 1e3) return `${(vol / 1e3).toFixed(1)}K`;
  return vol.toFixed(0);
};

export const getChangeClass = (pct) => {
  if (pct == null) return 'neutral';
  if (pct > 0) return 'positive';
  if (pct < 0) return 'negative';
  return 'neutral';
};

export const getDirectionEmoji = (direction) => {
  if (direction === 'UP') return '▲';
  if (direction === 'DOWN') return '▼';
  return '●';
};

export const getConfidenceColor = (confidence) => {
  if (confidence >= 0.75) return 'var(--accent-green)';
  if (confidence >= 0.60) return 'var(--accent-amber)';
  return 'var(--accent-red)';
};

export const formatDate = (dateStr) => {
  if (!dateStr) return '—';
  return new Date(dateStr).toLocaleDateString('en-US', {
    month: 'short', day: 'numeric', year: 'numeric'
  });
};

export const timeAgo = (dateStr) => {
  if (!dateStr) return '';
  const diff = Date.now() - new Date(dateStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
};
