import axios from 'axios';

const BASE_URL = process.env.REACT_APP_API_URL || '/api';

const api = axios.create({
  baseURL: BASE_URL,
  timeout: 30000,
});

// ── Global error interceptor ──────────────────────────────────────────────────
api.interceptors.response.use(
  response => response,
  error => {
    if (!error.response) {
      // Network error / backend unreachable
      console.error('[GPST] Network error — backend may be down:', error.message);
    } else if (error.response.status >= 500) {
      console.error(`[GPST] Server error ${error.response.status}:`, error.response.data);
    } else if (error.response.status === 404) {
      // 404s are expected for unknown tickers — don't spam the console
    } else {
      console.warn(`[GPST] API error ${error.response.status}:`, error.response.data);
    }
    return Promise.reject(error);
  }
);

export const stocksAPI = {
  list:       (page = 1, limit = 50) => api.get(`/stocks/?page=${page}&limit=${limit}`),
  getDetail:  (ticker)               => api.get(`/stocks/${ticker}`),
  getHistory: (ticker, period = '1y')=> api.get(`/stocks/${ticker}/history?period=${period}`),
  getNews:    (ticker)               => api.get(`/stocks/${ticker}/news`),
};

export const predictionsAPI = {
  getPrediction:        (ticker, refresh = false) =>
    api.get(`/predictions/${ticker}${refresh ? '?refresh=true' : ''}`),
  getPredictionHistory: (ticker) => api.get(`/predictions/${ticker}/history`),
  getModelMetrics:      (params = {}) => {
    const qs = new URLSearchParams(params).toString();
    return api.get(`/predictions/model/metrics${qs ? '?' + qs : ''}`);
  },
};

export const sectorsAPI = {
  getSectors:            () => api.get('/sectors/'),
  getEconomicIndicators: () => api.get('/sectors/economic-indicators'),
};

export const newsAPI = {
  getGlobalNews: () => api.get('/news/global'),
};

export default api;
