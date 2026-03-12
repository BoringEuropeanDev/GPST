import axios from 'axios';

const BASE_URL = process.env.REACT_APP_API_URL || '/api';

const api = axios.create({
  baseURL: BASE_URL,
  timeout: 30000,
});

export const stocksAPI = {
  list: (page = 1, limit = 50) => api.get(`/stocks/?page=${page}&limit=${limit}`),
  getDetail: (ticker) => api.get(`/stocks/${ticker}`),
  getHistory: (ticker, period = '1y') => api.get(`/stocks/${ticker}/history?period=${period}`),
  getNews: (ticker) => api.get(`/stocks/${ticker}/news`),
};

export const predictionsAPI = {
  getPrediction: (ticker) => api.get(`/predictions/${ticker}`),
  getPredictionHistory: (ticker) => api.get(`/predictions/${ticker}/history`),
  getModelMetrics: () => api.get('/predictions/model/metrics'),
};

export const sectorsAPI = {
  getSectors: () => api.get('/sectors/'),
  getEconomicIndicators: () => api.get('/sectors/economic-indicators'),
};

export const newsAPI = {
  getGlobalNews: () => api.get('/news/global'),
};

export default api;
