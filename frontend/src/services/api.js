import axios from "axios";

const api = axios.create({
  baseURL: "/api", // Vite proxy will handle this
});

export const getHistory = async (start, end) => {
  const params = {};
  if (start) params.start = start;
  if (end) params.end = end;
  const response = await api.get("/history", { params });
  return response.data;
};

export const getHistoryMeta = async () => {
  const response = await api.get("/history/meta");
  return response.data;
};

export const getIndicators = async (params = {}) => {
  const response = await api.get("/indicators", { params });
  return response.data;
};

export const getNews = async (
  days = null,
  start = null,
  end = null,
  page = 1,
  limit = 20,
) => {
  const params = { page, limit };
  if (days !== null) params.days = days;
  if (start) params.start = start;
  if (end) params.end = end;
  const response = await api.get("/news", { params });
  return response.data;
};

export const getModels = async () => {
  const response = await api.get("/models");
  return response.data;
};

export const getPrediction = async (
  modelName,
  start = null,
  end = null,
  predictionHours = 5,
) => {
  const params = { model_name: modelName, prediction_hours: predictionHours };
  if (start) params.start = start;
  if (end) params.end = end;
  const response = await api.post("/predict", null, { params });
  return response.data;
};

export const chatWithBot = async (data) => {
  const response = await api.post("/chat", data);
  return response.data;
};

export const clearChatHistory = async () => {
  const response = await api.post("/chat/clear");
  return response.data;
};

export const searchKnowledgeBase = async (query, collections = "kline,news,open_interest,analysis", top_k = 5) => {
  const response = await api.get("/chat/search", {
    params: { query, collections, top_k }
  });
  return response.data;
};

export const analyzePriceTrend = async (ticker, timeframe = "recent") => {
  const response = await api.post("/analyze/price-trend", { ticker, timeframe });
  return response.data;
};

export const analyzeSentiment = async (ticker, days = 7) => {
  const response = await api.post("/analyze/sentiment", { ticker, days });
  return response.data;
};

export const populateVectorDB = async (ticker = "BTCUSDT", days = 30) => {
  const response = await api.post("/vectordb/populate", null, {
    params: { ticker, days }
  });
  return response.data;
};