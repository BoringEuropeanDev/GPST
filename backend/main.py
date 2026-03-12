"""
Global Predictive Stock Terminal - Backend API
FastAPI application with async support
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.api import stocks, predictions, sectors, news, health
from app.services.data_ingestion import DataIngestionService
from app.services.prediction_engine import PredictionEngine
from app.database import init_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing database...")
    await init_db()

    logger.info("Starting data ingestion scheduler...")
    ingestion = DataIngestionService()
    prediction = PredictionEngine()

    scheduler.add_job(ingestion.refresh_stock_data, 'interval', minutes=15, id='stock_refresh')
    scheduler.add_job(ingestion.refresh_news_data, 'interval', minutes=30, id='news_refresh')
    scheduler.add_job(ingestion.refresh_economic_data, 'interval', hours=6, id='economic_refresh')
    scheduler.add_job(prediction.run_predictions, 'interval', hours=1, id='predictions')
    scheduler.add_job(prediction.evaluate_past_predictions, 'cron', hour=18, minute=0, id='evaluate')
    scheduler.start()

    asyncio.create_task(ingestion.initial_load())

    yield

    scheduler.shutdown()
    logger.info("Application shutdown complete")

app = FastAPI(
    title="Global Predictive Stock Terminal API",
    description="AI-powered stock prediction platform using free and open data sources",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

app.include_router(health.router, prefix="/api/health", tags=["health"])
app.include_router(stocks.router, prefix="/api/stocks", tags=["stocks"])
app.include_router(predictions.router, prefix="/api/predictions", tags=["predictions"])
app.include_router(sectors.router, prefix="/api/sectors", tags=["sectors"])
app.include_router(news.router, prefix="/api/news", tags=["news"])

@app.get("/")
async def root():
    return {
        "name": "Global Predictive Stock Terminal",
        "version": "1.0.0",
        "disclaimer": "This is not financial advice. All predictions are for informational purposes only.",
        "docs": "/docs"
    }
