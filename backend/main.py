"""
Global Predictive Stock Terminal - Backend API
FastAPI application with async support
"""
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import Response
from fastapi.middleware.gzip import GZipMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.api import stocks, predictions, sectors, news, health
from app.services.data_ingestion import ingestion_service          # shared singleton
from app.services.prediction_engine import prediction_engine       # shared singleton
from app.database import init_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing database...")
    await init_db()

    logger.info("Starting schedulers...")

    # ── Existing jobs ──────────────────────────────────────────────────────────
    scheduler.add_job(ingestion_service.refresh_stock_data,        'interval', minutes=15, id='stock_refresh')
    scheduler.add_job(ingestion_service.refresh_news_data,         'interval', minutes=30, id='news_refresh')
    scheduler.add_job(ingestion_service.refresh_economic_data,     'interval', hours=6,   id='economic_refresh')
    scheduler.add_job(prediction_engine.run_predictions,           'interval', hours=1,   id='predictions')
    scheduler.add_job(prediction_engine.evaluate_past_predictions, 'cron',     hour=18, minute=0, id='evaluate')

    # ── Nightly ML retrain at 03:00 UTC ───────────────────────────────────────
    # Set DISABLE_ML_TRAINING=true in Railway env to skip (useful while iterating).
    if os.getenv("DISABLE_ML_TRAINING", "").lower() not in ("true", "1", "yes"):
        scheduler.add_job(
            prediction_engine.train_models,
            'cron',
            hour=3, minute=0,
            id='retrain_models',
            max_instances=1,
            coalesce=True,
        )
        logger.info("Nightly ML retraining job scheduled at 03:00 UTC")
    else:
        logger.info("ML retraining disabled via DISABLE_ML_TRAINING env var")

    scheduler.start()

    asyncio.create_task(ingestion_service.initial_load())

    yield

    scheduler.shutdown()
    logger.info("Application shutdown complete")


app = FastAPI(
    title="Global Predictive Stock Terminal API",
    description="AI-powered stock prediction platform using free and open data sources",
    version="2.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def add_cors_headers(request: Request, call_next):
    if request.method == "OPTIONS":
        response = Response()
        response.headers["Access-Control-Allow-Origin"]  = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "*"
        response.headers["Access-Control-Max-Age"]       = "86400"
        return response
    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response


app.add_middleware(GZipMiddleware, minimum_size=1000)

app.include_router(health.router,      prefix="/api/health",      tags=["health"])
app.include_router(stocks.router,      prefix="/api/stocks",      tags=["stocks"])
app.include_router(predictions.router, prefix="/api/predictions", tags=["predictions"])
app.include_router(sectors.router,     prefix="/api/sectors",     tags=["sectors"])
app.include_router(news.router,        prefix="/api/news",        tags=["news"])


@app.get("/")
async def root():
    return {
        "name":       "Global Predictive Stock Terminal",
        "version":    "2.0.0",
        "disclaimer": "This is not financial advice. All predictions are for informational purposes only.",
        "docs":       "/docs",
    }
