"""News API"""
from fastapi import APIRouter
from app.services.data_ingestion import DataIngestionService
import asyncio

router = APIRouter()
ingestion = DataIngestionService()

@router.get("/global")
async def get_global_news():
    gdelt_task = ingestion.get_gdelt_news("stock market finance economy")
    geo_task = ingestion.get_gdelt_geopolitical_events()
    news, geo = await asyncio.gather(gdelt_task, geo_task, return_exceptions=True)
    return {
        "market_news": news if isinstance(news, list) else [],
        "geopolitical_events": geo if isinstance(geo, list) else [],
    }
