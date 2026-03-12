"""Sectors API"""
from fastapi import APIRouter
from app.services.data_ingestion import DataIngestionService, FRED_SERIES

router = APIRouter()
ingestion = DataIngestionService()

@router.get("/")
async def get_sectors():
    sector_data = await ingestion.get_sector_etf_data()
    return {"sectors": sector_data}

@router.get("/economic-indicators")
async def get_economic_indicators():
    results = {}
    for series_id in list(FRED_SERIES.keys())[:6]:
        data = await ingestion.get_fred_data(series_id, 5)
        if data:
            results[series_id] = {
                "name": FRED_SERIES[series_id],
                "latest": data[0].get("value") if data else None,
                "history": data[:5]
            }
    commodities = await ingestion.get_commodity_prices()
    return {"indicators": results, "commodities": commodities}
