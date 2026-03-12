"""
Stock API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import Optional
import asyncio

from app.database import get_db, Stock
from app.services.data_ingestion import DataIngestionService, GLOBAL_TICKERS

router = APIRouter()
ingestion = DataIngestionService()


@router.get("/")
async def list_stocks(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db)
):
    offset = (page - 1) * limit

    result = await db.execute(
        select(Stock)
        .where(Stock.is_active == True)
        .order_by(Stock.market_cap.desc().nullslast(), Stock.ticker)
        .offset(offset)
        .limit(limit)
    )
    stocks = result.scalars().all()

    total_result = await db.execute(select(func.count(Stock.id)).where(Stock.is_active == True))
    total = total_result.scalar()

    if not stocks:
        tickers_list = list(GLOBAL_TICKERS.items())
        page_items = tickers_list[offset:offset+limit]
        return {
            "stocks": [
                {"ticker": t, "name": n, "current_price": None, "previous_close": None,
                 "sector": None, "market_cap": None}
                for t, n in page_items
            ],
            "total": len(GLOBAL_TICKERS),
            "page": page,
            "limit": limit,
            "total_pages": (len(GLOBAL_TICKERS) + limit - 1) // limit
        }

    return {
        "stocks": [
            {
                "ticker": s.ticker,
                "name": s.name,
                "current_price": s.current_price,
                "previous_close": s.previous_close,
                "sector": s.sector,
                "market_cap": s.market_cap,
                "currency": s.currency,
                "exchange": s.exchange,
                "change_pct": (
                    ((s.current_price - s.previous_close) / s.previous_close * 100)
                    if s.current_price and s.previous_close and s.previous_close != 0
                    else None
                )
            }
            for s in stocks
        ],
        "total": total,
        "page": page,
        "limit": limit,
        "total_pages": (total + limit - 1) // limit
    }


@router.get("/{ticker}")
async def get_stock_detail(ticker: str, db: AsyncSession = Depends(get_db)):
    ticker = ticker.upper()

    result = await db.execute(select(Stock).where(Stock.ticker == ticker))
    stock = result.scalar_one_or_none()

    quote_task = ingestion.get_yahoo_quote(ticker)
    profile_task = ingestion.get_yahoo_profile(ticker)

    quote, profile = await asyncio.gather(quote_task, profile_task, return_exceptions=True)

    if isinstance(quote, Exception):
        quote = {}
    if isinstance(profile, Exception):
        profile = {}

    if not quote and not stock:
        raise HTTPException(status_code=404, detail=f"Ticker {ticker} not found")

    name = GLOBAL_TICKERS.get(ticker, (quote or {}).get("name", ticker))
    if stock:
        name = stock.name or name

    current_price = (quote or {}).get("current_price") or (stock.current_price if stock else None)
    prev_close = (quote or {}).get("previous_close") or (stock.previous_close if stock else None)

    return {
        "ticker": ticker,
        "name": name,
        "current_price": current_price,
        "previous_close": prev_close,
        "change_pct": ((current_price - prev_close) / prev_close * 100)
            if current_price and prev_close and prev_close != 0 else None,
        "currency": (quote or {}).get("currency", "USD"),
        "exchange": (quote or {}).get("exchange", ""),
        "market_cap": (quote or {}).get("market_cap") or (profile or {}).get("market_cap"),
        "volume": (quote or {}).get("volume"),
        "sector": (profile or {}).get("sector") or (stock.sector if stock else None),
        "industry": (profile or {}).get("industry") or (stock.industry if stock else None),
        "description": (profile or {}).get("description") or (stock.description if stock else None),
        "website": (profile or {}).get("website") or (stock.website if stock else None),
        "country": (profile or {}).get("country") or (stock.country if stock else None),
        "employees": (profile or {}).get("employees") or (stock.employees if stock else None),
        "pe_ratio": (profile or {}).get("pe_ratio"),
        "dividend_yield": (profile or {}).get("dividend_yield"),
        "fifty_two_week_high": (profile or {}).get("fifty_two_week_high"),
        "fifty_two_week_low": (profile or {}).get("fifty_two_week_low"),
        "avg_volume": (profile or {}).get("avg_volume"),
        "beta": (profile or {}).get("beta"),
        "forward_pe": (profile or {}).get("forward_pe"),
        "profit_margins": (profile or {}).get("profit_margins"),
    }


@router.get("/{ticker}/history")
async def get_stock_history(
    ticker: str,
    period: str = Query("1y", regex="^(1mo|3mo|6mo|1y|2y|5y)$")
):
    ticker = ticker.upper()
    data = await ingestion.get_yahoo_history(ticker, period)
    if not data:
        raise HTTPException(status_code=404, detail="Historical data not available")
    return {"ticker": ticker, "period": period, "data": data}


@router.get("/{ticker}/news")
async def get_stock_news(ticker: str):
    ticker = ticker.upper()
    av_task = ingestion.get_alpha_vantage_news(ticker)
    gdelt_task = ingestion.get_gdelt_news(ticker)

    av_news, gdelt_news = await asyncio.gather(av_task, gdelt_task, return_exceptions=True)

    all_news = []
    if isinstance(av_news, list):
        all_news.extend(av_news)
    if isinstance(gdelt_news, list):
        all_news.extend(gdelt_news)

    return {"ticker": ticker, "news": all_news[:20]}
