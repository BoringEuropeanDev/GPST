"""
Data Ingestion Service - Aggregates data from all free/open APIs
Sources: Yahoo Finance, Alpha Vantage, FRED, World Bank, GDELT, NewsAPI, etc.
"""
import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import httpx

logger = logging.getLogger(__name__)

ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY", "demo")
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")
MEDIASTACK_KEY = os.getenv("MEDIASTACK_KEY", "")
FRED_API_KEY = os.getenv("FRED_API_KEY", "")

GLOBAL_TICKERS = {
    # US Large Cap
    "AAPL": "Apple Inc.", "MSFT": "Microsoft Corporation", "GOOGL": "Alphabet Inc.",
    "AMZN": "Amazon.com Inc.", "NVDA": "NVIDIA Corporation", "META": "Meta Platforms Inc.",
    "TSLA": "Tesla Inc.", "BRK-B": "Berkshire Hathaway", "JPM": "JPMorgan Chase & Co.",
    "V": "Visa Inc.", "JNJ": "Johnson & Johnson", "WMT": "Walmart Inc.",
    "PG": "Procter & Gamble Co.", "MA": "Mastercard Inc.", "HD": "Home Depot Inc.",
    "UNH": "UnitedHealth Group", "NFLX": "Netflix Inc.", "LLY": "Eli Lilly and Co.",
    "AVGO": "Broadcom Inc.", "XOM": "Exxon Mobil Corp.", "COST": "Costco Wholesale",
    "ABBV": "AbbVie Inc.", "CVX": "Chevron Corporation", "PEP": "PepsiCo Inc.",
    "TMO": "Thermo Fisher Scientific", "MRK": "Merck & Co.", "ORCL": "Oracle Corporation",
    "CRM": "Salesforce Inc.", "AMD": "Advanced Micro Devices", "ACN": "Accenture plc",
    "NKE": "Nike Inc.", "ADBE": "Adobe Inc.", "IBM": "IBM Corporation",
    "QCOM": "Qualcomm Inc.", "TXN": "Texas Instruments", "INTC": "Intel Corporation",
    "PYPL": "PayPal Holdings", "UBER": "Uber Technologies", "SPOT": "Spotify Technology",
    "SHOP": "Shopify Inc.", "SQ": "Block Inc.", "COIN": "Coinbase Global",
    # ETFs
    "SPY": "SPDR S&P 500 ETF", "QQQ": "Invesco QQQ Trust", "IWM": "iShares Russell 2000",
    "DIA": "SPDR Dow Jones ETF", "VTI": "Vanguard Total Stock Market",
    "GLD": "SPDR Gold Shares", "SLV": "iShares Silver Trust",
    "TLT": "iShares 20+ Year Treasury", "HYG": "iShares High Yield Bond",
    # International
    "TSM": "Taiwan Semiconductor", "BABA": "Alibaba Group", "TM": "Toyota Motor",
    "SONY": "Sony Group Corporation", "NVO": "Novo Nordisk", "ASML": "ASML Holding",
    "SAP": "SAP SE", "HSBC": "HSBC Holdings", "BP": "BP plc", "RIO": "Rio Tinto",
    "BHP": "BHP Group", "VALE": "Vale S.A.", "BBVA": "Banco Bilbao Vizcaya",
    "SAN": "Banco Santander", "UBS": "UBS Group",
    # Energy
    "SLB": "Schlumberger Limited", "HAL": "Halliburton Company", "MPC": "Marathon Petroleum",
    "PSX": "Phillips 66", "VLO": "Valero Energy",
    # Healthcare
    "PFE": "Pfizer Inc.", "MRNA": "Moderna Inc.", "BIIB": "Biogen Inc.",
    "GILD": "Gilead Sciences", "REGN": "Regeneron Pharmaceuticals",
    # Financials
    "BAC": "Bank of America", "WFC": "Wells Fargo", "GS": "Goldman Sachs",
    "MS": "Morgan Stanley", "C": "Citigroup Inc.", "BLK": "BlackRock Inc.",
    # Materials
    "FCX": "Freeport-McMoRan", "NEM": "Newmont Corporation", "AA": "Alcoa Corporation",
    # Consumer
    "MCD": "McDonald's Corporation", "SBUX": "Starbucks Corporation",
    "KO": "Coca-Cola Company", "PM": "Philip Morris", "MO": "Altria Group",
}

SECTORS = [
    "Technology", "Healthcare", "Financials", "Consumer Discretionary",
    "Consumer Staples", "Energy", "Materials", "Industrials",
    "Utilities", "Real Estate", "Communication Services"
]

FRED_SERIES = {
    "GDP": "GDP",
    "UNRATE": "Unemployment Rate",
    "CPIAUCSL": "Consumer Price Index",
    "FEDFUNDS": "Federal Funds Rate",
    "T10Y2Y": "10-2 Year Treasury Spread",
    "DCOILWTICO": "Crude Oil Price WTI",
    "GOLDAMGBD228NLBM": "Gold Price",
    "DTWEXBGS": "US Dollar Index",
    "VIXCLS": "VIX Volatility Index",
    "RETAILSL": "Retail Sales",
    "INDPRO": "Industrial Production",
    "HOUST": "Housing Starts",
}


class DataIngestionService:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        self._cache = {}
        self._cache_ttl = {}

    async def _cached_get(self, url: str, ttl_seconds: int = 300) -> Optional[Dict]:
        now = datetime.now().timestamp()
        if url in self._cache and self._cache_ttl.get(url, 0) > now:
            return self._cache[url]
        try:
            resp = await self.client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                self._cache[url] = data
                self._cache_ttl[url] = now + ttl_seconds
                return data
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
        return None

    async def get_yahoo_quote(self, ticker: str) -> Optional[Dict]:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1d"
        try:
            resp = await self.client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200:
                data = resp.json()
                meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
                return {
                    "ticker": ticker,
                    "name": GLOBAL_TICKERS.get(ticker, meta.get("longName", ticker)),
                    "current_price": meta.get("regularMarketPrice"),
                    "previous_close": meta.get("previousClose") or meta.get("chartPreviousClose"),
                    "currency": meta.get("currency", "USD"),
                    "exchange": meta.get("exchangeName", ""),
                    "market_cap": meta.get("marketCap"),
                    "volume": meta.get("regularMarketVolume"),
                }
        except Exception as e:
            logger.error(f"Yahoo quote error for {ticker}: {e}")
        return None

    async def get_yahoo_history(self, ticker: str, period: str = "1y") -> List[Dict]:
        range_map = {"1mo": "1mo", "3mo": "3mo", "6mo": "6mo", "1y": "1y", "2y": "2y", "5y": "5y"}
        range_str = range_map.get(period, "1y")
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range={range_str}"
        try:
            resp = await self.client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200:
                data = resp.json()
                result = data.get("chart", {}).get("result", [])
                if not result:
                    return []
                timestamps = result[0].get("timestamp", [])
                quotes = result[0].get("indicators", {}).get("quote", [{}])[0]
                adj_close = result[0].get("indicators", {}).get("adjclose", [{}])[0].get("adjclose", [])
                prices = []
                for i, ts in enumerate(timestamps):
                    if quotes.get("close") and i < len(quotes["close"]) and quotes["close"][i]:
                        prices.append({
                            "date": datetime.fromtimestamp(ts).isoformat(),
                            "open": quotes.get("open", [])[i] if i < len(quotes.get("open", [])) else None,
                            "high": quotes.get("high", [])[i] if i < len(quotes.get("high", [])) else None,
                            "low": quotes.get("low", [])[i] if i < len(quotes.get("low", [])) else None,
                            "close": quotes.get("close", [])[i],
                            "volume": quotes.get("volume", [])[i] if i < len(quotes.get("volume", [])) else None,
                            "adjusted_close": adj_close[i] if i < len(adj_close) else None,
                        })
                return prices
        except Exception as e:
            logger.error(f"Yahoo history error for {ticker}: {e}")
        return []

    async def get_yahoo_profile(self, ticker: str) -> Optional[Dict]:
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=assetProfile,summaryDetail,defaultKeyStatistics"
        try:
            resp = await self.client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200:
                data = resp.json()
                result = data.get("quoteSummary", {}).get("result", [{}])[0]
                profile = result.get("assetProfile", {})
                summary = result.get("summaryDetail", {})
                stats = result.get("defaultKeyStatistics", {})
                return {
                    "sector": profile.get("sector"),
                    "industry": profile.get("industry"),
                    "description": profile.get("longBusinessSummary"),
                    "website": profile.get("website"),
                    "country": profile.get("country"),
                    "employees": profile.get("fullTimeEmployees"),
                    "pe_ratio": summary.get("trailingPE", {}).get("raw"),
                    "dividend_yield": summary.get("dividendYield", {}).get("raw"),
                    "fifty_two_week_high": summary.get("fiftyTwoWeekHigh", {}).get("raw"),
                    "fifty_two_week_low": summary.get("fiftyTwoWeekLow", {}).get("raw"),
                    "avg_volume": summary.get("averageVolume", {}).get("raw"),
                    "beta": stats.get("beta", {}).get("raw"),
                    "market_cap": summary.get("marketCap", {}).get("raw"),
                    "forward_pe": summary.get("forwardPE", {}).get("raw"),
                    "profit_margins": stats.get("profitMargins", {}).get("raw"),
                }
        except Exception as e:
            logger.error(f"Yahoo profile error for {ticker}: {e}")
        return None

    async def get_alpha_vantage_news(self, ticker: str) -> List[Dict]:
        if not ALPHA_VANTAGE_KEY or ALPHA_VANTAGE_KEY == "demo":
            return []
        url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&apikey={ALPHA_VANTAGE_KEY}&limit=10"
        try:
            data = await self._cached_get(url, ttl_seconds=3600)
            if data and "feed" in data:
                items = []
                for article in data["feed"][:10]:
                    sentiment = 0
                    for ts in article.get("ticker_sentiment", []):
                        if ts.get("ticker") == ticker:
                            sentiment = float(ts.get("ticker_sentiment_score", 0))
                    items.append({
                        "title": article.get("title"),
                        "summary": article.get("summary"),
                        "url": article.get("url"),
                        "source": article.get("source"),
                        "published_at": article.get("time_published"),
                        "sentiment": sentiment,
                    })
                return items
        except Exception as e:
            logger.error(f"Alpha Vantage news error: {e}")
        return []

    async def get_gdelt_news(self, query: str) -> List[Dict]:
        url = f"https://api.gdeltproject.org/api/v2/doc/doc?query={query}&mode=artlist&maxrecords=10&format=json&sort=datedesc"
        try:
            data = await self._cached_get(url, ttl_seconds=1800)
            if data and "articles" in data:
                return [{
                    "title": a.get("title"),
                    "url": a.get("url"),
                    "source": a.get("domain"),
                    "published_at": a.get("seendate"),
                    "sentiment": 0,
                    "summary": "",
                } for a in data["articles"][:10]]
        except Exception as e:
            logger.error(f"GDELT error: {e}")
        return []

    async def get_fred_data(self, series_id: str, limit: int = 100) -> List[Dict]:
        base_url = "https://api.stlouisfed.org/fred/series/observations"
        api_key = FRED_API_KEY if FRED_API_KEY else "abcdefghijklmnopqrstuvwxyz123456"
        params = f"?series_id={series_id}&sort_order=desc&limit={limit}&file_type=json&api_key={api_key}"
        url = base_url + params
        try:
            data = await self._cached_get(url, ttl_seconds=86400)
            if data and "observations" in data:
                return [{
                    "date": obs["date"],
                    "value": float(obs["value"]) if obs["value"] != "." else None,
                    "series": series_id
                } for obs in data["observations"] if obs["value"] != "."]
        except Exception as e:
            logger.error(f"FRED error for {series_id}: {e}")
        return []

    async def get_world_bank_data(self, indicator: str, country: str = "US") -> List[Dict]:
        url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json&per_page=10&mrv=10"
        try:
            data = await self._cached_get(url, ttl_seconds=86400 * 7)
            if data and len(data) > 1 and data[1]:
                return [{
                    "date": item.get("date"),
                    "value": item.get("value"),
                    "indicator": item.get("indicator", {}).get("value"),
                    "country": item.get("country", {}).get("value"),
                } for item in data[1] if item.get("value") is not None]
        except Exception as e:
            logger.error(f"World Bank error: {e}")
        return []

    async def get_commodity_prices(self) -> Dict:
        commodities = {
            "CL=F": "Crude Oil WTI", "GC=F": "Gold", "SI=F": "Silver",
            "NG=F": "Natural Gas", "ZC=F": "Corn", "ZW=F": "Wheat",
            "BTC-USD": "Bitcoin", "ETH-USD": "Ethereum",
        }
        results = {}
        tasks = [self.get_yahoo_quote(sym) for sym in commodities.keys()]
        quotes = await asyncio.gather(*tasks, return_exceptions=True)
        for sym, quote in zip(commodities.keys(), quotes):
            if isinstance(quote, dict) and quote:
                results[sym] = {
                    "name": commodities[sym],
                    "price": quote.get("current_price"),
                    "change": None
                }
        return results

    async def get_newsapi_headlines(self, query: str) -> List[Dict]:
        if not NEWS_API_KEY:
            return []
        url = f"https://newsapi.org/v2/everything?q={query}&sortBy=publishedAt&pageSize=5&apiKey={NEWS_API_KEY}"
        try:
            data = await self._cached_get(url, ttl_seconds=3600)
            if data and data.get("status") == "ok":
                return [{
                    "title": a.get("title"),
                    "summary": a.get("description"),
                    "url": a.get("url"),
                    "source": a.get("source", {}).get("name"),
                    "published_at": a.get("publishedAt"),
                    "sentiment": 0,
                } for a in data.get("articles", [])[:5]]
        except Exception as e:
            logger.error(f"NewsAPI error: {e}")
        return []

    async def get_gdelt_geopolitical_events(self) -> List[Dict]:
        url = "https://api.gdeltproject.org/api/v2/gkg/gkg?query=conflict+OR+war+OR+sanctions+OR+trade&mode=artlist&maxrecords=20&format=json"
        try:
            data = await self._cached_get(url, ttl_seconds=3600)
            if data and "articles" in data:
                return [{
                    "title": a.get("title", ""),
                    "url": a.get("url", ""),
                    "source": a.get("domain", ""),
                    "date": a.get("seendate", ""),
                    "event_type": "geopolitical",
                    "impact_score": 0.5,
                } for a in data.get("articles", [])[:20]]
        except Exception as e:
            logger.error(f"GDELT geopolitical error: {e}")
        return []

    async def get_sector_etf_data(self) -> Dict:
        sector_etfs = {
            "XLK": "Technology", "XLV": "Healthcare", "XLF": "Financials",
            "XLY": "Consumer Discretionary", "XLP": "Consumer Staples",
            "XLE": "Energy", "XLB": "Materials", "XLI": "Industrials",
            "XLU": "Utilities", "XLRE": "Real Estate", "XLC": "Communication Services"
        }
        results = {}
        tasks = [self.get_yahoo_quote(etf) for etf in sector_etfs.keys()]
        quotes = await asyncio.gather(*tasks, return_exceptions=True)
        for etf, quote in zip(sector_etfs.keys(), quotes):
            if isinstance(quote, dict) and quote:
                sector = sector_etfs[etf]
                prev = quote.get("previous_close") or quote.get("current_price", 1)
                curr = quote.get("current_price", 0)
                pct_change = ((curr - prev) / prev * 100) if prev else 0
                results[sector] = {
                    "etf": etf, "price": curr, "change_pct": pct_change, "name": sector
                }
        return results

    async def refresh_stock_data(self):
        logger.info("Refreshing stock data...")
        try:
            from app.database import AsyncSessionLocal, Stock
            from sqlalchemy import update, select
            async with AsyncSessionLocal() as session:
                tickers = list(GLOBAL_TICKERS.keys())
                for i in range(0, min(len(tickers), 50), 10):
                    batch = tickers[i:i+10]
                    tasks = [self.get_yahoo_quote(t) for t in batch]
                    quotes = await asyncio.gather(*tasks, return_exceptions=True)
                    for ticker, quote in zip(batch, quotes):
                        if isinstance(quote, dict) and quote:
                            result = await session.execute(
                                select(Stock).where(Stock.ticker == ticker)
                            )
                            stock = result.scalar_one_or_none()
                            if stock:
                                await session.execute(
                                    update(Stock).where(Stock.ticker == ticker).values(
                                        current_price=quote.get("current_price"),
                                        previous_close=quote.get("previous_close"),
                                        volume=quote.get("volume"),
                                        market_cap=quote.get("market_cap"),
                                    )
                                )
                            else:
                                session.add(Stock(
                                    ticker=ticker,
                                    name=GLOBAL_TICKERS.get(ticker, ticker),
                                    current_price=quote.get("current_price"),
                                    previous_close=quote.get("previous_close"),
                                    currency=quote.get("currency", "USD"),
                                    exchange=quote.get("exchange", ""),
                                    market_cap=quote.get("market_cap"),
                                    volume=quote.get("volume"),
                                ))
                    await session.commit()
                    await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Error refreshing stock data: {e}")

    async def refresh_news_data(self):
        logger.info("Refreshing news data...")

    async def refresh_economic_data(self):
        logger.info("Refreshing economic data...")

    async def initial_load(self):
        logger.info("Starting initial data load...")
        try:
            from app.database import AsyncSessionLocal, Stock
            from sqlalchemy import select
            async with AsyncSessionLocal() as session:
                tickers = list(GLOBAL_TICKERS.items())
                for ticker, name in tickers[:20]:
                    try:
                        quote = await self.get_yahoo_quote(ticker)
                        if quote:
                            stock = Stock(
                                ticker=ticker, name=name,
                                current_price=quote.get("current_price"),
                                previous_close=quote.get("previous_close"),
                                currency=quote.get("currency", "USD"),
                                exchange=quote.get("exchange", ""),
                                market_cap=quote.get("market_cap"),
                                volume=quote.get("volume"),
                            )
                            session.add(stock)
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logger.error(f"Error loading {ticker}: {e}")

                for ticker, name in tickers[20:]:
                    result = await session.execute(select(Stock).where(Stock.ticker == ticker))
                    if not result.scalar_one_or_none():
                        session.add(Stock(ticker=ticker, name=name))

                await session.commit()
                logger.info(f"Initial load complete: {len(tickers)} tickers")
        except Exception as e:
            logger.error(f"Initial load error: {e}")

    async def close(self):
        await self.client.aclose()
