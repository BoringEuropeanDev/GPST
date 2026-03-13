"""
Data Ingestion Service - Aggregates data from all free/open APIs
Sources: Yahoo Finance, Alpha Vantage, FRED, World Bank, GDELT, NewsAPI, etc.

Fixes vs original:
  - Removed pg_insert: uses dialect-agnostic select-then-insert/update upsert
  - DataIngestionService is now a true singleton (module-level instance)
  - _cache eviction: max 500 entries, oldest dropped first
  - refresh_news_data / refresh_economic_data now actually do work
  - get_yahoo_history / get_yahoo_quote log exceptions properly
"""
import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import httpx

logger = logging.getLogger(__name__)

ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY", "demo")
NEWS_API_KEY      = os.getenv("NEWS_API_KEY", "")
MEDIASTACK_KEY    = os.getenv("MEDIASTACK_KEY", "")
FRED_API_KEY      = os.getenv("FRED_API_KEY", "")

GLOBAL_TICKERS = {
    # US Mega-cap tech
    "AAPL": "Apple Inc.", "MSFT": "Microsoft Corporation", "GOOGL": "Alphabet Inc.",
    "AMZN": "Amazon.com Inc.", "NVDA": "NVIDIA Corporation", "META": "Meta Platforms Inc.",
    "TSLA": "Tesla Inc.", "AVGO": "Broadcom Inc.", "ORCL": "Oracle Corporation",
    "ADBE": "Adobe Inc.", "CRM": "Salesforce Inc.", "AMD": "Advanced Micro Devices",
    "QCOM": "Qualcomm Inc.", "TXN": "Texas Instruments", "INTC": "Intel Corporation",
    "IBM": "IBM Corporation", "CSCO": "Cisco Systems",
    "NOW": "ServiceNow Inc.", "SNOW": "Snowflake Inc.", "PLTR": "Palantir Technologies",
    # US Financials
    "JPM": "JPMorgan Chase & Co.", "BAC": "Bank of America", "WFC": "Wells Fargo",
    "GS": "Goldman Sachs", "MS": "Morgan Stanley", "C": "Citigroup Inc.",
    "BLK": "BlackRock Inc.", "V": "Visa Inc.", "MA": "Mastercard Inc.",
    "PYPL": "PayPal Holdings", "SQ": "Block Inc.", "COIN": "Coinbase Global",
    # US Consumer / Retail
    "WMT": "Walmart Inc.", "COST": "Costco Wholesale", "HD": "Home Depot Inc.",
    "TGT": "Target Corporation", "MCD": "McDonald's Corporation",
    "SBUX": "Starbucks Corporation", "NKE": "Nike Inc.",
    "KO": "Coca-Cola Company", "PEP": "PepsiCo Inc.",
    "PG": "Procter & Gamble Co.", "PM": "Philip Morris", "MO": "Altria Group",
    # US Healthcare / Pharma
    "UNH": "UnitedHealth Group", "JNJ": "Johnson & Johnson", "LLY": "Eli Lilly and Co.",
    "ABBV": "AbbVie Inc.", "MRK": "Merck & Co.", "PFE": "Pfizer Inc.",
    "MRNA": "Moderna Inc.", "BIIB": "Biogen Inc.", "GILD": "Gilead Sciences",
    "REGN": "Regeneron Pharmaceuticals", "TMO": "Thermo Fisher Scientific",
    # US Energy
    "XOM": "Exxon Mobil Corp.", "CVX": "Chevron Corporation",
    "SLB": "Schlumberger Limited", "HAL": "Halliburton Company",
    "MPC": "Marathon Petroleum", "PSX": "Phillips 66", "VLO": "Valero Energy",
    # US Industrials / Materials / Diversified
    "FCX": "Freeport-McMoRan", "NEM": "Newmont Corporation", "AA": "Alcoa Corporation",
    "UPS": "United Parcel Service", "FDX": "FedEx Corporation",
    "ACN": "Accenture plc", "BRK-B": "Berkshire Hathaway",
    # Growth / Tech mid-cap
    "NFLX": "Netflix Inc.", "UBER": "Uber Technologies", "SPOT": "Spotify Technology",
    "SHOP": "Shopify Inc.", "CRWD": "CrowdStrike Holdings",
    "DDOG": "Datadog Inc.", "ZS": "Zscaler Inc.", "NET": "Cloudflare Inc.",
    # ETFs
    "SPY": "SPDR S&P 500 ETF", "QQQ": "Invesco QQQ Trust", "IWM": "iShares Russell 2000",
    "DIA": "SPDR Dow Jones ETF", "VTI": "Vanguard Total Stock Market",
    "GLD": "SPDR Gold Shares", "SLV": "iShares Silver Trust",
    "TLT": "iShares 20+ Year Treasury", "HYG": "iShares High Yield Bond",
    # International ADRs
    "TSM": "Taiwan Semiconductor", "BABA": "Alibaba Group", "TM": "Toyota Motor",
    "SONY": "Sony Group Corporation", "NVO": "Novo Nordisk", "ASML": "ASML Holding",
    "SAP": "SAP SE", "HSBC": "HSBC Holdings", "BP": "BP plc", "RIO": "Rio Tinto",
    "BHP": "BHP Group", "VALE": "Vale S.A.", "BBVA": "Banco Bilbao Vizcaya",
    "SAN": "Banco Santander", "UBS": "UBS Group",
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

_CACHE_MAX = 500   # max entries before eviction


class DataIngestionService:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        self._cache: Dict[str, object] = {}
        self._cache_ttl: Dict[str, float] = {}

    # ── Cache helpers ─────────────────────────────────────────────────────────

    def _cache_get(self, url: str) -> Optional[object]:
        now = datetime.now().timestamp()
        if url in self._cache and self._cache_ttl.get(url, 0) > now:
            return self._cache[url]
        return None

    def _cache_set(self, url: str, data: object, ttl_seconds: int):
        # Evict oldest entries when cache is full
        if len(self._cache) >= _CACHE_MAX:
            oldest = min(self._cache_ttl, key=self._cache_ttl.get)
            self._cache.pop(oldest, None)
            self._cache_ttl.pop(oldest, None)
        self._cache[url] = data
        self._cache_ttl[url] = datetime.now().timestamp() + ttl_seconds

    async def _cached_get(self, url: str, ttl_seconds: int = 300) -> Optional[Dict]:
        cached = self._cache_get(url)
        if cached is not None:
            return cached
        try:
            resp = await self.client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                self._cache_set(url, data, ttl_seconds)
                return data
            logger.warning(f"HTTP {resp.status_code} fetching {url}")
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
        return None

    # ── Yahoo Finance ─────────────────────────────────────────────────────────

    async def get_yahoo_quote(self, ticker: str) -> Optional[Dict]:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1d"
        try:
            resp = await self.client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200:
                data = resp.json()
                meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
                return {
                    "ticker":         ticker,
                    "name":           GLOBAL_TICKERS.get(ticker, meta.get("longName", ticker)),
                    "current_price":  meta.get("regularMarketPrice"),
                    "previous_close": meta.get("previousClose") or meta.get("chartPreviousClose"),
                    "currency":       meta.get("currency", "USD"),
                    "exchange":       meta.get("exchangeName", ""),
                    "market_cap":     meta.get("marketCap"),
                    "volume":         meta.get("regularMarketVolume"),
                }
            logger.warning(f"Yahoo quote HTTP {resp.status_code} for {ticker}")
        except Exception as e:
            logger.error(f"Yahoo quote error for {ticker}: {e}")
        return None

    async def get_yahoo_history(self, ticker: str, period: str = "1y") -> List[Dict]:
        range_map = {"5d": "5d", "1mo": "1mo", "3mo": "3mo", "6mo": "6mo", "1y": "1y", "2y": "2y", "5y": "5y"}
        range_str = range_map.get(period, "1y")
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range={range_str}"
        try:
            resp = await self.client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200:
                data       = resp.json()
                result     = data.get("chart", {}).get("result", [])
                if not result:
                    return []
                timestamps = result[0].get("timestamp", [])
                quotes     = result[0].get("indicators", {}).get("quote", [{}])[0]
                adj_close  = result[0].get("indicators", {}).get("adjclose", [{}])[0].get("adjclose", [])
                prices     = []
                for i, ts in enumerate(timestamps):
                    closes = quotes.get("close", [])
                    if closes and i < len(closes) and closes[i]:
                        prices.append({
                            "date":          datetime.fromtimestamp(ts).isoformat(),
                            "open":          quotes.get("open",   [])[i] if i < len(quotes.get("open",   [])) else None,
                            "high":          quotes.get("high",   [])[i] if i < len(quotes.get("high",   [])) else None,
                            "low":           quotes.get("low",    [])[i] if i < len(quotes.get("low",    [])) else None,
                            "close":         closes[i],
                            "volume":        quotes.get("volume", [])[i] if i < len(quotes.get("volume", [])) else None,
                            "adjusted_close":adj_close[i] if i < len(adj_close) else None,
                        })
                return prices
            logger.warning(f"Yahoo history HTTP {resp.status_code} for {ticker}")
        except Exception as e:
            logger.error(f"Yahoo history error for {ticker}: {e}")
        return []

    async def get_yahoo_profile(self, ticker: str) -> Optional[Dict]:
        url = (
            f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
            f"?modules=assetProfile,summaryDetail,defaultKeyStatistics"
        )
        try:
            resp = await self.client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200:
                data    = resp.json()
                result  = data.get("quoteSummary", {}).get("result", [{}])[0]
                profile = result.get("assetProfile", {})
                summary = result.get("summaryDetail", {})
                stats   = result.get("defaultKeyStatistics", {})
                return {
                    "sector":             profile.get("sector"),
                    "industry":           profile.get("industry"),
                    "description":        profile.get("longBusinessSummary"),
                    "website":            profile.get("website"),
                    "country":            profile.get("country"),
                    "employees":          profile.get("fullTimeEmployees"),
                    "pe_ratio":           summary.get("trailingPE", {}).get("raw"),
                    "dividend_yield":     summary.get("dividendYield", {}).get("raw"),
                    "fifty_two_week_high":summary.get("fiftyTwoWeekHigh", {}).get("raw"),
                    "fifty_two_week_low": summary.get("fiftyTwoWeekLow", {}).get("raw"),
                    "avg_volume":         summary.get("averageVolume", {}).get("raw"),
                    "beta":               stats.get("beta", {}).get("raw"),
                    "market_cap":         summary.get("marketCap", {}).get("raw"),
                    "forward_pe":         summary.get("forwardPE", {}).get("raw"),
                    "profit_margins":     stats.get("profitMargins", {}).get("raw"),
                }
            logger.warning(f"Yahoo profile HTTP {resp.status_code} for {ticker}: partial data returned")
        except Exception as e:
            logger.error(f"Yahoo profile error for {ticker}: {e}")
        return None

    # ── News & sentiment ──────────────────────────────────────────────────────

    async def get_alpha_vantage_news(self, ticker: str) -> List[Dict]:
        if not ALPHA_VANTAGE_KEY or ALPHA_VANTAGE_KEY == "demo":
            return []
        url = (
            f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT"
            f"&tickers={ticker}&apikey={ALPHA_VANTAGE_KEY}&limit=10"
        )
        try:
            data = await self._cached_get(url, ttl_seconds=3600)
            if data and "feed" in data:
                items = []
                for article in data["feed"][:10]:
                    sentiment = 0.0
                    for ts in article.get("ticker_sentiment", []):
                        if ts.get("ticker") == ticker:
                            sentiment = float(ts.get("ticker_sentiment_score", 0))
                    items.append({
                        "title":        article.get("title"),
                        "summary":      article.get("summary"),
                        "url":          article.get("url"),
                        "source":       article.get("source"),
                        "published_at": article.get("time_published"),
                        "sentiment":    sentiment,
                    })
                return items
        except Exception as e:
            logger.error(f"Alpha Vantage news error: {e}")
        return []

    async def get_gdelt_news(self, query: str) -> List[Dict]:
        url = (
            f"https://api.gdeltproject.org/api/v2/doc/doc"
            f"?query={query}&mode=artlist&maxrecords=10&format=json&sort=datedesc"
        )
        try:
            data = await self._cached_get(url, ttl_seconds=1800)
            if data and "articles" in data:
                return [{
                    "title":        a.get("title"),
                    "url":          a.get("url"),
                    "source":       a.get("domain"),
                    "published_at": a.get("seendate"),
                    "sentiment":    0,
                    "summary":      "",
                } for a in data["articles"][:10]]
        except Exception as e:
            logger.error(f"GDELT error: {e}")
        return []

    async def get_newsapi_headlines(self, query: str) -> List[Dict]:
        if not NEWS_API_KEY:
            return []
        url = f"https://newsapi.org/v2/everything?q={query}&sortBy=publishedAt&pageSize=5&apiKey={NEWS_API_KEY}"
        try:
            data = await self._cached_get(url, ttl_seconds=3600)
            if data and data.get("status") == "ok":
                return [{
                    "title":        a.get("title"),
                    "summary":      a.get("description"),
                    "url":          a.get("url"),
                    "source":       a.get("source", {}).get("name"),
                    "published_at": a.get("publishedAt"),
                    "sentiment":    0,
                } for a in data.get("articles", [])[:5]]
        except Exception as e:
            logger.error(f"NewsAPI error: {e}")
        return []

    async def get_gdelt_geopolitical_events(self) -> List[Dict]:
        url = (
            "https://api.gdeltproject.org/api/v2/gkg/gkg"
            "?query=conflict+OR+war+OR+sanctions+OR+trade&mode=artlist&maxrecords=20&format=json"
        )
        try:
            data = await self._cached_get(url, ttl_seconds=3600)
            if data and "articles" in data:
                return [{
                    "title":        a.get("title", ""),
                    "url":          a.get("url", ""),
                    "source":       a.get("domain", ""),
                    "date":         a.get("seendate", ""),
                    "event_type":   "geopolitical",
                    "impact_score": 0.5,
                } for a in data.get("articles", [])[:20]]
        except Exception as e:
            logger.error(f"GDELT geopolitical error: {e}")
        return []

    # ── Economic data ─────────────────────────────────────────────────────────

    async def get_fred_data(self, series_id: str, limit: int = 100) -> List[Dict]:
        api_key = FRED_API_KEY if FRED_API_KEY else "abcdefghijklmnopqrstuvwxyz123456"
        url = (
            f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series_id}&sort_order=desc&limit={limit}&file_type=json&api_key={api_key}"
        )
        try:
            data = await self._cached_get(url, ttl_seconds=86400)
            if data and "observations" in data:
                return [{
                    "date":   obs["date"],
                    "value":  float(obs["value"]) if obs["value"] != "." else None,
                    "series": series_id,
                } for obs in data["observations"] if obs["value"] != "."]
        except Exception as e:
            logger.error(f"FRED error for {series_id}: {e}")
        return []

    async def get_world_bank_data(self, indicator: str, country: str = "US") -> List[Dict]:
        url = (
            f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
            f"?format=json&per_page=10&mrv=10"
        )
        try:
            data = await self._cached_get(url, ttl_seconds=86400 * 7)
            if data and len(data) > 1 and data[1]:
                return [{
                    "date":      item.get("date"),
                    "value":     item.get("value"),
                    "indicator": item.get("indicator", {}).get("value"),
                    "country":   item.get("country", {}).get("value"),
                } for item in data[1] if item.get("value") is not None]
        except Exception as e:
            logger.error(f"World Bank error: {e}")
        return []

    # ── Sector & commodity data ───────────────────────────────────────────────

    async def get_sector_etf_data(self) -> Dict:
        sector_etfs = {
            "XLK": "Technology", "XLV": "Healthcare", "XLF": "Financials",
            "XLY": "Consumer Discretionary", "XLP": "Consumer Staples",
            "XLE": "Energy", "XLB": "Materials", "XLI": "Industrials",
            "XLU": "Utilities", "XLRE": "Real Estate", "XLC": "Communication Services",
        }
        results = {}
        tasks  = [self.get_yahoo_quote(etf) for etf in sector_etfs]
        quotes = await asyncio.gather(*tasks, return_exceptions=True)
        for etf, quote in zip(sector_etfs, quotes):
            if isinstance(quote, dict) and quote:
                sector     = sector_etfs[etf]
                prev       = quote.get("previous_close") or quote.get("current_price", 1)
                curr       = quote.get("current_price", 0)
                pct_change = ((curr - prev) / prev * 100) if prev else 0
                results[sector] = {"etf": etf, "price": curr, "change_pct": pct_change, "name": sector}
        return results

    async def get_commodity_prices(self) -> Dict:
        commodities = {
            "CL=F": "Crude Oil WTI", "GC=F": "Gold", "SI=F": "Silver",
            "NG=F": "Natural Gas", "ZC=F": "Corn", "ZW=F": "Wheat",
            "BTC-USD": "Bitcoin", "ETH-USD": "Ethereum",
        }
        results = {}
        tasks  = [self.get_yahoo_quote(sym) for sym in commodities]
        quotes = await asyncio.gather(*tasks, return_exceptions=True)
        for sym, quote in zip(commodities, quotes):
            if isinstance(quote, dict) and quote:
                results[sym] = {"name": commodities[sym], "price": quote.get("current_price"), "change": None}
        return results

    # ── Scheduled refresh methods (previously stubs) ──────────────────────────

    async def refresh_stock_data(self):
        """Refresh current price / volume for all active tickers."""
        logger.info("Refreshing stock data...")
        try:
            from app.database import AsyncSessionLocal, Stock
            from sqlalchemy import select, update

            async with AsyncSessionLocal() as session:
                tickers = list(GLOBAL_TICKERS.keys())
                for i in range(0, min(len(tickers), 50), 10):
                    batch  = tickers[i:i + 10]
                    tasks  = [self.get_yahoo_quote(t) for t in batch]
                    quotes = await asyncio.gather(*tasks, return_exceptions=True)

                    for ticker, quote in zip(batch, quotes):
                        if not isinstance(quote, dict) or not quote:
                            continue
                        result = await session.execute(select(Stock).where(Stock.ticker == ticker))
                        stock  = result.scalar_one_or_none()
                        if stock:
                            await session.execute(
                                update(Stock).where(Stock.ticker == ticker).values(
                                    current_price  = quote.get("current_price"),
                                    previous_close = quote.get("previous_close"),
                                    volume         = quote.get("volume"),
                                    market_cap     = quote.get("market_cap"),
                                )
                            )
                        else:
                            session.add(Stock(
                                ticker         = ticker,
                                name           = GLOBAL_TICKERS.get(ticker, ticker),
                                current_price  = quote.get("current_price"),
                                previous_close = quote.get("previous_close"),
                                currency       = quote.get("currency", "USD"),
                                exchange       = quote.get("exchange", ""),
                                market_cap     = quote.get("market_cap"),
                                volume         = quote.get("volume"),
                            ))
                    await session.commit()
                    await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error refreshing stock data: {e}")

    async def refresh_news_data(self):
        """Store latest GDELT headlines for a few key tickers."""
        logger.info("Refreshing news data...")
        try:
            from app.database import AsyncSessionLocal, NewsItem
            from sqlalchemy import select

            key_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "SPY"]
            async with AsyncSessionLocal() as session:
                for ticker in key_tickers:
                    articles = await self.get_gdelt_news(ticker)
                    for a in articles[:5]:
                        if not a.get("url"):
                            continue
                        exists = await session.execute(
                            select(NewsItem).where(NewsItem.url == a["url"]).limit(1)
                        )
                        if exists.scalar_one_or_none():
                            continue
                        pub = None
                        if a.get("published_at"):
                            try:
                                pub = datetime.strptime(str(a["published_at"])[:14], "%Y%m%d%H%M%S")
                            except Exception:
                                pass
                        session.add(NewsItem(
                            ticker       = ticker,
                            title        = a.get("title", "")[:500],
                            url          = a["url"][:1000],
                            source       = a.get("source", "")[:100],
                            published_at = pub,
                            sentiment    = 0.0,
                        ))
                    await asyncio.sleep(0.5)
                await session.commit()
        except Exception as e:
            logger.error(f"Error refreshing news data: {e}")

    async def refresh_economic_data(self):
        """Persist latest FRED observations to EconomicIndicator table."""
        logger.info("Refreshing economic data...")
        try:
            from app.database import AsyncSessionLocal, EconomicIndicator
            from sqlalchemy import select

            priority = ["VIXCLS", "T10Y2Y", "FEDFUNDS", "CPIAUCSL", "UNRATE", "GDP"]
            async with AsyncSessionLocal() as session:
                for series_id in priority:
                    obs_list = await self.get_fred_data(series_id, limit=3)
                    for obs in obs_list:
                        if obs.get("value") is None:
                            continue
                        obs_date = None
                        try:
                            obs_date = datetime.strptime(obs["date"], "%Y-%m-%d")
                        except Exception:
                            continue
                        exists = await session.execute(
                            select(EconomicIndicator)
                            .where(
                                EconomicIndicator.indicator == series_id,
                                EconomicIndicator.date == obs_date,
                            )
                            .limit(1)
                        )
                        if exists.scalar_one_or_none():
                            continue
                        session.add(EconomicIndicator(
                            indicator = series_id,
                            date      = obs_date,
                            value     = obs["value"],
                            source    = "FRED",
                        ))
                await session.commit()
        except Exception as e:
            logger.error(f"Error refreshing economic data: {e}")

    async def initial_load(self):
        """Best-effort seed of the stocks table on first boot."""
        logger.info("Starting initial data load...")
        try:
            from app.database import AsyncSessionLocal, Stock
            from sqlalchemy import select

            async with AsyncSessionLocal() as session:
                tickers = list(GLOBAL_TICKERS.items())

                for ticker, name in tickers[:20]:
                    try:
                        quote = await self.get_yahoo_quote(ticker)
                        result = await session.execute(select(Stock).where(Stock.ticker == ticker))
                        existing = result.scalar_one_or_none()

                        if existing:
                            if quote:
                                existing.current_price  = quote.get("current_price")
                                existing.previous_close = quote.get("previous_close")
                                existing.volume         = quote.get("volume")
                                existing.market_cap     = quote.get("market_cap")
                        else:
                            session.add(Stock(
                                ticker         = ticker,
                                name           = name,
                                current_price  = quote.get("current_price")  if quote else None,
                                previous_close = quote.get("previous_close") if quote else None,
                                currency       = quote.get("currency", "USD")if quote else "USD",
                                exchange       = quote.get("exchange", "")   if quote else "",
                                market_cap     = quote.get("market_cap")     if quote else None,
                                volume         = quote.get("volume")         if quote else None,
                            ))
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        logger.error(f"Error loading {ticker}: {e}")

                # Insert remaining tickers with no price data if missing
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


# ── Module-level singleton ─────────────────────────────────────────────────────
# Import this instead of creating a new instance per call:
#   from app.services.data_ingestion import ingestion_service
ingestion_service = DataIngestionService()
