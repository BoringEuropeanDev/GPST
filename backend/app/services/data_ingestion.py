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
