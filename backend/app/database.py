"""
Database configuration using SQLAlchemy async with PostgreSQL/SQLite
"""
import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Boolean, JSON, Index
from sqlalchemy.sql import func

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite+aiosqlite:///./stockterminal.db"
)

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_size=10 if "postgresql" in DATABASE_URL else 1,
    max_overflow=20 if "postgresql" in DATABASE_URL else 0,
)

AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class Stock(Base):
    __tablename__ = "stocks"
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(20), unique=True, index=True, nullable=False)
    name = Column(String(255))
    exchange = Column(String(50))
    sector = Column(String(100))
    industry = Column(String(100))
    country = Column(String(50))
    currency = Column(String(10))
    current_price = Column(Float)
    previous_close = Column(Float)
    market_cap = Column(Float)
    pe_ratio = Column(Float)
    dividend_yield = Column(Float)
    fifty_two_week_high = Column(Float)
    fifty_two_week_low = Column(Float)
    volume = Column(Float)
    avg_volume = Column(Float)
    description = Column(Text)
    website = Column(String(255))
    logo_url = Column(String(500))
    employees = Column(Integer)
    founded = Column(String(20))
    last_updated = Column(DateTime, server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)

class StockPrice(Base):
    __tablename__ = "stock_prices"
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(20), index=True, nullable=False)
    date = Column(DateTime, index=True, nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    adjusted_close = Column(Float)
    __table_args__ = (Index('ix_stock_price_ticker_date', 'ticker', 'date'),)

class Prediction(Base):
    __tablename__ = "predictions"
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(20), index=True, nullable=False)
    prediction_date = Column(DateTime, index=True, nullable=False)
    target_date = Column(DateTime, nullable=False)
    predicted_direction = Column(String(10))
    predicted_change_pct = Column(Float)
    confidence = Column(Float)
    probability_up = Column(Float)
    probability_down = Column(Float)
    probability_neutral = Column(Float)
    rationale = Column(Text)
    sources_used = Column(JSON)
    features_used = Column(JSON)
    model_version = Column(String(50))
    actual_direction = Column(String(10))
    actual_change_pct = Column(Float)
    was_correct = Column(Boolean)
    created_at = Column(DateTime, server_default=func.now())

class NewsItem(Base):
    __tablename__ = "news_items"
    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(20), index=True)
    title = Column(String(500))
    summary = Column(Text)
    url = Column(String(1000))
    source = Column(String(100))
    published_at = Column(DateTime, index=True)
    sentiment = Column(Float)
    relevance_score = Column(Float)
    created_at = Column(DateTime, server_default=func.now())

class EconomicIndicator(Base):
    __tablename__ = "economic_indicators"
    id = Column(Integer, primary_key=True, index=True)
    indicator = Column(String(100), index=True)
    date = Column(DateTime, index=True)
    value = Column(Float)
    source = Column(String(50))
    created_at = Column(DateTime, server_default=func.now())

class GeopoliticalEvent(Base):
    __tablename__ = "geopolitical_events"
    id = Column(Integer, primary_key=True, index=True)
    event_type = Column(String(100))
    country = Column(String(100))
    description = Column(Text)
    impact_score = Column(Float)
    event_date = Column(DateTime, index=True)
    source = Column(String(100))
    sectors_affected = Column(JSON)
    created_at = Column(DateTime, server_default=func.now())

class SectorPerformance(Base):
    __tablename__ = "sector_performance"
    id = Column(Integer, primary_key=True, index=True)
    sector = Column(String(100), index=True)
    date = Column(DateTime, index=True)
    performance_pct = Column(Float)
    volume = Column(Float)
    top_movers = Column(JSON)
    created_at = Column(DateTime, server_default=func.now())

class ModelMetrics(Base):
    __tablename__ = "model_metrics"
    id = Column(Integer, primary_key=True, index=True)
    model_version = Column(String(50))
    evaluation_date = Column(DateTime)
    accuracy = Column(Float)
    precision = Column(Float)
    recall = Column(Float)
    f1_score = Column(Float)
    total_predictions = Column(Integer)
    correct_predictions = Column(Integer)
    metrics_detail = Column(JSON)
    created_at = Column(DateTime, server_default=func.now())

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
