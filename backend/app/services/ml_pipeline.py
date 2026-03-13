"""
ML Pipeline for GPST — Feature engineering, training, inference.
Two models: RandomForestClassifier (Model A) + LogisticRegression (Model B).
All heavy work is synchronous (pandas/sklearn); call via asyncio.to_thread().

v3 upgrades:
  - Parquet price cache  → prices saved to backend/app/dataset_store/prices/
    on first run a full 2-year history is downloaded; subsequent nightly runs
    only fetch the last ~7 trading days and merge, so the training job goes
    from ~80 Yahoo HTTP calls down to ~5-10 incremental rows per ticker.
  - ALL_TICKERS covers every symbol available on the site (not just 30).
  - Market context features: SPY, QQQ, ^VIX merged into every row.
      spy_ret_1d, spy_ret_5d, qqq_ret_1d, vix_level, vix_change_1d
  - Total features: 15 ticker-level + 5 market = 20.

File layout:
    backend/app/models/
        tree_model.pkl
        logreg_model.pkl
        scaler.pkl
        feature_meta.json
    backend/app/dataset_store/prices/
        AAPL.parquet
        MSFT.parquet
        ...
        _VIX.parquet    ← ^VIX stored with ^ replaced by _
"""
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import joblib

from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, roc_auc_score

logger = logging.getLogger(__name__)

# ─── Paths ─────────────────────────────────────────────────────────────────────
_BASE       = Path(__file__).parent.parent
MODEL_DIR   = _BASE / "models"
DATASET_DIR = _BASE / "dataset_store" / "prices"

MODEL_DIR.mkdir(exist_ok=True)
DATASET_DIR.mkdir(parents=True, exist_ok=True)

TREE_PATH   = MODEL_DIR / "tree_model.pkl"
LOGREG_PATH = MODEL_DIR / "logreg_model.pkl"
SCALER_PATH = MODEL_DIR / "scaler.pkl"
META_PATH   = MODEL_DIR / "feature_meta.json"

# ─── ALL tickers available on the site ────────────────────────────────────────
# Keep in sync with whatever GLOBAL_TICKERS / the stocks table contains.
ALL_TICKERS = [
    # Mega-cap US tech
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
    "AVGO", "ORCL", "ADBE", "CRM", "AMD", "QCOM", "TXN", "INTC",
    "IBM", "CSCO", "NOW", "SNOW", "PLTR",
    # US financials
    "JPM", "BAC", "WFC", "GS", "MS", "C", "BLK", "V", "MA", "PYPL", "SQ", "COIN",
    # US consumer / retail
    "WMT", "COST", "HD", "TGT", "MCD", "SBUX", "NKE",
    "KO", "PEP", "PG", "PM", "MO",
    # US healthcare / pharma
    "UNH", "JNJ", "LLY", "ABBV", "MRK", "PFE", "MRNA", "BIIB", "GILD", "REGN", "TMO",
    # US energy
    "XOM", "CVX", "SLB", "HAL", "MPC", "PSX", "VLO",
    # US industrials / materials
    "FCX", "NEM", "AA", "RIO", "UPS", "FDX", "ACN",
    # US diversified
    "BRK-B",
    # Growth / tech mid-cap
    "NFLX", "UBER", "SPOT", "SHOP", "CRWD", "DDOG", "ZS", "NET",
    # ETFs
    "SPY", "QQQ", "IWM", "DIA", "VTI", "GLD", "SLV", "TLT", "HYG",
    # International ADRs
    "TSM", "BABA", "TM", "SONY", "NVO", "ASML", "SAP", "HSBC",
    "BP", "BHP", "VALE", "BBVA", "SAN", "UBS",
]

# Market-context series fetched separately and merged as features — not trained on
MARKET_TICKERS = ["SPY", "QQQ"]
VIX_TICKER     = "^VIX"

# ─── Feature columns ───────────────────────────────────────────────────────────
TICKER_FEATURE_COLS = [
    "ret_1d", "ret_5d", "ret_10d", "ret_20d",
    "vol_5d", "vol_20d",
    "ma_5", "ma_20", "ma_50",
    "ma_5_vs_20", "ma_20_vs_50",
    "rsi_14", "bb_position",
    "volume_zscore_20", "volume_ratio",
]

MARKET_FEATURE_COLS = [
    "spy_ret_1d", "spy_ret_5d",
    "qqq_ret_1d",
    "vix_level", "vix_change_1d",
]

FEATURE_COLS = TICKER_FEATURE_COLS + MARKET_FEATURE_COLS   # 20 total


# ─── Parquet cache helpers ─────────────────────────────────────────────────────

def _cache_path(ticker: str) -> Path:
    """^VIX → _VIX.parquet, BRK-B → BRK-B.parquet"""
    safe = ticker.replace("^", "_").replace("/", "_")
    return DATASET_DIR / f"{safe}.parquet"


def load_cached(ticker: str) -> Optional[pd.DataFrame]:
    """Load parquet cache for ticker. Returns None if cache does not exist."""
    p = _cache_path(ticker)
    if not p.exists():
        return None
    try:
        df = pd.read_parquet(p)
        df["date"] = pd.to_datetime(df["date"])
        return df.sort_values("date").reset_index(drop=True)
    except Exception as e:
        logger.warning(f"Cache read failed for {ticker}: {e}")
        return None


def save_cached(ticker: str, df: pd.DataFrame) -> None:
    """Persist ticker DataFrame to parquet cache."""
    try:
        df.to_parquet(_cache_path(ticker), index=False)
    except Exception as e:
        logger.warning(f"Cache write failed for {ticker}: {e}")


def merge_and_cache(ticker: str, new_rows: pd.DataFrame) -> pd.DataFrame:
    """
    Append new_rows to existing cache, deduplicate on date, re-save.
    Returns the full merged DataFrame.
    """
    existing = load_cached(ticker)
    if existing is None or existing.empty:
        merged = new_rows.copy()
    else:
        merged = pd.concat([existing, new_rows], ignore_index=True)
        merged["date"] = pd.to_datetime(merged["date"])
        merged = merged.drop_duplicates(subset=["date"], keep="last")
        merged = merged.sort_values("date").reset_index(drop=True)
    save_cached(ticker, merged)
    return merged


def cache_is_fresh(ticker: str, max_age_days: int = 1) -> bool:
    """
    True if cache exists AND the most recent date is within max_age_days of today.
    Used by train_models() to skip the full 2y download on subsequent runs.
    """
    df = load_cached(ticker)
    if df is None or df.empty:
        return False
    latest  = pd.to_datetime(df["date"].max())
    cutoff  = pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=max_age_days)
    return latest >= cutoff


# ─── Feature engineering ───────────────────────────────────────────────────────

def _rsi(closes: pd.Series, period: int = 14) -> pd.Series:
    delta = closes.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _build_ticker_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute TICKER_FEATURE_COLS + target 'y', indexed by date.
    df must have columns: date (datetime), close (float), volume (float).
    """
    df = df.sort_values("date").reset_index(drop=True)
    c  = df["close"]
    v  = df["volume"].fillna(0)

    out         = pd.DataFrame(index=df.index)
    out["date"] = df["date"]

    # Returns
    out["ret_1d"]  = c.pct_change(1)
    out["ret_5d"]  = c.pct_change(5)
    out["ret_10d"] = c.pct_change(10)
    out["ret_20d"] = c.pct_change(20)

    # Volatility
    daily_ret      = c.pct_change()
    out["vol_5d"]  = daily_ret.rolling(5).std()
    out["vol_20d"] = daily_ret.rolling(20).std()

    # Moving averages — price-normalised so they generalise across tickers/decades
    ma5  = c.rolling(5).mean()
    ma20 = c.rolling(20).mean()
    ma50 = c.rolling(50).mean()
    out["ma_5"]        = ma5  / c
    out["ma_20"]       = ma20 / c
    out["ma_50"]       = ma50 / c
    out["ma_5_vs_20"]  = ma5  / ma20.replace(0, np.nan)
    out["ma_20_vs_50"] = ma20 / ma50.replace(0, np.nan)

    # RSI(14)
    out["rsi_14"] = _rsi(c, 14)

    # Bollinger Band position [0, 1]
    bb_mid   = c.rolling(20).mean()
    bb_std   = c.rolling(20).std()
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std
    out["bb_position"] = (c - bb_lower) / (bb_upper - bb_lower).replace(0, np.nan)

    # Volume features
    vol_mean20 = v.rolling(20).mean()
    vol_std20  = v.rolling(20).std().replace(0, np.nan)
    out["volume_zscore_20"] = (v - vol_mean20) / vol_std20
    vol_mean5 = v.rolling(5).mean()
    out["volume_ratio"] = vol_mean5 / vol_mean20.replace(0, np.nan)

    # Target: next-day UP (1) or DOWN (0)
    out["y"] = (c.shift(-1) > c).astype(int)

    return out.set_index("date")


def build_market_features(
    spy_df: pd.DataFrame,
    qqq_df: pd.DataFrame,
    vix_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build market context feature frame indexed by date.
    Any missing series is filled with 0 so training always has 20 features.
    """
    spy_c = spy_df.sort_values("date").set_index("date")["close"]
    mkt   = pd.DataFrame(index=spy_c.index)

    mkt["spy_ret_1d"] = spy_c.pct_change(1)
    mkt["spy_ret_5d"] = spy_c.pct_change(5)

    if qqq_df is not None and not qqq_df.empty:
        qqq_c = qqq_df.sort_values("date").set_index("date")["close"]
        mkt["qqq_ret_1d"] = qqq_c.pct_change(1).reindex(mkt.index)
    else:
        mkt["qqq_ret_1d"] = 0.0

    if vix_df is not None and not vix_df.empty:
        vix_c = vix_df.sort_values("date").set_index("date")["close"].reindex(mkt.index).ffill()
        mkt["vix_level"]     = vix_c / 100.0   # normalise: VIX 20 → 0.20
        mkt["vix_change_1d"] = vix_c.pct_change(1)
    else:
        mkt["vix_level"]     = 0.0
        mkt["vix_change_1d"] = 0.0

    return mkt.fillna(0.0)


def build_features_for_series(
    df: pd.DataFrame,
    market_features: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Full feature frame for one ticker.
    df: columns date, close, volume — sorted ascending.
    market_features: optional DataFrame indexed by date with MARKET_FEATURE_COLS.
    Returns DataFrame with FEATURE_COLS + 'y', NaN rows dropped, date as column.
    """
    ticker_feat = _build_ticker_features(df)   # indexed by date

    if market_features is not None and not market_features.empty:
        ticker_feat = ticker_feat.join(market_features[MARKET_FEATURE_COLS], how="left")
        ticker_feat[MARKET_FEATURE_COLS] = ticker_feat[MARKET_FEATURE_COLS].fillna(0.0)
    else:
        for col in MARKET_FEATURE_COLS:
            ticker_feat[col] = 0.0

    ticker_feat = ticker_feat.iloc[:-1]   # drop last row — no next-day close for target
    ticker_feat = ticker_feat.dropna(subset=FEATURE_COLS + ["y"])
    return ticker_feat.reset_index()       # bring date back as a column


# ─── Training frame builder ────────────────────────────────────────────────────

def build_training_frame(
    price_data: Dict[str, pd.DataFrame],
    market_features: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    price_data: { ticker: DataFrame(date, close, volume) }
    market_features: DataFrame indexed by date with MARKET_FEATURE_COLS.
    Returns pooled, chronologically-sorted feature frame across all tickers.
    """
    frames  = []
    skipped = 0

    for ticker, df in price_data.items():
        n = len(df) if df is not None else 0
        if n < 60:
            logger.warning(f"[train] Skipping {ticker}: only {n} rows")
            skipped += 1
            continue
        try:
            feat           = build_features_for_series(df, market_features)
            feat["ticker"] = ticker
            frames.append(feat)
        except Exception as e:
            logger.warning(f"[train] Feature build failed for {ticker}: {e}")
            skipped += 1

    if not frames:
        raise ValueError("No training data could be built from any ticker.")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values("date").reset_index(drop=True)

    logger.info(
        f"Training frame: {len(combined):,} rows across "
        f"{len(frames)} tickers ({skipped} skipped)"
    )
    return combined


# ─── Model training ────────────────────────────────────────────────────────────

def train_models_sync(frame: pd.DataFrame, train_frac: float = 0.80) -> Dict:
    """
    Synchronous training. Returns dict with models + metrics.
    Call via asyncio.to_thread() from async code.
    """
    X = frame[FEATURE_COLS].values
    y = frame["y"].values

    split = int(len(X) * train_frac)
    X_train, X_val = X[:split], X[split:]
    y_train, y_val = y[:split], y[split:]

    if len(X_train) < 100 or len(X_val) < 20:
        raise ValueError(f"Not enough data: train={len(X_train)}, val={len(X_val)}")

    # Scaler — fit only on training portion to prevent leakage
    scaler    = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_val_s   = scaler.transform(X_val)

    # Model A: RandomForest
    tree = RandomForestClassifier(
        n_estimators=300,
        max_depth=10,
        min_samples_leaf=15,
        max_features="sqrt",
        class_weight="balanced",
        n_jobs=-1,
        random_state=42,
    )
    tree.fit(X_train, y_train)
    tree_proba_val = tree.predict_proba(X_val)[:, 1]
    tree_pred_val  = (tree_proba_val > 0.5).astype(int)

    # Model B: LogisticRegression
    logreg = LogisticRegression(
        C=0.1, solver="lbfgs", max_iter=500,
        class_weight="balanced", random_state=42,
    )
    logreg.fit(X_train_s, y_train)
    logreg_proba_val = logreg.predict_proba(X_val_s)[:, 1]
    logreg_pred_val  = (logreg_proba_val > 0.5).astype(int)

    # Ensemble: 60% RF + 40% LR
    ens_proba = 0.6 * tree_proba_val + 0.4 * logreg_proba_val
    ens_pred  = (ens_proba > 0.5).astype(int)

    def _metrics(y_true, y_pred, y_prob, name):
        acc = accuracy_score(y_true, y_pred)
        try:
            auc = roc_auc_score(y_true, y_prob)
        except Exception:
            auc = 0.5
        up_mask   = y_true == 1
        down_mask = y_true == 0
        hit_up    = accuracy_score(y_true[up_mask],   y_pred[up_mask])   if up_mask.any()   else 0.0
        hit_down  = accuracy_score(y_true[down_mask], y_pred[down_mask]) if down_mask.any() else 0.0
        logger.info(
            f"[{name}] val_acc={acc:.3f}  val_auc={auc:.3f}  "
            f"hit_up={hit_up:.3f}  hit_down={hit_down:.3f}  n={len(y_true):,}"
        )
        return dict(
            val_accuracy=acc, val_auc=auc,
            hit_rate_up=hit_up, hit_rate_down=hit_down,
            samples=len(y_true),
        )

    tree_metrics   = _metrics(y_val, tree_pred_val,   tree_proba_val,   "RandomForest")
    logreg_metrics = _metrics(y_val, logreg_pred_val, logreg_proba_val, "LogisticReg")
    ens_metrics    = _metrics(y_val, ens_pred,         ens_proba,        "Ensemble")

    # Feature importance (RF only — log top 5)
    importance = dict(zip(FEATURE_COLS, tree.feature_importances_.tolist()))
    top5 = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:5]
    logger.info(f"Top-5 features: {top5}")

    # Persist artifacts
    joblib.dump(tree,   TREE_PATH)
    joblib.dump(logreg, LOGREG_PATH)
    joblib.dump(scaler, SCALER_PATH)

    meta = {
        "feature_cols":       FEATURE_COLS,
        "train_end_date":     datetime.utcnow().isoformat(),
        "train_samples":      int(split),
        "val_samples":        int(len(X_val)),
        "tree_metrics":       tree_metrics,
        "logreg_metrics":     logreg_metrics,
        "ensemble_metrics":   ens_metrics,
        "feature_importance": importance,
    }
    META_PATH.write_text(json.dumps(meta, indent=2))
    logger.info(f"Models saved → {MODEL_DIR}")

    return {"tree": tree, "logreg": logreg, "scaler": scaler, "meta": meta}


# ─── Model loading ─────────────────────────────────────────────────────────────

def load_models_sync() -> Optional[Dict]:
    """Returns None if models have not been trained yet."""
    if not all(p.exists() for p in [TREE_PATH, LOGREG_PATH, SCALER_PATH, META_PATH]):
        return None
    try:
        return {
            "tree":   joblib.load(TREE_PATH),
            "logreg": joblib.load(LOGREG_PATH),
            "scaler": joblib.load(SCALER_PATH),
            "meta":   json.loads(META_PATH.read_text()),
        }
    except Exception as e:
        logger.error(f"Failed to load models: {e}")
        return None


# ─── Single-row inference ──────────────────────────────────────────────────────

def predict_row_sync(
    df: pd.DataFrame,
    models: Dict,
    market_features: Optional[pd.DataFrame] = None,
) -> Optional[Dict]:
    """
    df: DataFrame(date, close, volume) for one ticker — at least 60 rows.
    market_features: optional DataFrame indexed by date with MARKET_FEATURE_COLS.
    Returns inference dict, or None if insufficient data.
    """
    if df is None or len(df) < 60:
        return None

    try:
        feat_frame = build_features_for_series(df, market_features)
    except Exception as e:
        logger.warning(f"Feature build error in inference: {e}")
        return None

    if feat_frame.empty:
        return None

    # Use the most recent completed trading day
    row   = feat_frame[FEATURE_COLS].iloc[[-1]].values
    row_s = models["scaler"].transform(row)

    tree_prob   = float(models["tree"].predict_proba(row)[0, 1])
    logreg_prob = float(models["logreg"].predict_proba(row_s)[0, 1])
    ens_prob    = 0.6 * tree_prob + 0.4 * logreg_prob

    if ens_prob > 0.55:
        direction  = "UP"
        confidence = ens_prob
    elif ens_prob < 0.45:
        direction  = "DOWN"
        confidence = 1.0 - ens_prob
    else:
        direction  = "NEUTRAL"
        confidence = 1.0 - abs(ens_prob - 0.5) * 2

    meta = models.get("meta", {})
    return {
        "tree_prob":      round(tree_prob, 4),
        "logreg_prob":    round(logreg_prob, 4),
        "ensemble_prob":  round(ens_prob, 4),
        "direction":      direction,
        "confidence":     round(confidence, 4),
        "train_end_date": meta.get("train_end_date"),
        "val_accuracy":   meta.get("ensemble_metrics", {}).get("val_accuracy"),
    }
