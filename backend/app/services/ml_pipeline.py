"""
ML Pipeline for GPST — Feature engineering, training, inference.
Two models: RandomForestClassifier (Model A) + LogisticRegression (Model B).
All heavy work is synchronous (pandas/sklearn); call via asyncio.to_thread().

File layout under backend/app/models/:
    tree_model.pkl
    logreg_model.pkl
    scaler.pkl
    feature_meta.json
"""
import os
import json
import logging
import asyncio
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import joblib

from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report
from sklearn.pipeline import Pipeline

logger = logging.getLogger(__name__)

# ─── Paths ────────────────────────────────────────────────────────────────────
MODEL_DIR = Path(__file__).parent.parent / "models"
MODEL_DIR.mkdir(exist_ok=True)

TREE_PATH   = MODEL_DIR / "tree_model.pkl"
LOGREG_PATH = MODEL_DIR / "logreg_model.pkl"
SCALER_PATH = MODEL_DIR / "scaler.pkl"
META_PATH   = MODEL_DIR / "feature_meta.json"

# ─── Core tickers used for pooled training ────────────────────────────────────
TRAINING_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "JPM",
    "V", "JNJ", "XOM", "BAC", "WFC", "GS", "SPY", "QQQ", "IWM",
    "NFLX", "AMD", "INTC", "PYPL", "COST", "PG", "KO", "MCD",
    "NKE", "ADBE", "CRM", "ORCL", "IBM",
]

FEATURE_COLS = [
    "ret_1d", "ret_5d", "ret_10d", "ret_20d",
    "vol_5d", "vol_20d",
    "ma_5", "ma_20", "ma_50",
    "ma_5_vs_20", "ma_20_vs_50",
    "rsi_14", "bb_position",
    "volume_zscore_20", "volume_ratio",
]


# ─── Feature engineering helpers ─────────────────────────────────────────────

def _rsi(closes: pd.Series, period: int = 14) -> pd.Series:
    delta = closes.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def build_features_for_series(df: pd.DataFrame) -> pd.DataFrame:
    """
    df must have columns: date, close, volume (sorted ascending by date).
    Returns a DataFrame with FEATURE_COLS + 'y' (1=UP next day, 0=DOWN).
    Last row has no future close → dropped.
    """
    df = df.sort_values("date").reset_index(drop=True)
    c = df["close"]
    v = df["volume"].fillna(0)

    out = pd.DataFrame(index=df.index)

    # Returns
    out["ret_1d"]  = c.pct_change(1)
    out["ret_5d"]  = c.pct_change(5)
    out["ret_10d"] = c.pct_change(10)
    out["ret_20d"] = c.pct_change(20)

    # Volatility (std of daily returns)
    daily_ret = c.pct_change()
    out["vol_5d"]  = daily_ret.rolling(5).std()
    out["vol_20d"] = daily_ret.rolling(20).std()

    # Moving averages (price-normalised)
    ma5  = c.rolling(5).mean()
    ma20 = c.rolling(20).mean()
    ma50 = c.rolling(50).mean()
    out["ma_5"]       = ma5  / c
    out["ma_20"]      = ma20 / c
    out["ma_50"]      = ma50 / c
    out["ma_5_vs_20"] = ma5  / ma20.replace(0, np.nan)
    out["ma_20_vs_50"]= ma20 / ma50.replace(0, np.nan)

    # RSI(14)
    out["rsi_14"] = _rsi(c, 14)

    # Bollinger Band position
    bb_mid = c.rolling(20).mean()
    bb_std = c.rolling(20).std()
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
    next_close = c.shift(-1)
    out["y"] = (next_close > c).astype(int)

    # Drop last row (no future close) and rows with NaN features
    out = out.iloc[:-1]  # drop last
    out = out.dropna(subset=FEATURE_COLS + ["y"])
    return out


# ─── Training frame builder ───────────────────────────────────────────────────

def build_training_frame(price_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    price_data: { ticker: DataFrame(date, close, volume) }
    Returns pooled feature frame across all tickers.
    """
    frames = []
    for ticker, df in price_data.items():
        if df is None or len(df) < 60:
            logger.warning(f"Skipping {ticker}: insufficient data ({len(df) if df is not None else 0} rows)")
            continue
        try:
            feat = build_features_for_series(df)
            feat["ticker"] = ticker
            frames.append(feat)
        except Exception as e:
            logger.warning(f"Feature build failed for {ticker}: {e}")

    if not frames:
        raise ValueError("No training data could be built.")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_index()   # keep original date order within tickers
    logger.info(f"Training frame: {len(combined)} rows, {len(price_data)} tickers")
    return combined


# ─── Model training ──────────────────────────────────────────────────────────

def train_models_sync(
    frame: pd.DataFrame,
    train_frac: float = 0.80,
) -> Dict:
    """
    Synchronous training. Returns a dict with models + metrics.
    Call via asyncio.to_thread() from async code.
    """
    X = frame[FEATURE_COLS].values
    y = frame["y"].values

    split = int(len(X) * train_frac)
    X_train, X_val = X[:split], X[split:]
    y_train, y_val = y[:split], y[split:]

    if len(X_train) < 50 or len(X_val) < 10:
        raise ValueError(f"Not enough data to train: train={len(X_train)}, val={len(X_val)}")

    # ── Scaler (fit on train only) ────────────────────────────────────────────
    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_val_s   = scaler.transform(X_val)

    # ── Model A: RandomForest ─────────────────────────────────────────────────
    tree = RandomForestClassifier(
        n_estimators=200,
        max_depth=8,
        min_samples_leaf=20,
        max_features="sqrt",
        class_weight="balanced",
        n_jobs=-1,
        random_state=42,
    )
    tree.fit(X_train, y_train)   # RF doesn't need scaling
    tree_proba_val = tree.predict_proba(X_val)[:, 1]
    tree_pred_val  = (tree_proba_val > 0.5).astype(int)

    # ── Model B: LogisticRegression ───────────────────────────────────────────
    logreg = LogisticRegression(
        C=0.1,
        solver="lbfgs",
        max_iter=500,
        class_weight="balanced",
        random_state=42,
    )
    logreg.fit(X_train_s, y_train)
    logreg_proba_val = logreg.predict_proba(X_val_s)[:, 1]
    logreg_pred_val  = (logreg_proba_val > 0.5).astype(int)

    # ── Ensemble ──────────────────────────────────────────────────────────────
    ens_proba = 0.6 * tree_proba_val + 0.4 * logreg_proba_val
    ens_pred  = (ens_proba > 0.5).astype(int)

    def _metrics(y_true, y_pred, y_prob, name):
        acc = accuracy_score(y_true, y_pred)
        try:
            auc = roc_auc_score(y_true, y_prob)
        except Exception:
            auc = 0.5
        up_idx   = y_true == 1
        down_idx = y_true == 0
        hit_up   = accuracy_score(y_true[up_idx],   y_pred[up_idx])   if up_idx.any()   else 0.0
        hit_down = accuracy_score(y_true[down_idx], y_pred[down_idx]) if down_idx.any() else 0.0
        logger.info(
            f"[{name}] val_acc={acc:.3f}  val_auc={auc:.3f}  "
            f"hit_up={hit_up:.3f}  hit_down={hit_down:.3f}  n={len(y_true)}"
        )
        return dict(val_accuracy=acc, val_auc=auc, hit_rate_up=hit_up,
                    hit_rate_down=hit_down, samples=len(y_true))

    tree_metrics   = _metrics(y_val, tree_pred_val,   tree_proba_val,   "RandomForest")
    logreg_metrics = _metrics(y_val, logreg_pred_val, logreg_proba_val, "LogisticReg")
    ens_metrics    = _metrics(y_val, ens_pred,         ens_proba,        "Ensemble")

    # ── Save artifacts ────────────────────────────────────────────────────────
    joblib.dump(tree,   TREE_PATH)
    joblib.dump(logreg, LOGREG_PATH)
    joblib.dump(scaler, SCALER_PATH)

    meta = {
        "feature_cols":   FEATURE_COLS,
        "train_end_date": datetime.utcnow().isoformat(),
        "train_samples":  int(split),
        "val_samples":    int(len(X_val)),
        "tree_metrics":   tree_metrics,
        "logreg_metrics": logreg_metrics,
        "ensemble_metrics": ens_metrics,
    }
    META_PATH.write_text(json.dumps(meta, indent=2))
    logger.info(f"Models saved to {MODEL_DIR}")

    return {
        "tree":   tree,
        "logreg": logreg,
        "scaler": scaler,
        "meta":   meta,
    }


# ─── Model loading ────────────────────────────────────────────────────────────

def load_models_sync() -> Optional[Dict]:
    """Returns None if models haven't been trained yet."""
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


# ─── Single-row inference ─────────────────────────────────────────────────────

def predict_row_sync(
    df: pd.DataFrame,
    models: Dict,
) -> Optional[Dict]:
    """
    df: same format as build_features_for_series input (date, close, volume).
    Returns dict with tree_prob, logreg_prob, ensemble_prob, direction, confidence.
    """
    if df is None or len(df) < 60:
        return None

    try:
        feat_frame = build_features_for_series(df)
    except Exception as e:
        logger.warning(f"Feature build error: {e}")
        return None

    if feat_frame.empty:
        return None

    # Use the LAST row as "today's" features (most recent completed bar)
    row = feat_frame[FEATURE_COLS].iloc[[-1]].values   # shape (1, n_features)

    tree   = models["tree"]
    logreg = models["logreg"]
    scaler = models["scaler"]

    tree_prob   = float(tree.predict_proba(row)[0, 1])
    row_s       = scaler.transform(row)
    logreg_prob = float(logreg.predict_proba(row_s)[0, 1])
    ens_prob    = 0.6 * tree_prob + 0.4 * logreg_prob

    if ens_prob > 0.55:
        direction  = "UP"
        confidence = ens_prob
    elif ens_prob < 0.45:
        direction  = "DOWN"
        confidence = 1.0 - ens_prob
    else:
        direction  = "NEUTRAL"
        confidence = 1.0 - abs(ens_prob - 0.5) * 2   # 1.0 at exactly 0.5

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
