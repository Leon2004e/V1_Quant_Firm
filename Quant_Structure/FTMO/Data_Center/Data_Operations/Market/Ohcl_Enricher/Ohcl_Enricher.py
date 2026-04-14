# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Data_Center/Data_Operations/Market/Ohcl_Enricher/Ohcl_Enricher.py

Zweck:
- Liest OHLC-Rohdaten aus Parquet
- Berechnet Marktfeatures und Regime-Labels
- Verwendet strenge Mindesthistorien:
    * Rolling/ATR/SMA erst ab voller Window-Länge
    * EMA ebenfalls erst ab voller Span-Länge
    * Regimes nur bei validen Inputs, sonst UNKNOWN
- Speichert die angereicherten Daten als Parquet
- Schreibt zusätzlich eine summary.json

Input:
- Quant_Structure/FTMO/Data_Center/Data/Ohcl/Raw/<TF>/<SYMBOL>.parquet

Output:
- Quant_Structure/FTMO/Data_Center/Data/Ohcl/Feutured/<TF>/<SYMBOL>.parquet
- Quant_Structure/FTMO/Data_Center/Data/Ohcl/Feutured/summary.json

Hinweis:
- Output-Ordner ist absichtlich "Feutured", passend zu deiner aktuellen Struktur
"""

from __future__ import annotations

import json
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# ============================================================
# ROOT / PATHS
# ============================================================

def find_ftmo_root(start: Path) -> Path:
    cur = start.resolve()
    for p in [cur] + list(cur.parents):
        if (p / "Data_Center").exists() and (p / "Dashboards").exists():
            return p
    raise RuntimeError(
        f"FTMO-Root nicht gefunden. Erwartet Root mit 'Data_Center' und 'Dashboards'. Start={start}"
    )


SCRIPT_PATH = Path(__file__).resolve()
FTMO_ROOT = find_ftmo_root(SCRIPT_PATH)

RAW_DIR = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Ohcl"
    / "Raw"
)

FEATURED_DIR = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Ohcl"
    / "Feutured"
)

SUMMARY_PATH = FEATURED_DIR / "summary.json"


# ============================================================
# CONFIG
# ============================================================

TIME_COL = "time"

ATR_WINDOWS = [14, 50]
ROLL_STD_WINDOWS = [20, 50]
SMA_WINDOWS = [20, 50, 100, 200]
EMA_WINDOWS = [20, 50, 200]

ZSCORE_WINDOWS = {
    "tick_volume": 50,
    "spread": 50,
    "bar_range": 50,
}

SLOPE_WINDOW = 20
VOL_PERCENTILE_WINDOW = 252
MIN_ROWS_WARN = 100


# ============================================================
# HELPERS
# ============================================================

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def safe_float(x: object) -> Optional[float]:
    try:
        if x is None or pd.isna(x):
            return None
        if math.isinf(float(x)):
            return None
        return float(x)
    except Exception:
        return None


def atomic_write_json(obj: dict, path: Path) -> None:
    ensure_dir(path.parent)

    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=path.stem + "_",
        suffix=".tmp",
        dir=str(path.parent),
    )
    os.close(tmp_fd)
    tmp_path = Path(tmp_name)

    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2, ensure_ascii=False)
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


def build_output_path(raw_path: Path) -> Path:
    rel = raw_path.relative_to(RAW_DIR)
    return FEATURED_DIR / rel


def get_parquet_metadata(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}

    try:
        pf = pq.ParquetFile(path)
        raw = pf.schema_arrow.metadata or {}
        return {
            k.decode("utf-8", errors="ignore"): v.decode("utf-8", errors="ignore")
            for k, v in raw.items()
        }
    except Exception:
        return {}


def build_parquet_metadata(df: pd.DataFrame, source_meta: Dict[str, str], symbol: str, tf: str) -> Dict[bytes, bytes]:
    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    if df is None or df.empty:
        meta = {
            "symbol": symbol,
            "timeframe": tf,
            "rows": "0",
            "from_utc": "",
            "to_utc": "",
            "written_at_utc": now_utc,
            "feature_set": "ohlc_enriched_v2_strict_min_periods",
        }
    else:
        tmin = pd.to_datetime(df[TIME_COL].min(), utc=True)
        tmax = pd.to_datetime(df[TIME_COL].max(), utc=True)
        meta = {
            "symbol": symbol,
            "timeframe": tf,
            "rows": str(int(len(df))),
            "from_utc": tmin.isoformat(),
            "to_utc": tmax.isoformat(),
            "written_at_utc": now_utc,
            "feature_set": "ohlc_enriched_v2_strict_min_periods",
        }

    for k, v in source_meta.items():
        if k not in meta:
            meta[k] = v

    return {str(k).encode("utf-8"): str(v).encode("utf-8") for k, v in meta.items()}


def atomic_write_df_parquet(df: pd.DataFrame, path: Path, symbol: str, tf: str, source_meta: Dict[str, str]) -> None:
    ensure_dir(path.parent)

    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=path.stem + "_",
        suffix=".tmp",
        dir=str(path.parent),
    )
    os.close(tmp_fd)
    tmp_path = Path(tmp_name)

    try:
        table = pa.Table.from_pandas(df, preserve_index=False)
        existing_meta = table.schema.metadata or {}
        new_meta = build_parquet_metadata(df, source_meta=source_meta, symbol=symbol, tf=tf)
        merged_meta = dict(existing_meta)
        merged_meta.update(new_meta)
        table = table.replace_schema_metadata(merged_meta)
        pq.write_table(table, tmp_path)
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


# ============================================================
# IO
# ============================================================

def load_raw_parquet(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)

    required = [
        "time", "open", "high", "low", "close",
        "tick_volume", "spread", "real_volume", "symbol", "timeframe"
    ]
    for col in required:
        if col not in df.columns:
            if col == "time":
                raise RuntimeError(f"Pflichtspalte fehlt: {col} | Datei={path}")
            df[col] = np.nan

    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df.dropna(subset=["time"]).copy()

    num_cols = ["open", "high", "low", "close", "tick_volume", "spread", "real_volume"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["symbol"] = df["symbol"].astype(str)
    df["timeframe"] = df["timeframe"].astype(str)

    df = (
        df[required]
        .drop_duplicates(subset=["time"], keep="last")
        .sort_values("time")
        .reset_index(drop=True)
    )

    return df


def discover_raw_files(raw_dir: Path) -> List[Path]:
    if not raw_dir.exists():
        raise RuntimeError(f"RAW_DIR nicht gefunden: {raw_dir}")

    files = sorted(raw_dir.rglob("*.parquet"))
    return [f for f in files if f.is_file()]


# ============================================================
# FEATURE HELPERS
# ============================================================

def rolling_zscore_strict(series: pd.Series, window: int) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    mean_ = s.rolling(window=window, min_periods=window).mean()
    std_ = s.rolling(window=window, min_periods=window).std(ddof=1)
    z = (s - mean_) / std_
    return z.replace([np.inf, -np.inf], np.nan)


def rolling_slope_strict(series: pd.Series, window: int) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")

    def _slope(arr: np.ndarray) -> float:
        if len(arr) < window:
            return np.nan
        y = np.asarray(arr, dtype=float)
        if np.isnan(y).any():
            return np.nan
        x = np.arange(len(y), dtype=float)
        try:
            return float(np.polyfit(x, y, 1)[0])
        except Exception:
            return np.nan

    return s.rolling(window=window, min_periods=window).apply(_slope, raw=True)


def rolling_percentile_rank_strict(series: pd.Series, window: int) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")

    def _pct_rank(arr: np.ndarray) -> float:
        if len(arr) < window:
            return np.nan
        x = pd.Series(arr)
        if x.isna().any():
            return np.nan
        last = x.iloc[-1]
        return float((x <= last).mean())

    return s.rolling(window=window, min_periods=window).apply(_pct_rank, raw=True)


# ============================================================
# FEATURE ENGINE
# ============================================================

def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    ts = pd.to_datetime(out["time"], utc=True)
    out["year"] = ts.dt.year
    out["quarter"] = ts.dt.quarter
    out["month"] = ts.dt.month
    out["weekday"] = ts.dt.weekday
    out["hour_utc"] = ts.dt.hour

    conditions = [
        out["hour_utc"].between(0, 6, inclusive="both"),
        out["hour_utc"].between(7, 12, inclusive="both"),
        out["hour_utc"].between(13, 16, inclusive="both"),
        out["hour_utc"].between(17, 21, inclusive="both"),
    ]
    choices = ["ASIA", "LONDON", "NEW_YORK", "NY_PM"]
    out["session_regime"] = np.select(conditions, choices, default="OFF_HOURS")

    return out


def add_bar_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["bar_range"] = out["high"] - out["low"]
    out["body_size"] = (out["close"] - out["open"]).abs()

    out["upper_wick"] = out["high"] - out[["open", "close"]].max(axis=1)
    out["lower_wick"] = out[["open", "close"]].min(axis=1) - out["low"]

    out["close_to_open_return"] = np.where(
        out["open"] != 0,
        (out["close"] - out["open"]) / out["open"],
        np.nan,
    )

    out["close_to_close_return"] = out["close"].pct_change()

    out["prev_close"] = out["close"].shift(1)
    out["tr_hl"] = out["high"] - out["low"]
    out["tr_h_pc"] = (out["high"] - out["prev_close"]).abs()
    out["tr_l_pc"] = (out["low"] - out["prev_close"]).abs()
    out["true_range"] = out[["tr_hl", "tr_h_pc", "tr_l_pc"]].max(axis=1)

    out["inside_bar_flag"] = (
        (out["high"] <= out["high"].shift(1)) &
        (out["low"] >= out["low"].shift(1))
    ).astype("int8")

    out["outside_bar_flag"] = (
        (out["high"] >= out["high"].shift(1)) &
        (out["low"] <= out["low"].shift(1))
    ).astype("int8")

    out["bull_bar_flag"] = (out["close"] > out["open"]).astype("int8")
    out["bear_bar_flag"] = (out["close"] < out["open"]).astype("int8")

    return out


def add_volatility_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for w in ATR_WINDOWS:
        out[f"atr_{w}"] = out["true_range"].rolling(window=w, min_periods=w).mean()

    for w in ROLL_STD_WINDOWS:
        out[f"rolling_std_{w}"] = out["close_to_close_return"].rolling(
            window=w, min_periods=w
        ).std(ddof=1)

    out["range_ma_20"] = out["bar_range"].rolling(window=20, min_periods=20).mean()
    out["range_ma_50"] = out["bar_range"].rolling(window=50, min_periods=50).mean()

    out["volatility_percentile_252"] = rolling_percentile_rank_strict(
        out["true_range"], VOL_PERCENTILE_WINDOW
    )

    out["volatility_features_ready"] = out["volatility_percentile_252"].notna().astype("int8")
    out["volatility_regime"] = "UNKNOWN"

    low_mask = out["volatility_features_ready"].eq(1) & (out["volatility_percentile_252"] < 0.33)
    mid_mask = out["volatility_features_ready"].eq(1) & (
        (out["volatility_percentile_252"] >= 0.33) &
        (out["volatility_percentile_252"] < 0.66)
    )
    high_mask = out["volatility_features_ready"].eq(1) & (out["volatility_percentile_252"] >= 0.66)

    out.loc[low_mask, "volatility_regime"] = "LOW_VOL"
    out.loc[mid_mask, "volatility_regime"] = "MID_VOL"
    out.loc[high_mask, "volatility_regime"] = "HIGH_VOL"

    return out


def add_trend_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for w in SMA_WINDOWS:
        out[f"sma_{w}"] = out["close"].rolling(window=w, min_periods=w).mean()

    for w in EMA_WINDOWS:
        out[f"ema_{w}"] = out["close"].ewm(span=w, adjust=False, min_periods=w).mean()

    out["distance_to_sma_20"] = np.where(
        out["sma_20"] != 0,
        (out["close"] - out["sma_20"]) / out["sma_20"],
        np.nan,
    )
    out["distance_to_sma_50"] = np.where(
        out["sma_50"] != 0,
        (out["close"] - out["sma_50"]) / out["sma_50"],
        np.nan,
    )
    out["distance_to_ema_20"] = np.where(
        out["ema_20"] != 0,
        (out["close"] - out["ema_20"]) / out["ema_20"],
        np.nan,
    )

    out["trend_slope_20"] = rolling_slope_strict(out["close"], SLOPE_WINDOW)
    out["sma_20_slope"] = rolling_slope_strict(out["sma_20"], SLOPE_WINDOW)
    out["ema_20_slope"] = rolling_slope_strict(out["ema_20"], SLOPE_WINDOW)

    out["trend_features_ready"] = (
        out["ema_20"].notna() &
        out["ema_50"].notna() &
        out["distance_to_ema_20"].notna() &
        out["trend_slope_20"].notna()
    ).astype("int8")

    out["trend_regime"] = "UNKNOWN"

    cond_up = (
        out["trend_features_ready"].eq(1) &
        (out["ema_20"] > out["ema_50"]) &
        (out["distance_to_ema_20"] > 0) &
        (out["trend_slope_20"] > 0)
    )

    cond_down = (
        out["trend_features_ready"].eq(1) &
        (out["ema_20"] < out["ema_50"]) &
        (out["distance_to_ema_20"] < 0) &
        (out["trend_slope_20"] < 0)
    )

    cond_range = (
        out["trend_features_ready"].eq(1) &
        ~cond_up &
        ~cond_down
    )

    out.loc[cond_up, "trend_regime"] = "UPTREND"
    out.loc[cond_down, "trend_regime"] = "DOWNTREND"
    out.loc[cond_range, "trend_regime"] = "RANGE"

    return out


def add_liquidity_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["tick_volume_zscore_50"] = rolling_zscore_strict(out["tick_volume"], ZSCORE_WINDOWS["tick_volume"])
    out["spread_zscore_50"] = rolling_zscore_strict(out["spread"], ZSCORE_WINDOWS["spread"])
    out["bar_range_zscore_50"] = rolling_zscore_strict(out["bar_range"], ZSCORE_WINDOWS["bar_range"])

    out["spread_ma_20"] = out["spread"].rolling(window=20, min_periods=20).mean()
    out["tick_volume_ma_20"] = out["tick_volume"].rolling(window=20, min_periods=20).mean()

    out["liquidity_features_ready"] = (
        out["tick_volume_zscore_50"].notna() &
        out["spread_zscore_50"].notna()
    ).astype("int8")

    out["liquidity_regime"] = "UNKNOWN"

    thin_mask = (
        out["liquidity_features_ready"].eq(1) &
        (out["spread_zscore_50"] > 1.0) &
        (out["tick_volume_zscore_50"] < -0.5)
    )
    active_mask = (
        out["liquidity_features_ready"].eq(1) &
        (out["spread_zscore_50"] < 0.5) &
        (out["tick_volume_zscore_50"] > 0.5)
    )
    normal_mask = out["liquidity_features_ready"].eq(1) & ~thin_mask & ~active_mask

    out.loc[thin_mask, "liquidity_regime"] = "THIN"
    out.loc[active_mask, "liquidity_regime"] = "ACTIVE"
    out.loc[normal_mask, "liquidity_regime"] = "NORMAL"

    return out


def cleanup_feature_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    drop_cols = ["prev_close", "tr_hl", "tr_h_pc", "tr_l_pc"]
    for col in drop_cols:
        if col in out.columns:
            out = out.drop(columns=[col])

    out = out.sort_values("time").reset_index(drop=True)

    str_cols = [
        "symbol", "timeframe",
        "session_regime", "volatility_regime",
        "trend_regime", "liquidity_regime"
    ]
    for col in str_cols:
        if col in out.columns:
            out[col] = out[col].astype(str)

    return out


def enrich_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = add_time_features(out)
    out = add_bar_features(out)
    out = add_volatility_features(out)
    out = add_trend_features(out)
    out = add_liquidity_features(out)
    out = cleanup_feature_df(out)
    return out


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    print("=" * 100)
    print("OHLC ENRICHER")
    print("=" * 100)
    print(f"[INFO] FTMO_ROOT    = {FTMO_ROOT}")
    print(f"[INFO] RAW_DIR      = {RAW_DIR}")
    print(f"[INFO] FEATURED_DIR = {FEATURED_DIR}")
    print("=" * 100)

    ensure_dir(FEATURED_DIR)

    raw_files = discover_raw_files(RAW_DIR)
    if not raw_files:
        raise RuntimeError(f"Keine Raw-Parquet-Dateien gefunden unter: {RAW_DIR}")

    summary: Dict[str, Dict[str, object]] = {}

    for raw_path in raw_files:
        rel = raw_path.relative_to(RAW_DIR)
        tf = rel.parts[0] if len(rel.parts) >= 2 else "UNKNOWN"
        symbol = raw_path.stem

        try:
            src_meta = get_parquet_metadata(raw_path)
            raw_df = load_raw_parquet(raw_path)
            feat_df = enrich_ohlc(raw_df)

            out_path = build_output_path(raw_path)
            atomic_write_df_parquet(
                feat_df,
                out_path,
                symbol=symbol,
                tf=tf,
                source_meta=src_meta,
            )

            time_min = pd.to_datetime(feat_df["time"].min(), utc=True) if not feat_df.empty else pd.NaT
            time_max = pd.to_datetime(feat_df["time"].max(), utc=True) if not feat_df.empty else pd.NaT

            summary_key = f"{tf}/{symbol}"
            summary[summary_key] = {
                "status": "ok",
                "symbol": symbol,
                "timeframe": tf,
                "rows": int(len(feat_df)),
                "from_utc": time_min.isoformat() if pd.notna(time_min) else "",
                "to_utc": time_max.isoformat() if pd.notna(time_max) else "",
                "input_file": str(raw_path),
                "output_file": str(out_path),
                "warning_low_rows": bool(len(feat_df) < MIN_ROWS_WARN),
                "source_parquet_metadata": src_meta,
                "output_parquet_metadata": get_parquet_metadata(out_path),
            }

            print(f"[OK] {tf} | {symbol} | rows={len(feat_df)} -> {out_path}")

        except Exception as e:
            summary_key = f"{tf}/{symbol}"
            summary[summary_key] = {
                "status": "error",
                "symbol": symbol,
                "timeframe": tf,
                "input_file": str(raw_path),
                "error": str(e),
            }
            print(f"[WARN] {tf} | {symbol} failed: {e}")

    atomic_write_json(summary, SUMMARY_PATH)

    print("-" * 100)
    print(f"[DONE] summary -> {SUMMARY_PATH}")
    print("=" * 100)


if __name__ == "__main__":
    main()