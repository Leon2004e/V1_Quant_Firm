#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype


# ============================================================
# Konfiguration
# ============================================================

DEFAULT_INITIAL_CAPITAL = 10000.0
DEFAULT_SPREAD_MODE = "from_data"          # from_data | zero
DEFAULT_INTRABAR_MODE = "pessimistic"      # pessimistic | optimistic
SUPPORTED_PRIMARY_TIMEFRAME = "M15"

FORCE_FIXED_LOTS = True
FIXED_LOTS_VALUE = 0.1

FX_CONTRACT_SIZE = 100000.0
METAL_CONTRACT_SIZE = 100.0
CFD_CONTRACT_SIZE = 1.0


# ============================================================
# Utils
# ============================================================

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def safe_write_df(df: pd.DataFrame, parquet_path: Path, csv_path: Path) -> str:
    x = df.copy()
    for col in x.columns:
        if pd.api.types.is_datetime64_any_dtype(x[col]):
            try:
                if getattr(x[col].dt, "tz", None) is not None:
                    x[col] = x[col].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                else:
                    x[col] = x[col].astype(str)
            except Exception:
                x[col] = x[col].astype(str)

    try:
        x.to_parquet(parquet_path, index=False)
        return str(parquet_path)
    except Exception:
        x.to_csv(csv_path, index=False)
        return str(csv_path)


def find_default_data_root(script_path: Path) -> Optional[Path]:
    candidates = []
    for parent in [script_path.parent, *script_path.parents]:
        candidates.extend([
            parent / "Data",
            parent / "Data_Center" / "Data",
        ])
    for c in candidates:
        if c.exists() and c.is_dir():
            return c.resolve()
    return None


def derive_paths(data_root: Path) -> Dict[str, Path]:
    return {
        "compiled_root": data_root / "Strategy" / "Strategy_Profile_V2" / "compiled_strategies",
        "ohlc_raw": data_root / "Ohcl" / "Raw",
        "out_root": data_root / "Trades" / "Raw" / "Backtest_V2",
    }


# ============================================================
# OHLC
# ============================================================

def read_parquet_or_jsonl(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)

    if suffix in {".jsonl", ".json", ".ndjson"}:
        rows = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return pd.DataFrame(rows)

    raise ValueError(f"Unsupported format: {path}")


def normalize_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    required = ["time", "open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"OHLC fehlt Spalten: {missing}")

    out = df.copy()

    if is_numeric_dtype(out["time"]):
        out["time"] = pd.to_datetime(out["time"], unit="ms", utc=True)
    elif is_datetime64_any_dtype(out["time"]):
        try:
            if out["time"].dt.tz is None:
                out["time"] = out["time"].dt.tz_localize("UTC")
            else:
                out["time"] = out["time"].dt.tz_convert("UTC")
        except Exception:
            out["time"] = pd.to_datetime(out["time"], utc=True, errors="coerce")
    else:
        out["time"] = pd.to_datetime(out["time"], utc=True, errors="coerce")

    for col in ["open", "high", "low", "close", "spread", "tick_volume", "real_volume"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    if "spread" not in out.columns:
        out["spread"] = 0.0
    if "tick_volume" not in out.columns:
        out["tick_volume"] = 1.0
    if "real_volume" not in out.columns:
        out["real_volume"] = 0.0
    if "symbol" not in out.columns:
        out["symbol"] = None
    if "timeframe" not in out.columns:
        out["timeframe"] = None

    out = out.dropna(subset=["time", "open", "high", "low", "close"])
    out = out[out["high"] >= out["low"]].copy()
    out = out.sort_values("time").reset_index(drop=True)
    return out


def find_ohlc_file(ohlc_raw_root: Path, timeframe: str, symbol: str) -> Optional[Path]:
    tf_dir = ohlc_raw_root / timeframe
    candidates = [
        tf_dir / f"{symbol}.parquet",
        tf_dir / f"{symbol}.jsonl",
        tf_dir / f"{symbol}.json",
        tf_dir / f"{symbol}.ndjson",
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


def load_ohlc(ohlc_raw_root: Path, timeframe: str, symbol: str) -> pd.DataFrame:
    f = find_ohlc_file(ohlc_raw_root, timeframe, symbol)
    if f is None:
        raise FileNotFoundError(f"Keine OHLC-Datei für {symbol} {timeframe}")
    return normalize_ohlc(read_parquet_or_jsonl(f))


def align_to_base(base_df: pd.DataFrame, other_df: pd.DataFrame, cols: List[str], prefix: str) -> pd.DataFrame:
    left = base_df[["time"]].copy().sort_values("time")
    right = other_df[["time"] + cols].copy().sort_values("time")

    merged = pd.merge_asof(
        left,
        right,
        on="time",
        direction="backward",
        allow_exact_matches=True,
    )
    rename = {c: f"{prefix}{c}" for c in cols}
    return merged.rename(columns=rename)


# ============================================================
# Symbol / contract helpers
# ============================================================

def infer_point(symbol: str, median_price: float) -> float:
    su = symbol.upper()
    if "JPY" in su:
        return 0.001
    if "XAU" in su or "XAG" in su:
        return 0.01
    if su.endswith(".CASH") or "OIL" in su:
        return 0.1
    if median_price >= 100:
        return 0.01
    return 0.0001


def infer_contract_size(symbol: str) -> float:
    su = symbol.upper()
    if "XAU" in su or "XAG" in su:
        return METAL_CONTRACT_SIZE
    if su.endswith(".CASH") or "OIL" in su:
        return CFD_CONTRACT_SIZE
    return FX_CONTRACT_SIZE


def mql_day_of_week(ts: pd.Series) -> pd.Series:
    return ((ts.dt.dayofweek + 1) % 7).astype(int)


# ============================================================
# Indicators
# ============================================================

def price_series(df: pd.DataFrame, mode: str) -> pd.Series:
    if mode == "PRICE_CLOSE":
        return df["close"]
    if mode == "PRICE_OPEN":
        return df["open"]
    if mode == "PRICE_HIGH":
        return df["high"]
    if mode == "PRICE_LOW":
        return df["low"]
    if mode == "PRICE_MEDIAN":
        return (df["high"] + df["low"]) / 2.0
    if mode == "PRICE_TYPICAL":
        return (df["high"] + df["low"] + df["close"]) / 3.0
    if mode == "PRICE_WEIGHTED":
        return (df["high"] + df["low"] + 2.0 * df["close"]) / 4.0
    return df["close"]


def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=max(1, int(period)), adjust=False).mean()


def smma(series: pd.Series, period: int) -> pd.Series:
    p = max(1, int(period))
    out = pd.Series(index=series.index, dtype=float)
    if len(series) == 0:
        return out
    out.iloc[0] = series.iloc[0]
    for i in range(1, len(series)):
        out.iloc[i] = (out.iloc[i - 1] * (p - 1) + series.iloc[i]) / p
    return out


def wma(series: pd.Series, period: int) -> pd.Series:
    p = max(1, int(period))
    weights = np.arange(1, p + 1)
    return series.rolling(p).apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True)


def hma(series: pd.Series, period: int) -> pd.Series:
    p = max(2, int(period))
    half = max(1, p // 2)
    sqrt_p = max(1, int(math.sqrt(p)))
    return wma(2 * wma(series, half) - wma(series, p), sqrt_p)


def atr(df: pd.DataFrame, period: int) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(max(1, int(period)), min_periods=max(1, int(period))).mean()


def macd(df: pd.DataFrame, fast: int, slow: int, smooth: int) -> pd.DataFrame:
    fast_ema = ema(df["close"], fast)
    slow_ema = ema(df["close"], slow)
    main = fast_ema - slow_ema
    signal = ema(main, smooth)
    hist = main - signal
    return pd.DataFrame({"main": main, "signal": signal, "hist": hist})


def adx(df: pd.DataFrame, period: int) -> pd.DataFrame:
    high = df["high"]
    low = df["low"]
    close = df["close"]

    plus_dm = high.diff()
    minus_dm = -low.diff()

    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)

    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs(),
    ], axis=1).max(axis=1)

    atr_val = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr_val.replace(0, np.nan))
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr_val.replace(0, np.nan))
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx_val = dx.rolling(period).mean()

    return pd.DataFrame({"adx": adx_val, "plus_di": plus_di, "minus_di": minus_di})


def cci(df: pd.DataFrame, period: int, price_mode: str) -> pd.Series:
    s = price_series(df, price_mode)
    sma = s.rolling(period).mean()
    mad = s.rolling(period).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
    return (s - sma) / (0.015 * mad.replace(0, np.nan))


def linreg(series: pd.Series, period: int) -> pd.Series:
    p = max(2, int(period))
    idx = np.arange(p)

    def _fit(x: np.ndarray) -> float:
        coef = np.polyfit(idx, x, 1)
        return coef[0] * idx[-1] + coef[1]

    return series.rolling(p).apply(_fit, raw=True)


def stddev(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(max(1, int(period))).std()


def parabolic_sar(df: pd.DataFrame, af_step: float = 0.02, af_max: float = 0.2) -> pd.Series:
    high = df["high"].values
    low = df["low"].values
    n = len(df)

    sar = np.zeros(n)
    bull = True
    af = af_step
    ep = high[0]
    sar[0] = low[0]

    for i in range(1, n):
        prev_sar = sar[i - 1]

        if bull:
            sar[i] = prev_sar + af * (ep - prev_sar)
            sar[i] = min(sar[i], low[i - 1])
            if i > 1:
                sar[i] = min(sar[i], low[i - 2])

            if high[i] > ep:
                ep = high[i]
                af = min(af + af_step, af_max)

            if low[i] < sar[i]:
                bull = False
                sar[i] = ep
                ep = low[i]
                af = af_step
        else:
            sar[i] = prev_sar + af * (ep - prev_sar)
            sar[i] = max(sar[i], high[i - 1])
            if i > 1:
                sar[i] = max(sar[i], high[i - 2])

            if low[i] < ep:
                ep = low[i]
                af = min(af + af_step, af_max)

            if high[i] > sar[i]:
                bull = True
                sar[i] = ep
                ep = high[i]
                af = af_step

    return pd.Series(sar, index=df.index)


def wpr(df: pd.DataFrame, period: int) -> pd.Series:
    hh = df["high"].rolling(period).max()
    ll = df["low"].rolling(period).min()
    return -100 * (hh - df["close"]) / (hh - ll).replace(0, np.nan)


def bears_power(df: pd.DataFrame, period: int, price_mode: str = "PRICE_LOW") -> pd.Series:
    base = ema(df["close"], period)
    if price_mode == "PRICE_LOW":
        return df["low"] - base
    return df["close"] - base


def highest_in_range(df: pd.DataFrame, t_from: str, t_to: str) -> pd.Series:
    out = pd.Series(index=df.index, dtype=float)
    hhmm = df["time"].dt.strftime("%H:%M")
    days = df["time"].dt.floor("D")
    for _, idxs in df.groupby(days).groups.items():
        idxs = list(idxs)
        session_idxs = [i for i in idxs if hhmm.iloc[i] >= t_from and hhmm.iloc[i] <= t_to]
        if not session_idxs:
            continue
        hv = df.loc[session_idxs, "high"].max()
        for i in idxs:
            out.iloc[i] = hv
    return out


def lowest_in_range(df: pd.DataFrame, t_from: str, t_to: str) -> pd.Series:
    out = pd.Series(index=df.index, dtype=float)
    hhmm = df["time"].dt.strftime("%H:%M")
    days = df["time"].dt.floor("D")
    for _, idxs in df.groupby(days).groups.items():
        idxs = list(idxs)
        session_idxs = [i for i in idxs if hhmm.iloc[i] >= t_from and hhmm.iloc[i] <= t_to]
        if not session_idxs:
            continue
        lv = df.loc[session_idxs, "low"].min()
        for i in idxs:
            out.iloc[i] = lv
    return out


def rolling_vwap(df: pd.DataFrame, period: int) -> pd.Series:
    vol = df["tick_volume"].replace(0, np.nan).fillna(1.0)
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    pv = tp * vol
    return pv.rolling(period).sum() / vol.rolling(period).sum()


def supertrend(df: pd.DataFrame, period: int, mult: float) -> pd.Series:
    hl2 = (df["high"] + df["low"]) / 2.0
    atr_val = atr(df, period)
    upper = hl2 + mult * atr_val
    lower = hl2 - mult * atr_val

    final_upper = upper.copy()
    final_lower = lower.copy()
    st = pd.Series(index=df.index, dtype=float)

    for i in range(len(df)):
        if i == 0 or pd.isna(atr_val.iloc[i]):
            st.iloc[i] = np.nan
            continue

        final_upper.iloc[i] = upper.iloc[i] if df["close"].iloc[i - 1] > final_upper.iloc[i - 1] else min(upper.iloc[i], final_upper.iloc[i - 1])
        final_lower.iloc[i] = lower.iloc[i] if df["close"].iloc[i - 1] < final_lower.iloc[i - 1] else max(lower.iloc[i], final_lower.iloc[i - 1])

        prev = st.iloc[i - 1]
        if pd.isna(prev):
            st.iloc[i] = final_upper.iloc[i] if df["close"].iloc[i] <= final_upper.iloc[i] else final_lower.iloc[i]
        elif prev == final_upper.iloc[i - 1]:
            st.iloc[i] = final_upper.iloc[i] if df["close"].iloc[i] <= final_upper.iloc[i] else final_lower.iloc[i]
        else:
            st.iloc[i] = final_lower.iloc[i] if df["close"].iloc[i] >= final_lower.iloc[i] else final_upper.iloc[i]

    return st


def ichimoku(df: pd.DataFrame, tenkan_p: int, kijun_p: int, senkou_p: int) -> pd.DataFrame:
    hh_t = df["high"].rolling(tenkan_p).max()
    ll_t = df["low"].rolling(tenkan_p).min()
    tenkan = (hh_t + ll_t) / 2.0

    hh_k = df["high"].rolling(kijun_p).max()
    ll_k = df["low"].rolling(kijun_p).min()
    kijun = (hh_k + ll_k) / 2.0

    span_a = ((tenkan + kijun) / 2.0).shift(kijun_p)
    hh_s = df["high"].rolling(senkou_p).max()
    ll_s = df["low"].rolling(senkou_p).min()
    span_b = ((hh_s + ll_s) / 2.0).shift(kijun_p)

    return pd.DataFrame({
        "tenkan": tenkan,
        "kijun": kijun,
        "span_a": span_a,
        "span_b": span_b,
    })


def fibo_neg_61_8(df: pd.DataFrame, lookback: int = 50) -> pd.Series:
    hh = df["high"].rolling(lookback).max()
    ll = df["low"].rolling(lookback).min()
    return ll + (hh - ll) * (-0.618)


# ============================================================
# SQX functions
# ============================================================

def crosses_below_series(lhs: pd.Series, rhs: pd.Series, shift: int = 0) -> pd.Series:
    s = int(shift)
    return ((lhs.shift(s) < rhs.shift(s)) & (lhs.shift(s + 1) >= rhs.shift(s + 1))).fillna(False)


def crosses_above_series(lhs: pd.Series, rhs: pd.Series, shift: int = 0) -> pd.Series:
    s = int(shift)
    return ((lhs.shift(s) > rhs.shift(s)) & (lhs.shift(s + 1) <= rhs.shift(s + 1))).fillna(False)


def changes_up(series: pd.Series, shift: int) -> pd.Series:
    s = int(shift)
    return (series.shift(s) > series.shift(s + 1)).fillna(False)


def sq_is_rising(series: pd.Series, bars: int, shift: int) -> pd.Series:
    bars = max(1, int(bars))
    shift = int(shift)
    base = series.shift(shift)
    out = pd.Series(True, index=series.index)
    for i in range(1, bars + 1):
        out &= base > series.shift(shift + i)
    return out.fillna(False)


def sq_is_falling(series: pd.Series, bars: int, shift: int) -> pd.Series:
    bars = max(1, int(bars))
    shift = int(shift)
    base = series.shift(shift)
    out = pd.Series(True, index=series.index)
    for i in range(1, bars + 1):
        out &= base < series.shift(shift + i)
    return out.fillna(False)


# ============================================================
# Compiled strategy
# ============================================================

@dataclass
class StrategySpec:
    strategy_id: str
    symbol: str
    direction: str
    primary_tf: str
    secondary_tfs: List[str]
    inputs: Dict[str, Any]
    time_filters: Dict[str, Any]
    buffers: List[Dict[str, Any]]
    entry: Dict[str, Any]
    exit: Dict[str, Any]
    risk: Dict[str, Any]
    functions_used: List[str]
    used_indicators: List[str]
    support_summary: Dict[str, Any]


def load_compiled_strategy(path: Path) -> StrategySpec:
    x = load_json(path)
    return StrategySpec(
        strategy_id=x["strategy_id"],
        symbol=x["symbol"],
        direction=x["direction"],
        primary_tf=x["primary_timeframe"],
        secondary_tfs=x.get("secondary_timeframes", []),
        inputs=x.get("inputs", {}),
        time_filters=x.get("time_filters", {}),
        buffers=x.get("buffers", []),
        entry=x.get("entry", {}),
        exit=x.get("exit", {}),
        risk=x.get("risk", {}),
        functions_used=x.get("functions_used", []),
        used_indicators=x.get("used_indicators", []),
        support_summary=x.get("support_summary", {}),
    )


# ============================================================
# Feature frame from compiled buffers
# ============================================================

def build_feature_frame(spec: StrategySpec, ohlc_raw_root: Path) -> pd.DataFrame:
    unsupported = [b for b in spec.buffers if not b.get("python_supported", False)]
    if unsupported:
        names = ", ".join(f"{b['id']}:{b.get('source')}" for b in unsupported)
        raise ValueError(f"Unsupported buffers in compiled strategy: {names}")

    base_df = load_ohlc(ohlc_raw_root, spec.primary_tf, spec.symbol)
    subcharts: Dict[str, pd.DataFrame] = {}
    for tf in spec.secondary_tfs:
        subcharts[tf] = load_ohlc(ohlc_raw_root, tf, spec.symbol)

    frame = base_df.copy()
    handle_map: Dict[str, Dict[str, Union[pd.Series, pd.DataFrame]]] = {}

    for buf in spec.buffers:
        name = buf["id"]
        expr = buf["expression"]
        tf = buf.get("timeframe") or spec.primary_tf
        src = base_df if tf == spec.primary_tf else subcharts[tf]
        source = buf.get("source")
        params = buf.get("params", [])

        if expr == "255" or source == "constant":
            frame[name] = np.nan
            handle_map[name] = {"default": frame[name]}
            continue

        if source == "iMA":
            period = int(params[0])
            ma_shift = int(params[1])
            ma_mode = str(params[2])
            price_mode = str(params[3])

            s = price_series(src, price_mode)
            if ma_mode == "MODE_EMA":
                out = ema(s, period)
            elif ma_mode == "MODE_SMMA":
                out = smma(s, period)
            else:
                out = ema(s, period)

            if ma_shift != 0:
                out = out.shift(ma_shift)

            if tf != spec.primary_tf:
                aligned = align_to_base(base_df, pd.DataFrame({"time": src["time"], "v": out}), ["v"], f"{name}__")
                out = aligned[f"{name}__v"]

            frame[name] = out
            handle_map[name] = {"default": out, "buf0": out}
            continue

        if source == "iMACD":
            fast = int(params[0])
            slow = int(params[1])
            smooth = int(params[2])
            macd_df = macd(src, fast, slow, smooth)

            if tf != spec.primary_tf:
                aligned = align_to_base(
                    base_df,
                    pd.DataFrame({"time": src["time"], "main": macd_df["main"], "signal": macd_df["signal"], "hist": macd_df["hist"]}),
                    ["main", "signal", "hist"],
                    f"{name}__",
                )
                frame[name] = aligned[f"{name}__main"]
                handle_map[name] = {
                    "default": aligned[f"{name}__main"],
                    "buf0": aligned[f"{name}__main"],
                    "buf1": aligned[f"{name}__main"],
                    "signal": aligned[f"{name}__signal"],
                    "hist": aligned[f"{name}__hist"],
                }
            else:
                frame[name] = macd_df["main"]
                handle_map[name] = {
                    "default": macd_df["main"],
                    "buf0": macd_df["main"],
                    "buf1": macd_df["main"],
                    "signal": macd_df["signal"],
                    "hist": macd_df["hist"],
                }
            continue

        if source == "iStdDev":
            period = int(params[0])
            ma_shift = int(params[1])
            # params[2] ma_method not used for current implementation
            price_mode = str(params[3])

            out = stddev(price_series(src, price_mode), period)
            if ma_shift != 0:
                out = out.shift(ma_shift)

            if tf != spec.primary_tf:
                aligned = align_to_base(base_df, pd.DataFrame({"time": src["time"], "v": out}), ["v"], f"{name}__")
                out = aligned[f"{name}__v"]

            frame[name] = out
            handle_map[name] = {"default": out, "buf0": out}
            continue

        if source == "SqATR":
            out = atr(src, int(params[0]))
            if tf != spec.primary_tf:
                aligned = align_to_base(base_df, pd.DataFrame({"time": src["time"], "v": out}), ["v"], f"{name}__")
                out = aligned[f"{name}__v"]
            frame[name] = out
            handle_map[name] = {"default": out, "buf0": out}
            continue

        if source == "SqADX":
            adx_df = adx(src, int(params[0]))
            if tf != spec.primary_tf:
                aligned = align_to_base(
                    base_df,
                    pd.DataFrame({"time": src["time"], "adx": adx_df["adx"], "plus_di": adx_df["plus_di"], "minus_di": adx_df["minus_di"]}),
                    ["adx", "plus_di", "minus_di"],
                    f"{name}__",
                )
                frame[name] = aligned[f"{name}__adx"]
                handle_map[name] = {
                    "default": aligned[f"{name}__adx"],
                    "buf0": aligned[f"{name}__adx"],
                    "buf1": aligned[f"{name}__plus_di"],
                    "buf2": aligned[f"{name}__minus_di"],
                }
            else:
                frame[name] = adx_df["adx"]
                handle_map[name] = {
                    "default": adx_df["adx"],
                    "buf0": adx_df["adx"],
                    "buf1": adx_df["plus_di"],
                    "buf2": adx_df["minus_di"],
                }
            continue

        if source == "SqSuperTrend":
            period = int(params[1])
            mult = float(params[2])
            out = supertrend(src, period, mult)
            if tf != spec.primary_tf:
                aligned = align_to_base(base_df, pd.DataFrame({"time": src["time"], "v": out}), ["v"], f"{name}__")
                out = aligned[f"{name}__v"]
            frame[name] = out
            handle_map[name] = {"default": out, "buf0": out}
            continue

        if source == "SqIchimoku":
            tenkan_p = int(params[0])
            kijun_p = int(params[1])
            senkou_p = int(params[2])
            ichi = ichimoku(src, tenkan_p, kijun_p, senkou_p)

            if tf != spec.primary_tf:
                aligned = align_to_base(
                    base_df,
                    pd.DataFrame({
                        "time": src["time"],
                        "tenkan": ichi["tenkan"],
                        "kijun": ichi["kijun"],
                        "span_a": ichi["span_a"],
                        "span_b": ichi["span_b"],
                    }),
                    ["tenkan", "kijun", "span_a", "span_b"],
                    f"{name}__",
                )
                frame[name] = aligned[f"{name}__kijun"]
                handle_map[name] = {
                    "default": aligned[f"{name}__kijun"],
                    "tenkan": aligned[f"{name}__tenkan"],
                    "kijun": aligned[f"{name}__kijun"],
                    "span_a": aligned[f"{name}__span_a"],
                    "span_b": aligned[f"{name}__span_b"],
                }
            else:
                frame[name] = ichi["kijun"]
                handle_map[name] = {
                    "default": ichi["kijun"],
                    "tenkan": ichi["tenkan"],
                    "kijun": ichi["kijun"],
                    "span_a": ichi["span_a"],
                    "span_b": ichi["span_b"],
                }
            continue

        if source == "SqWPR":
            out = wpr(src, int(params[0]))
            if tf != spec.primary_tf:
                aligned = align_to_base(base_df, pd.DataFrame({"time": src["time"], "v": out}), ["v"], f"{name}__")
                out = aligned[f"{name}__v"]
            frame[name] = out
            handle_map[name] = {"default": out, "buf0": out}
            continue

        if source == "SqParabolicSAR":
            out = parabolic_sar(src, float(params[0]), float(params[1]))
            if tf != spec.primary_tf:
                aligned = align_to_base(base_df, pd.DataFrame({"time": src["time"], "v": out}), ["v"], f"{name}__")
                out = aligned[f"{name}__v"]
            frame[name] = out
            handle_map[name] = {"default": out, "buf0": out}
            continue

        if source == "SqBearsPower":
            period = int(params[0])
            price_mode = str(params[1]) if len(params) > 1 else "PRICE_LOW"
            out = bears_power(src, period, price_mode)
            if tf != spec.primary_tf:
                aligned = align_to_base(base_df, pd.DataFrame({"time": src["time"], "v": out}), ["v"], f"{name}__")
                out = aligned[f"{name}__v"]
            frame[name] = out
            handle_map[name] = {"default": out, "buf0": out}
            continue

        if source == "SqHullMovingAverage":
            period = int(params[0])
            price_mode = str(params[2]) if len(params) > 2 else "PRICE_CLOSE"
            out = hma(price_series(src, price_mode), period)
            if tf != spec.primary_tf:
                aligned = align_to_base(base_df, pd.DataFrame({"time": src["time"], "v": out}), ["v"], f"{name}__")
                out = aligned[f"{name}__v"]
            frame[name] = out
            handle_map[name] = {"default": out, "buf0": out}
            continue

        if source == "SqFibo":
            out = fibo_neg_61_8(src)
            if tf != spec.primary_tf:
                aligned = align_to_base(base_df, pd.DataFrame({"time": src["time"], "v": out}), ["v"], f"{name}__")
                out = aligned[f"{name}__v"]
            frame[name] = out
            handle_map[name] = {"default": out, "buf0": out}
            continue

        if source == "SqHighestInRange":
            out = highest_in_range(src, str(params[0]), str(params[1]))
            if tf != spec.primary_tf:
                aligned = align_to_base(base_df, pd.DataFrame({"time": src["time"], "v": out}), ["v"], f"{name}__")
                out = aligned[f"{name}__v"]
            frame[name] = out
            handle_map[name] = {"default": out, "buf0": out}
            continue

        if source == "SqLowestInRange":
            out = lowest_in_range(src, str(params[0]), str(params[1]))
            if tf != spec.primary_tf:
                aligned = align_to_base(base_df, pd.DataFrame({"time": src["time"], "v": out}), ["v"], f"{name}__")
                out = aligned[f"{name}__v"]
            frame[name] = out
            handle_map[name] = {"default": out, "buf0": out}
            continue

        if source == "SqVWAP":
            out = rolling_vwap(src, int(params[0]))
            if tf != spec.primary_tf:
                aligned = align_to_base(base_df, pd.DataFrame({"time": src["time"], "v": out}), ["v"], f"{name}__")
                out = aligned[f"{name}__v"]
            frame[name] = out
            handle_map[name] = {"default": out, "buf0": out}
            continue

        if source == "SqLinReg":
            period = int(params[0])
            price_mode = str(params[1]) if len(params) > 1 else "PRICE_CLOSE"
            out = linreg(price_series(src, price_mode), period)
            if tf != spec.primary_tf:
                aligned = align_to_base(base_df, pd.DataFrame({"time": src["time"], "v": out}), ["v"], f"{name}__")
                out = aligned[f"{name}__v"]
            frame[name] = out
            handle_map[name] = {"default": out, "buf0": out}
            continue

        if source == "SqCCI":
            period = int(params[0])
            price_mode = str(params[1]) if len(params) > 1 else "PRICE_TYPICAL"
            out = cci(src, period, price_mode)
            if tf != spec.primary_tf:
                aligned = align_to_base(base_df, pd.DataFrame({"time": src["time"], "v": out}), ["v"], f"{name}__")
                out = aligned[f"{name}__v"]
            frame[name] = out
            handle_map[name] = {"default": out, "buf0": out}
            continue

        raise ValueError(f"Buffer source not implemented in backtester: {name}:{source}")

    frame.attrs["handle_map"] = handle_map
    return frame


# ============================================================
# Expression runtime
# ============================================================

def handle_series(df: pd.DataFrame, handle_name: str, buffer_idx: Optional[int] = None) -> pd.Series:
    hm = df.attrs["handle_map"].get(handle_name)
    if hm is None:
        if handle_name in df.columns:
            return df[handle_name]
        raise KeyError(f"Handle nicht gefunden: {handle_name}")

    if buffer_idx is None:
        return hm["default"]

    key = f"buf{int(buffer_idx)}"
    if key in hm:
        return hm[key]
    if int(buffer_idx) == 0 and "default" in hm:
        return hm["default"]
    return hm["default"]


def sq_get_indicator_value(df: pd.DataFrame, handle_name: str, buffer_idx: int = 0, shift: int = 0) -> pd.Series:
    return handle_series(df, handle_name, buffer_idx).shift(int(shift))


def sq_close(df: pd.DataFrame, shift: int = 0) -> pd.Series:
    return df["close"].shift(int(shift))


def expr_cleanup(expr: str) -> str:
    x = expr
    x = x.replace("\n", " ")
    x = re.sub(r"\s+", " ", x)
    x = x.replace("true", "True").replace("false", "False")
    x = x.replace("&&", " & ").replace("||", " | ")
    x = x.replace("!( ", "~(").replace("!(", "~(")
    x = re.sub(r"NormalizeDouble\(\(double\)\s*(.*?),\s*\d+\)", r"(\1)", x)
    x = re.sub(r"NormalizeDouble\(\s*(.*?),\s*\d+\)", r"(\1)", x)
    x = x.replace("(double)", "")
    return x


def translate_expr(expr: str) -> str:
    x = expr_cleanup(expr)

    x = re.sub(
        r'sqClose\([^)]+,\s*(\d+)\)',
        lambda m: f'__SQ_CLOSE__({m.group(1)})',
        x
    )

    x = re.sub(
        r'sqGetIndicatorValue\(([A-Za-z0-9_]+),\s*(\d+),\s*(\d+)(?:,\s*\d+)?\)',
        lambda m: f'__BUF__("{m.group(1)}",{m.group(2)},{m.group(3)})',
        x
    )

    x = re.sub(
        r'sqGetIndicatorValue\(([A-Za-z0-9_]+),\s*(\d+)\)',
        lambda m: f'__BUF__("{m.group(1)}",0,{m.group(2)})',
        x
    )

    x = re.sub(
        r'changesUp\(([A-Za-z0-9_]+),\s*(\d+)\)',
        lambda m: f'__CHGUP__("{m.group(1)}",{m.group(2)})',
        x
    )

    x = re.sub(
        r'sqIsRising\(([A-Za-z0-9_]+),\s*(\d+),\s*(?:true|false),\s*([^)]+?),\s*0\)',
        lambda m: f'__RISING__("{m.group(1)}",{m.group(2)},{m.group(3).replace(" ", "")})',
        x
    )

    x = re.sub(
        r'sqIsFalling\(([A-Za-z0-9_]+),\s*(\d+),\s*(?:true|false),\s*([^)]+?),\s*0\)',
        lambda m: f'__FALLING__("{m.group(1)}",{m.group(2)},{m.group(3).replace(" ", "")})',
        x
    )

    x = re.sub(
        r'crossesDown\(([A-Za-z0-9_]+),\s*(\d+),\s*([0-9.]+),\s*(\d+)\)',
        lambda m: f'__CROSS_DOWN_LEVEL__("{m.group(1)}",{m.group(2)},{m.group(3)},{m.group(4)})',
        x
    )

    x = re.sub(
        r'indyCrossesBelow\("sqClose\(NULL,0,\s*(\d+)\)",\s*([A-Za-z0-9_]+),\s*0,\s*(\d+),\s*0,\s*0\)',
        lambda m: f'__INDY_CROSS_BELOW_CLOSE__({m.group(1)},"{m.group(2)}",{m.group(3)})',
        x
    )

    x = re.sub(
        r'sqTimeDayOfWeek\(TimeCurrent\(\)\)',
        '__MQL_DOW__()',
        x
    )

    x = re.sub(
        r'sqIchimokuKijunSenCross\(\s*1,\s*NULL,0,\s*([A-Za-z0-9_]+),\s*1,\s*2\s*\)',
        lambda m: f'__ICHI_BULL_KIJUN_CROSS__("{m.group(1)}")',
        x
    )

    return x


def eval_expr(df: pd.DataFrame, expr: str) -> pd.Series:
    x = translate_expr(expr)

    def __BUF__(name: str, buffer_idx: int, shift: int) -> pd.Series:
        return sq_get_indicator_value(df, name, int(buffer_idx), int(eval(str(shift))))

    def __SQ_CLOSE__(shift: int) -> pd.Series:
        return sq_close(df, int(eval(str(shift))))

    def __CHGUP__(name: str, shift: int) -> pd.Series:
        return changes_up(handle_series(df, name), int(shift))

    def __RISING__(name: str, bars: int, shift_expr: str) -> pd.Series:
        shift = int(eval(str(shift_expr)))
        return sq_is_rising(handle_series(df, name), int(bars), shift)

    def __FALLING__(name: str, bars: int, shift_expr: str) -> pd.Series:
        shift = int(eval(str(shift_expr)))
        return sq_is_falling(handle_series(df, name), int(bars), shift)

    def __CROSS_DOWN_LEVEL__(name: str, buffer_idx: int, level: float, shift: int) -> pd.Series:
        s = handle_series(df, name, int(buffer_idx))
        sh = int(shift)
        lvl = float(level)
        return ((s.shift(sh) < lvl) & (s.shift(sh + 1) >= lvl)).fillna(False)

    def __INDY_CROSS_BELOW_CLOSE__(close_shift: int, handle_name: str, handle_shift: int) -> pd.Series:
        lhs = df["close"].shift(int(close_shift))
        rhs = handle_series(df, handle_name).shift(int(handle_shift))
        return crosses_below_series(lhs, rhs, 0)

    def __MQL_DOW__() -> pd.Series:
        return mql_day_of_week(df["time"])

    def __ICHI_BULL_KIJUN_CROSS__(name: str) -> pd.Series:
        hm = df.attrs["handle_map"].get(name, {})
        tenkan = hm.get("tenkan")
        kijun = hm.get("kijun")
        if tenkan is None or kijun is None:
            return pd.Series(False, index=df.index)
        return crosses_above_series(tenkan, kijun, 0)

    env = {
        "__BUF__": __BUF__,
        "__SQ_CLOSE__": __SQ_CLOSE__,
        "__CHGUP__": __CHGUP__,
        "__RISING__": __RISING__,
        "__FALLING__": __FALLING__,
        "__CROSS_DOWN_LEVEL__": __CROSS_DOWN_LEVEL__,
        "__INDY_CROSS_BELOW_CLOSE__": __INDY_CROSS_BELOW_CLOSE__,
        "__MQL_DOW__": __MQL_DOW__,
        "__ICHI_BULL_KIJUN_CROSS__": __ICHI_BULL_KIJUN_CROSS__,
        "True": True,
        "False": False,
    }

    result = eval(x, {"__builtins__": {}}, env)

    if isinstance(result, pd.Series):
        return result.fillna(False)
    return pd.Series(bool(result), index=df.index)


# ============================================================
# Signals
# ============================================================

def apply_time_filters(df: pd.DataFrame, time_filters: Dict[str, Any]) -> pd.Series:
    mask = pd.Series(True, index=df.index)

    if time_filters.get("dont_trade_on_weekends"):
        mask &= ~df["time"].dt.dayofweek.isin([5, 6])

    if time_filters.get("limit_time_range"):
        t_from = str(time_filters.get("signal_time_range_from") or "")
        t_to = str(time_filters.get("signal_time_range_to") or "")
        if t_from and t_to:
            hhmm = df["time"].dt.strftime("%H:%M")
            mask &= (hhmm >= t_from) & (hhmm <= t_to)

    return mask.fillna(False)


def build_signal_frame(spec: StrategySpec, feature_df: pd.DataFrame) -> pd.DataFrame:
    df = feature_df.copy()
    time_mask = apply_time_filters(df, spec.time_filters)

    df["LongEntrySignal_raw"] = False
    df["ShortEntrySignal_raw"] = False
    df["LongExitSignal_raw"] = False
    df["ShortExitSignal_raw"] = False

    if spec.entry.get("mode") == "signals_block":
        signals_block = spec.entry.get("signals_block") or {}

        if signals_block.get("LongEntrySignal"):
            df["LongEntrySignal_raw"] = eval_expr(df, signals_block["LongEntrySignal"])

        if signals_block.get("ShortEntrySignal"):
            df["ShortEntrySignal_raw"] = eval_expr(df, signals_block["ShortEntrySignal"])

        exit_block = spec.exit.get("signals_block") or {}
        if exit_block.get("LongExitSignal"):
            df["LongExitSignal_raw"] = eval_expr(df, exit_block["LongExitSignal"])
        if exit_block.get("ShortExitSignal"):
            df["ShortExitSignal_raw"] = eval_expr(df, exit_block["ShortExitSignal"])

    elif spec.entry.get("mode") == "generic_rules":
        for rule in spec.entry.get("generic_rules") or []:
            cond = rule.get("condition_raw") or ""
            cond = re.sub(r"^\s*_sqIsBarOpen\s*==\s*true\s*&&\s*", "", cond)
            series = eval_expr(df, cond)

            acts = rule.get("actions") or []
            order_type = acts[0].get("order_type") if acts else None

            if order_type == "ORDER_TYPE_BUY":
                df["LongEntrySignal_raw"] = df["LongEntrySignal_raw"] | series
            elif order_type == "ORDER_TYPE_SELL":
                df["ShortEntrySignal_raw"] = df["ShortEntrySignal_raw"] | series

    df["LongEntrySignal"] = df["LongEntrySignal_raw"] & time_mask
    df["ShortEntrySignal"] = df["ShortEntrySignal_raw"] & time_mask
    df["LongExitSignal"] = df["LongExitSignal_raw"]
    df["ShortExitSignal"] = df["ShortExitSignal_raw"]

    return df


# ============================================================
# Risk / execution helpers
# ============================================================

def classify_risk_model(risk: Dict[str, Any], direction: str) -> Dict[str, Any]:
    for m in risk.get("models", []):
        ot = m.get("order_type")
        if direction == "BUY" and ot == "ORDER_TYPE_BUY":
            return m
        if direction == "SELL" and ot == "ORDER_TYPE_SELL":
            return m
    return {"sl_model": {"type": None}, "pt_model": {"type": None}}


def resolve_value_ref(x: Any, inputs: Dict[str, Any]) -> Any:
    if isinstance(x, (int, float, bool)):
        return x
    if isinstance(x, str):
        return inputs.get(x, x)
    return x


def compute_sl_tp(spec: StrategySpec, direction: str, entry_price: float, row: pd.Series, point: float) -> Tuple[Optional[float], Optional[float]]:
    model = classify_risk_model(spec.risk, direction)
    sl_info = model.get("sl_model") or {"type": None}
    pt_info = model.get("pt_model") or {"type": None}

    sl = None
    pt = None

    if sl_info.get("type") == "fixed_points":
        val = resolve_value_ref(sl_info.get("value_ref"), spec.inputs)
        if isinstance(val, (int, float)):
            delta = float(val) * point
            sl = entry_price - delta if direction == "BUY" else entry_price + delta

    elif sl_info.get("type") == "indicator_multiple":
        coef = resolve_value_ref(sl_info.get("coef"), spec.inputs)
        ref = sl_info.get("indicator_ref")
        if isinstance(coef, (int, float)) and ref in row.index:
            base = row.get(ref, np.nan)
            if not pd.isna(base):
                delta = float(coef) * float(base)
                sl = entry_price - delta if direction == "BUY" else entry_price + delta

    if pt_info.get("type") == "fixed_points":
        val = resolve_value_ref(pt_info.get("value_ref"), spec.inputs)
        if isinstance(val, (int, float)):
            delta = float(val) * point
            pt = entry_price + delta if direction == "BUY" else entry_price - delta

    elif pt_info.get("type") == "indicator_multiple":
        coef = resolve_value_ref(pt_info.get("coef"), spec.inputs)
        ref = pt_info.get("indicator_ref")
        if isinstance(coef, (int, float)) and ref in row.index:
            base = row.get(ref, np.nan)
            if not pd.isna(base):
                delta = float(coef) * float(base)
                pt = entry_price + delta if direction == "BUY" else entry_price - delta

    return sl, pt


def compute_size_lots() -> float:
    return float(FIXED_LOTS_VALUE)


def spread_adjustment(row: pd.Series, point: float, spread_mode: str) -> float:
    if spread_mode == "zero":
        return 0.0
    return float(row.get("spread", 0.0) or 0.0) * point


def entry_price_from_bar(row: pd.Series, direction: str, point: float, spread_mode: str) -> float:
    px = float(row["open"])
    spr = spread_adjustment(row, point, spread_mode)
    return px + spr if direction == "BUY" else px - spr


def pnl_money(symbol: str, direction: str, entry_price: float, exit_price: float, lots: float) -> float:
    contract = infer_contract_size(symbol)
    raw = (exit_price - entry_price) if direction == "BUY" else (entry_price - exit_price)
    return raw * contract * lots


def intrabar_exit(direction: str, row: pd.Series, sl: Optional[float], pt: Optional[float], mode: str) -> Tuple[Optional[float], Optional[str]]:
    high = float(row["high"])
    low = float(row["low"])

    if direction == "BUY":
        sl_hit = sl is not None and low <= sl
        pt_hit = pt is not None and high >= pt
    else:
        sl_hit = sl is not None and high >= sl
        pt_hit = pt is not None and low <= pt

    if sl_hit and pt_hit:
        if mode == "optimistic":
            return pt, "SL_AND_TP_SAME_BAR_TP_FIRST"
        return sl, "SL_AND_TP_SAME_BAR_SL_FIRST"
    if sl_hit:
        return sl, "STOP_LOSS"
    if pt_hit:
        return pt, "TAKE_PROFIT"
    return None, None


# ============================================================
# Backtest core
# ============================================================

@dataclass
class Trade:
    strategy_id: str
    symbol: str
    direction: str
    entry_time: str
    exit_time: Optional[str]
    entry_price: float
    exit_price: Optional[float]
    sl_price: Optional[float]
    tp_price: Optional[float]
    size_lots: float
    pnl_money: Optional[float]
    bars_held: int
    exit_reason: Optional[str]


def max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    rm = equity.cummax()
    dd = equity - rm
    return float(dd.min())


def build_summary(spec: StrategySpec, trades_df: pd.DataFrame, equity_df: pd.DataFrame, initial_capital: float) -> Dict[str, Any]:
    if trades_df.empty:
        return {
            "strategy_id": spec.strategy_id,
            "symbol": spec.symbol,
            "timeframe": spec.primary_tf,
            "direction": spec.direction,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "net_pnl_money": 0.0,
            "gross_profit_money": 0.0,
            "gross_loss_money": 0.0,
            "profit_factor": 0.0,
            "avg_trade_money": 0.0,
            "max_drawdown_money": 0.0,
            "initial_capital": initial_capital,
            "final_equity": initial_capital,
            "avg_lots": 0.0,
            "min_lots": 0.0,
            "max_lots": 0.0,
        }

    wins = int((trades_df["pnl_money"] > 0).sum())
    losses = int((trades_df["pnl_money"] < 0).sum())
    gp = float(trades_df.loc[trades_df["pnl_money"] > 0, "pnl_money"].sum())
    gl = float(-trades_df.loc[trades_df["pnl_money"] < 0, "pnl_money"].sum())
    net = float(trades_df["pnl_money"].sum())
    final_eq = initial_capital + net
    pf = gp / gl if gl > 0 else (999999.0 if gp > 0 else 0.0)

    return {
        "strategy_id": spec.strategy_id,
        "symbol": spec.symbol,
        "timeframe": spec.primary_tf,
        "direction": spec.direction,
        "trades": int(len(trades_df)),
        "wins": wins,
        "losses": losses,
        "win_rate": float(wins / len(trades_df)),
        "net_pnl_money": net,
        "gross_profit_money": gp,
        "gross_loss_money": gl,
        "profit_factor": pf,
        "avg_trade_money": float(trades_df["pnl_money"].mean()),
        "max_drawdown_money": max_drawdown(equity_df["equity"]),
        "initial_capital": initial_capital,
        "final_equity": final_eq,
        "avg_lots": float(trades_df["size_lots"].mean()),
        "min_lots": float(trades_df["size_lots"].min()),
        "max_lots": float(trades_df["size_lots"].max()),
    }


def run_strategy(
    spec: StrategySpec,
    ohlc_raw_root: Path,
    initial_capital: float,
    spread_mode: str,
    intrabar_mode: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, Any], Dict[str, Any]]:
    feature_df = build_feature_frame(spec, ohlc_raw_root)
    signal_df = build_signal_frame(spec, feature_df)
    point = infer_point(spec.symbol, float(signal_df["close"].median()))

    equity = initial_capital
    trades: List[Trade] = []
    equity_curve: List[Dict[str, Any]] = []
    daily_trade_count: Dict[str, int] = {}

    diagnostics = {
        "strategy_id": spec.strategy_id,
        "symbol": spec.symbol,
        "primary_tf": spec.primary_tf,
        "secondary_tfs": spec.secondary_tfs,
        "force_fixed_lots": FORCE_FIXED_LOTS,
        "fixed_lots_value": FIXED_LOTS_VALUE,
        "support_summary": spec.support_summary,
        "functions_used": spec.functions_used,
        "used_indicators": spec.used_indicators,
        "signals_true_count": {
            "LongEntrySignal_raw": int(signal_df["LongEntrySignal_raw"].sum()),
            "ShortEntrySignal_raw": int(signal_df["ShortEntrySignal_raw"].sum()),
            "LongEntrySignal": int(signal_df["LongEntrySignal"].sum()),
            "ShortEntrySignal": int(signal_df["ShortEntrySignal"].sum()),
            "LongExitSignal": int(signal_df["LongExitSignal"].sum()),
            "ShortExitSignal": int(signal_df["ShortExitSignal"].sum()),
        },
        "buffer_nan_ratio": {},
    }

    for c in [x["id"] for x in spec.buffers if x["id"] in signal_df.columns]:
        diagnostics["buffer_nan_ratio"][c] = float(signal_df[c].isna().mean())

    pos = None

    for i in range(1, len(signal_df)):
        row = signal_df.iloc[i]
        prev = signal_df.iloc[i - 1]

        t = row["time"]
        day_key = t.strftime("%Y-%m-%d")
        equity_curve.append({"time": t.isoformat(), "equity": equity})

        if pos is not None:
            hhmm = t.strftime("%H:%M")
            dow = t.dayofweek

            if spec.time_filters.get("exit_at_end_of_day") and spec.time_filters.get("eod_exit_time") == hhmm:
                exit_px = float(row["open"])
                pnl = pnl_money(spec.symbol, pos["direction"], pos["entry_price"], exit_px, pos["size_lots"])
                equity += pnl
                trades.append(Trade(
                    strategy_id=spec.strategy_id,
                    symbol=spec.symbol,
                    direction=pos["direction"],
                    entry_time=pos["entry_time"].isoformat(),
                    exit_time=t.isoformat(),
                    entry_price=pos["entry_price"],
                    exit_price=exit_px,
                    sl_price=pos["sl_price"],
                    tp_price=pos["tp_price"],
                    size_lots=pos["size_lots"],
                    pnl_money=pnl,
                    bars_held=i - pos["entry_idx"],
                    exit_reason="EXIT_AT_END_OF_DAY",
                ))
                pos = None
                continue

            if spec.time_filters.get("exit_on_friday") and dow == 4 and spec.time_filters.get("friday_exit_time") == hhmm:
                exit_px = float(row["open"])
                pnl = pnl_money(spec.symbol, pos["direction"], pos["entry_price"], exit_px, pos["size_lots"])
                equity += pnl
                trades.append(Trade(
                    strategy_id=spec.strategy_id,
                    symbol=spec.symbol,
                    direction=pos["direction"],
                    entry_time=pos["entry_time"].isoformat(),
                    exit_time=t.isoformat(),
                    entry_price=pos["entry_price"],
                    exit_price=exit_px,
                    sl_price=pos["sl_price"],
                    tp_price=pos["tp_price"],
                    size_lots=pos["size_lots"],
                    pnl_money=pnl,
                    bars_held=i - pos["entry_idx"],
                    exit_reason="EXIT_ON_FRIDAY",
                ))
                pos = None
                continue

        if pos is not None:
            exit_px, reason = intrabar_exit(pos["direction"], row, pos["sl_price"], pos["tp_price"], intrabar_mode)

            if exit_px is None:
                if pos["direction"] == "BUY" and bool(prev["LongExitSignal"]):
                    exit_px = float(row["open"])
                    reason = "RULE_EXIT"
                elif pos["direction"] == "SELL" and bool(prev["ShortExitSignal"]):
                    exit_px = float(row["open"])
                    reason = "RULE_EXIT"

            if exit_px is not None:
                pnl = pnl_money(spec.symbol, pos["direction"], pos["entry_price"], float(exit_px), pos["size_lots"])
                equity += pnl
                trades.append(Trade(
                    strategy_id=spec.strategy_id,
                    symbol=spec.symbol,
                    direction=pos["direction"],
                    entry_time=pos["entry_time"].isoformat(),
                    exit_time=t.isoformat(),
                    entry_price=pos["entry_price"],
                    exit_price=float(exit_px),
                    sl_price=pos["sl_price"],
                    tp_price=pos["tp_price"],
                    size_lots=pos["size_lots"],
                    pnl_money=pnl,
                    bars_held=i - pos["entry_idx"],
                    exit_reason=reason,
                ))
                pos = None
                continue

        if pos is not None:
            continue

        max_per_day = int(spec.time_filters.get("max_trades_per_day") or 999999)
        if daily_trade_count.get(day_key, 0) >= max_per_day:
            continue

        enter_long = spec.direction in {"BUY", "BOTH"} and bool(prev["LongEntrySignal"])
        enter_short = spec.direction in {"SELL", "BOTH"} and bool(prev["ShortEntrySignal"])

        if enter_long:
            entry_px = entry_price_from_bar(row, "BUY", point, spread_mode)
            sl, pt = compute_sl_tp(spec, "BUY", entry_px, prev, point)
            size_lots = compute_size_lots()

            pos = {
                "direction": "BUY",
                "entry_price": float(entry_px),
                "entry_time": t,
                "entry_idx": i,
                "sl_price": None if sl is None else float(sl),
                "tp_price": None if pt is None else float(pt),
                "size_lots": float(size_lots),
            }
            daily_trade_count[day_key] = daily_trade_count.get(day_key, 0) + 1
            continue

        if enter_short:
            entry_px = entry_price_from_bar(row, "SELL", point, spread_mode)
            sl, pt = compute_sl_tp(spec, "SELL", entry_px, prev, point)
            size_lots = compute_size_lots()

            pos = {
                "direction": "SELL",
                "entry_price": float(entry_px),
                "entry_time": t,
                "entry_idx": i,
                "sl_price": None if sl is None else float(sl),
                "tp_price": None if pt is None else float(pt),
                "size_lots": float(size_lots),
            }
            daily_trade_count[day_key] = daily_trade_count.get(day_key, 0) + 1
            continue

    if pos is not None and len(signal_df) > 0:
        last = signal_df.iloc[-1]
        exit_px = float(last["close"])
        pnl = pnl_money(spec.symbol, pos["direction"], pos["entry_price"], exit_px, pos["size_lots"])
        equity += pnl
        trades.append(Trade(
            strategy_id=spec.strategy_id,
            symbol=spec.symbol,
            direction=pos["direction"],
            entry_time=pos["entry_time"].isoformat(),
            exit_time=last["time"].isoformat(),
            entry_price=pos["entry_price"],
            exit_price=exit_px,
            sl_price=pos["sl_price"],
            tp_price=pos["tp_price"],
            size_lots=pos["size_lots"],
            pnl_money=pnl,
            bars_held=len(signal_df) - 1 - pos["entry_idx"],
            exit_reason="FORCED_LAST_BAR_EXIT",
        ))

    trades_df = pd.DataFrame([asdict(x) for x in trades])
    equity_df = pd.DataFrame(equity_curve)
    if equity_df.empty and len(signal_df) > 0:
        equity_df = pd.DataFrame([{"time": signal_df.iloc[0]["time"].isoformat(), "equity": initial_capital}])

    summary = build_summary(spec, trades_df, equity_df, initial_capital)

    diagnostics["trade_count"] = int(len(trades_df))
    diagnostics["daily_trade_days"] = int(len(daily_trade_count))
    diagnostics["max_daily_trades_hit"] = int(max(daily_trade_count.values())) if daily_trade_count else 0
    diagnostics["avg_lots"] = float(trades_df["size_lots"].mean()) if not trades_df.empty else 0.0
    diagnostics["min_lots"] = float(trades_df["size_lots"].min()) if not trades_df.empty else 0.0
    diagnostics["max_lots"] = float(trades_df["size_lots"].max()) if not trades_df.empty else 0.0

    return trades_df, equity_df, signal_df, summary, diagnostics


# ============================================================
# Save outputs
# ============================================================

def save_outputs(
    out_root: Path,
    spec: StrategySpec,
    trades_df: pd.DataFrame,
    equity_df: pd.DataFrame,
    signal_df: pd.DataFrame,
    summary: Dict[str, Any],
    diagnostics: Dict[str, Any],
) -> Dict[str, str]:
    folder = out_root / spec.symbol / spec.strategy_id
    ensure_dir(folder)

    trades_file = safe_write_df(trades_df, folder / "trades.parquet", folder / "trades.csv")
    equity_file = safe_write_df(equity_df, folder / "equity.parquet", folder / "equity.csv")

    signal_cols = [
        "time", "open", "high", "low", "close", "spread",
        "LongEntrySignal_raw", "ShortEntrySignal_raw",
        "LongEntrySignal", "ShortEntrySignal",
        "LongExitSignal", "ShortExitSignal",
    ] + [x["id"] for x in spec.buffers if x["id"] in signal_df.columns]
    signal_cols = [c for c in signal_cols if c in signal_df.columns]

    signals_file = safe_write_df(signal_df[signal_cols], folder / "signals.parquet", folder / "signals.csv")

    save_json(folder / "summary.json", summary)
    save_json(folder / "diagnostics.json", diagnostics)
    save_json(folder / "compiled_strategy_snapshot.json", {
        "strategy_id": spec.strategy_id,
        "symbol": spec.symbol,
        "direction": spec.direction,
        "primary_tf": spec.primary_tf,
        "secondary_tfs": spec.secondary_tfs,
        "time_filters": spec.time_filters,
        "inputs": spec.inputs,
        "buffers": spec.buffers,
        "entry": spec.entry,
        "exit": spec.exit,
        "risk": spec.risk,
        "force_fixed_lots": FORCE_FIXED_LOTS,
        "fixed_lots_value": FIXED_LOTS_VALUE,
    })

    return {
        "folder": str(folder),
        "trades_file": trades_file,
        "equity_file": equity_file,
        "signals_file": signals_file,
        "summary_file": str(folder / "summary.json"),
        "diagnostics_file": str(folder / "diagnostics.json"),
    }


# ============================================================
# Runner
# ============================================================

def discover_compiled_files(compiled_root: Path) -> List[Path]:
    if not compiled_root.exists():
        return []
    return sorted(compiled_root.rglob("*.compiled.json"))


def run_all(
    compiled_root: Path,
    ohlc_raw_root: Path,
    out_root: Path,
    strategy_filter: Optional[str],
    symbol_filter: Optional[str],
    initial_capital: float,
    spread_mode: str,
    intrabar_mode: str,
) -> Dict[str, Any]:
    ensure_dir(out_root)
    compiled_files = discover_compiled_files(compiled_root)

    if strategy_filter:
        compiled_files = [p for p in compiled_files if strategy_filter.lower() in p.stem.lower()]

    results = []
    errors = []

    for fp in compiled_files:
        try:
            spec = load_compiled_strategy(fp)

            if symbol_filter and spec.symbol.lower() != symbol_filter.lower():
                continue

            if spec.primary_tf != SUPPORTED_PRIMARY_TIMEFRAME:
                continue

            trades_df, equity_df, signal_df, summary, diagnostics = run_strategy(
                spec=spec,
                ohlc_raw_root=ohlc_raw_root,
                initial_capital=initial_capital,
                spread_mode=spread_mode,
                intrabar_mode=intrabar_mode,
            )

            outputs = save_outputs(out_root, spec, trades_df, equity_df, signal_df, summary, diagnostics)

            results.append({
                "strategy_id": spec.strategy_id,
                "symbol": spec.symbol,
                "compiled_file": str(fp),
                "summary": summary,
                "outputs": outputs,
            })

        except Exception as exc:
            errors.append({
                "compiled_file": str(fp),
                "error": repr(exc),
            })

    index = {
        "meta": {
            "compiled_root": str(compiled_root),
            "ohlc_raw_root": str(ohlc_raw_root),
            "out_root": str(out_root),
            "initial_capital": initial_capital,
            "spread_mode": spread_mode,
            "intrabar_mode": intrabar_mode,
            "force_fixed_lots": FORCE_FIXED_LOTS,
            "fixed_lots_value": FIXED_LOTS_VALUE,
        },
        "results": results,
        "errors": errors,
    }
    save_json(out_root / "_index.json", index)
    return index


# ============================================================
# CLI
# ============================================================

def main() -> int:
    parser = argparse.ArgumentParser(description="Backtester V2 for compiled_strategies V4")
    parser.add_argument("--data-root", type=str, default=None)
    parser.add_argument("--compiled-root", type=str, default=None)
    parser.add_argument("--ohlc-root", type=str, default=None)
    parser.add_argument("--out-root", type=str, default=None)
    parser.add_argument("--strategy-filter", type=str, default=None)
    parser.add_argument("--symbol-filter", type=str, default=None)
    parser.add_argument("--initial-capital", type=float, default=DEFAULT_INITIAL_CAPITAL)
    parser.add_argument("--spread-mode", type=str, default=DEFAULT_SPREAD_MODE, choices=["from_data", "zero"])
    parser.add_argument("--intrabar-mode", type=str, default=DEFAULT_INTRABAR_MODE, choices=["pessimistic", "optimistic"])
    args = parser.parse_args()

    script_path = Path(__file__).resolve()

    if args.data_root:
        data_root = Path(args.data_root).expanduser().resolve()
    else:
        data_root = find_default_data_root(script_path)

    if data_root is None or not data_root.exists():
        print("FEHLER: Data root konnte nicht gefunden werden.", file=sys.stderr)
        return 1

    paths = derive_paths(data_root)

    compiled_root = Path(args.compiled_root).expanduser().resolve() if args.compiled_root else paths["compiled_root"]
    ohlc_raw_root = Path(args.ohlc_root).expanduser().resolve() if args.ohlc_root else paths["ohlc_raw"]
    out_root = Path(args.out_root).expanduser().resolve() if args.out_root else paths["out_root"]

    if not compiled_root.exists():
        print(f"FEHLER: compiled_strategies nicht gefunden: {compiled_root}", file=sys.stderr)
        return 1

    if not ohlc_raw_root.exists():
        print(f"FEHLER: OHLC root nicht gefunden: {ohlc_raw_root}", file=sys.stderr)
        return 1

    index = run_all(
        compiled_root=compiled_root,
        ohlc_raw_root=ohlc_raw_root,
        out_root=out_root,
        strategy_filter=args.strategy_filter,
        symbol_filter=args.symbol_filter,
        initial_capital=args.initial_capital,
        spread_mode=args.spread_mode,
        intrabar_mode=args.intrabar_mode,
    )

    print("FERTIG")
    print(f"Compiled root:    {compiled_root}")
    print(f"OHLC root:        {ohlc_raw_root}")
    print(f"Output root:      {out_root}")
    print(f"Fixed lots:       {FIXED_LOTS_VALUE}")
    print(f"Ergebnisse:       {len(index['results'])}")
    print(f"Fehler:           {len(index['errors'])}")
    print(f"Index:            {out_root / '_index.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())