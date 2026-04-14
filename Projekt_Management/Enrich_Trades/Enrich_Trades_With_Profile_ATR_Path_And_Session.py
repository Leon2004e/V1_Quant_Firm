# FTMO/1.Data_Center/Data_Operations/Enrich_Trades/Enrich_Trades_Add_Full_Market_Context.py
#
# Zweck
# - Liest bereits enriched Trade-CSV(s) aus:
#     1.Data_Center/Data/Strategy_Data/Backtest_Trades_Data_Enriched
# - Lädt OHLC (default M15) pro Symbol aus:
#     1.Data_Center/Data/Regime_Data/OHLC/M15
# - Schreibt pro Trade "Full Market Context" + SL/TP/Exit-Qualität in eine neue CSV:
#     1.Data_Center/Data/Strategy_Data/Backtest_Trades_Data_Enriched_FullContext
#
# Enthält:
#  - Day High/Low inkl. Uhrzeit + Day Open/Close/Range/Return + Entry/Exit Position + Distanzen (+ optional in R)
#  - Session (Asia/London/NewYork) High/Low inkl. Uhrzeit + Open/Close/Range/Return (für den Entry-Tag)
#  - Entry/Exit Position in jeder Session + Distanzen zu Session-Extremes (+ optional in R)
#  - Intratrade Path: MFE/MAE in price + R + Zeitpunkte + time_to_mfe/mae
#  - Trade-vs-Day Timing: trade_duration_min, time_to_day_high/low_after_entry
#  - Calendar: entry_weekday/hour/minute/weekofyear/month/quarter
#  - Weekly/Monthly "to entry" Structure (kein Lookahead): high/low + Uhrzeit + open + range + entry_pos + Distanzen (+ optional in R)
#  - SL/TP/Exit: nutzt vorhandene Spalten; Fallbacks wenn fehlend (z.B. SL-Dist aus exit_level_price bei exit_type=sl)
#
# Voraussetzungen OHLC:
#  - Spalten: time (UTC), open, high, low, close
#
# Dependencies: pandas, numpy

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Optional, List, Tuple

import numpy as np
import pandas as pd


# =========================
# PROJECT STRUCTURE
# =========================
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = PROJECT_ROOT / "1.Data_Center" / "Data"

DEFAULT_TRADES_IN_DIR = DATA_ROOT / "Strategy_Data" / "Backtest_Trades_Data_Enriched"
DEFAULT_OUT_DIR = DATA_ROOT / "Strategy_Data" / "Backtest_Trades_Data_Enriched_FullContext"

DEFAULT_OHLC_DIR = DATA_ROOT / "Regime_Data" / "OHLC" / "M15"
DEFAULT_OHLC_TF_TAG = "M15"

# Sessions in UTC hours
DEFAULT_SESSIONS = {
    "Asia": (0, 8),
    "London": (8, 16),
    "NewYork": (16, 24),
}

PRICE_TOL = 1e-6


# =========================
# TIME / PARSE HELPERS
# =========================
def _safe_to_datetime_series(s: pd.Series) -> pd.Series:
    # handles ISO + dd.mm.yyyy
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True, utc=False)
    if dt.isna().any():
        # second pass "mixed"
        dt2 = pd.to_datetime(s, errors="coerce", format="mixed", dayfirst=True, utc=False)
        dt = dt.where(~dt.isna(), dt2)
    return dt


def _as_utc(ts: pd.Timestamp) -> pd.Timestamp:
    # interpret naive timestamps as UTC
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _day_key(ts: pd.Timestamp) -> pd.Timestamp:
    return _as_utc(ts).floor("D")


def _week_key(ts: pd.Timestamp) -> pd.Period:
    # ISO week period (Mon-Sun) in UTC
    return _as_utc(ts).to_period("W")


def _month_key(ts: pd.Timestamp) -> pd.Period:
    return _as_utc(ts).to_period("M")


def _to_num(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        v = float(x)
        if np.isnan(v):
            return None
        return v
    except Exception:
        return None


def _to_int(x) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        v = int(float(x))
        return v
    except Exception:
        return None


def _fmt_ts(ts: Optional[pd.Timestamp]) -> Optional[str]:
    if ts is None or pd.isna(ts):
        return None
    try:
        return _as_utc(ts).isoformat()
    except Exception:
        return str(ts)


def side_sign(trade_type: str) -> int:
    t = str(trade_type).upper()
    if "BUY" in t:
        return +1
    if "SELL" in t:
        return -1
    raise ValueError(f"unknown trade type: {trade_type}")


# =========================
# OHLC LOADING
# =========================
def _find_ohlc_file(ohlc_dir: Path, symbol: str, tf_tag: str) -> Optional[Path]:
    direct = ohlc_dir / f"{symbol}_{tf_tag}.csv"
    if direct.exists():
        return direct

    patterns = [
        f"{symbol}_{tf_tag}*.csv",
        f"{symbol}*{tf_tag}*.csv",
        f"*{symbol}*{tf_tag}*.csv",
        f"{symbol}*.csv",
        f"*{symbol}*.csv",
    ]
    cands: List[Path] = []
    for pat in patterns:
        cands.extend(sorted(ohlc_dir.glob(pat)))

    uniq: List[Path] = []
    seen = set()
    for p in cands:
        if p.is_file() and p not in seen:
            seen.add(p)
            uniq.append(p)
    if not uniq:
        return None

    for p in uniq:
        if tf_tag.upper() in p.stem.upper():
            return p
    return uniq[0]


def load_ohlc(ohlc_dir: Path, symbol: str, tf_tag: str) -> pd.DataFrame:
    fp = _find_ohlc_file(ohlc_dir, symbol, tf_tag)
    if fp is None:
        raise FileNotFoundError(f"OHLC not found for symbol={symbol} in {ohlc_dir}")

    df = pd.read_csv(fp)
    need = {"time", "open", "high", "low", "close"}
    miss = need - set(df.columns)
    if miss:
        raise ValueError(f"OHLC missing columns {sorted(miss)} in {fp}")

    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    if df["time"].isna().any():
        raise ValueError(f"OHLC time unparsable in {fp}")

    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.sort_values("time").drop_duplicates(subset=["time"], keep="last").reset_index(drop=True)
    df.attrs["__file__"] = str(fp)
    return df


# =========================
# SESSION AGG (ENTRY DAY)
# =========================
def _session_name_from_hour(h: int, sessions: Dict[str, Tuple[int, int]]) -> str:
    for name, (a, b) in sessions.items():
        if a <= h < b:
            return name
    return list(sessions.keys())[-1]


def _slice_session(day_df: pd.DataFrame, sess: Tuple[int, int]) -> pd.DataFrame:
    a, b = sess
    h = day_df["time"].dt.hour
    return day_df[(h >= a) & (h < b)]


def _agg_ohlc_block(block: pd.DataFrame) -> Dict[str, object]:
    # returns open/close/high/low + times of high/low
    if block.empty:
        return {
            "open": None,
            "close": None,
            "high": None,
            "high_time": None,
            "low": None,
            "low_time": None,
            "range": None,
            "return": None,
        }

    # open = first open, close = last close
    open_ = float(block.iloc[0]["open"])
    close_ = float(block.iloc[-1]["close"])

    # high/low + timestamps
    i_high = int(block["high"].astype(float).idxmax())
    i_low = int(block["low"].astype(float).idxmin())
    high_ = float(block.loc[i_high, "high"])
    low_ = float(block.loc[i_low, "low"])
    high_t = block.loc[i_high, "time"]
    low_t = block.loc[i_low, "time"]

    rng = high_ - low_
    ret = close_ - open_
    return {
        "open": open_,
        "close": close_,
        "high": high_,
        "high_time": high_t,
        "low": low_,
        "low_time": low_t,
        "range": rng,
        "return": ret,
    }


def _pos_in_range(price: float, low: Optional[float], high: Optional[float]) -> Optional[float]:
    if low is None or high is None:
        return None
    rng = float(high) - float(low)
    if rng == 0.0 or not np.isfinite(rng):
        return None
    return float((price - float(low)) / rng)


def _dist_to_high(price: float, high: Optional[float]) -> Optional[float]:
    if high is None:
        return None
    return float(float(high) - float(price))


def _dist_to_low(price: float, low: Optional[float]) -> Optional[float]:
    if low is None:
        return None
    return float(float(price) - float(low))


def _norm_r(x: Optional[float], sl_dist: Optional[float]) -> Optional[float]:
    if x is None or sl_dist is None:
        return None
    if sl_dist == 0.0 or not np.isfinite(sl_dist):
        return None
    return float(x) / float(sl_dist)


# =========================
# TRADE SL/TP/EXIT FALLBACKS
# =========================
def ensure_sl_tp_exit_fields(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()

    # normalize time fields if needed
    if "open_time" not in d.columns and "Open time" in d.columns:
        d["open_time"] = d["Open time"]
    if "close_time" not in d.columns and "Close time" in d.columns:
        d["close_time"] = d["Close time"]
    d["open_time"] = _safe_to_datetime_series(d["open_time"])
    d["close_time"] = _safe_to_datetime_series(d["close_time"])

    # normalize core fields if needed
    if "type" not in d.columns and "Type" in d.columns:
        d["type"] = d["Type"]
    if "symbol" not in d.columns and "Symbol" in d.columns:
        d["symbol"] = d["Symbol"]
    if "open_price" not in d.columns and "Open price" in d.columns:
        d["open_price"] = d["Open price"]
    if "close_price" not in d.columns and "Close price" in d.columns:
        d["close_price"] = d["Close price"]

    d["type"] = d["type"].astype(str).str.strip().str.upper()
    d["symbol"] = d["symbol"].astype(str).str.strip().str.upper()
    d["open_price"] = pd.to_numeric(d["open_price"], errors="coerce")
    d["close_price"] = pd.to_numeric(d["close_price"], errors="coerce")

    # optional columns
    for c in [
        "init_sl_dist_price",
        "init_tp_dist_price",
        "init_sl_price",
        "init_tp_price",
        "exit_level_price",
        "realized_exit_dist_price",
        "r_multiple_price",
    ]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    if "exit_type" in d.columns:
        d["exit_type"] = d["exit_type"].astype(str).replace({"nan": np.nan})
    else:
        d["exit_type"] = np.nan

    # Fallback: if init_sl_dist_price missing, but exit_type=sl and exit_level_price exists:
    # init_sl_dist_price = abs(entry - exit_level_price)
    need_sl = ("init_sl_dist_price" not in d.columns) or d["init_sl_dist_price"].isna().any()
    if "init_sl_dist_price" not in d.columns:
        d["init_sl_dist_price"] = np.nan

    if "exit_level_price" in d.columns:
        mask = d["init_sl_dist_price"].isna() & (d["exit_type"].astype(str).str.lower() == "sl") & d["exit_level_price"].notna()
        if mask.any():
            d.loc[mask, "init_sl_dist_price"] = (d.loc[mask, "open_price"] - d.loc[mask, "exit_level_price"]).abs()

    # Fallback init_sl_price if missing and dist exists
    if "init_sl_price" not in d.columns:
        d["init_sl_price"] = np.nan
    mask = d["init_sl_price"].isna() & d["init_sl_dist_price"].notna()
    if mask.any():
        sgn = d.loc[mask, "type"].map(side_sign)
        d.loc[mask, "init_sl_price"] = d.loc[mask, "open_price"] - sgn * d.loc[mask, "init_sl_dist_price"]

    # Fallback r_multiple_price if missing and sl_dist exists
    if "r_multiple_price" not in d.columns:
        d["r_multiple_price"] = np.nan
    mask = d["r_multiple_price"].isna() & d["init_sl_dist_price"].notna() & (d["init_sl_dist_price"] != 0.0)
    if mask.any():
        sgn = d.loc[mask, "type"].map(side_sign)
        move = sgn * (d.loc[mask, "close_price"] - d.loc[mask, "open_price"])
        d.loc[mask, "r_multiple_price"] = move / d.loc[mask, "init_sl_dist_price"].replace(0.0, np.nan)

    return d


# =========================
# CONTEXT COMPUTATION (PER SYMBOL)
# =========================
def build_day_cache(ohlc: pd.DataFrame) -> Dict[pd.Timestamp, pd.DataFrame]:
    # map day_key -> sub-dataframe of that day
    day_keys = ohlc["time"].dt.floor("D")
    cache: Dict[pd.Timestamp, pd.DataFrame] = {}
    for dk, g in ohlc.groupby(day_keys, sort=True):
        cache[dk] = g.reset_index(drop=True)
    return cache


def build_week_bounds(ohlc: pd.DataFrame) -> Dict[pd.Period, Tuple[pd.Timestamp, pd.Timestamp]]:
    # week period -> [start, end)
    # use UTC index
    t = ohlc["time"]
    periods = t.dt.to_period("W")
    bounds: Dict[pd.Period, Tuple[pd.Timestamp, pd.Timestamp]] = {}
    for p in periods.unique():
        start = p.start_time.tz_localize("UTC") if p.start_time.tzinfo is None else p.start_time.tz_convert("UTC")
        end = p.end_time.tz_localize("UTC") if p.end_time.tzinfo is None else p.end_time.tz_convert("UTC")
        # pandas Period end_time is inclusive end-of-period (23:59:59.999...). use +1ns to make half-open
        end = end + pd.Timedelta(nanoseconds=1)
        bounds[p] = (start, end)
    return bounds


def build_month_bounds(ohlc: pd.DataFrame) -> Dict[pd.Period, Tuple[pd.Timestamp, pd.Timestamp]]:
    t = ohlc["time"]
    periods = t.dt.to_period("M")
    bounds: Dict[pd.Period, Tuple[pd.Timestamp, pd.Timestamp]] = {}
    for p in periods.unique():
        start = p.start_time.tz_localize("UTC") if p.start_time.tzinfo is None else p.start_time.tz_convert("UTC")
        end = p.end_time.tz_localize("UTC") if p.end_time.tzinfo is None else p.end_time.tz_convert("UTC")
        end = end + pd.Timedelta(nanoseconds=1)
        bounds[p] = (start, end)
    return bounds


def slice_ohlc(ohlc: pd.DataFrame, t0: pd.Timestamp, t1: pd.Timestamp) -> pd.DataFrame:
    # ohlc['time'] is utc
    tt = ohlc["time"]
    i0 = int(tt.searchsorted(t0, side="left"))
    i1 = int(tt.searchsorted(t1, side="right"))
    return ohlc.iloc[i0:i1]


# =========================
# MAIN ENRICH
# =========================
def enrich_full_context_for_file(
    trades_fp: Path,
    out_dir: Path,
    ohlc_dir: Path,
    tf_tag: str,
    sessions: Dict[str, Tuple[int, int]],
) -> None:
    df = pd.read_csv(trades_fp)
    df["__source_file"] = trades_fp.name

    df = ensure_sl_tp_exit_fields(df)

    # calendar features
    df["entry_hour"] = df["open_time"].dt.hour
    df["entry_minute"] = df["open_time"].dt.minute
    df["entry_weekday"] = df["open_time"].dt.weekday
    # ISO week-of-year (pandas >=1.1)
    try:
        df["entry_week_of_year"] = df["open_time"].dt.isocalendar().week.astype(int)
    except Exception:
        df["entry_week_of_year"] = df["open_time"].dt.week
    df["entry_month"] = df["open_time"].dt.month
    df["entry_quarter"] = df["open_time"].dt.quarter

    # ensure numeric for SL/TP
    for c in ["init_sl_dist_price", "init_tp_dist_price", "init_sl_price", "init_tp_price", "r_multiple_price"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # compute per symbol
    ohlc_cache: Dict[str, pd.DataFrame] = {}
    day_cache: Dict[str, Dict[pd.Timestamp, pd.DataFrame]] = {}
    week_bounds_cache: Dict[str, Dict[pd.Period, Tuple[pd.Timestamp, pd.Timestamp]]] = {}
    month_bounds_cache: Dict[str, Dict[pd.Period, Tuple[pd.Timestamp, pd.Timestamp]]] = {}

    # output columns accumulators
    out_cols: Dict[str, List[object]] = {}

    def push(col: str, val: object) -> None:
        out_cols.setdefault(col, []).append(val)

    for _, tr in df.iterrows():
        sym = str(tr["symbol"]).upper()
        t_entry = _as_utc(tr["open_time"])
        t_exit = _as_utc(tr["close_time"])
        entry = float(tr["open_price"])
        exitp = float(tr["close_price"])
        sgn = side_sign(tr["type"])

        sl_dist = _to_num(tr.get("init_sl_dist_price"))
        tp_dist = _to_num(tr.get("init_tp_dist_price"))
        sl_price = _to_num(tr.get("init_sl_price"))
        tp_price = _to_num(tr.get("init_tp_price"))

        # load OHLC
        try:
            if sym not in ohlc_cache:
                ohlc_cache[sym] = load_ohlc(ohlc_dir, sym, tf_tag)
                day_cache[sym] = build_day_cache(ohlc_cache[sym])
                week_bounds_cache[sym] = build_week_bounds(ohlc_cache[sym])
                month_bounds_cache[sym] = build_month_bounds(ohlc_cache[sym])
            ohlc = ohlc_cache[sym]
        except Exception:
            # if OHLC missing: push NaNs for all context fields
            # Day structure
            for base in [
                "day_open","day_close","day_high","day_high_time","day_low","day_low_time","day_range","day_return",
                "entry_pos_in_day","exit_pos_in_day",
                "dist_entry_to_day_high","dist_entry_to_day_low","dist_exit_to_day_high","dist_exit_to_day_low",
                "dist_entry_to_day_high_r","dist_entry_to_day_low_r","dist_exit_to_day_high_r","dist_exit_to_day_low_r",
                "trade_duration_minutes","time_to_day_high_after_entry_min","time_to_day_low_after_entry_min",
                "mfe_price","mfe_r","mfe_time","mae_price","mae_r","mae_time","time_to_mfe_minutes","time_to_mae_minutes",
                "edge_leakage_r",
                "week_open","week_high_to_entry","week_high_time_to_entry","week_low_to_entry","week_low_time_to_entry",
                "week_range_to_entry","entry_pos_in_week","dist_entry_to_week_high","dist_entry_to_week_low",
                "dist_entry_to_week_high_r","dist_entry_to_week_low_r",
                "month_open","month_high_to_entry","month_high_time_to_entry","month_low_to_entry","month_low_time_to_entry",
                "month_range_to_entry","entry_pos_in_month","dist_entry_to_month_high","dist_entry_to_month_low",
                "dist_entry_to_month_high_r","dist_entry_to_month_low_r",
            ]:
                push(base, None)
            for sess in sessions.keys():
                pref = sess.lower()
                for base in [
                    f"{pref}_open",f"{pref}_close",f"{pref}_high",f"{pref}_high_time",f"{pref}_low",f"{pref}_low_time",
                    f"{pref}_range",f"{pref}_return",
                    f"entry_pos_in_{pref}",f"exit_pos_in_{pref}",
                    f"dist_entry_to_{pref}_high",f"dist_entry_to_{pref}_low",
                    f"dist_exit_to_{pref}_high",f"dist_exit_to_{pref}_low",
                    f"dist_entry_to_{pref}_high_r",f"dist_entry_to_{pref}_low_r",
                    f"dist_exit_to_{pref}_high_r",f"dist_exit_to_{pref}_low_r",
                    f"time_to_{pref}_high_after_entry_min",f"time_to_{pref}_low_after_entry_min",
                ]:
                    push(base, None)

            # exit quality fields still computable without OHLC
            push("exit_hit_sl", None)
            push("exit_hit_tp", None)
            push("exit_dist_to_sl_price", None)
            push("exit_dist_to_tp_price", None)
            push("exit_dist_to_sl_r", None)
            push("exit_dist_to_tp_r", None)
            continue

        # ---------- Day structure (ENTRY day) ----------
        dk = _day_key(t_entry)
        day_df = day_cache[sym].get(dk)
        day_agg = _agg_ohlc_block(day_df) if day_df is not None else _agg_ohlc_block(pd.DataFrame())
        day_open = day_agg["open"]
        day_close = day_agg["close"]
        day_high = day_agg["high"]
        day_low = day_agg["low"]
        day_high_t = day_agg["high_time"]
        day_low_t = day_agg["low_time"]
        day_range = day_agg["range"]
        day_ret = day_agg["return"]

        push("day_open", day_open)
        push("day_close", day_close)
        push("day_high", day_high)
        push("day_low", day_low)
        push("day_high_time", _fmt_ts(day_high_t))
        push("day_low_time", _fmt_ts(day_low_t))
        push("day_range", day_range)
        push("day_return", day_ret)

        push("entry_pos_in_day", _pos_in_range(entry, day_low, day_high))
        push("exit_pos_in_day", _pos_in_range(exitp, day_low, day_high))

        de_dh = _dist_to_high(entry, day_high)
        de_dl = _dist_to_low(entry, day_low)
        dx_dh = _dist_to_high(exitp, day_high)
        dx_dl = _dist_to_low(exitp, day_low)

        push("dist_entry_to_day_high", de_dh)
        push("dist_entry_to_day_low", de_dl)
        push("dist_exit_to_day_high", dx_dh)
        push("dist_exit_to_day_low", dx_dl)

        push("dist_entry_to_day_high_r", _norm_r(de_dh, sl_dist))
        push("dist_entry_to_day_low_r", _norm_r(de_dl, sl_dist))
        push("dist_exit_to_day_high_r", _norm_r(dx_dh, sl_dist))
        push("dist_exit_to_day_low_r", _norm_r(dx_dl, sl_dist))

        # ---------- Sessions (ENTRY day), for all sessions ----------
        # prepare session aggregates for the day
        session_aggs: Dict[str, Dict[str, object]] = {}
        if day_df is None:
            for name in sessions:
                session_aggs[name] = _agg_ohlc_block(pd.DataFrame())
        else:
            for name, hours in sessions.items():
                blk = _slice_session(day_df, hours)
                session_aggs[name] = _agg_ohlc_block(blk)

        for name in sessions.keys():
            pref = name.lower()
            agg = session_aggs[name]
            push(f"{pref}_open", agg["open"])
            push(f"{pref}_close", agg["close"])
            push(f"{pref}_high", agg["high"])
            push(f"{pref}_low", agg["low"])
            push(f"{pref}_high_time", _fmt_ts(agg["high_time"]))
            push(f"{pref}_low_time", _fmt_ts(agg["low_time"]))
            push(f"{pref}_range", agg["range"])
            push(f"{pref}_return", agg["return"])

            # positions use that session's range regardless of whether entry is inside
            push(f"entry_pos_in_{pref}", _pos_in_range(entry, agg["low"], agg["high"]))
            push(f"exit_pos_in_{pref}", _pos_in_range(exitp, agg["low"], agg["high"]))

            de_h = _dist_to_high(entry, agg["high"])
            de_l = _dist_to_low(entry, agg["low"])
            dx_h = _dist_to_high(exitp, agg["high"])
            dx_l = _dist_to_low(exitp, agg["low"])

            push(f"dist_entry_to_{pref}_high", de_h)
            push(f"dist_entry_to_{pref}_low", de_l)
            push(f"dist_exit_to_{pref}_high", dx_h)
            push(f"dist_exit_to_{pref}_low", dx_l)

            push(f"dist_entry_to_{pref}_high_r", _norm_r(de_h, sl_dist))
            push(f"dist_entry_to_{pref}_low_r", _norm_r(de_l, sl_dist))
            push(f"dist_exit_to_{pref}_high_r", _norm_r(dx_h, sl_dist))
            push(f"dist_exit_to_{pref}_low_r", _norm_r(dx_l, sl_dist))

            # time to session high/low after entry (only if time >= entry)
            ht = agg["high_time"]
            lt = agg["low_time"]
            if ht is None or pd.isna(ht):
                push(f"time_to_{pref}_high_after_entry_min", None)
            else:
                dtm = (_as_utc(ht) - t_entry).total_seconds() / 60.0
                push(f"time_to_{pref}_high_after_entry_min", float(dtm) if dtm >= 0 else None)

            if lt is None or pd.isna(lt):
                push(f"time_to_{pref}_low_after_entry_min", None)
            else:
                dtm = (_as_utc(lt) - t_entry).total_seconds() / 60.0
                push(f"time_to_{pref}_low_after_entry_min", float(dtm) if dtm >= 0 else None)

        # ---------- Trade-vs-Day timing ----------
        dur_min = (t_exit - t_entry).total_seconds() / 60.0
        push("trade_duration_minutes", float(dur_min))

        if day_high_t is None or pd.isna(day_high_t):
            push("time_to_day_high_after_entry_min", None)
        else:
            dtm = (_as_utc(day_high_t) - t_entry).total_seconds() / 60.0
            push("time_to_day_high_after_entry_min", float(dtm) if dtm >= 0 else None)

        if day_low_t is None or pd.isna(day_low_t):
            push("time_to_day_low_after_entry_min", None)
        else:
            dtm = (_as_utc(day_low_t) - t_entry).total_seconds() / 60.0
            push("time_to_day_low_after_entry_min", float(dtm) if dtm >= 0 else None)

        # ---------- Intratrade path (MFE/MAE + time) ----------
        trade_slice = slice_ohlc(ohlc, t_entry, t_exit)
        if trade_slice.empty:
            push("mfe_price", None)
            push("mfe_r", None)
            push("mfe_time", None)
            push("mae_price", None)
            push("mae_r", None)
            push("mae_time", None)
            push("time_to_mfe_minutes", None)
            push("time_to_mae_minutes", None)
            push("edge_leakage_r", None)
        else:
            # times are utc
            # For BUY: MFE is max(high)-entry, MAE is entry-min(low)
            # For SELL: MFE is entry-min(low), MAE is max(high)-entry
            if sgn == +1:
                i_h = int(trade_slice["high"].astype(float).idxmax())
                i_l = int(trade_slice["low"].astype(float).idxmin())
                hh = float(trade_slice.loc[i_h, "high"])
                ll = float(trade_slice.loc[i_l, "low"])
                ht = trade_slice.loc[i_h, "time"]
                lt = trade_slice.loc[i_l, "time"]
                mfe_price = hh - entry
                mae_price = entry - ll
                mfe_t = ht
                mae_t = lt
            else:
                i_l = int(trade_slice["low"].astype(float).idxmin())
                i_h = int(trade_slice["high"].astype(float).idxmax())
                ll = float(trade_slice.loc[i_l, "low"])
                hh = float(trade_slice.loc[i_h, "high"])
                lt = trade_slice.loc[i_l, "time"]
                ht = trade_slice.loc[i_h, "time"]
                mfe_price = entry - ll
                mae_price = hh - entry
                mfe_t = lt
                mae_t = ht

            push("mfe_price", float(mfe_price))
            push("mae_price", float(mae_price))
            push("mfe_r", _norm_r(float(mfe_price), sl_dist))
            push("mae_r", _norm_r(float(mae_price), sl_dist))
            push("mfe_time", _fmt_ts(mfe_t))
            push("mae_time", _fmt_ts(mae_t))

            ttmfe = (_as_utc(mfe_t) - t_entry).total_seconds() / 60.0
            ttmae = (_as_utc(mae_t) - t_entry).total_seconds() / 60.0
            push("time_to_mfe_minutes", float(ttmfe) if ttmfe >= 0 else None)
            push("time_to_mae_minutes", float(ttmae) if ttmae >= 0 else None)

            # edge leakage in R: MFE_R - realized_R (if realized exists)
            realized_r = _to_num(tr.get("r_multiple_price"))
            mfe_r = _norm_r(float(mfe_price), sl_dist)
            if realized_r is None or mfe_r is None:
                push("edge_leakage_r", None)
            else:
                push("edge_leakage_r", float(mfe_r) - float(realized_r))

        # ---------- Weekly "to entry" (no lookahead) ----------
        wk = _week_key(t_entry)
        wb = week_bounds_cache[sym].get(wk)
        if wb is None:
            push("week_open", None)
            push("week_high_to_entry", None)
            push("week_high_time_to_entry", None)
            push("week_low_to_entry", None)
            push("week_low_time_to_entry", None)
            push("week_range_to_entry", None)
            push("entry_pos_in_week", None)
            push("dist_entry_to_week_high", None)
            push("dist_entry_to_week_low", None)
            push("dist_entry_to_week_high_r", None)
            push("dist_entry_to_week_low_r", None)
        else:
            wstart, _wend = wb
            wslice = slice_ohlc(ohlc, wstart, t_entry)
            wagg = _agg_ohlc_block(wslice)
            push("week_open", wagg["open"])
            push("week_high_to_entry", wagg["high"])
            push("week_low_to_entry", wagg["low"])
            push("week_high_time_to_entry", _fmt_ts(wagg["high_time"]))
            push("week_low_time_to_entry", _fmt_ts(wagg["low_time"]))
            push("week_range_to_entry", wagg["range"])
            push("entry_pos_in_week", _pos_in_range(entry, wagg["low"], wagg["high"]))
            de_wh = _dist_to_high(entry, wagg["high"])
            de_wl = _dist_to_low(entry, wagg["low"])
            push("dist_entry_to_week_high", de_wh)
            push("dist_entry_to_week_low", de_wl)
            push("dist_entry_to_week_high_r", _norm_r(de_wh, sl_dist))
            push("dist_entry_to_week_low_r", _norm_r(de_wl, sl_dist))

        # ---------- Monthly "to entry" (no lookahead) ----------
        mk = _month_key(t_entry)
        mb = month_bounds_cache[sym].get(mk)
        if mb is None:
            push("month_open", None)
            push("month_high_to_entry", None)
            push("month_high_time_to_entry", None)
            push("month_low_to_entry", None)
            push("month_low_time_to_entry", None)
            push("month_range_to_entry", None)
            push("entry_pos_in_month", None)
            push("dist_entry_to_month_high", None)
            push("dist_entry_to_month_low", None)
            push("dist_entry_to_month_high_r", None)
            push("dist_entry_to_month_low_r", None)
        else:
            mstart, _mend = mb
            mslice = slice_ohlc(ohlc, mstart, t_entry)
            magg = _agg_ohlc_block(mslice)
            push("month_open", magg["open"])
            push("month_high_to_entry", magg["high"])
            push("month_low_to_entry", magg["low"])
            push("month_high_time_to_entry", _fmt_ts(magg["high_time"]))
            push("month_low_time_to_entry", _fmt_ts(magg["low_time"]))
            push("month_range_to_entry", magg["range"])
            push("entry_pos_in_month", _pos_in_range(entry, magg["low"], magg["high"]))
            de_mh = _dist_to_high(entry, magg["high"])
            de_ml = _dist_to_low(entry, magg["low"])
            push("dist_entry_to_month_high", de_mh)
            push("dist_entry_to_month_low", de_ml)
            push("dist_entry_to_month_high_r", _norm_r(de_mh, sl_dist))
            push("dist_entry_to_month_low_r", _norm_r(de_ml, sl_dist))

        # ---------- SL/TP/Exit quality (derived) ----------
        # hit flags (heuristic, based on close price relative to init SL/TP)
        hit_sl = None
        hit_tp = None
        if sl_price is not None:
            if sgn == +1:
                hit_sl = bool(exitp <= sl_price + PRICE_TOL)
            else:
                hit_sl = bool(exitp >= sl_price - PRICE_TOL)
        if tp_price is not None:
            if sgn == +1:
                hit_tp = bool(exitp >= tp_price - PRICE_TOL)
            else:
                hit_tp = bool(exitp <= tp_price + PRICE_TOL)

        push("exit_hit_sl", hit_sl)
        push("exit_hit_tp", hit_tp)

        # distances from exit to SL/TP level (absolute in price directionally meaningful)
        # distance_to_tp: for BUY -> tp - exit (positive means below tp)
        #                for SELL -> exit - tp (positive means above tp)
        exit_dist_tp = None
        if tp_price is not None:
            exit_dist_tp = (tp_price - exitp) if sgn == +1 else (exitp - tp_price)

        exit_dist_sl = None
        if sl_price is not None:
            exit_dist_sl = (exitp - sl_price) if sgn == +1 else (sl_price - exitp)

        push("exit_dist_to_tp_price", _to_num(exit_dist_tp))
        push("exit_dist_to_sl_price", _to_num(exit_dist_sl))
        push("exit_dist_to_tp_r", _norm_r(_to_num(exit_dist_tp), sl_dist))
        push("exit_dist_to_sl_r", _norm_r(_to_num(exit_dist_sl), sl_dist))

    # merge new cols
    for c, vals in out_cols.items():
        df[c] = vals

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{trades_fp.stem}_full_context.csv"
    df.to_csv(out_path, index=False)
    print(f"[OK] {trades_fp.name} -> {out_path}")


# =========================
# CLI
# =========================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--in-dir", type=str, default=str(DEFAULT_TRADES_IN_DIR))
    p.add_argument("--pattern", type=str, default="*.csv")
    p.add_argument("--out-dir", type=str, default=str(DEFAULT_OUT_DIR))
    p.add_argument("--ohlc-dir", type=str, default=str(DEFAULT_OHLC_DIR))
    p.add_argument("--ohlc-tf-tag", type=str, default=DEFAULT_OHLC_TF_TAG)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    ohlc_dir = Path(args.ohlc_dir)

    files = sorted(in_dir.glob(args.pattern))
    if not files:
        raise FileNotFoundError(f"No trade CSV files matching {args.pattern} in {in_dir}")

    for fp in files:
        try:
            enrich_full_context_for_file(
                trades_fp=fp,
                out_dir=out_dir,
                ohlc_dir=ohlc_dir,
                tf_tag=str(args.ohlc_tf_tag),
                sessions=DEFAULT_SESSIONS,
            )
        except Exception as e:
            print(f"[ERROR] {fp.name}: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
