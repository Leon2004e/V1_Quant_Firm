"""
Feature pipeline that produces Blocks 00–25 as separate CSVs (+ optional master parquet/csv).

Sessions (Europe/Berlin) per your request:
- HALF-OPEN intervals: start <= t < end
  Asia    02:00–06:00
  London  08:00–12:00
  NewYork 14:00–18:00
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# =========================
# Config
# =========================

@dataclass
class SessionsConfig:
    # Active hours in Europe/Berlin (HALF-OPEN intervals)
    asia: Tuple[str, str] = ("02:00", "06:00")
    london: Tuple[str, str] = ("08:00", "12:00")
    newyork: Tuple[str, str] = ("14:00", "18:00")

@dataclass
class ORIBConfig:
    or_day_15m: int = 15
    or_day_30m: int = 30
    or_session_30m: int = 30
    ib_60m: int = 60
    wor_mon_60m: int = 60
    tib_tue_60m: int = 60

@dataclass
class MSLConfig:
    st_window: int = 5
    mt_window: int = 20
    lt_window: int = 80

@dataclass
class VPConfig:
    enabled: bool = False
    method: str = "ohlcv_typical"  # ohlcv_uniform | ohlcv_typical
    bin_size_price: Optional[float] = None  # if None -> 2*tick_size (needs tick_size)

@dataclass
class BuildConfig:
    tz: str = "Europe/Berlin"
    out_dir: str = "out"
    export_blocks_csv: bool = True
    export_master_parquet: bool = True
    export_master_csv: bool = False

    # Join keys: ticket is usually unique; if not, use ("ticket","open_time")
    key_cols: Tuple[str, ...] = ("ticket",)

    # Market data
    ohlcv_root: Optional[str] = None  # folder containing per-symbol files OR a single file path
    ohlcv_file_pattern: str = "{symbol}.parquet"  # or "{symbol}.csv"
    ohlcv_time_col: str = "time"  # timestamp col in OHLCV files if not index
    ohlcv_symbol_col: Optional[str] = None  # if single multi-symbol file, set e.g. "symbol"
    timeframe_minutes: int = 15

    sessions: SessionsConfig = SessionsConfig()
    orib: ORIBConfig = ORIBConfig()
    msl: MSLConfig = MSLConfig()
    vp: VPConfig = VPConfig()


# =========================
# IO Helpers
# =========================

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def export_csv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False)

def export_parquet(df: pd.DataFrame, path: str) -> None:
    df.to_parquet(path, index=False)

def _is_parquet(path: str) -> bool:
    return path.lower().endswith(".parquet")

def _read_any(path: str) -> pd.DataFrame:
    if _is_parquet(path):
        return pd.read_parquet(path)
    return pd.read_csv(path)

def _to_tz(series: pd.Series, tz: str) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce")
    if getattr(s.dt, "tz", None) is None:
        s = s.dt.tz_localize(tz)
    else:
        s = s.dt.tz_convert(tz)
    return s

def _safe_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


# =========================
# Session logic (HALF-OPEN)
# =========================

def _parse_time(x: str) -> pd._libs.tslibs.timestamps.Time:
    return pd.to_datetime(x).time()

def _session_label(ts: pd.Timestamp, sessions: SessionsConfig) -> str:
    """
    Returns asia/london/newyork/off based on HALF-OPEN rule: start <= t < end.
    """
    t = ts.time()
    for name in ["asia", "london", "newyork"]:
        start_s, end_s = getattr(sessions, name)
        s_t, e_t = _parse_time(start_s), _parse_time(end_s)

        # Standard (non-overnight) range
        if s_t <= e_t:
            if (t >= s_t) and (t < e_t):
                return name
        else:
            # Overnight wrap (not used with your current config, but safe)
            if (t >= s_t) or (t < e_t):
                return name
    return "off"

def _session_window(day: pd.Timestamp, start_hhmm: str, end_hhmm: str, tz: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """
    Creates [start, end) window on given day.
    """
    d = day.floor("D")
    start = pd.Timestamp(f"{d.date()} {start_hhmm}", tz=tz)
    end = pd.Timestamp(f"{d.date()} {end_hhmm}", tz=tz)
    return start, end


# =========================
# Market data loader
# =========================

def load_ohlcv_for_symbol(symbol: str, cfg: BuildConfig) -> Optional[pd.DataFrame]:
    """
    Returns 15m OHLCV indexed by tz-aware datetime, columns: open,high,low,close,volume (volume optional).
    """
    if not cfg.ohlcv_root:
        return None

    root = cfg.ohlcv_root
    if os.path.isfile(root):
        df = _read_any(root)
        if cfg.ohlcv_symbol_col is None:
            raise ValueError("ohlcv_symbol_col must be set when ohlcv_root is a single multi-symbol file.")
        df = df[df[cfg.ohlcv_symbol_col].astype(str) == str(symbol)].copy()
    else:
        path = os.path.join(root, cfg.ohlcv_file_pattern.format(symbol=symbol))
        if not os.path.exists(path):
            return None
        df = _read_any(path)

    # set index time
    if cfg.ohlcv_time_col in df.columns:
        df[cfg.ohlcv_time_col] = _to_tz(df[cfg.ohlcv_time_col], cfg.tz)
        df = df.set_index(cfg.ohlcv_time_col)
    else:
        df.index = _to_tz(pd.Series(df.index), cfg.tz).values

    # normalize columns (case-insensitive)
    colmap = {c.lower(): c for c in df.columns}
    def pick(name: str) -> Optional[str]:
        return colmap.get(name)

    for r in ["open", "high", "low", "close"]:
        if pick(r) is None:
            raise ValueError(f"OHLCV for {symbol} missing column '{r}' (case-insensitive).")

    out = pd.DataFrame(index=df.index)
    out["open"] = _safe_num(df[pick("open")])
    out["high"] = _safe_num(df[pick("high")])
    out["low"] = _safe_num(df[pick("low")])
    out["close"] = _safe_num(df[pick("close")])
    if pick("volume") is not None:
        out["volume"] = _safe_num(df[pick("volume")])
    else:
        out["volume"] = np.nan

    return out.sort_index()


# =========================
# Core utilities
# =========================

def _resample_levels(ohlc: pd.DataFrame, rule: str) -> pd.DataFrame:
    return ohlc.resample(rule).agg({"open": "first", "high": "max", "low": "min", "close": "last"})

def _prev_period_levels_asof(ohlc: pd.DataFrame, t: pd.Timestamp) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if ohlc is None or len(ohlc) == 0:
        return out

    d = _resample_levels(ohlc, "1D")
    w = _resample_levels(ohlc, "W-MON")
    m = _resample_levels(ohlc, "MS")

    def prev_row(frame: pd.DataFrame, tt: pd.Timestamp) -> Optional[pd.Series]:
        idx = frame.index.searchsorted(tt, side="right") - 1
        if idx - 1 < 0:
            return None
        return frame.iloc[idx - 1]

    def fill(prefix: str, row: Optional[pd.Series]):
        if row is None:
            return
        out[f"{prefix}_open"] = float(row["open"])
        out[f"{prefix}_high"] = float(row["high"])
        out[f"{prefix}_low"] = float(row["low"])
        out[f"{prefix}_close"] = float(row["close"])

    fill("ppl_prev_day", prev_row(d, t))
    fill("ppl_prev_week", prev_row(w, t))
    fill("ppl_prev_month", prev_row(m, t))
    return out

def _r_denom(row: pd.Series) -> float:
    for c in ["init_sl_dist_price", "atr_value"]:
        if c in row and pd.notna(row[c]) and float(row[c]) > 0:
            return float(row[c])
    if "open_price" in row and "close_price" in row and pd.notna(row["open_price"]) and pd.notna(row["close_price"]):
        v = abs(float(row["open_price"]) - float(row["close_price"]))
        return v if v > 0 else 1.0
    return 1.0

def _pos_in_range(price: float, lo: float, hi: float) -> float:
    r = hi - lo
    if not np.isfinite(r) or r == 0:
        return np.nan
    return (price - lo) / r


# =========================
# Block builders (00–25)
# =========================

def build_block_00_raw(df_raw: pd.DataFrame) -> pd.DataFrame:
    return df_raw.copy()

def build_block_01_normalized(df_raw: pd.DataFrame, cfg: BuildConfig) -> pd.DataFrame:
    rename = {
        "Ticket": "ticket",
        "Symbol": "symbol",
        "Type": "type",
        "Open time": "open_time",
        "Open price": "open_price",
        "Size": "size",
        "Close time": "close_time",
        "Close price": "close_price",
        "Comment": "comment",
        "Sample type": "sample_type",
    }
    df = df_raw.rename(columns={k: v for k, v in rename.items() if k in df_raw.columns}).copy()

    for c in ["ticket", "symbol", "type", "open_time", "open_price", "close_time", "close_price", "size", "comment", "sample_type"]:
        if c not in df.columns:
            df[c] = np.nan

    df["ticket"] = df["ticket"].astype(str)
    df["symbol"] = df["symbol"].astype(str)
    df["type"] = df["type"].astype(str)
    df["open_time"] = _to_tz(df["open_time"], cfg.tz)
    df["close_time"] = _to_tz(df["close_time"], cfg.tz)

    for c in ["open_price", "close_price", "size"]:
        df[c] = _safe_num(df[c])

    df["comment"] = df["comment"].astype("string")
    df["sample_type"] = df["sample_type"].astype("string")

    cols = ["ticket", "open_time", "open_price", "close_time", "close_price", "type", "symbol", "size", "comment", "sample_type"]
    return df[cols].copy()

def build_block_02_realized_pnl(df_raw: pd.DataFrame, b01: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": b01["ticket"]})

    df["pl_money"] = _safe_num(df_raw["P/L in money"]) if "P/L in money" in df_raw.columns else np.nan
    df["pl_pips"] = _safe_num(df_raw["P/L in pips"]) if "P/L in pips" in df_raw.columns else np.nan
    df["comm_swap"] = _safe_num(df_raw["Comm/Swap"]) if "Comm/Swap" in df_raw.columns else np.nan
    return df

def build_block_03_strategy_meta(b01: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": b01["ticket"]})
    df["strategy_id"] = np.nan
    df["exit_type"] = np.nan
    df["exit_level_price"] = np.nan
    df["profile_found"] = np.nan
    df["risk_model_type"] = np.nan
    df["__source_file"] = np.nan
    return df

def build_block_04_instrument_vol_sl_tp(b01: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": b01["ticket"]})
    df["tick_size"] = np.nan
    df["atr_period"] = np.nan
    df["atr_shift"] = np.nan
    df["atr_value"] = np.nan
    df["init_sl_dist_price"] = np.nan
    df["init_tp_dist_price"] = np.nan
    df["init_sl_price"] = np.nan
    df["init_tp_price"] = np.nan
    df["realized_exit_dist_price"] = np.nan
    df["r_multiple_price"] = np.nan
    return df

def build_block_05_sessions_daystats(master: pd.DataFrame, ohlcv_by_symbol: Dict[str, pd.DataFrame], cfg: BuildConfig) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    df["entry_session"] = master["open_time"].apply(lambda x: _session_label(x, cfg.sessions)).astype("string")
    df["exit_session"] = master["close_time"].apply(lambda x: _session_label(x, cfg.sessions)).astype("string")

    day_open, day_high, day_low, day_close, day_range = [], [], [], [], []
    for sym, ot in zip(master["symbol"], master["open_time"]):
        o = ohlcv_by_symbol.get(sym)
        if o is None or len(o) == 0 or pd.isna(ot):
            day_open.append(np.nan); day_high.append(np.nan); day_low.append(np.nan); day_close.append(np.nan); day_range.append(np.nan)
            continue
        d0 = ot.floor("D")
        d1 = d0 + pd.Timedelta(days=1)
        dd = o.loc[(o.index >= d0) & (o.index < d1)]
        if len(dd) == 0:
            day_open.append(np.nan); day_high.append(np.nan); day_low.append(np.nan); day_close.append(np.nan); day_range.append(np.nan)
            continue
        day_open.append(float(dd["open"].iloc[0]))
        hi = float(dd["high"].max()); lo = float(dd["low"].min())
        day_high.append(hi)
        day_low.append(lo)
        day_close.append(float(dd["close"].iloc[-1]))
        day_range.append(hi - lo)

    df["day_open"] = day_open
    df["day_high"] = day_high
    df["day_low"] = day_low
    df["day_close"] = day_close
    df["day_range"] = day_range
    return df

def build_block_06_day_positioning(master: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    df["entry_pos_in_day"] = [
        _pos_in_range(p, lo, hi) if pd.notna(p) and pd.notna(lo) and pd.notna(hi) else np.nan
        for p, lo, hi in zip(master["open_price"], master["day_low"], master["day_high"])
    ]
    df["exit_pos_in_day"] = [
        _pos_in_range(p, lo, hi) if pd.notna(p) and pd.notna(lo) and pd.notna(hi) else np.nan
        for p, lo, hi in zip(master["close_price"], master["day_low"], master["day_high"])
    ]
    df["entry_dist_to_day_high"] = master["open_price"] - master["day_high"]
    df["entry_dist_to_day_low"] = master["open_price"] - master["day_low"]
    df["exit_dist_to_day_high"] = master["close_price"] - master["day_high"]
    df["exit_dist_to_day_low"] = master["close_price"] - master["day_low"]
    df["mfe_vs_day_range"] = np.nan
    dr = master["day_range"].replace(0, np.nan)
    df["realized_vs_day_range"] = master.get("realized_exit_dist_price", np.nan) / dr
    return df

def build_block_07_entry_session_ohlc(master: pd.DataFrame, ohlcv_by_symbol: Dict[str, pd.DataFrame], cfg: BuildConfig) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    for c in ["entry_sess_open","entry_sess_close","entry_sess_high","entry_sess_low","entry_sess_range",
              "entry_sess_return","entry_sess_return_dir","entry_sess_return_dir_r"]:
        df[c] = np.nan

    for i, (sym, ot) in enumerate(zip(master["symbol"], master["open_time"])):
        o = ohlcv_by_symbol.get(sym)
        if o is None or len(o) == 0 or pd.isna(ot):
            continue
        sess = _session_label(ot, cfg.sessions)
        if sess == "off":
            continue
        start_s, end_s = getattr(cfg.sessions, sess)
        start, end = _session_window(ot, start_s, end_s, cfg.tz)
        dd = o.loc[(o.index >= start) & (o.index < end)]
        if len(dd) == 0:
            continue
        op = float(dd["open"].iloc[0]); cl = float(dd["close"].iloc[-1])
        hi = float(dd["high"].max()); lo = float(dd["low"].min())
        rng = hi - lo
        ret = cl - op
        df.loc[i, "entry_sess_open"] = op
        df.loc[i, "entry_sess_close"] = cl
        df.loc[i, "entry_sess_high"] = hi
        df.loc[i, "entry_sess_low"] = lo
        df.loc[i, "entry_sess_range"] = rng
        df.loc[i, "entry_sess_return"] = ret
        df.loc[i, "entry_sess_return_dir"] = 1 if ret > 0 else (-1 if ret < 0 else 0)
        rden = _r_denom(master.iloc[i])
        df.loc[i, "entry_sess_return_dir_r"] = ret / rden if rden else np.nan
    return df

def build_block_08_exit_session_ohlc(master: pd.DataFrame, ohlcv_by_symbol: Dict[str, pd.DataFrame], cfg: BuildConfig) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    for c in ["exit_sess_open","exit_sess_close","exit_sess_high","exit_sess_low","exit_sess_range",
              "exit_sess_return","exit_sess_return_dir","exit_sess_return_dir_r"]:
        df[c] = np.nan

    for i, (sym, ct) in enumerate(zip(master["symbol"], master["close_time"])):
        o = ohlcv_by_symbol.get(sym)
        if o is None or len(o) == 0 or pd.isna(ct):
            continue
        sess = _session_label(ct, cfg.sessions)
        if sess == "off":
            continue
        start_s, end_s = getattr(cfg.sessions, sess)
        start, end = _session_window(ct, start_s, end_s, cfg.tz)
        dd = o.loc[(o.index >= start) & (o.index < end)]
        if len(dd) == 0:
            continue
        op = float(dd["open"].iloc[0]); cl = float(dd["close"].iloc[-1])
        hi = float(dd["high"].max()); lo = float(dd["low"].min())
        rng = hi - lo
        ret = cl - op
        df.loc[i, "exit_sess_open"] = op
        df.loc[i, "exit_sess_close"] = cl
        df.loc[i, "exit_sess_high"] = hi
        df.loc[i, "exit_sess_low"] = lo
        df.loc[i, "exit_sess_range"] = rng
        df.loc[i, "exit_sess_return"] = ret
        df.loc[i, "exit_sess_return_dir"] = 1 if ret > 0 else (-1 if ret < 0 else 0)
        rden = _r_denom(master.iloc[i])
        df.loc[i, "exit_sess_return_dir_r"] = ret / rden if rden else np.nan
    return df

def build_block_09_entry_time_decomp(master: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    ot = master["open_time"]
    df["entry_hour"] = ot.dt.hour.astype("int16")
    df["entry_minute"] = ot.dt.minute.astype("int16")
    df["entry_weekday"] = ot.dt.weekday.astype("int16")
    df["entry_week_of_year"] = ot.dt.isocalendar().week.astype("int16")
    df["entry_month"] = ot.dt.month.astype("int16")
    df["entry_quarter"] = ot.dt.quarter.astype("int16")
    return df

def build_block_10_day_extremes_timing(master: pd.DataFrame, ohlcv_by_symbol: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    df["day_high_time"] = pd.NaT
    df["day_low_time"] = pd.NaT
    df["day_return"] = np.nan

    for i, (sym, ot) in enumerate(zip(master["symbol"], master["open_time"])):
        o = ohlcv_by_symbol.get(sym)
        if o is None or len(o) == 0 or pd.isna(ot):
            continue
        d0 = ot.floor("D")
        d1 = d0 + pd.Timedelta(days=1)
        dd = o.loc[(o.index >= d0) & (o.index < d1)]
        if len(dd) == 0:
            continue
        hi_idx = dd["high"].idxmax()
        lo_idx = dd["low"].idxmin()
        df.loc[i, "day_high_time"] = hi_idx
        df.loc[i, "day_low_time"] = lo_idx
        op = float(dd["open"].iloc[0]); cl = float(dd["close"].iloc[-1])
        df.loc[i, "day_return"] = cl - op
    return df

def build_block_11_dist_to_day_extremes(master: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    df["dist_entry_to_day_high"] = master["open_price"] - master["day_high"]
    df["dist_entry_to_day_low"] = master["open_price"] - master["day_low"]
    df["dist_exit_to_day_high"] = master["close_price"] - master["day_high"]
    df["dist_exit_to_day_low"] = master["close_price"] - master["day_low"]
    r = master.apply(_r_denom, axis=1).replace(0, np.nan)
    df["dist_entry_to_day_high_r"] = df["dist_entry_to_day_high"] / r
    df["dist_entry_to_day_low_r"] = df["dist_entry_to_day_low"] / r
    df["dist_exit_to_day_high_r"] = df["dist_exit_to_day_high"] / r
    df["dist_exit_to_day_low_r"] = df["dist_exit_to_day_low"] / r
    return df

def _build_session_block(master: pd.DataFrame, ohlcv_by_symbol: Dict[str, pd.DataFrame], cfg: BuildConfig, sess_name: str) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    pref = sess_name

    cols = [
        f"{pref}_open", f"{pref}_close", f"{pref}_high", f"{pref}_low",
        f"{pref}_high_time", f"{pref}_low_time", f"{pref}_range", f"{pref}_return",
        f"entry_pos_in_{pref}", f"exit_pos_in_{pref}",
        f"dist_entry_to_{pref}_high", f"dist_entry_to_{pref}_low",
        f"dist_exit_to_{pref}_high", f"dist_exit_to_{pref}_low",
        f"dist_entry_to_{pref}_high_r", f"dist_entry_to_{pref}_low_r",
        f"dist_exit_to_{pref}_high_r", f"dist_exit_to_{pref}_low_r",
        f"time_to_{pref}_high_after_entry_min", f"time_to_{pref}_low_after_entry_min",
    ]
    for c in cols:
        df[c] = np.nan
    df[f"{pref}_high_time"] = pd.NaT
    df[f"{pref}_low_time"] = pd.NaT

    start_s, end_s = getattr(cfg.sessions, pref)

    for i, (sym, ot, ct) in enumerate(zip(master["symbol"], master["open_time"], master["close_time"])):
        o = ohlcv_by_symbol.get(sym)
        if o is None or len(o) == 0 or pd.isna(ot) or pd.isna(ct):
            continue
        start, end = _session_window(ot, start_s, end_s, cfg.tz)
        ss = o.loc[(o.index >= start) & (o.index < end)]
        if len(ss) == 0:
            continue

        op = float(ss["open"].iloc[0]); cl = float(ss["close"].iloc[-1])
        hi = float(ss["high"].max()); lo = float(ss["low"].min())
        hi_t = ss["high"].idxmax()
        lo_t = ss["low"].idxmin()
        rng = hi - lo
        ret = cl - op

        df.loc[i, f"{pref}_open"] = op
        df.loc[i, f"{pref}_close"] = cl
        df.loc[i, f"{pref}_high"] = hi
        df.loc[i, f"{pref}_low"] = lo
        df.loc[i, f"{pref}_high_time"] = hi_t
        df.loc[i, f"{pref}_low_time"] = lo_t
        df.loc[i, f"{pref}_range"] = rng
        df.loc[i, f"{pref}_return"] = ret

        ep = master.loc[i, "open_price"]
        xp = master.loc[i, "close_price"]
        df.loc[i, f"entry_pos_in_{pref}"] = _pos_in_range(float(ep), lo, hi) if pd.notna(ep) else np.nan
        df.loc[i, f"exit_pos_in_{pref}"] = _pos_in_range(float(xp), lo, hi) if pd.notna(xp) else np.nan

        df.loc[i, f"dist_entry_to_{pref}_high"] = ep - hi if pd.notna(ep) else np.nan
        df.loc[i, f"dist_entry_to_{pref}_low"] = ep - lo if pd.notna(ep) else np.nan
        df.loc[i, f"dist_exit_to_{pref}_high"] = xp - hi if pd.notna(xp) else np.nan
        df.loc[i, f"dist_exit_to_{pref}_low"] = xp - lo if pd.notna(xp) else np.nan

        rden = _r_denom(master.loc[i])
        if rden and np.isfinite(rden):
            df.loc[i, f"dist_entry_to_{pref}_high_r"] = df.loc[i, f"dist_entry_to_{pref}_high"] / rden
            df.loc[i, f"dist_entry_to_{pref}_low_r"] = df.loc[i, f"dist_entry_to_{pref}_low"] / rden
            df.loc[i, f"dist_exit_to_{pref}_high_r"] = df.loc[i, f"dist_exit_to_{pref}_high"] / rden
            df.loc[i, f"dist_exit_to_{pref}_low_r"] = df.loc[i, f"dist_exit_to_{pref}_low"] / rden

        df.loc[i, f"time_to_{pref}_high_after_entry_min"] = (hi_t - ot).total_seconds() / 60.0 if pd.notna(hi_t) else np.nan
        df.loc[i, f"time_to_{pref}_low_after_entry_min"] = (lo_t - ot).total_seconds() / 60.0 if pd.notna(lo_t) else np.nan

    return df

def build_block_12_asia(master: pd.DataFrame, ohlcv_by_symbol: Dict[str, pd.DataFrame], cfg: BuildConfig) -> pd.DataFrame:
    return _build_session_block(master, ohlcv_by_symbol, cfg, "asia")

def build_block_13_london(master: pd.DataFrame, ohlcv_by_symbol: Dict[str, pd.DataFrame], cfg: BuildConfig) -> pd.DataFrame:
    return _build_session_block(master, ohlcv_by_symbol, cfg, "london")

def build_block_14_newyork(master: pd.DataFrame, ohlcv_by_symbol: Dict[str, pd.DataFrame], cfg: BuildConfig) -> pd.DataFrame:
    return _build_session_block(master, ohlcv_by_symbol, cfg, "newyork")

def build_block_15_trade_path(master: pd.DataFrame, ohlcv_by_symbol: Dict[str, pd.DataFrame], cfg: BuildConfig) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    df["trade_duration_minutes"] = (master["close_time"] - master["open_time"]).dt.total_seconds() / 60.0

    df["time_to_day_high_after_entry_min"] = np.nan
    df["time_to_day_low_after_entry_min"] = np.nan
    if "day_high_time" in master.columns:
        df["time_to_day_high_after_entry_min"] = (pd.to_datetime(master["day_high_time"]) - master["open_time"]).dt.total_seconds() / 60.0
    if "day_low_time" in master.columns:
        df["time_to_day_low_after_entry_min"] = (pd.to_datetime(master["day_low_time"]) - master["open_time"]).dt.total_seconds() / 60.0

    df["mfe_price"] = np.nan
    df["mfe_time"] = pd.NaT
    df["mae_price"] = np.nan
    df["mae_time"] = pd.NaT
    df["time_to_mfe_minutes"] = np.nan
    df["time_to_mae_minutes"] = np.nan

    for i, (sym, ot, ct, typ, ep) in enumerate(zip(master["symbol"], master["open_time"], master["close_time"], master["type"], master["open_price"])):
        o = ohlcv_by_symbol.get(sym)
        if o is None or len(o) == 0 or pd.isna(ot) or pd.isna(ct) or pd.isna(ep):
            continue
        seg = o.loc[(o.index >= ot) & (o.index <= ct)]
        if len(seg) == 0:
            continue

        is_long = str(typ).lower() in ["buy", "long", "1"]
        if is_long:
            mfe_idx = seg["high"].idxmax()
            mae_idx = seg["low"].idxmin()
            mfe_price = float(seg.loc[mfe_idx, "high"])
            mae_price = float(seg.loc[mae_idx, "low"])
        else:
            mfe_idx = seg["low"].idxmin()
            mae_idx = seg["high"].idxmax()
            mfe_price = float(seg.loc[mfe_idx, "low"])
            mae_price = float(seg.loc[mae_idx, "high"])

        df.loc[i, "mfe_price"] = mfe_price
        df.loc[i, "mfe_time"] = mfe_idx
        df.loc[i, "mae_price"] = mae_price
        df.loc[i, "mae_time"] = mae_idx
        df.loc[i, "time_to_mfe_minutes"] = (mfe_idx - ot).total_seconds() / 60.0
        df.loc[i, "time_to_mae_minutes"] = (mae_idx - ot).total_seconds() / 60.0

    return df

def build_block_16_week_context(master: pd.DataFrame, ohlcv_by_symbol: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    for c in ["week_open","week_high_to_entry","week_high_time_to_entry","week_low_to_entry","week_low_time_to_entry",
              "week_range_to_entry","entry_pos_in_week","dist_entry_to_week_high","dist_entry_to_week_low",
              "dist_entry_to_week_high_r","dist_entry_to_week_low_r"]:
        df[c] = np.nan
    df["week_high_time_to_entry"] = pd.NaT
    df["week_low_time_to_entry"] = pd.NaT

    for i, (sym, ot, ep) in enumerate(zip(master["symbol"], master["open_time"], master["open_price"])):
        o = ohlcv_by_symbol.get(sym)
        if o is None or len(o) == 0 or pd.isna(ot) or pd.isna(ep):
            continue
        week_start = (ot - pd.Timedelta(days=int(ot.weekday()))).floor("D")
        seg = o.loc[(o.index >= week_start) & (o.index <= ot)]
        if len(seg) == 0:
            continue
        w_open = float(seg["open"].iloc[0])
        w_high = float(seg["high"].max())
        w_low = float(seg["low"].min())
        hi_t = seg["high"].idxmax()
        lo_t = seg["low"].idxmin()

        df.loc[i, "week_open"] = w_open
        df.loc[i, "week_high_to_entry"] = w_high
        df.loc[i, "week_low_to_entry"] = w_low
        df.loc[i, "week_high_time_to_entry"] = hi_t
        df.loc[i, "week_low_time_to_entry"] = lo_t
        df.loc[i, "week_range_to_entry"] = w_high - w_low
        df.loc[i, "entry_pos_in_week"] = _pos_in_range(float(ep), w_low, w_high)
        df.loc[i, "dist_entry_to_week_high"] = float(ep) - w_high
        df.loc[i, "dist_entry_to_week_low"] = float(ep) - w_low

        rden = _r_denom(master.loc[i])
        if rden and np.isfinite(rden):
            df.loc[i, "dist_entry_to_week_high_r"] = df.loc[i, "dist_entry_to_week_high"] / rden
            df.loc[i, "dist_entry_to_week_low_r"] = df.loc[i, "dist_entry_to_week_low"] / rden

    return df

def build_block_17_month_context(master: pd.DataFrame, ohlcv_by_symbol: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    for c in ["month_open","month_high_to_entry","month_high_time_to_entry","month_low_to_entry","month_low_time_to_entry",
              "month_range_to_entry","entry_pos_in_month","dist_entry_to_month_high","dist_entry_to_month_low",
              "dist_entry_to_month_high_r","dist_entry_to_month_low_r"]:
        df[c] = np.nan
    df["month_high_time_to_entry"] = pd.NaT
    df["month_low_time_to_entry"] = pd.NaT

    for i, (sym, ot, ep) in enumerate(zip(master["symbol"], master["open_time"], master["open_price"])):
        o = ohlcv_by_symbol.get(sym)
        if o is None or len(o) == 0 or pd.isna(ot) or pd.isna(ep):
            continue
        month_start = ot.replace(day=1).floor("D")
        seg = o.loc[(o.index >= month_start) & (o.index <= ot)]
        if len(seg) == 0:
            continue
        m_open = float(seg["open"].iloc[0])
        m_high = float(seg["high"].max())
        m_low = float(seg["low"].min())
        hi_t = seg["high"].idxmax()
        lo_t = seg["low"].idxmin()

        df.loc[i, "month_open"] = m_open
        df.loc[i, "month_high_to_entry"] = m_high
        df.loc[i, "month_low_to_entry"] = m_low
        df.loc[i, "month_high_time_to_entry"] = hi_t
        df.loc[i, "month_low_time_to_entry"] = lo_t
        df.loc[i, "month_range_to_entry"] = m_high - m_low
        df.loc[i, "entry_pos_in_month"] = _pos_in_range(float(ep), m_low, m_high)
        df.loc[i, "dist_entry_to_month_high"] = float(ep) - m_high
        df.loc[i, "dist_entry_to_month_low"] = float(ep) - m_low

        rden = _r_denom(master.loc[i])
        if rden and np.isfinite(rden):
            df.loc[i, "dist_entry_to_month_high_r"] = df.loc[i, "dist_entry_to_month_high"] / rden
            df.loc[i, "dist_entry_to_month_low_r"] = df.loc[i, "dist_entry_to_month_low"] / rden

    return df

def build_block_18_exit_mechanics(master: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    for c in ["exit_hit_sl","exit_hit_tp","exit_dist_to_tp_price","exit_dist_to_sl_price","exit_dist_to_tp_r","exit_dist_to_sl_r"]:
        df[c] = np.nan
    return df

def build_block_19_prior_period_levels(master: pd.DataFrame, ohlcv_by_symbol: Dict[str, pd.DataFrame], cfg: BuildConfig) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    lvl_cols = [
        "ppl_prev_day_open","ppl_prev_day_high","ppl_prev_day_low","ppl_prev_day_close",
        "ppl_prev_week_open","ppl_prev_week_high","ppl_prev_week_low","ppl_prev_week_close",
        "ppl_prev_month_open","ppl_prev_month_high","ppl_prev_month_low","ppl_prev_month_close",
    ]
    for c in lvl_cols:
        df[c] = np.nan

    for c in [
        "ppl_dist_entry_to_prev_day_high","ppl_dist_entry_to_prev_day_low","ppl_dist_entry_to_prev_day_open","ppl_dist_entry_to_prev_day_close",
        "ppl_dist_entry_to_prev_week_high","ppl_dist_entry_to_prev_week_low","ppl_dist_entry_to_prev_month_high","ppl_dist_entry_to_prev_month_low",
        "ppl_dist_exit_to_prev_day_high","ppl_dist_exit_to_prev_day_low","ppl_dist_exit_to_prev_week_high","ppl_dist_exit_to_prev_week_low",
        "ppl_dist_exit_to_prev_month_high","ppl_dist_exit_to_prev_month_low",
        "ppl_dist_entry_to_prev_day_high_r","ppl_dist_entry_to_prev_day_low_r","ppl_dist_entry_to_prev_week_high_r","ppl_dist_entry_to_prev_week_low_r",
        "ppl_dist_entry_to_prev_month_high_r","ppl_dist_entry_to_prev_month_low_r",
        "ppl_dist_exit_to_prev_day_high_r","ppl_dist_exit_to_prev_day_low_r","ppl_dist_exit_to_prev_week_high_r","ppl_dist_exit_to_prev_week_low_r",
        "ppl_dist_exit_to_prev_month_high_r","ppl_dist_exit_to_prev_month_low_r",
    ]:
        df[c] = np.nan

    for i, (sym, ot, ep, xp) in enumerate(zip(master["symbol"], master["open_time"], master["open_price"], master["close_price"])):
        o = ohlcv_by_symbol.get(sym)
        if o is None or len(o) == 0 or pd.isna(ot):
            continue

        lvls = _prev_period_levels_asof(o, ot)
        for k, v in lvls.items():
            if k in df.columns:
                df.loc[i, k] = v

        rden = _r_denom(master.loc[i])

        def set_dist(dst: str, price: float, lvl_col: str):
            if pd.isna(price) or pd.isna(df.loc[i, lvl_col]):
                return
            df.loc[i, dst] = float(price) - float(df.loc[i, lvl_col])

        set_dist("ppl_dist_entry_to_prev_day_high", ep, "ppl_prev_day_high")
        set_dist("ppl_dist_entry_to_prev_day_low", ep, "ppl_prev_day_low")
        set_dist("ppl_dist_entry_to_prev_day_open", ep, "ppl_prev_day_open")
        set_dist("ppl_dist_entry_to_prev_day_close", ep, "ppl_prev_day_close")
        set_dist("ppl_dist_entry_to_prev_week_high", ep, "ppl_prev_week_high")
        set_dist("ppl_dist_entry_to_prev_week_low", ep, "ppl_prev_week_low")
        set_dist("ppl_dist_entry_to_prev_month_high", ep, "ppl_prev_month_high")
        set_dist("ppl_dist_entry_to_prev_month_low", ep, "ppl_prev_month_low")

        set_dist("ppl_dist_exit_to_prev_day_high", xp, "ppl_prev_day_high")
        set_dist("ppl_dist_exit_to_prev_day_low", xp, "ppl_prev_day_low")
        set_dist("ppl_dist_exit_to_prev_week_high", xp, "ppl_prev_week_high")
        set_dist("ppl_dist_exit_to_prev_week_low", xp, "ppl_prev_week_low")
        set_dist("ppl_dist_exit_to_prev_month_high", xp, "ppl_prev_month_high")
        set_dist("ppl_dist_exit_to_prev_month_low", xp, "ppl_prev_month_low")

        if rden and np.isfinite(rden) and rden != 0:
            for src, dst in [
                ("ppl_dist_entry_to_prev_day_high","ppl_dist_entry_to_prev_day_high_r"),
                ("ppl_dist_entry_to_prev_day_low","ppl_dist_entry_to_prev_day_low_r"),
                ("ppl_dist_entry_to_prev_week_high","ppl_dist_entry_to_prev_week_high_r"),
                ("ppl_dist_entry_to_prev_week_low","ppl_dist_entry_to_prev_week_low_r"),
                ("ppl_dist_entry_to_prev_month_high","ppl_dist_entry_to_prev_month_high_r"),
                ("ppl_dist_entry_to_prev_month_low","ppl_dist_entry_to_prev_month_low_r"),
                ("ppl_dist_exit_to_prev_day_high","ppl_dist_exit_to_prev_day_high_r"),
                ("ppl_dist_exit_to_prev_day_low","ppl_dist_exit_to_prev_day_low_r"),
                ("ppl_dist_exit_to_prev_week_high","ppl_dist_exit_to_prev_week_high_r"),
                ("ppl_dist_exit_to_prev_week_low","ppl_dist_exit_to_prev_week_low_r"),
                ("ppl_dist_exit_to_prev_month_high","ppl_dist_exit_to_prev_month_high_r"),
                ("ppl_dist_exit_to_prev_month_low","ppl_dist_exit_to_prev_month_low_r"),
            ]:
                if pd.notna(df.loc[i, src]):
                    df.loc[i, dst] = float(df.loc[i, src]) / rden

    return df

def build_block_20_msl_untaken(master: pd.DataFrame, cfg: BuildConfig) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    df["msl_st_window"] = cfg.msl.st_window
    df["msl_mt_window"] = cfg.msl.mt_window
    df["msl_lt_window"] = cfg.msl.lt_window
    cols = [
        "msl_st_last_untaken_high_price","msl_st_last_untaken_low_price",
        "msl_mt_last_untaken_high_price","msl_mt_last_untaken_low_price",
        "msl_lt_last_untaken_high_price","msl_lt_last_untaken_low_price",
        "msl_st_last_untaken_high_age_min","msl_st_last_untaken_low_age_min",
        "msl_mt_last_untaken_high_age_min","msl_mt_last_untaken_low_age_min",
        "msl_lt_last_untaken_high_age_min","msl_lt_last_untaken_low_age_min",
        "msl_st_dist_entry_to_untaken_high","msl_st_dist_entry_to_untaken_low",
        "msl_mt_dist_entry_to_untaken_high","msl_mt_dist_entry_to_untaken_low",
        "msl_lt_dist_entry_to_untaken_high","msl_lt_dist_entry_to_untaken_low",
        "msl_st_dist_exit_to_untaken_high","msl_st_dist_exit_to_untaken_low",
        "msl_mt_dist_exit_to_untaken_high","msl_mt_dist_exit_to_untaken_low",
        "msl_lt_dist_exit_to_untaken_high","msl_lt_dist_exit_to_untaken_low",
        "msl_st_dist_entry_to_untaken_high_r","msl_st_dist_entry_to_untaken_low_r",
        "msl_mt_dist_entry_to_untaken_high_r","msl_mt_dist_entry_to_untaken_low_r",
        "msl_lt_dist_entry_to_untaken_high_r","msl_lt_dist_entry_to_untaken_low_r",
        "msl_st_dist_exit_to_untaken_high_r","msl_st_dist_exit_to_untaken_low_r",
        "msl_mt_dist_exit_to_untaken_high_r","msl_mt_dist_exit_to_untaken_low_r",
        "msl_lt_dist_exit_to_untaken_high_r","msl_lt_dist_exit_to_untaken_low_r",
        "msl_st_untaken_high_touched_in_trade","msl_st_untaken_low_touched_in_trade",
        "msl_mt_untaken_high_touched_in_trade","msl_mt_untaken_low_touched_in_trade",
        "msl_lt_untaken_high_touched_in_trade","msl_lt_untaken_low_touched_in_trade",
    ]
    for c in cols:
        df[c] = np.nan
    return df

def build_block_21_or_ib(master: pd.DataFrame, ohlcv_by_symbol: Dict[str, pd.DataFrame], cfg: BuildConfig) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    cols = [
        "or_day_15m_high","or_day_15m_low","or_day_15m_mid","or_day_15m_size",
        "or_day_30m_high","or_day_30m_low","or_day_30m_mid","or_day_30m_size",
        "or_asia_30m_high","or_asia_30m_low","or_asia_30m_mid","or_asia_30m_size",
        "or_london_30m_high","or_london_30m_low","or_london_30m_mid","or_london_30m_size",
        "or_newyork_30m_high","or_newyork_30m_low","or_newyork_30m_mid","or_newyork_30m_size",
        "ib_london_60m_high","ib_london_60m_low","ib_london_60m_mid","ib_london_60m_size",
        "ib_newyork_60m_high","ib_newyork_60m_low","ib_newyork_60m_mid","ib_newyork_60m_size",
        "wor_mon_60m_high","wor_mon_60m_low","wor_mon_60m_mid","wor_mon_60m_size",
        "tib_tue_60m_high","tib_tue_60m_low","tib_tue_60m_mid","tib_tue_60m_size",
        "or_day_30m_break_up_before_exit","or_day_30m_break_dn_before_exit",
        "ib_london_60m_break_up_before_exit","ib_london_60m_break_dn_before_exit",
    ]
    for c in cols:
        df[c] = np.nan

    def window_hl(o: pd.DataFrame, start: pd.Timestamp, minutes: int) -> Optional[Tuple[float, float]]:
        end = start + pd.Timedelta(minutes=minutes)
        seg = o.loc[(o.index >= start) & (o.index < end)]
        if len(seg) == 0:
            return None
        return float(seg["high"].max()), float(seg["low"].min())

    for i, (sym, ot, ct) in enumerate(zip(master["symbol"], master["open_time"], master["close_time"])):
        o = ohlcv_by_symbol.get(sym)
        if o is None or len(o) == 0 or pd.isna(ot) or pd.isna(ct):
            continue

        d0 = ot.floor("D")

        # Day OR from 00:00
        day_start = d0
        hl15 = window_hl(o, day_start, cfg.orib.or_day_15m)
        if hl15:
            hi, lo = hl15
            df.loc[i, "or_day_15m_high"] = hi
            df.loc[i, "or_day_15m_low"] = lo
            df.loc[i, "or_day_15m_mid"] = (hi + lo) / 2
            df.loc[i, "or_day_15m_size"] = hi - lo

        hl30 = window_hl(o, day_start, cfg.orib.or_day_30m)
        if hl30:
            hi, lo = hl30
            df.loc[i, "or_day_30m_high"] = hi
            df.loc[i, "or_day_30m_low"] = lo
            df.loc[i, "or_day_30m_mid"] = (hi + lo) / 2
            df.loc[i, "or_day_30m_size"] = hi - lo

            or_end = day_start + pd.Timedelta(minutes=cfg.orib.or_day_30m)
            seg2 = o.loc[(o.index >= or_end) & (o.index <= ct)]
            if len(seg2) > 0:
                df.loc[i, "or_day_30m_break_up_before_exit"] = int(seg2["high"].max() > hi)
                df.loc[i, "or_day_30m_break_dn_before_exit"] = int(seg2["low"].min() < lo)

        # Session OR (30m) and IB (60m) anchored to your session starts
        for sess in ["asia","london","newyork"]:
            start_s, end_s = getattr(cfg.sessions, sess)
            sess_start, _ = _session_window(ot, start_s, end_s, cfg.tz)
            hl = window_hl(o, sess_start, cfg.orib.or_session_30m)
            if hl:
                hi, lo = hl
                df.loc[i, f"or_{sess}_30m_high"] = hi
                df.loc[i, f"or_{sess}_30m_low"] = lo
                df.loc[i, f"or_{sess}_30m_mid"] = (hi + lo) / 2
                df.loc[i, f"or_{sess}_30m_size"] = hi - lo

        for sess in ["london","newyork"]:
            start_s, end_s = getattr(cfg.sessions, sess)
            sess_start, _ = _session_window(ot, start_s, end_s, cfg.tz)
            hl = window_hl(o, sess_start, cfg.orib.ib_60m)
            if hl:
                hi, lo = hl
                df.loc[i, f"ib_{sess}_60m_high"] = hi
                df.loc[i, f"ib_{sess}_60m_low"] = lo
                df.loc[i, f"ib_{sess}_60m_mid"] = (hi + lo) / 2
                df.loc[i, f"ib_{sess}_60m_size"] = hi - lo

                ib_end = sess_start + pd.Timedelta(minutes=cfg.orib.ib_60m)
                seg3 = o.loc[(o.index >= ib_end) & (o.index <= ct)]
                if len(seg3) > 0 and sess == "london":
                    df.loc[i, "ib_london_60m_break_up_before_exit"] = int(seg3["high"].max() > hi)
                    df.loc[i, "ib_london_60m_break_dn_before_exit"] = int(seg3["low"].min() < lo)

        # Monday WOR: first 60m of Monday 00:00
        if ot.weekday() == 0:
            hl = window_hl(o, d0, cfg.orib.wor_mon_60m)
            if hl:
                hi, lo = hl
                df.loc[i, "wor_mon_60m_high"] = hi
                df.loc[i, "wor_mon_60m_low"] = lo
                df.loc[i, "wor_mon_60m_mid"] = (hi + lo) / 2
                df.loc[i, "wor_mon_60m_size"] = hi - lo

        # Tuesday TIB: first 60m of Tuesday 00:00
        if ot.weekday() == 1:
            hl = window_hl(o, d0, cfg.orib.tib_tue_60m)
            if hl:
                hi, lo = hl
                df.loc[i, "tib_tue_60m_high"] = hi
                df.loc[i, "tib_tue_60m_low"] = lo
                df.loc[i, "tib_tue_60m_mid"] = (hi + lo) / 2
                df.loc[i, "tib_tue_60m_size"] = hi - lo

    return df

def build_block_22_psych(master: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    df["psy_round_step_price"] = np.nan
    df["psy_round_step_ticks"] = np.nan
    df["psy_round_step_pips"] = np.nan

    for c in ["psy_round_below_entry","psy_round_above_entry","psy_round_nearest_entry"]:
        df[c] = np.nan
    for c in [
        "psy_dist_entry_to_round_nearest","psy_dist_entry_to_round_below","psy_dist_entry_to_round_above",
        "psy_dist_exit_to_round_nearest","psy_dist_exit_to_round_below","psy_dist_exit_to_round_above",
        "psy_round_crossed_in_trade","psy_round_touched_in_trade",
    ]:
        df[c] = np.nan

    tick = master.get("tick_size", pd.Series(np.nan, index=master.index))
    fallback = np.where(master["open_price"].astype(float) >= 100, 1.0, 0.0010)
    step = tick.fillna(pd.Series(fallback, index=master.index)) * 50.0
    df["psy_round_step_price"] = step

    ep = master["open_price"].astype(float)
    xp = master["close_price"].astype(float)

    below_e = np.floor(ep / step) * step
    above_e = np.ceil(ep / step) * step
    nearest_e = below_e.where((ep - below_e) <= (above_e - ep), other=above_e)

    df["psy_round_below_entry"] = below_e
    df["psy_round_above_entry"] = above_e
    df["psy_round_nearest_entry"] = nearest_e

    df["psy_dist_entry_to_round_nearest"] = ep - nearest_e
    df["psy_dist_entry_to_round_below"] = ep - below_e
    df["psy_dist_entry_to_round_above"] = ep - above_e

    below_x = np.floor(xp / step) * step
    above_x = np.ceil(xp / step) * step
    nearest_x = below_x.where((xp - below_x) <= (above_x - xp), other=above_x)

    df["psy_dist_exit_to_round_nearest"] = xp - nearest_x
    df["psy_dist_exit_to_round_below"] = xp - below_x
    df["psy_dist_exit_to_round_above"] = xp - above_x

    df["psy_round_touched_in_trade"] = (((df["psy_dist_entry_to_round_nearest"].abs() <= 0.1 * step) |
                                         (df["psy_dist_exit_to_round_nearest"].abs() <= 0.1 * step))).astype("int8")
    df["psy_round_crossed_in_trade"] = (np.floor(ep / step) != np.floor(xp / step)).astype("int8")
    return df

def build_block_23_vp(master: pd.DataFrame, cfg: BuildConfig) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    cols = [
        "vp_day_poc","vp_day_val","vp_day_vah",
        "vp_week_poc","vp_week_val","vp_week_vah",
        "vp_month_poc","vp_month_val","vp_month_vah",
        "vp_asia_poc","vp_london_poc","vp_newyork_poc",
        "vp_dist_entry_to_day_poc","vp_dist_exit_to_day_poc",
        "vp_dist_entry_to_week_poc","vp_dist_exit_to_week_poc",
        "vp_method","vp_bin_size_price",
    ]
    for c in cols:
        df[c] = np.nan
    df["vp_method"] = cfg.vp.method
    df["vp_bin_size_price"] = cfg.vp.bin_size_price
    return df

def build_block_24_confluence(master: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    for c in [
        "conf_prevday_and_round_confluence",
        "conf_untaken_and_prevday_confluence",
        "conf_num_key_levels_within_0p25r_entry",
        "conf_min_dist_to_any_key_levels_r",
        "conf_level_cluster_width_r",
    ]:
        df[c] = np.nan
    return df

def build_block_25_labels(master: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame({"ticket": master["ticket"]})
    if "pl_money" in master.columns and master["pl_money"].notna().any():
        df["label_win"] = (master["pl_money"] > 0).astype("int8")
    elif "pl_pips" in master.columns and master["pl_pips"].notna().any():
        df["label_win"] = (master["pl_pips"] > 0).astype("int8")
    else:
        df["label_win"] = np.nan

    df["label_r_multiple"] = master["r_multiple_price"].astype(float) if "r_multiple_price" in master.columns else np.nan
    df["label_exit_reason"] = master["exit_type"].astype("string") if "exit_type" in master.columns else "unknown"
    df["label_regime"] = np.nan
    return df


# =========================
# QA + Assembly
# =========================

def qa_block_unique(df: pd.DataFrame, key_cols: Tuple[str, ...], name: str) -> pd.DataFrame:
    key = list(key_cols)
    dup = df.duplicated(key).sum() if all(k in df.columns for k in key) else np.nan
    missing_key = df[key].isna().any(axis=1).sum() if all(k in df.columns for k in key) else np.nan
    return pd.DataFrame([{
        "block": name,
        "rows": len(df),
        "dup_keys": int(dup) if pd.notna(dup) else np.nan,
        "missing_key_rows": int(missing_key) if pd.notna(missing_key) else np.nan,
        "columns": len(df.columns),
    }])

def merge_master(blocks: Dict[str, pd.DataFrame], key_cols: Tuple[str, ...]) -> pd.DataFrame:
    key = list(key_cols)
    master = None
    for name, b in blocks.items():
        if all(k in b.columns for k in key) and b.duplicated(key).any():
            raise ValueError(f"{name}: duplicate keys on {key_cols}")
        master = b.copy() if master is None else master.merge(b, on=key, how="left")
    return master

def export_blocks_and_master(blocks: Dict[str, pd.DataFrame], master: pd.DataFrame, qa: pd.DataFrame, cfg: BuildConfig) -> None:
    ensure_dir(cfg.out_dir)
    blocks_dir = os.path.join(cfg.out_dir, "blocks")
    ensure_dir(blocks_dir)

    if cfg.export_blocks_csv:
        for name, df in blocks.items():
            export_csv(df, os.path.join(blocks_dir, f"{name}.csv"))

    export_csv(qa, os.path.join(cfg.out_dir, "qa_report.csv"))

    if cfg.export_master_parquet:
        export_parquet(master, os.path.join(cfg.out_dir, "trades_master.parquet"))
    if cfg.export_master_csv:
        export_csv(master, os.path.join(cfg.out_dir, "trades_master.csv"))


# =========================
# Pipeline runner
# =========================

def run_pipeline(trades_csv_path: str, cfg: BuildConfig) -> pd.DataFrame:
    df_raw = pd.read_csv(trades_csv_path)

    blocks: Dict[str, pd.DataFrame] = {}
    qa_rows: List[pd.DataFrame] = []

    b00 = build_block_00_raw(df_raw)
    blocks["block_00_raw"] = b00

    b01 = build_block_01_normalized(df_raw, cfg)
    blocks["block_01_normalized"] = b01
    qa_rows.append(qa_block_unique(b01, cfg.key_cols, "block_01_normalized"))

    symbols = sorted(set(b01["symbol"].dropna().astype(str).tolist()))
    ohlcv_by_symbol: Dict[str, pd.DataFrame] = {sym: load_ohlcv_for_symbol(sym, cfg) for sym in symbols}
    ohlcv_by_symbol = {k: v for k, v in ohlcv_by_symbol.items() if v is not None}

    b02 = build_block_02_realized_pnl(df_raw, b01)
    blocks["block_02_pnl"] = b02
    b03 = build_block_03_strategy_meta(b01)
    blocks["block_03_strategy_meta"] = b03
    b04 = build_block_04_instrument_vol_sl_tp(b01)
    blocks["block_04_instrument_vol_sl_tp"] = b04

    master = merge_master(
        {k: blocks[k] for k in ["block_01_normalized", "block_02_pnl", "block_03_strategy_meta", "block_04_instrument_vol_sl_tp"]},
        cfg.key_cols
    )

    b05 = build_block_05_sessions_daystats(master, ohlcv_by_symbol, cfg); blocks["block_05_sessions_daystats"] = b05; master = master.merge(b05, on=list(cfg.key_cols), how="left")
    b06 = build_block_06_day_positioning(master); blocks["block_06_day_positioning"] = b06; master = master.merge(b06, on=list(cfg.key_cols), how="left")
    b07 = build_block_07_entry_session_ohlc(master, ohlcv_by_symbol, cfg); blocks["block_07_entry_session_ohlc"] = b07; master = master.merge(b07, on=list(cfg.key_cols), how="left")
    b08 = build_block_08_exit_session_ohlc(master, ohlcv_by_symbol, cfg); blocks["block_08_exit_session_ohlc"] = b08; master = master.merge(b08, on=list(cfg.key_cols), how="left")
    b09 = build_block_09_entry_time_decomp(master); blocks["block_09_entry_time_decomp"] = b09; master = master.merge(b09, on=list(cfg.key_cols), how="left")
    b10 = build_block_10_day_extremes_timing(master, ohlcv_by_symbol); blocks["block_10_day_extremes_timing"] = b10; master = master.merge(b10, on=list(cfg.key_cols), how="left")
    b11 = build_block_11_dist_to_day_extremes(master); blocks["block_11_dist_to_day_extremes"] = b11; master = master.merge(b11, on=list(cfg.key_cols), how="left")
    b12 = build_block_12_asia(master, ohlcv_by_symbol, cfg); blocks["block_12_asia"] = b12; master = master.merge(b12, on=list(cfg.key_cols), how="left")
    b13 = build_block_13_london(master, ohlcv_by_symbol, cfg); blocks["block_13_london"] = b13; master = master.merge(b13, on=list(cfg.key_cols), how="left")
    b14 = build_block_14_newyork(master, ohlcv_by_symbol, cfg); blocks["block_14_newyork"] = b14; master = master.merge(b14, on=list(cfg.key_cols), how="left")
    b15 = build_block_15_trade_path(master, ohlcv_by_symbol, cfg); blocks["block_15_trade_path"] = b15; master = master.merge(b15, on=list(cfg.key_cols), how="left")
    b16 = build_block_16_week_context(master, ohlcv_by_symbol); blocks["block_16_week_context"] = b16; master = master.merge(b16, on=list(cfg.key_cols), how="left")
    b17 = build_block_17_month_context(master, ohlcv_by_symbol); blocks["block_17_month_context"] = b17; master = master.merge(b17, on=list(cfg.key_cols), how="left")
    b18 = build_block_18_exit_mechanics(master); blocks["block_18_exit_mechanics"] = b18; master = master.merge(b18, on=list(cfg.key_cols), how="left")
    b19 = build_block_19_prior_period_levels(master, ohlcv_by_symbol, cfg); blocks["block_19_prior_period_levels"] = b19; master = master.merge(b19, on=list(cfg.key_cols), how="left")
    b20 = build_block_20_msl_untaken(master, cfg); blocks["block_20_msl_untaken"] = b20; master = master.merge(b20, on=list(cfg.key_cols), how="left")
    b21 = build_block_21_or_ib(master, ohlcv_by_symbol, cfg); blocks["block_21_or_ib"] = b21; master = master.merge(b21, on=list(cfg.key_cols), how="left")
    b22 = build_block_22_psych(master); blocks["block_22_psych"] = b22; master = master.merge(b22, on=list(cfg.key_cols), how="left")
    b23 = build_block_23_vp(master, cfg); blocks["block_23_vp"] = b23; master = master.merge(b23, on=list(cfg.key_cols), how="left")
    b24 = build_block_24_confluence(master); blocks["block_24_confluence"] = b24; master = master.merge(b24, on=list(cfg.key_cols), how="left")
    b25 = build_block_25_labels(master); blocks["block_25_labels"] = b25; master = master.merge(b25, on=list(cfg.key_cols), how="left")

    for name, b in blocks.items():
        if all(k in b.columns for k in cfg.key_cols):
            qa_rows.append(qa_block_unique(b, cfg.key_cols, name))
    qa = pd.concat(qa_rows, ignore_index=True).drop_duplicates(subset=["block"], keep="last")

    export_blocks_and_master(blocks, master, qa, cfg)
    return master


if __name__ == "__main__":
    cfg = BuildConfig(
        tz="Europe/Berlin",
        out_dir="out",
        export_blocks_csv=True,
        export_master_parquet=True,
        export_master_csv=False,
        key_cols=("ticket",),
        # Set your OHLCV data location:
        ohlcv_root=None,  # e.g. "data/ohlcv_15m" or "data/ohlcv_all.parquet"
        ohlcv_file_pattern="{symbol}.parquet",
        ohlcv_time_col="time",
        ohlcv_symbol_col=None,
        timeframe_minutes=15,
        sessions=SessionsConfig(
            asia=("02:00", "06:00"),
            london=("08:00", "12:00"),
            newyork=("14:00", "18:00"),
        ),
    )

    # run_pipeline("trades.csv", cfg)
