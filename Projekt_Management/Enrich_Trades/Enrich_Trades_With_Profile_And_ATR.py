# 1.Data_Center/Data_Operations/Enrich_Trades/Enrich_Trades_With_Profile_ATR_Blocks_And_EOD.py
#
# Zweck
# - Liest MT5 Trade-CSV
# - Merged Strategy_Profile + ATR (M15) + Exit-Level aus Comment
# - Klassifiziert Exit zusätzlich als EOD/time-exit falls kein SL/TP im Comment
# - Schreibt enriched CSV nach:
#   1.Data_Center/Data/Strategy_Data/Backtest_Trades_Data_Enriched
# - Zusätzlich: schreibt Block-CSV (Block 00–25) pro Datei nach:
#   1.Data_Center/Data/Strategy_Data/Backtest_Trades_Data_Enriched/Blocks/<tradefile_stem>/
# - Block-Exporter garantiert: jede Block-CSV hat exakt die Zielspalten (fehlende werden als NA erzeugt)
# - QA Report pro Datei: missing columns pro block
#
# Hinweis:
# - Dieses Script berechnet aktuell nur Enrichment + EOD + minimal Labels.
# - Alle weiteren Features (Sessions/Day/OR/IB/MSL/VP/Confluence/Week/Month etc.) werden als Spalten erzeugt,
#   aber bleiben NA bis du die jeweiligen Feature-Builder integrierst.

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List

import pandas as pd


# =========================
# PROJECT STRUCTURE
# =========================
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = PROJECT_ROOT / "1.Data_Center" / "Data"

TRADES_DIR = DATA_ROOT / "Strategy_Data" / "Backtest_Trades_Data"
PROFILE_DIR = DATA_ROOT / "Strategy_Data" / "Strategy_Profile"
ATR_DIR = DATA_ROOT / "Regime_Data" / "OHLC" / "M15" / "ATR"

OUT_DIR = DATA_ROOT / "Strategy_Data" / "Backtest_Trades_Data_Enriched"
BLOCKS_ROOT = OUT_DIR / "Blocks"


# =========================
# REGEX
# =========================
RE_STRATEGY_ID = re.compile(r"(?:Strategy\s+)(\d+(?:\.\d+)*)", re.IGNORECASE)
RE_EXIT_LEVEL = re.compile(r"\b(sl|tp)\s+([0-9]+(?:\.[0-9]+)?)\b", re.IGNORECASE)


# =========================
# CONFIG
# =========================
ATR_SHIFT_DEFAULT = 1
FALLBACK_ATR_PERIOD = 14

# EOD/time exit detection:
# close_time within last N minutes before next day 00:00 (CSV local/broker time) => EOD close
EOD_CUTOFF_MIN = 30  # adjust to your broker behavior


# =========================
# DATA STRUCTURES
# =========================
@dataclass
class RiskModel:
    type: str
    stop_loss_pips: Optional[float] = None
    stop_loss_coef: Optional[float] = None
    take_profit_coef: Optional[float] = None
    atr_period: Optional[int] = None
    atr_shift: Optional[int] = None


@dataclass
class TradeModel:
    sl_fixed: Optional[float]
    tp_fixed: Optional[float]
    sl_coef: Optional[float]
    tp_coef: Optional[float]
    trailing_coef: Optional[float]


@dataclass
class Profile:
    strategy_id: str
    symbol: str
    side: str
    tick_size: Optional[float]
    risk_model: RiskModel
    trade_model: TradeModel
    raw: Dict[str, Any]


# =========================
# UTIL
# =========================
def _to_float_or_none(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _to_int_or_none(x) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(float(x))
    except Exception:
        return None


def parse_strategy_id_from_comment(comment: str) -> Optional[str]:
    m = RE_STRATEGY_ID.search(comment or "")
    return m.group(1) if m else None


def parse_exit_level(comment: str) -> Tuple[Optional[str], Optional[float]]:
    """
    Only parses sl/tp + price from comment.
    Time/EOD exits are inferred later.
    """
    m = RE_EXIT_LEVEL.search(comment or "")
    if not m:
        return None, None
    return m.group(1).lower(), float(m.group(2))


def side_sign(trade_type: str) -> int:
    t = str(trade_type).upper()
    if "BUY" in t:
        return +1
    if "SELL" in t:
        return -1
    raise ValueError(f"unknown trade type: {trade_type}")


def infer_eod_close(close_time: pd.Timestamp, cutoff_min: int = EOD_CUTOFF_MIN) -> bool:
    """
    EOD definition:
      close_time is within last `cutoff_min` minutes before next day 00:00.
    Works for naive timestamps (CSV local/broker time) and tz-aware timestamps.
    """
    if pd.isna(close_time):
        return False
    ct = close_time
    day_end = ct.normalize() + pd.Timedelta(days=1)  # next day 00:00
    return (day_end - ct) <= pd.Timedelta(minutes=cutoff_min)


def minutes_to_day_end(close_time: pd.Timestamp) -> Optional[float]:
    if pd.isna(close_time):
        return None
    ct = close_time
    day_end = ct.normalize() + pd.Timedelta(days=1)
    return float((day_end - ct).total_seconds() / 60.0)


# =========================
# LOADERS
# =========================
def load_profiles(profile_dir: Path) -> Dict[str, Profile]:
    profiles: Dict[str, Profile] = {}
    if not profile_dir.exists():
        return profiles

    for fp in profile_dir.glob("*.json"):
        try:
            d = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue

        ident = d.get("identity") or {}
        sid = str(ident.get("strategy_id") or "").strip()
        if not sid:
            continue

        symbol = str(ident.get("symbol") or "").strip()
        side = str(ident.get("side") or "").strip().upper()

        raw_inputs = d.get("raw_inputs") or {}
        tick_size = _to_float_or_none(raw_inputs.get("MainChartTickSizeSQ"))

        tp = d.get("trade_parameters") or {}
        fixed = tp.get("fixed") or {}
        dyn = tp.get("dynamic_coef") or {}

        trade_model = TradeModel(
            sl_fixed=_to_float_or_none(fixed.get("stop_loss_pips")),
            tp_fixed=_to_float_or_none(fixed.get("take_profit_pips")),
            sl_coef=_to_float_or_none(dyn.get("stop_loss_coef")),
            tp_coef=_to_float_or_none(dyn.get("take_profit_coef")),
            trailing_coef=_to_float_or_none(dyn.get("trailing_stop_coef")),
        )

        rm = d.get("risk_model") or {}
        rm_type = str(rm.get("type") or "unknown")

        atr = rm.get("atr") or {}
        risk_model = RiskModel(
            type=rm_type,
            stop_loss_pips=_to_float_or_none(rm.get("stop_loss_pips")),
            stop_loss_coef=_to_float_or_none(rm.get("stop_loss_coef")),
            take_profit_coef=_to_float_or_none(rm.get("take_profit_coef")),
            atr_period=_to_int_or_none(atr.get("atr_period")),
            atr_shift=_to_int_or_none(atr.get("atr_shift")),
        )
        if risk_model.atr_shift is None:
            risk_model.atr_shift = ATR_SHIFT_DEFAULT

        profiles[sid] = Profile(
            strategy_id=sid,
            symbol=symbol,
            side=side,
            tick_size=tick_size,
            risk_model=risk_model,
            trade_model=trade_model,
            raw=d,
        )

    return profiles


def load_trades_csv(trades_path: Path) -> pd.DataFrame:
    df = pd.read_csv(trades_path)

    rename_map = {
        "Ticket": "ticket",
        "Open time": "open_time",
        "Open price": "open_price",
        "Close time": "close_time",
        "Close price": "close_price",
        "Type": "type",
        "Symbol": "symbol",
        "Size": "size",
        "Comment": "comment",
        "Sample type": "sample_type",
        "P/L in money": "pl_money",
        "P/L in pips": "pl_pips",
        "Comm/Swap": "comm_swap",
    }
    for k, v in rename_map.items():
        if k in df.columns:
            df[v] = df[k]

    required = ["open_time", "open_price", "close_time", "close_price", "type", "symbol"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"missing required column: {c}")

    df["open_time"] = pd.to_datetime(df["open_time"], dayfirst=True, errors="coerce")
    df["close_time"] = pd.to_datetime(df["close_time"], dayfirst=True, errors="coerce")
    if df["open_time"].isna().any():
        raise ValueError("open_time contains unparsable timestamps")
    if df["close_time"].isna().any():
        raise ValueError("close_time contains unparsable timestamps")

    df["open_price"] = pd.to_numeric(df["open_price"], errors="coerce")
    df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
    if df["open_price"].isna().any() or df["close_price"].isna().any():
        raise ValueError("open_price/close_price contains NaNs after numeric conversion")

    df["type"] = df["type"].astype(str).str.strip().str.upper()
    df["symbol"] = df["symbol"].astype(str).str.strip()

    if "comment" in df.columns:
        df["comment"] = df["comment"].astype(str)
    else:
        df["comment"] = ""

    if "ticket" not in df.columns:
        df["ticket"] = pd.RangeIndex(start=1, stop=len(df) + 1, step=1).astype(str)
    else:
        df["ticket"] = df["ticket"].astype(str)

    return df


def load_atr_csv(symbol: str) -> pd.DataFrame:
    fp = ATR_DIR / f"{symbol}_M15_ATR.csv"
    if not fp.exists():
        raise FileNotFoundError(f"ATR file not found for symbol={symbol}: {fp}")

    df = pd.read_csv(fp)
    if "time" not in df.columns:
        raise ValueError(f"ATR CSV missing 'time': {fp}")

    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    if df["time"].isna().any():
        raise ValueError(f"ATR CSV time unparsable: {fp}")

    df = df.sort_values("time").drop_duplicates(subset=["time"], keep="last").reset_index(drop=True)
    return df


def choose_atr_column(atr_df: pd.DataFrame, period: int) -> str:
    col = f"atr_{period}"
    if col not in atr_df.columns:
        raise ValueError(f"ATR period {period} not found in ATR CSV columns: {list(atr_df.columns)}")
    return col


def atr_value_for_entry(atr_df: pd.DataFrame, entry_time: pd.Timestamp, period: int, shift: int) -> Optional[float]:
    """
    shift=1 => previous bar.
    ATR CSV time aligns with bar open times (UTC).
    We pick last atr_time <= entry_time(UTC) - 15min*shift
    """
    et = entry_time
    et = et.tz_localize("UTC") if et.tzinfo is None else et.tz_convert("UTC")
    target = et - pd.Timedelta(minutes=15 * shift)

    idx = atr_df["time"].searchsorted(target, side="right") - 1
    if idx < 0:
        return None

    col = choose_atr_column(atr_df, period)
    v = atr_df.iloc[idx][col]
    return float(v) if pd.notna(v) else None


def compute_initial_levels(
    profile: Profile,
    entry_price: float,
    trade_type: str,
    atr_value: Optional[float],
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Returns:
      (sl_dist_price, tp_dist_price, sl_price, tp_price)
    Distances are in PRICE UNITS.
    """
    sgn = side_sign(trade_type)
    tick = profile.tick_size

    def to_price_dist(x: Optional[float]) -> Optional[float]:
        if x is None:
            return None
        if tick is None:
            return float(x)
        return float(x) * float(tick)

    sl_dist = None
    tp_dist = None

    # fixed first
    if profile.trade_model.sl_fixed is not None:
        sl_dist = to_price_dist(profile.trade_model.sl_fixed)
    if profile.trade_model.tp_fixed is not None:
        tp_dist = to_price_dist(profile.trade_model.tp_fixed)

    # atr based
    if atr_value is not None:
        if sl_dist is None and profile.trade_model.sl_coef is not None:
            sl_dist = float(profile.trade_model.sl_coef) * float(atr_value)
        if tp_dist is None and profile.trade_model.tp_coef is not None:
            tp_dist = float(profile.trade_model.tp_coef) * float(atr_value)

    sl_price = entry_price - sgn * sl_dist if sl_dist is not None else None
    tp_price = entry_price + sgn * tp_dist if tp_dist is not None else None
    return sl_dist, tp_dist, sl_price, tp_price


# =========================
# FULL BLOCK SCHEMA (00–25)
# =========================
def get_block_schema() -> Dict[str, List[str]]:
    return {
        "block_00_raw": [
            "Ticket","Symbol","Type","Open time","Open price","Size","Close time","Close price","Time in trade",
            "Profit/Loss","Cummulative P/L","Comm/Swap","P/L in money","Cummulative money P/L",
            "P/L in pips","Cummulative pips P/L","P/L in %","Cummulative % P/L","Comment","Sample type"
        ],
        "block_01_normalized": [
            "ticket","open_time","open_price","close_time","close_price","type","symbol","size","comment","sample_type"
        ],
        "block_02_realized_pnl": ["pl_money","pl_pips","comm_swap"],
        "block_03_strategy_meta": [
            "strategy_id","exit_type","exit_level_price","profile_found","risk_model_type","__source_file",
            # EOD additions
            "exit_is_eod","exit_minutes_to_day_end","exit_reason_confidence"
        ],
        "block_04_instrument_vol_sl_tp": [
            "tick_size","atr_period","atr_shift","atr_value",
            "init_sl_dist_price","init_tp_dist_price","init_sl_price","init_tp_price",
            "realized_exit_dist_price","r_multiple_price"
        ],
        "block_05_sessions_daystats": [
            "entry_session","exit_session","day_open","day_high","day_low","day_close","day_range"
        ],
        "block_06_day_positioning": [
            "entry_pos_in_day","exit_pos_in_day",
            "entry_dist_to_day_high","entry_dist_to_day_low",
            "exit_dist_to_day_high","exit_dist_to_day_low",
            "mfe_vs_day_range","realized_vs_day_range"
        ],
        "block_07_entry_session_ohlc": [
            "entry_sess_open","entry_sess_close","entry_sess_high","entry_sess_low",
            "entry_sess_range","entry_sess_return","entry_sess_return_dir","entry_sess_return_dir_r"
        ],
        "block_08_exit_session_ohlc": [
            "exit_sess_open","exit_sess_close","exit_sess_high","exit_sess_low",
            "exit_sess_range","exit_sess_return","exit_sess_return_dir","exit_sess_return_dir_r"
        ],
        "block_09_entry_time_decomp": [
            "entry_hour","entry_minute","entry_weekday","entry_week_of_year","entry_month","entry_quarter"
        ],
        "block_10_day_extremes_timing": [
            "day_high_time","day_low_time","day_return"
        ],
        "block_11_dist_to_day_extremes": [
            "dist_entry_to_day_high","dist_entry_to_day_low","dist_exit_to_day_high","dist_exit_to_day_low",
            "dist_entry_to_day_high_r","dist_entry_to_day_low_r","dist_exit_to_day_high_r","dist_exit_to_day_low_r"
        ],
        "block_12_asia": [
            "asia_open","asia_close","asia_high","asia_low","asia_high_time","asia_low_time","asia_range","asia_return",
            "entry_pos_in_asia","exit_pos_in_asia",
            "dist_entry_to_asia_high","dist_entry_to_asia_low","dist_exit_to_asia_high","dist_exit_to_asia_low",
            "dist_entry_to_asia_high_r","dist_entry_to_asia_low_r","dist_exit_to_asia_high_r","dist_exit_to_asia_low_r",
            "time_to_asia_high_after_entry_min","time_to_asia_low_after_entry_min"
        ],
        "block_13_london": [
            "london_open","london_close","london_high","london_low","london_high_time","london_low_time","london_range","london_return",
            "entry_pos_in_london","exit_pos_in_london",
            "dist_entry_to_london_high","dist_entry_to_london_low","dist_exit_to_london_high","dist_exit_to_london_low",
            "dist_entry_to_london_high_r","dist_entry_to_london_low_r","dist_exit_to_london_high_r","dist_exit_to_london_low_r",
            "time_to_london_high_after_entry_min","time_to_london_low_after_entry_min"
        ],
        "block_14_newyork": [
            "newyork_open","newyork_close","newyork_high","newyork_low","newyork_high_time","newyork_low_time","newyork_range","newyork_return",
            "entry_pos_in_newyork","exit_pos_in_newyork",
            "dist_entry_to_newyork_high","dist_entry_to_newyork_low","dist_exit_to_newyork_high","dist_exit_to_newyork_low",
            "dist_entry_to_newyork_high_r","dist_entry_to_newyork_low_r","dist_exit_to_newyork_high_r","dist_exit_to_newyork_low_r",
            "time_to_newyork_high_after_entry_min","time_to_newyork_low_after_entry_min"
        ],
        "block_15_trade_path": [
            "trade_duration_minutes","time_to_day_high_after_entry_min","time_to_day_low_after_entry_min",
            "mfe_price","mfe_time","mae_price","mae_time","time_to_mfe_minutes","time_to_mae_minutes"
        ],
        "block_16_week_context": [
            "week_open","week_high_to_entry","week_high_time_to_entry","week_low_to_entry","week_low_time_to_entry",
            "week_range_to_entry","entry_pos_in_week","dist_entry_to_week_high","dist_entry_to_week_low",
            "dist_entry_to_week_high_r","dist_entry_to_week_low_r"
        ],
        "block_17_month_context": [
            "month_open","month_high_to_entry","month_high_time_to_entry","month_low_to_entry","month_low_time_to_entry",
            "month_range_to_entry","entry_pos_in_month","dist_entry_to_month_high","dist_entry_to_month_low",
            "dist_entry_to_month_high_r","dist_entry_to_month_low_r"
        ],
        "block_18_exit_mechanics": [
            "exit_hit_sl","exit_hit_tp","exit_dist_to_tp_price","exit_dist_to_sl_price","exit_dist_to_tp_r","exit_dist_to_sl_r"
        ],
        "block_19_prior_period_levels": [
            "ppl_prev_day_open","ppl_prev_day_high","ppl_prev_day_low","ppl_prev_day_close",
            "ppl_prev_week_open","ppl_prev_week_high","ppl_prev_week_low","ppl_prev_week_close",
            "ppl_prev_month_open","ppl_prev_month_high","ppl_prev_month_low","ppl_prev_month_close",
            "ppl_dist_entry_to_prev_day_high","ppl_dist_entry_to_prev_day_low","ppl_dist_entry_to_prev_day_open","ppl_dist_entry_to_prev_day_close",
            "ppl_dist_entry_to_prev_week_high","ppl_dist_entry_to_prev_week_low","ppl_dist_entry_to_prev_month_high","ppl_dist_entry_to_prev_month_low",
            "ppl_dist_exit_to_prev_day_high","ppl_dist_exit_to_prev_day_low","ppl_dist_exit_to_prev_week_high","ppl_dist_exit_to_prev_week_low",
            "ppl_dist_exit_to_prev_month_high","ppl_dist_exit_to_prev_month_low",
            "ppl_dist_entry_to_prev_day_high_r","ppl_dist_entry_to_prev_day_low_r",
            "ppl_dist_entry_to_prev_week_high_r","ppl_dist_entry_to_prev_week_low_r",
            "ppl_dist_entry_to_prev_month_high_r","ppl_dist_entry_to_prev_month_low_r",
            "ppl_dist_exit_to_prev_day_high_r","ppl_dist_exit_to_prev_day_low_r",
            "ppl_dist_exit_to_prev_week_high_r","ppl_dist_exit_to_prev_week_low_r",
            "ppl_dist_exit_to_prev_month_high_r","ppl_dist_exit_to_prev_month_low_r",
        ],
        "block_20_msl_untaken": [
            "msl_st_window","msl_mt_window","msl_lt_window",
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
        ],
        "block_21_or_ib": [
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
        ],
        "block_22_psych": [
            "psy_round_step_price","psy_round_step_ticks","psy_round_step_pips",
            "psy_round_below_entry","psy_round_above_entry","psy_round_nearest_entry",
            "psy_dist_entry_to_round_nearest","psy_dist_entry_to_round_below","psy_dist_entry_to_round_above",
            "psy_dist_exit_to_round_nearest","psy_dist_exit_to_round_below","psy_dist_exit_to_round_above",
            "psy_round_crossed_in_trade","psy_round_touched_in_trade",
        ],
        "block_23_vp": [
            "vp_day_poc","vp_day_val","vp_day_vah",
            "vp_week_poc","vp_week_val","vp_week_vah",
            "vp_month_poc","vp_month_val","vp_month_vah",
            "vp_asia_poc","vp_london_poc","vp_newyork_poc",
            "vp_dist_entry_to_day_poc","vp_dist_exit_to_day_poc",
            "vp_dist_entry_to_week_poc","vp_dist_exit_to_week_poc",
            "vp_method","vp_bin_size_price",
        ],
        "block_24_confluence": [
            "conf_prevday_and_round_confluence",
            "conf_untaken_and_prevday_confluence",
            "conf_num_key_levels_within_0p25r_entry",
            "conf_min_dist_to_any_key_level_r",
            "conf_level_cluster_width_r",
        ],
        "block_25_labels": [
            "label_win","label_r_multiple","label_exit_reason","label_regime"
        ],
    }


# =========================
# ENRICH (Profile + ATR + EOD)
# =========================
def enrich_trades(trades_df: pd.DataFrame, profiles: Dict[str, Profile]) -> pd.DataFrame:
    atr_cache: Dict[str, pd.DataFrame] = {}
    out = trades_df.copy()

    # minimal meta
    out["__source_file"] = pd.NA

    # parse comment
    out["strategy_id"] = out["comment"].apply(parse_strategy_id_from_comment)
    out["exit_type"], out["exit_level_price"] = zip(*out["comment"].apply(parse_exit_level))

    # EOD features
    out["exit_minutes_to_day_end"] = out["close_time"].apply(minutes_to_day_end)
    out["exit_is_eod"] = out["close_time"].apply(lambda t: infer_eod_close(t, cutoff_min=EOD_CUTOFF_MIN)).astype("Int64")

    # confidence: 1.0 if sl/tp parsed, else 0.7 if eod detected, else 0.2
    conf = []
    for et, is_eod in zip(out["exit_type"], out["exit_is_eod"]):
        if isinstance(et, str) and et in ("sl", "tp"):
            conf.append(1.0)
        elif pd.notna(is_eod) and int(is_eod) == 1:
            conf.append(0.7)
        else:
            conf.append(0.2)
    out["exit_reason_confidence"] = conf

    # infer time exit if no sl/tp and EOD
    mask_time = out["exit_type"].isna() & (out["exit_is_eod"] == 1)
    out.loc[mask_time, "exit_type"] = "time"
    out.loc[mask_time, "exit_level_price"] = pd.NA
    out["exit_type"] = out["exit_type"].fillna("unknown")

    def get_profile(sid: str) -> Optional[Profile]:
        if not sid:
            return None
        return profiles.get(sid)

    out["profile_found"] = out["strategy_id"].apply(lambda sid: get_profile(sid) is not None)
    out["risk_model_type"] = out["strategy_id"].apply(lambda sid: (get_profile(sid).risk_model.type if get_profile(sid) else None))
    out["tick_size"] = out["strategy_id"].apply(lambda sid: (get_profile(sid).tick_size if get_profile(sid) else None))
    out["atr_period"] = out["strategy_id"].apply(lambda sid: (get_profile(sid).risk_model.atr_period if get_profile(sid) else None))
    out["atr_shift"] = out["strategy_id"].apply(lambda sid: (get_profile(sid).risk_model.atr_shift if get_profile(sid) else None))

    # ATR lookup
    atr_vals = []
    for _, r in out.iterrows():
        sid = r["strategy_id"]
        prof = get_profile(sid)
        if prof is None:
            atr_vals.append(None)
            continue

        needs_atr = (
            (prof.trade_model.sl_coef is not None)
            or (prof.trade_model.tp_coef is not None)
            or (prof.risk_model.type == "coef_atr")
        )
        if not needs_atr:
            atr_vals.append(None)
            continue

        sym = r["symbol"]
        period = prof.risk_model.atr_period or FALLBACK_ATR_PERIOD
        shift = prof.risk_model.atr_shift or ATR_SHIFT_DEFAULT

        if sym not in atr_cache:
            atr_cache[sym] = load_atr_csv(sym)

        v = atr_value_for_entry(atr_cache[sym], r["open_time"], period=period, shift=shift)
        atr_vals.append(v)

    out["atr_value"] = atr_vals

    # initial SL/TP
    sl_dist_list, tp_dist_list, sl_price_list, tp_price_list = [], [], [], []
    for _, r in out.iterrows():
        sid = r["strategy_id"]
        prof = get_profile(sid)
        if prof is None:
            sl_dist_list.append(None)
            tp_dist_list.append(None)
            sl_price_list.append(None)
            tp_price_list.append(None)
            continue

        sl_dist, tp_dist, sl_price, tp_price = compute_initial_levels(
            prof, float(r["open_price"]), r["type"], r["atr_value"]
        )
        sl_dist_list.append(sl_dist)
        tp_dist_list.append(tp_dist)
        sl_price_list.append(sl_price)
        tp_price_list.append(tp_price)

    out["init_sl_dist_price"] = sl_dist_list
    out["init_tp_dist_price"] = tp_dist_list
    out["init_sl_price"] = sl_price_list
    out["init_tp_price"] = tp_price_list

    # realized exit dist only if sl/tp exists
    realized_dist = []
    for _, r in out.iterrows():
        et = r["exit_type"]
        lvl = r["exit_level_price"]
        if et not in ("sl", "tp") or lvl is None or pd.isna(lvl):
            realized_dist.append(None)
            continue

        entry = float(r["open_price"])
        sgn = side_sign(r["type"])
        if et == "sl":
            dist = (entry - lvl) if sgn == +1 else (lvl - entry)
        else:
            dist = (lvl - entry) if sgn == +1 else (entry - lvl)
        realized_dist.append(dist)

    out["realized_exit_dist_price"] = realized_dist

    # R multiple in price-space (uses close move / init SL dist)
    r_mult = []
    for _, r in out.iterrows():
        sl_dist = r["init_sl_dist_price"]
        if sl_dist is None or pd.isna(sl_dist) or float(sl_dist) == 0.0:
            r_mult.append(None)
            continue
        entry = float(r["open_price"])
        exitp = float(r["close_price"])
        sgn = side_sign(r["type"])
        move = sgn * (exitp - entry)
        r_mult.append(move / float(sl_dist))
    out["r_multiple_price"] = r_mult

    # minimal labels (Block 25)
    if "pl_money" in out.columns:
        out["label_win"] = (pd.to_numeric(out["pl_money"], errors="coerce") > 0).astype("Int64")
    else:
        out["label_win"] = pd.NA
    out["label_r_multiple"] = out.get("r_multiple_price", pd.NA)
    out["label_exit_reason"] = out["exit_type"]
    out["label_regime"] = pd.NA

    return out


# =========================
# EXPORT BLOCKS (guaranteed schema)
# =========================
def export_blocks(enriched_df: pd.DataFrame, blocks_dir: Path) -> None:
    blocks_dir.mkdir(parents=True, exist_ok=True)
    schema = get_block_schema()

    qa_rows = []

    for block_name, cols in schema.items():
        missing = [c for c in cols if c not in enriched_df.columns]
        existing = [c for c in cols if c in enriched_df.columns]

        # Build output with exact schema order
        out_df = pd.DataFrame(index=enriched_df.index)

        # copy existing
        for c in existing:
            out_df[c] = enriched_df[c]

        # add missing as NA
        for c in missing:
            out_df[c] = pd.NA

        # ensure column order exactly as schema
        out_df = out_df[cols]

        out_path = blocks_dir / f"{block_name}.csv"
        out_df.to_csv(out_path, index=False)

        qa_rows.append({
            "block": block_name,
            "exported_cols": len(existing),
            "missing_cols": len(missing),
            "missing_list": ";".join(missing) if missing else "",
        })

    pd.DataFrame(qa_rows).to_csv(blocks_dir / "_QA_missing_columns.csv", index=False)


# =========================
# RUN
# =========================
def main():
    if not PROFILE_DIR.exists():
        raise FileNotFoundError(f"Strategy_Profile dir not found: {PROFILE_DIR}")
    if not TRADES_DIR.exists():
        raise FileNotFoundError(f"Trades dir not found: {TRADES_DIR}")
    if not ATR_DIR.exists():
        raise FileNotFoundError(f"ATR dir not found: {ATR_DIR}")

    profiles = load_profiles(PROFILE_DIR)
    if not profiles:
        raise RuntimeError(f"No profiles loaded from: {PROFILE_DIR}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    BLOCKS_ROOT.mkdir(parents=True, exist_ok=True)

    trade_files = sorted(TRADES_DIR.glob("*.csv"))
    if not trade_files:
        print(f"[INFO] No trade CSV found in {TRADES_DIR}")
        return

    for trades_path in trade_files:
        try:
            df = load_trades_csv(trades_path)
            enriched = enrich_trades(df, profiles)

            # store source file name in enriched (useful downstream)
            enriched["__source_file"] = trades_path.name

            out_path = OUT_DIR / f"{trades_path.stem}_enriched.csv"
            enriched.to_csv(out_path, index=False)

            per_file_blocks_dir = BLOCKS_ROOT / trades_path.stem
            export_blocks(enriched, per_file_blocks_dir)

            print(f"[OK] {trades_path.name} -> {out_path}")
            print(f"[OK] blocks -> {per_file_blocks_dir}")

        except Exception as e:
            print(f"[ERROR] {trades_path.name}: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
