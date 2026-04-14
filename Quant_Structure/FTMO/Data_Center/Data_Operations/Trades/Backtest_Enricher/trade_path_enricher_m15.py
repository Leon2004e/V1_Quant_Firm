# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Data_Center/Data_Operations/Trades/Backtest_Enricher/trade_path_enricher_m15.py

Zweck:
- Liest rekursiv alle IS/OOS Backtest-Trade-Dateien
- Enriched jeden Trade mit M15-OHLC-Pfaddaten
- Joint optional Strategy_Profile-Daten über strategy_id
- Speichert angereicherte CSV-Dateien in spiegelnder Ordnerstruktur

Neue Input-Struktur:
- Quant_Structure/FTMO/Data_Center/Data/Trades/Feutered/Backtest/IS_OOS
- Quant_Structure/FTMO/Data_Center/Data/Ohcl/Raw/M15/*.parquet
- Quant_Structure/FTMO/Data_Center/Data/Strategy/Strategy_Profile/**/*.json

Neue Output-Struktur:
- Quant_Structure/FTMO/Data_Center/Data/Trades/Feutered/Backtest/IS_OOS_Enriched
    ├── DEMO_ACCOUNT
    │   ├── IS
    │   └── OOS
    └── LIVE_ACCOUNT
        ├── IS
        └── OOS

Wichtig:
- Pfad-Analyse erfolgt auf M15
- MFE/MAE werden über High/Low der M15-Bars zwischen Open und Close gemessen
- Trades vor verfügbarem OHLC-Start werden explizit markiert
- Hauptmetriken mfe_abs / mae_abs werden execution-konsistent angepasst:
    * Gewinntrade -> mfe_abs mindestens realized_abs_move
    * Verlusttrade -> mae_abs mindestens abs(realized_abs_move)
- Rohmetriken bleiben separat erhalten:
    * raw_mfe_abs
    * raw_mae_abs
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


# ============================================================
# PROJECT ROOT
# ============================================================

def find_ftmo_root(start: Path) -> Path:
    cur = start.resolve()
    for p in [cur] + list(cur.parents):
        if (p / "Data_Center").exists() and (p / "Dashboards").exists():
            return p
    raise RuntimeError(
        f"FTMO-Root nicht gefunden. Erwartet Root mit "
        f"'Data_Center' und 'Dashboards'. Start={start}"
    )


SCRIPT_PATH = Path(__file__).resolve()
FTMO_ROOT = find_ftmo_root(SCRIPT_PATH)


# ============================================================
# PATHS
# ============================================================

TRADES_ROOT = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Trades"
    / "Feutered"
    / "Backtest"
    / "IS_OOS"
)

OHLC_ROOT = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Ohcl"
    / "Raw"
    / "M15"
)

PROFILE_ROOT = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Strategy"
    / "Strategy_Profile"
)

OUTPUT_ROOT = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Trades"
    / "Feutered"
    / "Backtest"
    / "IS_OOS_Enriched"
)

FAILED_REPORT_PATH = OUTPUT_ROOT / "failed_files.csv"

VALID_ACCOUNT_TYPES = {"DEMO_ACCOUNT", "LIVE_ACCOUNT"}
VALID_SAMPLE_TYPES = {"IS", "OOS"}


# ============================================================
# CONFIG
# ============================================================

TRADE_REQUIRED_COLUMNS = [
    "position_id",
    "symbol",
    "direction",
    "open_time_utc",
    "close_time_utc",
    "entry_price",
    "exit_price",
    "profit_sum",
    "net_sum",
    "strategy_id",
    "sample_type",
    "account_type",
]

PROFILE_FIELDS_TO_ATTACH = [
    ("identity.strategy_id", "profile_strategy_id"),
    ("identity.symbol", "profile_symbol"),
    ("identity.side", "profile_side"),
    ("identity.variant_number", "profile_variant_number"),
    ("identity.timeframe", "profile_timeframe"),
    ("profile_naming.base_name", "profile_base_name"),
    ("profile_naming.display_name", "profile_display_name"),
    ("profile_naming.extended_display_name", "profile_extended_display_name"),
    ("profile_naming.exit_label", "profile_exit_label"),
    ("profile_naming.signal_label", "profile_signal_label"),
    ("profile_naming.time_label", "profile_time_label"),
    ("classification.exit_profile", "profile_exit_profile"),
    ("classification.signal_family", "profile_signal_family"),
    ("risk_model.type", "profile_risk_model_type"),
    ("trade_parameters.fixed.stop_loss_pips", "profile_sl_pips"),
    ("trade_parameters.fixed.take_profit_pips", "profile_tp_pips"),
    ("trade_parameters.dynamic_coef.stop_loss_coef", "profile_sl_coef"),
    ("trade_parameters.dynamic_coef.take_profit_coef", "profile_tp_coef"),
    ("trade_parameters.dynamic_coef.trailing_stop_coef", "profile_trailing_coef"),
]


# ============================================================
# HELPERS
# ============================================================

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def ensure_output_dirs() -> None:
    for account_type in VALID_ACCOUNT_TYPES:
        for sample_type in VALID_SAMPLE_TYPES:
            ensure_dir(OUTPUT_ROOT / account_type / sample_type)


def read_csv_safe(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def safe_to_datetime_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def floor_to_m15(ts: pd.Timestamp) -> pd.Timestamp:
    return ts.floor("15min")


def ceil_to_m15(ts: pd.Timestamp) -> pd.Timestamp:
    return ts.ceil("15min")


def safe_symbol_filename(symbol: str) -> str:
    return str(symbol).replace("/", "_") + ".parquet"


def get_ohlc_path(symbol: str) -> Path:
    return OHLC_ROOT / safe_symbol_filename(symbol)


def sanitize_name(name: str) -> str:
    return re.sub(r'[<>:"/\\\\|?*]', "_", str(name)).strip()


def extract_nested(d: dict, path: str):
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def safe_text(x: object) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def safe_upper(x: object) -> str:
    return safe_text(x).upper()


def detect_account_type_from_relpath(rel_parts: Tuple[str, ...]) -> str:
    if len(rel_parts) >= 1:
        candidate = str(rel_parts[0]).strip().upper()
        if candidate in VALID_ACCOUNT_TYPES:
            return candidate
    return "UNKNOWN"


def detect_sample_type_from_relpath(rel_parts: Tuple[str, ...]) -> str:
    if len(rel_parts) >= 2:
        candidate = str(rel_parts[1]).strip().upper()
        if candidate in VALID_SAMPLE_TYPES:
            return candidate
    return "UNKNOWN"


def build_output_path(input_file: Path) -> Path:
    rel = input_file.resolve().relative_to(TRADES_ROOT.resolve())
    output_dir = OUTPUT_ROOT / rel.parent
    ensure_dir(output_dir)
    return output_dir / sanitize_name(f"{input_file.stem}__enriched_m15.csv")


# ============================================================
# PROFILE LOADER
# ============================================================

def load_strategy_profiles(profile_root: Path) -> Dict[str, dict]:
    profiles: Dict[str, dict] = {}

    if not profile_root.exists():
        return profiles

    for path in sorted(profile_root.rglob("*.json")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)

            strategy_id = extract_nested(obj, "identity.strategy_id")
            if not strategy_id:
                continue

            strategy_id = safe_text(strategy_id)
            if strategy_id and strategy_id not in profiles:
                profiles[strategy_id] = obj

        except Exception:
            continue

    return profiles


# ============================================================
# OHLC CACHE
# ============================================================

class OHLCCache:
    def __init__(self) -> None:
        self._cache: Dict[str, pd.DataFrame] = {}

    def get(self, symbol: str) -> pd.DataFrame:
        symbol = safe_text(symbol)

        if symbol in self._cache:
            return self._cache[symbol]

        path = get_ohlc_path(symbol)
        if not path.exists():
            self._cache[symbol] = pd.DataFrame()
            return self._cache[symbol]

        df = pd.read_parquet(path)
        if df.empty:
            self._cache[symbol] = pd.DataFrame()
            return self._cache[symbol]

        if "time" not in df.columns:
            raise RuntimeError(f"OHLC file missing required column 'time': {path}")

        for col in ["open", "high", "low", "close"]:
            if col not in df.columns:
                raise RuntimeError(f"OHLC file missing required column '{col}': {path}")

        df = df.copy()

        if pd.api.types.is_integer_dtype(df["time"]) or pd.api.types.is_float_dtype(df["time"]):
            df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True, errors="coerce")
        else:
            df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")

        df["open"] = pd.to_numeric(df["open"], errors="coerce")
        df["high"] = pd.to_numeric(df["high"], errors="coerce")
        df["low"] = pd.to_numeric(df["low"], errors="coerce")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")

        if "spread" not in df.columns:
            df["spread"] = pd.NA
        df["spread"] = pd.to_numeric(df["spread"], errors="coerce")

        df = df.dropna(subset=["time"]).sort_values("time").drop_duplicates(subset=["time"], keep="last")
        df = df.reset_index(drop=True)

        self._cache[symbol] = df
        return self._cache[symbol]


# ============================================================
# ENRICHMENT CORE
# ============================================================

@dataclass
class TradePathMetrics:
    entry_bar_time_utc: Optional[str]
    exit_bar_time_utc: Optional[str]
    bars_in_trade_m15: Optional[int]
    first_bar_open: Optional[float]
    last_bar_close: Optional[float]
    highest_high_in_trade: Optional[float]
    lowest_low_in_trade: Optional[float]
    mfe_price: Optional[float]
    mae_price: Optional[float]
    raw_mfe_abs: Optional[float]
    raw_mae_abs: Optional[float]
    mfe_abs: Optional[float]
    mae_abs: Optional[float]
    mfe_vs_entry_pct: Optional[float]
    mae_vs_entry_pct: Optional[float]
    time_to_mfe_min: Optional[float]
    time_to_mae_min: Optional[float]
    realized_abs_move: Optional[float]
    profit_capture_ratio: Optional[float]
    bars_positive_close_count: Optional[int]
    bars_negative_close_count: Optional[int]
    ohlc_history_start_utc: Optional[str]
    ohlc_history_end_utc: Optional[str]
    path_consistency_flag: Optional[str]
    path_consistency_note: Optional[str]
    path_data_status: str
    path_data_reason: Optional[str]


def build_empty_metrics(reason: str) -> TradePathMetrics:
    return TradePathMetrics(
        entry_bar_time_utc=None,
        exit_bar_time_utc=None,
        bars_in_trade_m15=None,
        first_bar_open=None,
        last_bar_close=None,
        highest_high_in_trade=None,
        lowest_low_in_trade=None,
        mfe_price=None,
        mae_price=None,
        raw_mfe_abs=None,
        raw_mae_abs=None,
        mfe_abs=None,
        mae_abs=None,
        mfe_vs_entry_pct=None,
        mae_vs_entry_pct=None,
        time_to_mfe_min=None,
        time_to_mae_min=None,
        realized_abs_move=None,
        profit_capture_ratio=None,
        bars_positive_close_count=None,
        bars_negative_close_count=None,
        ohlc_history_start_utc=None,
        ohlc_history_end_utc=None,
        path_consistency_flag=None,
        path_consistency_note=None,
        path_data_status="missing",
        path_data_reason=reason,
    )


def compute_trade_path_metrics(
    trade_row: pd.Series,
    ohlc_df: pd.DataFrame,
) -> TradePathMetrics:
    symbol = safe_text(trade_row.get("symbol"))
    direction = safe_upper(trade_row.get("direction"))

    open_time = pd.to_datetime(trade_row.get("open_time_utc"), utc=True, errors="coerce")
    close_time = pd.to_datetime(trade_row.get("close_time_utc"), utc=True, errors="coerce")

    entry_price = pd.to_numeric(pd.Series([trade_row.get("entry_price")]), errors="coerce").iloc[0]
    exit_price = pd.to_numeric(pd.Series([trade_row.get("exit_price")]), errors="coerce").iloc[0]

    if pd.isna(open_time):
        return build_empty_metrics("open_time_invalid")

    if pd.isna(close_time):
        close_time = open_time

    if pd.isna(entry_price):
        return build_empty_metrics("entry_price_invalid")

    if ohlc_df is None or ohlc_df.empty:
        return build_empty_metrics(f"ohlc_missing_for_symbol:{symbol}")

    if direction not in {"BUY", "SELL"}:
        return build_empty_metrics(f"unsupported_direction:{direction}")

    ohlc_start = pd.to_datetime(ohlc_df["time"].min(), utc=True, errors="coerce")
    ohlc_end = pd.to_datetime(ohlc_df["time"].max(), utc=True, errors="coerce")

    ohlc_start_str = None if pd.isna(ohlc_start) else str(ohlc_start)
    ohlc_end_str = None if pd.isna(ohlc_end) else str(ohlc_end)

    if pd.notna(ohlc_start) and open_time < ohlc_start:
        m = build_empty_metrics("before_ohlc_history_start")
        m.ohlc_history_start_utc = ohlc_start_str
        m.ohlc_history_end_utc = ohlc_end_str
        m.path_data_status = "unavailable"
        return m

    if pd.notna(ohlc_end) and open_time > ohlc_end:
        m = build_empty_metrics("after_ohlc_history_end")
        m.ohlc_history_start_utc = ohlc_start_str
        m.ohlc_history_end_utc = ohlc_end_str
        m.path_data_status = "unavailable"
        return m

    entry_bar = floor_to_m15(open_time)
    exit_bar = ceil_to_m15(close_time)

    window = ohlc_df[(ohlc_df["time"] >= entry_bar) & (ohlc_df["time"] <= exit_bar)].copy()
    if window.empty:
        m = build_empty_metrics(
            f"no_ohlc_bars_in_trade_window|trade_open={open_time}|trade_close={close_time}"
        )
        m.ohlc_history_start_utc = ohlc_start_str
        m.ohlc_history_end_utc = ohlc_end_str
        return m

    window = window.sort_values("time").reset_index(drop=True)

    highest_high = pd.to_numeric(window["high"], errors="coerce").max()
    lowest_low = pd.to_numeric(window["low"], errors="coerce").min()
    first_bar_open = pd.to_numeric(pd.Series([window.iloc[0]["open"]]), errors="coerce").iloc[0]
    last_bar_close = pd.to_numeric(pd.Series([window.iloc[-1]["close"]]), errors="coerce").iloc[0]

    if direction == "BUY":
        raw_favorable_series = window["high"] - entry_price
        raw_adverse_series = entry_price - window["low"]
        realized_abs_move = (exit_price - entry_price) if not pd.isna(exit_price) else None
        mfe_price = highest_high
        mae_price = lowest_low
        close_path_move = window["close"] - entry_price
    else:
        raw_favorable_series = entry_price - window["low"]
        raw_adverse_series = window["high"] - entry_price
        realized_abs_move = (entry_price - exit_price) if not pd.isna(exit_price) else None
        mfe_price = lowest_low
        mae_price = highest_high
        close_path_move = entry_price - window["close"]

    raw_favorable_series = pd.to_numeric(raw_favorable_series, errors="coerce")
    raw_adverse_series = pd.to_numeric(raw_adverse_series, errors="coerce")
    close_path_move = pd.to_numeric(close_path_move, errors="coerce")

    raw_mfe_abs = raw_favorable_series.max()
    raw_mae_abs = raw_adverse_series.max()

    mfe_idx = raw_favorable_series.idxmax() if raw_favorable_series.notna().any() else None
    mae_idx = raw_adverse_series.idxmax() if raw_adverse_series.notna().any() else None

    mfe_time = window.loc[mfe_idx, "time"] if mfe_idx is not None and mfe_idx in window.index else pd.NaT
    mae_time = window.loc[mae_idx, "time"] if mae_idx is not None and mae_idx in window.index else pd.NaT

    time_to_mfe_min = None
    if pd.notna(mfe_time):
        time_to_mfe_min = (mfe_time - open_time).total_seconds() / 60.0

    time_to_mae_min = None
    if pd.notna(mae_time):
        time_to_mae_min = (mae_time - open_time).total_seconds() / 60.0

    mfe_abs = raw_mfe_abs
    mae_abs = raw_mae_abs
    path_consistency_flag = "ok"
    path_consistency_note = None

    if realized_abs_move is not None and not pd.isna(realized_abs_move):
        if realized_abs_move > 0:
            realized_pos = float(realized_abs_move)
            if pd.isna(mfe_abs):
                mfe_abs = realized_pos
                path_consistency_flag = "adjusted"
                path_consistency_note = "raw_mfe_missing_adjusted_to_realized_profit"
            elif float(mfe_abs) < realized_pos:
                mfe_abs = realized_pos
                path_consistency_flag = "adjusted"
                path_consistency_note = "raw_mfe_below_realized_profit_adjusted"
        elif realized_abs_move < 0:
            realized_loss = abs(float(realized_abs_move))
            if pd.isna(mae_abs):
                mae_abs = realized_loss
                path_consistency_flag = "adjusted"
                path_consistency_note = "raw_mae_missing_adjusted_to_realized_loss"
            elif float(mae_abs) < realized_loss:
                mae_abs = realized_loss
                path_consistency_flag = "adjusted"
                path_consistency_note = "raw_mae_below_realized_loss_adjusted"

    mfe_vs_entry_pct = None
    mae_vs_entry_pct = None
    if entry_price and not pd.isna(entry_price):
        mfe_vs_entry_pct = float(mfe_abs / entry_price * 100.0) if mfe_abs is not None and not pd.isna(mfe_abs) else None
        mae_vs_entry_pct = float(mae_abs / entry_price * 100.0) if mae_abs is not None and not pd.isna(mae_abs) else None

    profit_capture_ratio = None
    if (
        realized_abs_move is not None
        and mfe_abs is not None
        and not pd.isna(mfe_abs)
        and float(mfe_abs) > 0
    ):
        profit_capture_ratio = float(realized_abs_move) / float(mfe_abs)

    bars_positive_close_count = int((close_path_move > 0).sum())
    bars_negative_close_count = int((close_path_move < 0).sum())

    return TradePathMetrics(
        entry_bar_time_utc=str(entry_bar),
        exit_bar_time_utc=str(exit_bar),
        bars_in_trade_m15=int(len(window)),
        first_bar_open=float(first_bar_open) if not pd.isna(first_bar_open) else None,
        last_bar_close=float(last_bar_close) if not pd.isna(last_bar_close) else None,
        highest_high_in_trade=float(highest_high) if not pd.isna(highest_high) else None,
        lowest_low_in_trade=float(lowest_low) if not pd.isna(lowest_low) else None,
        mfe_price=float(mfe_price) if not pd.isna(mfe_price) else None,
        mae_price=float(mae_price) if not pd.isna(mae_price) else None,
        raw_mfe_abs=float(raw_mfe_abs) if raw_mfe_abs is not None and not pd.isna(raw_mfe_abs) else None,
        raw_mae_abs=float(raw_mae_abs) if raw_mae_abs is not None and not pd.isna(raw_mae_abs) else None,
        mfe_abs=float(mfe_abs) if mfe_abs is not None and not pd.isna(mfe_abs) else None,
        mae_abs=float(mae_abs) if mae_abs is not None and not pd.isna(mae_abs) else None,
        mfe_vs_entry_pct=mfe_vs_entry_pct,
        mae_vs_entry_pct=mae_vs_entry_pct,
        time_to_mfe_min=time_to_mfe_min,
        time_to_mae_min=time_to_mae_min,
        realized_abs_move=float(realized_abs_move) if realized_abs_move is not None and not pd.isna(realized_abs_move) else None,
        profit_capture_ratio=profit_capture_ratio,
        bars_positive_close_count=bars_positive_close_count,
        bars_negative_close_count=bars_negative_close_count,
        ohlc_history_start_utc=ohlc_start_str,
        ohlc_history_end_utc=ohlc_end_str,
        path_consistency_flag=path_consistency_flag,
        path_consistency_note=path_consistency_note,
        path_data_status="ok",
        path_data_reason=None,
    )


# ============================================================
# DATAFRAME ENRICHMENT
# ============================================================

def attach_profile_columns(df: pd.DataFrame, profiles: Dict[str, dict]) -> pd.DataFrame:
    out = df.copy()

    for _, out_col in PROFILE_FIELDS_TO_ATTACH:
        if out_col not in out.columns:
            out[out_col] = pd.NA

    if "profile_match_status" not in out.columns:
        out["profile_match_status"] = pd.NA

    for idx in out.index:
        strategy_id = safe_text(out.at[idx, "strategy_id"]) if "strategy_id" in out.columns else ""
        profile = profiles.get(strategy_id)

        if profile is None:
            out.at[idx, "profile_match_status"] = "not_found"
            continue

        for src_path, out_col in PROFILE_FIELDS_TO_ATTACH:
            out.at[idx, out_col] = extract_nested(profile, src_path)

        out.at[idx, "profile_match_status"] = "matched"

    return out


def enrich_trade_dataframe(
    trades_df: pd.DataFrame,
    ohlc_cache: OHLCCache,
    profiles: Dict[str, dict],
) -> pd.DataFrame:
    out = trades_df.copy()

    for col in TRADE_REQUIRED_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA

    out["open_time_utc"] = safe_to_datetime_utc(out["open_time_utc"])
    out["close_time_utc"] = safe_to_datetime_utc(out["close_time_utc"])
    out["entry_price"] = to_float(out["entry_price"])
    out["exit_price"] = to_float(out["exit_price"])
    out["profit_sum"] = to_float(out["profit_sum"])
    out["net_sum"] = to_float(out["net_sum"])

    out = attach_profile_columns(out, profiles)

    metric_rows: List[dict] = []

    for _, row in out.iterrows():
        symbol = safe_text(row.get("symbol"))
        ohlc_df = ohlc_cache.get(symbol)
        metrics = compute_trade_path_metrics(row, ohlc_df)
        metric_rows.append(metrics.__dict__)

    metrics_df = pd.DataFrame(metric_rows)
    result = pd.concat([out.reset_index(drop=True), metrics_df.reset_index(drop=True)], axis=1)

    result["mfe_to_profile_tp_ratio"] = pd.NA
    result["mae_to_profile_sl_ratio"] = pd.NA

    for idx in result.index:
        mfe_abs = pd.to_numeric(pd.Series([result.at[idx, "mfe_abs"]]), errors="coerce").iloc[0]
        mae_abs = pd.to_numeric(pd.Series([result.at[idx, "mae_abs"]]), errors="coerce").iloc[0]
        profile_sl = pd.to_numeric(pd.Series([result.at[idx, "profile_sl_pips"]]), errors="coerce").iloc[0]
        profile_tp = pd.to_numeric(pd.Series([result.at[idx, "profile_tp_pips"]]), errors="coerce").iloc[0]

        if not pd.isna(profile_sl) and profile_sl != 0 and not pd.isna(mae_abs):
            result.at[idx, "mae_to_profile_sl_ratio"] = float(mae_abs) / float(profile_sl)

        if not pd.isna(profile_tp) and profile_tp != 0 and not pd.isna(mfe_abs):
            result.at[idx, "mfe_to_profile_tp_ratio"] = float(mfe_abs) / float(profile_tp)

    return result


# ============================================================
# MAIN
# ============================================================

def save_failed_report(failed: List[Tuple[str, str]]) -> None:
    ensure_dir(OUTPUT_ROOT)
    df = pd.DataFrame(failed, columns=["file", "error"])
    df.to_csv(FAILED_REPORT_PATH, index=False)


def main() -> None:
    print("=" * 100)
    print("TRADE PATH ENRICHER M15")
    print("=" * 100)
    print(f"FTMO_ROOT    : {FTMO_ROOT}")
    print(f"TRADES_ROOT  : {TRADES_ROOT}")
    print(f"OHLC_ROOT    : {OHLC_ROOT}")
    print(f"PROFILE_ROOT : {PROFILE_ROOT}")
    print(f"OUTPUT_ROOT  : {OUTPUT_ROOT}")
    print("=" * 100)

    if not TRADES_ROOT.exists():
        raise RuntimeError(f"Trades root not found: {TRADES_ROOT}")

    if not OHLC_ROOT.exists():
        raise RuntimeError(f"OHLC root not found: {OHLC_ROOT}")

    ensure_output_dirs()

    profiles = load_strategy_profiles(PROFILE_ROOT)
    print(f"Strategy profiles loaded: {len(profiles)}")

    files = sorted(TRADES_ROOT.rglob("*.csv"))
    print(f"Trade CSV files found: {len(files)}")

    ohlc_cache = OHLCCache()
    failed_files: List[Tuple[str, str]] = []

    processed_files = 0
    total_input_rows = 0
    total_output_rows = 0

    for file_path in files:
        try:
            rel_parts = file_path.resolve().relative_to(TRADES_ROOT.resolve()).parts
            account_type = detect_account_type_from_relpath(rel_parts)
            sample_type = detect_sample_type_from_relpath(rel_parts)

            if account_type not in VALID_ACCOUNT_TYPES:
                raise RuntimeError(f"Account type not recognized from path: {file_path}")
            if sample_type not in VALID_SAMPLE_TYPES:
                raise RuntimeError(f"Sample type not recognized from path: {file_path}")

            df = read_csv_safe(file_path)
            total_input_rows += len(df)

            enriched = enrich_trade_dataframe(df, ohlc_cache, profiles)
            total_output_rows += len(enriched)

            output_file = build_output_path(file_path)
            enriched.to_csv(output_file, index=False)

            processed_files += 1
            print(
                f"[OK] {account_type}/{sample_type} | "
                f"{file_path.name} | rows={len(enriched)} -> {output_file.name}"
            )

        except Exception as e:
            failed_files.append((str(file_path), str(e)))
            print(f"[WARN] skipped: {file_path} | {e}")

    save_failed_report(failed_files)

    print("-" * 100)
    print(f"Processed files : {processed_files}")
    print(f"Input rows      : {total_input_rows}")
    print(f"Output rows     : {total_output_rows}")
    print(f"Failed files    : {len(failed_files)}")
    print(f"Failed report   : {FAILED_REPORT_PATH}")
    print("=" * 100)


if __name__ == "__main__":
    main()