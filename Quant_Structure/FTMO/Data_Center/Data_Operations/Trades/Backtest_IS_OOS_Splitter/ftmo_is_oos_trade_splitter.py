# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Data_Center/Data_Operations/Trades/Backtest_IS_OOS_Splitter/ftmo_is_oos_trade_splitter.py

Zweck:
- Liest rekursiv alle Backtest Trade CSV Dateien
- Trennt jede einzelne Backtest-Datei automatisch in IS / OOS
- Konvertiert Backtest-Trades in ein live-nahes, account-agnostisches Schema
- Verwendet den vollständigen Dateinamen als Basisnamen
- Ergänzt Ordner- und Dateinamen um den tatsächlichen Zeitraum der Teilmenge
- Trennt Output zusätzlich nach DEMO_ACCOUNT / LIVE_ACCOUNT
- Erstellt Hauptordner für DEMO_ACCOUNT / LIVE_ACCOUNT und darunter IS/OOS

FTMO Regel:
    IS  = bis einschließlich 2024-07-01 23:59:59
    OOS = ab 2024-07-02 00:00:00

WICHTIG:
- account_id ist absichtlich NICHT enthalten
- Backtest ist account-agnostisch
- dieselbe Strategy kann später auf mehreren Accounts laufen

Neue Input-Struktur:
Quant_Structure/FTMO/Data_Center/Data/Trades/Raw/Backtest
    ├── DEMO_ACCOUNT
    └── LIVE_ACCOUNT

Neue Output-Struktur:
Quant_Structure/FTMO/Data_Center/Data/Trades/Feutered/Backtest/IS_OOS
    ├── DEMO_ACCOUNT
    │   ├── IS
    │   └── OOS
    └── LIVE_ACCOUNT
        ├── IS
        └── OOS
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd


# ============================================================
# PROJECT ROOT
# ============================================================

def find_ftmo_root(start: Path) -> Path:
    cur = start.resolve()
    for p in [cur] + list(cur.parents):
        if (p / "Data_Center").exists():
            return p
    raise RuntimeError(
        f"FTMO-Root nicht gefunden. Erwartet Root mit 'Data_Center'. Start={start}"
    )


SCRIPT_PATH = Path(__file__).resolve()
FTMO_ROOT = find_ftmo_root(SCRIPT_PATH)


# ============================================================
# CONFIG
# ============================================================

BACKTEST_ROOT = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Trades"
    / "Raw"
    / "Backtest"
)

OUTPUT_ROOT = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Trades"
    / "Feutered"
    / "Backtest"
    / "IS_OOS"
)

IS_END_DATE = pd.Timestamp("2024-07-01 23:59:59")
IS_END_DATE_UTC = pd.Timestamp("2024-07-01 23:59:59", tz="UTC")

VALID_ACCOUNT_TYPES = {"DEMO_ACCOUNT", "LIVE_ACCOUNT"}

LIVE_LIKE_COLUMNS = [
    "position_id",
    "symbol",
    "direction",
    "open_time_utc",
    "close_time_utc",
    "entry_price",
    "exit_price",
    "price_delta",
    "volume_in",
    "volume_out",
    "profit_sum",
    "swap_sum",
    "commission_sum",
    "net_sum",
    "magic",
    "comment_last",
    "close_ticket",
    "strategy_id",
    "sample_type",
    "source_file",
    "source_path",
    "environment",
    "symbol_folder",
    "account_type",
]

INPUT_NUMERIC_COLUMNS = [
    "Open price",
    "Size",
    "Close price",
    "Profit/Loss",
    "Cummulative P/L",
    "Comm/Swap",
    "P/L in money",
    "Cummulative money P/L",
    "P/L in pips",
    "Cummulative pips P/L",
    "P/L in %",
    "Cummulative % P/L",
]


# ============================================================
# HELPERS
# ============================================================

def sanitize_folder_name(name: str) -> str:
    name = re.sub(r'[<>:"/\\\\|?*]', "_", str(name))
    return name.strip()


def ensure_output_dirs() -> None:
    for account_type in VALID_ACCOUNT_TYPES:
        (OUTPUT_ROOT / account_type / "IS").mkdir(parents=True, exist_ok=True)
        (OUTPUT_ROOT / account_type / "OOS").mkdir(parents=True, exist_ok=True)


def get_output_name_from_file(file_path: Path) -> str:
    return sanitize_folder_name(file_path.stem)


def _safe_string(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip()


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _parse_backtest_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(
        series,
        format="%d.%m.%Y %H:%M:%S",
        errors="coerce",
    )


def _to_utc_string(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    dt = dt.dt.tz_localize("UTC")
    return dt.dt.strftime("%Y-%m-%d %H:%M:%S+00:00").astype("string")


def _utc_string_to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def detect_account_type(file_path: Path, backtest_root: Path) -> str:
    rel_parts = file_path.resolve().relative_to(backtest_root.resolve()).parts
    if not rel_parts:
        return "UNKNOWN"

    candidate = str(rel_parts[0]).strip().upper()
    if candidate in VALID_ACCOUNT_TYPES:
        return candidate

    return "UNKNOWN"


def extract_strategy_id(file_path: Path, comment_series: Optional[pd.Series] = None) -> str:
    filename = file_path.stem

    match = re.search(r"listOfTrades_([0-9.]+)", filename, flags=re.IGNORECASE)
    if match:
        return match.group(1)

    if comment_series is not None and len(comment_series) > 0:
        extracted = comment_series.astype("string").str.extract(
            r"([0-9]+(?:\.[0-9]+)+)",
            expand=False
        )
        first_valid = extracted.dropna()
        if not first_valid.empty:
            return str(first_valid.iloc[0]).strip()

    return "UNKNOWN"


def _extract_tp_sl_comment(comment: str) -> str:
    if not comment:
        return ""
    c = str(comment).strip()
    if c.lower().startswith("tp "):
        return f"[{c}]"
    if c.lower().startswith("sl "):
        return f"[{c}]"
    return c


def _detect_sample_type(open_time: pd.Timestamp) -> Optional[str]:
    if pd.isna(open_time):
        return None
    return "IS" if open_time <= IS_END_DATE else "OOS"


def get_date_range_suffix(df: pd.DataFrame) -> str:
    if df.empty or "open_time_utc" not in df.columns:
        return "unknown_to_unknown"

    dt = _utc_string_to_datetime(df["open_time_utc"]).dropna()

    if dt.empty:
        return "unknown_to_unknown"

    start_date = dt.min().strftime("%Y-%m-%d")
    end_date = dt.max().strftime("%Y-%m-%d")
    return f"{start_date}_to_{end_date}"


def build_dataset_name(
    strategy_output_name: str,
    sample_type: str,
    df: pd.DataFrame,
) -> str:
    date_range_suffix = get_date_range_suffix(df)
    return sanitize_folder_name(f"{strategy_output_name}_{sample_type}_{date_range_suffix}")


# ============================================================
# DATA CLEANING
# ============================================================

def clean_input_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    for col in INPUT_NUMERIC_COLUMNS:
        if col in out.columns:
            out[col] = _to_numeric(out[col])

    if "Open time" not in out.columns:
        raise RuntimeError("column 'Open time' missing")

    out["Open time"] = _parse_backtest_datetime(out["Open time"])

    if "Close time" in out.columns:
        out["Close time"] = _parse_backtest_datetime(out["Close time"])
    else:
        out["Close time"] = pd.NaT

    for col in ["Ticket", "Symbol", "Type", "Comment", "Sample type"]:
        if col in out.columns:
            out[col] = _safe_string(out[col])

    return out


# ============================================================
# BACKTEST -> LIVE-LIKE SCHEMA
# ============================================================

def convert_backtest_to_live_like_schema(df: pd.DataFrame, file_path: Path) -> pd.DataFrame:
    out = df.copy()

    for required in ["Ticket", "Symbol", "Type", "Open time", "Close time", "Open price", "Close price", "Size"]:
        if required not in out.columns:
            out[required] = pd.NA

    if "Comment" not in out.columns:
        out["Comment"] = pd.NA

    if "Profit/Loss" not in out.columns:
        out["Profit/Loss"] = pd.NA

    if "P/L in money" not in out.columns:
        out["P/L in money"] = pd.NA

    if "Comm/Swap" not in out.columns:
        out["Comm/Swap"] = pd.NA

    out["Symbol"] = _safe_string(out["Symbol"]).fillna(file_path.parent.name)
    out["Type"] = _safe_string(out["Type"]).str.upper()
    out["Comment"] = _safe_string(out["Comment"]).fillna("")

    strategy_id = extract_strategy_id(file_path=file_path, comment_series=out["Comment"])
    account_type = detect_account_type(file_path=file_path, backtest_root=BACKTEST_ROOT)

    result = pd.DataFrame(index=out.index)

    result["position_id"] = _safe_string(out["Ticket"])
    result["close_ticket"] = _safe_string(out["Ticket"])
    result["strategy_id"] = strategy_id
    result["environment"] = account_type
    result["symbol_folder"] = file_path.parent.name
    result["source_file"] = file_path.name
    result["source_path"] = str(file_path)
    result["account_type"] = account_type

    result["symbol"] = _safe_string(out["Symbol"]).fillna(result["symbol_folder"])
    result["direction"] = _safe_string(out["Type"]).str.upper()

    result["open_time_utc"] = _to_utc_string(out["Open time"])
    result["close_time_utc"] = _to_utc_string(out["Close time"])

    result["entry_price"] = _to_numeric(out["Open price"])
    result["exit_price"] = _to_numeric(out["Close price"])
    result["price_delta"] = result["exit_price"] - result["entry_price"]

    size = _to_numeric(out["Size"])
    result["volume_in"] = size
    result["volume_out"] = size

    raw_profit = _to_numeric(out["Profit/Loss"])
    money_profit = _to_numeric(out["P/L in money"])
    combined_cost = _to_numeric(out["Comm/Swap"]).fillna(0.0)

    result["profit_sum"] = raw_profit.where(raw_profit.notna(), money_profit)
    result["swap_sum"] = 0.0
    result["commission_sum"] = combined_cost
    result["net_sum"] = result["profit_sum"] + result["swap_sum"] + result["commission_sum"]

    result["magic"] = pd.NA
    result["comment_last"] = out["Comment"].apply(_extract_tp_sl_comment).astype("string")

    # FTMO-Split IMMER ausschließlich über Open time berechnen
    # vorhandene Input-Spalte "Sample type" wird bewusst ignoriert
    result["sample_type"] = out["Open time"].apply(_detect_sample_type).astype("string")

    for col in [
        "position_id",
        "symbol",
        "direction",
        "open_time_utc",
        "close_time_utc",
        "comment_last",
        "close_ticket",
        "strategy_id",
        "sample_type",
        "source_file",
        "source_path",
        "environment",
        "symbol_folder",
        "account_type",
    ]:
        result[col] = result[col].astype("string")

    for col in [
        "entry_price",
        "exit_price",
        "price_delta",
        "volume_in",
        "volume_out",
        "profit_sum",
        "swap_sum",
        "commission_sum",
        "net_sum",
    ]:
        result[col] = _to_numeric(result[col])

    result = result.sort_values(
        by=["open_time_utc", "close_time_utc", "position_id"],
        na_position="last",
    ).reset_index(drop=True)

    result = result[LIVE_LIKE_COLUMNS]

    return result


# ============================================================
# IS / OOS SPLIT
# ============================================================

def split_is_oos(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df.copy(), df.copy()

    open_dt = _utc_string_to_datetime(df["open_time_utc"])
    cutoff = IS_END_DATE_UTC

    is_mask = open_dt <= cutoff
    oos_mask = open_dt > cutoff

    is_df = df[is_mask].copy()
    oos_df = df[oos_mask].copy()

    if not is_df.empty:
        is_df["sample_type"] = "IS"

    if not oos_df.empty:
        oos_df["sample_type"] = "OOS"

    return is_df, oos_df


# ============================================================
# OUTPUT
# ============================================================

def save_split_file(
    output_root: Path,
    account_type: str,
    strategy_output_name: str,
    sample_type: str,
    df: pd.DataFrame,
) -> Path:
    dataset_name = build_dataset_name(
        strategy_output_name=strategy_output_name,
        sample_type=sample_type,
        df=df,
    )

    strategy_dir = output_root / account_type / sample_type / dataset_name
    strategy_dir.mkdir(parents=True, exist_ok=True)

    output_file = strategy_dir / f"{dataset_name}.csv"
    df.to_csv(output_file, index=False)

    return output_file


def save_failed_files_report(failed_files: List[Tuple[str, str]]) -> None:
    failed_path = OUTPUT_ROOT / "failed_files.csv"
    df_failed = pd.DataFrame(failed_files, columns=["file", "error"])
    df_failed.to_csv(failed_path, index=False)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    print("=" * 80)
    print("FTMO BACKTEST CLEANING | IS / OOS TRADE SPLITTER")
    print("=" * 80)
    print(f"FTMO_ROOT    : {FTMO_ROOT}")
    print(f"BACKTEST_ROOT: {BACKTEST_ROOT}")
    print(f"OUTPUT_ROOT  : {OUTPUT_ROOT}")
    print(f"IS_END_DATE  : {IS_END_DATE}")
    print("=" * 80)

    ensure_output_dirs()

    files = sorted(BACKTEST_ROOT.rglob("*.csv"))
    print(f"CSV files found: {len(files)}")

    failed_files: List[Tuple[str, str]] = []
    total_is = 0
    total_oos = 0
    processed_files = 0

    for file_path in files:
        try:
            raw_df = pd.read_csv(file_path)
            raw_df = clean_input_dataframe(raw_df)
            live_like_df = convert_backtest_to_live_like_schema(raw_df, file_path)

            is_df, oos_df = split_is_oos(live_like_df)
            strategy_output_name = get_output_name_from_file(file_path)
            account_type = detect_account_type(file_path=file_path, backtest_root=BACKTEST_ROOT)

            if account_type == "UNKNOWN":
                raise RuntimeError(
                    f"Account-Typ konnte nicht aus Pfad erkannt werden: {file_path}"
                )

            is_output_file = None
            oos_output_file = None

            if not is_df.empty:
                is_output_file = save_split_file(
                    output_root=OUTPUT_ROOT,
                    account_type=account_type,
                    strategy_output_name=strategy_output_name,
                    sample_type="IS",
                    df=is_df,
                )

            if not oos_df.empty:
                oos_output_file = save_split_file(
                    output_root=OUTPUT_ROOT,
                    account_type=account_type,
                    strategy_output_name=strategy_output_name,
                    sample_type="OOS",
                    df=oos_df,
                )

            processed_files += 1
            total_is += len(is_df)
            total_oos += len(oos_df)

            is_range = get_date_range_suffix(is_df) if not is_df.empty else "empty"
            oos_range = get_date_range_suffix(oos_df) if not oos_df.empty else "empty"

            print(
                f"{account_type} | {file_path.name} | "
                f"IS={len(is_df)} [{is_range}] | "
                f"OOS={len(oos_df)} [{oos_range}]"
            )

            if is_output_file is not None:
                print(f"  saved IS : {is_output_file}")
            if oos_output_file is not None:
                print(f"  saved OOS: {oos_output_file}")

        except Exception as e:
            failed_files.append((str(file_path), str(e)))
            print(f"[WARN] file skipped: {file_path} | {e}")

    save_failed_files_report(failed_files)

    print("-" * 80)
    print(f"Processed files : {processed_files}")
    print(f"Total IS trades : {total_is}")
    print(f"Total OOS trades: {total_oos}")
    print(f"Failed files    : {len(failed_files)}")
    print(f"Output saved    : {OUTPUT_ROOT}")
    print("=" * 80)


if __name__ == "__main__":
    main()