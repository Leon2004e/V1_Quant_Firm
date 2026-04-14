# -*- coding: utf-8 -*-
"""
Analyse_Center/Performance/feuture.py

Zweck:
- Lädt rekursiv angereicherte Backtest-Trade-Dateien
- Baut pro Strategie reine Performance-Features
- Speichert die Ergebnisse in:
    FTMO/Data_Center/Data/Analysis/Performance/

Wichtig:
- Dieser Layer enthält nur Performance
- Keine Robustness-Features
- Keine Risk-/Drawdown-Features
- Keine Hybrid-Metriken wie Return/MaxDD

Input:
- FTMO/Data_Center/Data/Trades/Featured/Backtest/IS_OOS_Enriched/**/*.csv
  alternativ:
- FTMO/Data_Center/Data/Trades/Feutered/Backtest/IS_OOS_Enriched/**/*.csv

Output:
- FTMO/Data_Center/Data/Analysis/Performance/
    - performance_features.csv
    - performance_features.json
    - strategy_json/<strategy_name>__performance.json
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


# ============================================================
# ROOT / PATHS
# ============================================================

def find_project_root(start: Path) -> Path:
    """
    Erwartet als Projekt-Root den FTMO-Ordner,
    in dem gleichzeitig 'Data_Center' und 'Dashboards' existieren.
    """
    cur = start.resolve()

    for p in [cur] + list(cur.parents):
        if (p / "Data_Center").exists() and (p / "Dashboards").exists():
            return p

    raise RuntimeError(
        f"Projekt-Root nicht gefunden. Erwartet Root mit 'Data_Center' und 'Dashboards'. Start={start}"
    )


SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = find_project_root(SCRIPT_PATH)

INPUT_ROOT = (
    PROJECT_ROOT
    / "Data_Center"
    / "Data"
    / "Trades"
    / "Featured"
    / "Backtest"
    / "IS_OOS_Enriched"
)

if not INPUT_ROOT.exists():
    alt = (
        PROJECT_ROOT
        / "Data_Center"
        / "Data"
        / "Trades"
        / "Feutered"
        / "Backtest"
        / "IS_OOS_Enriched"
    )
    if alt.exists():
        INPUT_ROOT = alt

OUTPUT_ROOT = (
    PROJECT_ROOT
    / "Data_Center"
    / "Data"
    / "Analysis"
    / "Performance"
)

STRATEGY_JSON_OUTPUT_ROOT = OUTPUT_ROOT / "strategy_json"

SUMMARY_CSV_PATH = OUTPUT_ROOT / "performance_features.csv"
SUMMARY_JSON_PATH = OUTPUT_ROOT / "performance_features.json"


# ============================================================
# CONFIG
# ============================================================

REQUIRED_COLUMNS_MINIMAL = [
    "open_time_utc",
    "close_time_utc",
    "net_sum",
]


# ============================================================
# HELPERS
# ============================================================

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def safe_text(x: object) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def sanitize_name(name: str) -> str:
    return re.sub(r'[<>:"/\\\\|?*]', "_", str(name)).strip()


def safe_to_datetime_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def json_safe(value):
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        if pd.isna(value) or math.isinf(float(value)):
            return None
        return float(value)
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def extract_clean_strategy_name(raw_name: str) -> str:
    """
    Beispiel:
    listOfTrades_AUDJPY_1_3.14.146_BUY_M15_IS_2021-07-02_to_2024-07-01
    -> AUDJPY_1_3.14.146_BUY_M15
    """
    name = safe_text(raw_name)
    if not name:
        return ""

    name = re.sub(r"^listOfTrades_", "", name, flags=re.IGNORECASE)
    name = re.split(r"_(?:IS|OOS)_", name, maxsplit=1, flags=re.IGNORECASE)[0]
    return name.strip("_ ").strip()


def infer_strategy_name_from_source_file(path_text: str) -> str:
    if not path_text:
        return ""
    try:
        stem = Path(path_text).stem
        return extract_clean_strategy_name(stem)
    except Exception:
        return ""


def compute_profit_factor(pnl: pd.Series) -> Optional[float]:
    s = pd.to_numeric(pnl, errors="coerce").dropna()
    if s.empty:
        return None

    gross_profit = float(s[s > 0].sum())
    gross_loss = float(s[s < 0].sum())

    if abs(gross_loss) < 1e-12:
        return None

    return float(gross_profit / abs(gross_loss))


def compute_expectancy(pnl: pd.Series) -> Optional[float]:
    s = pd.to_numeric(pnl, errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.mean())


def compute_win_rate(pnl: pd.Series) -> Optional[float]:
    s = pd.to_numeric(pnl, errors="coerce").dropna()
    if s.empty:
        return None
    return float((s > 0).mean())


def compute_avg_win(pnl: pd.Series) -> Optional[float]:
    s = pd.to_numeric(pnl, errors="coerce").dropna()
    s = s[s > 0]
    if s.empty:
        return None
    return float(s.mean())


def compute_avg_loss(pnl: pd.Series) -> Optional[float]:
    s = pd.to_numeric(pnl, errors="coerce").dropna()
    s = s[s < 0]
    if s.empty:
        return None
    return float(s.mean())


def compute_payoff_ratio(pnl: pd.Series) -> Optional[float]:
    avg_win = compute_avg_win(pnl)
    avg_loss = compute_avg_loss(pnl)

    if avg_win is None or avg_loss is None:
        return None
    if abs(avg_loss) < 1e-12:
        return None

    return float(avg_win / abs(avg_loss))


def compute_active_months(close_time_utc: pd.Series) -> int:
    s = pd.to_datetime(close_time_utc, utc=True, errors="coerce").dropna()
    if s.empty:
        return 0
    return int(s.dt.to_period("M").nunique())


def compute_trades_per_month(trade_count: int, active_months: int) -> Optional[float]:
    if active_months <= 0:
        return None
    return float(trade_count / active_months)


# ============================================================
# DATA PREP
# ============================================================

def build_trade_series(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for col in REQUIRED_COLUMNS_MINIMAL:
        if col not in out.columns:
            raise RuntimeError(f"Pflichtspalte fehlt im Input: {col}")

    out["open_time_utc"] = safe_to_datetime_utc(out["open_time_utc"])
    out["close_time_utc"] = safe_to_datetime_utc(out["close_time_utc"])
    out["net_sum"] = to_float(out["net_sum"])

    out = out.dropna(subset=["close_time_utc", "net_sum"]).copy()

    if "source_file" not in out.columns:
        out["source_file"] = ""

    if "strategy_id" not in out.columns:
        out["strategy_id"] = ""

    source_strategy = out["source_file"].astype(str).apply(infer_strategy_name_from_source_file)
    raw_strategy = out["strategy_id"].astype(str).apply(extract_clean_strategy_name)
    clean_strategy = source_strategy.where(source_strategy.str.len() > 0, raw_strategy)

    out["strategy_id"] = clean_strategy.astype(str).str.strip()
    out = out[out["strategy_id"] != ""].copy()

    if "sample_type" in out.columns:
        out["sample_type"] = out["sample_type"].astype(str).str.upper().str.strip()
    else:
        out["sample_type"] = "ALL"

    out = out.sort_values(["strategy_id", "close_time_utc", "open_time_utc"], ascending=True)
    out = out.reset_index(drop=True)

    return out


def load_all_trades(input_root: Path) -> pd.DataFrame:
    if not input_root.exists():
        raise RuntimeError(f"Input root nicht gefunden: {input_root}")

    files = sorted(input_root.rglob("*.csv"))
    files = [f for f in files if f.name.lower() != "failed_files.csv"]

    if not files:
        raise RuntimeError(f"Keine CSV-Dateien gefunden unter: {input_root}")

    parts: List[pd.DataFrame] = []
    skipped_files: List[str] = []

    for path in files:
        try:
            df = pd.read_csv(path)
            if df.empty:
                skipped_files.append(f"{path} | leer")
                continue

            df["source_file"] = str(path)

            if "strategy_id" not in df.columns:
                df["strategy_id"] = ""

            parts.append(df)

        except Exception as e:
            skipped_files.append(f"{path} | {e}")

    if skipped_files:
        print("-" * 100)
        print("Übersprungene oder leere Dateien:")
        for line in skipped_files[:30]:
            print(f"[WARN] {line}")
        if len(skipped_files) > 30:
            print(f"[WARN] ... weitere {len(skipped_files) - 30} Einträge")
        print("-" * 100)

    if not parts:
        raise RuntimeError(
            "Keine lesbaren CSV-Dateien geladen. Prüfe INPUT_ROOT und Dateiformat.\n"
            f"INPUT_ROOT={input_root}"
        )

    all_trades = pd.concat(parts, ignore_index=True, sort=False)
    return build_trade_series(all_trades)


# ============================================================
# FEATURES
# ============================================================

@dataclass
class PerformanceFeatures:
    strategy_id: str

    trade_count_total: int
    trade_count_is: int
    trade_count_oos: int

    active_months: int
    trades_per_month: Optional[float]

    net_profit_total: Optional[float]
    net_profit_is: Optional[float]
    net_profit_oos: Optional[float]

    win_rate_total: Optional[float]
    win_rate_is: Optional[float]
    win_rate_oos: Optional[float]

    avg_win_total: Optional[float]
    avg_loss_total: Optional[float]
    payoff_ratio_total: Optional[float]

    expectancy_total: Optional[float]
    expectancy_is: Optional[float]
    expectancy_oos: Optional[float]

    profit_factor_total: Optional[float]
    profit_factor_is: Optional[float]
    profit_factor_oos: Optional[float]

    first_trade_time_utc: Optional[str]
    last_trade_time_utc: Optional[str]


def compute_strategy_features(strategy_id: str, trades: pd.DataFrame) -> PerformanceFeatures:
    df = trades.copy().sort_values("close_time_utc").reset_index(drop=True)
    pnl_total = pd.to_numeric(df["net_sum"], errors="coerce").dropna()

    is_df = df[df["sample_type"] == "IS"].copy()
    oos_df = df[df["sample_type"] == "OOS"].copy()

    active_months = compute_active_months(df["close_time_utc"])

    net_profit_total = float(pnl_total.sum()) if not pnl_total.empty else None
    net_profit_is = (
        float(pd.to_numeric(is_df["net_sum"], errors="coerce").dropna().sum())
        if not is_df.empty
        else None
    )
    net_profit_oos = (
        float(pd.to_numeric(oos_df["net_sum"], errors="coerce").dropna().sum())
        if not oos_df.empty
        else None
    )

    win_rate_total = compute_win_rate(df["net_sum"])
    win_rate_is = compute_win_rate(is_df["net_sum"]) if not is_df.empty else None
    win_rate_oos = compute_win_rate(oos_df["net_sum"]) if not oos_df.empty else None

    avg_win_total = compute_avg_win(df["net_sum"])
    avg_loss_total = compute_avg_loss(df["net_sum"])
    payoff_ratio_total = compute_payoff_ratio(df["net_sum"])

    expectancy_total = compute_expectancy(df["net_sum"])
    expectancy_is = compute_expectancy(is_df["net_sum"]) if not is_df.empty else None
    expectancy_oos = compute_expectancy(oos_df["net_sum"]) if not oos_df.empty else None

    profit_factor_total = compute_profit_factor(df["net_sum"])
    profit_factor_is = compute_profit_factor(is_df["net_sum"]) if not is_df.empty else None
    profit_factor_oos = compute_profit_factor(oos_df["net_sum"]) if not oos_df.empty else None

    first_trade = df["close_time_utc"].min() if not df.empty else None
    last_trade = df["close_time_utc"].max() if not df.empty else None

    return PerformanceFeatures(
        strategy_id=strategy_id,

        trade_count_total=int(len(df)),
        trade_count_is=int(len(is_df)),
        trade_count_oos=int(len(oos_df)),

        active_months=int(active_months),
        trades_per_month=compute_trades_per_month(len(df), active_months),

        net_profit_total=net_profit_total,
        net_profit_is=net_profit_is,
        net_profit_oos=net_profit_oos,

        win_rate_total=win_rate_total,
        win_rate_is=win_rate_is,
        win_rate_oos=win_rate_oos,

        avg_win_total=avg_win_total,
        avg_loss_total=avg_loss_total,
        payoff_ratio_total=payoff_ratio_total,

        expectancy_total=expectancy_total,
        expectancy_is=expectancy_is,
        expectancy_oos=expectancy_oos,

        profit_factor_total=profit_factor_total,
        profit_factor_is=profit_factor_is,
        profit_factor_oos=profit_factor_oos,

        first_trade_time_utc=first_trade.isoformat() if pd.notna(first_trade) else None,
        last_trade_time_utc=last_trade.isoformat() if pd.notna(last_trade) else None,
    )


# ============================================================
# WRITER
# ============================================================

def save_strategy_json(strategy_name: str, payload: Dict[str, object]) -> None:
    ensure_dir(STRATEGY_JSON_OUTPUT_ROOT)
    path = STRATEGY_JSON_OUTPUT_ROOT / f"{sanitize_name(strategy_name)}__performance.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def save_summary(rows: List[Dict[str, object]]) -> None:
    ensure_dir(OUTPUT_ROOT)
    pd.DataFrame(rows).to_csv(SUMMARY_CSV_PATH, index=False)

    with open(SUMMARY_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    print("=" * 100)
    print("PERFORMANCE FEATURE LAYER")
    print("=" * 100)
    print(f"PROJECT_ROOT : {PROJECT_ROOT}")
    print(f"INPUT_ROOT   : {INPUT_ROOT}")
    print(f"OUTPUT_ROOT  : {OUTPUT_ROOT}")
    print("=" * 100)

    ensure_dir(OUTPUT_ROOT)
    ensure_dir(STRATEGY_JSON_OUTPUT_ROOT)

    all_trades = load_all_trades(INPUT_ROOT)

    print(f"Loaded trades       : {len(all_trades)}")
    print(f"Unique strategy_ids : {all_trades['strategy_id'].nunique()}")
    print("-" * 100)

    outputs: List[Dict[str, object]] = []

    for strategy_id, trades in all_trades.groupby("strategy_id", dropna=False):
        strategy_id = safe_text(strategy_id)
        if not strategy_id:
            continue

        features = compute_strategy_features(strategy_id, trades)
        summary_dict = {k: json_safe(v) for k, v in asdict(features).items()}

        payload = {
            "strategy_id": strategy_id,
            "features": summary_dict,
        }

        outputs.append(summary_dict)
        save_strategy_json(strategy_id, payload)

        print(
            f"[OK] strategy_id={strategy_id} | "
            f"trades={summary_dict['trade_count_total']} | "
            f"pf_total={summary_dict['profit_factor_total']} | "
            f"pf_oos={summary_dict['profit_factor_oos']} | "
            f"expectancy_oos={summary_dict['expectancy_oos']}"
        )

    if not outputs:
        raise RuntimeError(
            "Es konnten keine Strategien verarbeitet werden. "
            "Prüfe strategy_id/source_file/sample_type in deinen Input-Dateien."
        )

    save_summary(outputs)

    print("-" * 100)
    print(f"Summary CSV  : {SUMMARY_CSV_PATH}")
    print(f"Summary JSON : {SUMMARY_JSON_PATH}")
    print(f"Strategy JSON: {STRATEGY_JSON_OUTPUT_ROOT}")
    print("=" * 100)


if __name__ == "__main__":
    main()