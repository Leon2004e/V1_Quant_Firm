# -*- coding: utf-8 -*-
"""
Analyse_Center/Strategy_Layer.py

Zweck:
- Liest rekursiv alle angereicherten Backtest-Trade-Dateien
- Aggregiert Trades auf Strategy-Layer-Ebene
- Berechnet tiefe Einzelstrategie-Metriken
- Verwendet überall einen einheitlichen Strategy-Namen:
    Beispiel:
    listOfTrades_AUDJPY_1_3.14.146_BUY_M15_IS_2021-07-02_to_2024-07-01
    -> AUDJPY_1_3.14.146_BUY_M15
- Exportiert:
    * globale Summary CSV + JSON
    * pro Strategy eigene Summary CSV + JSON
    * Equity Curves pro Strategy (CSV)
    * visuelle Charts pro Strategy (PNG)

Input:
- Data_Center/Data/Trades/Featured/Backtest/IS_OOS_Enriched/**/*.csv
  alternativ:
- Data_Center/Data/Trades/Feutered/Backtest/IS_OOS_Enriched/**/*.csv

Output:
- Data_Center/Data/Analysis/Strategy_Layer/
    - strategy_layer_summary.csv
    - strategy_layer_summary.json
    - equity_curves/<strategy_name>__equity_curve.csv
    - strategy_summaries/<strategy_name>__summary.csv
    - strategy_json/<strategy_name>__summary.json
    - visuals/
        - equity_curves/<strategy_name>__equity.png
        - drawdowns/<strategy_name>__drawdown.png
        - pnl_distributions/<strategy_name>__pnl_distribution.png
        - mfe_mae/<strategy_name>__mfe_mae.png
        - monthly_pnl/<strategy_name>__monthly_pnl.png

Hinweis:
- sample_type (IS/OOS) wird nur segmentiert ausgewiesen
- keine Robustness-Bewertung in diesem File
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# ============================================================
# ROOT / PATHS
# ============================================================

def find_project_root(start: Path) -> Path:
    cur = start.resolve()
    for p in [cur] + list(cur.parents):
        if (p / "Data_Center").exists() and (p / "Dashboards").exists():
            return p
    raise RuntimeError(
        f"Projekt-Root nicht gefunden. Erwartet Root mit "
        f"'Data_Center' und 'Dashboards'. Start={start}"
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
    / "Strategy_Layer"
)

EQUITY_OUTPUT_ROOT = OUTPUT_ROOT / "equity_curves"
STRATEGY_SUMMARY_OUTPUT_ROOT = OUTPUT_ROOT / "strategy_summaries"
STRATEGY_JSON_OUTPUT_ROOT = OUTPUT_ROOT / "strategy_json"

VISUALS_ROOT = OUTPUT_ROOT / "visuals"
VISUAL_EQUITY_ROOT = VISUALS_ROOT / "equity_curves"
VISUAL_DRAWDOWN_ROOT = VISUALS_ROOT / "drawdowns"
VISUAL_PNL_DIST_ROOT = VISUALS_ROOT / "pnl_distributions"
VISUAL_MFE_MAE_ROOT = VISUALS_ROOT / "mfe_mae"
VISUAL_MONTHLY_PNL_ROOT = VISUALS_ROOT / "monthly_pnl"

SUMMARY_CSV_PATH = OUTPUT_ROOT / "strategy_layer_summary.csv"
SUMMARY_JSON_PATH = OUTPUT_ROOT / "strategy_layer_summary.json"


# ============================================================
# CONFIG
# ============================================================

REQUIRED_COLUMNS_MINIMAL = [
    "symbol",
    "open_time_utc",
    "close_time_utc",
    "entry_price",
    "exit_price",
    "net_sum",
]

DEFAULT_START_CAPITAL = 100000.0


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


def extract_clean_strategy_name(raw_name: str) -> str:
    """
    Extrahiert den sauberen Strategy-Core aus z.B.:

    listOfTrades_AUDJPY_1_3.14.146_BUY_M15_IS_2021-07-02_to_2024-07-01
    -> AUDJPY_1_3.14.146_BUY_M15

    listOfTrades_AUDJPY_1_3.14.146_BUY_M15_OOS_2024-07-02_to_2025-07-01
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


def safe_mean(s: pd.Series) -> Optional[float]:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.mean())


def safe_median(s: pd.Series) -> Optional[float]:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.median())


def safe_std(s: pd.Series) -> Optional[float]:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if len(s) < 2:
        return None
    return float(s.std(ddof=1))


def safe_quantile(s: pd.Series, q: float) -> Optional[float]:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.quantile(q))


def safe_skew(s: pd.Series) -> Optional[float]:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if len(s) < 3:
        return None
    return float(s.skew())


def safe_kurt(s: pd.Series) -> Optional[float]:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if len(s) < 4:
        return None
    return float(s.kurt())


def compute_streaks(values: pd.Series) -> Dict[str, Optional[float]]:
    x = pd.to_numeric(values, errors="coerce").fillna(0.0).tolist()

    win_streaks: List[int] = []
    loss_streaks: List[int] = []

    current_sign = 0
    current_len = 0

    for v in x:
        sign = 1 if v > 0 else (-1 if v < 0 else 0)

        if sign == 0:
            if current_sign == 1 and current_len > 0:
                win_streaks.append(current_len)
            elif current_sign == -1 and current_len > 0:
                loss_streaks.append(current_len)
            current_sign = 0
            current_len = 0
            continue

        if sign == current_sign:
            current_len += 1
        else:
            if current_sign == 1 and current_len > 0:
                win_streaks.append(current_len)
            elif current_sign == -1 and current_len > 0:
                loss_streaks.append(current_len)

            current_sign = sign
            current_len = 1

    if current_sign == 1 and current_len > 0:
        win_streaks.append(current_len)
    elif current_sign == -1 and current_len > 0:
        loss_streaks.append(current_len)

    return {
        "max_win_streak": int(max(win_streaks)) if win_streaks else 0,
        "max_loss_streak": int(max(loss_streaks)) if loss_streaks else 0,
        "avg_win_streak": float(np.mean(win_streaks)) if win_streaks else None,
        "avg_loss_streak": float(np.mean(loss_streaks)) if loss_streaks else None,
    }


def build_trade_series(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["open_time_utc"] = safe_to_datetime_utc(out["open_time_utc"])
    out["close_time_utc"] = safe_to_datetime_utc(out["close_time_utc"])
    out["entry_price"] = to_float(out["entry_price"])
    out["exit_price"] = to_float(out["exit_price"])
    out["net_sum"] = to_float(out["net_sum"])

    for col in [
        "profit_sum",
        "mfe_abs",
        "mae_abs",
        "raw_mfe_abs",
        "raw_mae_abs",
        "profit_capture_ratio",
        "time_to_mfe_min",
        "time_to_mae_min",
        "bars_in_trade_m15",
    ]:
        if col in out.columns:
            out[col] = to_float(out[col])

    out = out.dropna(subset=["close_time_utc", "net_sum"]).copy()

    if "source_file" not in out.columns:
        out["source_file"] = ""

    if "strategy_id" not in out.columns:
        out["strategy_id"] = ""

    # ========================================================
    # WICHTIGER FIX:
    # Priorität jetzt: source_file -> strategy_id
    # Also Name IMMER zuerst aus dem Dateinamen ableiten.
    # ========================================================
    source_strategy = out["source_file"].astype(str).apply(infer_strategy_name_from_source_file)

    raw_strategy = out["strategy_id"].astype(str)
    raw_strategy_clean = raw_strategy.apply(extract_clean_strategy_name)

    clean_strategy = source_strategy.where(source_strategy.str.len() > 0, raw_strategy_clean)

    out["strategy_id"] = clean_strategy.astype(str).str.strip()
    out = out[out["strategy_id"] != ""].copy()

    out["holding_time_sec"] = (
        (out["close_time_utc"] - out["open_time_utc"]).dt.total_seconds()
    )
    out.loc[out["holding_time_sec"] < 0, "holding_time_sec"] = np.nan

    out = out.sort_values(["strategy_id", "close_time_utc", "open_time_utc"], ascending=True)
    out = out.reset_index(drop=True)

    return out


def build_equity_curve(
    trades: pd.DataFrame,
    start_capital: float = DEFAULT_START_CAPITAL
) -> pd.DataFrame:
    eq = trades.copy().sort_values("close_time_utc").reset_index(drop=True)

    eq["trade_index"] = np.arange(1, len(eq) + 1)
    eq["cum_net_sum"] = eq["net_sum"].cumsum()
    eq["equity"] = float(start_capital) + eq["cum_net_sum"]
    eq["equity_peak"] = eq["equity"].cummax()
    eq["drawdown_abs"] = eq["equity"] - eq["equity_peak"]
    eq["drawdown_pct"] = np.where(
        eq["equity_peak"] != 0,
        eq["drawdown_abs"] / eq["equity_peak"],
        np.nan,
    )

    eq["recovered_to_new_peak"] = eq["equity"] >= eq["equity_peak"]
    return eq


def compute_drawdown_durations(equity_df: pd.DataFrame) -> Dict[str, Optional[float]]:
    if equity_df.empty:
        return {
            "drawdown_count": 0,
            "max_drawdown_duration_trades": None,
            "avg_drawdown_duration_trades": None,
            "time_under_water_pct": None,
        }

    underwater = equity_df["drawdown_abs"] < 0
    durations = []
    current = 0

    for flag in underwater.tolist():
        if flag:
            current += 1
        else:
            if current > 0:
                durations.append(current)
            current = 0

    if current > 0:
        durations.append(current)

    return {
        "drawdown_count": int(len(durations)),
        "max_drawdown_duration_trades": int(max(durations)) if durations else 0,
        "avg_drawdown_duration_trades": float(np.mean(durations)) if durations else None,
        "time_under_water_pct": float(underwater.mean()) if len(underwater) else None,
    }


def compute_recovery_metrics(equity_df: pd.DataFrame) -> Dict[str, Optional[float]]:
    if equity_df.empty:
        return {
            "recovery_count": 0,
            "max_recovery_trades": None,
            "median_recovery_trades": None,
            "avg_recovery_trades": None,
        }

    underwater = (equity_df["drawdown_abs"] < 0).tolist()
    recoveries = []
    current = 0

    for flag in underwater:
        if flag:
            current += 1
        else:
            if current > 0:
                recoveries.append(current)
            current = 0

    if current > 0:
        recoveries.append(current)

    if not recoveries:
        return {
            "recovery_count": 0,
            "max_recovery_trades": 0,
            "median_recovery_trades": None,
            "avg_recovery_trades": None,
        }

    return {
        "recovery_count": int(len(recoveries)),
        "max_recovery_trades": int(max(recoveries)),
        "median_recovery_trades": float(np.median(recoveries)),
        "avg_recovery_trades": float(np.mean(recoveries)),
    }


def compute_equity_quality(equity_df: pd.DataFrame) -> Dict[str, Optional[float]]:
    if equity_df.empty or len(equity_df) < 2:
        return {
            "equity_slope_per_trade": None,
            "equity_r2": None,
        }

    y = equity_df["equity"].astype(float).values
    x = np.arange(len(y), dtype=float)

    try:
        slope, intercept = np.polyfit(x, y, 1)
        y_pred = slope * x + intercept
        ss_res = float(np.sum((y - y_pred) ** 2))
        ss_tot = float(np.sum((y - np.mean(y)) ** 2))
        r2 = None if ss_tot == 0 else float(1.0 - ss_res / ss_tot)
    except Exception:
        slope = None
        r2 = None

    return {
        "equity_slope_per_trade": float(slope) if slope is not None else None,
        "equity_r2": r2,
    }


def compute_profit_concentration(pnl: pd.Series) -> Dict[str, Optional[float]]:
    s = pd.to_numeric(pnl, errors="coerce").dropna()
    pos = s[s > 0].sort_values(ascending=False)

    total_profit = pos.sum()
    if total_profit <= 0 or pos.empty:
        return {
            "top_1_trade_share_of_profit": None,
            "top_5_trades_share_of_profit": None,
            "top_10_trades_share_of_profit": None,
        }

    return {
        "top_1_trade_share_of_profit": float(pos.head(1).sum() / total_profit),
        "top_5_trades_share_of_profit": float(pos.head(5).sum() / total_profit),
        "top_10_trades_share_of_profit": float(pos.head(10).sum() / total_profit),
    }


def compute_return_metrics_from_equity(equity_df: pd.DataFrame) -> Dict[str, Optional[float]]:
    if equity_df.empty:
        return {
            "return_mean_trade_pct": None,
            "return_median_trade_pct": None,
            "return_std_trade_pct": None,
            "sharpe_trade": None,
            "sortino_trade": None,
        }

    eq = equity_df.copy()
    eq["prev_equity"] = eq["equity"].shift(1)
    eq.loc[eq.index[0], "prev_equity"] = DEFAULT_START_CAPITAL

    eq["trade_return_pct"] = np.where(
        eq["prev_equity"] != 0,
        eq["net_sum"] / eq["prev_equity"],
        np.nan,
    )

    r = pd.to_numeric(eq["trade_return_pct"], errors="coerce").dropna()
    if r.empty:
        return {
            "return_mean_trade_pct": None,
            "return_median_trade_pct": None,
            "return_std_trade_pct": None,
            "sharpe_trade": None,
            "sortino_trade": None,
        }

    mean_r = float(r.mean())
    median_r = float(r.median())
    std_r = float(r.std(ddof=1)) if len(r) > 1 else None

    sharpe = None
    if std_r is not None and std_r > 0:
        sharpe = float(mean_r / std_r)

    downside = r[r < 0]
    downside_std = float(downside.std(ddof=1)) if len(downside) > 1 else None
    sortino = None
    if downside_std is not None and downside_std > 0:
        sortino = float(mean_r / downside_std)

    return {
        "return_mean_trade_pct": mean_r,
        "return_median_trade_pct": median_r,
        "return_std_trade_pct": std_r,
        "sharpe_trade": sharpe,
        "sortino_trade": sortino,
    }


def compute_calendar_segments(trades: pd.DataFrame) -> Dict[str, Dict[str, object]]:
    if trades.empty:
        return {"monthly": {}, "weekly": {}}

    df = trades.copy()
    df["close_time_utc"] = safe_to_datetime_utc(df["close_time_utc"])
    df = df.dropna(subset=["close_time_utc"]).copy()

    if df.empty:
        return {"monthly": {}, "weekly": {}}

    df["month_key"] = df["close_time_utc"].dt.to_period("M").astype(str)
    df["week_key"] = df["close_time_utc"].dt.to_period("W").astype(str)

    def segment_stats(grouped_df: pd.DataFrame) -> Dict[str, object]:
        pnl = pd.to_numeric(grouped_df["net_sum"], errors="coerce")
        return {
            "trade_count": int(len(grouped_df)),
            "net_profit": float(pnl.sum()) if not pnl.empty else 0.0,
            "avg_trade": safe_mean(pnl),
            "median_trade": safe_median(pnl),
            "win_rate": float((pnl > 0).mean()) if len(pnl) else None,
        }

    monthly = {}
    for k, sub in df.groupby("month_key"):
        monthly[str(k)] = segment_stats(sub)

    weekly = {}
    for k, sub in df.groupby("week_key"):
        weekly[str(k)] = segment_stats(sub)

    return {"monthly": monthly, "weekly": weekly}


def compute_monthly_weekly_rollups(calendar_segments: Dict[str, Dict[str, object]]) -> Dict[str, Optional[float]]:
    monthly = calendar_segments.get("monthly", {})
    weekly = calendar_segments.get("weekly", {})

    monthly_profits = pd.Series([v.get("net_profit") for v in monthly.values()], dtype="float64").dropna()
    weekly_profits = pd.Series([v.get("net_profit") for v in weekly.values()], dtype="float64").dropna()

    return {
        "monthly_net_mean": float(monthly_profits.mean()) if not monthly_profits.empty else None,
        "monthly_net_median": float(monthly_profits.median()) if not monthly_profits.empty else None,
        "monthly_positive_ratio": float((monthly_profits > 0).mean()) if not monthly_profits.empty else None,
        "best_month_net": float(monthly_profits.max()) if not monthly_profits.empty else None,
        "worst_month_net": float(monthly_profits.min()) if not monthly_profits.empty else None,

        "weekly_net_mean": float(weekly_profits.mean()) if not weekly_profits.empty else None,
        "weekly_net_median": float(weekly_profits.median()) if not weekly_profits.empty else None,
        "weekly_positive_ratio": float((weekly_profits > 0).mean()) if not weekly_profits.empty else None,
        "best_week_net": float(weekly_profits.max()) if not weekly_profits.empty else None,
        "worst_week_net": float(weekly_profits.min()) if not weekly_profits.empty else None,
    }


# ============================================================
# VISUAL HELPERS
# ============================================================

def save_equity_plot(strategy_name: str, equity_df: pd.DataFrame) -> None:
    if equity_df.empty:
        return

    ensure_dir(VISUAL_EQUITY_ROOT)
    path = VISUAL_EQUITY_ROOT / f"{sanitize_name(strategy_name)}__equity.png"

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(equity_df["trade_index"], equity_df["equity"])
    ax.set_title(f"Equity Curve - {strategy_name}")
    ax.set_xlabel("Trade Index")
    ax.set_ylabel("Equity")
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def save_drawdown_plot(strategy_name: str, equity_df: pd.DataFrame) -> None:
    if equity_df.empty:
        return

    ensure_dir(VISUAL_DRAWDOWN_ROOT)
    path = VISUAL_DRAWDOWN_ROOT / f"{sanitize_name(strategy_name)}__drawdown.png"

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(equity_df["trade_index"], equity_df["drawdown_abs"])
    ax.set_title(f"Drawdown Curve - {strategy_name}")
    ax.set_xlabel("Trade Index")
    ax.set_ylabel("Drawdown Abs")
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def save_pnl_distribution_plot(strategy_name: str, trades: pd.DataFrame) -> None:
    if trades.empty or "net_sum" not in trades.columns:
        return

    pnl = pd.to_numeric(trades["net_sum"], errors="coerce").dropna()
    if pnl.empty:
        return

    ensure_dir(VISUAL_PNL_DIST_ROOT)
    path = VISUAL_PNL_DIST_ROOT / f"{sanitize_name(strategy_name)}__pnl_distribution.png"

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.hist(pnl, bins=40)
    ax.set_title(f"Trade PnL Distribution - {strategy_name}")
    ax.set_xlabel("Trade PnL")
    ax.set_ylabel("Frequency")
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def save_mfe_mae_plot(strategy_name: str, trades: pd.DataFrame) -> None:
    if trades.empty:
        return
    if "mfe_abs" not in trades.columns or "mae_abs" not in trades.columns:
        return

    df = trades.copy()
    df["mfe_abs"] = pd.to_numeric(df["mfe_abs"], errors="coerce")
    df["mae_abs"] = pd.to_numeric(df["mae_abs"], errors="coerce")
    df = df.dropna(subset=["mfe_abs", "mae_abs"])

    if df.empty:
        return

    ensure_dir(VISUAL_MFE_MAE_ROOT)
    path = VISUAL_MFE_MAE_ROOT / f"{sanitize_name(strategy_name)}__mfe_mae.png"

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.scatter(df["mae_abs"], df["mfe_abs"], alpha=0.6)
    ax.set_title(f"MFE vs MAE - {strategy_name}")
    ax.set_xlabel("MAE Abs")
    ax.set_ylabel("MFE Abs")
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def save_monthly_pnl_plot(strategy_name: str, calendar_segments: Dict[str, Dict[str, object]]) -> None:
    monthly = calendar_segments.get("monthly", {})
    if not monthly:
        return

    monthly_items = sorted(monthly.items(), key=lambda x: x[0])
    x_labels = [k for k, _ in monthly_items]
    y_values = [v.get("net_profit", 0.0) for _, v in monthly_items]

    if not y_values:
        return

    ensure_dir(VISUAL_MONTHLY_PNL_ROOT)
    path = VISUAL_MONTHLY_PNL_ROOT / f"{sanitize_name(strategy_name)}__monthly_pnl.png"

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar(x_labels, y_values)
    ax.set_title(f"Monthly Net PnL - {strategy_name}")
    ax.set_xlabel("Month")
    ax.set_ylabel("Net PnL")
    ax.tick_params(axis="x", rotation=90)
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)


# ============================================================
# METRIC ENGINE
# ============================================================

@dataclass
class StrategyMetrics:
    strategy_id: str
    symbol_mode: Optional[str]
    timeframe_mode: Optional[str]
    side_mode: Optional[str]

    trade_count: int
    win_count: int
    loss_count: int
    flat_count: int

    first_trade_time_utc: Optional[str]
    last_trade_time_utc: Optional[str]
    active_days: Optional[int]
    trades_per_day: Optional[float]

    gross_profit: Optional[float]
    gross_loss: Optional[float]
    net_profit: Optional[float]

    avg_trade: Optional[float]
    median_trade: Optional[float]
    std_trade: Optional[float]
    skew_trade: Optional[float]
    kurt_trade: Optional[float]

    p01_trade: Optional[float]
    p05_trade: Optional[float]
    p25_trade: Optional[float]
    p50_trade: Optional[float]
    p75_trade: Optional[float]
    p95_trade: Optional[float]
    p99_trade: Optional[float]

    avg_win: Optional[float]
    median_win: Optional[float]
    max_win: Optional[float]

    avg_loss: Optional[float]
    median_loss: Optional[float]
    max_loss: Optional[float]

    win_rate: Optional[float]
    loss_rate: Optional[float]
    payoff_ratio: Optional[float]
    profit_factor: Optional[float]
    expectancy: Optional[float]

    max_drawdown_abs: Optional[float]
    max_drawdown_pct: Optional[float]
    drawdown_count: Optional[int]
    max_drawdown_duration_trades: Optional[int]
    avg_drawdown_duration_trades: Optional[float]
    time_under_water_pct: Optional[float]

    recovery_count: Optional[int]
    max_recovery_trades: Optional[int]
    median_recovery_trades: Optional[float]
    avg_recovery_trades: Optional[float]

    equity_slope_per_trade: Optional[float]
    equity_r2: Optional[float]

    return_mean_trade_pct: Optional[float]
    return_median_trade_pct: Optional[float]
    return_std_trade_pct: Optional[float]
    sharpe_trade: Optional[float]
    sortino_trade: Optional[float]

    avg_holding_time_sec: Optional[float]
    median_holding_time_sec: Optional[float]
    p90_holding_time_sec: Optional[float]
    max_holding_time_sec: Optional[float]

    avg_mfe_abs: Optional[float]
    median_mfe_abs: Optional[float]
    p90_mfe_abs: Optional[float]

    avg_mae_abs: Optional[float]
    median_mae_abs: Optional[float]
    p90_mae_abs: Optional[float]

    avg_profit_capture_ratio: Optional[float]
    median_profit_capture_ratio: Optional[float]
    avg_time_to_mfe_min: Optional[float]
    avg_time_to_mae_min: Optional[float]

    max_win_streak: Optional[int]
    max_loss_streak: Optional[int]
    avg_win_streak: Optional[float]
    avg_loss_streak: Optional[float]

    top_1_trade_share_of_profit: Optional[float]
    top_5_trades_share_of_profit: Optional[float]
    top_10_trades_share_of_profit: Optional[float]

    valid_path_ratio: Optional[float]

    monthly_net_mean: Optional[float]
    monthly_net_median: Optional[float]
    monthly_positive_ratio: Optional[float]
    best_month_net: Optional[float]
    worst_month_net: Optional[float]

    weekly_net_mean: Optional[float]
    weekly_net_median: Optional[float]
    weekly_positive_ratio: Optional[float]
    best_week_net: Optional[float]
    worst_week_net: Optional[float]

    segment_sample_types: Optional[str]
    segment_account_types: Optional[str]


def compute_strategy_metrics(
    trades: pd.DataFrame,
    strategy_id: str,
) -> Tuple[StrategyMetrics, pd.DataFrame, Dict[str, Dict[str, object]]]:
    df = trades.copy().sort_values("close_time_utc").reset_index(drop=True)

    pnl = pd.to_numeric(df["net_sum"], errors="coerce")
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]

    gross_profit = float(wins.sum()) if not wins.empty else 0.0
    gross_loss = float(losses.sum()) if not losses.empty else 0.0
    net_profit = float(pnl.sum()) if not pnl.empty else 0.0

    payoff_ratio = None
    avg_win = safe_mean(wins)
    avg_loss = safe_mean(losses)

    if avg_win is not None and avg_loss is not None and avg_loss != 0:
        payoff_ratio = float(avg_win / abs(avg_loss))

    profit_factor = None
    if gross_loss != 0:
        profit_factor = float(gross_profit / abs(gross_loss))
    elif gross_profit > 0 and gross_loss == 0:
        profit_factor = float("inf")

    expectancy = safe_mean(pnl)

    first_trade = df["close_time_utc"].min() if not df.empty else pd.NaT
    last_trade = df["close_time_utc"].max() if not df.empty else pd.NaT

    active_days = None
    trades_per_day = None
    if pd.notna(first_trade) and pd.notna(last_trade):
        days = max(1, (last_trade.normalize() - first_trade.normalize()).days + 1)
        active_days = int(days)
        trades_per_day = float(len(df) / days)

    equity_df = build_equity_curve(df)
    dd_info = compute_drawdown_durations(equity_df)
    recovery_info = compute_recovery_metrics(equity_df)
    eq_quality = compute_equity_quality(equity_df)
    ret_info = compute_return_metrics_from_equity(equity_df)
    streaks = compute_streaks(pnl)
    concentration = compute_profit_concentration(pnl)
    calendar_segments = compute_calendar_segments(df)
    calendar_rollups = compute_monthly_weekly_rollups(calendar_segments)

    max_drawdown_abs = None
    if not equity_df.empty and "drawdown_abs" in equity_df.columns:
        max_drawdown_abs = float(equity_df["drawdown_abs"].min())

    max_drawdown_pct = None
    if not equity_df.empty and "drawdown_pct" in equity_df.columns:
        dd_pct = pd.to_numeric(equity_df["drawdown_pct"], errors="coerce").dropna()
        if not dd_pct.empty:
            max_drawdown_pct = float(dd_pct.min())

    valid_path_ratio = None
    if "path_data_status" in df.columns:
        valid_path_ratio = float((df["path_data_status"].astype(str).str.lower() == "ok").mean())

    metrics = StrategyMetrics(
        strategy_id=strategy_id,
        symbol_mode=df["symbol"].mode().iloc[0] if "symbol" in df.columns and not df["symbol"].mode().empty else None,
        timeframe_mode=df["profile_timeframe"].mode().iloc[0] if "profile_timeframe" in df.columns and not df["profile_timeframe"].mode().empty else None,
        side_mode=df["profile_side"].mode().iloc[0] if "profile_side" in df.columns and not df["profile_side"].mode().empty else None,

        trade_count=int(len(df)),
        win_count=int((pnl > 0).sum()),
        loss_count=int((pnl < 0).sum()),
        flat_count=int((pnl == 0).sum()),

        first_trade_time_utc=first_trade.isoformat() if pd.notna(first_trade) else None,
        last_trade_time_utc=last_trade.isoformat() if pd.notna(last_trade) else None,
        active_days=active_days,
        trades_per_day=trades_per_day,

        gross_profit=gross_profit,
        gross_loss=gross_loss,
        net_profit=net_profit,

        avg_trade=safe_mean(pnl),
        median_trade=safe_median(pnl),
        std_trade=safe_std(pnl),
        skew_trade=safe_skew(pnl),
        kurt_trade=safe_kurt(pnl),

        p01_trade=safe_quantile(pnl, 0.01),
        p05_trade=safe_quantile(pnl, 0.05),
        p25_trade=safe_quantile(pnl, 0.25),
        p50_trade=safe_quantile(pnl, 0.50),
        p75_trade=safe_quantile(pnl, 0.75),
        p95_trade=safe_quantile(pnl, 0.95),
        p99_trade=safe_quantile(pnl, 0.99),

        avg_win=avg_win,
        median_win=safe_median(wins),
        max_win=float(wins.max()) if not wins.empty else None,

        avg_loss=avg_loss,
        median_loss=safe_median(losses),
        max_loss=float(losses.min()) if not losses.empty else None,

        win_rate=float((pnl > 0).mean()) if len(pnl) else None,
        loss_rate=float((pnl < 0).mean()) if len(pnl) else None,
        payoff_ratio=payoff_ratio,
        profit_factor=profit_factor,
        expectancy=expectancy,

        max_drawdown_abs=max_drawdown_abs,
        max_drawdown_pct=max_drawdown_pct,
        drawdown_count=dd_info["drawdown_count"],
        max_drawdown_duration_trades=dd_info["max_drawdown_duration_trades"],
        avg_drawdown_duration_trades=dd_info["avg_drawdown_duration_trades"],
        time_under_water_pct=dd_info["time_under_water_pct"],

        recovery_count=recovery_info["recovery_count"],
        max_recovery_trades=recovery_info["max_recovery_trades"],
        median_recovery_trades=recovery_info["median_recovery_trades"],
        avg_recovery_trades=recovery_info["avg_recovery_trades"],

        equity_slope_per_trade=eq_quality["equity_slope_per_trade"],
        equity_r2=eq_quality["equity_r2"],

        return_mean_trade_pct=ret_info["return_mean_trade_pct"],
        return_median_trade_pct=ret_info["return_median_trade_pct"],
        return_std_trade_pct=ret_info["return_std_trade_pct"],
        sharpe_trade=ret_info["sharpe_trade"],
        sortino_trade=ret_info["sortino_trade"],

        avg_holding_time_sec=safe_mean(df["holding_time_sec"]) if "holding_time_sec" in df.columns else None,
        median_holding_time_sec=safe_median(df["holding_time_sec"]) if "holding_time_sec" in df.columns else None,
        p90_holding_time_sec=safe_quantile(df["holding_time_sec"], 0.90) if "holding_time_sec" in df.columns else None,
        max_holding_time_sec=float(pd.to_numeric(df["holding_time_sec"], errors="coerce").max()) if "holding_time_sec" in df.columns and pd.to_numeric(df["holding_time_sec"], errors="coerce").notna().any() else None,

        avg_mfe_abs=safe_mean(df["mfe_abs"]) if "mfe_abs" in df.columns else None,
        median_mfe_abs=safe_median(df["mfe_abs"]) if "mfe_abs" in df.columns else None,
        p90_mfe_abs=safe_quantile(df["mfe_abs"], 0.90) if "mfe_abs" in df.columns else None,

        avg_mae_abs=safe_mean(df["mae_abs"]) if "mae_abs" in df.columns else None,
        median_mae_abs=safe_median(df["mae_abs"]) if "mae_abs" in df.columns else None,
        p90_mae_abs=safe_quantile(df["mae_abs"], 0.90) if "mae_abs" in df.columns else None,

        avg_profit_capture_ratio=safe_mean(df["profit_capture_ratio"]) if "profit_capture_ratio" in df.columns else None,
        median_profit_capture_ratio=safe_median(df["profit_capture_ratio"]) if "profit_capture_ratio" in df.columns else None,
        avg_time_to_mfe_min=safe_mean(df["time_to_mfe_min"]) if "time_to_mfe_min" in df.columns else None,
        avg_time_to_mae_min=safe_mean(df["time_to_mae_min"]) if "time_to_mae_min" in df.columns else None,

        max_win_streak=streaks["max_win_streak"],
        max_loss_streak=streaks["max_loss_streak"],
        avg_win_streak=streaks["avg_win_streak"],
        avg_loss_streak=streaks["avg_loss_streak"],

        top_1_trade_share_of_profit=concentration["top_1_trade_share_of_profit"],
        top_5_trades_share_of_profit=concentration["top_5_trades_share_of_profit"],
        top_10_trades_share_of_profit=concentration["top_10_trades_share_of_profit"],

        valid_path_ratio=valid_path_ratio,

        monthly_net_mean=calendar_rollups["monthly_net_mean"],
        monthly_net_median=calendar_rollups["monthly_net_median"],
        monthly_positive_ratio=calendar_rollups["monthly_positive_ratio"],
        best_month_net=calendar_rollups["best_month_net"],
        worst_month_net=calendar_rollups["worst_month_net"],

        weekly_net_mean=calendar_rollups["weekly_net_mean"],
        weekly_net_median=calendar_rollups["weekly_net_median"],
        weekly_positive_ratio=calendar_rollups["weekly_positive_ratio"],
        best_week_net=calendar_rollups["best_week_net"],
        worst_week_net=calendar_rollups["worst_week_net"],

        segment_sample_types="|".join(sorted({safe_text(x) for x in df["sample_type"].dropna().unique()})) if "sample_type" in df.columns else None,
        segment_account_types="|".join(sorted({safe_text(x) for x in df["account_type"].dropna().unique()})) if "account_type" in df.columns else None,
    )

    return metrics, equity_df, calendar_segments


def compute_segment_metrics(
    trades: pd.DataFrame,
    group_col: str,
) -> Dict[str, Dict[str, object]]:
    if group_col not in trades.columns:
        return {}

    out: Dict[str, Dict[str, object]] = {}
    for segment_value, sub in trades.groupby(group_col, dropna=False):
        name = safe_text(segment_value) or "UNKNOWN"
        pnl = pd.to_numeric(sub["net_sum"], errors="coerce")

        gross_pos = pnl[pnl > 0].sum()
        gross_neg = pnl[pnl < 0].sum()

        out[name] = {
            "trade_count": int(len(sub)),
            "net_profit": float(pnl.sum()) if not pnl.empty else 0.0,
            "avg_trade": safe_mean(pnl),
            "median_trade": safe_median(pnl),
            "win_rate": float((pnl > 0).mean()) if len(pnl) else None,
            "profit_factor": float(gross_pos / abs(gross_neg)) if gross_neg != 0 else None,
            "avg_mfe_abs": safe_mean(sub["mfe_abs"]) if "mfe_abs" in sub.columns else None,
            "avg_mae_abs": safe_mean(sub["mae_abs"]) if "mae_abs" in sub.columns else None,
        }

    return out


# ============================================================
# LOADER
# ============================================================

def load_all_enriched_trades(input_root: Path) -> pd.DataFrame:
    if not input_root.exists():
        raise RuntimeError(f"Input root nicht gefunden: {input_root}")

    files = sorted(input_root.rglob("*.csv"))
    files = [f for f in files if f.name != "failed_files.csv"]

    if not files:
        raise RuntimeError(f"Keine CSV-Dateien gefunden unter: {input_root}")

    parts: List[pd.DataFrame] = []

    for path in files:
        try:
            df = pd.read_csv(path)
            if df.empty:
                continue

            df["source_file"] = str(path)

            if "strategy_id" not in df.columns:
                df["strategy_id"] = ""

            missing_mask = df["strategy_id"].isna() | (df["strategy_id"].astype(str).str.strip() == "")
            if missing_mask.any():
                df.loc[missing_mask, "strategy_id"] = Path(path).stem

            parts.append(df)

        except Exception as e:
            print(f"[WARN] Datei übersprungen: {path} | {e}")

    if not parts:
        raise RuntimeError("Alle gefundenen Dateien waren leer oder unlesbar.")

    all_trades = pd.concat(parts, ignore_index=True, sort=False)

    for col in REQUIRED_COLUMNS_MINIMAL:
        if col not in all_trades.columns:
            raise RuntimeError(f"Pflichtspalte fehlt im Gesamtinput: {col}")

    return build_trade_series(all_trades)


# ============================================================
# WRITER
# ============================================================

def save_equity_curve_csv(strategy_name: str, equity_df: pd.DataFrame) -> None:
    ensure_dir(EQUITY_OUTPUT_ROOT)
    path = EQUITY_OUTPUT_ROOT / f"{sanitize_name(strategy_name)}__equity_curve.csv"
    equity_df.to_csv(path, index=False)


def save_strategy_summary_csv(strategy_name: str, summary: Dict[str, object]) -> None:
    ensure_dir(STRATEGY_SUMMARY_OUTPUT_ROOT)
    path = STRATEGY_SUMMARY_OUTPUT_ROOT / f"{sanitize_name(strategy_name)}__summary.csv"
    pd.DataFrame([summary]).to_csv(path, index=False)


def save_strategy_json(
    strategy_name: str,
    payload: Dict[str, object],
) -> None:
    ensure_dir(STRATEGY_JSON_OUTPUT_ROOT)
    path = STRATEGY_JSON_OUTPUT_ROOT / f"{sanitize_name(strategy_name)}__summary.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def save_summary(outputs: List[Dict[str, object]]) -> None:
    ensure_dir(OUTPUT_ROOT)

    flat_rows = []
    json_rows = []

    for row in outputs:
        summary = row["summary"]
        by_sample_type = row["by_sample_type"]
        by_account_type = row["by_account_type"]

        flat = summary.copy()

        for seg_name, seg_vals in by_sample_type.items():
            prefix = f"sample_{sanitize_name(seg_name)}__"
            for k, v in seg_vals.items():
                flat[prefix + k] = v

        for seg_name, seg_vals in by_account_type.items():
            prefix = f"account_{sanitize_name(seg_name)}__"
            for k, v in seg_vals.items():
                flat[prefix + k] = v

        flat_rows.append(flat)
        json_rows.append(row)

    pd.DataFrame(flat_rows).to_csv(SUMMARY_CSV_PATH, index=False)

    with open(SUMMARY_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(json_rows, f, ensure_ascii=False, indent=2)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    print("=" * 100)
    print("STRATEGY LAYER")
    print("=" * 100)
    print(f"PROJECT_ROOT : {PROJECT_ROOT}")
    print(f"INPUT_ROOT   : {INPUT_ROOT}")
    print(f"OUTPUT_ROOT  : {OUTPUT_ROOT}")
    print("=" * 100)

    ensure_dir(OUTPUT_ROOT)
    ensure_dir(EQUITY_OUTPUT_ROOT)
    ensure_dir(STRATEGY_SUMMARY_OUTPUT_ROOT)
    ensure_dir(STRATEGY_JSON_OUTPUT_ROOT)
    ensure_dir(VISUALS_ROOT)
    ensure_dir(VISUAL_EQUITY_ROOT)
    ensure_dir(VISUAL_DRAWDOWN_ROOT)
    ensure_dir(VISUAL_PNL_DIST_ROOT)
    ensure_dir(VISUAL_MFE_MAE_ROOT)
    ensure_dir(VISUAL_MONTHLY_PNL_ROOT)

    all_trades = load_all_enriched_trades(INPUT_ROOT)

    print(f"Loaded trades       : {len(all_trades)}")
    print(f"Unique strategy_ids : {all_trades['strategy_id'].nunique()}")

    outputs: List[Dict[str, object]] = []

    for strategy_id, trades in all_trades.groupby("strategy_id", dropna=False):
        strategy_id = safe_text(strategy_id)
        if not strategy_id:
            continue

        metrics, equity_df, calendar_segments = compute_strategy_metrics(trades, strategy_id)
        by_sample_type = compute_segment_metrics(trades, "sample_type")
        by_account_type = compute_segment_metrics(trades, "account_type")

        summary_dict = {k: json_safe(v) for k, v in asdict(metrics).items()}

        payload = {
            "strategy_id": strategy_id,
            "summary": summary_dict,
            "by_sample_type": by_sample_type,
            "by_account_type": by_account_type,
            "calendar_segments": calendar_segments,
        }

        outputs.append(payload)

        save_equity_curve_csv(strategy_id, equity_df)
        save_strategy_summary_csv(strategy_id, summary_dict)
        save_strategy_json(strategy_id, payload)

        save_equity_plot(strategy_id, equity_df)
        save_drawdown_plot(strategy_id, equity_df)
        save_pnl_distribution_plot(strategy_id, trades)
        save_mfe_mae_plot(strategy_id, trades)
        save_monthly_pnl_plot(strategy_id, calendar_segments)

        print(
            f"[OK] strategy_id={strategy_id} | "
            f"trades={summary_dict['trade_count']} | "
            f"net_profit={summary_dict['net_profit']} | "
            f"max_dd={summary_dict['max_drawdown_abs']}"
        )

    save_summary(outputs)

    print("-" * 100)
    print(f"Global Summary CSV : {SUMMARY_CSV_PATH}")
    print(f"Global Summary JSON: {SUMMARY_JSON_PATH}")
    print(f"Per-Strategy CSV   : {STRATEGY_SUMMARY_OUTPUT_ROOT}")
    print(f"Per-Strategy JSON  : {STRATEGY_JSON_OUTPUT_ROOT}")
    print(f"Equity CSV dir     : {EQUITY_OUTPUT_ROOT}")
    print(f"Visuals dir        : {VISUALS_ROOT}")
    print("=" * 100)


if __name__ == "__main__":
    main()