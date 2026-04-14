# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Analyse_Center/Risk_Modeling_Layer/Risk_Modeling_Layer.py

Zweck:
- Übersetzt historische Strategiedaten in konkrete Risikomodelle
- Nimmt 0.1 Lot als beobachtete Baseline an
- Skaliert Risiko nur über diskrete MT5-Lot-Stufen
- Liefert:
    * expected_dd
    * stress_dd
    * recommended_lot_size
    * max_safe_lot_size
    * max_loss_per_day_recommendation
    * sizing_constraints
    * recommended_risk_budget

Input:
- FTMO/Data_Center/Data/Trades/Featured/Backtest/IS_OOS_Enriched/**/*.csv
  alternativ:
- FTMO/Data_Center/Data/Trades/Feutered/Backtest/IS_OOS_Enriched/**/*.csv

Optional Input:
- FTMO/Data_Center/Data/Analysis/Robustness_Layer/robustness_summary.csv
- FTMO/Data_Center/Data/Analysis/Portfolio_Layer/portfolio_layer_summary.csv
- FTMO/Data_Center/Data/Analysis/Regime_Layer/regime_layer_summary.csv

Output:
- FTMO/Data_Center/Data/Analysis/Risk_Modeling_Layer/
    - risk_modeling_summary.csv
    - risk_modeling_summary.json
    - strategy_json/<strategy_name>__risk_model.json
    - lot_tables/<strategy_name>__lot_table.csv
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
        f"Projekt-Root nicht gefunden. Erwartet Root mit 'Data_Center' und 'Dashboards'. Start={start}"
    )


SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = find_project_root(SCRIPT_PATH)

TRADES_INPUT_ROOT = (
    PROJECT_ROOT
    / "Data_Center"
    / "Data"
    / "Trades"
    / "Featured"
    / "Backtest"
    / "IS_OOS_Enriched"
)

if not TRADES_INPUT_ROOT.exists():
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
        TRADES_INPUT_ROOT = alt

ANALYSIS_ROOT = (
    PROJECT_ROOT
    / "Data_Center"
    / "Data"
    / "Analysis"
)

ROBUSTNESS_SUMMARY_PATH = ANALYSIS_ROOT / "Robustness_Layer" / "robustness_summary.csv"
PORTFOLIO_SUMMARY_PATH = ANALYSIS_ROOT / "Portfolio_Layer" / "portfolio_layer_summary.csv"
REGIME_SUMMARY_PATH = ANALYSIS_ROOT / "Regime_Layer" / "regime_layer_summary.csv"

OUTPUT_ROOT = ANALYSIS_ROOT / "Risk_Modeling_Layer"
STRATEGY_JSON_OUTPUT_ROOT = OUTPUT_ROOT / "strategy_json"
LOT_TABLE_OUTPUT_ROOT = OUTPUT_ROOT / "lot_tables"

SUMMARY_CSV_PATH = OUTPUT_ROOT / "risk_modeling_summary.csv"
SUMMARY_JSON_PATH = OUTPUT_ROOT / "risk_modeling_summary.json"


# ============================================================
# CONFIG
# ============================================================

REQUIRED_TRADE_COLUMNS = [
    "symbol",
    "open_time_utc",
    "close_time_utc",
    "net_sum",
]

SIDE_CANDIDATE_COLUMNS = [
    "profile_side",
    "side",
    "direction",
    "trade_side",
    "order_type",
]

BASELINE_LOT = 0.1
LOT_STEP = 0.1
MAX_LOT = 5.0

MAX_ALLOWED_STRESS_DD_ABS = 2500.0
MAX_ALLOWED_EXPECTED_DD_ABS = 1200.0
MAX_ALLOWED_WORST_DAY_ABS = 650.0
MAX_ALLOWED_DAILY_P05_ABS = 450.0

MIN_TRADES_FOR_STRONG_CONFIDENCE = 150
MIN_TRADES_FOR_BASIC_CONFIDENCE = 50

MONTE_CARLO_RUNS = 500
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


def safe_float(x: object) -> Optional[float]:
    try:
        if x is None or pd.isna(x):
            return None
        if math.isinf(float(x)):
            return None
        return float(x)
    except Exception:
        return None


def sanitize_name(name: str) -> str:
    return re.sub(r'[<>:"/\\\\|?*]', "_", str(name)).strip()


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
    x = pd.to_numeric(s, errors="coerce").dropna()
    if x.empty:
        return None
    return float(x.mean())


def safe_median(s: pd.Series) -> Optional[float]:
    x = pd.to_numeric(s, errors="coerce").dropna()
    if x.empty:
        return None
    return float(x.median())


def safe_std(s: pd.Series) -> Optional[float]:
    x = pd.to_numeric(s, errors="coerce").dropna()
    if len(x) < 2:
        return None
    return float(x.std(ddof=1))


def safe_quantile(s: pd.Series, q: float) -> Optional[float]:
    x = pd.to_numeric(s, errors="coerce").dropna()
    if x.empty:
        return None
    return float(x.quantile(q))


def percentile_safe(values: List[float], q: float) -> Optional[float]:
    s = pd.Series(values, dtype="float64").dropna()
    if s.empty:
        return None
    return float(np.percentile(s, q))


def extract_clean_strategy_name(raw_name: str) -> str:
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


def normalize_side_value(x: object) -> str:
    s = safe_text(x).upper()

    if s in {"BUY", "LONG", "BULL", "1"}:
        return "BUY"
    if s in {"SELL", "SHORT", "BEAR", "-1"}:
        return "SELL"
    if s in {"BOTH"}:
        return "BOTH"
    return ""


def derive_trade_side(df: pd.DataFrame) -> pd.Series:
    for col in SIDE_CANDIDATE_COLUMNS:
        if col in df.columns:
            vals = df[col].apply(normalize_side_value)
            if vals.str.len().gt(0).any():
                return vals

    if "strategy_id" in df.columns:
        sid = df["strategy_id"].astype(str).str.upper()
        out = pd.Series("", index=df.index, dtype="object")
        out.loc[sid.str.contains("_BUY", regex=False, na=False)] = "BUY"
        out.loc[sid.str.contains("_SELL", regex=False, na=False)] = "SELL"
        return out

    return pd.Series("", index=df.index, dtype="object")


def build_lot_grid(start: float = BASELINE_LOT, stop: float = MAX_LOT, step: float = LOT_STEP) -> List[float]:
    n = int(round((stop - start) / step)) + 1
    vals = [round(start + i * step, 10) for i in range(n)]
    return vals


def compute_profit_factor(pnl: pd.Series) -> Optional[float]:
    x = pd.to_numeric(pnl, errors="coerce").dropna()
    if x.empty:
        return None

    gross_profit = float(x[x > 0].sum())
    gross_loss = float(x[x < 0].sum())

    if gross_loss == 0:
        return None

    return float(gross_profit / abs(gross_loss))


def compute_equity_drawdown_from_pnl(pnl: pd.Series) -> Tuple[pd.Series, pd.Series]:
    x = pd.to_numeric(pnl, errors="coerce").fillna(0.0)
    eq = x.cumsum()
    peak = eq.cummax()
    dd = eq - peak
    return eq, dd


def compute_max_drawdown_abs(pnl: pd.Series) -> Optional[float]:
    if pnl.empty:
        return None
    _, dd = compute_equity_drawdown_from_pnl(pnl)
    return float(dd.min()) if not dd.empty else None


def compute_monte_carlo_dd_p95(pnl: pd.Series, runs: int = MONTE_CARLO_RUNS) -> Optional[float]:
    x = pd.to_numeric(pnl, errors="coerce").dropna()
    if x.empty:
        return None

    arr = x.to_numpy(dtype=float)
    dds: List[float] = []

    for _ in range(runs):
        shuffled = np.random.permutation(arr)
        _, dd = compute_equity_drawdown_from_pnl(pd.Series(shuffled))
        if not dd.empty:
            dds.append(float(dd.min()))

    if not dds:
        return None

    return float(np.percentile(dds, 5))


def summarize_side_mix(sub: pd.DataFrame) -> Optional[str]:
    sides = sorted([x for x in sub["trade_side"].dropna().astype(str).unique().tolist() if x != "UNKNOWN"])
    if not sides:
        return None
    if len(sides) == 1:
        return sides[0]
    return "BOTH"


def scale_to_lot(value: Optional[float], target_lot: Optional[float], baseline_lot: float = BASELINE_LOT) -> Optional[float]:
    if value is None or target_lot is None or pd.isna(value) or pd.isna(target_lot):
        return None
    factor = float(target_lot / baseline_lot)
    return float(value * factor)


def score_from_trade_count(trade_count: int) -> float:
    if trade_count >= MIN_TRADES_FOR_STRONG_CONFIDENCE:
        return 1.0
    if trade_count >= MIN_TRADES_FOR_BASIC_CONFIDENCE:
        return 0.7
    return 0.4


def load_optional_summary(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        if "strategy_id" not in df.columns:
            return pd.DataFrame()
        return df
    except Exception:
        return pd.DataFrame()


# ============================================================
# TRADE LOADING
# ============================================================

def build_trade_series(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for col in REQUIRED_TRADE_COLUMNS:
        if col not in out.columns:
            raise RuntimeError(f"Pflichtspalte fehlt im Trade-Input: {col}")

    out["open_time_utc"] = pd.to_datetime(out["open_time_utc"], utc=True, errors="coerce")
    out["close_time_utc"] = pd.to_datetime(out["close_time_utc"], utc=True, errors="coerce")
    out["net_sum"] = pd.to_numeric(out["net_sum"], errors="coerce")

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

    out["trade_side"] = derive_trade_side(out)
    out["trade_side"] = out["trade_side"].replace("", "UNKNOWN")

    out["trade_date_utc"] = pd.to_datetime(out["close_time_utc"], utc=True).dt.floor("D")

    if "sample_type" in out.columns:
        out["sample_type"] = out["sample_type"].astype(str).str.upper().str.strip()
    else:
        out["sample_type"] = "UNKNOWN"

    out = out.sort_values(["strategy_id", "close_time_utc", "open_time_utc"]).reset_index(drop=True)
    return out


def load_all_trades(input_root: Path) -> pd.DataFrame:
    if not input_root.exists():
        raise RuntimeError(f"Trade-Input-Root nicht gefunden: {input_root}")

    files = sorted(input_root.rglob("*.csv"))
    files = [f for f in files if f.name.lower() != "failed_files.csv"]

    if not files:
        raise RuntimeError(f"Keine Trade-CSV-Dateien gefunden unter: {input_root}")

    parts: List[pd.DataFrame] = []
    skipped: List[str] = []

    for path in files:
        try:
            df = pd.read_csv(path)
            if df.empty:
                skipped.append(f"{path} | leer")
                continue

            df["source_file"] = str(path)
            if "strategy_id" not in df.columns:
                df["strategy_id"] = ""

            parts.append(df)

        except Exception as e:
            skipped.append(f"{path} | {e}")

    if skipped:
        print("-" * 100)
        print("Übersprungene Trade-Dateien:")
        for line in skipped[:30]:
            print(f"[WARN] {line}")
        if len(skipped) > 30:
            print(f"[WARN] ... weitere {len(skipped) - 30} Einträge")
        print("-" * 100)

    if not parts:
        raise RuntimeError("Keine lesbaren Trade-Dateien geladen.")

    all_trades = pd.concat(parts, ignore_index=True, sort=False)
    return build_trade_series(all_trades)


# ============================================================
# DAILY / BASELINE RISK METRICS
# ============================================================

def build_daily_strategy_pnl(sub: pd.DataFrame) -> pd.Series:
    daily = (
        sub.groupby("trade_date_utc", dropna=False)["net_sum"]
        .sum()
        .sort_index()
    )
    daily.index = pd.to_datetime(daily.index, utc=True)
    return daily


def compute_baseline_metrics_for_strategy(sub: pd.DataFrame) -> Dict[str, object]:
    trade_pnl = pd.to_numeric(sub["net_sum"], errors="coerce").dropna()
    daily_pnl = build_daily_strategy_pnl(sub)

    mc_dd_p95 = compute_monte_carlo_dd_p95(trade_pnl, runs=MONTE_CARLO_RUNS)

    return {
        "trade_count": int(len(sub)),
        "daily_count": int(len(daily_pnl)),
        "net_profit_0_1": float(trade_pnl.sum()) if not trade_pnl.empty else 0.0,

        "avg_trade_0_1": safe_mean(trade_pnl),
        "median_trade_0_1": safe_median(trade_pnl),
        "std_trade_0_1": safe_std(trade_pnl),

        "avg_loss_0_1": safe_mean(trade_pnl[trade_pnl < 0]),
        "p05_trade_0_1": safe_quantile(trade_pnl, 0.05),
        "p01_trade_0_1": safe_quantile(trade_pnl, 0.01),

        "profit_factor_0_1": compute_profit_factor(trade_pnl),

        "historical_max_dd_0_1": compute_max_drawdown_abs(trade_pnl),
        "mc_dd_p95_0_1": mc_dd_p95,

        "avg_day_0_1": safe_mean(daily_pnl),
        "std_day_0_1": safe_std(daily_pnl),
        "worst_day_0_1": float(daily_pnl.min()) if not daily_pnl.empty else None,
        "p05_day_0_1": safe_quantile(daily_pnl, 0.05),

        "daily_pnl_series": daily_pnl,
        "trade_pnl_series": trade_pnl,
    }


# ============================================================
# OPTIONAL LAYER INPUTS
# ============================================================

def get_optional_layer_metrics(
    strategy_id: str,
    robustness_df: pd.DataFrame,
    portfolio_df: pd.DataFrame,
    regime_df: pd.DataFrame,
) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {
        "robustness_score": None,
        "portfolio_fit_score": None,
        "diversification_score": None,
        "correlation_penalty": None,
        "regime_dependency_score": None,
    }

    if not robustness_df.empty:
        row = robustness_df.loc[robustness_df["strategy_id"] == strategy_id]
        if not row.empty:
            row0 = row.iloc[0]
            out["robustness_score"] = safe_float(row0.get("robustness_score"))

    if not portfolio_df.empty:
        row = portfolio_df.loc[portfolio_df["strategy_id"] == strategy_id]
        if not row.empty:
            row0 = row.iloc[0]
            out["portfolio_fit_score"] = safe_float(row0.get("portfolio_fit_score"))
            out["diversification_score"] = safe_float(row0.get("diversification_score"))
            out["correlation_penalty"] = safe_float(row0.get("correlation_penalty"))

    if not regime_df.empty:
        row = regime_df.loc[regime_df["strategy_id"] == strategy_id]
        if not row.empty:
            row0 = row.iloc[0]
            candidates = [
                safe_float(row0.get("composite_regime_dependency_score")),
                safe_float(row0.get("trend_regime_dependency_score")),
                safe_float(row0.get("volatility_regime_dependency_score")),
                safe_float(row0.get("session_regime_dependency_score")),
            ]
            candidates = [x for x in candidates if x is not None]
            out["regime_dependency_score"] = float(np.mean(candidates)) if candidates else None

    return out


# ============================================================
# LOT SEARCH
# ============================================================

def allowed_limits_from_optional_metrics(
    robustness_score: Optional[float],
    portfolio_fit_score: Optional[float],
    regime_dependency_score: Optional[float],
    trade_count: int,
) -> Dict[str, float]:
    stress_limit = MAX_ALLOWED_STRESS_DD_ABS
    expected_dd_limit = MAX_ALLOWED_EXPECTED_DD_ABS
    worst_day_limit = MAX_ALLOWED_WORST_DAY_ABS
    day_p05_limit = MAX_ALLOWED_DAILY_P05_ABS

    sample_score = score_from_trade_count(trade_count)
    if sample_score < 1.0:
        factor = sample_score
        stress_limit *= factor
        expected_dd_limit *= factor
        worst_day_limit *= factor
        day_p05_limit *= factor

    if robustness_score is not None:
        if robustness_score < 0.55:
            stress_limit *= 0.65
            expected_dd_limit *= 0.65
            worst_day_limit *= 0.70
            day_p05_limit *= 0.70
        elif robustness_score < 0.75:
            stress_limit *= 0.85
            expected_dd_limit *= 0.85

    if portfolio_fit_score is not None:
        if portfolio_fit_score < 0.45:
            stress_limit *= 0.75
            expected_dd_limit *= 0.80
        elif portfolio_fit_score < 0.60:
            stress_limit *= 0.90

    if regime_dependency_score is not None:
        if regime_dependency_score > 0.60:
            stress_limit *= 0.80
            expected_dd_limit *= 0.85
        elif regime_dependency_score > 0.45:
            stress_limit *= 0.90

    return {
        "stress_limit_abs": float(stress_limit),
        "expected_dd_limit_abs": float(expected_dd_limit),
        "worst_day_limit_abs": float(worst_day_limit),
        "day_p05_limit_abs": float(day_p05_limit),
    }


def build_lot_table(
    baseline_metrics: Dict[str, object],
    lot_grid: List[float],
    limits: Dict[str, float],
) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []

    hist_dd_0_1 = safe_float(baseline_metrics["historical_max_dd_0_1"])
    mc_dd_0_1 = safe_float(baseline_metrics["mc_dd_p95_0_1"])
    worst_day_0_1 = safe_float(baseline_metrics["worst_day_0_1"])
    p05_day_0_1 = safe_float(baseline_metrics["p05_day_0_1"])

    for lot in lot_grid:
        expected_dd = scale_to_lot(hist_dd_0_1, lot)
        stress_dd = scale_to_lot(mc_dd_0_1, lot)
        worst_day = scale_to_lot(worst_day_0_1, lot)
        day_p05 = scale_to_lot(p05_day_0_1, lot)

        pass_expected_dd = False if expected_dd is None else abs(expected_dd) <= limits["expected_dd_limit_abs"]
        pass_stress_dd = False if stress_dd is None else abs(stress_dd) <= limits["stress_limit_abs"]
        pass_worst_day = False if worst_day is None else abs(worst_day) <= limits["worst_day_limit_abs"]
        pass_day_p05 = False if day_p05 is None else abs(day_p05) <= limits["day_p05_limit_abs"]

        pass_all = bool(pass_expected_dd and pass_stress_dd and pass_worst_day and pass_day_p05)

        rows.append({
            "lot_size": lot,
            "expected_dd_abs": expected_dd,
            "stress_dd_abs": stress_dd,
            "worst_day_abs": worst_day,
            "daily_p05_abs": day_p05,
            "pass_expected_dd": pass_expected_dd,
            "pass_stress_dd": pass_stress_dd,
            "pass_worst_day": pass_worst_day,
            "pass_day_p05": pass_day_p05,
            "pass_all": pass_all,
        })

    return pd.DataFrame(rows)


def choose_recommended_lots(lot_table: pd.DataFrame) -> Tuple[Optional[float], Optional[float]]:
    if lot_table.empty:
        return None, None

    passed = lot_table[lot_table["pass_all"] == True].copy()
    if passed.empty:
        return BASELINE_LOT, None

    max_safe = float(passed["lot_size"].max())
    recommended = max(BASELINE_LOT, round(max_safe - LOT_STEP, 10))
    recommended = min(recommended, max_safe)

    return float(recommended), float(max_safe)


def risk_budget_bucket(
    recommended_lot_size: Optional[float],
    max_safe_lot_size: Optional[float],
) -> Optional[str]:
    if recommended_lot_size is None or max_safe_lot_size is None:
        return None

    if max_safe_lot_size <= 0.3:
        return "VERY_LOW"
    if max_safe_lot_size <= 0.7:
        return "LOW"
    if max_safe_lot_size <= 1.5:
        return "MEDIUM"
    if max_safe_lot_size <= 3.0:
        return "HIGH"
    return "VERY_HIGH"


# ============================================================
# OUTPUT DATACLASS
# ============================================================

@dataclass
class RiskModelSummary:
    strategy_id: str
    side_mix: Optional[str]
    symbol_count: int
    trade_count: int

    baseline_lot: float

    avg_trade_0_1: Optional[float]
    avg_loss_0_1: Optional[float]
    p05_trade_0_1: Optional[float]
    p01_trade_0_1: Optional[float]
    worst_day_0_1: Optional[float]
    p05_day_0_1: Optional[float]

    expected_dd_0_1: Optional[float]
    stress_dd_0_1: Optional[float]

    robustness_score: Optional[float]
    portfolio_fit_score: Optional[float]
    diversification_score: Optional[float]
    regime_dependency_score: Optional[float]
    correlation_penalty: Optional[float]

    allowed_expected_dd_limit_abs: float
    allowed_stress_dd_limit_abs: float
    allowed_worst_day_limit_abs: float
    allowed_day_p05_limit_abs: float

    recommended_lot_size: Optional[float]
    max_safe_lot_size: Optional[float]

    expected_dd_at_recommended_lot: Optional[float]
    stress_dd_at_recommended_lot: Optional[float]
    max_loss_per_day_recommendation: Optional[float]

    recommended_risk_budget: Optional[str]

    sizing_constraints: Optional[str]


# ============================================================
# CORE
# ============================================================

def compute_risk_model_for_strategy(
    strategy_id: str,
    trades: pd.DataFrame,
    robustness_df: pd.DataFrame,
    portfolio_df: pd.DataFrame,
    regime_df: pd.DataFrame,
) -> Tuple[RiskModelSummary, pd.DataFrame, Dict[str, object]]:
    sub = trades[trades["strategy_id"] == strategy_id].copy().sort_values("close_time_utc").reset_index(drop=True)

    baseline = compute_baseline_metrics_for_strategy(sub)
    opt = get_optional_layer_metrics(strategy_id, robustness_df, portfolio_df, regime_df)

    limits = allowed_limits_from_optional_metrics(
        robustness_score=opt["robustness_score"],
        portfolio_fit_score=opt["portfolio_fit_score"],
        regime_dependency_score=opt["regime_dependency_score"],
        trade_count=int(baseline["trade_count"]),
    )

    lot_grid = build_lot_grid(BASELINE_LOT, MAX_LOT, LOT_STEP)
    lot_table = build_lot_table(baseline, lot_grid, limits)
    recommended_lot, max_safe_lot = choose_recommended_lots(lot_table)

    expected_dd_at_rec = scale_to_lot(safe_float(baseline["historical_max_dd_0_1"]), recommended_lot)
    stress_dd_at_rec = scale_to_lot(safe_float(baseline["mc_dd_p95_0_1"]), recommended_lot)
    max_loss_per_day_reco = scale_to_lot(safe_float(baseline["p05_day_0_1"]), recommended_lot)

    sizing_constraints = {
        "baseline_lot": BASELINE_LOT,
        "recommended_lot_size": recommended_lot,
        "max_safe_lot_size": max_safe_lot,
        "lot_step": LOT_STEP,
        "max_supported_lot": MAX_LOT,
        "max_expected_dd_abs": limits["expected_dd_limit_abs"],
        "max_stress_dd_abs": limits["stress_limit_abs"],
        "max_worst_day_abs": limits["worst_day_limit_abs"],
        "max_daily_p05_abs": limits["day_p05_limit_abs"],
    }

    summary = RiskModelSummary(
        strategy_id=strategy_id,
        side_mix=summarize_side_mix(sub),
        symbol_count=int(sub["symbol"].dropna().astype(str).nunique()),
        trade_count=int(baseline["trade_count"]),

        baseline_lot=BASELINE_LOT,

        avg_trade_0_1=safe_float(baseline["avg_trade_0_1"]),
        avg_loss_0_1=safe_float(baseline["avg_loss_0_1"]),
        p05_trade_0_1=safe_float(baseline["p05_trade_0_1"]),
        p01_trade_0_1=safe_float(baseline["p01_trade_0_1"]),
        worst_day_0_1=safe_float(baseline["worst_day_0_1"]),
        p05_day_0_1=safe_float(baseline["p05_day_0_1"]),

        expected_dd_0_1=safe_float(baseline["historical_max_dd_0_1"]),
        stress_dd_0_1=safe_float(baseline["mc_dd_p95_0_1"]),

        robustness_score=opt["robustness_score"],
        portfolio_fit_score=opt["portfolio_fit_score"],
        diversification_score=opt["diversification_score"],
        regime_dependency_score=opt["regime_dependency_score"],
        correlation_penalty=opt["correlation_penalty"],

        allowed_expected_dd_limit_abs=limits["expected_dd_limit_abs"],
        allowed_stress_dd_limit_abs=limits["stress_limit_abs"],
        allowed_worst_day_limit_abs=limits["worst_day_limit_abs"],
        allowed_day_p05_limit_abs=limits["day_p05_limit_abs"],

        recommended_lot_size=recommended_lot,
        max_safe_lot_size=max_safe_lot,

        expected_dd_at_recommended_lot=expected_dd_at_rec,
        stress_dd_at_recommended_lot=stress_dd_at_rec,
        max_loss_per_day_recommendation=max_loss_per_day_reco,

        recommended_risk_budget=risk_budget_bucket(recommended_lot, max_safe_lot),

        sizing_constraints=json.dumps(sizing_constraints, ensure_ascii=False),
    )

    payload = {
        "strategy_id": strategy_id,
        "summary": {k: json_safe(v) for k, v in asdict(summary).items()},
        "baseline_metrics": {
            k: json_safe(v)
            for k, v in baseline.items()
            if k not in {"daily_pnl_series", "trade_pnl_series"}
        },
        "optional_layer_metrics": {k: json_safe(v) for k, v in opt.items()},
        "limits": {k: json_safe(v) for k, v in limits.items()},
        "lot_grid_table": lot_table.to_dict(orient="records"),
    }

    return summary, lot_table, payload


# ============================================================
# WRITER
# ============================================================

def save_strategy_json(strategy_name: str, payload: Dict[str, object]) -> None:
    ensure_dir(STRATEGY_JSON_OUTPUT_ROOT)
    path = STRATEGY_JSON_OUTPUT_ROOT / f"{sanitize_name(strategy_name)}__risk_model.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def save_lot_table(strategy_name: str, lot_table: pd.DataFrame) -> None:
    ensure_dir(LOT_TABLE_OUTPUT_ROOT)
    path = LOT_TABLE_OUTPUT_ROOT / f"{sanitize_name(strategy_name)}__lot_table.csv"
    lot_table.to_csv(path, index=False)


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
    print("RISK MODELING LAYER")
    print("=" * 100)
    print(f"PROJECT_ROOT           : {PROJECT_ROOT}")
    print(f"TRADES_INPUT_ROOT      : {TRADES_INPUT_ROOT}")
    print(f"ROBUSTNESS_SUMMARY     : {ROBUSTNESS_SUMMARY_PATH}")
    print(f"PORTFOLIO_SUMMARY      : {PORTFOLIO_SUMMARY_PATH}")
    print(f"REGIME_SUMMARY         : {REGIME_SUMMARY_PATH}")
    print(f"OUTPUT_ROOT            : {OUTPUT_ROOT}")
    print("=" * 100)

    ensure_dir(OUTPUT_ROOT)
    ensure_dir(STRATEGY_JSON_OUTPUT_ROOT)
    ensure_dir(LOT_TABLE_OUTPUT_ROOT)

    all_trades = load_all_trades(TRADES_INPUT_ROOT)
    robustness_df = load_optional_summary(ROBUSTNESS_SUMMARY_PATH)
    portfolio_df = load_optional_summary(PORTFOLIO_SUMMARY_PATH)
    regime_df = load_optional_summary(REGIME_SUMMARY_PATH)

    print(f"Loaded trades          : {len(all_trades)}")
    print(f"Unique strategy_ids    : {all_trades['strategy_id'].nunique()}")
    print("-" * 100)

    outputs: List[Dict[str, object]] = []

    strategy_ids = sorted(all_trades["strategy_id"].dropna().astype(str).unique().tolist())

    for strategy_id in strategy_ids:
        summary, lot_table, payload = compute_risk_model_for_strategy(
            strategy_id=strategy_id,
            trades=all_trades,
            robustness_df=robustness_df,
            portfolio_df=portfolio_df,
            regime_df=regime_df,
        )

        summary_dict = {k: json_safe(v) for k, v in asdict(summary).items()}
        outputs.append(summary_dict)

        save_strategy_json(strategy_id, payload)
        save_lot_table(strategy_id, lot_table)

        print(
            f"[OK] strategy_id={strategy_id} | "
            f"recommended_lot={summary_dict['recommended_lot_size']} | "
            f"max_safe_lot={summary_dict['max_safe_lot_size']} | "
            f"risk_budget={summary_dict['recommended_risk_budget']}"
        )

    if not outputs:
        raise RuntimeError("Es konnten keine Strategien verarbeitet werden.")

    save_summary(outputs)

    print("-" * 100)
    print(f"Summary CSV   : {SUMMARY_CSV_PATH}")
    print(f"Summary JSON  : {SUMMARY_JSON_PATH}")
    print(f"Strategy JSON : {STRATEGY_JSON_OUTPUT_ROOT}")
    print(f"Lot Tables    : {LOT_TABLE_OUTPUT_ROOT}")
    print("=" * 100)


if __name__ == "__main__":
    main()