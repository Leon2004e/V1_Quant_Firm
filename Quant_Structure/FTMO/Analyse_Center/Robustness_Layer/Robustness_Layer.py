# -*- coding: utf-8 -*-
"""
Analyse_Center/Robustness_Layer/Robustness_Layer.py

Zweck:
- Liest rekursiv alle angereicherten Backtest-Trade-Dateien
- Nutzt dieselbe Strategy-Namenslogik wie der Strategy_Layer
- Bewertet Strategien auf Robustheit:
    * IS vs OOS Decay
    * Monte Carlo Trade Shuffle
    * Bootstrap Resampling
    * Outlier Dependency
    * Pass/Fail Flags
    * einfacher Robustness Score

Input:
- FTMO/Data_Center/Data/Trades/Featured/Backtest/IS_OOS_Enriched/**/*.csv
  alternativ:
- FTMO/Data_Center/Data/Trades/Feutered/Backtest/IS_OOS_Enriched/**/*.csv

Output:
- FTMO/Data_Center/Data/Analysis/Robustness_Layer/
    - robustness_summary.csv
    - robustness_summary.json
    - strategy_json/<strategy_name>__robustness.json
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
    Erwartet als echten Projekt-Root den FTMO-Ordner, also den Ordner,
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
    / "Robustness_Layer"
)

STRATEGY_JSON_OUTPUT_ROOT = OUTPUT_ROOT / "strategy_json"

SUMMARY_CSV_PATH = OUTPUT_ROOT / "robustness_summary.csv"
SUMMARY_JSON_PATH = OUTPUT_ROOT / "robustness_summary.json"


# ============================================================
# CONFIG
# ============================================================

REQUIRED_COLUMNS_MINIMAL = [
    "open_time_utc",
    "close_time_utc",
    "net_sum",
]

DEFAULT_START_CAPITAL = 100000.0
MONTE_CARLO_RUNS = 500
BOOTSTRAP_RUNS = 500


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


def percentile_safe(values: List[float], q: float) -> Optional[float]:
    s = pd.Series(values, dtype="float64").dropna()
    if s.empty:
        return None
    return float(np.percentile(s, q))


def compute_decay_ratio(base_value: Optional[float], new_value: Optional[float]) -> Optional[float]:
    if base_value is None or new_value is None:
        return None
    if pd.isna(base_value) or pd.isna(new_value):
        return None
    if abs(float(base_value)) < 1e-12:
        return None
    return float((float(new_value) - float(base_value)) / abs(float(base_value)))


def compute_profit_factor(pnl: pd.Series) -> Optional[float]:
    s = pd.to_numeric(pnl, errors="coerce").dropna()
    if s.empty:
        return None

    gross_profit = float(s[s > 0].sum())
    gross_loss = float(s[s < 0].sum())

    if gross_loss == 0:
        return None

    return float(gross_profit / abs(gross_loss))


def compute_expectancy(pnl: pd.Series) -> Optional[float]:
    s = pd.to_numeric(pnl, errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.mean())


def build_equity_curve_from_pnl(
    pnl_values: np.ndarray,
    start_capital: float = DEFAULT_START_CAPITAL
) -> pd.DataFrame:
    pnl_values = np.asarray(pnl_values, dtype=float)

    eq = pd.DataFrame({
        "trade_index": np.arange(1, len(pnl_values) + 1),
        "net_sum": pnl_values,
    })
    eq["cum_net_sum"] = eq["net_sum"].cumsum()
    eq["equity"] = float(start_capital) + eq["cum_net_sum"]
    eq["equity_peak"] = eq["equity"].cummax()
    eq["drawdown_abs"] = eq["equity"] - eq["equity_peak"]
    eq["drawdown_pct"] = np.where(
        eq["equity_peak"] != 0,
        eq["drawdown_abs"] / eq["equity_peak"],
        np.nan,
    )
    return eq


def max_drawdown_abs_from_pnl(
    pnl_values: np.ndarray,
    start_capital: float = DEFAULT_START_CAPITAL
) -> Optional[float]:
    if len(pnl_values) == 0:
        return None
    eq = build_equity_curve_from_pnl(pnl_values, start_capital=start_capital)
    dd = pd.to_numeric(eq["drawdown_abs"], errors="coerce").dropna()
    if dd.empty:
        return None
    return float(dd.min())


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
# ROBUSTNESS METRICS
# ============================================================

@dataclass
class RobustnessMetrics:
    strategy_id: str

    trade_count: int
    is_trade_count: int
    oos_trade_count: int

    pf_is: Optional[float]
    pf_oos: Optional[float]
    oos_pf_decay: Optional[float]

    expectancy_is: Optional[float]
    expectancy_oos: Optional[float]
    oos_expectancy_decay: Optional[float]

    mc_max_dd_p50: Optional[float]
    mc_max_dd_p95: Optional[float]
    mc_terminal_p05: Optional[float]
    mc_terminal_p50: Optional[float]
    mc_terminal_p95: Optional[float]

    bootstrap_pf_p05: Optional[float]
    bootstrap_pf_p50: Optional[float]
    bootstrap_pf_p95: Optional[float]

    outlier_dependency_top_1: Optional[float]
    outlier_dependency_top_5: Optional[float]
    outlier_dependency_top_10: Optional[float]

    flag_oos_pf_decay_bad: bool
    flag_oos_expectancy_decay_bad: bool
    flag_mc_drawdown_bad: bool
    flag_bootstrap_pf_bad: bool
    flag_outlier_dependency_bad: bool

    pass_fail_flags: str
    robustness_score: float
    pass_flag: bool


def compute_monte_carlo_metrics(
    pnl: pd.Series,
    runs: int = MONTE_CARLO_RUNS,
    start_capital: float = DEFAULT_START_CAPITAL,
) -> Dict[str, Optional[float]]:
    s = pd.to_numeric(pnl, errors="coerce").dropna()
    if s.empty:
        return {
            "mc_max_dd_p50": None,
            "mc_max_dd_p95": None,
            "mc_terminal_p05": None,
            "mc_terminal_p50": None,
            "mc_terminal_p95": None,
        }

    arr = s.to_numpy(dtype=float)
    max_dds: List[float] = []
    terminals: List[float] = []

    for _ in range(runs):
        shuffled = np.random.permutation(arr)
        eq = build_equity_curve_from_pnl(shuffled, start_capital=start_capital)
        max_dds.append(float(eq["drawdown_abs"].min()))
        terminals.append(float(eq["equity"].iloc[-1]))

    return {
        "mc_max_dd_p50": percentile_safe(max_dds, 50),
        "mc_max_dd_p95": percentile_safe(max_dds, 95),
        "mc_terminal_p05": percentile_safe(terminals, 5),
        "mc_terminal_p50": percentile_safe(terminals, 50),
        "mc_terminal_p95": percentile_safe(terminals, 95),
    }


def compute_bootstrap_metrics(
    pnl: pd.Series,
    runs: int = BOOTSTRAP_RUNS,
) -> Dict[str, Optional[float]]:
    s = pd.to_numeric(pnl, errors="coerce").dropna()
    if s.empty:
        return {
            "bootstrap_pf_p05": None,
            "bootstrap_pf_p50": None,
            "bootstrap_pf_p95": None,
        }

    arr = s.to_numpy(dtype=float)
    n = len(arr)
    pf_list: List[float] = []

    for _ in range(runs):
        sample = np.random.choice(arr, size=n, replace=True)
        pf = compute_profit_factor(pd.Series(sample))
        if pf is not None and np.isfinite(pf):
            pf_list.append(float(pf))

    return {
        "bootstrap_pf_p05": percentile_safe(pf_list, 5),
        "bootstrap_pf_p50": percentile_safe(pf_list, 50),
        "bootstrap_pf_p95": percentile_safe(pf_list, 95),
    }


def compute_outlier_dependency(pnl: pd.Series) -> Dict[str, Optional[float]]:
    s = pd.to_numeric(pnl, errors="coerce").dropna()
    pos = s[s > 0].sort_values(ascending=False)

    total_profit = float(pos.sum()) if not pos.empty else 0.0
    if total_profit <= 0:
        return {
            "outlier_dependency_top_1": None,
            "outlier_dependency_top_5": None,
            "outlier_dependency_top_10": None,
        }

    return {
        "outlier_dependency_top_1": float(pos.head(1).sum() / total_profit),
        "outlier_dependency_top_5": float(pos.head(5).sum() / total_profit),
        "outlier_dependency_top_10": float(pos.head(10).sum() / total_profit),
    }


def compute_strategy_robustness(strategy_id: str, trades: pd.DataFrame) -> RobustnessMetrics:
    df = trades.copy().sort_values("close_time_utc").reset_index(drop=True)
    pnl = pd.to_numeric(df["net_sum"], errors="coerce").dropna()

    is_df = df[df["sample_type"] == "IS"].copy() if "sample_type" in df.columns else df.iloc[0:0].copy()
    oos_df = df[df["sample_type"] == "OOS"].copy() if "sample_type" in df.columns else df.iloc[0:0].copy()

    if is_df.empty and oos_df.empty:
        is_df = df.copy()
        oos_df = df.copy()

    pf_is = compute_profit_factor(is_df["net_sum"]) if not is_df.empty else None
    pf_oos = compute_profit_factor(oos_df["net_sum"]) if not oos_df.empty else None

    expectancy_is = compute_expectancy(is_df["net_sum"]) if not is_df.empty else None
    expectancy_oos = compute_expectancy(oos_df["net_sum"]) if not oos_df.empty else None

    oos_pf_decay = compute_decay_ratio(pf_is, pf_oos)
    oos_expectancy_decay = compute_decay_ratio(expectancy_is, expectancy_oos)

    mc = compute_monte_carlo_metrics(pnl)
    bs = compute_bootstrap_metrics(pnl)
    outlier = compute_outlier_dependency(pnl)

    actual_max_dd = max_drawdown_abs_from_pnl(pnl.to_numpy(dtype=float))

    flag_oos_pf_decay_bad = False if oos_pf_decay is None else (oos_pf_decay < -0.30)
    flag_oos_expectancy_decay_bad = False if oos_expectancy_decay is None else (oos_expectancy_decay < -0.40)

    flag_mc_drawdown_bad = False
    if actual_max_dd is not None and mc["mc_max_dd_p95"] is not None:
        flag_mc_drawdown_bad = mc["mc_max_dd_p95"] < (1.5 * actual_max_dd)

    flag_bootstrap_pf_bad = False if bs["bootstrap_pf_p05"] is None else (bs["bootstrap_pf_p05"] < 1.0)
    flag_outlier_dependency_bad = False if outlier["outlier_dependency_top_5"] is None else (outlier["outlier_dependency_top_5"] > 0.65)

    score = 1.0
    if flag_oos_pf_decay_bad:
        score -= 0.25
    if flag_oos_expectancy_decay_bad:
        score -= 0.20
    if flag_mc_drawdown_bad:
        score -= 0.20
    if flag_bootstrap_pf_bad:
        score -= 0.20
    if flag_outlier_dependency_bad:
        score -= 0.15
    score = max(0.0, min(1.0, score))

    flags: List[str] = []
    if flag_oos_pf_decay_bad:
        flags.append("OOS_PF_DECAY_BAD")
    if flag_oos_expectancy_decay_bad:
        flags.append("OOS_EXPECTANCY_DECAY_BAD")
    if flag_mc_drawdown_bad:
        flags.append("MC_DRAWDOWN_BAD")
    if flag_bootstrap_pf_bad:
        flags.append("BOOTSTRAP_PF_BAD")
    if flag_outlier_dependency_bad:
        flags.append("OUTLIER_DEPENDENCY_BAD")

    pass_flag = score >= 0.55

    return RobustnessMetrics(
        strategy_id=strategy_id,

        trade_count=int(len(df)),
        is_trade_count=int(len(is_df)),
        oos_trade_count=int(len(oos_df)),

        pf_is=pf_is,
        pf_oos=pf_oos,
        oos_pf_decay=oos_pf_decay,

        expectancy_is=expectancy_is,
        expectancy_oos=expectancy_oos,
        oos_expectancy_decay=oos_expectancy_decay,

        mc_max_dd_p50=mc["mc_max_dd_p50"],
        mc_max_dd_p95=mc["mc_max_dd_p95"],
        mc_terminal_p05=mc["mc_terminal_p05"],
        mc_terminal_p50=mc["mc_terminal_p50"],
        mc_terminal_p95=mc["mc_terminal_p95"],

        bootstrap_pf_p05=bs["bootstrap_pf_p05"],
        bootstrap_pf_p50=bs["bootstrap_pf_p50"],
        bootstrap_pf_p95=bs["bootstrap_pf_p95"],

        outlier_dependency_top_1=outlier["outlier_dependency_top_1"],
        outlier_dependency_top_5=outlier["outlier_dependency_top_5"],
        outlier_dependency_top_10=outlier["outlier_dependency_top_10"],

        flag_oos_pf_decay_bad=flag_oos_pf_decay_bad,
        flag_oos_expectancy_decay_bad=flag_oos_expectancy_decay_bad,
        flag_mc_drawdown_bad=flag_mc_drawdown_bad,
        flag_bootstrap_pf_bad=flag_bootstrap_pf_bad,
        flag_outlier_dependency_bad=flag_outlier_dependency_bad,

        pass_fail_flags="|".join(flags) if flags else "PASS",
        robustness_score=float(score),
        pass_flag=bool(pass_flag),
    )


# ============================================================
# WRITER
# ============================================================

def save_strategy_json(strategy_name: str, payload: Dict[str, object]) -> None:
    ensure_dir(STRATEGY_JSON_OUTPUT_ROOT)
    path = STRATEGY_JSON_OUTPUT_ROOT / f"{sanitize_name(strategy_name)}__robustness.json"
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
    print("ROBUSTNESS LAYER")
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

        metrics = compute_strategy_robustness(strategy_id, trades)
        summary_dict = {k: json_safe(v) for k, v in asdict(metrics).items()}

        payload = {
            "strategy_id": strategy_id,
            "summary": summary_dict,
        }

        outputs.append(summary_dict)
        save_strategy_json(strategy_id, payload)

        print(
            f"[OK] strategy_id={strategy_id} | "
            f"trades={summary_dict['trade_count']} | "
            f"score={summary_dict['robustness_score']} | "
            f"flags={summary_dict['pass_fail_flags']}"
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