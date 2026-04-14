# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Analyse_Center/Strategy_Selection_Layer/Strategy_Selection_Layer.py

Zweck:
- Führt alle bisherigen Analyse-Layer zu einer finalen Auswahlentscheidung zusammen
- Nutzt:
    * Strategy_Layer
    * Robustness_Layer
    * Regime_Layer
    * Portfolio_Layer
    * Risk_Modeling_Layer
- Erzeugt:
    * selection_status
    * final_score
    * selection_reason_codes

Mögliche Stati:
- APPROVED
- WATCHLIST
- REJECTED
- SHADOW_ONLY
"""

from __future__ import annotations

import json
import math
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

ANALYSIS_ROOT = (
    PROJECT_ROOT
    / "Data_Center"
    / "Data"
    / "Analysis"
)

STRATEGY_SUMMARY_PATH = ANALYSIS_ROOT / "Strategy_Layer" / "strategy_layer_summary.csv"
ROBUSTNESS_SUMMARY_PATH = ANALYSIS_ROOT / "Robustness_Layer" / "robustness_summary.csv"
REGIME_SUMMARY_PATH = ANALYSIS_ROOT / "Regime_Layer" / "regime_layer_summary.csv"
PORTFOLIO_SUMMARY_PATH = ANALYSIS_ROOT / "Portfolio_Layer" / "portfolio_layer_summary.csv"
RISK_MODEL_SUMMARY_PATH = ANALYSIS_ROOT / "Risk_Modeling_Layer" / "risk_modeling_summary.csv"

OUTPUT_ROOT = ANALYSIS_ROOT / "Strategy_Selection_Layer"
STRATEGY_JSON_OUTPUT_ROOT = OUTPUT_ROOT / "strategy_json"

SUMMARY_CSV_PATH = OUTPUT_ROOT / "strategy_selection_summary.csv"
SUMMARY_JSON_PATH = OUTPUT_ROOT / "strategy_selection_summary.json"


# ============================================================
# CONFIG
# ============================================================

MIN_TRADES = 80
MIN_PROFIT_FACTOR = 1.05

MIN_ROBUSTNESS_SCORE_APPROVED = 0.70
MIN_ROBUSTNESS_SCORE_WATCHLIST = 0.55

MIN_PORTFOLIO_FIT_SCORE_APPROVED = 0.50
MIN_PORTFOLIO_FIT_SCORE_WATCHLIST = 0.35

MAX_CORRELATION_PENALTY_FOR_APPROVAL = 0.60
MAX_REGIME_DEPENDENCY_FOR_APPROVAL = 0.55

MIN_FINAL_SCORE_APPROVED = 0.68
MIN_FINAL_SCORE_WATCHLIST = 0.50
MIN_FINAL_SCORE_SHADOW = 0.35

MIN_RECOMMENDED_LOT_APPROVED = 0.2
MIN_MAX_SAFE_LOT_APPROVED = 0.3

BLOCKING_RISK_BUDGETS_FOR_APPROVAL = {"VERY_LOW", ""}
HARD_REJECT_REASONS = {
    "TOO_FEW_TRADES",
    "PROFIT_FACTOR_TOO_LOW",
    "ROBUSTNESS_TOO_LOW",
    "NO_LOT_RECOMMENDATION",
    "NO_SAFE_LOT",
    "ROBUSTNESS_FAIL_FLAG",
}


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
        v = float(x)
        if math.isinf(v):
            return None
        return float(v)
    except Exception:
        return None


def safe_int(x: object) -> Optional[int]:
    try:
        if x is None or pd.isna(x):
            return None
        return int(x)
    except Exception:
        return None


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


def sanitize_name(name: str) -> str:
    return "".join("_" if c in '<>:"/\\|?*' else c for c in str(name)).strip()


def load_required_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise RuntimeError(f"{label} nicht gefunden: {path}")
    try:
        df = pd.read_csv(path)
    except Exception as e:
        raise RuntimeError(f"{label} konnte nicht geladen werden: {path} | {e}")

    if "strategy_id" not in df.columns:
        raise RuntimeError(f"{label} hat keine 'strategy_id'-Spalte: {path}")

    return df.copy()


def scale_0_1(x: Optional[float], low: float, high: float, reverse: bool = False) -> Optional[float]:
    if x is None or pd.isna(x):
        return None
    if high <= low:
        return None
    z = (float(x) - low) / (high - low)
    z = max(0.0, min(1.0, z))
    if reverse:
        z = 1.0 - z
    return float(z)


# ============================================================
# DATA MODEL
# ============================================================

@dataclass
class SelectionSummary:
    strategy_id: str

    trade_count: Optional[int]
    profit_factor: Optional[float]
    net_profit: Optional[float]

    robustness_score: Optional[float]
    portfolio_fit_score: Optional[float]
    diversification_score: Optional[float]
    regime_dependency_score: Optional[float]
    correlation_penalty: Optional[float]

    recommended_lot_size: Optional[float]
    max_safe_lot_size: Optional[float]
    recommended_risk_budget: Optional[str]

    selection_status: str
    final_score: float
    selection_reason_codes: str


# ============================================================
# MERGE / FEATURE EXTRACTION
# ============================================================

def build_master_table(
    strategy_df: pd.DataFrame,
    robustness_df: pd.DataFrame,
    regime_df: pd.DataFrame,
    portfolio_df: pd.DataFrame,
    risk_df: pd.DataFrame,
) -> pd.DataFrame:
    keep_strategy = [
        "strategy_id",
        "trade_count",
        "profit_factor",
        "net_profit",
        "win_rate",
        "max_drawdown_abs",
        "avg_trade",
    ]
    keep_strategy = [c for c in keep_strategy if c in strategy_df.columns]
    s = strategy_df[keep_strategy].copy()

    keep_rob = [
        "strategy_id",
        "robustness_score",
        "pass_flag",
        "pass_fail_flags",
        "oos_pf_decay",
        "oos_expectancy_decay",
    ]
    keep_rob = [c for c in keep_rob if c in robustness_df.columns]
    r = robustness_df[keep_rob].copy()

    keep_reg = [
        "strategy_id",
        "session_regime_dependency_score",
        "volatility_regime_dependency_score",
        "trend_regime_dependency_score",
        "composite_regime_dependency_score",
        "session_forbidden_regimes",
        "volatility_forbidden_regimes",
        "trend_forbidden_regimes",
        "composite_forbidden_regimes",
    ]
    keep_reg = [c for c in keep_reg if c in regime_df.columns]
    g = regime_df[keep_reg].copy()

    keep_port = [
        "strategy_id",
        "portfolio_fit_score",
        "diversification_score",
        "correlation_penalty",
        "marginal_dd_contribution",
        "cluster_membership",
    ]
    keep_port = [c for c in keep_port if c in portfolio_df.columns]
    p = portfolio_df[keep_port].copy()

    keep_risk = [
        "strategy_id",
        "recommended_lot_size",
        "max_safe_lot_size",
        "recommended_risk_budget",
        "stress_dd_at_recommended_lot",
        "expected_dd_at_recommended_lot",
    ]
    keep_risk = [c for c in keep_risk if c in risk_df.columns]
    k = risk_df[keep_risk].copy()

    master = s.merge(r, on="strategy_id", how="outer")
    master = master.merge(g, on="strategy_id", how="outer")
    master = master.merge(p, on="strategy_id", how="outer")
    master = master.merge(k, on="strategy_id", how="outer")

    return master


def derive_regime_dependency_score(row: pd.Series) -> Optional[float]:
    vals = [
        safe_float(row.get("composite_regime_dependency_score")),
        safe_float(row.get("trend_regime_dependency_score")),
        safe_float(row.get("volatility_regime_dependency_score")),
        safe_float(row.get("session_regime_dependency_score")),
    ]
    vals = [x for x in vals if x is not None]
    if not vals:
        return None
    return float(np.mean(vals))


def collect_forbidden_regimes(row: pd.Series) -> List[str]:
    out: List[str] = []
    cols = [
        "session_forbidden_regimes",
        "volatility_forbidden_regimes",
        "trend_forbidden_regimes",
        "composite_forbidden_regimes",
    ]
    for c in cols:
        val = safe_text(row.get(c))
        if not val:
            continue
        parts = [x.strip() for x in val.split("|") if x.strip()]
        out.extend(parts)
    return sorted(set(out))


# ============================================================
# SCORING
# ============================================================

def compute_final_score(row: pd.Series) -> float:
    trade_count = safe_int(row.get("trade_count")) or 0
    profit_factor = safe_float(row.get("profit_factor"))
    robustness_score = safe_float(row.get("robustness_score"))
    portfolio_fit_score = safe_float(row.get("portfolio_fit_score"))
    diversification_score = safe_float(row.get("diversification_score"))
    regime_dependency_score = derive_regime_dependency_score(row)
    correlation_penalty = safe_float(row.get("correlation_penalty"))
    recommended_lot_size = safe_float(row.get("recommended_lot_size"))
    max_safe_lot_size = safe_float(row.get("max_safe_lot_size"))

    trade_count_score = scale_0_1(trade_count, 30, 250)
    pf_score = scale_0_1(profit_factor, 0.95, 1.8)
    rob_score = robustness_score
    port_score = portfolio_fit_score
    div_score = diversification_score
    regime_score = scale_0_1(regime_dependency_score, 0.20, 0.80, reverse=True)
    corr_score = scale_0_1(correlation_penalty, 0.10, 0.80, reverse=True)
    lot_score = scale_0_1(recommended_lot_size, 0.1, 1.0)
    safe_lot_score = scale_0_1(max_safe_lot_size, 0.1, 1.5)

    parts = []
    weights = []

    def add_part(val: Optional[float], w: float) -> None:
        if val is None or pd.isna(val):
            return
        parts.append(float(val))
        weights.append(float(w))

    add_part(trade_count_score, 0.10)
    add_part(pf_score, 0.20)
    add_part(rob_score, 0.25)
    add_part(port_score, 0.15)
    add_part(div_score, 0.10)
    add_part(regime_score, 0.08)
    add_part(corr_score, 0.04)
    add_part(lot_score, 0.04)
    add_part(safe_lot_score, 0.04)

    if not parts or not weights:
        return 0.0

    score = float(np.average(parts, weights=weights))
    return max(0.0, min(1.0, score))


def build_reason_codes(row: pd.Series) -> List[str]:
    reason_codes: List[str] = []

    trade_count = safe_int(row.get("trade_count")) or 0
    profit_factor = safe_float(row.get("profit_factor"))
    robustness_score = safe_float(row.get("robustness_score"))
    portfolio_fit_score = safe_float(row.get("portfolio_fit_score"))
    correlation_penalty = safe_float(row.get("correlation_penalty"))
    regime_dependency_score = derive_regime_dependency_score(row)
    recommended_lot_size = safe_float(row.get("recommended_lot_size"))
    max_safe_lot_size = safe_float(row.get("max_safe_lot_size"))
    risk_budget = safe_text(row.get("recommended_risk_budget")).upper()
    pass_flag_robust = safe_text(row.get("pass_flag")).upper()

    forbidden_regimes = collect_forbidden_regimes(row)

    if trade_count < MIN_TRADES:
        reason_codes.append("TOO_FEW_TRADES")
    if profit_factor is None or profit_factor < MIN_PROFIT_FACTOR:
        reason_codes.append("PROFIT_FACTOR_TOO_LOW")
    if robustness_score is None or robustness_score < MIN_ROBUSTNESS_SCORE_WATCHLIST:
        reason_codes.append("ROBUSTNESS_TOO_LOW")
    if recommended_lot_size is None:
        reason_codes.append("NO_LOT_RECOMMENDATION")
    if max_safe_lot_size is None:
        reason_codes.append("NO_SAFE_LOT")
    if pass_flag_robust == "FALSE":
        reason_codes.append("ROBUSTNESS_FAIL_FLAG")

    if portfolio_fit_score is not None and portfolio_fit_score < MIN_PORTFOLIO_FIT_SCORE_WATCHLIST:
        reason_codes.append("PORTFOLIO_FIT_WEAK")
    if correlation_penalty is not None and correlation_penalty > MAX_CORRELATION_PENALTY_FOR_APPROVAL:
        reason_codes.append("CORRELATION_PENALTY_HIGH")
    if regime_dependency_score is not None and regime_dependency_score > MAX_REGIME_DEPENDENCY_FOR_APPROVAL:
        reason_codes.append("REGIME_DEPENDENCY_HIGH")

    if risk_budget == "VERY_LOW":
        reason_codes.append("RISK_BUDGET_VERY_LOW")
    elif risk_budget == "":
        reason_codes.append("RISK_BUDGET_MISSING")

    if forbidden_regimes:
        reason_codes.append("HAS_FORBIDDEN_REGIMES")

    return reason_codes


def approval_blocked_by_regimes(reason_codes: List[str], regime_dependency_score: Optional[float]) -> bool:
    has_forbidden = "HAS_FORBIDDEN_REGIMES" in reason_codes
    if not has_forbidden:
        return False
    if regime_dependency_score is None:
        return False
    return regime_dependency_score > MAX_REGIME_DEPENDENCY_FOR_APPROVAL


def apply_selection_logic(row: pd.Series) -> Tuple[str, float, List[str]]:
    reason_codes = build_reason_codes(row)
    final_score = compute_final_score(row)

    robustness_score = safe_float(row.get("robustness_score"))
    portfolio_fit_score = safe_float(row.get("portfolio_fit_score"))
    correlation_penalty = safe_float(row.get("correlation_penalty"))
    regime_dependency_score = derive_regime_dependency_score(row)
    recommended_lot_size = safe_float(row.get("recommended_lot_size"))
    max_safe_lot_size = safe_float(row.get("max_safe_lot_size"))
    risk_budget = safe_text(row.get("recommended_risk_budget")).upper()

    hard_reject_found = any(r in HARD_REJECT_REASONS for r in reason_codes)
    if hard_reject_found:
        if final_score >= MIN_FINAL_SCORE_SHADOW:
            return "SHADOW_ONLY", final_score, reason_codes
        return "REJECTED", final_score, reason_codes

    approval_blocked = False

    if risk_budget in BLOCKING_RISK_BUDGETS_FOR_APPROVAL:
        approval_blocked = True

    if approval_blocked_by_regimes(reason_codes, regime_dependency_score):
        approval_blocked = True

    if "CORRELATION_PENALTY_HIGH" in reason_codes:
        approval_blocked = True

    approved_conditions = [
        final_score >= MIN_FINAL_SCORE_APPROVED,
        robustness_score is not None and robustness_score >= MIN_ROBUSTNESS_SCORE_APPROVED,
        portfolio_fit_score is not None and portfolio_fit_score >= MIN_PORTFOLIO_FIT_SCORE_APPROVED,
        recommended_lot_size is not None and recommended_lot_size >= MIN_RECOMMENDED_LOT_APPROVED,
        max_safe_lot_size is not None and max_safe_lot_size >= MIN_MAX_SAFE_LOT_APPROVED,
        correlation_penalty is None or correlation_penalty <= MAX_CORRELATION_PENALTY_FOR_APPROVAL,
        regime_dependency_score is None or regime_dependency_score <= MAX_REGIME_DEPENDENCY_FOR_APPROVAL,
        not approval_blocked,
    ]

    if all(approved_conditions):
        return "APPROVED", final_score, reason_codes if reason_codes else ["PASS"]

    watchlist_conditions = [
        final_score >= MIN_FINAL_SCORE_WATCHLIST,
        robustness_score is None or robustness_score >= MIN_ROBUSTNESS_SCORE_WATCHLIST,
        portfolio_fit_score is None or portfolio_fit_score >= MIN_PORTFOLIO_FIT_SCORE_WATCHLIST,
    ]

    if all(watchlist_conditions):
        return "WATCHLIST", final_score, reason_codes if reason_codes else ["MONITOR"]

    if final_score >= MIN_FINAL_SCORE_SHADOW:
        return "SHADOW_ONLY", final_score, reason_codes if reason_codes else ["LIMITED_CONFIDENCE"]

    return "REJECTED", final_score, reason_codes if reason_codes else ["LOW_FINAL_SCORE"]


# ============================================================
# WRITER
# ============================================================

def save_strategy_json(strategy_name: str, payload: Dict[str, object]) -> None:
    ensure_dir(STRATEGY_JSON_OUTPUT_ROOT)
    path = STRATEGY_JSON_OUTPUT_ROOT / f"{sanitize_name(strategy_name)}__selection.json"
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
    print("STRATEGY SELECTION LAYER")
    print("=" * 100)
    print(f"STRATEGY_SUMMARY   : {STRATEGY_SUMMARY_PATH}")
    print(f"ROBUSTNESS_SUMMARY : {ROBUSTNESS_SUMMARY_PATH}")
    print(f"REGIME_SUMMARY     : {REGIME_SUMMARY_PATH}")
    print(f"PORTFOLIO_SUMMARY  : {PORTFOLIO_SUMMARY_PATH}")
    print(f"RISK_MODEL_SUMMARY : {RISK_MODEL_SUMMARY_PATH}")
    print(f"OUTPUT_ROOT        : {OUTPUT_ROOT}")
    print("=" * 100)

    ensure_dir(OUTPUT_ROOT)
    ensure_dir(STRATEGY_JSON_OUTPUT_ROOT)

    strategy_df = load_required_csv(STRATEGY_SUMMARY_PATH, "Strategy_Layer Summary")
    robustness_df = load_required_csv(ROBUSTNESS_SUMMARY_PATH, "Robustness_Layer Summary")
    regime_df = load_required_csv(REGIME_SUMMARY_PATH, "Regime_Layer Summary")
    portfolio_df = load_required_csv(PORTFOLIO_SUMMARY_PATH, "Portfolio_Layer Summary")
    risk_df = load_required_csv(RISK_MODEL_SUMMARY_PATH, "Risk_Modeling_Layer Summary")

    master = build_master_table(
        strategy_df=strategy_df,
        robustness_df=robustness_df,
        regime_df=regime_df,
        portfolio_df=portfolio_df,
        risk_df=risk_df,
    )

    print(f"Strategies merged : {len(master)}")
    print("-" * 100)

    outputs: List[Dict[str, object]] = []

    for _, row in master.iterrows():
        strategy_id = safe_text(row.get("strategy_id"))
        if not strategy_id:
            continue

        selection_status, final_score, reason_codes = apply_selection_logic(row)

        summary = SelectionSummary(
            strategy_id=strategy_id,

            trade_count=safe_int(row.get("trade_count")),
            profit_factor=safe_float(row.get("profit_factor")),
            net_profit=safe_float(row.get("net_profit")),

            robustness_score=safe_float(row.get("robustness_score")),
            portfolio_fit_score=safe_float(row.get("portfolio_fit_score")),
            diversification_score=safe_float(row.get("diversification_score")),
            regime_dependency_score=derive_regime_dependency_score(row),
            correlation_penalty=safe_float(row.get("correlation_penalty")),

            recommended_lot_size=safe_float(row.get("recommended_lot_size")),
            max_safe_lot_size=safe_float(row.get("max_safe_lot_size")),
            recommended_risk_budget=safe_text(row.get("recommended_risk_budget")) or None,

            selection_status=selection_status,
            final_score=float(final_score),
            selection_reason_codes="|".join(reason_codes) if reason_codes else "PASS",
        )

        summary_dict = {k: json_safe(v) for k, v in asdict(summary).items()}
        outputs.append(summary_dict)

        payload = {
            "strategy_id": strategy_id,
            "summary": summary_dict,
            "inputs_snapshot": {
                k: json_safe(v) for k, v in row.to_dict().items()
            },
        }

        save_strategy_json(strategy_id, payload)

        print(
            f"[OK] strategy_id={strategy_id} | "
            f"status={summary_dict['selection_status']} | "
            f"score={summary_dict['final_score']} | "
            f"reasons={summary_dict['selection_reason_codes']}"
        )

    if not outputs:
        raise RuntimeError("Es konnten keine Strategien selektiert werden.")

    save_summary(outputs)

    print("-" * 100)
    print(f"Summary CSV   : {SUMMARY_CSV_PATH}")
    print(f"Summary JSON  : {SUMMARY_JSON_PATH}")
    print(f"Strategy JSON : {STRATEGY_JSON_OUTPUT_ROOT}")
    print("=" * 100)


if __name__ == "__main__":
    main()