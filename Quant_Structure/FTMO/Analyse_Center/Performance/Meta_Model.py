# -*- coding: utf-8 -*-
"""
Analyse_Center/Performance/Meta_Model.py

Zweck:
- Lädt Performance-Features aus:
    FTMO/Data_Center/Data/Analysis/Performance/performance_features.csv
- Baut ein erstes Meta-Target
- Erzeugt ein lineares Meta-Modell auf Basis rank-normalisierter Features
- Berechnet pro Strategie:
    * meta_score
    * meta_probability
    * meta_pass_flag
- Speichert Outputs nach:
    FTMO/Data_Center/Data/Analysis/Performance/

Hinweis:
- Version 1 nutzt noch kein echtes Future-Labeling
- Stattdessen wird ein sauberes Proxy-Meta-Target gebaut
- Später kann dieses Modul auf echte Future-OOS-Targets umgestellt werden
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, asdict
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

FEATURES_PATH = (
    PROJECT_ROOT
    / "Data_Center"
    / "Data"
    / "Analysis"
    / "Performance"
    / "performance_features.csv"
)

OUTPUT_ROOT = (
    PROJECT_ROOT
    / "Data_Center"
    / "Data"
    / "Analysis"
    / "Performance"
)

META_MODEL_RESULTS_CSV = OUTPUT_ROOT / "meta_model_results.csv"
META_MODEL_RESULTS_JSON = OUTPUT_ROOT / "meta_model_results.json"
META_MODEL_CONFIG_JSON = OUTPUT_ROOT / "meta_model_config.json"


# ============================================================
# CONFIG
# ============================================================

MIN_ROWS_REQUIRED = 10
META_PASS_THRESHOLD = 0.60

POSITIVE_FEATURES = [
    "trade_count_total",
    "trade_count_oos",
    "active_months",
    "trades_per_month",
    "net_profit_total",
    "net_profit_oos",
    "win_rate_total",
    "win_rate_oos",
    "payoff_ratio_total",
    "expectancy_total",
    "expectancy_oos",
    "profit_factor_total",
    "profit_factor_oos",
    "return_over_maxdd",
]

NEGATIVE_ABS_FEATURES = [
    "max_drawdown_abs",
    "max_drawdown_pct",
]

DECAY_FEATURES = [
    "pf_decay_oos_vs_is",
    "expectancy_decay_oos_vs_is",
    "net_profit_decay_oos_vs_is",
]

MODEL_WEIGHTS = {
    "rank__profit_factor_oos": 0.16,
    "rank__expectancy_oos": 0.14,
    "rank__return_over_maxdd": 0.18,
    "rank__trade_count_oos": 0.08,
    "rank__active_months": 0.08,
    "rank__win_rate_oos": 0.08,
    "rank__payoff_ratio_total": 0.07,
    "rank__profit_factor_total": 0.06,
    "rank__max_drawdown_pct": 0.08,
    "rank__pf_decay_oos_vs_is": 0.08,
    "rank__expectancy_decay_oos_vs_is": 0.07,
}


# ============================================================
# HELPERS
# ============================================================

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


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


def clip_series(s: pd.Series, lower_q: float = 0.01, upper_q: float = 0.99) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    valid = s.dropna()
    if valid.empty:
        return s
    lo = valid.quantile(lower_q)
    hi = valid.quantile(upper_q)
    return s.clip(lower=lo, upper=hi)


def rank_normalize(s: pd.Series, ascending: bool = True) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    return s.rank(pct=True, ascending=ascending, method="average")


def sigmoid(x: pd.Series) -> pd.Series:
    x = pd.to_numeric(x, errors="coerce")
    return 1.0 / (1.0 + np.exp(-x))


# ============================================================
# DATA PREP
# ============================================================

def load_features(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise RuntimeError(f"performance_features.csv nicht gefunden: {path}")

    df = pd.read_csv(path)
    if df.empty:
        raise RuntimeError("performance_features.csv ist leer.")

    if "strategy_id" not in df.columns:
        raise RuntimeError("Spalte 'strategy_id' fehlt in performance_features.csv")

    if len(df) < MIN_ROWS_REQUIRED:
        raise RuntimeError(
            f"Zu wenige Strategien für Meta_Model. Gefunden={len(df)}, benötigt mindestens {MIN_ROWS_REQUIRED}"
        )

    return df.copy()


def build_rank_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    required = sorted(set(POSITIVE_FEATURES + NEGATIVE_ABS_FEATURES + DECAY_FEATURES))
    missing = [c for c in required if c not in out.columns]
    if missing:
        raise RuntimeError(f"Fehlende Spalten in performance_features.csv: {missing}")

    for col in required:
        out[col] = pd.to_numeric(out[col], errors="coerce")
        out[col] = clip_series(out[col])

    # Positive Features: höher = besser
    for col in POSITIVE_FEATURES:
        out[f"rank__{col}"] = rank_normalize(out[col], ascending=True)

    # Drawdown: kleinerer Betrag ist besser
    for col in NEGATIVE_ABS_FEATURES:
        out[f"rank__{col}"] = rank_normalize(out[col].abs(), ascending=False)

    # Decay: höher = besser, stark negativ = schlecht
    for col in DECAY_FEATURES:
        out[f"rank__{col}"] = rank_normalize(out[col], ascending=True)

    return out


# ============================================================
# META TARGET
# ============================================================

def build_proxy_meta_target(df: pd.DataFrame) -> pd.DataFrame:
    """
    Baut ein Proxy-Target für Version 1.

    Idee:
    Gute Strategien haben:
    - hohen OOS PF
    - hohe OOS Expectancy
    - gutes Return/MaxDD
    - genug OOS Trades
    - genug aktive Monate
    - keinen extremen Drawdown
    - keinen starken OOS Decay
    """
    out = df.copy()

    target = (
        0.22 * out["rank__profit_factor_oos"].fillna(0.5)
        + 0.18 * out["rank__expectancy_oos"].fillna(0.5)
        + 0.20 * out["rank__return_over_maxdd"].fillna(0.5)
        + 0.10 * out["rank__trade_count_oos"].fillna(0.5)
        + 0.10 * out["rank__active_months"].fillna(0.5)
        + 0.08 * out["rank__max_drawdown_pct"].fillna(0.5)
        + 0.06 * out["rank__pf_decay_oos_vs_is"].fillna(0.5)
        + 0.06 * out["rank__expectancy_decay_oos_vs_is"].fillna(0.5)
    )

    out["proxy_meta_target"] = target.astype(float)

    target_threshold = float(out["proxy_meta_target"].median())
    out["proxy_meta_target_label"] = (out["proxy_meta_target"] >= target_threshold).astype(int)

    return out


# ============================================================
# META MODEL
# ============================================================

@dataclass
class MetaModelRow:
    strategy_id: str
    proxy_meta_target: Optional[float]
    proxy_meta_target_label: int
    meta_score_raw: Optional[float]
    meta_score: Optional[float]
    meta_probability: Optional[float]
    meta_pass_flag: bool


def build_meta_score(df: pd.DataFrame, weights: Dict[str, float]) -> pd.Series:
    missing = [k for k in weights.keys() if k not in df.columns]
    if missing:
        raise RuntimeError(f"Gewichtete Rank-Features fehlen im DataFrame: {missing}")

    score = pd.Series(0.0, index=df.index, dtype="float64")

    for feature_name, weight in weights.items():
        score = score + (df[feature_name].fillna(0.5) * float(weight))

    return score


def scale_to_unit_interval(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    valid = s.dropna()
    if valid.empty:
        return pd.Series(np.nan, index=s.index, dtype="float64")

    lo = float(valid.min())
    hi = float(valid.max())

    if abs(hi - lo) < 1e-12:
        return pd.Series(0.5, index=s.index, dtype="float64")

    return (s - lo) / (hi - lo)


def build_meta_model_results(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    raw_score = build_meta_score(out, MODEL_WEIGHTS)
    score_01 = scale_to_unit_interval(raw_score)

    centered = (score_01 - 0.5) * 6.0
    probability = sigmoid(centered)

    out["meta_score_raw"] = raw_score.astype(float)
    out["meta_score"] = score_01.astype(float)
    out["meta_probability"] = probability.astype(float)
    out["meta_pass_flag"] = (out["meta_probability"] >= META_PASS_THRESHOLD).astype(bool)

    return out


# ============================================================
# EVALUATION
# ============================================================

def evaluate_meta_model(df: pd.DataFrame) -> Dict[str, Optional[float]]:
    out = df.copy()

    work = out.dropna(subset=["meta_score", "proxy_meta_target"]).copy()
    if work.empty:
        return {
            "spearman_meta_vs_target": None,
            "pearson_meta_vs_target": None,
            "pass_rate": None,
            "top_bucket_target_mean": None,
            "bottom_bucket_target_mean": None,
            "top_bottom_spread": None,
        }

    spearman = work["meta_score"].corr(work["proxy_meta_target"], method="spearman")
    pearson = work["meta_score"].corr(work["proxy_meta_target"], method="pearson")

    n_top = max(1, int(len(work) * 0.20))
    ranked = work.sort_values("meta_score", ascending=False).reset_index(drop=True)
    top = ranked.head(n_top)
    bottom = ranked.tail(n_top)

    top_mean = float(top["proxy_meta_target"].mean()) if not top.empty else None
    bottom_mean = float(bottom["proxy_meta_target"].mean()) if not bottom.empty else None
    spread = None
    if top_mean is not None and bottom_mean is not None:
        spread = float(top_mean - bottom_mean)

    pass_rate = float(work["meta_pass_flag"].mean()) if not work.empty else None

    return {
        "spearman_meta_vs_target": None if pd.isna(spearman) else float(spearman),
        "pearson_meta_vs_target": None if pd.isna(pearson) else float(pearson),
        "pass_rate": pass_rate,
        "top_bucket_target_mean": top_mean,
        "bottom_bucket_target_mean": bottom_mean,
        "top_bottom_spread": spread,
    }


# ============================================================
# SAVE
# ============================================================

def save_meta_model_outputs(df: pd.DataFrame, evaluation: Dict[str, Optional[float]]) -> None:
    ensure_dir(OUTPUT_ROOT)

    result_columns = [
        "strategy_id",
        "proxy_meta_target",
        "proxy_meta_target_label",
        "meta_score_raw",
        "meta_score",
        "meta_probability",
        "meta_pass_flag",
        "profit_factor_oos",
        "expectancy_oos",
        "return_over_maxdd",
        "trade_count_oos",
        "active_months",
        "max_drawdown_pct",
        "pf_decay_oos_vs_is",
        "expectancy_decay_oos_vs_is",
    ]

    existing_result_columns = [c for c in result_columns if c in df.columns]
    result_df = df[existing_result_columns].copy()
    result_df = result_df.sort_values("meta_score", ascending=False).reset_index(drop=True)

    result_df.to_csv(META_MODEL_RESULTS_CSV, index=False)

    with open(META_MODEL_RESULTS_JSON, "w", encoding="utf-8") as f:
        json.dump(
            [{k: json_safe(v) for k, v in row.items()} for row in result_df.to_dict(orient="records")],
            f,
            ensure_ascii=False,
            indent=2,
        )

    config_payload = {
        "model_type": "linear_rank_meta_model_v1",
        "features_path": str(FEATURES_PATH),
        "output_csv": str(META_MODEL_RESULTS_CSV),
        "meta_pass_threshold": META_PASS_THRESHOLD,
        "weights": MODEL_WEIGHTS,
        "evaluation": {k: json_safe(v) for k, v in evaluation.items()},
        "notes": [
            "Version 1 nutzt ein Proxy-Meta-Target.",
            "Kein echtes Future-Labeling enthalten.",
            "Meta-Modell basiert auf rank-normalisierten Features.",
            "Später auf echte OOS-Future-Targets umstellen.",
        ],
    }

    with open(META_MODEL_CONFIG_JSON, "w", encoding="utf-8") as f:
        json.dump(config_payload, f, ensure_ascii=False, indent=2)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    print("=" * 100)
    print("PERFORMANCE META MODEL")
    print("=" * 100)
    print(f"PROJECT_ROOT  : {PROJECT_ROOT}")
    print(f"FEATURES_PATH : {FEATURES_PATH}")
    print(f"OUTPUT_ROOT   : {OUTPUT_ROOT}")
    print("=" * 100)

    raw = load_features(FEATURES_PATH)
    ranked = build_rank_features(raw)
    targeted = build_proxy_meta_target(ranked)
    modeled = build_meta_model_results(targeted)
    evaluation = evaluate_meta_model(modeled)

    save_meta_model_outputs(modeled, evaluation)

    print(f"Input strategies : {len(modeled)}")
    print("-" * 100)
    print("EVALUATION")
    print(f"spearman_meta_vs_target : {evaluation['spearman_meta_vs_target']}")
    print(f"pearson_meta_vs_target  : {evaluation['pearson_meta_vs_target']}")
    print(f"pass_rate               : {evaluation['pass_rate']}")
    print(f"top_bucket_target_mean  : {evaluation['top_bucket_target_mean']}")
    print(f"bottom_bucket_target_mean: {evaluation['bottom_bucket_target_mean']}")
    print(f"top_bottom_spread       : {evaluation['top_bottom_spread']}")
    print("-" * 100)
    print(f"Results CSV   : {META_MODEL_RESULTS_CSV}")
    print(f"Results JSON  : {META_MODEL_RESULTS_JSON}")
    print(f"Config JSON   : {META_MODEL_CONFIG_JSON}")
    print("=" * 100)


if __name__ == "__main__":
    main()