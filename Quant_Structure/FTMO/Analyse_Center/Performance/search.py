# -*- coding: utf-8 -*-
"""
Analyse_Center/Performance/search.py

Zweck:
- Lädt reine Performance-Features aus:
    FTMO/Data_Center/Data/Analysis/Performance/performance_features.csv
- Baut rank-normalisierte reine Performance-Features
- Führt Random Search über Performance-Score-Kandidaten durch
- Bewertet die Kandidaten nur auf Basis von Performance
- Speichert die besten Kandidaten

Wichtig:
- Keine Robustness-Features
- Keine Risk-/Drawdown-Features
- Keine Hybrid-Metriken wie Return/MaxDD

Output:
- FTMO/Data_Center/Data/Analysis/Performance/
    - search_results.csv
    - search_results.json
    - best_score_model.json
    - best_candidate_strategy_scores.csv
    - top_models.csv
    - feature_usage_top_models.csv
"""

from __future__ import annotations

import json
import math
import random
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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

SEARCH_RESULTS_CSV = OUTPUT_ROOT / "search_results.csv"
SEARCH_RESULTS_JSON = OUTPUT_ROOT / "search_results.json"
BEST_MODEL_JSON = OUTPUT_ROOT / "best_score_model.json"
BEST_STRATEGY_SCORES_CSV = OUTPUT_ROOT / "best_candidate_strategy_scores.csv"
TOP_MODELS_CSV = OUTPUT_ROOT / "top_models.csv"
FEATURE_USAGE_CSV = OUTPUT_ROOT / "feature_usage_top_models.csv"


# ============================================================
# CONFIG
# ============================================================

RANDOM_SEED = 42

N_CANDIDATES = 5000
MIN_FEATURES_PER_MODEL = 3
MAX_FEATURES_PER_MODEL = 7

TOP_QUANTILE = 0.20
MIN_ROWS_REQUIRED = 10

MIN_TRADE_COUNT_TOTAL = 30
MIN_TRADE_COUNT_OOS = 10
MIN_ACTIVE_MONTHS = 3

TOP_MODELS_FOR_ANALYSIS = 100

OBJECTIVE_WEIGHTS = {
    "spearman": 0.50,
    "spread": 0.25,
    "top_mean": 0.15,
    "complexity_penalty": 0.05,
    "concentration_penalty": 0.05,
}


# ============================================================
# PURE PERFORMANCE FEATURE SCHEMA
# ============================================================

FEATURE_SCHEMA: Dict[str, Dict[str, str]] = {
    "trade_count_total": {"rank_mode": "higher_better", "role": "score"},
    "trade_count_is": {"rank_mode": "higher_better", "role": "score"},
    "trade_count_oos": {"rank_mode": "higher_better", "role": "score"},
    "active_months": {"rank_mode": "higher_better", "role": "score"},
    "trades_per_month": {"rank_mode": "higher_better", "role": "score"},

    "net_profit_total": {"rank_mode": "higher_better", "role": "score"},
    "net_profit_is": {"rank_mode": "higher_better", "role": "score"},
    "net_profit_oos": {"rank_mode": "higher_better", "role": "score"},

    "win_rate_total": {"rank_mode": "higher_better", "role": "score"},
    "win_rate_is": {"rank_mode": "higher_better", "role": "score"},
    "win_rate_oos": {"rank_mode": "higher_better", "role": "score"},

    "avg_win_total": {"rank_mode": "higher_better", "role": "score"},
    "avg_loss_total": {"rank_mode": "lower_abs_better", "role": "score"},
    "payoff_ratio_total": {"rank_mode": "higher_better", "role": "score"},

    "expectancy_total": {"rank_mode": "higher_better", "role": "score"},
    "expectancy_is": {"rank_mode": "higher_better", "role": "score"},
    "expectancy_oos": {"rank_mode": "higher_better", "role": "score"},

    "profit_factor_total": {"rank_mode": "higher_better", "role": "score"},
    "profit_factor_is": {"rank_mode": "higher_better", "role": "score"},
    "profit_factor_oos": {"rank_mode": "higher_better", "role": "score"},
}

# Proxy-Target bleibt rein performance-basiert
PROXY_TARGET_WEIGHTS = {
    "rank__profit_factor_oos": 0.22,
    "rank__expectancy_oos": 0.20,
    "rank__net_profit_oos": 0.16,
    "rank__win_rate_oos": 0.12,
    "rank__payoff_ratio_total": 0.10,
    "rank__trade_count_oos": 0.10,
    "rank__active_months": 0.05,
    "rank__trades_per_month": 0.05,
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


def rank_normalize(s: pd.Series, mode: str) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")

    if mode == "higher_better":
        return s.rank(pct=True, ascending=True, method="average")

    if mode == "lower_better":
        return s.rank(pct=True, ascending=False, method="average")

    if mode == "lower_abs_better":
        return s.abs().rank(pct=True, ascending=False, method="average")

    raise RuntimeError(f"Unbekannter rank_mode: {mode}")


def normalize_weights(raw: np.ndarray) -> np.ndarray:
    raw = np.asarray(raw, dtype=float)
    total = float(raw.sum())
    if total <= 0:
        return np.ones(len(raw), dtype=float) / max(1, len(raw))
    return raw / total


def compute_weight_entropy(weights: np.ndarray) -> float:
    w = np.asarray(weights, dtype=float)
    w = w[w > 0]
    if len(w) == 0:
        return 0.0
    return float(-(w * np.log(w)).sum())


def effective_number_of_features(weights: np.ndarray) -> float:
    w = np.asarray(weights, dtype=float)
    denom = float((w ** 2).sum())
    if denom <= 1e-12:
        return 0.0
    return float(1.0 / denom)


def complexity_penalty(n_features: int) -> float:
    extra = max(0, n_features - 4)
    return float(0.015 * extra)


def concentration_penalty(weights: np.ndarray) -> float:
    w = np.asarray(weights, dtype=float)
    if len(w) == 0:
        return 0.0
    max_weight = float(w.max())
    penalty = max(0.0, max_weight - 0.45) * 0.50
    return float(penalty)


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
            f"Zu wenige Strategien für die Suche. Gefunden={len(df)}, benötigt mindestens {MIN_ROWS_REQUIRED}"
        )

    return df.copy()


def apply_quality_filters(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    needed = ["trade_count_total", "trade_count_oos", "active_months"]
    missing = [c for c in needed if c not in out.columns]
    if missing:
        raise RuntimeError(f"Pflichtspalten für Qualitätsfilter fehlen: {missing}")

    for col in needed:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out[
        (out["trade_count_total"] >= MIN_TRADE_COUNT_TOTAL)
        & (out["trade_count_oos"] >= MIN_TRADE_COUNT_OOS)
        & (out["active_months"] >= MIN_ACTIVE_MONTHS)
    ].copy()

    return out.reset_index(drop=True)


def build_rank_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    required_cols = sorted(set(FEATURE_SCHEMA.keys()))
    missing = [c for c in required_cols if c not in out.columns]
    if missing:
        raise RuntimeError(f"Fehlende Spalten in performance_features.csv: {missing}")

    for col, cfg in FEATURE_SCHEMA.items():
        out[col] = pd.to_numeric(out[col], errors="coerce")
        out[col] = clip_series(out[col])
        out[f"rank__{col}"] = rank_normalize(out[col], mode=cfg["rank_mode"])

    out = out.dropna(subset=["strategy_id"]).copy()

    rank_cols = [c for c in out.columns if c.startswith("rank__")]
    non_na_rank_count = out[rank_cols].notna().sum(axis=1)
    out = out[non_na_rank_count >= 5].copy()

    return out.reset_index(drop=True)


def get_score_rank_columns(df: pd.DataFrame) -> List[str]:
    cols: List[str] = []
    for key, cfg in FEATURE_SCHEMA.items():
        if cfg["role"] == "score":
            rank_col = f"rank__{key}"
            if rank_col in df.columns:
                cols.append(rank_col)
    return cols


# ============================================================
# PURE PERFORMANCE PROXY TARGET
# ============================================================

def build_proxy_target(df: pd.DataFrame) -> pd.Series:
    parts: List[pd.Series] = []

    for col, weight in PROXY_TARGET_WEIGHTS.items():
        if col in df.columns:
            parts.append(df[col].fillna(0.5) * float(weight))

    if not parts:
        return pd.Series(np.nan, index=df.index, dtype="float64")

    target = sum(parts)
    return target.astype(float)


# ============================================================
# SEARCH
# ============================================================

def pick_candidate_features(rank_columns: List[str]) -> List[str]:
    k = random.randint(MIN_FEATURES_PER_MODEL, min(MAX_FEATURES_PER_MODEL, len(rank_columns)))
    return random.sample(rank_columns, k=k)


def generate_random_weights(n: int) -> np.ndarray:
    raw = np.random.uniform(0.05, 1.0, size=n)
    return normalize_weights(raw)


def build_candidate_score(df: pd.DataFrame, feature_names: List[str], weights: np.ndarray) -> pd.Series:
    score = pd.Series(0.0, index=df.index, dtype="float64")

    for col, w in zip(feature_names, weights):
        score = score + (df[col].fillna(0.5) * float(w))

    return score.astype(float)


def evaluate_candidate(
    df: pd.DataFrame,
    score: pd.Series,
    proxy_target: pd.Series,
    weights: np.ndarray,
    n_features: int,
) -> Dict[str, Optional[float]]:
    work = df.copy()
    work["candidate_score"] = pd.to_numeric(score, errors="coerce")
    work["proxy_target"] = pd.to_numeric(proxy_target, errors="coerce")

    work = work.dropna(subset=["candidate_score", "proxy_target"]).copy()
    if work.empty:
        return {
            "objective": None,
            "spearman_score_vs_target": None,
            "pearson_score_vs_target": None,
            "top_bucket_target_mean": None,
            "bottom_bucket_target_mean": None,
            "top_bottom_spread": None,
            "complexity_penalty": None,
            "concentration_penalty": None,
            "max_weight": None,
            "weight_entropy": None,
            "effective_n_features": None,
        }

    ranked = work.sort_values("candidate_score", ascending=False).reset_index(drop=True)

    spearman = ranked["candidate_score"].corr(ranked["proxy_target"], method="spearman")
    pearson = ranked["candidate_score"].corr(ranked["proxy_target"], method="pearson")

    n_top = max(1, int(len(ranked) * TOP_QUANTILE))
    top = ranked.head(n_top)
    bottom = ranked.tail(n_top)

    top_mean = float(top["proxy_target"].mean()) if not top.empty else None
    bottom_mean = float(bottom["proxy_target"].mean()) if not bottom.empty else None

    spread = None
    if top_mean is not None and bottom_mean is not None:
        spread = float(top_mean - bottom_mean)

    c_pen = complexity_penalty(n_features)
    w_pen = concentration_penalty(weights)

    objective = None
    if spearman is not None and not pd.isna(spearman) and spread is not None and top_mean is not None:
        objective = float(
            (OBJECTIVE_WEIGHTS["spearman"] * float(spearman))
            + (OBJECTIVE_WEIGHTS["spread"] * float(spread))
            + (OBJECTIVE_WEIGHTS["top_mean"] * float(top_mean))
            - (OBJECTIVE_WEIGHTS["complexity_penalty"] * float(c_pen))
            - (OBJECTIVE_WEIGHTS["concentration_penalty"] * float(w_pen))
        )

    return {
        "objective": objective,
        "spearman_score_vs_target": None if pd.isna(spearman) else float(spearman),
        "pearson_score_vs_target": None if pd.isna(pearson) else float(pearson),
        "top_bucket_target_mean": top_mean,
        "bottom_bucket_target_mean": bottom_mean,
        "top_bottom_spread": spread,
        "complexity_penalty": float(c_pen),
        "concentration_penalty": float(w_pen),
        "max_weight": float(np.max(weights)) if len(weights) > 0 else None,
        "weight_entropy": compute_weight_entropy(weights),
        "effective_n_features": effective_number_of_features(weights),
    }


# ============================================================
# ANALYSIS
# ============================================================

def build_feature_usage_table(results_df: pd.DataFrame, top_k: int) -> pd.DataFrame:
    if results_df.empty:
        return pd.DataFrame(columns=["feature", "count", "share_in_top_k"])

    top_df = results_df.head(top_k).copy()
    counter: Dict[str, int] = {}

    for _, row in top_df.iterrows():
        feats = str(row["features"]).split("|") if pd.notna(row["features"]) else []
        feats = [f for f in feats if f]
        for feat in feats:
            counter[feat] = counter.get(feat, 0) + 1

    usage_rows = []
    for feat, count in sorted(counter.items(), key=lambda x: (-x[1], x[0])):
        usage_rows.append({
            "feature": feat,
            "count": int(count),
            "share_in_top_k": float(count / max(1, len(top_df))),
        })

    return pd.DataFrame(usage_rows)


def build_best_strategy_scores(
    feature_df: pd.DataFrame,
    results_df: pd.DataFrame,
    proxy_target: pd.Series,
) -> pd.DataFrame:
    out = feature_df.copy()

    if results_df.empty or pd.isna(results_df.loc[0, "objective"]):
        return pd.DataFrame()

    best_features = str(results_df.loc[0, "features"]).split("|")
    best_features = [x for x in best_features if x]

    best_weights = np.array(
        [float(x) for x in str(results_df.loc[0, "weights"]).split("|") if x],
        dtype=float,
    )

    out["best_candidate_score"] = build_candidate_score(out, best_features, best_weights)
    out["proxy_target"] = pd.to_numeric(proxy_target, errors="coerce")
    out = out.sort_values("best_candidate_score", ascending=False).reset_index(drop=True)

    keep_cols = [c for c in out.columns if c in [
        "strategy_id",
        "best_candidate_score",
        "proxy_target",
        "trade_count_total",
        "trade_count_is",
        "trade_count_oos",
        "active_months",
        "trades_per_month",
        "net_profit_total",
        "net_profit_is",
        "net_profit_oos",
        "win_rate_total",
        "win_rate_is",
        "win_rate_oos",
        "avg_win_total",
        "avg_loss_total",
        "payoff_ratio_total",
        "expectancy_total",
        "expectancy_is",
        "expectancy_oos",
        "profit_factor_total",
        "profit_factor_is",
        "profit_factor_oos",
    ]]

    return out[keep_cols].copy()


# ============================================================
# MAIN SEARCH
# ============================================================

def run_search() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.Series]:
    raw = load_features(FEATURES_PATH)
    filtered = apply_quality_filters(raw)

    if filtered.empty:
        raise RuntimeError(
            "Nach Qualitätsfiltern sind keine Strategien mehr übrig. "
            "Prüfe Mindestwerte für trade_count_total, trade_count_oos und active_months."
        )

    feature_df = build_rank_feature_frame(filtered)

    if len(feature_df) < MIN_ROWS_REQUIRED:
        raise RuntimeError(
            f"Zu wenige Strategien für die Suche nach Qualitätsfiltern. "
            f"Gefunden={len(feature_df)}, benötigt mindestens {MIN_ROWS_REQUIRED}"
        )

    proxy_target = build_proxy_target(feature_df)
    if proxy_target.dropna().empty:
        raise RuntimeError("Proxy-Target konnte nicht gebaut werden.")

    rank_columns = get_score_rank_columns(feature_df)
    if len(rank_columns) < MIN_FEATURES_PER_MODEL:
        raise RuntimeError("Zu wenige Score-Rank-Features für die Suche vorhanden.")

    results: List[Dict[str, object]] = []

    for _ in range(N_CANDIDATES):
        feat_names = pick_candidate_features(rank_columns)
        weights = generate_random_weights(len(feat_names))
        score = build_candidate_score(feature_df, feat_names, weights)
        evaluation = evaluate_candidate(
            df=feature_df,
            score=score,
            proxy_target=proxy_target,
            weights=weights,
            n_features=len(feat_names),
        )

        result = {
            "objective": evaluation["objective"],
            "spearman_score_vs_target": evaluation["spearman_score_vs_target"],
            "pearson_score_vs_target": evaluation["pearson_score_vs_target"],
            "top_bucket_target_mean": evaluation["top_bucket_target_mean"],
            "bottom_bucket_target_mean": evaluation["bottom_bucket_target_mean"],
            "top_bottom_spread": evaluation["top_bottom_spread"],
            "complexity_penalty": evaluation["complexity_penalty"],
            "concentration_penalty": evaluation["concentration_penalty"],
            "max_weight": evaluation["max_weight"],
            "weight_entropy": evaluation["weight_entropy"],
            "effective_n_features": evaluation["effective_n_features"],
            "n_features": len(feat_names),
            "features": "|".join(feat_names),
            "weights": "|".join([f"{w:.8f}" for w in weights]),
        }
        results.append(result)

    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values(
        [
            "objective",
            "spearman_score_vs_target",
            "top_bottom_spread",
            "top_bucket_target_mean",
        ],
        ascending=False,
        na_position="last",
    ).reset_index(drop=True)

    results_df.insert(0, "rank", np.arange(1, len(results_df) + 1))

    top_models_df = results_df.head(TOP_MODELS_FOR_ANALYSIS).copy()
    feature_usage_df = build_feature_usage_table(results_df, top_k=TOP_MODELS_FOR_ANALYSIS)
    best_strategy_scores_df = build_best_strategy_scores(feature_df, results_df, proxy_target)

    return results_df, top_models_df, feature_usage_df, best_strategy_scores_df, feature_df, proxy_target


# ============================================================
# SAVE
# ============================================================

def save_outputs(
    results_df: pd.DataFrame,
    top_models_df: pd.DataFrame,
    feature_usage_df: pd.DataFrame,
    best_strategy_scores_df: pd.DataFrame,
    feature_df: pd.DataFrame,
    proxy_target: pd.Series,
) -> None:
    ensure_dir(OUTPUT_ROOT)

    results_df.to_csv(SEARCH_RESULTS_CSV, index=False)
    top_models_df.to_csv(TOP_MODELS_CSV, index=False)
    feature_usage_df.to_csv(FEATURE_USAGE_CSV, index=False)

    with open(SEARCH_RESULTS_JSON, "w", encoding="utf-8") as f:
        json.dump(
            [{k: json_safe(v) for k, v in row.items()} for row in results_df.to_dict(orient="records")],
            f,
            ensure_ascii=False,
            indent=2,
        )

    if not best_strategy_scores_df.empty:
        best_strategy_scores_df.to_csv(BEST_STRATEGY_SCORES_CSV, index=False)

    if not results_df.empty:
        best_row = results_df.iloc[0].to_dict()

        best_payload = {
            "model_type": "pure_performance_random_rank_weighted_search_v1",
            "features_source": str(FEATURES_PATH),
            "search_config": {
                "random_seed": RANDOM_SEED,
                "n_candidates": N_CANDIDATES,
                "min_features_per_model": MIN_FEATURES_PER_MODEL,
                "max_features_per_model": MAX_FEATURES_PER_MODEL,
                "top_quantile": TOP_QUANTILE,
                "min_rows_required": MIN_ROWS_REQUIRED,
                "min_trade_count_total": MIN_TRADE_COUNT_TOTAL,
                "min_trade_count_oos": MIN_TRADE_COUNT_OOS,
                "min_active_months": MIN_ACTIVE_MONTHS,
                "top_models_for_analysis": TOP_MODELS_FOR_ANALYSIS,
            },
            "objective_weights": OBJECTIVE_WEIGHTS,
            "proxy_target_weights": PROXY_TARGET_WEIGHTS,
            "n_input_rows_after_filter": int(len(feature_df)),
            "proxy_target_summary": {
                "mean": json_safe(proxy_target.mean()),
                "std": json_safe(proxy_target.std()),
                "min": json_safe(proxy_target.min()),
                "max": json_safe(proxy_target.max()),
            },
            "best_model": {k: json_safe(v) for k, v in best_row.items()},
            "top_models_preview": [
                {k: json_safe(v) for k, v in row.items()}
                for row in top_models_df.head(10).to_dict(orient="records")
            ],
            "feature_usage_top_models": [
                {k: json_safe(v) for k, v in row.items()}
                for row in feature_usage_df.to_dict(orient="records")
            ],
            "notes": [
                "Reiner Performance-Search.",
                "Keine Risk- oder Robustness-Metriken enthalten.",
                "Proxy-Target bleibt eine Übergangslösung innerhalb des Performance-Layers.",
                "Top-Modelle und Feature Usage werden separat gespeichert.",
            ],
        }

        with open(BEST_MODEL_JSON, "w", encoding="utf-8") as f:
            json.dump(best_payload, f, ensure_ascii=False, indent=2)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    print("=" * 100)
    print("PURE PERFORMANCE SCORE SEARCH")
    print("=" * 100)
    print(f"PROJECT_ROOT             : {PROJECT_ROOT}")
    print(f"FEATURES_PATH            : {FEATURES_PATH}")
    print(f"OUTPUT_ROOT              : {OUTPUT_ROOT}")
    print(f"N_CANDIDATES             : {N_CANDIDATES}")
    print(f"MIN_TRADE_COUNT_TOTAL    : {MIN_TRADE_COUNT_TOTAL}")
    print(f"MIN_TRADE_COUNT_OOS      : {MIN_TRADE_COUNT_OOS}")
    print(f"MIN_ACTIVE_MONTHS        : {MIN_ACTIVE_MONTHS}")
    print("=" * 100)

    (
        results_df,
        top_models_df,
        feature_usage_df,
        best_strategy_scores_df,
        feature_df,
        proxy_target,
    ) = run_search()

    save_outputs(
        results_df=results_df,
        top_models_df=top_models_df,
        feature_usage_df=feature_usage_df,
        best_strategy_scores_df=best_strategy_scores_df,
        feature_df=feature_df,
        proxy_target=proxy_target,
    )

    print("-" * 100)
    print(f"Search Results CSV       : {SEARCH_RESULTS_CSV}")
    print(f"Search Results JSON      : {SEARCH_RESULTS_JSON}")
    print(f"Best Model JSON          : {BEST_MODEL_JSON}")
    print(f"Best Strategy Scores CSV : {BEST_STRATEGY_SCORES_CSV}")
    print(f"Top Models CSV           : {TOP_MODELS_CSV}")
    print(f"Feature Usage CSV        : {FEATURE_USAGE_CSV}")

    if not results_df.empty:
        best = results_df.iloc[0]
        print("-" * 100)
        print("BEST MODEL")
        print(f"rank                    : {best['rank']}")
        print(f"objective               : {best['objective']}")
        print(f"spearman_vs_target      : {best['spearman_score_vs_target']}")
        print(f"top_bottom_spread       : {best['top_bottom_spread']}")
        print(f"top_bucket_target_mean  : {best['top_bucket_target_mean']}")
        print(f"n_features              : {best['n_features']}")
        print(f"max_weight              : {best['max_weight']}")
        print(f"weight_entropy          : {best['weight_entropy']}")
        print(f"effective_n_features    : {best['effective_n_features']}")
        print(f"features                : {best['features']}")
        print(f"weights                 : {best['weights']}")

    print("=" * 100)


if __name__ == "__main__":
    main()