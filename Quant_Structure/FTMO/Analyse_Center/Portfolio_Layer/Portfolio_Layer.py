# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Analyse_Center/Portfolio_Layer/Portfolio_Layer.py

Zweck:
- Bewertet jede Strategie im Kontext aller anderen Strategien
- Misst Diversifikation, Korrelationsrisiko und Stress-Überlappung
- Nutzt tägliche Strategy-PnL-Zeitreihen als Portfolio-Proxy
- Funktioniert auch mit BUY / SELL / BOTH Strategien

Input:
- FTMO/Data_Center/Data/Trades/Featured/Backtest/IS_OOS_Enriched/**/*.csv
  alternativ:
- FTMO/Data_Center/Data/Trades/Feutered/Backtest/IS_OOS_Enriched/**/*.csv

Output:
- FTMO/Data_Center/Data/Analysis/Portfolio_Layer/
    - portfolio_layer_summary.csv
    - portfolio_layer_summary.json
    - strategy_json/<strategy_name>__portfolio_fit.json
    - matrices/return_correlation.csv
    - matrices/drawdown_correlation.csv
    - matrices/stress_overlap.csv
    - matrices/symbol_overlap.csv
    - matrices/side_overlap.csv
    - matrices/portfolio_daily_pnl.csv
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

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

OUTPUT_ROOT = (
    PROJECT_ROOT
    / "Data_Center"
    / "Data"
    / "Analysis"
    / "Portfolio_Layer"
)

STRATEGY_JSON_OUTPUT_ROOT = OUTPUT_ROOT / "strategy_json"
MATRICES_OUTPUT_ROOT = OUTPUT_ROOT / "matrices"

SUMMARY_CSV_PATH = OUTPUT_ROOT / "portfolio_layer_summary.csv"
SUMMARY_JSON_PATH = OUTPUT_ROOT / "portfolio_layer_summary.json"


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

STRESS_QUANTILE = 0.10
CLUSTER_CORR_THRESHOLD = 0.50


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


def jaccard_similarity(a: Set[str], b: Set[str]) -> Optional[float]:
    if not a and not b:
        return None
    union = a | b
    if not union:
        return None
    return float(len(a & b) / len(union))


def compute_max_drawdown_from_daily_returns(daily_pnl: pd.Series) -> Optional[float]:
    s = pd.to_numeric(daily_pnl, errors="coerce").fillna(0.0)
    if s.empty:
        return None
    eq = s.cumsum()
    peak = eq.cummax()
    dd = eq - peak
    return float(dd.min()) if not dd.empty else None


def percentile_safe(values: List[float], q: float) -> Optional[float]:
    s = pd.Series(values, dtype="float64").dropna()
    if s.empty:
        return None
    return float(np.percentile(s, q))


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
# DAILY PORTFOLIO TABLES
# ============================================================

def build_daily_pnl_matrix(trades: pd.DataFrame) -> pd.DataFrame:
    daily = (
        trades.groupby(["trade_date_utc", "strategy_id"], dropna=False)["net_sum"]
        .sum()
        .reset_index()
    )

    mat = daily.pivot(index="trade_date_utc", columns="strategy_id", values="net_sum").sort_index()
    mat = mat.fillna(0.0)
    return mat


def build_daily_drawdown_matrix(daily_pnl: pd.DataFrame) -> pd.DataFrame:
    eq = daily_pnl.cumsum()
    peak = eq.cummax()
    dd = eq - peak
    return dd


def build_return_correlation_matrix(daily_pnl: pd.DataFrame) -> pd.DataFrame:
    if daily_pnl.empty:
        return pd.DataFrame()
    return daily_pnl.corr()


def build_drawdown_correlation_matrix(daily_drawdown: pd.DataFrame) -> pd.DataFrame:
    if daily_drawdown.empty:
        return pd.DataFrame()
    return daily_drawdown.corr()


def build_stress_overlap_matrix(daily_pnl: pd.DataFrame, q: float = STRESS_QUANTILE) -> pd.DataFrame:
    strategies = list(daily_pnl.columns)
    out = pd.DataFrame(index=strategies, columns=strategies, dtype="float64")

    stress_days: Dict[str, Set[pd.Timestamp]] = {}
    for s in strategies:
        x = pd.to_numeric(daily_pnl[s], errors="coerce").dropna()
        if x.empty:
            stress_days[s] = set()
            continue
        cutoff = float(x.quantile(q))
        stress_days[s] = set(x[x <= cutoff].index)

    for a in strategies:
        for b in strategies:
            if a == b:
                out.loc[a, b] = 1.0
            else:
                out.loc[a, b] = jaccard_similarity(stress_days[a], stress_days[b])

    return out


def build_symbol_overlap_matrix(trades: pd.DataFrame) -> pd.DataFrame:
    strategies = sorted(trades["strategy_id"].dropna().astype(str).unique().tolist())
    out = pd.DataFrame(index=strategies, columns=strategies, dtype="float64")

    symbol_sets = {
        s: set(trades.loc[trades["strategy_id"] == s, "symbol"].dropna().astype(str).unique().tolist())
        for s in strategies
    }

    for a in strategies:
        for b in strategies:
            if a == b:
                out.loc[a, b] = 1.0
            else:
                out.loc[a, b] = jaccard_similarity(symbol_sets[a], symbol_sets[b])

    return out


def build_side_overlap_matrix(trades: pd.DataFrame) -> pd.DataFrame:
    strategies = sorted(trades["strategy_id"].dropna().astype(str).unique().tolist())
    out = pd.DataFrame(index=strategies, columns=strategies, dtype="float64")

    side_sets = {
        s: set(trades.loc[trades["strategy_id"] == s, "trade_side"].dropna().astype(str).unique().tolist())
        for s in strategies
    }

    for a in strategies:
        for b in strategies:
            if a == b:
                out.loc[a, b] = 1.0
            else:
                out.loc[a, b] = jaccard_similarity(side_sets[a], side_sets[b])

    return out


# ============================================================
# CLUSTERING
# ============================================================

def build_abs_corr_graph(return_corr: pd.DataFrame, threshold: float = CLUSTER_CORR_THRESHOLD) -> Dict[str, Set[str]]:
    strategies = list(return_corr.index)
    graph: Dict[str, Set[str]] = {s: set() for s in strategies}

    for a in strategies:
        for b in strategies:
            if a == b:
                continue
            v = return_corr.loc[a, b]
            if pd.notna(v) and abs(float(v)) >= threshold:
                graph[a].add(b)
                graph[b].add(a)

    return graph


def connected_components(graph: Dict[str, Set[str]]) -> Dict[str, str]:
    visited = set()
    cluster_map: Dict[str, str] = {}
    cid = 1

    for node in graph.keys():
        if node in visited:
            continue

        stack = [node]
        comp = []

        while stack:
            cur = stack.pop()
            if cur in visited:
                continue
            visited.add(cur)
            comp.append(cur)
            for nxt in graph.get(cur, set()):
                if nxt not in visited:
                    stack.append(nxt)

        label = f"CLUSTER_{cid:02d}"
        for n in comp:
            cluster_map[n] = label
        cid += 1

    return cluster_map


# ============================================================
# PER-STRATEGY METRICS
# ============================================================

@dataclass
class PortfolioMetrics:
    strategy_id: str
    symbol_count: int
    side_mix: Optional[str]
    trade_count: int

    avg_return_correlation: Optional[float]
    max_return_correlation: Optional[float]

    avg_drawdown_correlation: Optional[float]
    max_drawdown_correlation: Optional[float]

    avg_stress_overlap: Optional[float]
    max_stress_overlap: Optional[float]

    avg_symbol_overlap: Optional[float]
    max_symbol_overlap: Optional[float]

    avg_side_overlap: Optional[float]
    max_side_overlap: Optional[float]

    correlation_penalty: Optional[float]
    marginal_dd_contribution: Optional[float]

    diversification_score: Optional[float]
    portfolio_fit_score: Optional[float]

    cluster_membership: Optional[str]


def summarize_side_mix(sub: pd.DataFrame) -> Optional[str]:
    sides = sorted([x for x in sub["trade_side"].dropna().astype(str).unique().tolist() if x != "UNKNOWN"])
    if not sides:
        return None
    if len(sides) == 1:
        return sides[0]
    return "BOTH"


def non_self_stats(mat: pd.DataFrame, strategy_id: str) -> Tuple[Optional[float], Optional[float]]:
    if strategy_id not in mat.index:
        return None, None

    x = pd.to_numeric(mat.loc[strategy_id], errors="coerce")
    x = x.drop(labels=[strategy_id], errors="ignore").dropna()
    if x.empty:
        return None, None

    return float(x.mean()), float(x.max())


def compute_marginal_dd_contribution(daily_pnl: pd.DataFrame, strategy_id: str) -> Optional[float]:
    if strategy_id not in daily_pnl.columns:
        return None

    cols_all = list(daily_pnl.columns)
    if len(cols_all) <= 1:
        return None

    portfolio_with = daily_pnl.mean(axis=1)
    dd_with = compute_max_drawdown_from_daily_returns(portfolio_with)

    cols_without = [c for c in cols_all if c != strategy_id]
    portfolio_without = daily_pnl[cols_without].mean(axis=1)
    dd_without = compute_max_drawdown_from_daily_returns(portfolio_without)

    if dd_with is None or dd_without is None:
        return None

    # negativer Wert = Strategie verschlechtert DD
    # positiver Wert = Strategie verbessert DD
    return float(abs(dd_without) - abs(dd_with))


def compute_strategy_portfolio_metrics(
    strategy_id: str,
    trades: pd.DataFrame,
    daily_pnl: pd.DataFrame,
    return_corr: pd.DataFrame,
    drawdown_corr: pd.DataFrame,
    stress_overlap: pd.DataFrame,
    symbol_overlap: pd.DataFrame,
    side_overlap: pd.DataFrame,
    cluster_map: Dict[str, str],
) -> PortfolioMetrics:
    sub = trades[trades["strategy_id"] == strategy_id].copy()

    avg_return_corr, max_return_corr = non_self_stats(return_corr, strategy_id)
    avg_dd_corr, max_dd_corr = non_self_stats(drawdown_corr, strategy_id)
    avg_stress, max_stress = non_self_stats(stress_overlap, strategy_id)
    avg_symbol, max_symbol = non_self_stats(symbol_overlap, strategy_id)
    avg_side, max_side = non_self_stats(side_overlap, strategy_id)

    corr_penalty = None
    if avg_return_corr is not None and avg_dd_corr is not None:
        corr_penalty = float(0.5 * abs(avg_return_corr) + 0.5 * abs(avg_dd_corr))

    marginal_dd = compute_marginal_dd_contribution(daily_pnl, strategy_id)

    corr_component = scale_0_1(abs(avg_return_corr) if avg_return_corr is not None else None, 0.0, 0.75, reverse=True)
    stress_component = scale_0_1(avg_stress, 0.0, 0.75, reverse=True)
    symbol_component = scale_0_1(avg_symbol, 0.0, 1.0, reverse=True)

    diversification_parts = [x for x in [corr_component, stress_component, symbol_component] if x is not None]
    diversification_score = float(np.mean(diversification_parts)) if diversification_parts else None

    dd_component = None
    if marginal_dd is not None:
        # -5000 schlecht, +5000 gut als grobe Skala
        dd_component = scale_0_1(marginal_dd, -5000.0, 5000.0, reverse=False)

    fit_parts = [x for x in [diversification_score, dd_component] if x is not None]
    portfolio_fit_score = float(np.mean(fit_parts)) if fit_parts else diversification_score

    return PortfolioMetrics(
        strategy_id=strategy_id,
        symbol_count=int(sub["symbol"].dropna().astype(str).nunique()),
        side_mix=summarize_side_mix(sub),
        trade_count=int(len(sub)),

        avg_return_correlation=avg_return_corr,
        max_return_correlation=max_return_corr,

        avg_drawdown_correlation=avg_dd_corr,
        max_drawdown_correlation=max_dd_corr,

        avg_stress_overlap=avg_stress,
        max_stress_overlap=max_stress,

        avg_symbol_overlap=avg_symbol,
        max_symbol_overlap=max_symbol,

        avg_side_overlap=avg_side,
        max_side_overlap=max_side,

        correlation_penalty=corr_penalty,
        marginal_dd_contribution=marginal_dd,

        diversification_score=diversification_score,
        portfolio_fit_score=portfolio_fit_score,

        cluster_membership=cluster_map.get(strategy_id),
    )


# ============================================================
# WRITER
# ============================================================

def save_matrix(df: pd.DataFrame, name: str) -> None:
    ensure_dir(MATRICES_OUTPUT_ROOT)
    path = MATRICES_OUTPUT_ROOT / name
    df.to_csv(path, index=True)


def save_strategy_json(strategy_name: str, payload: Dict[str, object]) -> None:
    ensure_dir(STRATEGY_JSON_OUTPUT_ROOT)
    path = STRATEGY_JSON_OUTPUT_ROOT / f"{sanitize_name(strategy_name)}__portfolio_fit.json"
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
    print("PORTFOLIO LAYER")
    print("=" * 100)
    print(f"PROJECT_ROOT      : {PROJECT_ROOT}")
    print(f"TRADES_INPUT_ROOT : {TRADES_INPUT_ROOT}")
    print(f"OUTPUT_ROOT       : {OUTPUT_ROOT}")
    print("=" * 100)

    ensure_dir(OUTPUT_ROOT)
    ensure_dir(STRATEGY_JSON_OUTPUT_ROOT)
    ensure_dir(MATRICES_OUTPUT_ROOT)

    all_trades = load_all_trades(TRADES_INPUT_ROOT)

    print(f"Loaded trades       : {len(all_trades)}")
    print(f"Unique strategy_ids : {all_trades['strategy_id'].nunique()}")
    print("-" * 100)

    daily_pnl = build_daily_pnl_matrix(all_trades)
    daily_drawdown = build_daily_drawdown_matrix(daily_pnl)

    return_corr = build_return_correlation_matrix(daily_pnl)
    drawdown_corr = build_drawdown_correlation_matrix(daily_drawdown)
    stress_overlap = build_stress_overlap_matrix(daily_pnl, q=STRESS_QUANTILE)
    symbol_overlap = build_symbol_overlap_matrix(all_trades)
    side_overlap = build_side_overlap_matrix(all_trades)

    graph = build_abs_corr_graph(return_corr, threshold=CLUSTER_CORR_THRESHOLD)
    cluster_map = connected_components(graph)

    save_matrix(return_corr, "return_correlation.csv")
    save_matrix(drawdown_corr, "drawdown_correlation.csv")
    save_matrix(stress_overlap, "stress_overlap.csv")
    save_matrix(symbol_overlap, "symbol_overlap.csv")
    save_matrix(side_overlap, "side_overlap.csv")
    save_matrix(daily_pnl, "portfolio_daily_pnl.csv")

    outputs: List[Dict[str, object]] = []

    for strategy_id in sorted(all_trades["strategy_id"].dropna().astype(str).unique().tolist()):
        metrics = compute_strategy_portfolio_metrics(
            strategy_id=strategy_id,
            trades=all_trades,
            daily_pnl=daily_pnl,
            return_corr=return_corr,
            drawdown_corr=drawdown_corr,
            stress_overlap=stress_overlap,
            symbol_overlap=symbol_overlap,
            side_overlap=side_overlap,
            cluster_map=cluster_map,
        )

        summary_dict = {k: json_safe(v) for k, v in asdict(metrics).items()}
        outputs.append(summary_dict)

        payload = {
            "strategy_id": strategy_id,
            "summary": summary_dict,
            "peer_snapshot": {
                "return_correlation_row": {
                    k: json_safe(v)
                    for k, v in return_corr.loc[strategy_id].to_dict().items()
                } if strategy_id in return_corr.index else {},
                "drawdown_correlation_row": {
                    k: json_safe(v)
                    for k, v in drawdown_corr.loc[strategy_id].to_dict().items()
                } if strategy_id in drawdown_corr.index else {},
                "stress_overlap_row": {
                    k: json_safe(v)
                    for k, v in stress_overlap.loc[strategy_id].to_dict().items()
                } if strategy_id in stress_overlap.index else {},
                "symbol_overlap_row": {
                    k: json_safe(v)
                    for k, v in symbol_overlap.loc[strategy_id].to_dict().items()
                } if strategy_id in symbol_overlap.index else {},
                "side_overlap_row": {
                    k: json_safe(v)
                    for k, v in side_overlap.loc[strategy_id].to_dict().items()
                } if strategy_id in side_overlap.index else {},
            },
        }

        save_strategy_json(strategy_id, payload)

        print(
            f"[OK] strategy_id={strategy_id} | "
            f"fit={summary_dict['portfolio_fit_score']} | "
            f"div={summary_dict['diversification_score']} | "
            f"cluster={summary_dict['cluster_membership']}"
        )

    if not outputs:
        raise RuntimeError("Es konnten keine Strategien verarbeitet werden.")

    save_summary(outputs)

    print("-" * 100)
    print(f"Summary CSV    : {SUMMARY_CSV_PATH}")
    print(f"Summary JSON   : {SUMMARY_JSON_PATH}")
    print(f"Strategy JSON  : {STRATEGY_JSON_OUTPUT_ROOT}")
    print(f"Matrices       : {MATRICES_OUTPUT_ROOT}")
    print("=" * 100)


if __name__ == "__main__":
    main()
    