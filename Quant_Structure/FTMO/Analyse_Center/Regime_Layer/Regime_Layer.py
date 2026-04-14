# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Analyse_Center/Regime_Layer/Regime_Layer.py

Zweck:
- Bewertet Strategien nach Marktregimen
- Verknüpft Trades mit OHLC-Regime-Features am Entry-Zeitpunkt
- Funktioniert auch für Strategien mit BUY, SELL oder BOTH
- Reiner Analyse-Layer, keine Eingriffe in Execution

Input:
- FTMO/Data_Center/Data/Trades/Featured/Backtest/IS_OOS_Enriched/**/*.csv
  alternativ:
- FTMO/Data_Center/Data/Trades/Feutered/Backtest/IS_OOS_Enriched/**/*.csv

- FTMO/Data_Center/Data/Ohcl/Feutured/H1/*.parquet
- FTMO/Data_Center/Data/Ohcl/Feutured/H4/*.parquet

Output:
- FTMO/Data_Center/Data/Analysis/Regime_Layer/
    - regime_layer_summary.csv
    - regime_layer_summary.json
    - strategy_json/<strategy_name>__regime.json
    - trade_regime_snapshots/<strategy_name>__trade_regimes.csv
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

OHLC_FEATURED_ROOT = (
    PROJECT_ROOT
    / "Data_Center"
    / "Data"
    / "Ohcl"
    / "Feutured"
)

OUTPUT_ROOT = (
    PROJECT_ROOT
    / "Data_Center"
    / "Data"
    / "Analysis"
    / "Regime_Layer"
)

STRATEGY_JSON_OUTPUT_ROOT = OUTPUT_ROOT / "strategy_json"
TRADE_REGIME_SNAPSHOT_ROOT = OUTPUT_ROOT / "trade_regime_snapshots"

SUMMARY_CSV_PATH = OUTPUT_ROOT / "regime_layer_summary.csv"
SUMMARY_JSON_PATH = OUTPUT_ROOT / "regime_layer_summary.json"


# ============================================================
# CONFIG
# ============================================================

REQUIRED_TRADE_COLUMNS = [
    "symbol",
    "open_time_utc",
    "close_time_utc",
    "net_sum",
]

H1_REGIME_FILE_REQUIRED_COLUMNS = [
    "time",
    "symbol",
    "volatility_regime",
    "liquidity_regime",
]

H4_REGIME_FILE_REQUIRED_COLUMNS = [
    "time",
    "symbol",
    "trend_regime",
]

SIDE_CANDIDATE_COLUMNS = [
    "profile_side",
    "side",
    "direction",
    "trade_side",
    "order_type",
]

DEFAULT_UNKNOWN = "UNKNOWN"


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


def compute_profit_factor(pnl: pd.Series) -> Optional[float]:
    x = pd.to_numeric(pnl, errors="coerce").dropna()
    if x.empty:
        return None

    gross_profit = float(x[x > 0].sum())
    gross_loss = float(x[x < 0].sum())

    if gross_loss == 0:
        if gross_profit > 0:
            return None
        return None

    return float(gross_profit / abs(gross_loss))


def compute_expectancy(pnl: pd.Series) -> Optional[float]:
    x = pd.to_numeric(pnl, errors="coerce").dropna()
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

    # fallback: strategy_id kann BUY/SELL enthalten
    if "strategy_id" in df.columns:
        sid = df["strategy_id"].astype(str).str.upper()
        out = pd.Series("", index=df.index, dtype="object")
        out.loc[sid.str.contains("_BUY", regex=False, na=False)] = "BUY"
        out.loc[sid.str.contains("_SELL", regex=False, na=False)] = "SELL"
        return out

    return pd.Series("", index=df.index, dtype="object")


def compute_session_regime(ts: pd.Series) -> pd.Series:
    ts = pd.to_datetime(ts, utc=True, errors="coerce")
    hour = ts.dt.hour

    return pd.Series(
        np.select(
            [
                hour.between(0, 6, inclusive="both"),
                hour.between(7, 12, inclusive="both"),
                hour.between(13, 16, inclusive="both"),
                hour.between(17, 21, inclusive="both"),
            ],
            ["ASIA", "LONDON", "NEW_YORK", "NY_PM"],
            default="OFF_HOURS",
        ),
        index=ts.index,
        dtype="object",
    )


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

    out = out.dropna(subset=["open_time_utc", "close_time_utc", "net_sum"]).copy()

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

    if "sample_type" in out.columns:
        out["sample_type"] = out["sample_type"].astype(str).str.upper().str.strip()
    else:
        out["sample_type"] = "UNKNOWN"

    out["session_regime_at_entry"] = compute_session_regime(out["open_time_utc"])

    out = out.sort_values(["strategy_id", "symbol", "open_time_utc", "close_time_utc"]).reset_index(drop=True)
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
# OHLC REGIME LOADING
# ============================================================

def load_h1_regimes(featured_root: Path) -> pd.DataFrame:
    h1_root = featured_root / "H1"
    if not h1_root.exists():
        raise RuntimeError(f"H1 OHLC-Feature-Ordner nicht gefunden: {h1_root}")

    files = sorted(h1_root.glob("*.parquet"))
    if not files:
        raise RuntimeError(f"Keine H1-Feature-Parquets gefunden unter: {h1_root}")

    parts: List[pd.DataFrame] = []

    for path in files:
        try:
            df = pd.read_parquet(path)
            cols_missing = [c for c in H1_REGIME_FILE_REQUIRED_COLUMNS if c not in df.columns]
            if cols_missing:
                print(f"[WARN] H1-Datei übersprungen, Spalten fehlen: {path} | {cols_missing}")
                continue

            out = df[["time", "symbol", "volatility_regime", "liquidity_regime"]].copy()
            out["time"] = pd.to_datetime(out["time"], utc=True, errors="coerce")
            out["symbol"] = out["symbol"].astype(str)
            out["volatility_regime"] = out["volatility_regime"].astype(str).fillna(DEFAULT_UNKNOWN)
            out["liquidity_regime"] = out["liquidity_regime"].astype(str).fillna(DEFAULT_UNKNOWN)

            out = out.dropna(subset=["time"]).sort_values(["symbol", "time"]).reset_index(drop=True)
            parts.append(out)

        except Exception as e:
            print(f"[WARN] H1-Datei übersprungen: {path} | {e}")

    if not parts:
        raise RuntimeError("Keine validen H1-Regime-Dateien geladen.")

    return pd.concat(parts, ignore_index=True, sort=False)


def load_h4_regimes(featured_root: Path) -> pd.DataFrame:
    h4_root = featured_root / "H4"
    if not h4_root.exists():
        raise RuntimeError(f"H4 OHLC-Feature-Ordner nicht gefunden: {h4_root}")

    files = sorted(h4_root.glob("*.parquet"))
    if not files:
        raise RuntimeError(f"Keine H4-Feature-Parquets gefunden unter: {h4_root}")

    parts: List[pd.DataFrame] = []

    for path in files:
        try:
            df = pd.read_parquet(path)
            cols_missing = [c for c in H4_REGIME_FILE_REQUIRED_COLUMNS if c not in df.columns]
            if cols_missing:
                print(f"[WARN] H4-Datei übersprungen, Spalten fehlen: {path} | {cols_missing}")
                continue

            out = df[["time", "symbol", "trend_regime"]].copy()
            out["time"] = pd.to_datetime(out["time"], utc=True, errors="coerce")
            out["symbol"] = out["symbol"].astype(str)
            out["trend_regime"] = out["trend_regime"].astype(str).fillna(DEFAULT_UNKNOWN)

            out = out.dropna(subset=["time"]).sort_values(["symbol", "time"]).reset_index(drop=True)
            parts.append(out)

        except Exception as e:
            print(f"[WARN] H4-Datei übersprungen: {path} | {e}")

    if not parts:
        raise RuntimeError("Keine validen H4-Regime-Dateien geladen.")

    return pd.concat(parts, ignore_index=True, sort=False)


# ============================================================
# REGIME JOIN
# ============================================================

def join_h1_regimes_to_trades(trades: pd.DataFrame, h1: pd.DataFrame) -> pd.DataFrame:
    left = trades.copy().sort_values(["symbol", "open_time_utc"]).reset_index(drop=True)
    right = h1.copy().sort_values(["symbol", "time"]).reset_index(drop=True)

    out_parts: List[pd.DataFrame] = []
    for symbol, sub_left in left.groupby("symbol", sort=False):
        sub_right = right[right["symbol"] == symbol].copy()
        if sub_right.empty:
            temp = sub_left.copy()
            temp["volatility_regime_at_entry"] = DEFAULT_UNKNOWN
            temp["liquidity_regime_at_entry"] = DEFAULT_UNKNOWN
            out_parts.append(temp)
            continue

        merged = pd.merge_asof(
            sub_left.sort_values("open_time_utc"),
            sub_right.sort_values("time"),
            left_on="open_time_utc",
            right_on="time",
            direction="backward",
        )

        merged["volatility_regime_at_entry"] = merged["volatility_regime"].fillna(DEFAULT_UNKNOWN).astype(str)
        merged["liquidity_regime_at_entry"] = merged["liquidity_regime"].fillna(DEFAULT_UNKNOWN).astype(str)

        drop_cols = ["time", "symbol_y", "volatility_regime", "liquidity_regime"]
        for c in drop_cols:
            if c in merged.columns:
                merged = merged.drop(columns=[c])

        if "symbol_x" in merged.columns:
            merged = merged.rename(columns={"symbol_x": "symbol"})

        out_parts.append(merged)

    out = pd.concat(out_parts, ignore_index=True, sort=False)
    return out


def join_h4_regimes_to_trades(trades: pd.DataFrame, h4: pd.DataFrame) -> pd.DataFrame:
    left = trades.copy().sort_values(["symbol", "open_time_utc"]).reset_index(drop=True)
    right = h4.copy().sort_values(["symbol", "time"]).reset_index(drop=True)

    out_parts: List[pd.DataFrame] = []
    for symbol, sub_left in left.groupby("symbol", sort=False):
        sub_right = right[right["symbol"] == symbol].copy()
        if sub_right.empty:
            temp = sub_left.copy()
            temp["trend_regime_at_entry"] = DEFAULT_UNKNOWN
            out_parts.append(temp)
            continue

        merged = pd.merge_asof(
            sub_left.sort_values("open_time_utc"),
            sub_right.sort_values("time"),
            left_on="open_time_utc",
            right_on="time",
            direction="backward",
        )

        merged["trend_regime_at_entry"] = merged["trend_regime"].fillna(DEFAULT_UNKNOWN).astype(str)

        drop_cols = ["time", "symbol_y", "trend_regime"]
        for c in drop_cols:
            if c in merged.columns:
                merged = merged.drop(columns=[c])

        if "symbol_x" in merged.columns:
            merged = merged.rename(columns={"symbol_x": "symbol"})

        out_parts.append(merged)

    out = pd.concat(out_parts, ignore_index=True, sort=False)
    return out


def attach_regimes(trades: pd.DataFrame, h1: pd.DataFrame, h4: pd.DataFrame) -> pd.DataFrame:
    out = join_h1_regimes_to_trades(trades, h1)
    out = join_h4_regimes_to_trades(out, h4)

    for col in [
        "session_regime_at_entry",
        "volatility_regime_at_entry",
        "liquidity_regime_at_entry",
        "trend_regime_at_entry",
    ]:
        if col not in out.columns:
            out[col] = DEFAULT_UNKNOWN
        out[col] = out[col].astype(str).replace("", DEFAULT_UNKNOWN).fillna(DEFAULT_UNKNOWN)

    out["composite_regime_at_entry"] = (
        out["session_regime_at_entry"].astype(str) + "|" +
        out["volatility_regime_at_entry"].astype(str) + "|" +
        out["trend_regime_at_entry"].astype(str)
    )

    out = out.sort_values(["strategy_id", "symbol", "open_time_utc"]).reset_index(drop=True)
    return out


# ============================================================
# METRICS
# ============================================================

def compute_group_metrics(sub: pd.DataFrame) -> Dict[str, object]:
    pnl = pd.to_numeric(sub["net_sum"], errors="coerce").dropna()

    return {
        "trade_count": int(len(sub)),
        "net_profit": float(pnl.sum()) if not pnl.empty else 0.0,
        "avg_trade": safe_mean(pnl),
        "median_trade": safe_median(pnl),
        "std_trade": safe_std(pnl),
        "win_rate": float((pnl > 0).mean()) if len(pnl) else None,
        "loss_rate": float((pnl < 0).mean()) if len(pnl) else None,
        "profit_factor": compute_profit_factor(pnl),
        "expectancy": compute_expectancy(pnl),
    }


def build_regime_table(
    df: pd.DataFrame,
    regime_col: str,
    side_filter: Optional[str] = None,
) -> Dict[str, Dict[str, object]]:
    if regime_col not in df.columns:
        return {}

    sub = df.copy()
    if side_filter is not None:
        sub = sub[sub["trade_side"] == side_filter].copy()

    out: Dict[str, Dict[str, object]] = {}
    for regime_value, grp in sub.groupby(regime_col, dropna=False):
        key = safe_text(regime_value) or DEFAULT_UNKNOWN
        out[key] = compute_group_metrics(grp)

    return out


def select_preferred_forbidden_regimes(regime_table: Dict[str, Dict[str, object]]) -> Tuple[List[str], List[str]]:
    preferred: List[str] = []
    forbidden: List[str] = []

    for regime_name, stats in regime_table.items():
        trade_count = stats.get("trade_count", 0) or 0
        pf = stats.get("profit_factor")
        expectancy = stats.get("expectancy")
        win_rate = stats.get("win_rate")

        if trade_count >= 15 and pf is not None and expectancy is not None:
            if pf >= 1.20 and expectancy > 0 and (win_rate is None or win_rate >= 0.45):
                preferred.append(regime_name)
            if pf < 1.0 or expectancy < 0:
                forbidden.append(regime_name)

    return sorted(preferred), sorted(forbidden)


def compute_regime_dependency_score(regime_table: Dict[str, Dict[str, object]]) -> Optional[float]:
    if not regime_table:
        return None

    profits = []
    for _, stats in regime_table.items():
        net = stats.get("net_profit")
        if net is not None and net > 0:
            profits.append(float(net))

    if not profits:
        return None

    total_profit = float(sum(profits))
    if total_profit <= 0:
        return None

    top_share = max(profits) / total_profit
    return float(top_share)


def compute_regime_confidence(regime_table: Dict[str, Dict[str, object]]) -> Optional[float]:
    if not regime_table:
        return None

    counts = [float(v.get("trade_count", 0) or 0) for v in regime_table.values()]
    total = float(sum(counts))
    if total <= 0:
        return None

    # einfacher Confidence-Proxy: steigt mit Sample Size und Verteilung
    enough = sum(c >= 15 for c in counts)
    ratio_enough = enough / max(1, len(counts))
    size_component = min(1.0, total / 200.0)

    return float(0.5 * ratio_enough + 0.5 * size_component)


@dataclass
class RegimeSummary:
    strategy_id: str
    symbol_mode: Optional[str]
    side_mode: Optional[str]
    side_mix: Optional[str]

    trade_count: int
    buy_trade_count: int
    sell_trade_count: int

    session_preferred_regimes: Optional[str]
    session_forbidden_regimes: Optional[str]
    session_regime_dependency_score: Optional[float]
    session_regime_confidence: Optional[float]

    volatility_preferred_regimes: Optional[str]
    volatility_forbidden_regimes: Optional[str]
    volatility_regime_dependency_score: Optional[float]
    volatility_regime_confidence: Optional[float]

    trend_preferred_regimes: Optional[str]
    trend_forbidden_regimes: Optional[str]
    trend_regime_dependency_score: Optional[float]
    trend_regime_confidence: Optional[float]

    composite_preferred_regimes: Optional[str]
    composite_forbidden_regimes: Optional[str]
    composite_regime_dependency_score: Optional[float]
    composite_regime_confidence: Optional[float]


def summarize_strategy_regimes(strategy_id: str, trades: pd.DataFrame) -> Tuple[RegimeSummary, Dict[str, object]]:
    df = trades.copy()

    # Seitenmix
    unique_sides = sorted([s for s in df["trade_side"].dropna().astype(str).unique() if s != "UNKNOWN"])
    if not unique_sides:
        side_mix = "UNKNOWN"
        side_mode = None
    elif len(unique_sides) == 1:
        side_mix = unique_sides[0]
        side_mode = unique_sides[0]
    else:
        side_mix = "BOTH"
        side_mode = "BOTH"

    session_table_all = build_regime_table(df, "session_regime_at_entry")
    volatility_table_all = build_regime_table(df, "volatility_regime_at_entry")
    trend_table_all = build_regime_table(df, "trend_regime_at_entry")
    composite_table_all = build_regime_table(df, "composite_regime_at_entry")

    session_pref, session_forb = select_preferred_forbidden_regimes(session_table_all)
    vol_pref, vol_forb = select_preferred_forbidden_regimes(volatility_table_all)
    trend_pref, trend_forb = select_preferred_forbidden_regimes(trend_table_all)
    comp_pref, comp_forb = select_preferred_forbidden_regimes(composite_table_all)

    payload = {
        "strategy_id": strategy_id,
        "summary": {},
        "overall": {
            "session_regimes": session_table_all,
            "volatility_regimes": volatility_table_all,
            "trend_regimes": trend_table_all,
            "composite_regimes": composite_table_all,
        },
        "by_side": {
            "BUY": {
                "session_regimes": build_regime_table(df, "session_regime_at_entry", side_filter="BUY"),
                "volatility_regimes": build_regime_table(df, "volatility_regime_at_entry", side_filter="BUY"),
                "trend_regimes": build_regime_table(df, "trend_regime_at_entry", side_filter="BUY"),
                "composite_regimes": build_regime_table(df, "composite_regime_at_entry", side_filter="BUY"),
            },
            "SELL": {
                "session_regimes": build_regime_table(df, "session_regime_at_entry", side_filter="SELL"),
                "volatility_regimes": build_regime_table(df, "volatility_regime_at_entry", side_filter="SELL"),
                "trend_regimes": build_regime_table(df, "trend_regime_at_entry", side_filter="SELL"),
                "composite_regimes": build_regime_table(df, "composite_regime_at_entry", side_filter="SELL"),
            },
        },
    }

    summary = RegimeSummary(
        strategy_id=strategy_id,
        symbol_mode=df["symbol"].mode().iloc[0] if "symbol" in df.columns and not df["symbol"].mode().empty else None,
        side_mode=side_mode,
        side_mix=side_mix,

        trade_count=int(len(df)),
        buy_trade_count=int((df["trade_side"] == "BUY").sum()),
        sell_trade_count=int((df["trade_side"] == "SELL").sum()),

        session_preferred_regimes="|".join(session_pref) if session_pref else None,
        session_forbidden_regimes="|".join(session_forb) if session_forb else None,
        session_regime_dependency_score=compute_regime_dependency_score(session_table_all),
        session_regime_confidence=compute_regime_confidence(session_table_all),

        volatility_preferred_regimes="|".join(vol_pref) if vol_pref else None,
        volatility_forbidden_regimes="|".join(vol_forb) if vol_forb else None,
        volatility_regime_dependency_score=compute_regime_dependency_score(volatility_table_all),
        volatility_regime_confidence=compute_regime_confidence(volatility_table_all),

        trend_preferred_regimes="|".join(trend_pref) if trend_pref else None,
        trend_forbidden_regimes="|".join(trend_forb) if trend_forb else None,
        trend_regime_dependency_score=compute_regime_dependency_score(trend_table_all),
        trend_regime_confidence=compute_regime_confidence(trend_table_all),

        composite_preferred_regimes="|".join(comp_pref) if comp_pref else None,
        composite_forbidden_regimes="|".join(comp_forb) if comp_forb else None,
        composite_regime_dependency_score=compute_regime_dependency_score(composite_table_all),
        composite_regime_confidence=compute_regime_confidence(composite_table_all),
    )

    payload["summary"] = {k: json_safe(v) for k, v in asdict(summary).items()}
    return summary, payload


# ============================================================
# WRITER
# ============================================================

def save_strategy_json(strategy_name: str, payload: Dict[str, object]) -> None:
    ensure_dir(STRATEGY_JSON_OUTPUT_ROOT)
    path = STRATEGY_JSON_OUTPUT_ROOT / f"{sanitize_name(strategy_name)}__regime.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def save_trade_snapshot(strategy_name: str, trades: pd.DataFrame) -> None:
    ensure_dir(TRADE_REGIME_SNAPSHOT_ROOT)

    keep_cols = [
        "strategy_id",
        "symbol",
        "trade_side",
        "sample_type",
        "open_time_utc",
        "close_time_utc",
        "net_sum",
        "session_regime_at_entry",
        "volatility_regime_at_entry",
        "liquidity_regime_at_entry",
        "trend_regime_at_entry",
        "composite_regime_at_entry",
    ]
    keep_cols = [c for c in keep_cols if c in trades.columns]

    path = TRADE_REGIME_SNAPSHOT_ROOT / f"{sanitize_name(strategy_name)}__trade_regimes.csv"
    trades[keep_cols].to_csv(path, index=False)


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
    print("REGIME LAYER")
    print("=" * 100)
    print(f"PROJECT_ROOT        : {PROJECT_ROOT}")
    print(f"TRADES_INPUT_ROOT   : {TRADES_INPUT_ROOT}")
    print(f"OHLC_FEATURED_ROOT  : {OHLC_FEATURED_ROOT}")
    print(f"OUTPUT_ROOT         : {OUTPUT_ROOT}")
    print("=" * 100)

    ensure_dir(OUTPUT_ROOT)
    ensure_dir(STRATEGY_JSON_OUTPUT_ROOT)
    ensure_dir(TRADE_REGIME_SNAPSHOT_ROOT)

    all_trades = load_all_trades(TRADES_INPUT_ROOT)
    h1_regimes = load_h1_regimes(OHLC_FEATURED_ROOT)
    h4_regimes = load_h4_regimes(OHLC_FEATURED_ROOT)

    all_trades = attach_regimes(all_trades, h1_regimes, h4_regimes)

    print(f"Loaded trades       : {len(all_trades)}")
    print(f"Unique strategy_ids : {all_trades['strategy_id'].nunique()}")
    print("-" * 100)

    outputs: List[Dict[str, object]] = []

    for strategy_id, trades in all_trades.groupby("strategy_id", dropna=False):
        strategy_id = safe_text(strategy_id)
        if not strategy_id:
            continue

        summary, payload = summarize_strategy_regimes(strategy_id, trades)
        summary_dict = {k: json_safe(v) for k, v in asdict(summary).items()}

        outputs.append(summary_dict)

        save_strategy_json(strategy_id, payload)
        save_trade_snapshot(strategy_id, trades)

        print(
            f"[OK] strategy_id={strategy_id} | "
            f"trades={summary_dict['trade_count']} | "
            f"side_mix={summary_dict['side_mix']} | "
            f"trend_dep={summary_dict['trend_regime_dependency_score']}"
        )

    if not outputs:
        raise RuntimeError("Es konnten keine Strategien verarbeitet werden.")

    save_summary(outputs)

    print("-" * 100)
    print(f"Summary CSV   : {SUMMARY_CSV_PATH}")
    print(f"Summary JSON  : {SUMMARY_JSON_PATH}")
    print(f"Strategy JSON : {STRATEGY_JSON_OUTPUT_ROOT}")
    print(f"Trade Snapshots: {TRADE_REGIME_SNAPSHOT_ROOT}")
    print("=" * 100)


if __name__ == "__main__":
    main()