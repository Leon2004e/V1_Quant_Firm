# -*- coding: utf-8 -*-
"""
Dashboards/Main_Board/pages/Performance/Analytics_dashboard.py

Performance Analytics Dashboard V2
- kompatibel mit Main Board Loader
- kein pandas erforderlich
- kein matplotlib erforderlich
- dunkler Stil passend zum Main Board
- standalone nutzbar
- als eingebettetes Panel nutzbar
- liest Performance CSV-Dateien
- Fokus auf Performance Analytics, nicht nur Tabellen
- KPI, Ranking, Distribution, Scatter, Compare, Feature Usage, Top Models
- Canvas-basierte Charts
- Sortieren per Spaltenkopf
- Filterzeile direkt über der Tabelle

Liest:
- FTMO/Data_Center/Data/Analysis/Performance/performance_features.csv
- FTMO/Data_Center/Data/Analysis/Performance/best_candidate_strategy_scores.csv
- optional:
    - FTMO/Data_Center/Data/Analysis/Performance/feature_usage_top_models.csv
    - FTMO/Data_Center/Data/Analysis/Performance/top_models.csv

Start standalone:
    python Quant_Structure/FTMO/Dashboards/Main_Board/pages/Performance/Analytics_dashboard.py
"""

from __future__ import annotations

import csv
import math
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk


# ============================================================
# PATHS
# ============================================================

def find_ftmo_root(start: Path) -> Path:
    cur = start.resolve()
    for p in [cur] + list(cur.parents):
        if (p / "Data_Center").exists() and (p / "Dashboards").exists():
            return p
    raise RuntimeError(
        f"FTMO-Root nicht gefunden. Erwartet Root mit 'Data_Center' und 'Dashboards'. Start={start}"
    )


SCRIPT_PATH = Path(__file__).resolve()
FTMO_ROOT = find_ftmo_root(SCRIPT_PATH)

PERFORMANCE_ROOT = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Analysis"
    / "Performance"
)

PERFORMANCE_FEATURES_CSV = PERFORMANCE_ROOT / "performance_features.csv"
BEST_STRATEGY_SCORES_CSV = PERFORMANCE_ROOT / "best_candidate_strategy_scores.csv"
FEATURE_USAGE_CSV = PERFORMANCE_ROOT / "feature_usage_top_models.csv"
TOP_MODELS_CSV = PERFORMANCE_ROOT / "top_models.csv"


# ============================================================
# CONFIG
# ============================================================

APP_TITLE = "Performance Analytics"
AUTO_REFRESH_MS = 5000
AUTO_REFRESH_DEFAULT = True

TABLE_ROW_LIMIT = 5000
SUMMARY_ROW_LIMIT = 500
FEATURE_USAGE_LIMIT = 200
TOP_MODEL_LIMIT = 100
CHART_MAX_SYMBOLS = 12

SCATTER_POINT_RADIUS = 4
HIST_BINS = 14

COMPARE_SLOT_COUNT = 3


# ============================================================
# MAIN BOARD STYLE
# ============================================================

BG_MAIN = "#0A1118"
BG_TOP = "#0E1721"
BG_SURFACE = "#101B27"
BG_CARD = "#142131"
BG_CARD_HOVER = "#1A2A3D"
BG_PANEL = BG_SURFACE
BG_PANEL_2 = "#132130"
BG_HEADER = BG_TOP
BG_BUTTON = "#2563EB"
BG_BUTTON_HOVER = "#3B82F6"
BG_BUTTON_SECONDARY = "#223246"

FG_MAIN = "#EAF2F9"
FG_MUTED = "#93A4B5"
FG_SUBTLE = "#708396"
FG_WHITE = "#FFFFFF"
FG_ACCENT = "#60A5FA"
FG_POS = "#22C55E"
FG_WARN = "#F59E0B"
FG_NEG = "#EF4444"

BORDER = "#223244"
DIVIDER = "#1B2A39"

FONT_TITLE = ("Segoe UI", 17, "bold")
FONT_SECTION = ("Segoe UI", 11, "bold")
FONT_LABEL = ("Segoe UI", 9)
FONT_VALUE = ("Segoe UI", 12, "bold")
FONT_TEXT = ("Segoe UI", 9)
FONT_MONO = ("Consolas", 10)
FONT_SMALL = ("Segoe UI", 8)

CHART_COLORS = {
    "score": "#60A5FA",
    "pf": "#22C55E",
    "expectancy": "#F59E0B",
    "win_rate": "#A78BFA",
    "payoff": "#F97316",
    "net_profit": "#2DD4BF",
    "grid": DIVIDER,
    "axis": BORDER,
    "point": "#60A5FA",
    "point_selected": "#F59E0B",
    "point_alt": "#22C55E",
    "bar": "#60A5FA",
    "bar_alt": "#22C55E",
    "bar_warn": "#F59E0B",
    "bar_neg": "#EF4444",
}


# ============================================================
# HELPERS
# ============================================================

def safe_text(x: object) -> str:
    if x is None:
        return ""
    try:
        s = str(x).strip()
    except Exception:
        return ""
    if s.lower() == "nan":
        return ""
    return s


def to_float(x: Any) -> Optional[float]:
    try:
        if x in (None, ""):
            return None
        return float(x)
    except Exception:
        return None


def to_int(x: Any) -> Optional[int]:
    try:
        if x in (None, ""):
            return None
        return int(float(x))
    except Exception:
        return None


def fmt_int(x: object) -> str:
    try:
        return f"{int(float(x)):,}"
    except Exception:
        return "-"


def fmt_float(x: object, ndigits: int = 2) -> str:
    try:
        v = float(x)
        return f"{v:,.{ndigits}f}"
    except Exception:
        return "-"


def fmt_pct_from_ratio(x: object, ndigits: int = 2) -> str:
    try:
        v = float(x) * 100.0
        return f"{v:.{ndigits}f}%"
    except Exception:
        return "-"


def unique_sorted(values: List[str], with_all: bool = True) -> List[str]:
    cleaned = sorted({safe_text(v) for v in values if safe_text(v)})
    return (["ALL"] + cleaned) if with_all else cleaned


def make_tree_iid(prefix: str, row_index: int, key: str = "") -> str:
    clean_key = (
        safe_text(key)
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
    )
    return f"{prefix}__{row_index}__{clean_key}"


def contains_ci(value: Any, needle: str) -> bool:
    return needle in safe_text(value).lower()


def infer_symbol(strategy_id: str) -> str:
    s = safe_text(strategy_id)
    if not s:
        return "UNKNOWN"
    parts = s.split("_")
    return safe_text(parts[0]).upper() if parts else "UNKNOWN"


def infer_side(strategy_id: str) -> str:
    s = safe_text(strategy_id).upper()
    if "_BUY_" in s:
        return "BUY"
    if "_SELL_" in s:
        return "SELL"
    if "_BOTH_" in s:
        return "BOTH"
    return "UNKNOWN"


def infer_timeframe(strategy_id: str) -> str:
    s = safe_text(strategy_id).upper()
    parts = s.split("_")
    for part in reversed(parts):
        if part.startswith("M") or part.startswith("H") or part.startswith("D") or part.startswith("W"):
            return part
    return "UNKNOWN"


def merge_dicts(base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    out.update(extra)
    return out


def safe_csv_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if isinstance(row, dict):
                    rows.append(dict(row))
    except Exception:
        return []
    return rows


def mean_ignore_none(values: List[Optional[float]]) -> Optional[float]:
    vals = [x for x in values if x is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def median_ignore_none(values: List[Optional[float]]) -> Optional[float]:
    vals = sorted([x for x in values if x is not None])
    if not vals:
        return None
    return float(median(vals))


def percentile_sorted(sorted_vals: List[float], q: float) -> Optional[float]:
    if not sorted_vals:
        return None
    if q <= 0:
        return sorted_vals[0]
    if q >= 1:
        return sorted_vals[-1]
    pos = (len(sorted_vals) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_vals[lo]
    frac = pos - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac


def percentile_ignore_none(values: List[Optional[float]], q: float) -> Optional[float]:
    vals = sorted([x for x in values if x is not None])
    return percentile_sorted(vals, q)


def zfill_none(v: Optional[float], fallback: float = 0.0) -> float:
    return fallback if v is None else float(v)


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def ratio_color(v: Optional[float], good: float, warn: float) -> str:
    if v is None:
        return FG_MUTED
    if v >= good:
        return FG_POS
    if v >= warn:
        return FG_WARN
    return FG_NEG


def sign_color(v: Optional[float]) -> str:
    if v is None:
        return FG_MUTED
    if v > 0:
        return FG_POS
    if v < 0:
        return FG_NEG
    return FG_MUTED


def parse_feature_weights(features: str, weights: str) -> List[Tuple[str, float]]:
    feat_list = [safe_text(x) for x in safe_text(features).split("|") if safe_text(x)]
    weight_list: List[float] = []
    for raw in safe_text(weights).split("|"):
        val = to_float(raw)
        if val is not None:
            weight_list.append(val)

    out: List[Tuple[str, float]] = []
    for idx, feat in enumerate(feat_list):
        w = weight_list[idx] if idx < len(weight_list) else 0.0
        out.append((feat, w))
    out.sort(key=lambda x: -x[1])
    return out


def pretty_feature_name(name: str) -> str:
    s = safe_text(name)
    if s.startswith("rank__"):
        s = s[6:]
    return s


# ============================================================
# REPOSITORY
# ============================================================

class PerformanceRepository:
    def __init__(self, root: Path):
        self.root = root

    def scan(self) -> Dict[str, List[Dict[str, Any]]]:
        performance_rows = safe_csv_rows(PERFORMANCE_FEATURES_CSV)
        best_score_rows = safe_csv_rows(BEST_STRATEGY_SCORES_CSV)
        feature_usage_rows = safe_csv_rows(FEATURE_USAGE_CSV)
        top_models_rows = safe_csv_rows(TOP_MODELS_CSV)

        perf_map: Dict[str, Dict[str, Any]] = {}
        for row in performance_rows:
            strategy_id = safe_text(row.get("strategy_id"))
            if not strategy_id:
                continue

            clean = {
                "strategy_id": strategy_id,
                "symbol": infer_symbol(strategy_id),
                "side": infer_side(strategy_id),
                "timeframe": infer_timeframe(strategy_id),

                "trade_count_total": to_float(row.get("trade_count_total")),
                "trade_count_is": to_float(row.get("trade_count_is")),
                "trade_count_oos": to_float(row.get("trade_count_oos")),
                "active_months": to_float(row.get("active_months")),
                "trades_per_month": to_float(row.get("trades_per_month")),

                "net_profit_total": to_float(row.get("net_profit_total")),
                "net_profit_is": to_float(row.get("net_profit_is")),
                "net_profit_oos": to_float(row.get("net_profit_oos")),

                "win_rate_total": to_float(row.get("win_rate_total")),
                "win_rate_is": to_float(row.get("win_rate_is")),
                "win_rate_oos": to_float(row.get("win_rate_oos")),

                "avg_win_total": to_float(row.get("avg_win_total")),
                "avg_loss_total": to_float(row.get("avg_loss_total")),
                "payoff_ratio_total": to_float(row.get("payoff_ratio_total")),

                "expectancy_total": to_float(row.get("expectancy_total")),
                "expectancy_is": to_float(row.get("expectancy_is")),
                "expectancy_oos": to_float(row.get("expectancy_oos")),

                "profit_factor_total": to_float(row.get("profit_factor_total")),
                "profit_factor_is": to_float(row.get("profit_factor_is")),
                "profit_factor_oos": to_float(row.get("profit_factor_oos")),

                "first_trade_time_utc": safe_text(row.get("first_trade_time_utc")),
                "last_trade_time_utc": safe_text(row.get("last_trade_time_utc")),
            }
            clean["row_key"] = strategy_id
            perf_map[strategy_id] = clean

        score_map: Dict[str, Dict[str, Any]] = {}
        for row in best_score_rows:
            strategy_id = safe_text(row.get("strategy_id"))
            if not strategy_id:
                continue

            clean = {
                "strategy_id": strategy_id,
                "symbol": infer_symbol(strategy_id),
                "side": infer_side(strategy_id),
                "timeframe": infer_timeframe(strategy_id),
                "best_candidate_score": to_float(row.get("best_candidate_score")),
                "proxy_target": to_float(row.get("proxy_target")),
            }
            score_map[strategy_id] = clean

        all_ids = sorted(set(perf_map.keys()) | set(score_map.keys()))
        merged_rows: List[Dict[str, Any]] = []

        for strategy_id in all_ids:
            base = perf_map.get(strategy_id, {
                "strategy_id": strategy_id,
                "symbol": infer_symbol(strategy_id),
                "side": infer_side(strategy_id),
                "timeframe": infer_timeframe(strategy_id),
                "row_key": strategy_id,
            })
            extra = score_map.get(strategy_id, {})
            row = merge_dicts(base, extra)

            row["strategy_uid"] = f"{row.get('symbol', 'UNKNOWN')}__{strategy_id}"
            row["archetype"] = self._infer_archetype(row)
            merged_rows.append(row)

        merged_rows.sort(
            key=lambda r: (
                -(to_float(r.get("best_candidate_score")) if to_float(r.get("best_candidate_score")) is not None else -1e18),
                -(to_float(r.get("profit_factor_oos")) if to_float(r.get("profit_factor_oos")) is not None else -1e18),
                safe_text(r.get("strategy_id")),
            )
        )

        clean_feature_usage: List[Dict[str, Any]] = []
        for row in feature_usage_rows:
            clean_feature_usage.append({
                "feature": safe_text(row.get("feature")),
                "count": to_int(row.get("count")) or 0,
                "share_in_top_k": to_float(row.get("share_in_top_k")) or 0.0,
            })

        clean_top_models: List[Dict[str, Any]] = []
        for row in top_models_rows:
            clean_top_models.append({
                "rank": to_int(row.get("rank")),
                "objective": to_float(row.get("objective")),
                "spearman_score_vs_target": to_float(row.get("spearman_score_vs_target")),
                "top_bottom_spread": to_float(row.get("top_bottom_spread")),
                "top_bucket_target_mean": to_float(row.get("top_bucket_target_mean")),
                "n_features": to_int(row.get("n_features")),
                "features": safe_text(row.get("features")),
                "weights": safe_text(row.get("weights")),
            })

        return {
            "performance_rows": merged_rows,
            "feature_usage_rows": clean_feature_usage,
            "top_models_rows": clean_top_models,
        }

    @staticmethod
    def _infer_archetype(row: Dict[str, Any]) -> str:
        win = to_float(row.get("win_rate_oos"))
        payoff = to_float(row.get("payoff_ratio_total"))
        trades = to_float(row.get("trade_count_oos"))
        exp = to_float(row.get("expectancy_oos"))

        if win is None or payoff is None:
            return "UNKNOWN"

        if trades is not None and trades >= 400 and (exp is not None and exp < 5):
            return "HIGH_ACTIVITY_SMALL_EDGE"

        if win >= 0.62 and payoff < 0.8:
            return "HIGH_WIN_LOW_PAYOFF"

        if win <= 0.48 and payoff >= 1.25:
            return "LOW_WIN_HIGH_PAYOFF"

        if 0.48 < win < 0.62 and 0.85 <= payoff <= 1.25:
            return "BALANCED"

        if trades is not None and trades < 120 and exp is not None and exp >= 8:
            return "LOW_ACTIVITY_LARGE_EDGE"

        return "MIXED"


# ============================================================
# AGGREGATIONS
# ============================================================

def build_symbol_summary(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[safe_text(row.get("symbol"))].append(row)

    out: List[Dict[str, Any]] = []
    for symbol, items in grouped.items():
        score_vals = [to_float(x.get("best_candidate_score")) for x in items]
        pf_vals = [to_float(x.get("profit_factor_oos")) for x in items]
        exp_vals = [to_float(x.get("expectancy_oos")) for x in items]
        np_vals = [to_float(x.get("net_profit_oos")) for x in items]

        out.append({
            "symbol": symbol,
            "count": len(items),
            "buy": sum(1 for x in items if safe_text(x.get("side")) == "BUY"),
            "sell": sum(1 for x in items if safe_text(x.get("side")) == "SELL"),
            "both": sum(1 for x in items if safe_text(x.get("side")) == "BOTH"),
            "avg_score": mean_ignore_none(score_vals),
            "avg_pf_oos": mean_ignore_none(pf_vals),
            "avg_expectancy_oos": mean_ignore_none(exp_vals),
            "avg_net_profit_oos": mean_ignore_none(np_vals),
        })

    out.sort(
        key=lambda r: (
            -(to_float(r.get("avg_score")) if to_float(r.get("avg_score")) is not None else -1e18),
            -int(r["count"]),
            safe_text(r["symbol"]),
        )
    )
    return out


def build_side_pivot(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        symbol = safe_text(row.get("symbol"))
        side = safe_text(row.get("side"))
        side_norm = side if side in {"BUY", "SELL", "BOTH"} else "UNKNOWN"
        grouped[symbol][side_norm] += 1

    out: List[Dict[str, Any]] = []
    for symbol, ctr in grouped.items():
        buy = int(ctr.get("BUY", 0))
        sell = int(ctr.get("SELL", 0))
        both = int(ctr.get("BOTH", 0))
        unknown = int(ctr.get("UNKNOWN", 0))
        total = buy + sell + both + unknown
        out.append({
            "symbol": symbol,
            "BUY": buy,
            "SELL": sell,
            "BOTH": both,
            "UNKNOWN": unknown,
            "TOTAL": total,
        })

    out.sort(key=lambda r: (-int(r["TOTAL"]), safe_text(r["symbol"])))
    return out


def build_universe_stats(rows: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    score_vals = [to_float(r.get("best_candidate_score")) for r in rows]
    pf_vals = [to_float(r.get("profit_factor_oos")) for r in rows]
    exp_vals = [to_float(r.get("expectancy_oos")) for r in rows]
    np_vals = [to_float(r.get("net_profit_oos")) for r in rows]

    return {
        "avg_score": mean_ignore_none(score_vals),
        "median_score": median_ignore_none(score_vals),
        "p90_score": percentile_ignore_none(score_vals, 0.90),

        "avg_pf_oos": mean_ignore_none(pf_vals),
        "median_pf_oos": median_ignore_none(pf_vals),
        "p90_pf_oos": percentile_ignore_none(pf_vals, 0.90),

        "avg_expectancy_oos": mean_ignore_none(exp_vals),
        "median_expectancy_oos": median_ignore_none(exp_vals),
        "p90_expectancy_oos": percentile_ignore_none(exp_vals, 0.90),

        "avg_net_profit_oos": mean_ignore_none(np_vals),
        "median_net_profit_oos": median_ignore_none(np_vals),
        "p90_net_profit_oos": percentile_ignore_none(np_vals, 0.90),
    }


def build_top_bottom_stats(rows: List[Dict[str, Any]], metric: str, top_frac: float = 0.20) -> Dict[str, Optional[float]]:
    valid_rows = [r for r in rows if to_float(r.get("best_candidate_score")) is not None and to_float(r.get(metric)) is not None]
    if not valid_rows:
        return {
            "top_mean": None,
            "bottom_mean": None,
            "spread": None,
        }

    ranked = sorted(valid_rows, key=lambda r: to_float(r.get("best_candidate_score")) or -1e18, reverse=True)
    n = max(1, int(len(ranked) * top_frac))
    top = ranked[:n]
    bottom = ranked[-n:]

    top_vals = [to_float(r.get(metric)) for r in top]
    bottom_vals = [to_float(r.get(metric)) for r in bottom]

    top_mean = mean_ignore_none(top_vals)
    bottom_mean = mean_ignore_none(bottom_vals)

    return {
        "top_mean": top_mean,
        "bottom_mean": bottom_mean,
        "spread": None if top_mean is None or bottom_mean is None else float(top_mean - bottom_mean),
    }


# ============================================================
# UI WIDGETS
# ============================================================

class KpiCard(tk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        self.configure(height=84)
        self.pack_propagate(False)

        tk.Label(
            self,
            text=title,
            font=FONT_LABEL,
            bg=BG_CARD,
            fg=FG_MUTED,
        ).pack(anchor="w", padx=12, pady=(8, 2))

        self.value_var = tk.StringVar(value="-")
        self.value_label = tk.Label(
            self,
            textvariable=self.value_var,
            font=FONT_VALUE,
            bg=BG_CARD,
            fg=FG_MAIN,
        )
        self.value_label.pack(anchor="w", padx=12)

        self.sub_var = tk.StringVar(value="")
        self.sub_label = tk.Label(
            self,
            textvariable=self.sub_var,
            font=FONT_SMALL,
            bg=BG_CARD,
            fg=FG_SUBTLE,
        )
        self.sub_label.pack(anchor="w", padx=12, pady=(2, 0))

    def set_value(self, value: str, color: Optional[str] = None, sub: str = ""):
        self.value_var.set(value)
        self.value_label.configure(fg=color or FG_MAIN)
        self.sub_var.set(sub)


class CompareCard(tk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        self.title_var = tk.StringVar(value=title)
        self.lines: List[tk.StringVar] = []

        tk.Label(
            self,
            textvariable=self.title_var,
            font=FONT_SECTION,
            bg=BG_CARD,
            fg=FG_WHITE,
        ).pack(anchor="w", padx=10, pady=(8, 6))

        body = tk.Frame(self, bg=BG_CARD)
        body.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        for _ in range(12):
            var = tk.StringVar(value="")
            self.lines.append(var)
            tk.Label(
                body,
                textvariable=var,
                font=FONT_TEXT,
                bg=BG_CARD,
                fg=FG_MAIN,
                anchor="w",
                justify="left",
            ).pack(anchor="w")

    def set_title(self, title: str):
        self.title_var.set(title)

    def set_lines(self, lines: List[str]):
        for idx, var in enumerate(self.lines):
            var.set(lines[idx] if idx < len(lines) else "")


# ============================================================
# PANEL
# ============================================================

class PerformanceDashboardPanel(tk.Frame):
    def __init__(self, parent, repo: PerformanceRepository):
        super().__init__(parent, bg=BG_MAIN)

        self.repo = repo

        self.raw_rows: List[Dict[str, Any]] = []
        self.filtered_rows: List[Dict[str, Any]] = []
        self.summary_rows: List[Dict[str, Any]] = []
        self.pivot_rows: List[Dict[str, Any]] = []
        self.feature_usage_rows: List[Dict[str, Any]] = []
        self.top_models_rows: List[Dict[str, Any]] = []

        self.auto_refresh_enabled = tk.BooleanVar(value=AUTO_REFRESH_DEFAULT)
        self._refresh_job: Optional[str] = None
        self._filter_job: Optional[str] = None

        self.search_var = tk.StringVar(value="")
        self.symbol_var = tk.StringVar(value="ALL")
        self.side_var = tk.StringVar(value="ALL")
        self.timeframe_var = tk.StringVar(value="ALL")
        self.archetype_var = tk.StringVar(value="ALL")

        self.col_filter_symbol = tk.StringVar(value="")
        self.col_filter_strategy_id = tk.StringVar(value="")
        self.col_filter_side = tk.StringVar(value="")
        self.col_filter_timeframe = tk.StringVar(value="")

        self._sort_state: Dict[str, bool] = {}
        self._table_row_map: Dict[str, Dict[str, Any]] = {}
        self._selected_row: Optional[Dict[str, Any]] = None

        self._compare_rows: List[Dict[str, Any]] = []

        self._configure_ttk_style()
        self._build_ui()
        self._refresh_all(live=False)
        self._schedule_auto_refresh()

    # ========================================================
    # STYLE
    # ============================================================

    def _configure_ttk_style(self):
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure("TNotebook", background=BG_PANEL, borderwidth=0)
        style.configure(
            "TNotebook.Tab",
            background=BG_CARD,
            foreground=FG_MAIN,
            padding=(10, 6),
            borderwidth=0,
        )
        style.map(
            "TNotebook.Tab",
            background=[("selected", BG_TOP)],
            foreground=[("selected", FG_WHITE)],
        )

        style.configure(
            "Treeview",
            background=BG_SURFACE,
            fieldbackground=BG_SURFACE,
            foreground=FG_MAIN,
            rowheight=24,
            bordercolor=BORDER,
            borderwidth=0,
            font=FONT_TEXT,
        )
        style.configure(
            "Treeview.Heading",
            background=BG_CARD,
            foreground=FG_WHITE,
            relief="flat",
            font=("Segoe UI", 9, "bold"),
        )
        style.map(
            "Treeview",
            background=[("selected", "#1F3650")],
            foreground=[("selected", FG_WHITE)],
        )
        style.map(
            "Treeview.Heading",
            background=[("active", BG_CARD_HOVER)],
        )

        style.configure(
            "TCombobox",
            fieldbackground=BG_CARD,
            background=BG_CARD,
            foreground=FG_WHITE,
            arrowsize=14,
            bordercolor=BORDER,
            lightcolor=BORDER,
            darkcolor=BORDER,
        )

    # ========================================================
    # BUILD UI
    # ============================================================

    def _build_ui(self):
        root = tk.Frame(self, bg=BG_MAIN)
        root.pack(fill="both", expand=True, padx=12, pady=12)

        topbar = tk.Frame(root, bg=BG_TOP, height=54, highlightbackground=BORDER, highlightthickness=1)
        topbar.pack(fill="x", pady=(0, 12))
        topbar.pack_propagate(False)

        tk.Label(
            topbar,
            text="Performance Analytics",
            font=FONT_TITLE,
            bg=BG_TOP,
            fg=FG_WHITE,
        ).pack(side="left", padx=14)

        self.live_status_var = tk.StringVar(value="READY")
        tk.Label(
            topbar,
            textvariable=self.live_status_var,
            font=("Segoe UI", 9),
            bg=BG_TOP,
            fg=FG_MUTED,
        ).pack(side="right", padx=(0, 14))

        self.info_var = tk.StringVar(value=str(PERFORMANCE_ROOT))
        tk.Label(
            topbar,
            textvariable=self.info_var,
            font=("Segoe UI", 9),
            bg=BG_TOP,
            fg=FG_SUBTLE,
        ).pack(side="right", padx=14)

        controls = tk.Frame(root, bg=BG_MAIN)
        controls.pack(fill="x", pady=(0, 12))

        def make_label(text: str):
            tk.Label(controls, text=text, bg=BG_MAIN, fg=FG_MUTED, font=FONT_LABEL).pack(side="left", padx=(0, 6))

        def make_entry(var: tk.StringVar, width: int):
            entry = tk.Entry(
                controls,
                textvariable=var,
                bg=BG_CARD,
                fg=FG_MAIN,
                insertbackground=FG_MAIN,
                relief="flat",
                width=width,
            )
            entry.pack(side="left", padx=(0, 10), ipady=5)
            entry.bind("<KeyRelease>", self._on_filter_change)
            return entry

        make_label("Search")
        make_entry(self.search_var, 24)

        make_label("Symbol")
        self.symbol_combo = ttk.Combobox(controls, state="readonly", width=12, textvariable=self.symbol_var)
        self.symbol_combo.pack(side="left", padx=(0, 10))
        self.symbol_combo.bind("<<ComboboxSelected>>", self._on_filter_change)

        make_label("Side")
        self.side_combo = ttk.Combobox(controls, state="readonly", width=10, textvariable=self.side_var)
        self.side_combo.pack(side="left", padx=(0, 10))
        self.side_combo.bind("<<ComboboxSelected>>", self._on_filter_change)

        make_label("TF")
        self.timeframe_combo = ttk.Combobox(controls, state="readonly", width=10, textvariable=self.timeframe_var)
        self.timeframe_combo.pack(side="left", padx=(0, 10))
        self.timeframe_combo.bind("<<ComboboxSelected>>", self._on_filter_change)

        make_label("Archetype")
        self.archetype_combo = ttk.Combobox(controls, state="readonly", width=24, textvariable=self.archetype_var)
        self.archetype_combo.pack(side="left", padx=(0, 10))
        self.archetype_combo.bind("<<ComboboxSelected>>", self._on_filter_change)

        tk.Button(
            controls,
            text="Refresh",
            command=lambda: self._refresh_all(live=False),
            bg=BG_BUTTON,
            fg=FG_WHITE,
            activebackground=BG_BUTTON_HOVER,
            activeforeground=FG_WHITE,
            relief="flat",
            padx=12,
            pady=6,
            bd=0,
        ).pack(side="left", padx=(0, 10))

        tk.Button(
            controls,
            text="Clear Filters",
            command=self._clear_filters,
            bg=BG_BUTTON_SECONDARY,
            fg=FG_MAIN,
            activebackground=BG_CARD_HOVER,
            activeforeground=FG_WHITE,
            relief="flat",
            padx=12,
            pady=6,
            bd=0,
        ).pack(side="left", padx=(0, 10))

        tk.Button(
            controls,
            text="Clear Compare",
            command=self._clear_compare,
            bg=BG_BUTTON_SECONDARY,
            fg=FG_MAIN,
            activebackground=BG_CARD_HOVER,
            activeforeground=FG_WHITE,
            relief="flat",
            padx=12,
            pady=6,
            bd=0,
        ).pack(side="left", padx=(0, 10))

        tk.Checkbutton(
            controls,
            text="Auto Refresh",
            variable=self.auto_refresh_enabled,
            command=self._on_toggle_auto_refresh,
            bg=BG_MAIN,
            fg=FG_MAIN,
            activebackground=BG_MAIN,
            activeforeground=FG_MAIN,
            selectcolor=BG_CARD,
            relief="flat",
        ).pack(side="left")

        # KPI ROW 1
        kpi_row_1 = tk.Frame(root, bg=BG_MAIN)
        kpi_row_1.pack(fill="x", pady=(0, 8))

        self.card_total = KpiCard(kpi_row_1, "Total Strategies")
        self.card_total.pack(side="left", fill="x", expand=True, padx=4)

        self.card_symbols = KpiCard(kpi_row_1, "Symbols")
        self.card_symbols.pack(side="left", fill="x", expand=True, padx=4)

        self.card_avg_score = KpiCard(kpi_row_1, "Avg Score")
        self.card_avg_score.pack(side="left", fill="x", expand=True, padx=4)

        self.card_median_score = KpiCard(kpi_row_1, "Median Score")
        self.card_median_score.pack(side="left", fill="x", expand=True, padx=4)

        self.card_avg_pf = KpiCard(kpi_row_1, "Avg PF OOS")
        self.card_avg_pf.pack(side="left", fill="x", expand=True, padx=4)

        self.card_avg_exp = KpiCard(kpi_row_1, "Avg Expectancy OOS")
        self.card_avg_exp.pack(side="left", fill="x", expand=True, padx=4)

        # KPI ROW 2
        kpi_row_2 = tk.Frame(root, bg=BG_MAIN)
        kpi_row_2.pack(fill="x", pady=(0, 12))

        self.card_selected = KpiCard(kpi_row_2, "Filtered Rows")
        self.card_selected.pack(side="left", fill="x", expand=True, padx=4)

        self.card_top_pf = KpiCard(kpi_row_2, "Top20 PF OOS")
        self.card_top_pf.pack(side="left", fill="x", expand=True, padx=4)

        self.card_top_exp = KpiCard(kpi_row_2, "Top20 Expectancy OOS")
        self.card_top_exp.pack(side="left", fill="x", expand=True, padx=4)

        self.card_top_np = KpiCard(kpi_row_2, "Top20 Net Profit OOS")
        self.card_top_np.pack(side="left", fill="x", expand=True, padx=4)

        self.card_best_symbol = KpiCard(kpi_row_2, "Best Symbol")
        self.card_best_symbol.pack(side="left", fill="x", expand=True, padx=4)

        self.card_top_archetype = KpiCard(kpi_row_2, "Top Archetype")
        self.card_top_archetype.pack(side="left", fill="x", expand=True, padx=4)

        notebook = ttk.Notebook(root)
        notebook.pack(fill="both", expand=True)

        tab_overview = tk.Frame(notebook, bg=BG_SURFACE)
        tab_ranking = tk.Frame(notebook, bg=BG_SURFACE)
        tab_analytics = tk.Frame(notebook, bg=BG_SURFACE)
        tab_model = tk.Frame(notebook, bg=BG_SURFACE)

        notebook.add(tab_overview, text="Overview")
        notebook.add(tab_ranking, text="Ranking")
        notebook.add(tab_analytics, text="Analytics")
        notebook.add(tab_model, text="Model Insight")

        self._build_overview_tab(tab_overview)
        self._build_ranking_tab(tab_ranking)
        self._build_analytics_tab(tab_analytics)
        self._build_model_tab(tab_model)

    def _build_overview_tab(self, parent: tk.Frame):
        main = tk.PanedWindow(parent, orient="horizontal", bg=BG_SURFACE, sashwidth=6)
        main.pack(fill="both", expand=True, padx=10, pady=10)

        left = tk.Frame(main, bg=BG_SURFACE, highlightbackground=BORDER, highlightthickness=1)
        right = tk.Frame(main, bg=BG_SURFACE, highlightbackground=BORDER, highlightthickness=1)
        main.add(left, minsize=430)
        main.add(right, minsize=950)

        tk.Label(
            left,
            text="Symbol Summary",
            font=FONT_SECTION,
            bg=BG_SURFACE,
            fg=FG_WHITE,
        ).pack(anchor="w", padx=10, pady=(10, 8))

        cols = ("symbol", "count", "buy", "sell", "both", "avg_score", "avg_pf_oos", "avg_expectancy_oos")
        self.summary_tree = ttk.Treeview(left, columns=cols, show="headings", height=14)
        self.summary_tree.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.summary_tree.bind("<<TreeviewSelect>>", self._on_summary_select)

        widths = {
            "symbol": 90,
            "count": 70,
            "buy": 60,
            "sell": 60,
            "both": 60,
            "avg_score": 90,
            "avg_pf_oos": 90,
            "avg_expectancy_oos": 110,
        }
        for col in cols:
            self.summary_tree.heading(col, text=col)
            self.summary_tree.column(col, width=widths[col], anchor="w")

        info_box = tk.Frame(left, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        info_box.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        tk.Label(
            info_box,
            text="Universe Notes",
            font=FONT_SECTION,
            bg=BG_CARD,
            fg=FG_WHITE,
        ).pack(anchor="w", padx=10, pady=(8, 6))

        self.selection_text = tk.Text(
            info_box,
            height=14,
            wrap="word",
            bg=BG_CARD,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            font=FONT_TEXT,
        )
        self.selection_text.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.selection_text.configure(state="disabled")

        # right side
        top_row = tk.Frame(right, bg=BG_SURFACE)
        top_row.pack(fill="both", expand=True, padx=10, pady=(10, 5))

        card_a = tk.Frame(top_row, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        card_b = tk.Frame(top_row, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        card_a.pack(side="left", fill="both", expand=True, padx=(0, 5))
        card_b.pack(side="left", fill="both", expand=True, padx=(5, 0))

        tk.Label(card_a, text="Distribution | Score", font=FONT_SECTION, bg=BG_CARD, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))
        self.hist_score_canvas = tk.Canvas(card_a, bg=BG_CARD, highlightthickness=0)
        self.hist_score_canvas.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.hist_score_canvas.bind("<Configure>", lambda _e: self._render_hist(self.hist_score_canvas, "best_candidate_score", "Score"))

        tk.Label(card_b, text="Distribution | PF OOS", font=FONT_SECTION, bg=BG_CARD, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))
        self.hist_pf_canvas = tk.Canvas(card_b, bg=BG_CARD, highlightthickness=0)
        self.hist_pf_canvas.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.hist_pf_canvas.bind("<Configure>", lambda _e: self._render_hist(self.hist_pf_canvas, "profit_factor_oos", "PF OOS"))

        bottom_row = tk.Frame(right, bg=BG_SURFACE)
        bottom_row.pack(fill="both", expand=True, padx=10, pady=(5, 10))

        card_c = tk.Frame(bottom_row, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        card_d = tk.Frame(bottom_row, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        card_c.pack(side="left", fill="both", expand=True, padx=(0, 5))
        card_d.pack(side="left", fill="both", expand=True, padx=(5, 0))

        tk.Label(card_c, text="Scatter | PF OOS vs Expectancy OOS", font=FONT_SECTION, bg=BG_CARD, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))
        self.scatter_pf_exp_canvas = tk.Canvas(card_c, bg=BG_CARD, highlightthickness=0)
        self.scatter_pf_exp_canvas.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.scatter_pf_exp_canvas.bind(
            "<Configure>",
            lambda _e: self._render_scatter(
                self.scatter_pf_exp_canvas,
                x_key="profit_factor_oos",
                y_key="expectancy_oos",
                x_label="PF OOS",
                y_label="Expectancy OOS",
            )
        )

        tk.Label(card_d, text="Scatter | Win Rate OOS vs Payoff", font=FONT_SECTION, bg=BG_CARD, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))
        self.scatter_win_payoff_canvas = tk.Canvas(card_d, bg=BG_CARD, highlightthickness=0)
        self.scatter_win_payoff_canvas.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.scatter_win_payoff_canvas.bind(
            "<Configure>",
            lambda _e: self._render_scatter(
                self.scatter_win_payoff_canvas,
                x_key="win_rate_oos",
                y_key="payoff_ratio_total",
                x_label="Win Rate OOS",
                y_label="Payoff Ratio",
            )
        )

    def _build_ranking_tab(self, parent: tk.Frame):
        top = tk.Frame(parent, bg=BG_SURFACE)
        top.pack(fill="both", expand=True, padx=10, pady=10)

        upper = tk.Frame(top, bg=BG_SURFACE)
        upper.pack(fill="both", expand=True)

        table_card = tk.Frame(upper, bg=BG_SURFACE, highlightbackground=BORDER, highlightthickness=1)
        table_card.pack(side="left", fill="both", expand=True, padx=(0, 6))

        tk.Label(
            table_card,
            text="Strategy Ranking",
            font=FONT_SECTION,
            bg=BG_SURFACE,
            fg=FG_WHITE,
        ).pack(anchor="w", padx=10, pady=(10, 8))

        filter_bar = tk.Frame(table_card, bg=BG_SURFACE)
        filter_bar.pack(fill="x", padx=10, pady=(0, 8))

        def make_filter_entry(parent_, label, var, width):
            box = tk.Frame(parent_, bg=BG_SURFACE)
            box.pack(side="left", padx=(0, 8))
            tk.Label(box, text=label, bg=BG_SURFACE, fg=FG_MUTED, font=FONT_LABEL).pack(anchor="w")
            e = tk.Entry(
                box,
                textvariable=var,
                bg=BG_CARD,
                fg=FG_MAIN,
                insertbackground=FG_MAIN,
                relief="flat",
                width=width,
            )
            e.pack(ipady=4)
            e.bind("<KeyRelease>", self._on_filter_change)

        make_filter_entry(filter_bar, "symbol", self.col_filter_symbol, 10)
        make_filter_entry(filter_bar, "strategy_id", self.col_filter_strategy_id, 18)
        make_filter_entry(filter_bar, "side", self.col_filter_side, 8)
        make_filter_entry(filter_bar, "timeframe", self.col_filter_timeframe, 10)

        table_shell = tk.Frame(table_card, bg=BG_SURFACE)
        table_shell.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        cols = (
            "strategy_id",
            "symbol",
            "side",
            "timeframe",
            "archetype",
            "best_candidate_score",
            "proxy_target",
            "profit_factor_oos",
            "expectancy_oos",
            "net_profit_oos",
            "win_rate_oos",
            "payoff_ratio_total",
            "trade_count_oos",
            "active_months",
        )

        self.table = ttk.Treeview(table_shell, columns=cols, show="headings", height=24)
        self.table.pack(side="left", fill="both", expand=True)
        self.table.bind("<Double-1>", self._on_row_double_click)
        self.table.bind("<<TreeviewSelect>>", self._on_table_select)

        scrollbar_y = ttk.Scrollbar(table_shell, orient="vertical", command=self.table.yview)
        scrollbar_y.pack(side="right", fill="y")
        self.table.configure(yscrollcommand=scrollbar_y.set)

        widths = {
            "strategy_id": 260,
            "symbol": 90,
            "side": 80,
            "timeframe": 80,
            "archetype": 170,
            "best_candidate_score": 110,
            "proxy_target": 100,
            "profit_factor_oos": 100,
            "expectancy_oos": 100,
            "net_profit_oos": 110,
            "win_rate_oos": 95,
            "payoff_ratio_total": 110,
            "trade_count_oos": 100,
            "active_months": 95,
        }

        for col in cols:
            self.table.heading(col, text=col, command=lambda c=col: self._sort_main_table_by(c))
            self.table.column(col, width=widths[col], anchor="w")

        side_panel = tk.Frame(upper, bg=BG_SURFACE, highlightbackground=BORDER, highlightthickness=1)
        side_panel.pack(side="left", fill="y", padx=(6, 0))

        tk.Label(side_panel, text="Selected Strategy", font=FONT_SECTION, bg=BG_SURFACE, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))

        self.selected_strategy_text = tk.Text(
            side_panel,
            width=40,
            height=18,
            wrap="word",
            bg=BG_CARD,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            font=FONT_TEXT,
        )
        self.selected_strategy_text.pack(fill="both", expand=False, padx=10, pady=(0, 10))
        self.selected_strategy_text.configure(state="disabled")

        tk.Button(
            side_panel,
            text="Add Selected to Compare",
            command=self._add_selected_to_compare,
            bg=BG_BUTTON,
            fg=FG_WHITE,
            activebackground=BG_BUTTON_HOVER,
            activeforeground=FG_WHITE,
            relief="flat",
            padx=12,
            pady=6,
            bd=0,
        ).pack(fill="x", padx=10, pady=(0, 10))

        lower = tk.Frame(top, bg=BG_SURFACE)
        lower.pack(fill="both", expand=False, pady=(10, 0))

        tk.Label(lower, text="Compare Strategies", font=FONT_SECTION, bg=BG_SURFACE, fg=FG_WHITE).pack(anchor="w", pady=(0, 8))

        compare_row = tk.Frame(lower, bg=BG_SURFACE)
        compare_row.pack(fill="x")

        self.compare_cards: List[CompareCard] = []
        for idx in range(COMPARE_SLOT_COUNT):
            card = CompareCard(compare_row, f"Slot {idx+1}")
            card.pack(side="left", fill="both", expand=True, padx=4)
            self.compare_cards.append(card)

    def _build_analytics_tab(self, parent: tk.Frame):
        grid = tk.Frame(parent, bg=BG_SURFACE)
        grid.pack(fill="both", expand=True, padx=10, pady=10)

        # row 1
        r1 = tk.Frame(grid, bg=BG_SURFACE)
        r1.pack(fill="both", expand=True)

        card_a = tk.Frame(r1, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        card_b = tk.Frame(r1, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        card_a.pack(side="left", fill="both", expand=True, padx=(0, 5), pady=(0, 5))
        card_b.pack(side="left", fill="both", expand=True, padx=(5, 0), pady=(0, 5))

        tk.Label(card_a, text="Distribution | Expectancy OOS", font=FONT_SECTION, bg=BG_CARD, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))
        self.hist_exp_canvas = tk.Canvas(card_a, bg=BG_CARD, highlightthickness=0)
        self.hist_exp_canvas.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.hist_exp_canvas.bind("<Configure>", lambda _e: self._render_hist(self.hist_exp_canvas, "expectancy_oos", "Expectancy OOS"))

        tk.Label(card_b, text="Distribution | Win Rate OOS", font=FONT_SECTION, bg=BG_CARD, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))
        self.hist_win_canvas = tk.Canvas(card_b, bg=BG_CARD, highlightthickness=0)
        self.hist_win_canvas.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.hist_win_canvas.bind("<Configure>", lambda _e: self._render_hist(self.hist_win_canvas, "win_rate_oos", "Win Rate OOS"))

        # row 2
        r2 = tk.Frame(grid, bg=BG_SURFACE)
        r2.pack(fill="both", expand=True)

        card_c = tk.Frame(r2, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        card_d = tk.Frame(r2, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        card_c.pack(side="left", fill="both", expand=True, padx=(0, 5), pady=(5, 5))
        card_d.pack(side="left", fill="both", expand=True, padx=(5, 0), pady=(5, 5))

        tk.Label(card_c, text="Scatter | Score vs PF OOS", font=FONT_SECTION, bg=BG_CARD, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))
        self.scatter_score_pf_canvas = tk.Canvas(card_c, bg=BG_CARD, highlightthickness=0)
        self.scatter_score_pf_canvas.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.scatter_score_pf_canvas.bind(
            "<Configure>",
            lambda _e: self._render_scatter(
                self.scatter_score_pf_canvas,
                x_key="best_candidate_score",
                y_key="profit_factor_oos",
                x_label="Score",
                y_label="PF OOS",
            )
        )

        tk.Label(card_d, text="Scatter | Net Profit OOS vs OOS Trades", font=FONT_SECTION, bg=BG_CARD, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))
        self.scatter_np_trades_canvas = tk.Canvas(card_d, bg=BG_CARD, highlightthickness=0)
        self.scatter_np_trades_canvas.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.scatter_np_trades_canvas.bind(
            "<Configure>",
            lambda _e: self._render_scatter(
                self.scatter_np_trades_canvas,
                x_key="trade_count_oos",
                y_key="net_profit_oos",
                x_label="OOS Trades",
                y_label="Net Profit OOS",
            )
        )

        # row 3
        r3 = tk.Frame(grid, bg=BG_SURFACE)
        r3.pack(fill="both", expand=True)

        card_e = tk.Frame(r3, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        card_f = tk.Frame(r3, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        card_e.pack(side="left", fill="both", expand=True, padx=(0, 5), pady=(5, 0))
        card_f.pack(side="left", fill="both", expand=True, padx=(5, 0), pady=(5, 0))

        tk.Label(card_e, text="Top vs Bottom | Performance Shape", font=FONT_SECTION, bg=BG_CARD, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))
        self.top_bottom_canvas = tk.Canvas(card_e, bg=BG_CARD, highlightthickness=0)
        self.top_bottom_canvas.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.top_bottom_canvas.bind("<Configure>", lambda _e: self._render_top_bottom_compare())

        tk.Label(card_f, text="Archetype Distribution", font=FONT_SECTION, bg=BG_CARD, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))
        self.archetype_canvas = tk.Canvas(card_f, bg=BG_CARD, highlightthickness=0)
        self.archetype_canvas.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.archetype_canvas.bind("<Configure>", lambda _e: self._render_archetype_chart())

    def _build_model_tab(self, parent: tk.Frame):
        main = tk.PanedWindow(parent, orient="horizontal", bg=BG_SURFACE, sashwidth=6)
        main.pack(fill="both", expand=True, padx=10, pady=10)

        left = tk.Frame(main, bg=BG_SURFACE, highlightbackground=BORDER, highlightthickness=1)
        right = tk.Frame(main, bg=BG_SURFACE, highlightbackground=BORDER, highlightthickness=1)
        main.add(left, minsize=420)
        main.add(right, minsize=1000)

        tk.Label(left, text="Feature Usage | Top Models", font=FONT_SECTION, bg=BG_SURFACE, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))

        usage_shell = tk.Frame(left, bg=BG_SURFACE)
        usage_shell.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        usage_cols = ("feature", "count", "share_in_top_k")
        self.feature_usage_tree = ttk.Treeview(usage_shell, columns=usage_cols, show="headings", height=16)
        self.feature_usage_tree.pack(side="left", fill="both", expand=True)

        usage_scroll = ttk.Scrollbar(usage_shell, orient="vertical", command=self.feature_usage_tree.yview)
        usage_scroll.pack(side="right", fill="y")
        self.feature_usage_tree.configure(yscrollcommand=usage_scroll.set)

        usage_widths = {
            "feature": 240,
            "count": 70,
            "share_in_top_k": 100,
        }
        for col in usage_cols:
            self.feature_usage_tree.heading(col, text=col)
            self.feature_usage_tree.column(col, width=usage_widths[col], anchor="w")

        tk.Label(right, text="Top Search Models", font=FONT_SECTION, bg=BG_SURFACE, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))

        top_block = tk.Frame(right, bg=BG_SURFACE)
        top_block.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        upper = tk.Frame(top_block, bg=BG_SURFACE)
        upper.pack(fill="both", expand=True)

        model_shell = tk.Frame(upper, bg=BG_SURFACE)
        model_shell.pack(side="left", fill="both", expand=True)

        cols = (
            "rank",
            "objective",
            "spearman_score_vs_target",
            "top_bottom_spread",
            "top_bucket_target_mean",
            "n_features",
            "features",
        )

        self.models_tree = ttk.Treeview(model_shell, columns=cols, show="headings", height=16)
        self.models_tree.pack(side="left", fill="both", expand=True)
        self.models_tree.bind("<<TreeviewSelect>>", self._on_model_select)

        models_scroll = ttk.Scrollbar(model_shell, orient="vertical", command=self.models_tree.yview)
        models_scroll.pack(side="right", fill="y")
        self.models_tree.configure(yscrollcommand=models_scroll.set)

        widths = {
            "rank": 60,
            "objective": 100,
            "spearman_score_vs_target": 130,
            "top_bottom_spread": 120,
            "top_bucket_target_mean": 140,
            "n_features": 90,
            "features": 720,
        }

        for col in cols:
            self.models_tree.heading(col, text=col)
            self.models_tree.column(col, width=widths[col], anchor="w")

        lower = tk.Frame(top_block, bg=BG_SURFACE)
        lower.pack(fill="both", expand=True)

        card_a = tk.Frame(lower, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        card_b = tk.Frame(lower, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        card_a.pack(side="left", fill="both", expand=True, padx=(0, 5))
        card_b.pack(side="left", fill="both", expand=True, padx=(5, 0))

        tk.Label(card_a, text="Selected Model Weights", font=FONT_SECTION, bg=BG_CARD, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))
        self.model_weights_text = tk.Text(
            card_a,
            wrap="word",
            bg=BG_CARD,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            font=FONT_MONO,
            height=14,
        )
        self.model_weights_text.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.model_weights_text.configure(state="disabled")

        tk.Label(card_b, text="Feature Usage Chart", font=FONT_SECTION, bg=BG_CARD, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))
        self.feature_usage_canvas = tk.Canvas(card_b, bg=BG_CARD, highlightthickness=0)
        self.feature_usage_canvas.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.feature_usage_canvas.bind("<Configure>", lambda _e: self._render_feature_usage_chart())

    # ========================================================
    # DATA FLOW
    # ============================================================

    def _refresh_all(self, live: bool = False):
        try:
            data = self.repo.scan()
            self.raw_rows = data.get("performance_rows", [])
            self.feature_usage_rows = data.get("feature_usage_rows", [])
            self.top_models_rows = data.get("top_models_rows", [])

            self._reload_filter_options()
            self._apply_filters()

            import datetime as _dt
            last_refresh_label = _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            mode = "LIVE" if live else "MANUAL"
            self.live_status_var.set(f"{mode} | last refresh {last_refresh_label}")
        except Exception as e:
            self.live_status_var.set(f"ERROR | {e}")

    def _reload_filter_options(self):
        rows = self.raw_rows

        symbols = unique_sorted([safe_text(r.get("symbol")) for r in rows]) if rows else ["ALL"]
        sides = unique_sorted([safe_text(r.get("side")) for r in rows]) if rows else ["ALL"]
        timeframes = unique_sorted([safe_text(r.get("timeframe")) for r in rows]) if rows else ["ALL"]
        archetypes = unique_sorted([safe_text(r.get("archetype")) for r in rows]) if rows else ["ALL"]

        def keep_or_all(var: tk.StringVar, values: List[str]):
            cur = var.get() or "ALL"
            var.set(cur if cur in values else "ALL")

        self.symbol_combo["values"] = symbols
        self.side_combo["values"] = sides
        self.timeframe_combo["values"] = timeframes
        self.archetype_combo["values"] = archetypes

        keep_or_all(self.symbol_var, symbols)
        keep_or_all(self.side_var, sides)
        keep_or_all(self.timeframe_var, timeframes)
        keep_or_all(self.archetype_var, archetypes)

    def _apply_filters(self):
        rows = list(self.raw_rows)

        search = safe_text(self.search_var.get()).lower()
        symbol = safe_text(self.symbol_var.get())
        side = safe_text(self.side_var.get())
        timeframe = safe_text(self.timeframe_var.get())
        archetype = safe_text(self.archetype_var.get())

        if symbol and symbol != "ALL":
            rows = [r for r in rows if safe_text(r.get("symbol")) == symbol]
        if side and side != "ALL":
            rows = [r for r in rows if safe_text(r.get("side")) == side]
        if timeframe and timeframe != "ALL":
            rows = [r for r in rows if safe_text(r.get("timeframe")) == timeframe]
        if archetype and archetype != "ALL":
            rows = [r for r in rows if safe_text(r.get("archetype")) == archetype]

        if search:
            cols = [
                "strategy_id",
                "symbol",
                "side",
                "timeframe",
                "archetype",
            ]
            rows = [r for r in rows if any(contains_ci(r.get(col), search) for col in cols)]

        col_symbol = safe_text(self.col_filter_symbol.get()).lower()
        col_strategy_id = safe_text(self.col_filter_strategy_id.get()).lower()
        col_side = safe_text(self.col_filter_side.get()).lower()
        col_timeframe = safe_text(self.col_filter_timeframe.get()).lower()

        if col_symbol:
            rows = [r for r in rows if contains_ci(r.get("symbol"), col_symbol)]
        if col_strategy_id:
            rows = [r for r in rows if contains_ci(r.get("strategy_id"), col_strategy_id)]
        if col_side:
            rows = [r for r in rows if contains_ci(r.get("side"), col_side)]
        if col_timeframe:
            rows = [r for r in rows if contains_ci(r.get("timeframe"), col_timeframe)]

        self.filtered_rows = rows
        self.summary_rows = build_symbol_summary(self.filtered_rows)
        self.pivot_rows = build_side_pivot(self.filtered_rows)

        self._update_kpis()
        self._load_summary_table()
        self._load_feature_usage_table()
        self._load_main_table()
        self._load_top_models_table()
        self._load_pivot_table()
        self._update_selection_info()
        self._render_all_charts()
        self._refresh_compare_cards()

        if self.top_models_rows:
            self._show_model_detail(self.top_models_rows[0])

    # ========================================================
    # KPI / TABLES
    # ============================================================

    def _update_kpis(self):
        raw = self.raw_rows
        flt = self.filtered_rows

        total = len(raw)
        symbols = len({safe_text(r.get("symbol")) for r in raw if safe_text(r.get("symbol"))})

        stats = build_universe_stats(flt)
        pf_tb = build_top_bottom_stats(flt, "profit_factor_oos", top_frac=0.20)
        exp_tb = build_top_bottom_stats(flt, "expectancy_oos", top_frac=0.20)
        np_tb = build_top_bottom_stats(flt, "net_profit_oos", top_frac=0.20)

        best_symbol = safe_text(self.summary_rows[0]["symbol"]) if self.summary_rows else "-"
        arche_counter = Counter(safe_text(r.get("archetype")) for r in flt if safe_text(r.get("archetype")))
        top_arch = arche_counter.most_common(1)[0][0] if arche_counter else "-"

        self.card_total.set_value(fmt_int(total), sub=f"raw universe")
        self.card_symbols.set_value(fmt_int(symbols), sub=f"unique symbols")
        self.card_avg_score.set_value(
            fmt_float(stats.get("avg_score"), 3),
            color=FG_ACCENT,
            sub=f"p90 {fmt_float(stats.get('p90_score'), 3)}"
        )
        self.card_median_score.set_value(
            fmt_float(stats.get("median_score"), 3),
            color=FG_ACCENT,
            sub="median"
        )
        self.card_avg_pf.set_value(
            fmt_float(stats.get("avg_pf_oos"), 3),
            color=ratio_color(stats.get("avg_pf_oos"), good=1.2, warn=1.0),
            sub=f"p90 {fmt_float(stats.get('p90_pf_oos'), 3)}"
        )
        self.card_avg_exp.set_value(
            fmt_float(stats.get("avg_expectancy_oos"), 2),
            color=sign_color(stats.get("avg_expectancy_oos")),
            sub=f"p90 {fmt_float(stats.get('p90_expectancy_oos'), 2)}"
        )

        self.card_selected.set_value(fmt_int(len(flt)), color=FG_POS if len(flt) > 0 else FG_NEG, sub="filtered rows")
        self.card_top_pf.set_value(
            fmt_float(pf_tb.get("top_mean"), 3),
            color=ratio_color(pf_tb.get("top_mean"), good=1.3, warn=1.0),
            sub=f"bottom {fmt_float(pf_tb.get('bottom_mean'), 3)}"
        )
        self.card_top_exp.set_value(
            fmt_float(exp_tb.get("top_mean"), 2),
            color=sign_color(exp_tb.get("top_mean")),
            sub=f"bottom {fmt_float(exp_tb.get('bottom_mean'), 2)}"
        )
        self.card_top_np.set_value(
            fmt_float(np_tb.get("top_mean"), 2),
            color=sign_color(np_tb.get("top_mean")),
            sub=f"bottom {fmt_float(np_tb.get('bottom_mean'), 2)}"
        )
        self.card_best_symbol.set_value(best_symbol, color=FG_ACCENT, sub="avg score leader")
        self.card_top_archetype.set_value(top_arch, color=FG_ACCENT, sub=f"{arche_counter.get(top_arch, 0)} rows" if top_arch != "-" else "")

    def _load_summary_table(self):
        self.summary_tree.delete(*self.summary_tree.get_children())
        for idx, row in enumerate(self.summary_rows[:SUMMARY_ROW_LIMIT]):
            iid = make_tree_iid("summary", idx, safe_text(row.get("symbol")))
            self.summary_tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    row.get("symbol", ""),
                    row.get("count", ""),
                    row.get("buy", ""),
                    row.get("sell", ""),
                    row.get("both", ""),
                    fmt_float(row.get("avg_score"), 3),
                    fmt_float(row.get("avg_pf_oos"), 3),
                    fmt_float(row.get("avg_expectancy_oos"), 2),
                ),
            )

    def _load_feature_usage_table(self):
        self.feature_usage_tree.delete(*self.feature_usage_tree.get_children())
        for idx, row in enumerate(self.feature_usage_rows[:FEATURE_USAGE_LIMIT]):
            iid = make_tree_iid("feature_usage", idx, safe_text(row.get("feature")))
            self.feature_usage_tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    pretty_feature_name(row.get("feature", "")),
                    row.get("count", 0),
                    fmt_pct_from_ratio(row.get("share_in_top_k"), 1),
                ),
            )

    def _load_top_models_table(self):
        self.models_tree.delete(*self.models_tree.get_children())
        for idx, row in enumerate(self.top_models_rows[:TOP_MODEL_LIMIT]):
            iid = make_tree_iid("top_models", idx, safe_text(row.get("rank")))
            self.models_tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    row.get("rank", ""),
                    fmt_float(row.get("objective"), 4),
                    fmt_float(row.get("spearman_score_vs_target"), 4),
                    fmt_float(row.get("top_bottom_spread"), 4),
                    fmt_float(row.get("top_bucket_target_mean"), 4),
                    row.get("n_features", ""),
                    row.get("features", ""),
                ),
            )

    def _load_pivot_table(self):
        self.pivot_tree.delete(*self.pivot_tree.get_children())
        if not self.pivot_rows:
            return

        total_buy = sum(int(r.get("BUY", 0)) for r in self.pivot_rows)
        total_sell = sum(int(r.get("SELL", 0)) for r in self.pivot_rows)
        total_both = sum(int(r.get("BOTH", 0)) for r in self.pivot_rows)
        total_unknown = sum(int(r.get("UNKNOWN", 0)) for r in self.pivot_rows)
        total_total = sum(int(r.get("TOTAL", 0)) for r in self.pivot_rows)

        for idx, row in enumerate(self.pivot_rows[:SUMMARY_ROW_LIMIT]):
            iid = make_tree_iid("pivot", idx, safe_text(row.get("symbol")))
            self.pivot_tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    row.get("symbol", ""),
                    row.get("BUY", 0),
                    row.get("SELL", 0),
                    row.get("BOTH", 0),
                    row.get("UNKNOWN", 0),
                    row.get("TOTAL", 0),
                ),
            )

        self.pivot_tree.insert(
            "",
            "end",
            iid="pivot__total",
            values=("Gesamtergebnis", total_buy, total_sell, total_both, total_unknown, total_total),
        )

    def _load_main_table(self):
        self.table.delete(*self.table.get_children())
        self._table_row_map.clear()

        for idx, row in enumerate(self.filtered_rows[:TABLE_ROW_LIMIT]):
            iid = make_tree_iid("table", idx, safe_text(row.get("row_key", row.get("strategy_id"))))
            self._table_row_map[iid] = row

            self.table.insert(
                "",
                "end",
                iid=iid,
                values=(
                    row.get("strategy_id", ""),
                    row.get("symbol", ""),
                    row.get("side", ""),
                    row.get("timeframe", ""),
                    row.get("archetype", ""),
                    fmt_float(row.get("best_candidate_score"), 4),
                    fmt_float(row.get("proxy_target"), 4),
                    fmt_float(row.get("profit_factor_oos"), 4),
                    fmt_float(row.get("expectancy_oos"), 4),
                    fmt_float(row.get("net_profit_oos"), 2),
                    fmt_pct_from_ratio(row.get("win_rate_oos"), 2),
                    fmt_float(row.get("payoff_ratio_total"), 4),
                    fmt_int(row.get("trade_count_oos")),
                    fmt_int(row.get("active_months")),
                ),
            )

    def _sort_main_table_by(self, col: str):
        if not self.filtered_rows:
            return

        ascending = self._sort_state.get(col, True)

        def sort_key(row: Dict[str, Any]):
            value = row.get(col)
            num = to_float(value)
            if num is not None:
                return (0, num)
            return (1, safe_text(value).lower())

        self.filtered_rows = sorted(self.filtered_rows, key=sort_key, reverse=not ascending)
        self._sort_state[col] = not ascending
        self._load_main_table()

    def _clear_filters(self):
        self.search_var.set("")
        self.symbol_var.set("ALL")
        self.side_var.set("ALL")
        self.timeframe_var.set("ALL")
        self.archetype_var.set("ALL")
        self.col_filter_symbol.set("")
        self.col_filter_strategy_id.set("")
        self.col_filter_side.set("")
        self.col_filter_timeframe.set("")
        self._apply_filters()

    def _clear_compare(self):
        self._compare_rows = []
        self._refresh_compare_cards()

    def _update_selection_info(self):
        raw = self.raw_rows
        flt = self.filtered_rows

        score_vals = [to_float(r.get("best_candidate_score")) for r in flt]
        pf_vals = [to_float(r.get("profit_factor_oos")) for r in flt]
        exp_vals = [to_float(r.get("expectancy_oos")) for r in flt]
        wr_vals = [to_float(r.get("win_rate_oos")) for r in flt]

        lines = [
            f"Root                  : {PERFORMANCE_ROOT}",
            f"Total Rows            : {len(raw)}",
            f"Filtered Rows         : {len(flt)}",
            f"Selected Symbol       : {safe_text(self.symbol_var.get()) or 'ALL'}",
            f"Selected Side         : {safe_text(self.side_var.get()) or 'ALL'}",
            f"Selected Timeframe    : {safe_text(self.timeframe_var.get()) or 'ALL'}",
            f"Selected Archetype    : {safe_text(self.archetype_var.get()) or 'ALL'}",
            f"Search                : {safe_text(self.search_var.get()) or '-'}",
            "",
            f"Filtered Avg Score    : {fmt_float(mean_ignore_none(score_vals), 4)}",
            f"Filtered Median Score : {fmt_float(median_ignore_none(score_vals), 4)}",
            f"Filtered Avg PF OOS   : {fmt_float(mean_ignore_none(pf_vals), 4)}",
            f"Filtered Avg Exp OOS  : {fmt_float(mean_ignore_none(exp_vals), 4)}",
            f"Filtered Avg Win OOS  : {fmt_pct_from_ratio(mean_ignore_none(wr_vals), 2)}",
            "",
            "Archetype Counts:",
        ]

        arch_counter = Counter(safe_text(r.get("archetype")) for r in flt if safe_text(r.get("archetype")))
        for name, cnt in arch_counter.most_common(8):
            lines.append(f"  {name:<24} {cnt}")

        self.selection_text.configure(state="normal")
        self.selection_text.delete("1.0", "end")
        self.selection_text.insert("1.0", "\n".join(lines))
        self.selection_text.configure(state="disabled")

    def _update_selected_strategy_panel(self, row: Optional[Dict[str, Any]]):
        self.selected_strategy_text.configure(state="normal")
        self.selected_strategy_text.delete("1.0", "end")

        if not row:
            self.selected_strategy_text.insert("1.0", "Keine Strategie ausgewählt.")
            self.selected_strategy_text.configure(state="disabled")
            return

        score = to_float(row.get("best_candidate_score"))
        pf = to_float(row.get("profit_factor_oos"))
        exp = to_float(row.get("expectancy_oos"))
        wr = to_float(row.get("win_rate_oos"))
        payoff = to_float(row.get("payoff_ratio_total"))
        np_oos = to_float(row.get("net_profit_oos"))
        tc = to_float(row.get("trade_count_oos"))

        all_scores = sorted([to_float(r.get("best_candidate_score")) for r in self.filtered_rows if to_float(r.get("best_candidate_score")) is not None])
        all_pf = sorted([to_float(r.get("profit_factor_oos")) for r in self.filtered_rows if to_float(r.get("profit_factor_oos")) is not None])
        all_exp = sorted([to_float(r.get("expectancy_oos")) for r in self.filtered_rows if to_float(r.get("expectancy_oos")) is not None])

        def percentile_rank(sorted_vals: List[float], value: Optional[float]) -> str:
            if value is None or not sorted_vals:
                return "-"
            count = sum(1 for x in sorted_vals if x <= value)
            p = 100.0 * count / len(sorted_vals)
            return f"{p:.1f}%"

        lines = [
            f"strategy_id           : {safe_text(row.get('strategy_id'))}",
            f"symbol                : {safe_text(row.get('symbol'))}",
            f"side                  : {safe_text(row.get('side'))}",
            f"timeframe             : {safe_text(row.get('timeframe'))}",
            f"archetype             : {safe_text(row.get('archetype'))}",
            "",
            f"score                 : {fmt_float(score, 4)} | pct {percentile_rank(all_scores, score)}",
            f"proxy_target          : {fmt_float(row.get('proxy_target'), 4)}",
            f"pf_oos                : {fmt_float(pf, 4)} | pct {percentile_rank(all_pf, pf)}",
            f"expectancy_oos        : {fmt_float(exp, 4)} | pct {percentile_rank(all_exp, exp)}",
            f"net_profit_oos        : {fmt_float(np_oos, 2)}",
            f"win_rate_oos          : {fmt_pct_from_ratio(wr, 2)}",
            f"payoff_ratio_total    : {fmt_float(payoff, 4)}",
            f"trade_count_oos       : {fmt_int(tc)}",
            f"active_months         : {fmt_int(row.get('active_months'))}",
        ]

        self.selected_strategy_text.insert("1.0", "\n".join(lines))
        self.selected_strategy_text.configure(state="disabled")

    # ========================================================
    # COMPARE
    # ============================================================

    def _add_selected_to_compare(self):
        if not self._selected_row:
            return

        strategy_id = safe_text(self._selected_row.get("strategy_id"))
        if not strategy_id:
            return

        already = [safe_text(r.get("strategy_id")) for r in self._compare_rows]
        if strategy_id in already:
            return

        if len(self._compare_rows) >= COMPARE_SLOT_COUNT:
            self._compare_rows.pop(0)
        self._compare_rows.append(dict(self._selected_row))
        self._refresh_compare_cards()

    def _refresh_compare_cards(self):
        for idx, card in enumerate(self.compare_cards):
            if idx >= len(self._compare_rows):
                card.set_title(f"Slot {idx+1}")
                card.set_lines(["leer"])
                continue

            row = self._compare_rows[idx]
            card.set_title(safe_text(row.get("strategy_id")))
            card.set_lines([
                f"Symbol: {safe_text(row.get('symbol'))}",
                f"Side: {safe_text(row.get('side'))}",
                f"TF: {safe_text(row.get('timeframe'))}",
                f"Archetype: {safe_text(row.get('archetype'))}",
                f"Score: {fmt_float(row.get('best_candidate_score'), 4)}",
                f"Proxy: {fmt_float(row.get('proxy_target'), 4)}",
                f"PF OOS: {fmt_float(row.get('profit_factor_oos'), 4)}",
                f"Exp OOS: {fmt_float(row.get('expectancy_oos'), 4)}",
                f"NP OOS: {fmt_float(row.get('net_profit_oos'), 2)}",
                f"Win OOS: {fmt_pct_from_ratio(row.get('win_rate_oos'), 2)}",
                f"Payoff: {fmt_float(row.get('payoff_ratio_total'), 4)}",
                f"Trades OOS: {fmt_int(row.get('trade_count_oos'))}",
            ])

    # ========================================================
    # MODEL DETAIL
    # ============================================================

    def _show_model_detail(self, row: Dict[str, Any]):
        pairs = parse_feature_weights(safe_text(row.get("features")), safe_text(row.get("weights")))

        lines = [
            f"rank                    : {safe_text(row.get('rank'))}",
            f"objective               : {fmt_float(row.get('objective'), 6)}",
            f"spearman_score_vs_target: {fmt_float(row.get('spearman_score_vs_target'), 6)}",
            f"top_bottom_spread       : {fmt_float(row.get('top_bottom_spread'), 6)}",
            f"top_bucket_target_mean  : {fmt_float(row.get('top_bucket_target_mean'), 6)}",
            f"n_features              : {fmt_int(row.get('n_features'))}",
            "",
            "weights:",
        ]

        for feat, w in pairs:
            lines.append(f"  {pretty_feature_name(feat):<28} {w:.6f}")

        self.model_weights_text.configure(state="normal")
        self.model_weights_text.delete("1.0", "end")
        self.model_weights_text.insert("1.0", "\n".join(lines))
        self.model_weights_text.configure(state="disabled")

    # ========================================================
    # CHARTS
    # ============================================================

    def _render_all_charts(self):
        self._render_hist(self.hist_score_canvas, "best_candidate_score", "Score")
        self._render_hist(self.hist_pf_canvas, "profit_factor_oos", "PF OOS")
        self._render_hist(self.hist_exp_canvas, "expectancy_oos", "Expectancy OOS")
        self._render_hist(self.hist_win_canvas, "win_rate_oos", "Win Rate OOS")

        self._render_scatter(self.scatter_pf_exp_canvas, "profit_factor_oos", "expectancy_oos", "PF OOS", "Expectancy OOS")
        self._render_scatter(self.scatter_win_payoff_canvas, "win_rate_oos", "payoff_ratio_total", "Win Rate OOS", "Payoff Ratio")
        self._render_scatter(self.scatter_score_pf_canvas, "best_candidate_score", "profit_factor_oos", "Score", "PF OOS")
        self._render_scatter(self.scatter_np_trades_canvas, "trade_count_oos", "net_profit_oos", "OOS Trades", "Net Profit OOS")

        self._render_top_bottom_compare()
        self._render_archetype_chart()
        self._render_score_chart()
        self._render_feature_usage_chart()

    def _render_hist(self, canvas: tk.Canvas, key: str, label: str):
        canvas.delete("all")
        width = max(canvas.winfo_width(), 300)
        height = max(canvas.winfo_height(), 220)

        values = [to_float(r.get(key)) for r in self.filtered_rows]
        values = [x for x in values if x is not None]
        if not values:
            canvas.create_text(width / 2, height / 2, text="No data", fill=FG_MUTED, font=FONT_SECTION)
            return

        lo = min(values)
        hi = max(values)
        if abs(hi - lo) < 1e-12:
            hi = lo + 1.0

        bins = HIST_BINS
        counts = [0 for _ in range(bins)]

        for v in values:
            pos = (v - lo) / (hi - lo)
            idx = min(bins - 1, max(0, int(pos * bins)))
            counts[idx] += 1

        max_count = max(counts) if counts else 1

        left = 42
        right = width - 16
        top = 18
        bottom = height - 35
        plot_w = max(100, right - left)
        plot_h = max(80, bottom - top)

        canvas.create_line(left, top, left, bottom, fill=CHART_COLORS["axis"])
        canvas.create_line(left, bottom, right, bottom, fill=CHART_COLORS["axis"])

        bar_w = plot_w / bins
        for i, count in enumerate(counts):
            x1 = left + i * bar_w + 1
            x2 = left + (i + 1) * bar_w - 1
            y2 = bottom - 1
            y1 = bottom - (count / max_count) * (plot_h - 6)
            canvas.create_rectangle(x1, y1, x2, y2, fill=CHART_COLORS["bar"], outline=CHART_COLORS["bar"])

        canvas.create_text(left, bottom + 12, text=fmt_float(lo, 2), fill=FG_MUTED, font=FONT_SMALL, anchor="w")
        canvas.create_text(right, bottom + 12, text=fmt_float(hi, 2), fill=FG_MUTED, font=FONT_SMALL, anchor="e")
        canvas.create_text(left - 6, top, text=str(max_count), fill=FG_MUTED, font=FONT_SMALL, anchor="e")
        canvas.create_text(width / 2, 8, text=label, fill=FG_MUTED, font=FONT_SMALL)

    def _render_scatter(self, canvas: tk.Canvas, x_key: str, y_key: str, x_label: str, y_label: str):
        canvas.delete("all")
        width = max(canvas.winfo_width(), 300)
        height = max(canvas.winfo_height(), 220)

        points: List[Tuple[float, float, Dict[str, Any]]] = []
        for row in self.filtered_rows:
            x = to_float(row.get(x_key))
            y = to_float(row.get(y_key))
            if x is None or y is None:
                continue
            points.append((x, y, row))

        if not points:
            canvas.create_text(width / 2, height / 2, text="No data", fill=FG_MUTED, font=FONT_SECTION)
            return

        x_vals = [p[0] for p in points]
        y_vals = [p[1] for p in points]

        x_lo, x_hi = min(x_vals), max(x_vals)
        y_lo, y_hi = min(y_vals), max(y_vals)

        if abs(x_hi - x_lo) < 1e-12:
            x_hi = x_lo + 1.0
        if abs(y_hi - y_lo) < 1e-12:
            y_hi = y_lo + 1.0

        left = 52
        right = width - 20
        top = 18
        bottom = height - 38
        plot_w = max(100, right - left)
        plot_h = max(80, bottom - top)

        for i in range(5):
            x = left + plot_w * i / 4
            y = top + plot_h * i / 4
            canvas.create_line(x, top, x, bottom, fill=CHART_COLORS["grid"])
            canvas.create_line(left, y, right, y, fill=CHART_COLORS["grid"])

        canvas.create_line(left, top, left, bottom, fill=CHART_COLORS["axis"])
        canvas.create_line(left, bottom, right, bottom, fill=CHART_COLORS["axis"])

        for x, y, row in points:
            px = left + ((x - x_lo) / (x_hi - x_lo)) * plot_w
            py = bottom - ((y - y_lo) / (y_hi - y_lo)) * plot_h

            sid = safe_text(row.get("strategy_id"))
            is_selected = self._selected_row is not None and sid == safe_text(self._selected_row.get("strategy_id"))

            color = CHART_COLORS["point_selected"] if is_selected else CHART_COLORS["point"]
            r = SCATTER_POINT_RADIUS + (1 if is_selected else 0)
            canvas.create_oval(px - r, py - r, px + r, py + r, fill=color, outline=color)

        canvas.create_text(left, bottom + 14, text=fmt_float(x_lo, 2), fill=FG_MUTED, font=FONT_SMALL, anchor="w")
        canvas.create_text(right, bottom + 14, text=fmt_float(x_hi, 2), fill=FG_MUTED, font=FONT_SMALL, anchor="e")

        canvas.create_text(left - 8, bottom, text=fmt_float(y_lo, 2), fill=FG_MUTED, font=FONT_SMALL, anchor="e")
        canvas.create_text(left - 8, top, text=fmt_float(y_hi, 2), fill=FG_MUTED, font=FONT_SMALL, anchor="e")

        canvas.create_text(width / 2, height - 10, text=x_label, fill=FG_MUTED, font=FONT_SMALL)
        canvas.create_text(16, height / 2, text=y_label, fill=FG_MUTED, font=FONT_SMALL, angle=90)

    def _render_score_chart(self):
        c = self.chart_canvas
        c.delete("all")

        width = max(c.winfo_width(), 300)
        height = max(c.winfo_height(), 220)

        if not self.summary_rows:
            c.create_text(width / 2, height / 2, text="No data", fill=FG_MUTED, font=FONT_SECTION)
            return

        rows = self.summary_rows[:CHART_MAX_SYMBOLS]
        vals = [to_float(r.get("avg_score")) for r in rows]
        vals = [x for x in vals if x is not None]
        max_val = max(vals) if vals else 1.0
        if max_val <= 0:
            max_val = 1.0

        left = 60
        right = width - 20
        top = 20
        bottom = height - 45
        plot_w = max(100, right - left)
        plot_h = max(80, bottom - top)

        c.create_line(left, top, left, bottom, fill=CHART_COLORS["axis"])
        c.create_line(left, bottom, right, bottom, fill=CHART_COLORS["axis"])

        for i in range(5):
            y = top + (plot_h / 4) * i
            c.create_line(left, y, right, y, fill=CHART_COLORS["grid"])
            val = round(max_val * (1 - i / 4), 3)
            c.create_text(left - 8, y, text=str(val), fill=FG_MUTED, font=FONT_LABEL, anchor="e")

        group_w = plot_w / max(len(rows), 1)
        bar_w = max(10, min(30, group_w / 2.5))

        for idx, row in enumerate(rows):
            center_x = left + group_w * idx + group_w / 2
            val = to_float(row.get("avg_score")) or 0.0
            symbol = safe_text(row.get("symbol"))

            x1 = center_x - bar_w / 2
            x2 = center_x + bar_w / 2
            y2 = bottom - 1
            y1 = bottom - (val / max_val) * (plot_h - 6)

            c.create_rectangle(x1, y1, x2, y2, fill=CHART_COLORS["score"], outline=CHART_COLORS["score"])
            c.create_text(center_x, bottom + 14, text=symbol, fill=FG_MUTED, font=FONT_LABEL, anchor="n")

    def _render_top_bottom_compare(self):
        c = self.top_bottom_canvas
        c.delete("all")
        width = max(c.winfo_width(), 300)
        height = max(c.winfo_height(), 220)

        if not self.filtered_rows:
            c.create_text(width / 2, height / 2, text="No data", fill=FG_MUTED, font=FONT_SECTION)
            return

        metrics = [
            ("profit_factor_oos", "PF OOS"),
            ("expectancy_oos", "Exp OOS"),
            ("net_profit_oos", "NP OOS"),
            ("win_rate_oos", "Win OOS"),
            ("payoff_ratio_total", "Payoff"),
        ]

        stats_rows: List[Tuple[str, float, float]] = []
        max_abs = 0.0
        for key, label in metrics:
            st = build_top_bottom_stats(self.filtered_rows, key, top_frac=0.20)
            top_mean = to_float(st.get("top_mean")) or 0.0
            bottom_mean = to_float(st.get("bottom_mean")) or 0.0
            stats_rows.append((label, top_mean, bottom_mean))
            max_abs = max(max_abs, abs(top_mean), abs(bottom_mean))

        if max_abs <= 1e-12:
            max_abs = 1.0

        left = 110
        right = width - 20
        top = 22
        bottom = height - 20
        row_h = max(28, (bottom - top) / max(len(stats_rows), 1))
        mid = left + (right - left) / 2

        c.create_line(mid, top - 8, mid, bottom, fill=CHART_COLORS["axis"])
        c.create_text(mid - 70, 8, text="Top 20%", fill=FG_POS, font=FONT_SECTION)
        c.create_text(mid + 70, 8, text="Bottom 20%", fill=FG_NEG, font=FONT_SECTION)

        for idx, (label, top_mean, bottom_mean) in enumerate(stats_rows):
            y = top + idx * row_h + row_h / 2
            c.create_text(10, y, text=label, fill=FG_MAIN, font=FONT_LABEL, anchor="w")

            bar_half = (right - left) / 2 - 14
            top_len = (abs(top_mean) / max_abs) * bar_half
            bottom_len = (abs(bottom_mean) / max_abs) * bar_half

            c.create_rectangle(mid - top_len, y - 8, mid - 2, y + 8, fill=FG_POS, outline=FG_POS)
            c.create_rectangle(mid + 2, y - 8, mid + bottom_len, y + 8, fill=FG_NEG, outline=FG_NEG)

            c.create_text(mid - top_len - 6, y, text=fmt_float(top_mean, 2), fill=FG_MUTED, font=FONT_SMALL, anchor="e")
            c.create_text(mid + bottom_len + 6, y, text=fmt_float(bottom_mean, 2), fill=FG_MUTED, font=FONT_SMALL, anchor="w")

    def _render_archetype_chart(self):
        c = self.archetype_canvas
        c.delete("all")
        width = max(c.winfo_width(), 300)
        height = max(c.winfo_height(), 220)

        if not self.filtered_rows:
            c.create_text(width / 2, height / 2, text="No data", fill=FG_MUTED, font=FONT_SECTION)
            return

        ctr = Counter(safe_text(r.get("archetype")) for r in self.filtered_rows if safe_text(r.get("archetype")))
        rows = ctr.most_common(6)
        if not rows:
            c.create_text(width / 2, height / 2, text="No data", fill=FG_MUTED, font=FONT_SECTION)
            return

        max_count = max(v for _, v in rows) or 1

        left = 140
        right = width - 20
        top = 20
        bottom = height - 20
        row_h = max(26, (bottom - top) / len(rows))
        bar_w = right - left

        for idx, (name, count) in enumerate(rows):
            y = top + idx * row_h + row_h / 2
            length = (count / max_count) * bar_w
            c.create_text(10, y, text=name, fill=FG_MAIN, font=FONT_LABEL, anchor="w")
            c.create_rectangle(left, y - 8, left + length, y + 8, fill=CHART_COLORS["bar_alt"], outline=CHART_COLORS["bar_alt"])
            c.create_text(left + length + 8, y, text=str(count), fill=FG_MUTED, font=FONT_SMALL, anchor="w")

    def _render_feature_usage_chart(self):
        c = self.feature_usage_canvas
        c.delete("all")
        width = max(c.winfo_width(), 300)
        height = max(c.winfo_height(), 220)

        rows = self.feature_usage_rows[:8]
        if not rows:
            c.create_text(width / 2, height / 2, text="No data", fill=FG_MUTED, font=FONT_SECTION)
            return

        max_count = max(int(r.get("count", 0)) for r in rows) or 1
        left = 180
        right = width - 20
        top = 18
        bottom = height - 20
        row_h = max(24, (bottom - top) / len(rows))
        bar_w = right - left

        for idx, row in enumerate(rows):
            y = top + idx * row_h + row_h / 2
            name = pretty_feature_name(safe_text(row.get("feature")))
            count = int(row.get("count", 0))
            share = to_float(row.get("share_in_top_k")) or 0.0
            length = (count / max_count) * bar_w

            c.create_text(10, y, text=name, fill=FG_MAIN, font=FONT_LABEL, anchor="w")
            c.create_rectangle(left, y - 8, left + length, y + 8, fill=CHART_COLORS["bar"], outline=CHART_COLORS["bar"])
            c.create_text(left + length + 8, y, text=f"{count} | {share*100:.1f}%", fill=FG_MUTED, font=FONT_SMALL, anchor="w")

    # ========================================================
    # EVENTS
    # ============================================================

    def _on_filter_change(self, _event=None):
        if self._filter_job is not None:
            try:
                self.after_cancel(self._filter_job)
            except Exception:
                pass
        self._filter_job = self.after(120, self._apply_filters)

    def _on_summary_select(self, _event=None):
        selected = self.summary_tree.selection()
        if not selected:
            return

        values = self.summary_tree.item(selected[0], "values")
        if not values:
            return

        symbol = safe_text(values[0])
        if symbol in list(self.symbol_combo["values"]):
            self.symbol_var.set(symbol)
            self._apply_filters()

    def _on_pivot_select(self, _event=None):
        selected = self.pivot_tree.selection()
        if not selected:
            return

        values = self.pivot_tree.item(selected[0], "values")
        if not values:
            return

        symbol = safe_text(values[0])
        if symbol == "Gesamtergebnis":
            return

        if symbol in list(self.symbol_combo["values"]):
            self.symbol_var.set(symbol)
            self._apply_filters()

    def _on_row_double_click(self, _event=None):
        selected = self.table.selection()
        if not selected:
            return
        iid = selected[0]
        row = self._table_row_map.get(iid)
        if not row:
            return
        self._open_detail_window(row)

    def _on_table_select(self, _event=None):
        selected = self.table.selection()
        if not selected:
            self._selected_row = None
            self._update_selected_strategy_panel(None)
            self._render_all_charts()
            return

        iid = selected[0]
        row = self._table_row_map.get(iid)
        self._selected_row = row
        self._update_selected_strategy_panel(row)
        self._render_all_charts()

    def _on_model_select(self, _event=None):
        selected = self.models_tree.selection()
        if not selected:
            return
        iid = selected[0]
        values = self.models_tree.item(iid, "values")
        if not values:
            return

        rank = to_int(values[0])
        if rank is None:
            return

        for row in self.top_models_rows:
            if to_int(row.get("rank")) == rank:
                self._show_model_detail(row)
                return

    # ========================================================
    # DETAIL WINDOW
    # ============================================================

    def _open_detail_window(self, row: Dict[str, Any]):
        win = tk.Toplevel(self)
        win.title(f"Performance Detail | {safe_text(row.get('strategy_id'))}")
        win.geometry("1240x860")
        win.minsize(1020, 720)
        win.configure(bg=BG_MAIN)

        header = tk.Frame(win, bg=BG_TOP, highlightbackground=BORDER, highlightthickness=1, height=52)
        header.pack(fill="x", padx=10, pady=10)
        header.pack_propagate(False)

        tk.Label(
            header,
            text="PERFORMANCE DETAIL",
            font=FONT_TITLE,
            bg=BG_TOP,
            fg=FG_WHITE,
        ).pack(side="left", padx=12)

        tk.Label(
            header,
            text=safe_text(row.get("strategy_id")),
            font=("Segoe UI", 9),
            bg=BG_TOP,
            fg=FG_MUTED,
        ).pack(side="right", padx=12)

        kpi_row = tk.Frame(win, bg=BG_MAIN)
        kpi_row.pack(fill="x", padx=10, pady=(0, 10))

        for title, value, color in [
            ("Symbol", safe_text(row.get("symbol")), FG_MAIN),
            ("Side", safe_text(row.get("side")), FG_MAIN),
            ("TF", safe_text(row.get("timeframe")), FG_MAIN),
            ("PF OOS", fmt_float(row.get("profit_factor_oos"), 4), ratio_color(to_float(row.get("profit_factor_oos")), 1.2, 1.0)),
            ("Score", fmt_float(row.get("best_candidate_score"), 4), FG_ACCENT),
            ("Exp OOS", fmt_float(row.get("expectancy_oos"), 4), sign_color(to_float(row.get("expectancy_oos")))),
        ]:
            card = KpiCard(kpi_row, title)
            card.pack(side="left", fill="x", expand=True, padx=4)
            card.set_value(value, color=color)

        body = tk.PanedWindow(win, orient="horizontal", bg=BG_MAIN, sashwidth=6)
        body.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        left = tk.Frame(body, bg=BG_SURFACE, highlightbackground=BORDER, highlightthickness=1)
        right = tk.Frame(body, bg=BG_SURFACE, highlightbackground=BORDER, highlightthickness=1)
        body.add(left, minsize=420)
        body.add(right, minsize=700)

        tk.Label(left, text="Metric Detail", font=FONT_SECTION, bg=BG_SURFACE, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))

        text = tk.Text(
            left,
            wrap="none",
            bg=BG_SURFACE,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            font=FONT_MONO,
        )
        text.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        fields = [
            "strategy_id", "symbol", "side", "timeframe", "archetype",
            "trade_count_total", "trade_count_is", "trade_count_oos",
            "active_months", "trades_per_month",
            "net_profit_total", "net_profit_is", "net_profit_oos",
            "win_rate_total", "win_rate_is", "win_rate_oos",
            "avg_win_total", "avg_loss_total", "payoff_ratio_total",
            "expectancy_total", "expectancy_is", "expectancy_oos",
            "profit_factor_total", "profit_factor_is", "profit_factor_oos",
            "best_candidate_score", "proxy_target",
            "first_trade_time_utc", "last_trade_time_utc",
        ]

        lines = [f"{key:<28}: {safe_text(row.get(key))}" for key in fields]
        text.insert("1.0", "\n".join(lines))
        text.configure(state="disabled")

        chart_card = tk.Frame(right, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        chart_card.pack(fill="both", expand=True, padx=10, pady=10)

        tk.Label(chart_card, text="Performance Profile", font=FONT_SECTION, bg=BG_CARD, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10, 8))

        canvas = tk.Canvas(chart_card, bg=BG_CARD, highlightthickness=0)
        canvas.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        def draw_profile():
            canvas.delete("all")
            width = max(canvas.winfo_width(), 500)
            height = max(canvas.winfo_height(), 300)

            metrics = [
                ("Score", to_float(row.get("best_candidate_score")), 1.0, FG_ACCENT),
                ("PF OOS", to_float(row.get("profit_factor_oos")), 2.0, FG_POS),
                ("Exp OOS", to_float(row.get("expectancy_oos")), max(1.0, (to_float(row.get("expectancy_oos")) or 0.0) * 1.1), FG_WARN),
                ("Win OOS", to_float(row.get("win_rate_oos")), 1.0, "#A78BFA"),
                ("Payoff", to_float(row.get("payoff_ratio_total")), 2.0, "#F97316"),
            ]

            left = 140
            right = width - 30
            top = 30
            row_h = 48
            max_w = right - left

            for idx, (label, value, ref, color) in enumerate(metrics):
                y = top + idx * row_h
                canvas.create_text(10, y + 10, text=label, fill=FG_MAIN, font=FONT_LABEL, anchor="w")
                canvas.create_rectangle(left, y, right, y + 18, fill=BG_PANEL_2, outline=BG_PANEL_2)

                val = max(0.0, float(value or 0.0))
                ref_val = max(ref, 1e-9)
                frac = clamp(val / ref_val, 0.0, 1.0)
                canvas.create_rectangle(left, y, left + max_w * frac, y + 18, fill=color, outline=color)

                canvas.create_text(right, y + 10, text=fmt_float(value, 4), fill=FG_MUTED, font=FONT_SMALL, anchor="e")

        canvas.bind("<Configure>", lambda _e: draw_profile())
        draw_profile()

    # ========================================================
    # AUTO REFRESH
    # ============================================================

    def _on_toggle_auto_refresh(self):
        self.live_status_var.set(
            "LIVE | auto refresh enabled" if self.auto_refresh_enabled.get() else "LIVE | auto refresh disabled"
        )
        self._schedule_auto_refresh()

    def _schedule_auto_refresh(self):
        if self._refresh_job is not None:
            try:
                self.after_cancel(self._refresh_job)
            except Exception:
                pass
            self._refresh_job = None

        if self.auto_refresh_enabled.get():
            self._refresh_job = self.after(AUTO_REFRESH_MS, self._auto_refresh_tick)

    def _auto_refresh_tick(self):
        try:
            if self.auto_refresh_enabled.get():
                self._refresh_all(live=True)
        finally:
            self._schedule_auto_refresh()

    def destroy(self):
        if self._refresh_job is not None:
            try:
                self.after_cancel(self._refresh_job)
            except Exception:
                pass
            self._refresh_job = None

        if self._filter_job is not None:
            try:
                self.after_cancel(self._filter_job)
            except Exception:
                pass
            self._filter_job = None

        super().destroy()


# ============================================================
# STANDALONE WINDOW
# ============================================================

class PerformanceDashboard(tk.Tk):
    def __init__(self, repo: PerformanceRepository):
        super().__init__()

        self.title(APP_TITLE)
        self.geometry("1880x1120")
        self.minsize(1480, 920)
        self.configure(bg=BG_MAIN)

        self.panel = PerformanceDashboardPanel(self, repo=repo)
        self.panel.pack(fill="both", expand=True)

    def destroy(self):
        try:
            self.panel.destroy()
        except Exception:
            pass
        super().destroy()


# ============================================================
# MAIN
# ============================================================

def main():
    if not PERFORMANCE_ROOT.exists():
        raise RuntimeError(f"Performance Root nicht gefunden: {PERFORMANCE_ROOT}")

    repo = PerformanceRepository(PERFORMANCE_ROOT)
    app = PerformanceDashboard(repo)
    app.mainloop()


if __name__ == "__main__":
    main()