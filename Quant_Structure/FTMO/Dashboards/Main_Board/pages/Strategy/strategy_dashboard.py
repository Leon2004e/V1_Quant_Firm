# -*- coding: utf-8 -*-
"""
Dashboards/Main_Board/pages/Strategy/strategy_dashboard.py

Strategy Dashboard
- kompatibel mit Main Board Loader
- kein pandas erforderlich
- kein matplotlib erforderlich
- dunkler Stil passend zum Main Board
- standalone nutzbar
- als eingebettetes Panel nutzbar
- liest Strategy_Profile JSON-Dateien
- Filter, KPI, Summary, Pivot, Canvas-Chart, Detailfenster
- Sortieren per Spaltenkopf
- Filterzeile direkt über der Tabelle

Start standalone:
    python Quant_Structure/FTMO/Dashboards/Main_Board/pages/Strategy/strategy_dashboard.py
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
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

STRATEGY_PROFILE_ROOT = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Strategy"
    / "Strategy_Profile"
)


# ============================================================
# CONFIG
# ============================================================

APP_TITLE = "Strategy Dashboard"
AUTO_REFRESH_MS = 5000
AUTO_REFRESH_DEFAULT = True
TABLE_ROW_LIMIT = 5000
SUMMARY_ROW_LIMIT = 500
PIVOT_ROW_LIMIT = 500
CHART_MAX_SYMBOLS = 12


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

CHART_COLORS = {
    "BUY": "#22C55E",
    "SELL": "#EF4444",
    "BOTH": "#93A4B5",
    "UNKNOWN": "#708396",
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


def fmt_int(x: object) -> str:
    try:
        return f"{int(x):,}"
    except Exception:
        return "-"


def unique_sorted(values: List[str], with_all: bool = True) -> List[str]:
    cleaned = sorted({safe_text(v) for v in values if safe_text(v)})
    return (["ALL"] + cleaned) if with_all else cleaned


def get_nested(d: Dict[str, Any], path: str, default=None):
    cur = d
    for key in path.split("."):
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def as_bool_label(v: Any) -> str:
    if v is True:
        return "YES"
    if v is False:
        return "NO"
    return "UNKNOWN"


def safe_read_json(path: Path) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def make_tree_iid(prefix: str, row_index: int, key: str = "") -> str:
    clean_key = (
        safe_text(key)
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
    )
    return f"{prefix}__{row_index}__{clean_key}"


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


def contains_ci(value: Any, needle: str) -> bool:
    return needle in safe_text(value).lower()


# ============================================================
# REPOSITORY
# ============================================================

class StrategyProfileRepository:
    def __init__(self, root: Path):
        self.root = root

    def scan(self) -> List[Dict[str, Any]]:
        if not self.root.exists():
            return []

        rows: List[Dict[str, Any]] = []

        for json_path in sorted(self.root.rglob("*.json")):
            profile = safe_read_json(json_path)
            if not profile:
                continue

            identity = profile.get("identity", {})
            naming = profile.get("profile_naming", {})
            classification = profile.get("classification", {})
            risk_model = profile.get("risk_model", {})
            fixed = get_nested(profile, "trade_parameters.fixed", {}) or {}
            dynamic = get_nested(profile, "trade_parameters.dynamic_coef", {}) or {}
            mm = profile.get("money_management", {})
            filters = profile.get("filters", {})
            checks = profile.get("checks", {})
            signal_family = classification.get("signal_family", [])

            if isinstance(signal_family, list):
                signal_family_label = "_".join(
                    [safe_text(x) for x in signal_family if safe_text(x)]
                ) or "UNKNOWN"
            else:
                signal_family_label = safe_text(signal_family) or "UNKNOWN"

            symbol = safe_text(identity.get("symbol", "unknown")).upper() or "UNKNOWN"
            variant_number = identity.get("variant_number")
            strategy_id = safe_text(identity.get("strategy_id", "unknown")) or "UNKNOWN"
            side = safe_text(identity.get("side", "unknown")).upper() or "UNKNOWN"
            timeframe = safe_text(identity.get("timeframe", "unknown")).upper() or "UNKNOWN"

            profile_file = json_path.name
            profile_relative_path = str(json_path.relative_to(self.root))

            row = {
                "profile_file": profile_file,
                "profile_path": str(json_path.resolve()),
                "profile_relative_path": profile_relative_path,
                "symbol_folder": json_path.parent.name,

                "schema_version": safe_text(profile.get("schema_version")),
                "ea_file": safe_text(get_nested(profile, "source.ea_file")),
                "ea_path": safe_text(get_nested(profile, "source.ea_path")),

                "symbol": symbol,
                "variant_number": variant_number,
                "strategy_id": strategy_id,
                "side": side,
                "timeframe": timeframe,

                "base_name": safe_text(naming.get("base_name")),
                "exit_label": safe_text(naming.get("exit_label")),
                "signal_label": safe_text(naming.get("signal_label")),
                "time_label": safe_text(naming.get("time_label")),
                "display_name": safe_text(naming.get("display_name")),
                "extended_display_name": safe_text(naming.get("extended_display_name")),

                "sl_type": safe_text(classification.get("sl_type")),
                "tp_type": safe_text(classification.get("tp_type")),
                "trailing_type": safe_text(classification.get("trailing_type")),
                "exit_profile": safe_text(classification.get("exit_profile")),
                "signal_family": signal_family_label,
                "overlap_key": safe_text(classification.get("overlap_key")),

                "risk_model_type": safe_text(risk_model.get("type")),
                "fixed_sl": fixed.get("stop_loss_pips"),
                "fixed_tp": fixed.get("take_profit_pips"),
                "sl_coef": dynamic.get("stop_loss_coef"),
                "tp_coef": dynamic.get("take_profit_coef"),
                "trailing_coef": dynamic.get("trailing_stop_coef"),

                "mm_enabled": as_bool_label(mm.get("enabled")),
                "risk_percent": mm.get("risk_percent"),
                "fixed_lot": mm.get("fixed_lot"),
                "initial_capital": mm.get("initial_capital"),

                "limit_time_range": as_bool_label(get_nested(filters, "limit_time_range.enabled")),
                "time_from": safe_text(get_nested(filters, "limit_time_range.from")),
                "time_to": safe_text(get_nested(filters, "limit_time_range.to")),
                "eod_exit": as_bool_label(get_nested(filters, "exit_at_end_of_day.enabled")),
                "friday_exit": as_bool_label(get_nested(filters, "exit_on_friday.enabled")),
                "weekend_protection": as_bool_label(get_nested(filters, "dont_trade_on_weekends.enabled")),
                "max_trades_per_day": filters.get("max_trades_per_day"),

                "has_defined_stop_loss": as_bool_label(checks.get("has_defined_stop_loss")),
                "has_defined_take_profit_or_exit": as_bool_label(checks.get("has_defined_take_profit_or_exit")),
                "uses_aggressive_mm": as_bool_label(checks.get("uses_aggressive_mm")),
                "has_parser_uncertainty": as_bool_label(checks.get("has_parser_uncertainty")),
                "has_tight_stop": as_bool_label(checks.get("has_tight_stop")),
            }

            row["strategy_uid"] = f"{symbol}__{variant_number}__{strategy_id}__{side}__{timeframe}"
            row["row_key"] = f"{profile_relative_path}__{profile_file}"
            row["variant_sort"] = to_float(variant_number)
            row["risk_percent_sort"] = to_float(row["risk_percent"])
            row["fixed_sl_sort"] = to_float(row["fixed_sl"])
            row["fixed_tp_sort"] = to_float(row["fixed_tp"])

            rows.append(row)

        rows.sort(
            key=lambda r: (
                safe_text(r.get("symbol")),
                float("inf") if r.get("variant_sort") is None else r.get("variant_sort"),
                safe_text(r.get("strategy_id")),
                safe_text(r.get("side")),
                safe_text(r.get("timeframe")),
                safe_text(r.get("profile_file")),
            )
        )
        return rows


# ============================================================
# AGGREGATIONS
# ============================================================

def build_symbol_summary(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[safe_text(row.get("symbol"))].append(row)

    out: List[Dict[str, Any]] = []
    for symbol, items in grouped.items():
        tfs = sorted({safe_text(x.get("timeframe")) for x in items if safe_text(x.get("timeframe"))})
        out.append(
            {
                "symbol": symbol,
                "count": len(items),
                "buy": sum(1 for x in items if safe_text(x.get("side")) == "BUY"),
                "sell": sum(1 for x in items if safe_text(x.get("side")) == "SELL"),
                "both": sum(1 for x in items if safe_text(x.get("side")) == "BOTH"),
                "timeframes": ", ".join(tfs),
            }
        )

    out.sort(key=lambda r: (-int(r["count"]), safe_text(r["symbol"])))
    return out


def build_symbol_side_pivot(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []

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
        out.append(
            {
                "symbol": symbol,
                "BUY": buy,
                "SELL": sell,
                "BOTH": both,
                "UNKNOWN": unknown,
                "TOTAL": total,
            }
        )

    out.sort(key=lambda r: (-int(r["TOTAL"]), safe_text(r["symbol"])))
    return out


# ============================================================
# UI WIDGETS
# ============================================================

class KpiCard(tk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        self.configure(height=74)
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

    def set_value(self, value: str, color: Optional[str] = None):
        self.value_var.set(value)
        self.value_label.configure(fg=color or FG_MAIN)


# ============================================================
# PANEL
# ============================================================

class StrategyDashboardPanel(tk.Frame):
    def __init__(self, parent, repo: StrategyProfileRepository):
        super().__init__(parent, bg=BG_MAIN)

        self.repo = repo

        self.raw_rows: List[Dict[str, Any]] = []
        self.filtered_rows: List[Dict[str, Any]] = []
        self.summary_rows: List[Dict[str, Any]] = []
        self.pivot_rows: List[Dict[str, Any]] = []

        self.auto_refresh_enabled = tk.BooleanVar(value=AUTO_REFRESH_DEFAULT)
        self._refresh_job: Optional[str] = None
        self._filter_job: Optional[str] = None
        self.last_refresh_label = "-"

        self.search_var = tk.StringVar(value="")
        self.symbol_var = tk.StringVar(value="ALL")
        self.side_var = tk.StringVar(value="ALL")
        self.timeframe_var = tk.StringVar(value="ALL")
        self.exit_profile_var = tk.StringVar(value="ALL")
        self.signal_family_var = tk.StringVar(value="ALL")
        self.risk_model_var = tk.StringVar(value="ALL")

        self.col_filter_symbol = tk.StringVar(value="")
        self.col_filter_variant = tk.StringVar(value="")
        self.col_filter_strategy_id = tk.StringVar(value="")
        self.col_filter_side = tk.StringVar(value="")
        self.col_filter_timeframe = tk.StringVar(value="")
        self.col_filter_exit = tk.StringVar(value="")

        self._sort_state: Dict[str, bool] = {}
        self._table_row_map: Dict[str, Dict[str, Any]] = {}

        self._configure_ttk_style()
        self._build_ui()
        self._refresh_all(live=False)
        self._schedule_auto_refresh()

    # ========================================================
    # STYLE
    # ========================================================

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
            text="Strategy",
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

        self.info_var = tk.StringVar(value=str(STRATEGY_PROFILE_ROOT))
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

        make_label("Exit")
        self.exit_profile_combo = ttk.Combobox(controls, state="readonly", width=24, textvariable=self.exit_profile_var)
        self.exit_profile_combo.pack(side="left", padx=(0, 10))
        self.exit_profile_combo.bind("<<ComboboxSelected>>", self._on_filter_change)

        make_label("Signal")
        self.signal_family_combo = ttk.Combobox(controls, state="readonly", width=16, textvariable=self.signal_family_var)
        self.signal_family_combo.pack(side="left", padx=(0, 10))
        self.signal_family_combo.bind("<<ComboboxSelected>>", self._on_filter_change)

        make_label("Risk")
        self.risk_model_combo = ttk.Combobox(controls, state="readonly", width=16, textvariable=self.risk_model_var)
        self.risk_model_combo.pack(side="left", padx=(0, 10))
        self.risk_model_combo.bind("<<ComboboxSelected>>", self._on_filter_change)

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
            text="Clear Column Filters",
            command=self._clear_column_filters,
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

        kpi_row = tk.Frame(root, bg=BG_MAIN)
        kpi_row.pack(fill="x", pady=(0, 12))

        self.card_total = KpiCard(kpi_row, "Total Strategies")
        self.card_total.pack(side="left", fill="x", expand=True, padx=4)

        self.card_symbols = KpiCard(kpi_row, "Symbols")
        self.card_symbols.pack(side="left", fill="x", expand=True, padx=4)

        self.card_aggr = KpiCard(kpi_row, "Aggressive MM")
        self.card_aggr.pack(side="left", fill="x", expand=True, padx=4)

        self.card_uncertain = KpiCard(kpi_row, "Parser Uncertainty")
        self.card_uncertain.pack(side="left", fill="x", expand=True, padx=4)

        self.card_stops = KpiCard(kpi_row, "Defined Stop Loss")
        self.card_stops.pack(side="left", fill="x", expand=True, padx=4)

        self.card_selected = KpiCard(kpi_row, "Filtered Rows")
        self.card_selected.pack(side="left", fill="x", expand=True, padx=4)

        main = tk.PanedWindow(root, orient="horizontal", bg=BG_MAIN, sashwidth=6)
        main.pack(fill="both", expand=True)

        left = tk.Frame(main, bg=BG_SURFACE, highlightbackground=BORDER, highlightthickness=1)
        right = tk.Frame(main, bg=BG_SURFACE, highlightbackground=BORDER, highlightthickness=1)

        main.add(left, minsize=430)
        main.add(right, minsize=1100)

        self._build_left_panel(left)
        self._build_right_panel(right)

    def _build_left_panel(self, parent: tk.Frame):
        tk.Label(
            parent,
            text="Symbol Summary",
            font=FONT_SECTION,
            bg=BG_SURFACE,
            fg=FG_WHITE,
        ).pack(anchor="w", padx=10, pady=(10, 8))

        cols = ("symbol", "count", "buy", "sell", "both", "timeframes")
        self.summary_tree = ttk.Treeview(parent, columns=cols, show="headings", height=14)
        self.summary_tree.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.summary_tree.bind("<<TreeviewSelect>>", self._on_summary_select)

        widths = {"symbol": 110, "count": 70, "buy": 60, "sell": 60, "both": 60, "timeframes": 130}
        for col in cols:
            self.summary_tree.heading(col, text=col)
            self.summary_tree.column(col, width=widths[col], anchor="w")

        info_box = tk.Frame(parent, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        info_box.pack(fill="both", expand=False, padx=10, pady=(0, 10))

        tk.Label(
            info_box,
            text="Selection Info",
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

    def _build_right_panel(self, parent: tk.Frame):
        notebook = ttk.Notebook(parent)
        notebook.pack(fill="both", expand=True, padx=10, pady=10)

        tab_table = tk.Frame(notebook, bg=BG_SURFACE)
        tab_pivot = tk.Frame(notebook, bg=BG_SURFACE)

        notebook.add(tab_table, text="Strategy Table")
        notebook.add(tab_pivot, text="Pivot + Chart")

        self._build_table_tab(tab_table)
        self._build_pivot_tab(tab_pivot)

    def _build_table_tab(self, parent: tk.Frame):
        tk.Label(
            parent,
            text="Strategy Profile Table",
            font=FONT_SECTION,
            bg=BG_SURFACE,
            fg=FG_WHITE,
        ).pack(anchor="w", padx=10, pady=(10, 8))

        filter_bar = tk.Frame(parent, bg=BG_SURFACE)
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
        make_filter_entry(filter_bar, "variant", self.col_filter_variant, 10)
        make_filter_entry(filter_bar, "strategy_id", self.col_filter_strategy_id, 12)
        make_filter_entry(filter_bar, "side", self.col_filter_side, 8)
        make_filter_entry(filter_bar, "timeframe", self.col_filter_timeframe, 10)
        make_filter_entry(filter_bar, "exit_profile", self.col_filter_exit, 18)

        table_shell = tk.Frame(parent, bg=BG_SURFACE)
        table_shell.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        cols = (
            "symbol",
            "variant_number",
            "strategy_id",
            "side",
            "timeframe",
            "exit_profile",
            "signal_family",
            "risk_model_type",
            "risk_percent",
            "fixed_sl",
            "fixed_tp",
            "time_label",
            "display_name",
        )

        self.table = ttk.Treeview(table_shell, columns=cols, show="headings", height=24)
        self.table.pack(side="left", fill="both", expand=True)
        self.table.bind("<Double-1>", self._on_row_double_click)

        scrollbar_y = ttk.Scrollbar(table_shell, orient="vertical", command=self.table.yview)
        scrollbar_y.pack(side="right", fill="y")
        self.table.configure(yscrollcommand=scrollbar_y.set)

        widths = {
            "symbol": 90,
            "variant_number": 80,
            "strategy_id": 100,
            "side": 80,
            "timeframe": 80,
            "exit_profile": 190,
            "signal_family": 130,
            "risk_model_type": 130,
            "risk_percent": 90,
            "fixed_sl": 80,
            "fixed_tp": 80,
            "time_label": 120,
            "display_name": 360,
        }

        for col in cols:
            self.table.heading(col, text=col, command=lambda c=col: self._sort_main_table_by(c))
            self.table.column(col, width=widths[col], anchor="w")

    def _build_pivot_tab(self, parent: tk.Frame):
        top = tk.Frame(parent, bg=BG_SURFACE)
        top.pack(fill="both", expand=True)

        pivot_frame = tk.Frame(top, bg=BG_SURFACE, highlightbackground=BORDER, highlightthickness=1)
        pivot_frame.pack(fill="x", padx=10, pady=(10, 10))

        tk.Label(
            pivot_frame,
            text="Pivot Table | Symbol x Side",
            font=FONT_SECTION,
            bg=BG_SURFACE,
            fg=FG_WHITE,
        ).pack(anchor="w", padx=10, pady=(10, 8))

        pivot_shell = tk.Frame(pivot_frame, bg=BG_SURFACE)
        pivot_shell.pack(fill="x", padx=10, pady=(0, 10))

        cols = ("symbol", "BUY", "SELL", "BOTH", "UNKNOWN", "TOTAL")
        self.pivot_tree = ttk.Treeview(pivot_shell, columns=cols, show="headings", height=10)
        self.pivot_tree.pack(side="left", fill="x", expand=True)
        self.pivot_tree.bind("<<TreeviewSelect>>", self._on_pivot_select)

        pivot_scroll = ttk.Scrollbar(pivot_shell, orient="vertical", command=self.pivot_tree.yview)
        pivot_scroll.pack(side="right", fill="y")
        self.pivot_tree.configure(yscrollcommand=pivot_scroll.set)

        widths = {"symbol": 110, "BUY": 70, "SELL": 70, "BOTH": 70, "UNKNOWN": 80, "TOTAL": 70}
        for col in cols:
            self.pivot_tree.heading(col, text=col)
            self.pivot_tree.column(col, width=widths[col], anchor="center" if col != "symbol" else "w")

        chart_frame = tk.Frame(top, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        chart_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        tk.Label(
            chart_frame,
            text="Chart | Strategies per Symbol",
            font=FONT_SECTION,
            bg=BG_CARD,
            fg=FG_WHITE,
        ).pack(anchor="w", padx=10, pady=(10, 8))

        self.chart_canvas = tk.Canvas(
            chart_frame,
            bg=BG_CARD,
            highlightthickness=0,
            relief="flat",
        )
        self.chart_canvas.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.chart_canvas.bind("<Configure>", lambda _e: self._render_pivot_chart())

    # ========================================================
    # DATA FLOW
    # ============================================================

    def _refresh_all(self, live: bool = False):
        try:
            self.raw_rows = self.repo.scan()
            self._reload_filter_options()
            self._apply_filters()

            import datetime as _dt
            self.last_refresh_label = _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            mode = "LIVE" if live else "MANUAL"
            self.live_status_var.set(f"{mode} | last refresh {self.last_refresh_label}")
        except Exception as e:
            self.live_status_var.set(f"ERROR | {e}")

    def _reload_filter_options(self):
        rows = self.raw_rows

        symbols = unique_sorted([safe_text(r.get("symbol")) for r in rows]) if rows else ["ALL"]
        sides = unique_sorted([safe_text(r.get("side")) for r in rows]) if rows else ["ALL"]
        timeframes = unique_sorted([safe_text(r.get("timeframe")) for r in rows]) if rows else ["ALL"]
        exits = unique_sorted([safe_text(r.get("exit_profile")) for r in rows]) if rows else ["ALL"]
        signals = unique_sorted([safe_text(r.get("signal_family")) for r in rows]) if rows else ["ALL"]
        risks = unique_sorted([safe_text(r.get("risk_model_type")) for r in rows]) if rows else ["ALL"]

        def keep_or_all(var: tk.StringVar, values: List[str]):
            cur = var.get() or "ALL"
            var.set(cur if cur in values else "ALL")

        self.symbol_combo["values"] = symbols
        self.side_combo["values"] = sides
        self.timeframe_combo["values"] = timeframes
        self.exit_profile_combo["values"] = exits
        self.signal_family_combo["values"] = signals
        self.risk_model_combo["values"] = risks

        keep_or_all(self.symbol_var, symbols)
        keep_or_all(self.side_var, sides)
        keep_or_all(self.timeframe_var, timeframes)
        keep_or_all(self.exit_profile_var, exits)
        keep_or_all(self.signal_family_var, signals)
        keep_or_all(self.risk_model_var, risks)

    def _apply_filters(self):
        rows = list(self.raw_rows)

        search = safe_text(self.search_var.get()).lower()
        symbol = safe_text(self.symbol_var.get())
        side = safe_text(self.side_var.get())
        timeframe = safe_text(self.timeframe_var.get())
        exit_profile = safe_text(self.exit_profile_var.get())
        signal_family = safe_text(self.signal_family_var.get())
        risk_model = safe_text(self.risk_model_var.get())

        if symbol and symbol != "ALL":
            rows = [r for r in rows if safe_text(r.get("symbol")) == symbol]
        if side and side != "ALL":
            rows = [r for r in rows if safe_text(r.get("side")) == side]
        if timeframe and timeframe != "ALL":
            rows = [r for r in rows if safe_text(r.get("timeframe")) == timeframe]
        if exit_profile and exit_profile != "ALL":
            rows = [r for r in rows if safe_text(r.get("exit_profile")) == exit_profile]
        if signal_family and signal_family != "ALL":
            rows = [r for r in rows if safe_text(r.get("signal_family")) == signal_family]
        if risk_model and risk_model != "ALL":
            rows = [r for r in rows if safe_text(r.get("risk_model_type")) == risk_model]

        if search:
            cols = [
                "symbol",
                "strategy_id",
                "side",
                "timeframe",
                "exit_profile",
                "signal_family",
                "risk_model_type",
                "display_name",
                "extended_display_name",
                "profile_file",
            ]
            rows = [
                r for r in rows
                if any(contains_ci(r.get(col), search) for col in cols)
            ]

        col_symbol = safe_text(self.col_filter_symbol.get()).lower()
        col_variant = safe_text(self.col_filter_variant.get()).lower()
        col_strategy_id = safe_text(self.col_filter_strategy_id.get()).lower()
        col_side = safe_text(self.col_filter_side.get()).lower()
        col_timeframe = safe_text(self.col_filter_timeframe.get()).lower()
        col_exit = safe_text(self.col_filter_exit.get()).lower()

        if col_symbol:
            rows = [r for r in rows if contains_ci(r.get("symbol"), col_symbol)]
        if col_variant:
            rows = [r for r in rows if contains_ci(r.get("variant_number"), col_variant)]
        if col_strategy_id:
            rows = [r for r in rows if contains_ci(r.get("strategy_id"), col_strategy_id)]
        if col_side:
            rows = [r for r in rows if contains_ci(r.get("side"), col_side)]
        if col_timeframe:
            rows = [r for r in rows if contains_ci(r.get("timeframe"), col_timeframe)]
        if col_exit:
            rows = [r for r in rows if contains_ci(r.get("exit_profile"), col_exit)]

        self.filtered_rows = rows
        self.summary_rows = build_symbol_summary(self.filtered_rows)
        self.pivot_rows = build_symbol_side_pivot(self.filtered_rows)

        self._update_kpis()
        self._load_summary_table()
        self._load_main_table()
        self._update_selection_info()
        self._load_pivot_table()
        self._render_pivot_chart()

    # ========================================================
    # KPI / TABLES
    # ============================================================

    def _update_kpis(self):
        raw = self.raw_rows
        flt = self.filtered_rows

        total = len(raw)
        symbols = len({safe_text(r.get("symbol")) for r in raw if safe_text(r.get("symbol"))})
        aggressive = sum(1 for r in raw if safe_text(r.get("uses_aggressive_mm")) == "YES")
        uncertain = sum(1 for r in raw if safe_text(r.get("has_parser_uncertainty")) == "YES")
        stop_loss = sum(1 for r in raw if safe_text(r.get("has_defined_stop_loss")) == "YES")
        filtered = len(flt)

        self.card_total.set_value(fmt_int(total))
        self.card_symbols.set_value(fmt_int(symbols))
        self.card_aggr.set_value(fmt_int(aggressive), color=FG_NEG if aggressive > 0 else FG_MAIN)
        self.card_uncertain.set_value(fmt_int(uncertain), color=FG_WARN if uncertain > 0 else FG_MAIN)
        self.card_stops.set_value(fmt_int(stop_loss), color=FG_POS if stop_loss > 0 else FG_MAIN)
        self.card_selected.set_value(fmt_int(filtered), color=FG_POS if filtered > 0 else FG_NEG)

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
                    row.get("timeframes", ""),
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

        for idx, row in enumerate(self.pivot_rows[:PIVOT_ROW_LIMIT]):
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

    def _update_selection_info(self):
        raw = self.raw_rows
        flt = self.filtered_rows

        duplicates = 0
        seen = set()
        for r in raw:
            uid = safe_text(r.get("strategy_uid"))
            if uid in seen:
                duplicates += 1
            else:
                seen.add(uid)

        lines = [
            f"Root                : {STRATEGY_PROFILE_ROOT}",
            f"Total Profiles      : {len(raw)}",
            f"Filtered Profiles   : {len(flt)}",
            f"Duplicate UID Rows  : {duplicates}",
            f"Selected Symbol     : {safe_text(self.symbol_var.get()) or 'ALL'}",
            f"Selected Side       : {safe_text(self.side_var.get()) or 'ALL'}",
            f"Selected Timeframe  : {safe_text(self.timeframe_var.get()) or 'ALL'}",
            f"Selected Exit       : {safe_text(self.exit_profile_var.get()) or 'ALL'}",
            f"Selected Signal     : {safe_text(self.signal_family_var.get()) or 'ALL'}",
            f"Selected Risk       : {safe_text(self.risk_model_var.get()) or 'ALL'}",
            f"Search              : {safe_text(self.search_var.get()) or '-'}",
            f"Column Symbol       : {safe_text(self.col_filter_symbol.get()) or '-'}",
            f"Column Variant      : {safe_text(self.col_filter_variant.get()) or '-'}",
            f"Column Strategy ID  : {safe_text(self.col_filter_strategy_id.get()) or '-'}",
            f"Column Side         : {safe_text(self.col_filter_side.get()) or '-'}",
            f"Column Timeframe    : {safe_text(self.col_filter_timeframe.get()) or '-'}",
            f"Column Exit         : {safe_text(self.col_filter_exit.get()) or '-'}",
            "",
        ]

        if not flt:
            lines.append("Keine Strategien im aktuellen Filter.")
        else:
            symbols = {safe_text(r.get('symbol')) for r in flt if safe_text(r.get('symbol'))}
            sides = {safe_text(r.get('side')) for r in flt if safe_text(r.get('side'))}
            exits = {safe_text(r.get('exit_profile')) for r in flt if safe_text(r.get('exit_profile'))}
            signals = {safe_text(r.get('signal_family')) for r in flt if safe_text(r.get('signal_family'))}
            lines.extend([
                f"Symbols in Filter   : {len(symbols)}",
                f"Sides in Filter     : {len(sides)}",
                f"Exit Profiles       : {len(exits)}",
                f"Signal Families     : {len(signals)}",
                "",
                "Top Symbols:",
            ])
            counter = Counter(safe_text(r.get("symbol")) for r in flt if safe_text(r.get("symbol")))
            for sym, cnt in counter.most_common(10):
                lines.append(f"  {sym:<12} {cnt}")

        self.selection_text.configure(state="normal")
        self.selection_text.delete("1.0", "end")
        self.selection_text.insert("1.0", "\n".join(lines))
        self.selection_text.configure(state="disabled")

    def _load_main_table(self):
        self.table.delete(*self.table.get_children())
        self._table_row_map.clear()

        for idx, row in enumerate(self.filtered_rows[:TABLE_ROW_LIMIT]):
            iid = make_tree_iid("table", idx, safe_text(row.get("row_key")))
            self._table_row_map[iid] = row
            self.table.insert(
                "",
                "end",
                iid=iid,
                values=(
                    row.get("symbol", ""),
                    row.get("variant_number", ""),
                    row.get("strategy_id", ""),
                    row.get("side", ""),
                    row.get("timeframe", ""),
                    row.get("exit_profile", ""),
                    row.get("signal_family", ""),
                    row.get("risk_model_type", ""),
                    row.get("risk_percent", ""),
                    row.get("fixed_sl", ""),
                    row.get("fixed_tp", ""),
                    row.get("time_label", ""),
                    row.get("display_name", ""),
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
        self._update_selection_info()

    def _clear_column_filters(self):
        self.col_filter_symbol.set("")
        self.col_filter_variant.set("")
        self.col_filter_strategy_id.set("")
        self.col_filter_side.set("")
        self.col_filter_timeframe.set("")
        self.col_filter_exit.set("")
        self._apply_filters()

    # ========================================================
    # CHART
    # ============================================================

    def _render_pivot_chart(self):
        c = self.chart_canvas
        if c is None:
            return

        c.delete("all")
        width = max(c.winfo_width(), 300)
        height = max(c.winfo_height(), 220)

        if not self.pivot_rows:
            c.create_text(width / 2, height / 2, text="No data", fill=FG_MUTED, font=FONT_SECTION)
            return

        rows = self.pivot_rows[:CHART_MAX_SYMBOLS]
        max_total = max(int(r.get("TOTAL", 0)) for r in rows) or 1

        left = 60
        right = width - 20
        top = 20
        bottom = height - 45
        plot_w = max(100, right - left)
        plot_h = max(80, bottom - top)

        c.create_line(left, top, left, bottom, fill=BORDER, width=1)
        c.create_line(left, bottom, right, bottom, fill=BORDER, width=1)

        for i in range(5):
            y = top + (plot_h / 4) * i
            c.create_line(left, y, right, y, fill=DIVIDER, width=1)
            val = round(max_total * (1 - i / 4))
            c.create_text(left - 8, y, text=str(val), fill=FG_MUTED, font=FONT_LABEL, anchor="e")

        group_w = plot_w / max(len(rows), 1)
        bar_w = max(8, min(22, group_w / 5))

        for idx, row in enumerate(rows):
            center_x = left + group_w * idx + group_w / 2
            buy = int(row.get("BUY", 0))
            sell = int(row.get("SELL", 0))
            both = int(row.get("BOTH", 0))
            symbol = safe_text(row.get("symbol"))

            series = [
                (-bar_w, buy, CHART_COLORS["BUY"]),
                (0, sell, CHART_COLORS["SELL"]),
                (bar_w, both, CHART_COLORS["BOTH"]),
            ]

            for dx, val, color in series:
                x1 = center_x + dx - bar_w / 2
                x2 = center_x + dx + bar_w / 2
                y2 = bottom - 1
                y1 = bottom - (val / max_total) * (plot_h - 6)
                c.create_rectangle(x1, y1, x2, y2, fill=color, outline=color)

            c.create_text(center_x, bottom + 14, text=symbol, fill=FG_MUTED, font=FONT_LABEL, anchor="n")

        legend_y = 8
        legend_x = right - 180
        for i, name in enumerate(["BUY", "SELL", "BOTH"]):
            x = legend_x + i * 58
            c.create_rectangle(x, legend_y, x + 10, legend_y + 10, fill=CHART_COLORS[name], outline=CHART_COLORS[name])
            c.create_text(x + 16, legend_y + 5, text=name, fill=FG_MAIN, font=FONT_LABEL, anchor="w")

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

    # ========================================================
    # DETAIL WINDOW
    # ============================================================

    def _open_detail_window(self, row: Dict[str, Any]):
        win = tk.Toplevel(self)
        win.title(f"Strategy Detail | {safe_text(row.get('strategy_uid'))}")
        win.geometry("1180x780")
        win.minsize(980, 680)
        win.configure(bg=BG_MAIN)

        header = tk.Frame(win, bg=BG_TOP, highlightbackground=BORDER, highlightthickness=1, height=52)
        header.pack(fill="x", padx=10, pady=10)
        header.pack_propagate(False)

        tk.Label(
            header,
            text="STRATEGY PROFILE DETAIL",
            font=FONT_TITLE,
            bg=BG_TOP,
            fg=FG_WHITE,
        ).pack(side="left", padx=12)

        tk.Label(
            header,
            text=safe_text(row.get("profile_file")),
            font=("Segoe UI", 9),
            bg=BG_TOP,
            fg=FG_MUTED,
        ).pack(side="right", padx=12)

        kpi_row = tk.Frame(win, bg=BG_MAIN)
        kpi_row.pack(fill="x", padx=10, pady=(0, 10))

        for title, key in [
            ("Symbol", "symbol"),
            ("Variant", "variant_number"),
            ("Strategy ID", "strategy_id"),
            ("Side", "side"),
            ("Timeframe", "timeframe"),
        ]:
            card = KpiCard(kpi_row, title)
            card.pack(side="left", fill="x", expand=True, padx=4)
            card.set_value(safe_text(row.get(key)))

        body = tk.Frame(win, bg=BG_SURFACE, highlightbackground=BORDER, highlightthickness=1)
        body.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        tk.Label(
            body,
            text="Profile Metadata",
            font=FONT_SECTION,
            bg=BG_SURFACE,
            fg=FG_WHITE,
        ).pack(anchor="w", padx=10, pady=(10, 8))

        text = tk.Text(
            body,
            wrap="none",
            bg=BG_SURFACE,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            font=FONT_MONO,
        )
        text.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        fields = [
            "strategy_uid", "row_key", "symbol", "variant_number", "strategy_id", "side", "timeframe",
            "base_name", "exit_label", "signal_label", "time_label", "display_name", "extended_display_name",
            "exit_profile", "signal_family", "risk_model_type", "sl_type", "tp_type", "trailing_type",
            "fixed_sl", "fixed_tp", "sl_coef", "tp_coef", "trailing_coef", "risk_percent", "fixed_lot",
            "initial_capital", "mm_enabled", "limit_time_range", "time_from", "time_to", "eod_exit",
            "friday_exit", "weekend_protection", "max_trades_per_day", "has_defined_stop_loss",
            "has_defined_take_profit_or_exit", "uses_aggressive_mm", "has_parser_uncertainty", "has_tight_stop",
            "profile_file", "profile_relative_path", "profile_path", "ea_file", "ea_path",
        ]

        lines = [f"{key:<32}: {safe_text(row.get(key))}" for key in fields]
        text.insert("1.0", "\n".join(lines))
        text.configure(state="disabled")

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

class StrategyDashboard(tk.Tk):
    def __init__(self, repo: StrategyProfileRepository):
        super().__init__()

        self.title(APP_TITLE)
        self.geometry("1860x1080")
        self.minsize(1460, 900)
        self.configure(bg=BG_MAIN)

        self.panel = StrategyDashboardPanel(self, repo=repo)
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
    if not STRATEGY_PROFILE_ROOT.exists():
        raise RuntimeError(f"Strategy_Profile Root nicht gefunden: {STRATEGY_PROFILE_ROOT}")

    repo = StrategyProfileRepository(STRATEGY_PROFILE_ROOT)
    app = StrategyDashboard(repo)
    app.mainloop()


if __name__ == "__main__":
    main()