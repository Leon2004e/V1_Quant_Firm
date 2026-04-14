# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Dashboards/Main_Board/pages/Selection/selection_dashboard.py

Selection Dashboard
- visualisiert Strategy_Selection_Layer
- zeigt Ranking, Status, Scores, Reason Codes und Strategie-Details
- professionelles Tkinter + Matplotlib Dashboard
"""

from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional

import tkinter as tk
from tkinter import ttk

try:
    import pandas as pd
except Exception:
    pd = None

try:
    import matplotlib
    matplotlib.use("TkAgg")
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    from matplotlib.figure import Figure
except Exception:
    Figure = None
    FigureCanvasTkAgg = None


# ============================================================
# ROOT / PATHS
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

SELECTION_ROOT = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Analysis"
    / "Strategy_Selection_Layer"
)

SUMMARY_CSV_PATH = SELECTION_ROOT / "strategy_selection_summary.csv"
SUMMARY_JSON_PATH = SELECTION_ROOT / "strategy_selection_summary.json"
STRATEGY_JSON_DIR = SELECTION_ROOT / "strategy_json"


# ============================================================
# THEME
# ============================================================

BG_APP = "#0A1118"
BG_TOP = "#0E1721"
BG_SURFACE = "#101B27"
BG_CARD = "#142131"
BG_CARD_ALT = "#0F1C2A"

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

FONT_TITLE = ("Segoe UI", 20, "bold")
FONT_SUBTITLE = ("Segoe UI", 10)
FONT_SECTION = ("Segoe UI", 11, "bold")
FONT_TEXT = ("Segoe UI", 10)
FONT_LABEL = ("Segoe UI", 9)
FONT_SMALL = ("Segoe UI", 8)
FONT_CARD_TITLE = ("Segoe UI", 9)
FONT_CARD_VALUE = ("Segoe UI", 17, "bold")
FONT_MONO = ("Consolas", 9)


# ============================================================
# HELPERS
# ============================================================

def safe_text(x) -> str:
    if x is None:
        return ""
    try:
        if pd is not None and pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if pd is not None and pd.isna(x):
            return None
        v = float(x)
        if math.isinf(v):
            return None
        return float(v)
    except Exception:
        return None


def safe_int(x) -> Optional[int]:
    try:
        if x is None:
            return None
        if pd is not None and pd.isna(x):
            return None
        return int(x)
    except Exception:
        return None


def format_num(x, digits: int = 2) -> str:
    v = safe_float(x)
    if v is None:
        return "-"
    return f"{v:,.{digits}f}"


def read_csv(path: Path):
    if pd is None or not path.exists():
        return pd.DataFrame() if pd is not None else None
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def load_json(path: Path):
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def sanitize_name(name: str) -> str:
    bad = '<>:"/\\|?*'
    return "".join("_" if c in bad else c for c in str(name)).strip()


def make_divider(parent, pady=(0, 0)):
    tk.Frame(parent, bg=DIVIDER, height=1).pack(fill="x", pady=pady)


def status_color(status: str) -> str:
    s = safe_text(status).upper()
    if s == "APPROVED":
        return FG_POS
    if s == "WATCHLIST":
        return FG_WARN
    if s in {"REJECTED", "SHADOW_ONLY"}:
        return FG_NEG
    return FG_MUTED


# ============================================================
# UI ELEMENTS
# ============================================================

class SoftPanel(tk.Frame):
    def __init__(self, parent, bg=BG_SURFACE, padx=12, pady=12):
        super().__init__(parent, bg=bg, bd=0, highlightthickness=1, highlightbackground=BORDER)
        self.inner = tk.Frame(self, bg=bg)
        self.inner.pack(fill="both", expand=True, padx=padx, pady=pady)


class KpiCard(tk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent, bg=BG_CARD, highlightthickness=1, highlightbackground=BORDER)
        self.title_lbl = tk.Label(self, text=title, font=FONT_CARD_TITLE, bg=BG_CARD, fg=FG_SUBTLE)
        self.title_lbl.pack(anchor="w", padx=12, pady=(10, 4))

        self.value_lbl = tk.Label(self, text="-", font=FONT_CARD_VALUE, bg=BG_CARD, fg=FG_WHITE)
        self.value_lbl.pack(anchor="w", padx=12)

        self.sub_lbl = tk.Label(self, text="", font=FONT_SMALL, bg=BG_CARD, fg=FG_MUTED)
        self.sub_lbl.pack(anchor="w", padx=12, pady=(4, 10))

    def set(self, value: str, sub: str = "", fg: Optional[str] = None):
        self.value_lbl.configure(text=value)
        self.sub_lbl.configure(text=sub)
        if fg is not None:
            self.value_lbl.configure(fg=fg)
        else:
            self.value_lbl.configure(fg=FG_WHITE)


class TextViewer(tk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent, bg=BG_SURFACE, highlightthickness=1, highlightbackground=BORDER)

        head = tk.Frame(self, bg=BG_SURFACE)
        head.pack(fill="x", padx=10, pady=(8, 6))
        tk.Label(head, text=title, font=FONT_SECTION, bg=BG_SURFACE, fg=FG_WHITE).pack(side="left")

        self.text = tk.Text(
            self,
            wrap="word",
            bg=BG_SURFACE,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            bd=0,
            highlightthickness=0,
            font=FONT_MONO,
        )
        self.text.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    def set_text(self, text: str):
        self.text.configure(state="normal")
        self.text.delete("1.0", "end")
        self.text.insert("1.0", text)
        self.text.configure(state="disabled")


class SimpleTable(tk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent, bg=BG_SURFACE, highlightthickness=1, highlightbackground=BORDER)

        head = tk.Frame(self, bg=BG_SURFACE)
        head.pack(fill="x", padx=10, pady=(8, 6))
        tk.Label(head, text=title, font=FONT_SECTION, bg=BG_SURFACE, fg=FG_WHITE).pack(side="left")

        style = ttk.Style()
        try:
            style.theme_use("default")
        except Exception:
            pass

        style.configure(
            "Selection.Treeview",
            background=BG_SURFACE,
            foreground=FG_MAIN,
            fieldbackground=BG_SURFACE,
            bordercolor=BORDER,
            rowheight=24,
            font=FONT_LABEL,
        )
        style.configure(
            "Selection.Treeview.Heading",
            background=BG_TOP,
            foreground=FG_WHITE,
            relief="flat",
            font=FONT_LABEL,
        )

        body = tk.Frame(self, bg=BG_SURFACE)
        body.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.tree = ttk.Treeview(body, style="Selection.Treeview", show="headings")
        self.tree.pack(side="left", fill="both", expand=True)

        ysb = ttk.Scrollbar(body, orient="vertical", command=self.tree.yview)
        ysb.pack(side="right", fill="y")
        self.tree.configure(yscrollcommand=ysb.set)

    def set_dataframe(self, df, max_rows: int = 100):
        self.tree.delete(*self.tree.get_children())

        if pd is None or df is None or df.empty:
            self.tree["columns"] = ["info"]
            self.tree.heading("info", text="info")
            self.tree.column("info", width=300, anchor="w")
            self.tree.insert("", "end", values=("No data",))
            return

        df_show = df.head(max_rows).copy()
        cols = [str(c) for c in df_show.columns.tolist()]
        self.tree["columns"] = cols

        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=145, anchor="w")

        for _, row in df_show.iterrows():
            self.tree.insert("", "end", values=[safe_text(v) for v in row.tolist()])


class ChartPanel(tk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent, bg=BG_SURFACE, highlightthickness=1, highlightbackground=BORDER)
        self.title = title
        self.figure = None
        self.canvas_widget = None

        head = tk.Frame(self, bg=BG_SURFACE)
        head.pack(fill="x", padx=10, pady=(8, 6))
        tk.Label(head, text=title, font=FONT_SECTION, bg=BG_SURFACE, fg=FG_WHITE).pack(side="left")

        self.chart_host = tk.Frame(self, bg=BG_SURFACE)
        self.chart_host.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        self.empty = tk.Label(self.chart_host, text="No chart data", bg=BG_SURFACE, fg=FG_MUTED, font=FONT_LABEL)
        self.empty.pack(expand=True)

    def clear(self, message: str = "No chart data"):
        for child in self.chart_host.winfo_children():
            child.destroy()
        self.empty = tk.Label(self.chart_host, text=message, bg=BG_SURFACE, fg=FG_MUTED, font=FONT_LABEL)
        self.empty.pack(expand=True)

    def _build_base(self, title: str):
        if Figure is None or FigureCanvasTkAgg is None:
            self.clear("matplotlib not available")
            return None, None, None

        for child in self.chart_host.winfo_children():
            child.destroy()

        fig = Figure(figsize=(6, 3), dpi=100)
        ax = fig.add_subplot(111)
        fig.patch.set_facecolor(BG_SURFACE)
        ax.set_facecolor(BG_CARD_ALT)
        ax.set_title(title, color=FG_WHITE, fontsize=10)
        ax.tick_params(colors=FG_MUTED, labelsize=8)
        ax.grid(True, alpha=0.25)
        for spine in ax.spines.values():
            spine.set_color(BORDER)

        return fig, ax, self.chart_host

    def draw_bar(self, labels, values, title=""):
        if not labels or not values:
            self.clear()
            return
        fig, ax, host = self._build_base(title or self.title)
        if fig is None:
            return

        x = list(range(len(labels)))
        ax.bar(x, values)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=35, ha="right", color=FG_MUTED, fontsize=8)
        fig.tight_layout()

        canvas = FigureCanvasTkAgg(fig, master=host)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

        self.figure = fig
        self.canvas_widget = canvas

    def draw_scatter(self, x, y, title="", xlabel="", ylabel="", labels=None):
        if x is None or y is None or len(x) == 0 or len(y) == 0:
            self.clear()
            return
        fig, ax, host = self._build_base(title or self.title)
        if fig is None:
            return

        ax.scatter(x, y)
        ax.set_xlabel(xlabel, color=FG_MUTED, fontsize=8)
        ax.set_ylabel(ylabel, color=FG_MUTED, fontsize=8)

        if labels and len(labels) == len(x):
            for xi, yi, lab in zip(x, y, labels):
                ax.annotate(str(lab)[:12], (xi, yi), fontsize=6, color=FG_MUTED)

        fig.tight_layout()

        canvas = FigureCanvasTkAgg(fig, master=host)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

        self.figure = fig
        self.canvas_widget = canvas


# ============================================================
# MAIN PANEL
# ============================================================

class SelectionDashboardPanel(tk.Frame):
    def __init__(self, parent, app=None, ftmo_root: Optional[Path] = None):
        super().__init__(parent, bg=BG_APP)
        self.app = app
        self.ftmo_root = ftmo_root or FTMO_ROOT

        self.summary_df = pd.DataFrame() if pd is not None else None
        self.filtered_df = pd.DataFrame() if pd is not None else None
        self.current_strategy_id: Optional[str] = None

        self.search_var = tk.StringVar(value="")
        self.status_filter_var = tk.StringVar(value="ALL")
        self.status_var = tk.StringVar(value="Ready")

        self.kpis: Dict[str, KpiCard] = {}

        self._build_ui()
        self.reload_all()

    # --------------------------------------------------------
    # UI
    # --------------------------------------------------------

    def _build_ui(self):
        self.columnconfigure(1, weight=1)
        self.rowconfigure(1, weight=1)

        self._build_topbar()
        self._build_body()
        self._build_statusbar()

    def _build_topbar(self):
        top = tk.Frame(self, bg=BG_TOP, height=60, highlightthickness=1, highlightbackground=BORDER)
        top.grid(row=0, column=0, columnspan=2, sticky="ew", padx=10, pady=(10, 8))
        top.grid_columnconfigure(1, weight=1)
        top.pack_propagate(False)

        tk.Label(top, text="Selection Dashboard", font=FONT_TITLE, bg=BG_TOP, fg=FG_WHITE).grid(
            row=0, column=0, sticky="w", padx=14
        )
        tk.Label(top, text="Strategy_Selection_Layer visual explorer", font=FONT_SUBTITLE, bg=BG_TOP, fg=FG_SUBTLE).grid(
            row=0, column=1, sticky="w", padx=(10, 0)
        )

        status_combo = ttk.Combobox(
            top,
            textvariable=self.status_filter_var,
            values=["ALL", "APPROVED", "WATCHLIST", "SHADOW_ONLY", "REJECTED"],
            state="readonly",
            width=14,
        )
        status_combo.grid(row=0, column=2, sticky="e", padx=(10, 8), pady=10)
        status_combo.bind("<<ComboboxSelected>>", lambda _e: self._apply_filter())

        search = tk.Entry(
            top,
            textvariable=self.search_var,
            bg=BG_SURFACE,
            fg=FG_WHITE,
            insertbackground=FG_WHITE,
            relief="flat",
            font=FONT_TEXT,
            width=32,
        )
        search.grid(row=0, column=3, sticky="e", padx=(0, 8), pady=10)
        search.bind("<KeyRelease>", lambda _e: self._apply_filter())

        reload_lbl = tk.Label(
            top,
            text="Reload",
            bg=FG_ACCENT,
            fg=FG_WHITE,
            font=FONT_LABEL,
            padx=12,
            pady=6,
            cursor="hand2",
        )
        reload_lbl.grid(row=0, column=4, sticky="e", padx=(0, 14))
        reload_lbl.bind("<Button-1>", lambda _e: self.reload_all())

    def _build_body(self):
        left = SoftPanel(self, bg=BG_SURFACE, padx=10, pady=10)
        left.grid(row=1, column=0, sticky="nsw", padx=(10, 6), pady=(0, 8))
        left.configure(width=410)
        left.grid_propagate(False)

        tk.Label(left.inner, text="Strategies", font=FONT_SECTION, bg=BG_SURFACE, fg=FG_WHITE).pack(anchor="w")
        make_divider(left.inner, pady=(8, 8))

        self.strategy_listbox = tk.Listbox(
            left.inner,
            bg=BG_SURFACE,
            fg=FG_MAIN,
            selectbackground=FG_ACCENT,
            selectforeground=FG_WHITE,
            relief="flat",
            bd=0,
            highlightthickness=0,
            font=("Consolas", 10),
            width=42,
        )
        self.strategy_listbox.pack(fill="both", expand=True)
        self.strategy_listbox.bind("<<ListboxSelect>>", self._on_select_strategy)

        right = tk.Frame(self, bg=BG_APP)
        right.grid(row=1, column=1, sticky="nsew", padx=(6, 10), pady=(0, 8))
        right.columnconfigure(0, weight=1)
        right.rowconfigure(1, weight=1)

        kpi_row = tk.Frame(right, bg=BG_APP)
        kpi_row.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        for i in range(6):
            kpi_row.columnconfigure(i, weight=1)

        defs = [
            ("count", "Strategies"),
            ("approved", "Approved"),
            ("watchlist", "Watchlist"),
            ("shadow", "Shadow"),
            ("rejected", "Rejected"),
            ("avg_score", "Avg Final Score"),
        ]
        for i, (key, title) in enumerate(defs):
            card = KpiCard(kpi_row, title)
            card.grid(row=0, column=i, sticky="ew", padx=(0 if i == 0 else 6, 0))
            self.kpis[key] = card

        style = ttk.Style()
        try:
            style.theme_use("default")
        except Exception:
            pass

        style.configure("SelectionNotebook.TNotebook", background=BG_APP, borderwidth=0)
        style.configure(
            "SelectionNotebook.TNotebook.Tab",
            background=BG_TOP,
            foreground=FG_MAIN,
            padding=(14, 8),
            font=FONT_LABEL,
        )
        style.map(
            "SelectionNotebook.TNotebook.Tab",
            background=[("selected", BG_SURFACE)],
            foreground=[("selected", FG_WHITE)],
        )

        self.notebook = ttk.Notebook(right, style="SelectionNotebook.TNotebook")
        self.notebook.grid(row=1, column=0, sticky="nsew")

        self.tab_overview = tk.Frame(self.notebook, bg=BG_APP)
        self.tab_charts = tk.Frame(self.notebook, bg=BG_APP)
        self.tab_tables = tk.Frame(self.notebook, bg=BG_APP)
        self.tab_json = tk.Frame(self.notebook, bg=BG_APP)

        self.notebook.add(self.tab_overview, text="Overview")
        self.notebook.add(self.tab_charts, text="Charts")
        self.notebook.add(self.tab_tables, text="Tables")
        self.notebook.add(self.tab_json, text="JSON")

        self._build_overview_tab()
        self._build_charts_tab()
        self._build_tables_tab()
        self._build_json_tab()

    def _build_overview_tab(self):
        self.tab_overview.columnconfigure(0, weight=1)
        self.tab_overview.columnconfigure(1, weight=1)
        self.tab_overview.rowconfigure(0, weight=1)
        self.tab_overview.rowconfigure(1, weight=1)

        self.detail_table = SimpleTable(self.tab_overview, "Strategy Selection Summary")
        self.detail_table.grid(row=0, column=0, columnspan=2, sticky="nsew", pady=(0, 6))

        self.chart_status = ChartPanel(self.tab_overview, "Status Distribution")
        self.chart_status.grid(row=1, column=0, sticky="nsew", padx=(0, 4), pady=(6, 0))

        self.chart_reasons = ChartPanel(self.tab_overview, "Reason Code Frequency")
        self.chart_reasons.grid(row=1, column=1, sticky="nsew", padx=(4, 0), pady=(6, 0))

    def _build_charts_tab(self):
        self.tab_charts.columnconfigure(0, weight=1)
        self.tab_charts.columnconfigure(1, weight=1)
        self.tab_charts.rowconfigure(0, weight=1)
        self.tab_charts.rowconfigure(1, weight=1)

        self.chart_ranking = ChartPanel(self.tab_charts, "Top Final Scores")
        self.chart_ranking.grid(row=0, column=0, sticky="nsew", padx=(0, 4), pady=(0, 4))

        self.chart_score_rob = ChartPanel(self.tab_charts, "Final Score vs Robustness")
        self.chart_score_rob.grid(row=0, column=1, sticky="nsew", padx=(4, 0), pady=(0, 4))

        self.chart_port_regime = ChartPanel(self.tab_charts, "Portfolio Fit vs Regime Dependency")
        self.chart_port_regime.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(4, 0))

    def _build_tables_tab(self):
        self.tab_tables.columnconfigure(0, weight=1)
        self.tab_tables.rowconfigure(0, weight=1)
        self.tab_tables.rowconfigure(1, weight=1)

        self.rank_table = SimpleTable(self.tab_tables, "Selection Ranking Table")
        self.rank_table.grid(row=0, column=0, sticky="nsew", pady=(0, 6))

        self.reason_table = SimpleTable(self.tab_tables, "Reason Code Table")
        self.reason_table.grid(row=1, column=0, sticky="nsew", pady=(6, 0))

    def _build_json_tab(self):
        self.tab_json.columnconfigure(0, weight=1)
        self.tab_json.rowconfigure(0, weight=1)

        self.json_viewer = TextViewer(self.tab_json, "Strategy Selection JSON")
        self.json_viewer.grid(row=0, column=0, sticky="nsew")

    def _build_statusbar(self):
        status = tk.Frame(self, bg=BG_APP, height=24)
        status.grid(row=2, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 6))
        status.pack_propagate(False)

        tk.Label(status, textvariable=self.status_var, font=FONT_SMALL, bg=BG_APP, fg=FG_SUBTLE).pack(side="left")

    # --------------------------------------------------------
    # DATA
    # --------------------------------------------------------

    def reload_all(self):
        if pd is None:
            self.status_var.set("pandas not available")
            return

        self.summary_df = read_csv(SUMMARY_CSV_PATH)
        self._apply_filter()
        self._update_global_views()

        if self.filtered_df is not None and not self.filtered_df.empty:
            first = self.filtered_df.iloc[0]["strategy_id"]
            self._select_strategy_in_listbox(first)
            self._load_strategy(first)
        else:
            self._clear_strategy_views()

        count = 0 if self.summary_df is None or self.summary_df.empty else len(self.summary_df)
        self.status_var.set(f"Loaded selection summary | strategies={count}")

    def _apply_filter(self):
        self.strategy_listbox.delete(0, "end")

        if self.summary_df is None or self.summary_df.empty:
            self.filtered_df = pd.DataFrame() if pd is not None else None
            return

        df = self.summary_df.copy()

        q = self.search_var.get().strip().lower()
        status_filter = self.status_filter_var.get().strip().upper()

        if q:
            df = df[df["strategy_id"].astype(str).str.lower().str.contains(q, na=False)]

        if status_filter and status_filter != "ALL":
            df = df[df["selection_status"].astype(str).str.upper() == status_filter]

        self.filtered_df = df.reset_index(drop=True)

        for sid in self.filtered_df["strategy_id"].astype(str).tolist():
            self.strategy_listbox.insert("end", sid)

        self._update_global_views()

    def _update_global_views(self):
        if self.filtered_df is None or self.filtered_df.empty:
            self.kpis["count"].set("0")
            self.kpis["approved"].set("0")
            self.kpis["watchlist"].set("0")
            self.kpis["shadow"].set("0")
            self.kpis["rejected"].set("0")
            self.kpis["avg_score"].set("-")

            self.rank_table.set_dataframe(pd.DataFrame() if pd is not None else None)
            self.reason_table.set_dataframe(pd.DataFrame() if pd is not None else None)

            self.chart_status.clear()
            self.chart_reasons.clear()
            self.chart_ranking.clear()
            self.chart_score_rob.clear()
            self.chart_port_regime.clear()
            return

        df = self.filtered_df.copy()

        count = len(df)
        approved = int((df["selection_status"].astype(str).str.upper() == "APPROVED").sum())
        watchlist = int((df["selection_status"].astype(str).str.upper() == "WATCHLIST").sum())
        shadow = int((df["selection_status"].astype(str).str.upper() == "SHADOW_ONLY").sum())
        rejected = int((df["selection_status"].astype(str).str.upper() == "REJECTED").sum())

        avg_score = pd.to_numeric(df["final_score"], errors="coerce").dropna().mean()

        self.kpis["count"].set(str(count))
        self.kpis["approved"].set(str(approved), fg=FG_POS)
        self.kpis["watchlist"].set(str(watchlist), fg=FG_WARN)
        self.kpis["shadow"].set(str(shadow), fg=FG_NEG)
        self.kpis["rejected"].set(str(rejected), fg=FG_NEG)
        self.kpis["avg_score"].set(format_num(avg_score, 3))

        rank_cols = [
            c for c in [
                "strategy_id",
                "selection_status",
                "final_score",
                "robustness_score",
                "portfolio_fit_score",
                "regime_dependency_score",
                "recommended_risk_budget",
                "recommended_lot_size",
                "max_safe_lot_size",
                "selection_reason_codes",
            ] if c in df.columns
        ]
        rank_df = df[rank_cols].sort_values("final_score", ascending=False)
        self.rank_table.set_dataframe(rank_df, max_rows=200)

        reason_df = self._build_reason_count_df(df)
        self.reason_table.set_dataframe(reason_df, max_rows=100)

        self._update_global_charts(df, reason_df)

    def _build_reason_count_df(self, df):
        codes = []
        if "selection_reason_codes" in df.columns:
            for v in df["selection_reason_codes"].astype(str).tolist():
                for part in v.split("|"):
                    p = part.strip()
                    if p:
                        codes.append(p)

        counter = Counter(codes)
        rows = [{"reason_code": k, "count": v} for k, v in counter.most_common()]
        return pd.DataFrame(rows) if pd is not None else None

    def _update_global_charts(self, df, reason_df):
        # status distribution
        status_counts = (
            df["selection_status"]
            .astype(str)
            .value_counts()
            .to_dict()
        )
        self.chart_status.draw_bar(
            labels=list(status_counts.keys()),
            values=list(status_counts.values()),
            title="Selection Status Distribution",
        )

        # reason codes
        if reason_df is not None and not reason_df.empty:
            top_reason_df = reason_df.head(10)
            self.chart_reasons.draw_bar(
                labels=top_reason_df["reason_code"].astype(str).tolist(),
                values=top_reason_df["count"].astype(float).tolist(),
                title="Top Reason Codes",
            )
        else:
            self.chart_reasons.clear()

        # top ranking
        top_df = df.sort_values("final_score", ascending=False).head(15)
        self.chart_ranking.draw_bar(
            labels=top_df["strategy_id"].astype(str).tolist(),
            values=pd.to_numeric(top_df["final_score"], errors="coerce").fillna(0).tolist(),
            title="Top Final Scores",
        )

        # score vs robustness
        x1 = pd.to_numeric(df.get("final_score"), errors="coerce")
        y1 = pd.to_numeric(df.get("robustness_score"), errors="coerce")
        d1 = pd.DataFrame({
            "x": x1,
            "y": y1,
            "label": df["strategy_id"].astype(str),
        }).dropna()
        self.chart_score_rob.draw_scatter(
            x=d1["x"].tolist(),
            y=d1["y"].tolist(),
            labels=d1["label"].tolist(),
            title="Final Score vs Robustness",
            xlabel="Final Score",
            ylabel="Robustness",
        )

        # portfolio fit vs regime dependency
        x2 = pd.to_numeric(df.get("portfolio_fit_score"), errors="coerce")
        y2 = pd.to_numeric(df.get("regime_dependency_score"), errors="coerce")
        d2 = pd.DataFrame({
            "x": x2,
            "y": y2,
            "label": df["strategy_id"].astype(str),
        }).dropna()
        self.chart_port_regime.draw_scatter(
            x=d2["x"].tolist(),
            y=d2["y"].tolist(),
            labels=d2["label"].tolist(),
            title="Portfolio Fit vs Regime Dependency",
            xlabel="Portfolio Fit",
            ylabel="Regime Dependency",
        )

    # --------------------------------------------------------
    # STRATEGY DETAIL
    # --------------------------------------------------------

    def _select_strategy_in_listbox(self, strategy_id: str):
        for i in range(self.strategy_listbox.size()):
            if self.strategy_listbox.get(i) == strategy_id:
                self.strategy_listbox.selection_clear(0, "end")
                self.strategy_listbox.selection_set(i)
                self.strategy_listbox.activate(i)
                self.strategy_listbox.see(i)
                break

    def _on_select_strategy(self, _event=None):
        sel = self.strategy_listbox.curselection()
        if not sel:
            return
        strategy_id = self.strategy_listbox.get(sel[0])
        self._load_strategy(strategy_id)

    def _load_strategy(self, strategy_id: str):
        self.current_strategy_id = strategy_id

        if self.summary_df is None or self.summary_df.empty:
            self._clear_strategy_views()
            return

        row_df = self.summary_df[self.summary_df["strategy_id"].astype(str) == strategy_id].copy()
        if row_df.empty:
            self._clear_strategy_views()
            return

        row = row_df.iloc[0].to_dict()

        detail_df = pd.DataFrame(
            [{"metric": k, "value": safe_text(v)} for k, v in row.items()]
        )
        self.detail_table.set_dataframe(detail_df, max_rows=200)

        json_path = STRATEGY_JSON_DIR / f"{sanitize_name(strategy_id)}__selection.json"
        payload = load_json(json_path)
        pretty = json.dumps(payload, ensure_ascii=False, indent=2) if payload else "{}"
        self.json_viewer.set_text(pretty)

        status = safe_text(row.get("selection_status")).upper()
        score = format_num(row.get("final_score"), 3)
        self.status_var.set(f"Loaded strategy: {strategy_id} | status={status} | final_score={score}")

    def _clear_strategy_views(self):
        self.detail_table.set_dataframe(pd.DataFrame() if pd is not None else None)
        self.json_viewer.set_text("{}")
        self.status_var.set("No strategy selected")


# ============================================================
# STANDALONE
# ============================================================

def main():
    root = tk.Tk()
    root.title("Selection Dashboard")
    root.geometry("1900x1080")
    root.configure(bg=BG_APP)

    panel = SelectionDashboardPanel(root)
    panel.pack(fill="both", expand=True)

    root.mainloop()


if __name__ == "__main__":
    main()