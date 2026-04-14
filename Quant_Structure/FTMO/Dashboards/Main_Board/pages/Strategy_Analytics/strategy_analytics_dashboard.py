# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Dashboards/Main_Board/pages/Strategy_Analytics/strategy_analytics_dashboard.py

Professionelles Strategy Analytics Dashboard
- breite Strategieliste
- KPI Cards
- echte Matplotlib Charts aus CSV / JSON
- Tabs:
    * Overview
    * Visuals
    * Tables
    * JSON
- PNG Visuals aus Strategy_Layer werden zusätzlich angezeigt
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk

try:
    import pandas as pd
except Exception:
    pd = None

try:
    from PIL import Image, ImageTk
except Exception:
    Image = None
    ImageTk = None

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

ANALYSIS_ROOT = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Analysis"
    / "Strategy_Layer"
)

SUMMARY_CSV_PATH = ANALYSIS_ROOT / "strategy_layer_summary.csv"
SUMMARY_JSON_PATH = ANALYSIS_ROOT / "strategy_layer_summary.json"

EQUITY_DIR = ANALYSIS_ROOT / "equity_curves"
STRATEGY_JSON_DIR = ANALYSIS_ROOT / "strategy_json"
STRATEGY_SUMMARY_DIR = ANALYSIS_ROOT / "strategy_summaries"

VISUAL_ROOT = ANALYSIS_ROOT / "visuals"
VISUAL_EQUITY_DIR = VISUAL_ROOT / "equity_curves"
VISUAL_DRAWDOWN_DIR = VISUAL_ROOT / "drawdowns"
VISUAL_PNL_DIR = VISUAL_ROOT / "pnl_distributions"
VISUAL_MFE_MAE_DIR = VISUAL_ROOT / "mfe_mae"
VISUAL_MONTHLY_DIR = VISUAL_ROOT / "monthly_pnl"


# ============================================================
# THEME
# ============================================================

BG_APP = "#0A1118"
BG_TOP = "#0E1721"
BG_SURFACE = "#101B27"
BG_CARD = "#142131"
BG_CARD_ALT = "#0F1C2A"
BG_HOVER = "#1A2A3D"

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
        return v
    except Exception:
        return None


def format_num(x, digits: int = 2) -> str:
    v = safe_float(x)
    if v is None:
        return "-"
    return f"{v:,.{digits}f}"


def format_pct(x, digits: int = 2) -> str:
    v = safe_float(x)
    if v is None:
        return "-"
    return f"{100.0 * v:.{digits}f}%"


def sanitize_name(name: str) -> str:
    bad = '<>:"/\\|?*'
    return "".join("_" if c in bad else c for c in str(name)).strip()


def read_csv(path: Path):
    if pd is None or not path.exists():
        return pd.DataFrame() if pd is not None else None
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def make_divider(parent, pady=(0, 0)):
    tk.Frame(parent, bg=DIVIDER, height=1).pack(fill="x", pady=pady)


# ============================================================
# BASIC UI
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

    def set(self, value: str, sub: str = ""):
        self.value_lbl.configure(text=value)
        self.sub_lbl.configure(text=sub)


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
            "StrategyAnalytics.Treeview",
            background=BG_SURFACE,
            foreground=FG_MAIN,
            fieldbackground=BG_SURFACE,
            bordercolor=BORDER,
            rowheight=24,
            font=FONT_LABEL,
        )
        style.configure(
            "StrategyAnalytics.Treeview.Heading",
            background=BG_TOP,
            foreground=FG_WHITE,
            relief="flat",
            font=FONT_LABEL,
        )

        body = tk.Frame(self, bg=BG_SURFACE)
        body.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.tree = ttk.Treeview(body, style="StrategyAnalytics.Treeview", show="headings")
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
            self.tree.column(c, width=140, anchor="w")

        for _, row in df_show.iterrows():
            self.tree.insert("", "end", values=[safe_text(v) for v in row.tolist()])


class ChartPanel(tk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent, bg=BG_SURFACE, highlightthickness=1, highlightbackground=BORDER)
        self.title = title
        self.canvas_widget = None
        self.figure = None

        head = tk.Frame(self, bg=BG_SURFACE)
        head.pack(fill="x", padx=10, pady=(8, 6))
        tk.Label(head, text=title, font=FONT_SECTION, bg=BG_SURFACE, fg=FG_WHITE).pack(side="left")

        self.chart_host = tk.Frame(self, bg=BG_SURFACE)
        self.chart_host.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        self.empty_label = tk.Label(
            self.chart_host,
            text="No chart data",
            bg=BG_SURFACE,
            fg=FG_MUTED,
            font=FONT_LABEL,
        )
        self.empty_label.pack(expand=True)

    def clear(self, message: str = "No chart data"):
        for child in self.chart_host.winfo_children():
            child.destroy()
        self.empty_label = tk.Label(
            self.chart_host,
            text=message,
            bg=BG_SURFACE,
            fg=FG_MUTED,
            font=FONT_LABEL,
        )
        self.empty_label.pack(expand=True)

    def draw_line(self, x, y, xlabel="", ylabel="", title=""):
        if Figure is None or FigureCanvasTkAgg is None:
            self.clear("matplotlib not available")
            return
        if y is None or len(y) == 0:
            self.clear("No chart data")
            return

        for child in self.chart_host.winfo_children():
            child.destroy()

        fig = Figure(figsize=(6, 3), dpi=100)
        ax = fig.add_subplot(111)
        fig.patch.set_facecolor(BG_SURFACE)
        ax.set_facecolor(BG_CARD_ALT)

        ax.plot(x, y)
        ax.set_title(title or self.title, color=FG_WHITE, fontsize=10)
        ax.set_xlabel(xlabel, color=FG_MUTED, fontsize=8)
        ax.set_ylabel(ylabel, color=FG_MUTED, fontsize=8)
        ax.tick_params(colors=FG_MUTED, labelsize=8)
        ax.grid(True, alpha=0.25)

        for spine in ax.spines.values():
            spine.set_color(BORDER)

        canvas = FigureCanvasTkAgg(fig, master=self.chart_host)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

        self.figure = fig
        self.canvas_widget = canvas

    def draw_hist(self, values, bins=30, xlabel="", ylabel="Frequency", title=""):
        if Figure is None or FigureCanvasTkAgg is None:
            self.clear("matplotlib not available")
            return
        if values is None or len(values) == 0:
            self.clear("No chart data")
            return

        for child in self.chart_host.winfo_children():
            child.destroy()

        fig = Figure(figsize=(6, 3), dpi=100)
        ax = fig.add_subplot(111)
        fig.patch.set_facecolor(BG_SURFACE)
        ax.set_facecolor(BG_CARD_ALT)

        ax.hist(values, bins=bins)
        ax.set_title(title or self.title, color=FG_WHITE, fontsize=10)
        ax.set_xlabel(xlabel, color=FG_MUTED, fontsize=8)
        ax.set_ylabel(ylabel, color=FG_MUTED, fontsize=8)
        ax.tick_params(colors=FG_MUTED, labelsize=8)
        ax.grid(True, alpha=0.25)

        for spine in ax.spines.values():
            spine.set_color(BORDER)

        canvas = FigureCanvasTkAgg(fig, master=self.chart_host)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

        self.figure = fig
        self.canvas_widget = canvas

    def draw_bar(self, labels, values, xlabel="", ylabel="", title=""):
        if Figure is None or FigureCanvasTkAgg is None:
            self.clear("matplotlib not available")
            return
        if values is None or len(values) == 0:
            self.clear("No chart data")
            return

        for child in self.chart_host.winfo_children():
            child.destroy()

        fig = Figure(figsize=(6, 3), dpi=100)
        ax = fig.add_subplot(111)
        fig.patch.set_facecolor(BG_SURFACE)
        ax.set_facecolor(BG_CARD_ALT)

        x = list(range(len(labels)))
        ax.bar(x, values)
        ax.set_title(title or self.title, color=FG_WHITE, fontsize=10)
        ax.set_xlabel(xlabel, color=FG_MUTED, fontsize=8)
        ax.set_ylabel(ylabel, color=FG_MUTED, fontsize=8)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=45, ha="right", color=FG_MUTED, fontsize=7)
        ax.tick_params(axis="y", colors=FG_MUTED, labelsize=8)
        ax.grid(True, alpha=0.25)

        for spine in ax.spines.values():
            spine.set_color(BORDER)

        fig.tight_layout()

        canvas = FigureCanvasTkAgg(fig, master=self.chart_host)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

        self.figure = fig
        self.canvas_widget = canvas


class ImagePanel(tk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent, bg=BG_SURFACE, highlightthickness=1, highlightbackground=BORDER)
        self._img_ref = None

        head = tk.Frame(self, bg=BG_SURFACE)
        head.pack(fill="x", padx=10, pady=(8, 6))
        tk.Label(head, text=title, font=FONT_SECTION, bg=BG_SURFACE, fg=FG_WHITE).pack(side="left")

        self.label = tk.Label(
            self,
            text="No image",
            bg=BG_SURFACE,
            fg=FG_MUTED,
            font=FONT_LABEL,
            justify="center",
        )
        self.label.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    def set_image(self, path: Path, max_size=(560, 300)):
        if not path.exists():
            self._img_ref = None
            self.label.configure(text="No image", image="", compound="none")
            return

        if Image is None or ImageTk is None:
            self._img_ref = None
            self.label.configure(text=f"Image available:\n{path.name}", image="", compound="none")
            return

        try:
            img = Image.open(path)
            img.thumbnail(max_size)
            tk_img = ImageTk.PhotoImage(img)
            self._img_ref = tk_img
            self.label.configure(text="", image=tk_img, compound="center")
        except Exception as e:
            self._img_ref = None
            self.label.configure(text=f"Image load error:\n{e}", image="", compound="none")


# ============================================================
# MAIN PANEL
# ============================================================

class StrategyAnalyticsPanel(tk.Frame):
    def __init__(self, parent, app=None, ftmo_root: Optional[Path] = None):
        super().__init__(parent, bg=BG_APP)
        self.app = app
        self.ftmo_root = ftmo_root or FTMO_ROOT

        self.summary_df = pd.DataFrame() if pd is not None else None
        self.filtered_ids: List[str] = []
        self.current_strategy_id: Optional[str] = None

        self.search_var = tk.StringVar(value="")
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

        tk.Label(top, text="Strategy Analytics", font=FONT_TITLE, bg=BG_TOP, fg=FG_WHITE).grid(
            row=0, column=0, sticky="w", padx=14
        )
        tk.Label(top, text="Professional Strategy_Layer dashboard", font=FONT_SUBTITLE, bg=BG_TOP, fg=FG_SUBTLE).grid(
            row=0, column=1, sticky="w", padx=(10, 0)
        )

        search = tk.Entry(
            top,
            textvariable=self.search_var,
            bg=BG_SURFACE,
            fg=FG_WHITE,
            insertbackground=FG_WHITE,
            relief="flat",
            font=FONT_TEXT,
            width=36,
        )
        search.grid(row=0, column=2, sticky="e", padx=(10, 8), pady=10)
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
        reload_lbl.grid(row=0, column=3, sticky="e", padx=(0, 14))
        reload_lbl.bind("<Button-1>", lambda _e: self.reload_all())

    def _build_body(self):
        # left
        left = SoftPanel(self, bg=BG_SURFACE, padx=10, pady=10)
        left.grid(row=1, column=0, sticky="nsw", padx=(10, 6), pady=(0, 8))
        left.configure(width=380)
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
            width=38,
        )
        self.strategy_listbox.pack(fill="both", expand=True)
        self.strategy_listbox.bind("<<ListboxSelect>>", self._on_select_strategy)

        # right
        right = tk.Frame(self, bg=BG_APP)
        right.grid(row=1, column=1, sticky="nsew", padx=(6, 10), pady=(0, 8))
        right.columnconfigure(0, weight=1)
        right.rowconfigure(1, weight=1)

        # KPI row
        kpi_row = tk.Frame(right, bg=BG_APP)
        kpi_row.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        for i in range(6):
            kpi_row.columnconfigure(i, weight=1)

        defs = [
            ("trade_count", "Trade Count"),
            ("profit_factor", "Profit Factor"),
            ("net_profit", "Net Profit"),
            ("max_drawdown_abs", "Max DD"),
            ("win_rate", "Win Rate"),
            ("avg_trade", "Avg Trade"),
        ]
        for i, (key, title) in enumerate(defs):
            card = KpiCard(kpi_row, title)
            card.grid(row=0, column=i, sticky="ew", padx=(0 if i == 0 else 6, 0))
            self.kpis[key] = card

        # Notebook
        style = ttk.Style()
        try:
            style.theme_use("default")
        except Exception:
            pass

        style.configure("MainAnalytics.TNotebook", background=BG_APP, borderwidth=0)
        style.configure(
            "MainAnalytics.TNotebook.Tab",
            background=BG_TOP,
            foreground=FG_MAIN,
            padding=(14, 8),
            font=FONT_LABEL,
        )
        style.map(
            "MainAnalytics.TNotebook.Tab",
            background=[("selected", BG_SURFACE)],
            foreground=[("selected", FG_WHITE)],
        )

        self.notebook = ttk.Notebook(right, style="MainAnalytics.TNotebook")
        self.notebook.grid(row=1, column=0, sticky="nsew")

        self.tab_overview = tk.Frame(self.notebook, bg=BG_APP)
        self.tab_visuals = tk.Frame(self.notebook, bg=BG_APP)
        self.tab_tables = tk.Frame(self.notebook, bg=BG_APP)
        self.tab_json = tk.Frame(self.notebook, bg=BG_APP)

        self.notebook.add(self.tab_overview, text="Overview")
        self.notebook.add(self.tab_visuals, text="Visuals")
        self.notebook.add(self.tab_tables, text="Tables")
        self.notebook.add(self.tab_json, text="JSON")

        self._build_overview_tab()
        self._build_visuals_tab()
        self._build_tables_tab()
        self._build_json_tab()

    def _build_overview_tab(self):
        self.tab_overview.columnconfigure(0, weight=1)
        self.tab_overview.columnconfigure(1, weight=1)
        self.tab_overview.rowconfigure(0, weight=1)
        self.tab_overview.rowconfigure(1, weight=1)

        self.chart_equity = ChartPanel(self.tab_overview, "Equity Curve")
        self.chart_equity.grid(row=0, column=0, sticky="nsew", padx=(0, 4), pady=(0, 4))

        self.chart_dd = ChartPanel(self.tab_overview, "Drawdown")
        self.chart_dd.grid(row=0, column=1, sticky="nsew", padx=(4, 0), pady=(0, 4))

        self.chart_hist = ChartPanel(self.tab_overview, "Trade PnL Distribution")
        self.chart_hist.grid(row=1, column=0, sticky="nsew", padx=(0, 4), pady=(4, 0))

        self.chart_monthly = ChartPanel(self.tab_overview, "Monthly Net PnL")
        self.chart_monthly.grid(row=1, column=1, sticky="nsew", padx=(4, 0), pady=(4, 0))

    def _build_visuals_tab(self):
        self.tab_visuals.columnconfigure(0, weight=1)
        self.tab_visuals.columnconfigure(1, weight=1)
        self.tab_visuals.rowconfigure(0, weight=1)
        self.tab_visuals.rowconfigure(1, weight=1)
        self.tab_visuals.rowconfigure(2, weight=1)

        self.img_equity = ImagePanel(self.tab_visuals, "Saved Equity PNG")
        self.img_equity.grid(row=0, column=0, sticky="nsew", padx=(0, 4), pady=(0, 4))

        self.img_dd = ImagePanel(self.tab_visuals, "Saved Drawdown PNG")
        self.img_dd.grid(row=0, column=1, sticky="nsew", padx=(4, 0), pady=(0, 4))

        self.img_pnl = ImagePanel(self.tab_visuals, "Saved PnL Distribution PNG")
        self.img_pnl.grid(row=1, column=0, sticky="nsew", padx=(0, 4), pady=4)

        self.img_mfe = ImagePanel(self.tab_visuals, "Saved MFE / MAE PNG")
        self.img_mfe.grid(row=1, column=1, sticky="nsew", padx=(4, 0), pady=4)

        self.img_monthly = ImagePanel(self.tab_visuals, "Saved Monthly PnL PNG")
        self.img_monthly.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=(4, 0))

    def _build_tables_tab(self):
        self.tab_tables.columnconfigure(0, weight=1)
        self.tab_tables.columnconfigure(1, weight=1)
        self.tab_tables.rowconfigure(0, weight=1)
        self.tab_tables.rowconfigure(1, weight=1)

        self.summary_table = SimpleTable(self.tab_tables, "Strategy Summary")
        self.summary_table.grid(row=0, column=0, columnspan=2, sticky="nsew", pady=(0, 6))

        self.calendar_table = SimpleTable(self.tab_tables, "Calendar Segments")
        self.calendar_table.grid(row=1, column=0, sticky="nsew", padx=(0, 4), pady=(6, 0))

        self.equity_table = SimpleTable(self.tab_tables, "Equity Curve CSV Preview")
        self.equity_table.grid(row=1, column=1, sticky="nsew", padx=(4, 0), pady=(6, 0))

    def _build_json_tab(self):
        self.tab_json.columnconfigure(0, weight=1)
        self.tab_json.rowconfigure(0, weight=1)

        self.json_viewer = TextViewer(self.tab_json, "Strategy JSON")
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

        if self.filtered_ids:
            first = self.filtered_ids[0]
            self._select_strategy_in_listbox(first)
            self._load_strategy(first)
        else:
            self._clear_views()

        count = 0 if self.summary_df is None or self.summary_df.empty else len(self.summary_df)
        self.status_var.set(f"Loaded strategy summary | strategies={count}")

    def _apply_filter(self):
        self.strategy_listbox.delete(0, "end")

        if self.summary_df is None or self.summary_df.empty:
            self.filtered_ids = []
            return

        q = self.search_var.get().strip().lower()
        ids = self.summary_df["strategy_id"].astype(str).tolist()

        if q:
            ids = [x for x in ids if q in x.lower()]

        self.filtered_ids = ids

        for sid in ids:
            self.strategy_listbox.insert("end", sid)

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
        sid = self.strategy_listbox.get(sel[0])
        self._load_strategy(sid)

    # --------------------------------------------------------
    # LOAD STRATEGY
    # --------------------------------------------------------

    def _load_strategy(self, strategy_id: str):
        self.current_strategy_id = strategy_id

        if self.summary_df is None or self.summary_df.empty:
            self._clear_views()
            return

        row_df = self.summary_df[self.summary_df["strategy_id"].astype(str) == strategy_id].copy()
        if row_df.empty:
            self._clear_views()
            return

        row = row_df.iloc[0].to_dict()

        self.kpis["trade_count"].set(safe_text(row.get("trade_count")), "trades")
        self.kpis["profit_factor"].set(format_num(row.get("profit_factor"), 3))
        self.kpis["net_profit"].set(format_num(row.get("net_profit"), 2))
        self.kpis["max_drawdown_abs"].set(format_num(row.get("max_drawdown_abs"), 2))
        self.kpis["win_rate"].set(format_pct(row.get("win_rate"), 2))
        self.kpis["avg_trade"].set(format_num(row.get("avg_trade"), 2))

        # files
        equity_path = EQUITY_DIR / f"{sanitize_name(strategy_id)}__equity_curve.csv"
        json_path = STRATEGY_JSON_DIR / f"{sanitize_name(strategy_id)}__summary.json"

        equity_df = read_csv(equity_path)
        payload = load_json(json_path)

        # charts
        self._update_charts(strategy_id, equity_df, payload)

        # visuals
        self.img_equity.set_image(VISUAL_EQUITY_DIR / f"{sanitize_name(strategy_id)}__equity.png")
        self.img_dd.set_image(VISUAL_DRAWDOWN_DIR / f"{sanitize_name(strategy_id)}__drawdown.png")
        self.img_pnl.set_image(VISUAL_PNL_DIR / f"{sanitize_name(strategy_id)}__pnl_distribution.png")
        self.img_mfe.set_image(VISUAL_MFE_MAE_DIR / f"{sanitize_name(strategy_id)}__mfe_mae.png")
        self.img_monthly.set_image(VISUAL_MONTHLY_DIR / f"{sanitize_name(strategy_id)}__monthly_pnl.png")

        # tables
        summary_view_df = pd.DataFrame(
            [{"metric": k, "value": safe_text(v)} for k, v in row.items()]
        )
        self.summary_table.set_dataframe(summary_view_df, max_rows=200)
        self.calendar_table.set_dataframe(self._build_calendar_df(payload), max_rows=200)
        self.equity_table.set_dataframe(equity_df, max_rows=80)

        # json
        pretty = json.dumps(payload, ensure_ascii=False, indent=2) if payload else "{}"
        self.json_viewer.set_text(pretty)

        self.status_var.set(f"Loaded strategy: {strategy_id}")

    def _update_charts(self, strategy_id: str, equity_df, payload: Dict):
        # Equity / DD
        if pd is not None and equity_df is not None and not equity_df.empty:
            if "trade_index" in equity_df.columns and "equity" in equity_df.columns:
                self.chart_equity.draw_line(
                    x=equity_df["trade_index"].tolist(),
                    y=equity_df["equity"].tolist(),
                    xlabel="Trade Index",
                    ylabel="Equity",
                    title=f"Equity Curve | {strategy_id}",
                )
            else:
                self.chart_equity.clear("No equity data")

            if "trade_index" in equity_df.columns and "drawdown_abs" in equity_df.columns:
                self.chart_dd.draw_line(
                    x=equity_df["trade_index"].tolist(),
                    y=equity_df["drawdown_abs"].tolist(),
                    xlabel="Trade Index",
                    ylabel="Drawdown",
                    title=f"Drawdown | {strategy_id}",
                )
            else:
                self.chart_dd.clear("No drawdown data")

            pnl_col = None
            for c in ["net_sum", "pnl", "trade_pnl"]:
                if c in equity_df.columns:
                    pnl_col = c
                    break

            if pnl_col is not None:
                pnl_vals = pd.to_numeric(equity_df[pnl_col], errors="coerce").dropna().tolist()
                self.chart_hist.draw_hist(
                    pnl_vals,
                    bins=30,
                    xlabel="Trade PnL",
                    title=f"PnL Distribution | {strategy_id}",
                )
            else:
                self.chart_hist.clear("No trade pnl in equity csv")
        else:
            self.chart_equity.clear()
            self.chart_dd.clear()
            self.chart_hist.clear()

        # Monthly
        monthly = {}
        if isinstance(payload, dict):
            cal = payload.get("calendar_segments", {})
            if isinstance(cal, dict):
                monthly = cal.get("monthly", {}) or {}

        if monthly:
            items = sorted(monthly.items(), key=lambda x: x[0])
            labels = [k for k, _ in items]
            values = [safe_float(v.get("net_profit")) or 0.0 for _, v in items]
            self.chart_monthly.draw_bar(
                labels=labels,
                values=values,
                ylabel="Net PnL",
                title=f"Monthly Net PnL | {strategy_id}",
            )
        else:
            self.chart_monthly.clear("No monthly segment data")

    def _build_calendar_df(self, payload: Dict):
        if pd is None:
            return None

        rows = []
        cal = payload.get("calendar_segments", {}) if isinstance(payload, dict) else {}
        monthly = cal.get("monthly", {}) if isinstance(cal, dict) else {}
        weekly = cal.get("weekly", {}) if isinstance(cal, dict) else {}

        for k, v in monthly.items():
            if isinstance(v, dict):
                rows.append({
                    "segment_type": "monthly",
                    "segment": k,
                    "trade_count": v.get("trade_count"),
                    "net_profit": v.get("net_profit"),
                    "avg_trade": v.get("avg_trade"),
                    "median_trade": v.get("median_trade"),
                    "win_rate": v.get("win_rate"),
                })

        for k, v in weekly.items():
            if isinstance(v, dict):
                rows.append({
                    "segment_type": "weekly",
                    "segment": k,
                    "trade_count": v.get("trade_count"),
                    "net_profit": v.get("net_profit"),
                    "avg_trade": v.get("avg_trade"),
                    "median_trade": v.get("median_trade"),
                    "win_rate": v.get("win_rate"),
                })

        if not rows:
            return pd.DataFrame(columns=[
                "segment_type", "segment", "trade_count", "net_profit", "avg_trade", "median_trade", "win_rate"
            ])

        return pd.DataFrame(rows)

    def _clear_views(self):
        for card in self.kpis.values():
            card.set("-", "")

        if pd is not None:
            self.summary_table.set_dataframe(pd.DataFrame())
            self.calendar_table.set_dataframe(pd.DataFrame())
            self.equity_table.set_dataframe(pd.DataFrame())

        self.chart_equity.clear()
        self.chart_dd.clear()
        self.chart_hist.clear()
        self.chart_monthly.clear()

        self.json_viewer.set_text("{}")

        self.img_equity.set_image(Path("__missing__"))
        self.img_dd.set_image(Path("__missing__"))
        self.img_pnl.set_image(Path("__missing__"))
        self.img_mfe.set_image(Path("__missing__"))
        self.img_monthly.set_image(Path("__missing__"))

        self.status_var.set("No strategy selected")


# ============================================================
# STANDALONE
# ============================================================

def main():
    root = tk.Tk()
    root.title("Strategy Analytics")
    root.geometry("1900x1080")
    root.configure(bg=BG_APP)

    panel = StrategyAnalyticsPanel(root)
    panel.pack(fill="both", expand=True)

    root.mainloop()


if __name__ == "__main__":
    main()