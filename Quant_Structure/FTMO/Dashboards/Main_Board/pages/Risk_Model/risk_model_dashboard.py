# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Dashboards/Main_Board/pages/Risk_Model/risk_model_dashboard.py

Risk Model Dashboard
- visualisiert Risk_Modeling_Layer
- zeigt Lots, DD, Risk Budgets, Constraints und Detail-JSONs
"""

from __future__ import annotations

import json
import math
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

RISK_ROOT = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Analysis"
    / "Risk_Modeling_Layer"
)

SUMMARY_CSV_PATH = RISK_ROOT / "risk_modeling_summary.csv"
SUMMARY_JSON_PATH = RISK_ROOT / "risk_modeling_summary.json"
STRATEGY_JSON_DIR = RISK_ROOT / "strategy_json"
LOT_TABLE_DIR = RISK_ROOT / "lot_tables"


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


def budget_color(budget: str) -> str:
    b = safe_text(budget).upper()
    if b in {"VERY_HIGH", "HIGH"}:
        return FG_POS
    if b in {"MEDIUM", "LOW"}:
        return FG_WARN
    if b in {"VERY_LOW", ""}:
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
        self.value_lbl.configure(fg=fg or FG_WHITE)


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
            "RiskModel.Treeview",
            background=BG_SURFACE,
            foreground=FG_MAIN,
            fieldbackground=BG_SURFACE,
            bordercolor=BORDER,
            rowheight=24,
            font=FONT_LABEL,
        )
        style.configure(
            "RiskModel.Treeview.Heading",
            background=BG_TOP,
            foreground=FG_WHITE,
            relief="flat",
            font=FONT_LABEL,
        )

        body = tk.Frame(self, bg=BG_SURFACE)
        body.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.tree = ttk.Treeview(body, style="RiskModel.Treeview", show="headings")
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

        head = tk.Frame(self, bg=BG_SURFACE)
        head.pack(fill="x", padx=10, pady=(8, 6))
        tk.Label(head, text=title, font=FONT_SECTION, bg=BG_SURFACE, fg=FG_WHITE).pack(side="left")

        self.chart_host = tk.Frame(self, bg=BG_SURFACE)
        self.chart_host.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        self.figure = None
        self.canvas_widget = None
        self.clear()

    def clear(self, message: str = "No chart data"):
        for child in self.chart_host.winfo_children():
            child.destroy()
        lbl = tk.Label(self.chart_host, text=message, bg=BG_SURFACE, fg=FG_MUTED, font=FONT_LABEL)
        lbl.pack(expand=True)

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

class RiskModelDashboardPanel(tk.Frame):
    def __init__(self, parent, app=None, ftmo_root: Optional[Path] = None):
        super().__init__(parent, bg=BG_APP)
        self.app = app
        self.ftmo_root = ftmo_root or FTMO_ROOT

        self.summary_df = pd.DataFrame() if pd is not None else None
        self.filtered_df = pd.DataFrame() if pd is not None else None
        self.current_strategy_id: Optional[str] = None

        self.search_var = tk.StringVar(value="")
        self.budget_filter_var = tk.StringVar(value="ALL")
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

        tk.Label(top, text="Risk Model Dashboard", font=FONT_TITLE, bg=BG_TOP, fg=FG_WHITE).grid(
            row=0, column=0, sticky="w", padx=14
        )
        tk.Label(top, text="Risk_Modeling_Layer visual explorer", font=FONT_SUBTITLE, bg=BG_TOP, fg=FG_SUBTLE).grid(
            row=0, column=1, sticky="w", padx=(10, 0)
        )

        budget_combo = ttk.Combobox(
            top,
            textvariable=self.budget_filter_var,
            values=["ALL", "VERY_HIGH", "HIGH", "MEDIUM", "LOW", "VERY_LOW"],
            state="readonly",
            width=14,
        )
        budget_combo.grid(row=0, column=2, sticky="e", padx=(10, 8), pady=10)
        budget_combo.bind("<<ComboboxSelected>>", lambda _e: self._apply_filter())

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
            ("avg_lot", "Avg Rec Lot"),
            ("avg_safe_lot", "Avg Safe Lot"),
            ("avg_expected_dd", "Avg Exp DD"),
            ("avg_stress_dd", "Avg Stress DD"),
            ("avg_budget", "Top Budget Group"),
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

        style.configure("RiskNotebook.TNotebook", background=BG_APP, borderwidth=0)
        style.configure(
            "RiskNotebook.TNotebook.Tab",
            background=BG_TOP,
            foreground=FG_MAIN,
            padding=(14, 8),
            font=FONT_LABEL,
        )
        style.map(
            "RiskNotebook.TNotebook.Tab",
            background=[("selected", BG_SURFACE)],
            foreground=[("selected", FG_WHITE)],
        )

        self.notebook = ttk.Notebook(right, style="RiskNotebook.TNotebook")
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

        self.detail_table = SimpleTable(self.tab_overview, "Risk Model Summary")
        self.detail_table.grid(row=0, column=0, columnspan=2, sticky="nsew", pady=(0, 6))

        self.chart_budget = ChartPanel(self.tab_overview, "Risk Budget Distribution")
        self.chart_budget.grid(row=1, column=0, sticky="nsew", padx=(0, 4), pady=(6, 0))

        self.chart_lots = ChartPanel(self.tab_overview, "Recommended vs Safe Lots")
        self.chart_lots.grid(row=1, column=1, sticky="nsew", padx=(4, 0), pady=(6, 0))

    def _build_charts_tab(self):
        self.tab_charts.columnconfigure(0, weight=1)
        self.tab_charts.columnconfigure(1, weight=1)
        self.tab_charts.rowconfigure(0, weight=1)
        self.tab_charts.rowconfigure(1, weight=1)

        self.chart_expected_stress = ChartPanel(self.tab_charts, "Expected DD vs Stress DD")
        self.chart_expected_stress.grid(row=0, column=0, sticky="nsew", padx=(0, 4), pady=(0, 4))

        self.chart_rec_vs_safe = ChartPanel(self.tab_charts, "Recommended Lot vs Max Safe Lot")
        self.chart_rec_vs_safe.grid(row=0, column=1, sticky="nsew", padx=(4, 0), pady=(0, 4))

        self.chart_ranking = ChartPanel(self.tab_charts, "Top Recommended Lots")
        self.chart_ranking.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(4, 0))

    def _build_tables_tab(self):
        self.tab_tables.columnconfigure(0, weight=1)
        self.tab_tables.rowconfigure(0, weight=1)
        self.tab_tables.rowconfigure(1, weight=1)

        self.summary_table = SimpleTable(self.tab_tables, "Risk Modeling Ranking Table")
        self.summary_table.grid(row=0, column=0, sticky="nsew", pady=(0, 6))

        self.lot_table = SimpleTable(self.tab_tables, "Lot Table")
        self.lot_table.grid(row=1, column=0, sticky="nsew", pady=(6, 0))

    def _build_json_tab(self):
        self.tab_json.columnconfigure(0, weight=1)
        self.tab_json.rowconfigure(0, weight=1)

        self.json_viewer = TextViewer(self.tab_json, "Risk Model JSON")
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
        self.status_var.set(f"Loaded risk modeling summary | strategies={count}")

    def _apply_filter(self):
        self.strategy_listbox.delete(0, "end")

        if self.summary_df is None or self.summary_df.empty:
            self.filtered_df = pd.DataFrame() if pd is not None else None
            return

        df = self.summary_df.copy()

        q = self.search_var.get().strip().lower()
        budget_filter = self.budget_filter_var.get().strip().upper()

        if q:
            df = df[df["strategy_id"].astype(str).str.lower().str.contains(q, na=False)]

        if budget_filter and budget_filter != "ALL" and "recommended_risk_budget" in df.columns:
            df = df[df["recommended_risk_budget"].astype(str).str.upper() == budget_filter]

        self.filtered_df = df.reset_index(drop=True)

        for sid in self.filtered_df["strategy_id"].astype(str).tolist():
            self.strategy_listbox.insert("end", sid)

        self._update_global_views()

    def _update_global_views(self):
        if self.filtered_df is None or self.filtered_df.empty:
            self.kpis["count"].set("0")
            self.kpis["avg_lot"].set("-")
            self.kpis["avg_safe_lot"].set("-")
            self.kpis["avg_expected_dd"].set("-")
            self.kpis["avg_stress_dd"].set("-")
            self.kpis["avg_budget"].set("-")

            self.summary_table.set_dataframe(pd.DataFrame() if pd is not None else None)
            self.chart_budget.clear()
            self.chart_lots.clear()
            self.chart_expected_stress.clear()
            self.chart_rec_vs_safe.clear()
            self.chart_ranking.clear()
            return

        df = self.filtered_df.copy()

        self.kpis["count"].set(str(len(df)))

        avg_rec_lot = pd.to_numeric(df.get("recommended_lot_size"), errors="coerce").dropna().mean()
        avg_safe_lot = pd.to_numeric(df.get("max_safe_lot_size"), errors="coerce").dropna().mean()
        avg_exp_dd = pd.to_numeric(df.get("expected_dd_at_recommended_lot"), errors="coerce").dropna().mean()
        avg_stress_dd = pd.to_numeric(df.get("stress_dd_at_recommended_lot"), errors="coerce").dropna().mean()

        self.kpis["avg_lot"].set(format_num(avg_rec_lot, 2))
        self.kpis["avg_safe_lot"].set(format_num(avg_safe_lot, 2))
        self.kpis["avg_expected_dd"].set(format_num(avg_exp_dd, 2))
        self.kpis["avg_stress_dd"].set(format_num(avg_stress_dd, 2))

        if "recommended_risk_budget" in df.columns:
            top_budget = df["recommended_risk_budget"].astype(str).value_counts()
            budget_name = top_budget.index[0] if len(top_budget) else "-"
        else:
            budget_name = "-"
        self.kpis["avg_budget"].set(budget_name, fg=budget_color(budget_name))

        rank_cols = [
            c for c in [
                "strategy_id",
                "recommended_lot_size",
                "max_safe_lot_size",
                "recommended_risk_budget",
                "expected_dd_at_recommended_lot",
                "stress_dd_at_recommended_lot",
                "max_loss_per_day_recommendation",
            ] if c in df.columns
        ]
        rank_df = df[rank_cols].copy()
        if "recommended_lot_size" in rank_df.columns:
            rank_df = rank_df.sort_values("recommended_lot_size", ascending=False)

        self.summary_table.set_dataframe(rank_df, max_rows=200)
        self._update_global_charts(df)

    def _update_global_charts(self, df):
        # budget distribution
        if "recommended_risk_budget" in df.columns:
            budget_counts = df["recommended_risk_budget"].astype(str).value_counts().to_dict()
            self.chart_budget.draw_bar(
                labels=list(budget_counts.keys()),
                values=list(budget_counts.values()),
                title="Risk Budget Distribution",
            )
        else:
            self.chart_budget.clear()

        # top lots bar
        top_df = df.copy()
        if "recommended_lot_size" in top_df.columns:
            top_df["recommended_lot_size"] = pd.to_numeric(top_df["recommended_lot_size"], errors="coerce")
            top_df = top_df.dropna(subset=["recommended_lot_size"]).sort_values("recommended_lot_size", ascending=False).head(15)
            self.chart_lots.draw_bar(
                labels=top_df["strategy_id"].astype(str).tolist(),
                values=top_df["recommended_lot_size"].tolist(),
                title="Top Recommended Lots",
            )
        else:
            self.chart_lots.clear()

        # expected vs stress
        d1 = pd.DataFrame({
            "x": pd.to_numeric(df.get("expected_dd_at_recommended_lot"), errors="coerce"),
            "y": pd.to_numeric(df.get("stress_dd_at_recommended_lot"), errors="coerce"),
            "label": df["strategy_id"].astype(str),
        }).dropna()
        self.chart_expected_stress.draw_scatter(
            x=d1["x"].tolist(),
            y=d1["y"].tolist(),
            labels=d1["label"].tolist(),
            title="Expected DD vs Stress DD",
            xlabel="Expected DD",
            ylabel="Stress DD",
        )

        # recommended vs safe
        d2 = pd.DataFrame({
            "x": pd.to_numeric(df.get("recommended_lot_size"), errors="coerce"),
            "y": pd.to_numeric(df.get("max_safe_lot_size"), errors="coerce"),
            "label": df["strategy_id"].astype(str),
        }).dropna()
        self.chart_rec_vs_safe.draw_scatter(
            x=d2["x"].tolist(),
            y=d2["y"].tolist(),
            labels=d2["label"].tolist(),
            title="Recommended Lot vs Max Safe Lot",
            xlabel="Recommended Lot",
            ylabel="Max Safe Lot",
        )

        # ranking
        top_df2 = df.copy()
        if "max_safe_lot_size" in top_df2.columns:
            top_df2["max_safe_lot_size"] = pd.to_numeric(top_df2["max_safe_lot_size"], errors="coerce")
            top_df2 = top_df2.dropna(subset=["max_safe_lot_size"]).sort_values("max_safe_lot_size", ascending=False).head(15)
            self.chart_ranking.draw_bar(
                labels=top_df2["strategy_id"].astype(str).tolist(),
                values=top_df2["max_safe_lot_size"].tolist(),
                title="Top Max Safe Lots",
            )
        else:
            self.chart_ranking.clear()

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
        detail_df = pd.DataFrame([{"metric": k, "value": safe_text(v)} for k, v in row.items()])
        self.detail_table.set_dataframe(detail_df, max_rows=200)

        json_path = STRATEGY_JSON_DIR / f"{sanitize_name(strategy_id)}__risk_model.json"
        if not json_path.exists():
            json_path = STRATEGY_JSON_DIR / f"{sanitize_name(strategy_id)}__summary.json"

        payload = load_json(json_path)
        pretty = json.dumps(payload, ensure_ascii=False, indent=2) if payload else "{}"
        self.json_viewer.set_text(pretty)

        lot_table_path = LOT_TABLE_DIR / f"{sanitize_name(strategy_id)}__lot_table.csv"
        lot_df = read_csv(lot_table_path)
        self.lot_table.set_dataframe(lot_df, max_rows=200)

        budget = safe_text(row.get("recommended_risk_budget"))
        rec_lot = format_num(row.get("recommended_lot_size"), 2)
        self.status_var.set(f"Loaded strategy: {strategy_id} | budget={budget or '-'} | rec_lot={rec_lot}")

    def _clear_strategy_views(self):
        self.detail_table.set_dataframe(pd.DataFrame() if pd is not None else None)
        self.lot_table.set_dataframe(pd.DataFrame() if pd is not None else None)
        self.json_viewer.set_text("{}")
        self.status_var.set("No strategy selected")


# ============================================================
# STANDALONE
# ============================================================

def main():
    root = tk.Tk()
    root.title("Risk Model Dashboard")
    root.geometry("1900x1080")
    root.configure(bg=BG_APP)

    panel = RiskModelDashboardPanel(root)
    panel.pack(fill="both", expand=True)

    root.mainloop()


if __name__ == "__main__":
    main()