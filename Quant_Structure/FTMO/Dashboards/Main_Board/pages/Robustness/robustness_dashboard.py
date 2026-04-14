# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Dashboards/Main_Board/pages/Robustness/robustness_dashboard.py

Robustness Dashboard
- Monte Carlo
- Drawdown Distribution
- Noise Test
- Shuffle Test
- robust gegen unterschiedliche JSON-Strukturen
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd
import tkinter as tk
from tkinter import ttk

from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg


# ============================================================
# ROOT / PATHS
# ============================================================

def find_ftmo_root(start: Path) -> Path:
    cur = start.resolve()
    for p in [cur] + list(cur.parents):
        if (p / "Data_Center").exists() and (p / "Dashboards").exists():
            return p
    raise RuntimeError(
        f"FTMO root not found. Expected folder with 'Data_Center' and 'Dashboards'. Start={start}"
    )


SCRIPT_PATH = Path(__file__).resolve()
FTMO_ROOT = find_ftmo_root(SCRIPT_PATH)

STRATEGY_LAYER_ROOT = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Analysis"
    / "Strategy_Layer"
)

STRATEGY_JSON_DIR = STRATEGY_LAYER_ROOT / "strategy_json"
EQUITY_CURVE_DIR = STRATEGY_LAYER_ROOT / "equity_curves"


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
FONT_MONO = ("Consolas", 9)
FONT_CARD_TITLE = ("Segoe UI", 9)
FONT_CARD_VALUE = ("Segoe UI", 17, "bold")


# ============================================================
# HELPERS
# ============================================================

def safe_text(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if pd.isna(x):
            return None
        v = float(x)
        if np.isinf(v):
            return None
        return float(v)
    except Exception:
        return None


def format_num(x, digits: int = 2) -> str:
    v = safe_float(x)
    if v is None:
        return "-"
    return f"{v:,.{digits}f}"


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def sanitize_name(name: str) -> str:
    bad = '<>:"/\\|?*'
    return "".join("_" if c in bad else c for c in str(name)).strip()


def make_divider(parent, pady=(0, 0)):
    tk.Frame(parent, bg=DIVIDER, height=1).pack(fill="x", pady=pady)


# ============================================================
# DATA EXTRACTION
# ============================================================

def extract_equity_from_payload(payload: Dict[str, Any]) -> List[float]:
    """
    Unterstützt mehrere mögliche Formate:
    1) payload["equity_curve"] = [100000, 100020, ...]
    2) payload["equity_curve"] = [{"equity": ...}, ...]
    3) payload["summary"] only -> kein equity
    """
    if not isinstance(payload, dict):
        return []

    eq = payload.get("equity_curve")

    if isinstance(eq, list):
        if not eq:
            return []

        # list of numbers
        if all(isinstance(x, (int, float)) for x in eq):
            return [float(x) for x in eq]

        # list of dicts
        if all(isinstance(x, dict) for x in eq):
            out = []
            for row in eq:
                v = safe_float(row.get("equity"))
                if v is not None:
                    out.append(v)
            return out

    return []


def load_equity_curve_csv(strategy_id: str) -> List[float]:
    path = EQUITY_CURVE_DIR / f"{sanitize_name(strategy_id)}__equity_curve.csv"
    if not path.exists():
        return []

    df = read_csv(path)
    if df.empty:
        return []

    if "equity" in df.columns:
        s = pd.to_numeric(df["equity"], errors="coerce").dropna()
        return s.astype(float).tolist()

    return []


def compute_returns_from_equity(equity: List[float]) -> np.ndarray:
    if equity is None or len(equity) < 2:
        return np.array([], dtype=float)

    arr = np.asarray(equity, dtype=float)
    diffs = np.diff(arr)
    diffs = diffs[~np.isnan(diffs)]
    return diffs.astype(float)


def load_all_strategy_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    if not STRATEGY_JSON_DIR.exists():
        return rows

    files = sorted(STRATEGY_JSON_DIR.glob("*.json"))

    for f in files:
        payload = load_json(f)

        strategy_id = safe_text(payload.get("strategy_id"))
        if not strategy_id:
            strategy_id = f.stem.replace("__summary", "")

        equity = extract_equity_from_payload(payload)
        if not equity:
            equity = load_equity_curve_csv(strategy_id)

        returns = compute_returns_from_equity(equity)

        rows.append({
            "strategy_id": strategy_id,
            "payload": payload,
            "equity": equity,
            "returns": returns,
            "trade_count": int(len(returns)),
        })

    return rows


# ============================================================
# ROBUSTNESS ENGINE
# ============================================================

def monte_carlo_simulations(returns: np.ndarray, runs: int = 200) -> List[np.ndarray]:
    if returns is None or len(returns) == 0:
        return []

    sims: List[np.ndarray] = []
    for _ in range(runs):
        shuffled = np.random.permutation(returns)
        sims.append(np.cumsum(shuffled))
    return sims


def drawdown_series(equity_like: np.ndarray) -> np.ndarray:
    if equity_like is None or len(equity_like) == 0:
        return np.array([], dtype=float)

    arr = np.asarray(equity_like, dtype=float)
    peak = np.maximum.accumulate(arr)
    dd = arr - peak
    return dd


def max_drawdown_abs(equity_like: np.ndarray) -> Optional[float]:
    dd = drawdown_series(equity_like)
    if len(dd) == 0:
        return None
    return float(np.min(dd))


def noise_test_curve(returns: np.ndarray, noise_frac: float = 0.10) -> np.ndarray:
    if returns is None or len(returns) == 0:
        return np.array([], dtype=float)

    std = float(np.std(returns))
    noise = np.random.normal(0.0, std * noise_frac, len(returns))
    noisy_returns = returns + noise
    return np.cumsum(noisy_returns)


def shuffle_test_curve(returns: np.ndarray) -> np.ndarray:
    if returns is None or len(returns) == 0:
        return np.array([], dtype=float)

    shuffled = np.random.permutation(returns)
    return np.cumsum(shuffled)


def bootstrap_curves(returns: np.ndarray, runs: int = 100) -> List[np.ndarray]:
    if returns is None or len(returns) == 0:
        return []

    out = []
    n = len(returns)
    for _ in range(runs):
        sample = np.random.choice(returns, size=n, replace=True)
        out.append(np.cumsum(sample))
    return out


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
        self.value_lbl.configure(text=value, fg=fg or FG_WHITE)
        self.sub_lbl.configure(text=sub)


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
            "Robustness.Treeview",
            background=BG_SURFACE,
            foreground=FG_MAIN,
            fieldbackground=BG_SURFACE,
            bordercolor=BORDER,
            rowheight=24,
            font=FONT_LABEL,
        )
        style.configure(
            "Robustness.Treeview.Heading",
            background=BG_TOP,
            foreground=FG_WHITE,
            relief="flat",
            font=FONT_LABEL,
        )

        body = tk.Frame(self, bg=BG_SURFACE)
        body.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.tree = ttk.Treeview(body, style="Robustness.Treeview", show="headings")
        self.tree.pack(side="left", fill="both", expand=True)

        ysb = ttk.Scrollbar(body, orient="vertical", command=self.tree.yview)
        ysb.pack(side="right", fill="y")
        self.tree.configure(yscrollcommand=ysb.set)

    def set_dataframe(self, df: pd.DataFrame, max_rows: int = 200):
        self.tree.delete(*self.tree.get_children())

        if df is None or df.empty:
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


class Chart:
    def __init__(self, parent, title: str = ""):
        self.parent = parent
        self.title = title

        self.fig = Figure(figsize=(5, 3), dpi=100)
        self.ax = self.fig.add_subplot(111)
        self.fig.patch.set_facecolor(BG_SURFACE)
        self.ax.set_facecolor(BG_CARD_ALT)

        self.canvas = FigureCanvasTkAgg(self.fig, parent)
        self.widget = self.canvas.get_tk_widget()

    def grid(self, **kwargs):
        self.widget.grid(**kwargs)

    def clear(self, title: Optional[str] = None, message: Optional[str] = None):
        self.ax.clear()
        self.ax.set_facecolor(BG_CARD_ALT)
        self.ax.tick_params(colors=FG_MUTED, labelsize=8)
        for spine in self.ax.spines.values():
            spine.set_color(BORDER)
        self.ax.grid(True, alpha=0.25)
        self.ax.set_title(title or self.title, color=FG_WHITE, fontsize=10)
        if message:
            self.ax.text(0.5, 0.5, message, ha="center", va="center", color=FG_MUTED, transform=self.ax.transAxes)
        self.fig.tight_layout()
        self.canvas.draw()

    def plot_equity(self, series_list: List[np.ndarray], title: Optional[str] = None):
        self.ax.clear()
        self.ax.set_facecolor(BG_CARD_ALT)

        if not series_list:
            self.clear(title=title, message="No equity data")
            return

        for s in series_list:
            if s is None or len(s) == 0:
                continue
            self.ax.plot(s, alpha=0.30)

        self.ax.set_title(title or self.title, color=FG_WHITE, fontsize=10)
        self.ax.tick_params(colors=FG_MUTED, labelsize=8)
        self.ax.grid(True, alpha=0.25)
        for spine in self.ax.spines.values():
            spine.set_color(BORDER)

        self.fig.tight_layout()
        self.canvas.draw()

    def plot_hist(self, data: List[float], title: Optional[str] = None):
        self.ax.clear()
        self.ax.set_facecolor(BG_CARD_ALT)

        if not data:
            self.clear(title=title, message="No histogram data")
            return

        self.ax.hist(data, bins=30)
        self.ax.set_title(title or self.title, color=FG_WHITE, fontsize=10)
        self.ax.tick_params(colors=FG_MUTED, labelsize=8)
        self.ax.grid(True, alpha=0.25)
        for spine in self.ax.spines.values():
            spine.set_color(BORDER)

        self.fig.tight_layout()
        self.canvas.draw()

    def plot_bar(self, labels: List[str], values: List[float], title: Optional[str] = None):
        self.ax.clear()
        self.ax.set_facecolor(BG_CARD_ALT)

        if not labels or not values:
            self.clear(title=title, message="No bar data")
            return

        x = list(range(len(labels)))
        self.ax.bar(x, values)
        self.ax.set_xticks(x)
        self.ax.set_xticklabels(labels, rotation=35, ha="right", color=FG_MUTED, fontsize=8)

        self.ax.set_title(title or self.title, color=FG_WHITE, fontsize=10)
        self.ax.tick_params(colors=FG_MUTED, labelsize=8)
        self.ax.grid(True, alpha=0.25)
        for spine in self.ax.spines.values():
            spine.set_color(BORDER)

        self.fig.tight_layout()
        self.canvas.draw()


# ============================================================
# MAIN DASHBOARD
# ============================================================

class RobustnessDashboardPanel(tk.Frame):
    def __init__(self, parent, app=None, ftmo_root: Optional[Path] = None):
        super().__init__(parent, bg=BG_APP)
        self.app = app
        self.ftmo_root = ftmo_root or FTMO_ROOT

        self.rows: List[Dict[str, Any]] = []
        self.filtered_rows: List[Dict[str, Any]] = []
        self.current_row: Optional[Dict[str, Any]] = None

        self.search_var = tk.StringVar(value="")
        self.status_var = tk.StringVar(value="Ready")

        self.kpis: Dict[str, KpiCard] = {}

        self._build_ui()
        self.reload_all()

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

        tk.Label(top, text="Robustness Dashboard", font=FONT_TITLE, bg=BG_TOP, fg=FG_WHITE).grid(
            row=0, column=0, sticky="w", padx=14
        )
        tk.Label(top, text="Monte Carlo / Noise / Shuffle / Drawdown", font=FONT_SUBTITLE, bg=BG_TOP, fg=FG_SUBTLE).grid(
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
            width=32,
        )
        search.grid(row=0, column=2, sticky="e", padx=(0, 8), pady=10)
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

        # KPI row
        kpi_row = tk.Frame(right, bg=BG_APP)
        kpi_row.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        for i in range(5):
            kpi_row.columnconfigure(i, weight=1)

        defs = [
            ("count", "Strategies"),
            ("trades", "Trades"),
            ("mc_dd95", "MC DD 95%"),
            ("noise_dd", "Noise DD"),
            ("shuffle_dd", "Shuffle DD"),
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

        style.configure("RobustnessNotebook.TNotebook", background=BG_APP, borderwidth=0)
        style.configure(
            "RobustnessNotebook.TNotebook.Tab",
            background=BG_TOP,
            foreground=FG_MAIN,
            padding=(14, 8),
            font=FONT_LABEL,
        )
        style.map(
            "RobustnessNotebook.TNotebook.Tab",
            background=[("selected", BG_SURFACE)],
            foreground=[("selected", FG_WHITE)],
        )

        self.notebook = ttk.Notebook(right, style="RobustnessNotebook.TNotebook")
        self.notebook.grid(row=1, column=0, sticky="nsew")

        self.tab_overview = tk.Frame(self.notebook, bg=BG_APP)
        self.tab_charts = tk.Frame(self.notebook, bg=BG_APP)
        self.tab_table = tk.Frame(self.notebook, bg=BG_APP)
        self.tab_json = tk.Frame(self.notebook, bg=BG_APP)

        self.notebook.add(self.tab_overview, text="Overview")
        self.notebook.add(self.tab_charts, text="Charts")
        self.notebook.add(self.tab_table, text="Table")
        self.notebook.add(self.tab_json, text="JSON")

        self._build_overview_tab()
        self._build_charts_tab()
        self._build_table_tab()
        self._build_json_tab()

    def _build_overview_tab(self):
        self.tab_overview.columnconfigure(0, weight=1)
        self.tab_overview.rowconfigure(0, weight=1)
        self.tab_overview.rowconfigure(1, weight=1)

        self.detail_table = SimpleTable(self.tab_overview, "Robustness Detail")
        self.detail_table.grid(row=0, column=0, sticky="nsew", pady=(0, 6))

        chart_grid = tk.Frame(self.tab_overview, bg=BG_APP)
        chart_grid.grid(row=1, column=0, sticky="nsew", pady=(6, 0))
        chart_grid.columnconfigure(0, weight=1)
        chart_grid.columnconfigure(1, weight=1)
        chart_grid.rowconfigure(0, weight=1)
        chart_grid.rowconfigure(1, weight=1)

        self.chart_mc = Chart(chart_grid, "Monte Carlo Equity")
        self.chart_mc.grid(row=0, column=0, sticky="nsew", padx=(0, 4), pady=(0, 4))

        self.chart_dd = Chart(chart_grid, "Monte Carlo Drawdown Distribution")
        self.chart_dd.grid(row=0, column=1, sticky="nsew", padx=(4, 0), pady=(0, 4))

        self.chart_noise = Chart(chart_grid, "Noise Test Equity")
        self.chart_noise.grid(row=1, column=0, sticky="nsew", padx=(0, 4), pady=(4, 0))

        self.chart_shuffle = Chart(chart_grid, "Shuffle Test Equity")
        self.chart_shuffle.grid(row=1, column=1, sticky="nsew", padx=(4, 0), pady=(4, 0))

    def _build_charts_tab(self):
        self.tab_charts.columnconfigure(0, weight=1)
        self.tab_charts.rowconfigure(0, weight=1)

        wrap = tk.Frame(self.tab_charts, bg=BG_APP)
        wrap.grid(row=0, column=0, sticky="nsew")
        wrap.columnconfigure(0, weight=1)
        wrap.columnconfigure(1, weight=1)
        wrap.rowconfigure(0, weight=1)

        self.chart_global_mcdd = Chart(wrap, "Global MC DD by Strategy")
        self.chart_global_mcdd.grid(row=0, column=0, sticky="nsew", padx=(0, 4))

        self.chart_global_trades = Chart(wrap, "Trade Count by Strategy")
        self.chart_global_trades.grid(row=0, column=1, sticky="nsew", padx=(4, 0))

    def _build_table_tab(self):
        self.tab_table.columnconfigure(0, weight=1)
        self.tab_table.rowconfigure(0, weight=1)

        self.summary_table = SimpleTable(self.tab_table, "Robustness Summary Table")
        self.summary_table.grid(row=0, column=0, sticky="nsew")

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

    def reload_all(self):
        self.rows = load_all_strategy_rows()
        self._apply_filter()

        if self.filtered_rows:
            first_id = self.filtered_rows[0]["strategy_id"]
            self._select_strategy_in_listbox(first_id)
            self._load_strategy(first_id)
        else:
            self._clear_strategy_views()

        self.status_var.set(f"Loaded robustness input | strategies={len(self.rows)}")

    def _apply_filter(self):
        self.strategy_listbox.delete(0, "end")

        q = self.search_var.get().strip().lower()
        if q:
            self.filtered_rows = [
                r for r in self.rows
                if q in safe_text(r.get("strategy_id")).lower()
            ]
        else:
            self.filtered_rows = list(self.rows)

        for row in self.filtered_rows:
            self.strategy_listbox.insert("end", row["strategy_id"])

        self._update_global_views()

    def _build_global_summary_df(self) -> pd.DataFrame:
        rows = []

        for row in self.filtered_rows:
            strategy_id = row["strategy_id"]
            rets = row["returns"]

            if rets is None or len(rets) == 0:
                rows.append({
                    "strategy_id": strategy_id,
                    "trade_count": 0,
                    "mc_dd_95": None,
                    "noise_dd": None,
                    "shuffle_dd": None,
                })
                continue

            sims = monte_carlo_simulations(rets, runs=100)
            mc_dds = [max_drawdown_abs(s) for s in sims if s is not None and len(s) > 0]
            mc_dds = [x for x in mc_dds if x is not None]

            noise_curve = noise_test_curve(rets)
            shuffle_curve = shuffle_test_curve(rets)

            rows.append({
                "strategy_id": strategy_id,
                "trade_count": len(rets),
                "mc_dd_95": float(np.quantile(mc_dds, 0.05)) if mc_dds else None,
                "noise_dd": max_drawdown_abs(noise_curve),
                "shuffle_dd": max_drawdown_abs(shuffle_curve),
            })

        return pd.DataFrame(rows)

    def _update_global_views(self):
        if not self.filtered_rows:
            self.kpis["count"].set("0")
            self.kpis["trades"].set("-")
            self.kpis["mc_dd95"].set("-")
            self.kpis["noise_dd"].set("-")
            self.kpis["shuffle_dd"].set("-")

            self.summary_table.set_dataframe(pd.DataFrame())
            self.chart_global_mcdd.clear(message="No strategies")
            self.chart_global_trades.clear(message="No strategies")
            return

        df = self._build_global_summary_df()
        self.summary_table.set_dataframe(df, max_rows=200)

        total_trades = int(df["trade_count"].fillna(0).sum()) if not df.empty else 0
        mc_mean = pd.to_numeric(df["mc_dd_95"], errors="coerce").dropna().mean() if not df.empty else None
        noise_mean = pd.to_numeric(df["noise_dd"], errors="coerce").dropna().mean() if not df.empty else None
        shuffle_mean = pd.to_numeric(df["shuffle_dd"], errors="coerce").dropna().mean() if not df.empty else None

        self.kpis["count"].set(str(len(df)))
        self.kpis["trades"].set(str(total_trades))
        self.kpis["mc_dd95"].set(format_num(mc_mean, 2))
        self.kpis["noise_dd"].set(format_num(noise_mean, 2))
        self.kpis["shuffle_dd"].set(format_num(shuffle_mean, 2))

        # Global charts
        top_df = df.copy()

        if not top_df.empty:
            mcdf = top_df.dropna(subset=["mc_dd_95"]).sort_values("mc_dd_95", ascending=True).head(15)
            self.chart_global_mcdd.plot_bar(
                labels=mcdf["strategy_id"].astype(str).tolist(),
                values=mcdf["mc_dd_95"].astype(float).tolist(),
                title="Global MC DD 95% by Strategy",
            )

            trdf = top_df.sort_values("trade_count", ascending=False).head(15)
            self.chart_global_trades.plot_bar(
                labels=trdf["strategy_id"].astype(str).tolist(),
                values=trdf["trade_count"].astype(float).tolist(),
                title="Trade Count by Strategy",
            )
        else:
            self.chart_global_mcdd.clear(message="No data")
            self.chart_global_trades.clear(message="No data")

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

    def _find_row(self, strategy_id: str) -> Optional[Dict[str, Any]]:
        for row in self.rows:
            if row["strategy_id"] == strategy_id:
                return row
        return None

    def _load_strategy(self, strategy_id: str):
        row = self._find_row(strategy_id)
        self.current_row = row

        if row is None:
            self._clear_strategy_views()
            return

        rets = row["returns"]
        equity = row["equity"]

        if rets is None or len(rets) == 0:
            self.detail_table.set_dataframe(pd.DataFrame([{
                "metric": "info",
                "value": "No returns/equity data for strategy"
            }]))
            self.chart_mc.clear(message="No returns")
            self.chart_dd.clear(message="No returns")
            self.chart_noise.clear(message="No returns")
            self.chart_shuffle.clear(message="No returns")
            self.json_viewer.set_text(json.dumps(row.get("payload", {}), ensure_ascii=False, indent=2))
            self.status_var.set(f"Loaded strategy: {strategy_id} | no robustness data")
            return

        sims = monte_carlo_simulations(rets, runs=150)
        mc_dds = [max_drawdown_abs(s) for s in sims if s is not None and len(s) > 0]
        mc_dds = [x for x in mc_dds if x is not None]

        noise_curve = noise_test_curve(rets)
        shuffle_curve = shuffle_test_curve(rets)
        boot_curves = bootstrap_curves(rets, runs=50)

        mc_dd_95 = float(np.quantile(mc_dds, 0.05)) if mc_dds else None
        noise_dd = max_drawdown_abs(noise_curve)
        shuffle_dd = max_drawdown_abs(shuffle_curve)
        base_dd = max_drawdown_abs(np.cumsum(rets))

        detail_df = pd.DataFrame([
            {"metric": "strategy_id", "value": strategy_id},
            {"metric": "trade_count", "value": len(rets)},
            {"metric": "equity_points", "value": len(equity)},
            {"metric": "base_drawdown_abs", "value": base_dd},
            {"metric": "mc_drawdown_95", "value": mc_dd_95},
            {"metric": "noise_drawdown_abs", "value": noise_dd},
            {"metric": "shuffle_drawdown_abs", "value": shuffle_dd},
            {"metric": "mean_trade_pnl", "value": float(np.mean(rets)) if len(rets) else None},
            {"metric": "std_trade_pnl", "value": float(np.std(rets)) if len(rets) else None},
        ])
        self.detail_table.set_dataframe(detail_df, max_rows=200)

        # Charts
        plot_sims = sims[:60] if len(sims) > 60 else sims
        self.chart_mc.plot_equity(plot_sims, title=f"Monte Carlo Equity - {strategy_id}")
        self.chart_dd.plot_hist(mc_dds, title=f"Monte Carlo DD Distribution - {strategy_id}")
        self.chart_noise.plot_equity([noise_curve], title=f"Noise Test - {strategy_id}")
        self.chart_shuffle.plot_equity([shuffle_curve] + boot_curves[:10], title=f"Shuffle / Bootstrap - {strategy_id}")

        # JSON
        self.json_viewer.set_text(json.dumps(row.get("payload", {}), ensure_ascii=False, indent=2))

        self.status_var.set(
            f"Loaded strategy: {strategy_id} | trades={len(rets)} | mc_dd95={format_num(mc_dd_95, 2)}"
        )

    def _clear_strategy_views(self):
        self.detail_table.set_dataframe(pd.DataFrame())
        self.chart_mc.clear(message="No strategy selected")
        self.chart_dd.clear(message="No strategy selected")
        self.chart_noise.clear(message="No strategy selected")
        self.chart_shuffle.clear(message="No strategy selected")
        self.json_viewer.set_text("{}")
        self.status_var.set("No strategy selected")


# ============================================================
# MAIN
# ============================================================

def main():
    root = tk.Tk()
    root.title("Robustness Dashboard")
    root.geometry("1800x1000")
    root.configure(bg=BG_APP)

    panel = RobustnessDashboardPanel(root)
    panel.pack(fill="both", expand=True)

    root.mainloop()


if __name__ == "__main__":
    main()