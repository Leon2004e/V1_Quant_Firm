# -*- coding: utf-8 -*-
"""
Core/Dashboard.py

Einfaches Personal OS Dashboard für den aktuellen Projektstand.

Unterstützt aktuell:
- Health/Sleep_Tracking/Sleep_Data.csv
- Health/Gym_Body_Tracking/Weight_Data.csv
- Performance/Work_Tracker/Work_Data.csv
- optional: Core/Weekly_Control/Weekly_Review_Data.csv

Start:
    python Core/Dashboard.py
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import tkinter as tk
from tkinter import ttk

import pandas as pd
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

PATHS = {
    "weekly": BASE_DIR / "Core" / "Weekly_Control" / "Weekly_Review_Data.csv",
    "sleep": BASE_DIR / "Health" / "Sleep_Tracking" / "Sleep_Data.csv",
    "work": BASE_DIR / "Performance" / "Work_Tracker" / "Work_Data.csv",
    "weight": BASE_DIR / "Health" / "Gym_Body_Tracking" / "Weight_Data.csv",
}


# ============================================================
# STYLE
# ============================================================

BG_MAIN = "#0A0C10"
BG_PANEL = "#11151B"
BG_PANEL_2 = "#151A21"
BG_HEADER = "#171C24"
BG_CARD = "#11151B"
BG_BUTTON = "#1A2029"

FG_MAIN = "#E6EAF0"
FG_MUTED = "#9BA6B2"

BORDER = "#232A34"

FONT_TITLE = ("Segoe UI", 17, "bold")
FONT_SECTION = ("Segoe UI", 11, "bold")
FONT_LABEL = ("Segoe UI", 9)
FONT_VALUE = ("Segoe UI", 12, "bold")
FONT_TEXT = ("Segoe UI", 9)


# ============================================================
# HELPERS
# ============================================================

def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def parse_date_column(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    candidates = ["Date", "Datum", "Week", "date", "datum", "week"]

    for col in candidates:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce", dayfirst=True)
            if col != "Date":
                out = out.rename(columns={col: "Date"})
            break

    return out


def fmt_number(x, digits: int = 2) -> str:
    try:
        if x is None or pd.isna(x):
            return "-"
        return f"{float(x):.{digits}f}"
    except Exception:
        return "-"


def fmt_hours(x, digits: int = 2) -> str:
    try:
        if x is None or pd.isna(x):
            return "-"
        return f"{float(x):.{digits}f} h"
    except Exception:
        return "-"


def fmt_kg(x, digits: int = 2) -> str:
    try:
        if x is None or pd.isna(x):
            return "-"
        return f"{float(x):.{digits}f} kg"
    except Exception:
        return "-"


def safe_mean(series: pd.Series) -> Optional[float]:
    if series is None or series.empty:
        return None
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.mean())


def latest_value(series: pd.Series) -> Optional[float]:
    if series is None or series.empty:
        return None
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.iloc[-1])


def sleep_duration_to_hours(val) -> Optional[float]:
    """
    Konvertiert Schlafdauer:
    - '8:30' -> 8.5
    - '08:30' -> 8.5
    - '8.5' -> 8.5
    - leer -> None
    """
    if pd.isna(val):
        return None

    text = str(val).strip()
    if not text:
        return None

    if ":" in text:
        parts = text.split(":")
        if len(parts) == 2:
            try:
                h = int(parts[0])
                m = int(parts[1])
                return h + m / 60.0
            except Exception:
                return None

    try:
        return float(text.replace(",", "."))
    except Exception:
        return None


def sleep_quality_to_score(val) -> Optional[float]:
    if pd.isna(val):
        return None

    text = str(val).strip().lower()
    if not text:
        return None

    mapping = {
        "sehr schlecht": 1,
        "schlecht": 2,
        "okay": 3,
        "ok": 3,
        "mittel": 3,
        "gut": 4,
        "sehr gut": 5,
    }
    return mapping.get(text)


def focus_to_score(val) -> Optional[float]:
    if pd.isna(val):
        return None

    text = str(val).strip()
    if not text:
        return None

    try:
        return float(text.replace(",", "."))
    except Exception:
        pass

    mapping = {
        "sehr schlecht": 1,
        "schlecht": 2,
        "mittel": 3,
        "okay": 3,
        "ok": 3,
        "gut": 4,
        "sehr gut": 5,
    }
    return mapping.get(text.lower())


def setup_treeview_from_df(tree: ttk.Treeview, df: pd.DataFrame, max_rows: int = 100):
    tree.delete(*tree.get_children())

    if tree["columns"]:
        tree["columns"] = ()

    if df.empty:
        tree["columns"] = ("info",)
        tree.heading("info", text="Info")
        tree.column("info", width=240, anchor="w")
        tree.insert("", "end", values=("No data",))
        return

    cols = list(df.columns)
    tree["columns"] = cols

    for col in cols:
        tree.heading(col, text=col)
        tree.column(col, width=130, anchor="w")

    show_df = df.head(max_rows).copy()

    for _, row in show_df.iterrows():
        vals = ["" if pd.isna(v) else str(v) for v in row.tolist()]
        tree.insert("", "end", values=vals)


# ============================================================
# UI COMPONENTS
# ============================================================

class KpiCard(tk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        self.configure(height=78)
        self.pack_propagate(False)

        tk.Label(
            self,
            text=title,
            font=FONT_LABEL,
            bg=BG_CARD,
            fg=FG_MUTED,
        ).pack(anchor="w", padx=10, pady=(8, 2))

        self.value_var = tk.StringVar(value="-")
        self.value_label = tk.Label(
            self,
            textvariable=self.value_var,
            font=FONT_VALUE,
            bg=BG_CARD,
            fg=FG_MAIN,
        )
        self.value_label.pack(anchor="w", padx=10)

    def set_value(self, value: str):
        self.value_var.set(value)


# ============================================================
# DASHBOARD
# ============================================================

class PersonalOSDashboard(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("Personal OS Dashboard")
        self.geometry("1680x980")
        self.minsize(1300, 820)
        self.configure(bg=BG_MAIN)

        self.sleep_df = pd.DataFrame()
        self.work_df = pd.DataFrame()
        self.weekly_df = pd.DataFrame()
        self.weight_df = pd.DataFrame()

        self.sleep_fig = None
        self.sleep_ax = None
        self.sleep_canvas = None

        self.work_fig = None
        self.work_ax = None
        self.work_canvas = None

        self.weight_fig = None
        self.weight_ax = None
        self.weight_canvas = None

        self.weekly_fig = None
        self.weekly_ax = None
        self.weekly_canvas = None

        self.days_filter_var = tk.IntVar(value=30)

        self._build_ui()
        self._load_and_render()

    # --------------------------------------------------------
    # UI
    # --------------------------------------------------------
    def _build_ui(self):
        root = tk.Frame(self, bg=BG_MAIN)
        root.pack(fill="both", expand=True, padx=12, pady=12)

        header = tk.Frame(root, bg=BG_HEADER, height=54, highlightbackground=BORDER, highlightthickness=1)
        header.pack(fill="x", pady=(0, 12))
        header.pack_propagate(False)

        tk.Label(
            header,
            text="PERSONAL OS DASHBOARD",
            font=FONT_TITLE,
            bg=BG_HEADER,
            fg=FG_MAIN,
        ).pack(side="left", padx=14)

        tk.Label(
            header,
            text=str(BASE_DIR),
            font=FONT_TEXT,
            bg=BG_HEADER,
            fg=FG_MUTED,
        ).pack(side="right", padx=14)

        controls = tk.Frame(root, bg=BG_MAIN)
        controls.pack(fill="x", pady=(0, 12))

        tk.Label(
            controls,
            text="Last N days",
            bg=BG_MAIN,
            fg=FG_MUTED,
            font=FONT_LABEL,
        ).pack(side="left", padx=(0, 8))

        self.days_combo = ttk.Combobox(
            controls,
            state="readonly",
            width=8,
            values=[7, 14, 30, 60, 90, 180],
            textvariable=self.days_filter_var,
        )
        self.days_combo.pack(side="left", padx=(0, 12))
        self.days_combo.bind("<<ComboboxSelected>>", lambda e: self._load_and_render())

        tk.Button(
            controls,
            text="Refresh",
            command=self._load_and_render,
            bg=BG_BUTTON,
            fg=FG_MAIN,
            activebackground=BG_PANEL_2,
            activeforeground=FG_MAIN,
            relief="flat",
            padx=12,
            pady=6,
            bd=0,
        ).pack(side="left")

        kpi_row = tk.Frame(root, bg=BG_MAIN)
        kpi_row.pack(fill="x", pady=(0, 12))

        self.card_sleep = KpiCard(kpi_row, "Avg Sleep")
        self.card_sleep.pack(side="left", fill="x", expand=True, padx=4)

        self.card_quality = KpiCard(kpi_row, "Sleep Quality")
        self.card_quality.pack(side="left", fill="x", expand=True, padx=4)

        self.card_deep = KpiCard(kpi_row, "Avg Deep Work")
        self.card_deep.pack(side="left", fill="x", expand=True, padx=4)

        self.card_sessions = KpiCard(kpi_row, "Deep Work Sessions")
        self.card_sessions.pack(side="left", fill="x", expand=True, padx=4)

        self.card_weight = KpiCard(kpi_row, "Avg Weight")
        self.card_weight.pack(side="left", fill="x", expand=True, padx=4)

        main = tk.PanedWindow(root, orient="horizontal", bg=BG_MAIN, sashwidth=6)
        main.pack(fill="both", expand=True)

        left = tk.Frame(main, bg=BG_PANEL, highlightbackground=BORDER, highlightthickness=1)
        right = tk.Frame(main, bg=BG_PANEL, highlightbackground=BORDER, highlightthickness=1)

        main.add(left, minsize=380)
        main.add(right, minsize=980)

        self._build_left_panel(left)
        self._build_right_panel(right)

    def _build_left_panel(self, parent: tk.Frame):
        summary_box = tk.Frame(parent, bg=BG_PANEL_2, highlightbackground=BORDER, highlightthickness=1)
        summary_box.pack(fill="x", padx=10, pady=10)

        tk.Label(
            summary_box,
            text="Quick Summary",
            font=FONT_SECTION,
            bg=BG_PANEL_2,
            fg=FG_MAIN,
        ).pack(anchor="w", padx=10, pady=(8, 6))

        self.summary_text = tk.Text(
            summary_box,
            height=13,
            wrap="word",
            bg=BG_PANEL_2,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            font=FONT_TEXT,
        )
        self.summary_text.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        snapshot_box = tk.Frame(parent, bg=BG_PANEL_2, highlightbackground=BORDER, highlightthickness=1)
        snapshot_box.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        tk.Label(
            snapshot_box,
            text="Current Snapshot",
            font=FONT_SECTION,
            bg=BG_PANEL_2,
            fg=FG_MAIN,
        ).pack(anchor="w", padx=10, pady=(8, 6))

        cols = ("metric", "value")
        self.snapshot_tree = ttk.Treeview(snapshot_box, columns=cols, show="headings", height=14)
        self.snapshot_tree.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.snapshot_tree.heading("metric", text="Metric")
        self.snapshot_tree.heading("value", text="Value")
        self.snapshot_tree.column("metric", width=180, anchor="w")
        self.snapshot_tree.column("value", width=130, anchor="w")

    def _build_right_panel(self, parent: tk.Frame):
        notebook = ttk.Notebook(parent)
        notebook.pack(fill="both", expand=True, padx=10, pady=10)

        self.tab_overview = tk.Frame(notebook, bg=BG_PANEL)
        self.tab_raw = tk.Frame(notebook, bg=BG_PANEL)

        notebook.add(self.tab_overview, text="Overview")
        notebook.add(self.tab_raw, text="Raw Data")

        self._build_overview_tab(self.tab_overview)
        self._build_raw_tab(self.tab_raw)

    def _build_overview_tab(self, parent: tk.Frame):
        chart_row_1 = tk.Frame(parent, bg=BG_PANEL)
        chart_row_1.pack(fill="both", expand=True)

        sleep_box = tk.Frame(chart_row_1, bg=BG_PANEL_2, highlightbackground=BORDER, highlightthickness=1)
        sleep_box.pack(side="left", fill="both", expand=True, padx=(0, 5), pady=(0, 10))

        work_box = tk.Frame(chart_row_1, bg=BG_PANEL_2, highlightbackground=BORDER, highlightthickness=1)
        work_box.pack(side="left", fill="both", expand=True, padx=(5, 0), pady=(0, 10))

        tk.Label(sleep_box, text="Sleep Trend", font=FONT_SECTION, bg=BG_PANEL_2, fg=FG_MAIN).pack(anchor="w", padx=10, pady=(8, 4))
        tk.Label(work_box, text="Deep Work Trend", font=FONT_SECTION, bg=BG_PANEL_2, fg=FG_MAIN).pack(anchor="w", padx=10, pady=(8, 4))

        self.sleep_fig = Figure(figsize=(6, 3.0), dpi=100, facecolor=BG_PANEL_2)
        self.sleep_ax = self.sleep_fig.add_subplot(111)
        self.sleep_canvas = FigureCanvasTkAgg(self.sleep_fig, master=sleep_box)
        self.sleep_canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.work_fig = Figure(figsize=(6, 3.0), dpi=100, facecolor=BG_PANEL_2)
        self.work_ax = self.work_fig.add_subplot(111)
        self.work_canvas = FigureCanvasTkAgg(self.work_fig, master=work_box)
        self.work_canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=(0, 10))

        chart_row_2 = tk.Frame(parent, bg=BG_PANEL)
        chart_row_2.pack(fill="both", expand=True)

        weight_box = tk.Frame(chart_row_2, bg=BG_PANEL_2, highlightbackground=BORDER, highlightthickness=1)
        weight_box.pack(side="left", fill="both", expand=True, padx=(0, 5), pady=(0, 10))

        weekly_box = tk.Frame(chart_row_2, bg=BG_PANEL_2, highlightbackground=BORDER, highlightthickness=1)
        weekly_box.pack(side="left", fill="both", expand=True, padx=(5, 0), pady=(0, 10))

        tk.Label(weight_box, text="Weight Trend", font=FONT_SECTION, bg=BG_PANEL_2, fg=FG_MAIN).pack(anchor="w", padx=10, pady=(8, 4))
        tk.Label(weekly_box, text="Weekly Control", font=FONT_SECTION, bg=BG_PANEL_2, fg=FG_MAIN).pack(anchor="w", padx=10, pady=(8, 4))

        self.weight_fig = Figure(figsize=(6, 3.0), dpi=100, facecolor=BG_PANEL_2)
        self.weight_ax = self.weight_fig.add_subplot(111)
        self.weight_canvas = FigureCanvasTkAgg(self.weight_fig, master=weight_box)
        self.weight_canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.weekly_fig = Figure(figsize=(6, 3.0), dpi=100, facecolor=BG_PANEL_2)
        self.weekly_ax = self.weekly_fig.add_subplot(111)
        self.weekly_canvas = FigureCanvasTkAgg(self.weekly_fig, master=weekly_box)
        self.weekly_canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=(0, 10))

    def _build_raw_tab(self, parent: tk.Frame):
        shell = tk.Frame(parent, bg=BG_PANEL)
        shell.pack(fill="both", expand=True, padx=10, pady=10)

        top = tk.PanedWindow(shell, orient="horizontal", bg=BG_PANEL, sashwidth=6)
        top.pack(fill="both", expand=True)

        sleep_box = tk.Frame(top, bg=BG_PANEL_2, highlightbackground=BORDER, highlightthickness=1)
        work_box = tk.Frame(top, bg=BG_PANEL_2, highlightbackground=BORDER, highlightthickness=1)
        weight_box = tk.Frame(top, bg=BG_PANEL_2, highlightbackground=BORDER, highlightthickness=1)
        weekly_box = tk.Frame(top, bg=BG_PANEL_2, highlightbackground=BORDER, highlightthickness=1)

        top.add(sleep_box, minsize=250)
        top.add(work_box, minsize=250)
        top.add(weight_box, minsize=250)
        top.add(weekly_box, minsize=250)

        tk.Label(sleep_box, text="Sleep Data", font=FONT_SECTION, bg=BG_PANEL_2, fg=FG_MAIN).pack(anchor="w", padx=10, pady=(8, 6))
        tk.Label(work_box, text="Work Data", font=FONT_SECTION, bg=BG_PANEL_2, fg=FG_MAIN).pack(anchor="w", padx=10, pady=(8, 6))
        tk.Label(weight_box, text="Weight Data", font=FONT_SECTION, bg=BG_PANEL_2, fg=FG_MAIN).pack(anchor="w", padx=10, pady=(8, 6))
        tk.Label(weekly_box, text="Weekly Review Data", font=FONT_SECTION, bg=BG_PANEL_2, fg=FG_MAIN).pack(anchor="w", padx=10, pady=(8, 6))

        self.sleep_raw = ttk.Treeview(sleep_box, show="headings", height=18)
        self.sleep_raw.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.work_raw = ttk.Treeview(work_box, show="headings", height=18)
        self.work_raw.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.weight_raw = ttk.Treeview(weight_box, show="headings", height=18)
        self.weight_raw.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.weekly_raw = ttk.Treeview(weekly_box, show="headings", height=18)
        self.weekly_raw.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    # --------------------------------------------------------
    # DATA PREP
    # --------------------------------------------------------
    def _load_data(self):
        self.sleep_df = parse_date_column(normalize_columns(safe_read_csv(PATHS["sleep"])))
        self.work_df = parse_date_column(normalize_columns(safe_read_csv(PATHS["work"])))
        self.weekly_df = parse_date_column(normalize_columns(safe_read_csv(PATHS["weekly"])))
        self.weight_df = parse_date_column(normalize_columns(safe_read_csv(PATHS["weight"])))

        if "Schlafdauer" in self.sleep_df.columns:
            self.sleep_df["Sleep_Hours_Num"] = self.sleep_df["Schlafdauer"].apply(sleep_duration_to_hours)
        else:
            self.sleep_df["Sleep_Hours_Num"] = pd.NA

        if "Schlafqualität" in self.sleep_df.columns:
            self.sleep_df["Sleep_Quality_Score"] = self.sleep_df["Schlafqualität"].apply(sleep_quality_to_score)
        else:
            self.sleep_df["Sleep_Quality_Score"] = pd.NA

        if "Deep Work Hours" in self.work_df.columns:
            self.work_df["Deep_Work_Hours_Num"] = pd.to_numeric(self.work_df["Deep Work Hours"], errors="coerce")
        else:
            self.work_df["Deep_Work_Hours_Num"] = pd.NA

        if "Deep Work Sessions" in self.work_df.columns:
            self.work_df["Deep_Work_Sessions_Num"] = pd.to_numeric(self.work_df["Deep Work Sessions"], errors="coerce")
        else:
            self.work_df["Deep_Work_Sessions_Num"] = pd.NA

        if "Deep Work Fokus" in self.work_df.columns:
            self.work_df["Deep_Work_Focus_Score"] = self.work_df["Deep Work Fokus"].apply(focus_to_score)
        else:
            self.work_df["Deep_Work_Focus_Score"] = pd.NA

        if "Weight" in self.weight_df.columns:
            self.weight_df["Weight_Num"] = pd.to_numeric(self.weight_df["Weight"], errors="coerce")
        else:
            self.weight_df["Weight_Num"] = pd.NA

        days = int(self.days_filter_var.get())

        if "Date" in self.sleep_df.columns:
            self.sleep_df = self.sleep_df[self.sleep_df["Date"] >= pd.Timestamp.today().normalize() - pd.Timedelta(days=days)]

        if "Date" in self.work_df.columns:
            self.work_df = self.work_df[self.work_df["Date"] >= pd.Timestamp.today().normalize() - pd.Timedelta(days=days)]

        if "Date" in self.weight_df.columns:
            self.weight_df = self.weight_df[self.weight_df["Date"] >= pd.Timestamp.today().normalize() - pd.Timedelta(days=days)]

        if "Date" in self.weekly_df.columns:
            self.weekly_df = self.weekly_df[self.weekly_df["Date"] >= pd.Timestamp.today().normalize() - pd.Timedelta(days=max(days, 60))]

    # --------------------------------------------------------
    # RENDER
    # --------------------------------------------------------
    def _load_and_render(self):
        self._load_data()
        self._render_kpis()
        self._render_summary()
        self._render_snapshot()
        self._render_sleep_chart()
        self._render_work_chart()
        self._render_weight_chart()
        self._render_weekly_chart()
        self._render_raw_tables()

    def _render_kpis(self):
        avg_sleep = safe_mean(self.sleep_df["Sleep_Hours_Num"]) if "Sleep_Hours_Num" in self.sleep_df.columns else None
        avg_quality = safe_mean(self.sleep_df["Sleep_Quality_Score"]) if "Sleep_Quality_Score" in self.sleep_df.columns else None
        avg_deep = safe_mean(self.work_df["Deep_Work_Hours_Num"]) if "Deep_Work_Hours_Num" in self.work_df.columns else None
        avg_sessions = safe_mean(self.work_df["Deep_Work_Sessions_Num"]) if "Deep_Work_Sessions_Num" in self.work_df.columns else None
        avg_weight = safe_mean(self.weight_df["Weight_Num"]) if "Weight_Num" in self.weight_df.columns else None

        self.card_sleep.set_value(fmt_hours(avg_sleep, 2))
        self.card_quality.set_value(fmt_number(avg_quality, 2))
        self.card_deep.set_value(fmt_hours(avg_deep, 2))
        self.card_sessions.set_value(fmt_number(avg_sessions, 2))
        self.card_weight.set_value(fmt_kg(avg_weight, 2))

    def _render_summary(self):
        avg_sleep = safe_mean(self.sleep_df["Sleep_Hours_Num"]) if "Sleep_Hours_Num" in self.sleep_df.columns else None
        avg_quality = safe_mean(self.sleep_df["Sleep_Quality_Score"]) if "Sleep_Quality_Score" in self.sleep_df.columns else None
        avg_deep = safe_mean(self.work_df["Deep_Work_Hours_Num"]) if "Deep_Work_Hours_Num" in self.work_df.columns else None
        avg_sessions = safe_mean(self.work_df["Deep_Work_Sessions_Num"]) if "Deep_Work_Sessions_Num" in self.work_df.columns else None
        avg_focus = safe_mean(self.work_df["Deep_Work_Focus_Score"]) if "Deep_Work_Focus_Score" in self.work_df.columns else None
        avg_weight = safe_mean(self.weight_df["Weight_Num"]) if "Weight_Num" in self.weight_df.columns else None

        lines = [
            f"Durchschnittlicher Schlaf: {fmt_hours(avg_sleep, 2)}",
            f"Durchschnittliche Schlafqualität: {fmt_number(avg_quality, 2)} / 5",
            f"Durchschnittliche Deep-Work-Zeit: {fmt_hours(avg_deep, 2)}",
            f"Durchschnittliche Deep-Work-Sessions: {fmt_number(avg_sessions, 2)}",
            f"Durchschnittlicher Fokus: {fmt_number(avg_focus, 2)} / 5",
            f"Durchschnittliches Gewicht: {fmt_kg(avg_weight, 2)}",
            "",
            "Hinweis:",
            "Das Dashboard ist auf deinen aktuellen Projektstand ausgelegt.",
            "Leere Felder werden toleriert.",
        ]

        self.summary_text.delete("1.0", "end")
        self.summary_text.insert("1.0", "\n".join(lines))

    def _render_snapshot(self):
        self.snapshot_tree.delete(*self.snapshot_tree.get_children())

        latest_sleep = latest_value(self.sleep_df.sort_values("Date")["Sleep_Hours_Num"]) if not self.sleep_df.empty else None
        latest_quality = latest_value(self.sleep_df.sort_values("Date")["Sleep_Quality_Score"]) if not self.sleep_df.empty else None
        latest_deep = latest_value(self.work_df.sort_values("Date")["Deep_Work_Hours_Num"]) if not self.work_df.empty else None
        latest_sessions = latest_value(self.work_df.sort_values("Date")["Deep_Work_Sessions_Num"]) if not self.work_df.empty else None
        latest_focus = latest_value(self.work_df.sort_values("Date")["Deep_Work_Focus_Score"]) if not self.work_df.empty else None
        latest_weight = latest_value(self.weight_df.sort_values("Date")["Weight_Num"]) if not self.weight_df.empty else None

        rows = [
            ("Latest Sleep", fmt_hours(latest_sleep, 2)),
            ("Latest Sleep Quality", fmt_number(latest_quality, 2)),
            ("Latest Deep Work Hours", fmt_hours(latest_deep, 2)),
            ("Latest Deep Work Sessions", fmt_number(latest_sessions, 2)),
            ("Latest Deep Work Focus", fmt_number(latest_focus, 2)),
            ("Latest Weight", fmt_kg(latest_weight, 2)),
        ]

        for metric, value in rows:
            self.snapshot_tree.insert("", "end", values=(metric, value))

    def _style_axis(self, ax):
        ax.set_facecolor(BG_PANEL_2)
        ax.tick_params(colors=FG_MUTED, labelsize=8)
        for spine in ax.spines.values():
            spine.set_color(BORDER)
        ax.grid(True, alpha=0.15)
        ax.title.set_color(FG_MAIN)
        ax.xaxis.label.set_color(FG_MUTED)
        ax.yaxis.label.set_color(FG_MUTED)

    def _render_sleep_chart(self):
        self.sleep_ax.clear()
        self._style_axis(self.sleep_ax)

        if not self.sleep_df.empty and "Date" in self.sleep_df.columns:
            chart_df = self.sleep_df[["Date", "Sleep_Hours_Num"]].dropna().sort_values("Date")
            if not chart_df.empty:
                self.sleep_ax.plot(chart_df["Date"], chart_df["Sleep_Hours_Num"], linewidth=2.0)
                self.sleep_ax.set_title("Sleep Hours")
                self.sleep_ax.set_ylabel("Hours")
            else:
                self.sleep_ax.text(0.5, 0.5, "Noch keine nutzbaren Schlafdaten", color=FG_MUTED, ha="center", va="center", transform=self.sleep_ax.transAxes)
        else:
            self.sleep_ax.text(0.5, 0.5, "Keine Sleep_Data.csv gefunden", color=FG_MUTED, ha="center", va="center", transform=self.sleep_ax.transAxes)

        self.sleep_fig.tight_layout()
        self.sleep_canvas.draw_idle()

    def _render_work_chart(self):
        self.work_ax.clear()
        self._style_axis(self.work_ax)

        if not self.work_df.empty and "Date" in self.work_df.columns:
            chart_df = self.work_df[["Date", "Deep_Work_Hours_Num", "Deep_Work_Sessions_Num"]].dropna(how="all").sort_values("Date")
            if not chart_df.empty:
                if chart_df["Deep_Work_Hours_Num"].notna().any():
                    self.work_ax.plot(chart_df["Date"], chart_df["Deep_Work_Hours_Num"], linewidth=2.0, label="Deep Work Hours")
                if chart_df["Deep_Work_Sessions_Num"].notna().any():
                    self.work_ax.plot(chart_df["Date"], chart_df["Deep_Work_Sessions_Num"], linewidth=2.0, label="Sessions")

                self.work_ax.set_title("Deep Work Trend")
                self.work_ax.set_ylabel("Value")
                self.work_ax.legend(facecolor=BG_PANEL_2, edgecolor=BORDER, labelcolor=FG_MAIN)
            else:
                self.work_ax.text(0.5, 0.5, "Noch keine nutzbaren Work-Daten", color=FG_MUTED, ha="center", va="center", transform=self.work_ax.transAxes)
        else:
            self.work_ax.text(0.5, 0.5, "Keine Work_Data.csv gefunden", color=FG_MUTED, ha="center", va="center", transform=self.work_ax.transAxes)

        self.work_fig.tight_layout()
        self.work_canvas.draw_idle()

    def _render_weight_chart(self):
        self.weight_ax.clear()
        self._style_axis(self.weight_ax)

        if not self.weight_df.empty and "Date" in self.weight_df.columns:
            chart_df = self.weight_df[["Date", "Weight_Num"]].dropna().sort_values("Date")
            if not chart_df.empty:
                self.weight_ax.plot(chart_df["Date"], chart_df["Weight_Num"], linewidth=2.0)
                self.weight_ax.set_title("Weight Trend")
                self.weight_ax.set_ylabel("kg")
            else:
                self.weight_ax.text(0.5, 0.5, "Noch keine nutzbaren Weight-Daten", color=FG_MUTED, ha="center", va="center", transform=self.weight_ax.transAxes)
        else:
            self.weight_ax.text(0.5, 0.5, "Keine Weight_Data.csv gefunden", color=FG_MUTED, ha="center", va="center", transform=self.weight_ax.transAxes)

        self.weight_fig.tight_layout()
        self.weight_canvas.draw_idle()

    def _render_weekly_chart(self):
        self.weekly_ax.clear()
        self._style_axis(self.weekly_ax)

        if self.weekly_df.empty or "Date" not in self.weekly_df.columns:
            self.weekly_ax.text(0.5, 0.5, "Weekly Review noch nicht vorhanden", color=FG_MUTED, ha="center", va="center", transform=self.weekly_ax.transAxes)
            self.weekly_fig.tight_layout()
            self.weekly_canvas.draw_idle()
            return

        possible_cols = [c for c in ["Score_total", "Energie_Score", "Fokus_Score"] if c in self.weekly_df.columns]
        if not possible_cols:
            self.weekly_ax.text(0.5, 0.5, "Weekly Review hat noch keine nutzbaren Spalten", color=FG_MUTED, ha="center", va="center", transform=self.weekly_ax.transAxes)
            self.weekly_fig.tight_layout()
            self.weekly_canvas.draw_idle()
            return

        chart_df = self.weekly_df[["Date"] + possible_cols].dropna(how="all").sort_values("Date")
        if chart_df.empty:
            self.weekly_ax.text(0.5, 0.5, "Weekly Review hat noch keine Daten", color=FG_MUTED, ha="center", va="center", transform=self.weekly_ax.transAxes)
        else:
            for col in possible_cols:
                self.weekly_ax.plot(chart_df["Date"], chart_df[col], linewidth=2.0, label=col)

            self.weekly_ax.set_title("Weekly Control")
            self.weekly_ax.set_ylabel("Score")
            self.weekly_ax.legend(facecolor=BG_PANEL_2, edgecolor=BORDER, labelcolor=FG_MAIN)

        self.weekly_fig.tight_layout()
        self.weekly_canvas.draw_idle()

    def _render_raw_tables(self):
        setup_treeview_from_df(self.sleep_raw, self.sleep_df, max_rows=120)
        setup_treeview_from_df(self.work_raw, self.work_df, max_rows=120)
        setup_treeview_from_df(self.weight_raw, self.weight_df, max_rows=120)
        setup_treeview_from_df(self.weekly_raw, self.weekly_df, max_rows=120)


# ============================================================
# MAIN
# ============================================================

def main():
    app = PersonalOSDashboard()
    app.mainloop()


if __name__ == "__main__":
    main()