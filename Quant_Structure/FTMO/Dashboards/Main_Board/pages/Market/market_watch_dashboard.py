# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Dashboards/Main_Board/pages/Market/market_watch_dashboard.py

Zweck:
- Corporate / institutional Market Moves Dashboard
- Datenquelle: lokale Parquet-OHLC-Dateien
- Standalone nutzbar
- Auch als eingebettetes Panel im Main Board nutzbar

Funktionen:
- Daily % = latest available intraday close vs previous D1 close
- Session Return % = latest available session price vs session open
- Session Range % = (session high - session low) / session open
- Sortierung nach stärkster absoluter Daily-Bewegung
- Session-Erkennung auf Europe/Berlin
- Bloomberg-/Terminal-artiger Look
- Bereinigte Haupttabelle ohne Contribution-Spalten

Erwartete Datenstruktur:
Quant_Structure/FTMO/Data_Center/Data/Ohcl/Raw/M1/<SYMBOL>.parquet   (optional)
Quant_Structure/FTMO/Data_Center/Data/Ohcl/Raw/M5/<SYMBOL>.parquet   (fallback)
Quant_Structure/FTMO/Data_Center/Data/Ohcl/Raw/M15/<SYMBOL>.parquet  (fallback)
Quant_Structure/FTMO/Data_Center/Data/Ohcl/Raw/D1/<SYMBOL>.parquet
Quant_Structure/FTMO/Data_Center/Data/Ohcl/Raw/summary.json

Standalone:
    python Quant_Structure/FTMO/Dashboards/Main_Board/pages/Market/market_watch_dashboard.py

Einbettung:
    from market_watch_dashboard import MarketWatchPanel
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any
from zoneinfo import ZoneInfo

import tkinter as tk
from tkinter import messagebox

import pandas as pd


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

OHLC_ROOT = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Ohcl"
    / "Raw"
)

SUMMARY_PATH = OHLC_ROOT / "summary.json"


# ============================================================
# CONFIG
# ============================================================

APP_TITLE = "Market Watch Dashboard"
AUTO_REFRESH_MS = 1500
AUTO_REFRESH_DEFAULT = True

DISPLAY_TZ = ZoneInfo("Europe/Berlin")

SYMBOLS: List[str] = [
    "AUDJPY", "AUDUSD", "EURGBP", "EURUSD", "GBPUSD", "GBPJPY",
    "NZDUSD", "US500.cash", "USDCAD", "USDCHF", "USDJPY", "USOIL.cash", "XAUUSD"
]

SESSION_WINDOWS = {
    "asia": (2, 6),
    "eu": (8, 12),
    "us": (14, 18),
}

# Deine OHLC-Struktur hat aktuell kein M1 im gezeigten Baum.
# Deshalb fallback-Reihenfolge für intraday:
INTRADAY_TF_PRIORITY = ["M1", "M5", "M15", "H1"]


# ============================================================
# STYLE
# ============================================================

BG_MAIN = "#0A0C10"
BG_PANEL = "#11151B"
BG_PANEL_2 = "#151A21"
BG_HEADER = "#171C24"
BG_TOPBAR = "#0E1319"
BG_ROW_1 = "#11151B"
BG_ROW_2 = "#151A21"
BG_ACTIVE = "#1E232B"
BG_CARD = "#11151B"
BG_BUTTON = "#1A2029"

FG_MAIN = "#E6EAF0"
FG_MUTED = "#9BA6B2"
FG_HEADER = "#C7D0DA"
FG_ACCENT = "#F5A623"
FG_POS = "#22C55E"
FG_NEG = "#EF4444"
FG_NEU = "#D1D5DB"
FG_WHITE = "#FFFFFF"

BORDER = "#232A34"

FONT_TITLE = ("Segoe UI", 18, "bold")
FONT_TOP = ("Segoe UI", 10, "bold")
FONT_SECTION = ("Segoe UI", 11, "bold")
FONT_HEADER = ("Segoe UI", 9, "bold")
FONT_CELL = ("Segoe UI", 9)
FONT_KPI_LABEL = ("Segoe UI", 9)
FONT_KPI_VALUE = ("Segoe UI", 12, "bold")
FONT_STATUS = ("Segoe UI", 9)


# ============================================================
# PATH / IO
# ============================================================

def safe_symbol_filename(symbol: str) -> str:
    return symbol.replace("/", "_") + ".parquet"


def parquet_path(tf: str, symbol: str) -> Path:
    return OHLC_ROOT / tf / safe_symbol_filename(symbol)


def parquet_exists(tf: str, symbol: str) -> bool:
    return parquet_path(tf, symbol).exists()


def find_best_intraday_tf(symbol: str) -> Optional[str]:
    for tf in INTRADAY_TF_PRIORITY:
        if parquet_exists(tf, symbol):
            return tf
    return None


def load_parquet_df(tf: str, symbol: str) -> pd.DataFrame:
    path = parquet_path(tf, symbol)
    if not path.exists():
        raise FileNotFoundError(f"Parquet nicht gefunden: {path}")

    df = pd.read_parquet(path)
    if df.empty:
        return df

    if "time" not in df.columns:
        raise ValueError(f"Spalte 'time' fehlt in {path}")

    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)

    for c in ["open", "high", "low", "close"]:
        if c not in df.columns:
            raise ValueError(f"Spalte '{c}' fehlt in {path}")
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


# ============================================================
# HELPERS
# ============================================================

def pct_change(current: float, ref: float) -> Optional[float]:
    if ref is None or ref == 0 or pd.isna(ref) or pd.isna(current):
        return None
    return ((current - ref) / ref) * 100.0


def fmt_pct(x: Optional[float]) -> str:
    if x is None or pd.isna(x):
        return ""
    return f"{float(x):+.3f}%"


def value_color(x: Optional[float]) -> str:
    if x is None or pd.isna(x):
        return FG_NEU
    if float(x) > 0:
        return FG_POS
    if float(x) < 0:
        return FG_NEG
    return FG_NEU


def range_color(x: Optional[float]) -> str:
    if x is None or pd.isna(x):
        return FG_NEU
    return FG_ACCENT


def get_display_now() -> datetime:
    return datetime.now(DISPLAY_TZ)


def get_current_session_name(now_local: datetime) -> Optional[str]:
    h = now_local.hour
    if 2 <= h < 6:
        return "asia"
    if 8 <= h < 12:
        return "eu"
    if 14 <= h < 18:
        return "us"
    return None


def build_local_session_datetime(now_local: datetime, hour: int) -> datetime:
    d = now_local.date()
    return datetime(
        d.year,
        d.month,
        d.day,
        hour,
        0,
        0,
        0,
        tzinfo=DISPLAY_TZ,
    )


def local_session_bounds_to_utc(now_local: datetime, start_hour: int, end_hour: int) -> tuple[datetime, datetime]:
    session_start_local = build_local_session_datetime(now_local, start_hour)
    session_end_local = build_local_session_datetime(now_local, end_hour)

    return (
        session_start_local.astimezone(timezone.utc),
        session_end_local.astimezone(timezone.utc),
    )


# ============================================================
# DATA ACCESS
# ============================================================

def get_latest_intraday_close(symbol: str) -> tuple[float, str]:
    tf = find_best_intraday_tf(symbol)
    if tf is None:
        raise RuntimeError(f"Keine Intraday-Daten gefunden für {symbol}. Erwartet einen von {INTRADAY_TF_PRIORITY}")

    df = load_parquet_df(tf, symbol)
    if df.empty:
        raise RuntimeError(f"Keine {tf} Daten: {symbol}")

    val = float(df.iloc[-1]["close"])
    if not math.isfinite(val):
        raise RuntimeError(f"Ungültiger letzter {tf} close: {symbol}")

    return val, tf


def get_previous_d1_close(symbol: str) -> float:
    """
    Ziel:
    - vorheriger abgeschlossener D1 close

    Logik:
    - wenn letzter D1-Bar heute ist -> nimm vorletzten close
    - sonst nimm letzten close
    """
    df = load_parquet_df("D1", symbol)
    if df.empty or len(df) == 0:
        raise RuntimeError(f"Keine D1 Daten: {symbol}")

    now_local = get_display_now()
    today_local = now_local.date()

    d = df.copy()
    d["time_local"] = d["time"].dt.tz_convert(DISPLAY_TZ)
    d["date_local"] = d["time_local"].dt.date

    if len(d) >= 2 and d.iloc[-1]["date_local"] == today_local:
        val = float(d.iloc[-2]["close"])
    else:
        val = float(d.iloc[-1]["close"])

    if not math.isfinite(val):
        raise RuntimeError(f"Ungültiger previous D1 close: {symbol}")
    return val


def get_intraday_df(symbol: str, dt_from_utc: datetime, dt_to_utc: datetime) -> tuple[pd.DataFrame, str]:
    tf = find_best_intraday_tf(symbol)
    if tf is None:
        return pd.DataFrame(), ""

    df = load_parquet_df(tf, symbol)
    if df.empty:
        return df, tf

    mask = (
        (df["time"] >= pd.Timestamp(dt_from_utc))
        & (df["time"] <= pd.Timestamp(dt_to_utc))
    )
    return df.loc[mask].sort_values("time").reset_index(drop=True), tf


def get_session_metrics(symbol: str, now_local: datetime, start_hour: int, end_hour: int) -> Dict[str, Optional[float]]:
    """
    return_pct:
        vor Sessionende = letzter verfügbarer Session-Preis vs session_open
        nach Sessionende = session_close vs session_open

    range_pct:
        (session_high - session_low) / session_open
    """
    session_start_utc, session_end_utc = local_session_bounds_to_utc(now_local, start_hour, end_hour)
    now_utc = now_local.astimezone(timezone.utc)

    if now_utc < session_start_utc:
        return {"return_pct": None, "range_pct": None}

    dt_from = session_start_utc - timedelta(minutes=15)
    dt_to = min(now_utc, session_end_utc) + timedelta(minutes=5)

    df, _tf = get_intraday_df(symbol, dt_from, dt_to)
    if df.empty:
        return {"return_pct": None, "range_pct": None}

    before_start = df[df["time"] < pd.Timestamp(session_start_utc)].copy()
    during = df[df["time"] >= pd.Timestamp(session_start_utc)].copy()

    if during.empty:
        return {"return_pct": None, "range_pct": None}

    if not before_start.empty:
        session_open = float(before_start.iloc[-1]["close"])
    else:
        session_open = float(during.iloc[0]["open"])

    if not math.isfinite(session_open) or session_open == 0:
        return {"return_pct": None, "range_pct": None}

    if now_utc < session_end_utc:
        active_df = during.copy()
        session_ref_price = float(active_df.iloc[-1]["close"])
    else:
        active_df = during[during["time"] < pd.Timestamp(session_end_utc)].copy()
        if active_df.empty:
            return {"return_pct": None, "range_pct": None}
        session_ref_price = float(active_df.iloc[-1]["close"])

    if not math.isfinite(session_ref_price):
        return {"return_pct": None, "range_pct": None}

    session_return_pct = pct_change(session_ref_price, session_open)

    session_high = float(active_df["high"].max()) if "high" in active_df.columns and not active_df.empty else None
    session_low = float(active_df["low"].min()) if "low" in active_df.columns and not active_df.empty else None

    if session_high is None or session_low is None or not math.isfinite(session_high) or not math.isfinite(session_low):
        session_range_pct = None
    else:
        high_pct = pct_change(session_high, session_open)
        low_pct = pct_change(session_low, session_open)
        if high_pct is not None and low_pct is not None:
            session_range_pct = high_pct - low_pct
        else:
            session_range_pct = None

    return {
        "return_pct": session_return_pct,
        "range_pct": session_range_pct,
    }


def get_symbol_row(symbol: str) -> Dict[str, Any]:
    now_local = get_display_now()
    latest_close, intraday_tf = get_latest_intraday_close(symbol)
    prev_close = get_previous_d1_close(symbol)

    daily_pct = pct_change(latest_close, prev_close)

    asia = get_session_metrics(symbol, now_local, *SESSION_WINDOWS["asia"])
    eu = get_session_metrics(symbol, now_local, *SESSION_WINDOWS["eu"])
    us = get_session_metrics(symbol, now_local, *SESSION_WINDOWS["us"])

    return {
        "symbol": symbol,
        "intraday_tf": intraday_tf,
        "latest_close": latest_close,
        "daily_pct": daily_pct,
        "asia_ret_pct": asia["return_pct"],
        "eu_ret_pct": eu["return_pct"],
        "us_ret_pct": us["return_pct"],
        "asia_rng_pct": asia["range_pct"],
        "eu_rng_pct": eu["range_pct"],
        "us_rng_pct": us["range_pct"],
    }


# ============================================================
# KPI CARD
# ============================================================

class KpiCard(tk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1, width=250, height=72)
        self.pack_propagate(False)

        tk.Label(
            self,
            text=title,
            font=FONT_KPI_LABEL,
            bg=BG_CARD,
            fg=FG_MUTED,
        ).pack(anchor="w", padx=10, pady=(8, 2))

        self.value_var = tk.StringVar(value="-")
        self.value_label = tk.Label(
            self,
            textvariable=self.value_var,
            font=FONT_KPI_VALUE,
            bg=BG_CARD,
            fg=FG_MAIN,
        )
        self.value_label.pack(anchor="w", padx=10)

    def set_value(self, value: str, color: Optional[str] = None):
        self.value_var.set(value)
        self.value_label.configure(fg=color or FG_MAIN)


# ============================================================
# PANEL
# ============================================================

class MarketWatchPanel(tk.Frame):
    def __init__(self, parent):
        super().__init__(parent, bg=BG_MAIN)

        self.running = True
        self.auto_refresh_enabled = tk.BooleanVar(value=AUTO_REFRESH_DEFAULT)
        self._refresh_job: Optional[str] = None

        self.headers = [
            "rank",
            "symbol",
            "daily_%",
            "asia_ret_%",
            "eu_ret_%",
            "us_ret_%",
            "asia_rng_%",
            "eu_rng_%",
            "us_rng_%",
            "tf",
        ]
        self.col_widths = [60, 180, 120, 120, 120, 120, 120, 120, 120, 80]

        self._build_ui()
        self._validate_and_init()
        self._schedule_refresh()

    # ========================================================
    # INIT
    # ========================================================

    def _validate_and_init(self) -> None:
        try:
            self._validate_data_source()
            self.status_var.set(f"PARQUET SOURCE OK | root={OHLC_ROOT}")
            self._refresh_market_data()
        except Exception as e:
            self.status_var.set(f"ERROR | {e}")
            self.topbar_info_var.set("DATA SOURCE ERROR")
            try:
                messagebox.showerror("Parquet Fehler", str(e))
            except Exception:
                pass

    def _validate_data_source(self) -> None:
        if not OHLC_ROOT.exists():
            raise RuntimeError(f"OHLC Root fehlt: {OHLC_ROOT}")

        if not (OHLC_ROOT / "D1").exists():
            raise RuntimeError(f"Fehlender OHLC Ordner: {OHLC_ROOT / 'D1'}")

        intraday_exists = any((OHLC_ROOT / tf).exists() for tf in INTRADAY_TF_PRIORITY)
        if not intraday_exists:
            raise RuntimeError(
                f"Kein Intraday-OHLC Ordner gefunden. Erwartet einen von: {INTRADAY_TF_PRIORITY}"
            )

    # ========================================================
    # UI
    # ========================================================

    def _build_ui(self) -> None:
        root = tk.Frame(self, bg=BG_MAIN)
        root.pack(fill="both", expand=True, padx=12, pady=12)

        topbar = tk.Frame(root, bg=BG_TOPBAR, height=52, highlightbackground=BORDER, highlightthickness=1)
        topbar.pack(fill="x", pady=(0, 12))
        topbar.pack_propagate(False)

        tk.Label(
            topbar,
            text="MARKET MOVES MONITOR",
            font=FONT_TITLE,
            bg=BG_TOPBAR,
            fg=FG_WHITE,
        ).pack(side="left", padx=14)

        self.topbar_info_var = tk.StringVar(value="")
        tk.Label(
            topbar,
            textvariable=self.topbar_info_var,
            font=FONT_TOP,
            bg=BG_TOPBAR,
            fg=FG_MUTED,
        ).pack(side="right", padx=14)

        kpi_row = tk.Frame(root, bg=BG_MAIN)
        kpi_row.pack(fill="x", pady=(0, 12))

        self.card_top_gainer = KpiCard(kpi_row, "Top Gainer")
        self.card_top_gainer.pack(side="left", fill="x", expand=True, padx=4)

        self.card_top_loser = KpiCard(kpi_row, "Top Loser")
        self.card_top_loser.pack(side="left", fill="x", expand=True, padx=4)

        self.card_session = KpiCard(kpi_row, "Active Session")
        self.card_session.pack(side="left", fill="x", expand=True, padx=4)

        self.card_symbols = KpiCard(kpi_row, "Symbols Online")
        self.card_symbols.pack(side="left", fill="x", expand=True, padx=4)

        control_row = tk.Frame(root, bg=BG_MAIN)
        control_row.pack(fill="x", pady=(0, 10))

        tk.Label(
            control_row,
            text="Cross-Asset Market Table",
            font=FONT_SECTION,
            bg=BG_MAIN,
            fg=FG_MAIN,
        ).pack(side="left")

        tk.Checkbutton(
            control_row,
            text="Auto Refresh",
            variable=self.auto_refresh_enabled,
            command=self._on_toggle_auto_refresh,
            bg=BG_MAIN,
            fg=FG_MAIN,
            activebackground=BG_MAIN,
            activeforeground=FG_MAIN,
            selectcolor=BG_PANEL_2,
            relief="flat",
        ).pack(side="right", padx=(8, 0))

        tk.Button(
            control_row,
            text="Refresh Now",
            command=self._refresh_market_data,
            bg=BG_BUTTON,
            fg=FG_MAIN,
            activebackground=BG_ACTIVE,
            activeforeground=FG_WHITE,
            relief="flat",
            padx=12,
            pady=6,
            highlightthickness=0,
            bd=0,
        ).pack(side="right")

        table_wrap = tk.Frame(root, bg=BG_PANEL, highlightbackground=BORDER, highlightthickness=1)
        table_wrap.pack(fill="both", expand=True)

        self.canvas = tk.Canvas(table_wrap, bg=BG_PANEL, highlightthickness=0)
        self.scrollbar_y = tk.Scrollbar(table_wrap, orient="vertical", command=self.canvas.yview)
        self.scrollbar_x = tk.Scrollbar(table_wrap, orient="horizontal", command=self.canvas.xview)
        self.table_frame = tk.Frame(self.canvas, bg=BG_PANEL)

        self.table_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )

        self.canvas.create_window((0, 0), window=self.table_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar_y.set, xscrollcommand=self.scrollbar_x.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar_y.pack(side="right", fill="y")
        self.scrollbar_x.pack(side="bottom", fill="x")

        footer = tk.Frame(root, bg=BG_TOPBAR, height=34, highlightbackground=BORDER, highlightthickness=1)
        footer.pack(fill="x", pady=(12, 0))
        footer.pack_propagate(False)

        self.status_var = tk.StringVar(value="DISCONNECTED")
        tk.Label(
            footer,
            textvariable=self.status_var,
            font=FONT_STATUS,
            bg=BG_TOPBAR,
            fg=FG_MUTED,
        ).pack(side="left", padx=10)

    # ========================================================
    # TABLE
    # ========================================================

    def _clear_table(self) -> None:
        for widget in self.table_frame.winfo_children():
            widget.destroy()

    def _header_bg(self, header_key: str, active_session: Optional[str]) -> str:
        if header_key.startswith("asia_") and active_session == "asia":
            return BG_ACTIVE
        if header_key.startswith("eu_") and active_session == "eu":
            return BG_ACTIVE
        if header_key.startswith("us_") and active_session == "us":
            return BG_ACTIVE
        return BG_HEADER

    def _draw_table(self, rows: List[Dict[str, Any]], active_session: Optional[str]) -> None:
        self._clear_table()

        for c, header in enumerate(self.headers):
            anchor = "w" if header in ("symbol", "tf") else "e"
            lbl = tk.Label(
                self.table_frame,
                text=header,
                font=FONT_HEADER,
                bg=self._header_bg(header, active_session),
                fg=FG_HEADER,
                width=max(8, self.col_widths[c] // 9),
                padx=8,
                pady=7,
                anchor=anchor,
                relief="flat",
            )
            lbl.grid(row=0, column=c, sticky="nsew", padx=1, pady=1)

        for i, row in enumerate(rows, start=1):
            row_bg = BG_ROW_1 if i % 2 == 1 else BG_ROW_2

            row_values = [
                str(row["rank"]),
                row["symbol"],
                fmt_pct(row["daily_pct"]),
                fmt_pct(row["asia_ret_pct"]),
                fmt_pct(row["eu_ret_pct"]),
                fmt_pct(row["us_ret_pct"]),
                fmt_pct(row["asia_rng_pct"]),
                fmt_pct(row["eu_rng_pct"]),
                fmt_pct(row["us_rng_pct"]),
                row.get("intraday_tf", ""),
            ]

            num_values = [
                None,
                None,
                row["daily_pct"],
                row["asia_ret_pct"],
                row["eu_ret_pct"],
                row["us_ret_pct"],
                row["asia_rng_pct"],
                row["eu_rng_pct"],
                row["us_rng_pct"],
                None,
            ]

            for c, text in enumerate(row_values):
                bg = row_bg
                header_key = self.headers[c]

                if header_key.startswith("asia_") and active_session == "asia":
                    bg = BG_ACTIVE
                elif header_key.startswith("eu_") and active_session == "eu":
                    bg = BG_ACTIVE
                elif header_key.startswith("us_") and active_session == "us":
                    bg = BG_ACTIVE

                if c == 0:
                    fg = FG_MUTED
                    anchor = "e"
                elif c == 1:
                    fg = FG_MAIN
                    anchor = "w"
                elif header_key == "tf":
                    fg = FG_ACCENT
                    anchor = "w"
                else:
                    if header_key.endswith("rng_%"):
                        fg = range_color(num_values[c])
                    else:
                        fg = value_color(num_values[c])
                    anchor = "e"

                lbl = tk.Label(
                    self.table_frame,
                    text=text,
                    font=FONT_CELL,
                    bg=bg,
                    fg=fg,
                    width=max(8, self.col_widths[c] // 9),
                    padx=8,
                    pady=5,
                    anchor=anchor,
                    relief="flat",
                )
                lbl.grid(row=i, column=c, sticky="nsew", padx=1, pady=1)

        for idx in range(len(self.headers)):
            self.table_frame.grid_columnconfigure(idx, weight=1)

    # ========================================================
    # KPI
    # ========================================================

    def _update_kpis(self, rows: List[Dict[str, Any]], current_session: Optional[str]) -> None:
        valid_rows = [r for r in rows if r.get("daily_pct") is not None]

        if valid_rows:
            top_gainer = max(valid_rows, key=lambda x: float(x["daily_pct"]))
            top_loser = min(valid_rows, key=lambda x: float(x["daily_pct"]))

            self.card_top_gainer.set_value(
                f"{top_gainer['symbol']}  {fmt_pct(top_gainer['daily_pct'])}",
                color=value_color(top_gainer["daily_pct"]),
            )
            self.card_top_loser.set_value(
                f"{top_loser['symbol']}  {fmt_pct(top_loser['daily_pct'])}",
                color=value_color(top_loser["daily_pct"]),
            )
        else:
            self.card_top_gainer.set_value("-")
            self.card_top_loser.set_value("-")

        self.card_session.set_value(current_session.upper() if current_session else "NONE", color=FG_ACCENT)
        self.card_symbols.set_value(str(len(valid_rows)), color=FG_MAIN)

    # ========================================================
    # REFRESH
    # ========================================================

    def _refresh_market_data(self) -> None:
        if not self.running:
            return

        rows: List[Dict[str, Any]] = []
        err_count = 0

        now_local = get_display_now()
        current_session = get_current_session_name(now_local)

        for symbol in SYMBOLS:
            try:
                row = get_symbol_row(symbol)
                rows.append(row)
            except Exception as e:
                err_count += 1
                print(f"[WARN] {symbol} failed: {e}")

        rows.sort(
            key=lambda x: abs(float(x["daily_pct"])) if x.get("daily_pct") is not None else -1.0,
            reverse=True,
        )

        ranked_rows = []
        for idx, row in enumerate(rows, start=1):
            r = dict(row)
            r["rank"] = idx
            ranked_rows.append(r)

        self._draw_table(ranked_rows, current_session)
        self._update_kpis(ranked_rows, current_session)

        self.topbar_info_var.set(
            f"PARQUET   |   {now_local.strftime('%Y-%m-%d %H:%M:%S')}   |   {current_session.upper() if current_session else 'NONE'}   |   {OHLC_ROOT}"
        )

        self.status_var.set(
            f"PARQUET SOURCE | symbols={len(rows)} | err={err_count} | refresh={AUTO_REFRESH_MS}ms"
        )

    def _on_toggle_auto_refresh(self):
        self._schedule_refresh()

    def _schedule_refresh(self):
        if self._refresh_job is not None:
            try:
                self.after_cancel(self._refresh_job)
            except Exception:
                pass
            self._refresh_job = None

        if self.auto_refresh_enabled.get() and self.running:
            self._refresh_job = self.after(AUTO_REFRESH_MS, self._auto_refresh_tick)

    def _auto_refresh_tick(self):
        try:
            if self.running and self.auto_refresh_enabled.get():
                self._refresh_market_data()
        finally:
            self._schedule_refresh()

    def destroy(self):
        self.running = False
        if self._refresh_job is not None:
            try:
                self.after_cancel(self._refresh_job)
            except Exception:
                pass
            self._refresh_job = None
        super().destroy()


# ============================================================
# STANDALONE APP
# ============================================================

class MarketWatchDashboard(tk.Tk):
    def __init__(self) -> None:
        super().__init__()

        self.title(APP_TITLE)
        self.geometry("1420x860")
        self.minsize(1100, 650)
        self.configure(bg=BG_MAIN)

        self.panel = MarketWatchPanel(self)
        self.panel.pack(fill="both", expand=True)

        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _on_close(self):
        try:
            self.panel.destroy()
        except Exception:
            pass
        self.destroy()


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    app = MarketWatchDashboard()
    app.mainloop()


if __name__ == "__main__":
    main()