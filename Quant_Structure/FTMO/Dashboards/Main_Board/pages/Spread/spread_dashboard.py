# -*- coding: utf-8 -*-
"""
3.Control_Panel/Spread_Dashboard/spread_dashboard.py

Zweck:
- Live Spread Dashboard auf Basis der vom Spread-Loop geschriebenen SQLite-DBs
- Liest:
    <PROJECT_ROOT>/1.Data_Center/Data/Spreads_Data/Spreads/<symbol>.db
- Erwartet je Symbol eine SQLite-Datei mit Tabelle:
    spreads

Zeigt pro Symbol:
    - aktuellen Spread
    - aktuellen Spread in Points
    - Avg 1m
    - Avg 5m
    - Min 5m
    - Max 5m
    - letzten Timestamp

Sortierung:
    - nach aktuellem spread_points absteigend

Vorteil:
- Keine zweite MT5-Verbindung nötig
- Dashboard liest nur die Daten, die dein Spread-Loop bereits schreibt
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import tkinter as tk
from tkinter import ttk

import pandas as pd


# ============================================================
# PROJECT ROOT / PATHS
# ============================================================

def find_project_root(start: Path) -> Path:
    cur = start.resolve()
    for p in [cur] + list(cur.parents):
        if (p / "1.Data_Center").exists() and (p / "3.Control_Panel").exists():
            return p
    raise RuntimeError(
        f"Projekt-Root nicht gefunden. Erwartet Root mit '1.Data_Center' und '3.Control_Panel'. Start={start}"
    )


SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = find_project_root(SCRIPT_PATH)

SPREAD_DIR = PROJECT_ROOT / "1.Data_Center" / "Data" / "Spreads_Data" / "Spreads"


# ============================================================
# CONFIG
# ============================================================

REFRESH_MS = 1000
TAIL_ROWS = 5000
SPREAD_TABLE_NAME = "spreads"

SYMBOLS: List[str] = [
    "AUDJPY", "AUDUSD", "EURGBP", "EURUSD", "GBPUSD", "GBPJPY",
    "NZDUSD", "US500.cash", "USDCAD", "USDCHF", "USDJPY", "USOIL.cash", "XAUUSD"
]


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
FG_HEADER = "#C7D0DA"
FG_ACCENT = "#F5A623"
FG_POS = "#22C55E"
FG_NEG = "#EF4444"
FG_WHITE = "#FFFFFF"

BORDER = "#232A34"

FONT_TITLE = ("Segoe UI", 17, "bold")
FONT_SECTION = ("Segoe UI", 11, "bold")
FONT_LABEL = ("Segoe UI", 9)
FONT_VALUE = ("Segoe UI", 12, "bold")
FONT_TABLE = ("Segoe UI", 9)


# ============================================================
# HELPERS
# ============================================================

def fmt_num(x, digits: int = 2) -> str:
    try:
        v = float(x)
    except Exception:
        return "-"
    return f"{v:.{digits}f}"


def fmt_ts(x) -> str:
    if pd.isna(x):
        return "-"
    try:
        ts = pd.to_datetime(x, utc=True)
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "-"


def safe_symbol_filename(symbol: str) -> str:
    return symbol.replace("/", "_") + ".db"


def spread_db_path(symbol: str) -> Path:
    return SPREAD_DIR / safe_symbol_filename(symbol)


def safe_read_tail_sqlite(
    db_path: Path,
    table_name: str = SPREAD_TABLE_NAME,
    tail_rows: int = TAIL_ROWS,
) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame()

    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(str(db_path))
        query = f"""
            SELECT *
            FROM {table_name}
            ORDER BY time_utc DESC
            LIMIT {int(tail_rows)}
        """
        df = pd.read_sql_query(query, conn)
        if df.empty:
            return df

        # wieder aufsteigend sortieren, damit rolling/logische Zeitreihen stimmen
        if "time_utc" in df.columns:
            df["time_utc"] = pd.to_datetime(df["time_utc"], errors="coerce", utc=True)
            df = df.sort_values("time_utc").reset_index(drop=True)

        return df
    except Exception:
        return pd.DataFrame()
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# DATA REPOSITORY
# ============================================================

class SpreadRepository:
    def __init__(self, spread_dir: Path):
        self.spread_dir = spread_dir

    def load_symbol_data(self, symbol: str) -> pd.DataFrame:
        path = spread_db_path(symbol)
        df = safe_read_tail_sqlite(path)

        if df.empty:
            return df

        expected_cols = [
            "time_utc",
            "symbol",
            "bid",
            "ask",
            "spread",
            "spread_points",
            "digits",
            "trade_mode",
        ]
        for c in expected_cols:
            if c not in df.columns:
                df[c] = None

        df["time_utc"] = pd.to_datetime(df["time_utc"], errors="coerce", utc=True)
        for c in ["bid", "ask", "spread", "spread_points"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.dropna(subset=["time_utc"]).sort_values("time_utc").reset_index(drop=True)
        return df

    def build_live_snapshot(self, symbols: List[str]) -> pd.DataFrame:
        rows = []
        now = datetime.now(timezone.utc)
        cutoff_1m = now - timedelta(minutes=1)
        cutoff_5m = now - timedelta(minutes=5)

        for symbol in symbols:
            df = self.load_symbol_data(symbol)

            if df.empty:
                rows.append(
                    {
                        "symbol": symbol,
                        "spread": None,
                        "spread_points": None,
                        "avg_1m": None,
                        "avg_5m": None,
                        "min_5m": None,
                        "max_5m": None,
                        "last_time_utc": None,
                        "rows": 0,
                        "status": "NO_DATA",
                    }
                )
                continue

            last = df.iloc[-1]

            d1 = df[df["time_utc"] >= cutoff_1m].copy()
            d5 = df[df["time_utc"] >= cutoff_5m].copy()

            rows.append(
                {
                    "symbol": symbol,
                    "spread": float(last["spread"]) if pd.notna(last["spread"]) else None,
                    "spread_points": float(last["spread_points"]) if pd.notna(last["spread_points"]) else None,
                    "avg_1m": float(d1["spread_points"].mean()) if not d1.empty else None,
                    "avg_5m": float(d5["spread_points"].mean()) if not d5.empty else None,
                    "min_5m": float(d5["spread_points"].min()) if not d5.empty else None,
                    "max_5m": float(d5["spread_points"].max()) if not d5.empty else None,
                    "last_time_utc": last["time_utc"],
                    "rows": int(len(df)),
                    "status": "LIVE",
                }
            )

        out = pd.DataFrame(rows)

        if out.empty:
            return out

        if "spread_points" in out.columns:
            out["spread_points_sort"] = pd.to_numeric(out["spread_points"], errors="coerce")
            out = out.sort_values("spread_points_sort", ascending=False, na_position="last").reset_index(drop=True)
            out = out.drop(columns=["spread_points_sort"], errors="ignore")

        return out


# ============================================================
# KPI CARD
# ============================================================

class KpiCard(tk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        self.configure(height=72)
        self.pack_propagate(False)

        self.title_label = tk.Label(
            self,
            text=title,
            font=FONT_LABEL,
            bg=BG_CARD,
            fg=FG_MUTED,
        )
        self.title_label.pack(anchor="w", padx=10, pady=(8, 2))

        self.value_var = tk.StringVar(value="-")
        self.value_label = tk.Label(
            self,
            textvariable=self.value_var,
            font=FONT_VALUE,
            bg=BG_CARD,
            fg=FG_MAIN,
        )
        self.value_label.pack(anchor="w", padx=10)

    def set_value(self, value: str, color: Optional[str] = None):
        self.value_var.set(value)
        self.value_label.configure(fg=color or FG_MAIN)


# ============================================================
# DASHBOARD
# ============================================================

class SpreadDashboard(tk.Tk):
    def __init__(self, repo: SpreadRepository):
        super().__init__()

        self.repo = repo
        self.title("Live Spread Dashboard")
        self.geometry("1500x860")
        self.minsize(1200, 700)
        self.configure(bg=BG_MAIN)

        self._refresh_job = None

        self._build_ui()
        self._refresh()

    def _build_ui(self):
        root = tk.Frame(self, bg=BG_MAIN)
        root.pack(fill="both", expand=True, padx=12, pady=12)

        topbar = tk.Frame(root, bg=BG_HEADER, height=54, highlightbackground=BORDER, highlightthickness=1)
        topbar.pack(fill="x", pady=(0, 12))
        topbar.pack_propagate(False)

        tk.Label(
            topbar,
            text="LIVE SPREAD DASHBOARD",
            font=FONT_TITLE,
            bg=BG_HEADER,
            fg=FG_MAIN,
        ).pack(side="left", padx=14)

        self.info_var = tk.StringVar(value=str(SPREAD_DIR))
        tk.Label(
            topbar,
            textvariable=self.info_var,
            font=("Segoe UI", 9),
            bg=BG_HEADER,
            fg=FG_MUTED,
        ).pack(side="right", padx=14)

        controls = tk.Frame(root, bg=BG_MAIN)
        controls.pack(fill="x", pady=(0, 12))

        tk.Button(
            controls,
            text="Refresh Now",
            command=self._refresh_now,
            bg=BG_BUTTON,
            fg=FG_MAIN,
            activebackground=BG_PANEL_2,
            activeforeground=FG_MAIN,
            relief="flat",
            padx=12,
            pady=6,
            bd=0,
        ).pack(side="left")

        self.status_var = tk.StringVar(value="INIT")
        tk.Label(
            controls,
            textvariable=self.status_var,
            bg=BG_MAIN,
            fg=FG_MUTED,
            font=FONT_LABEL,
        ).pack(side="right")

        kpi_row = tk.Frame(root, bg=BG_MAIN)
        kpi_row.pack(fill="x", pady=(0, 12))

        self.card_widest = KpiCard(kpi_row, "Widest Spread")
        self.card_widest.pack(side="left", fill="x", expand=True, padx=4)

        self.card_tightest = KpiCard(kpi_row, "Tightest Spread")
        self.card_tightest.pack(side="left", fill="x", expand=True, padx=4)

        self.card_live = KpiCard(kpi_row, "Live Symbols")
        self.card_live.pack(side="left", fill="x", expand=True, padx=4)

        self.card_timestamp = KpiCard(kpi_row, "Latest Tick")
        self.card_timestamp.pack(side="left", fill="x", expand=True, padx=4)

        table_wrap = tk.Frame(root, bg=BG_PANEL, highlightbackground=BORDER, highlightthickness=1)
        table_wrap.pack(fill="both", expand=True)

        header = tk.Frame(table_wrap, bg=BG_PANEL)
        header.pack(fill="x", padx=10, pady=(10, 6))

        tk.Label(
            header,
            text="Spread Monitor",
            font=FONT_SECTION,
            bg=BG_PANEL,
            fg=FG_MAIN,
        ).pack(anchor="w")

        cols = (
            "rank",
            "symbol",
            "spread",
            "spread_points",
            "avg_1m",
            "avg_5m",
            "min_5m",
            "max_5m",
            "last_time_utc",
            "status",
        )

        self.tree = ttk.Treeview(table_wrap, columns=cols, show="headings", height=28)
        self.tree.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        widths = {
            "rank": 60,
            "symbol": 140,
            "spread": 110,
            "spread_points": 120,
            "avg_1m": 110,
            "avg_5m": 110,
            "min_5m": 110,
            "max_5m": 110,
            "last_time_utc": 180,
            "status": 90,
        }

        for col in cols:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=widths[col], anchor="w")

    def _refresh_now(self):
        self._refresh()

    def _update_kpis(self, df: pd.DataFrame):
        if df.empty:
            self.card_widest.set_value("-")
            self.card_tightest.set_value("-")
            self.card_live.set_value("0")
            self.card_timestamp.set_value("-")
            return

        live_df = df[df["status"] == "LIVE"].copy()

        if live_df.empty:
            self.card_widest.set_value("-")
            self.card_tightest.set_value("-")
            self.card_live.set_value("0")
            self.card_timestamp.set_value("-")
            return

        widest = live_df.sort_values("spread_points", ascending=False, na_position="last").iloc[0]
        tightest = live_df.sort_values("spread_points", ascending=True, na_position="last").iloc[0]

        self.card_widest.set_value(
            f"{widest['symbol']} | {fmt_num(widest['spread_points'], 2)} pt",
            color=FG_NEG,
        )
        self.card_tightest.set_value(
            f"{tightest['symbol']} | {fmt_num(tightest['spread_points'], 2)} pt",
            color=FG_POS,
        )
        self.card_live.set_value(str(len(live_df)))

        latest_ts = pd.to_datetime(live_df["last_time_utc"], errors="coerce", utc=True).max()
        self.card_timestamp.set_value(fmt_ts(latest_ts))

    def _update_table(self, df: pd.DataFrame):
        self.tree.delete(*self.tree.get_children())

        if df.empty:
            return

        for idx, row in df.iterrows():
            self.tree.insert(
                "",
                "end",
                values=(
                    idx + 1,
                    row.get("symbol", ""),
                    fmt_num(row.get("spread"), 5) if pd.notna(row.get("spread")) else "-",
                    fmt_num(row.get("spread_points"), 2) if pd.notna(row.get("spread_points")) else "-",
                    fmt_num(row.get("avg_1m"), 2) if pd.notna(row.get("avg_1m")) else "-",
                    fmt_num(row.get("avg_5m"), 2) if pd.notna(row.get("avg_5m")) else "-",
                    fmt_num(row.get("min_5m"), 2) if pd.notna(row.get("min_5m")) else "-",
                    fmt_num(row.get("max_5m"), 2) if pd.notna(row.get("max_5m")) else "-",
                    fmt_ts(row.get("last_time_utc")),
                    row.get("status", ""),
                ),
            )

    def _refresh(self):
        try:
            df = self.repo.build_live_snapshot(SYMBOLS)
            self._update_kpis(df)
            self._update_table(df)

            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            self.status_var.set(f"updated={now_str} | symbols={len(df)} | dir={SPREAD_DIR}")
        except Exception as e:
            self.status_var.set(f"ERROR: {e}")

        self._refresh_job = self.after(REFRESH_MS, self._refresh)

    def destroy(self):
        if self._refresh_job is not None:
            try:
                self.after_cancel(self._refresh_job)
            except Exception:
                pass
            self._refresh_job = None
        super().destroy()


# ============================================================
# MAIN
# ============================================================

def main():
    if not SPREAD_DIR.exists():
        raise RuntimeError(f"Spread-Verzeichnis nicht gefunden: {SPREAD_DIR}")

    repo = SpreadRepository(SPREAD_DIR)
    app = SpreadDashboard(repo)
    app.mainloop()


if __name__ == "__main__":
    main()