# -*- coding: utf-8 -*-
"""
3.Control_Panel/Performance_Dashboard/strategy_performance_dashboard.py

Zweck:
- Visuelles Strategy Performance Dashboard im dunklen Corporate-Stil
- Hauptansicht = Monatskalender mit Daily PnL / Trades pro Tag
- Unterstützt:
    - einzelne Strategie
    - Account Overall (aggregiert alle Strategie-Trades des Accounts)
- Liest Daten aus:
    1.Data_Center/Data/Strategy_Data/Live_Trades_Data/Strategy_Live_Performance/account_*/strategies/*
- Live-Refresh im Loop
- Zeigt:
    - Account-Auswahl
    - Strategie-Auswahl inkl. "Account Overall"
    - KPI-Karten
    - Monatsnavigation
    - Kalenderansicht mit Day-PnL + Trade Count
    - Monats- und Wochen-Summary
    - Strategie-Ranking pro Account
    - Trade-Tabelle
    - Live-Status / Auto-Refresh
    - Klick auf Kalendertag => Day Insights Fenster
    - Extra Tab: Equity Curve
    - Großer Equity-Chart in eigenem Fenster

Erwartete Datei pro Strategieordner:
    trades.db

Erwartete Tabellen:
    trades
    kpis
    weekly_performance
    monthly_performance

Start:
    python 3.Control_Panel/Performance_Dashboard/strategy_performance_dashboard.py
"""

from __future__ import annotations

import calendar
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import tkinter as tk
from tkinter import ttk

import pandas as pd

from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg


# ============================================================
# PATHS
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

LIVE_PERF_ROOT = (
    PROJECT_ROOT
    / "1.Data_Center"
    / "Data"
    / "Strategy_Data"
    / "Live_Trades_Data"
    / "Strategy_Live_Performance"
)

ACCOUNT_OVERALL_LABEL = "Account Overall"
STRATEGY_DB_FILENAME = "trades.db"


# ============================================================
# CONFIG
# ============================================================

AUTO_REFRESH_MS = 3000
AUTO_REFRESH_DEFAULT = True
MAX_TRADES_IN_TABLE = 250
MAX_DAY_TRADES_IN_TABLE = 500
START_EQUITY_DEFAULT = 100000.0


# ============================================================
# STYLE
# ============================================================

BG_MAIN = "#0A0C10"
BG_PANEL = "#11151B"
BG_PANEL_2 = "#151A21"
BG_HEADER = "#171C24"
BG_CARD = "#11151B"
BG_BUTTON = "#1A2029"
BG_CELL = "#11151B"
BG_CELL_EMPTY = "#0F1318"
BG_POS = "#123139"
BG_NEG = "#351A22"
BG_NEUTRAL = "#1A1F27"

FG_MAIN = "#E6EAF0"
FG_MUTED = "#9BA6B2"
FG_POS = "#26D07C"
FG_NEG = "#FF5A67"
FG_WHITE = "#FFFFFF"

BORDER = "#232A34"

FONT_TITLE = ("Segoe UI", 17, "bold")
FONT_SECTION = ("Segoe UI", 11, "bold")
FONT_LABEL = ("Segoe UI", 9)
FONT_VALUE = ("Segoe UI", 12, "bold")
FONT_DAY = ("Segoe UI", 10)
FONT_DAY_SMALL = ("Segoe UI", 8)


# ============================================================
# HELPERS
# ============================================================

def fmt_money(x) -> str:
    try:
        v = float(x)
    except Exception:
        return "-"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:,.2f}"


def fmt_pct(x) -> str:
    try:
        v = float(x)
    except Exception:
        return "-"
    return f"{v:.2%}"


def fmt_float(x, digits: int = 2) -> str:
    try:
        v = float(x)
    except Exception:
        return "-"
    return f"{v:.{digits}f}"


def parse_utc_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    return df


def list_account_dirs(root: Path) -> List[Path]:
    if not root.exists():
        return []
    out = [p for p in root.iterdir() if p.is_dir() and p.name.startswith("account_")]
    return sorted(out, key=lambda x: x.name)


def list_strategy_dirs(account_dir: Path) -> List[Path]:
    strategies_dir = account_dir / "strategies"
    if not strategies_dir.exists():
        return []
    out = [p for p in strategies_dir.iterdir() if p.is_dir()]
    return sorted(out, key=lambda x: x.name)


def posneg_color(v: Optional[float]) -> str:
    if v is None or pd.isna(v):
        return FG_MAIN
    return FG_POS if float(v) >= 0 else FG_NEG


def day_bg_from_pnl(v: Optional[float]) -> str:
    if v is None or pd.isna(v):
        return BG_CELL_EMPTY
    if float(v) > 0:
        return BG_POS
    if float(v) < 0:
        return BG_NEG
    return BG_NEUTRAL


def safe_read_sqlite_table(db_path: Path, table_name: str) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame()

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql_query(query, conn)
    except Exception:
        return pd.DataFrame()
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ============================================================
# DATA MODELS
# ============================================================

@dataclass
class StrategyBundle:
    trades: pd.DataFrame
    kpis: pd.DataFrame
    weekly: pd.DataFrame
    monthly: pd.DataFrame


# ============================================================
# KPI / AGGREGATION
# ============================================================

def compute_kpis_from_trades(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty or "net_sum" not in trades.columns:
        return pd.DataFrame()

    d = trades.copy()
    d["net_sum"] = pd.to_numeric(d["net_sum"], errors="coerce").fillna(0.0)

    n = int(len(d))
    wins = int((d["net_sum"] > 0).sum())
    losses = int((d["net_sum"] < 0).sum())
    net_pnl = float(d["net_sum"].sum())
    gross_profit = float(d.loc[d["net_sum"] > 0, "net_sum"].sum())
    gross_loss = float(d.loc[d["net_sum"] < 0, "net_sum"].sum())
    profit_factor = gross_profit / abs(gross_loss) if gross_loss < 0 else None
    win_rate = wins / n if n > 0 else None
    avg_trade = float(d["net_sum"].mean()) if n > 0 else None

    equity = d["net_sum"].cumsum()
    rolling_max = equity.cummax()
    dd = equity - rolling_max
    max_dd_closed = float(dd.min()) if len(dd) else 0.0

    out = pd.DataFrame([{
        "net_pnl": net_pnl,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": profit_factor,
        "n_trades": n,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "avg_trade": avg_trade,
        "max_drawdown_closed": max_dd_closed,
    }])
    return out


def compute_perf_from_trades(trades: pd.DataFrame, freq: str, start_equity: float = START_EQUITY_DEFAULT) -> pd.DataFrame:
    if trades.empty or "close_time_utc" not in trades.columns or "net_sum" not in trades.columns:
        return pd.DataFrame()

    d = trades.copy()
    d = d.dropna(subset=["close_time_utc"]).copy()
    if d.empty:
        return pd.DataFrame()

    d = d.sort_values("close_time_utc").set_index("close_time_utc")
    d["net_sum"] = pd.to_numeric(d["net_sum"], errors="coerce").fillna(0.0)

    pnl = d["net_sum"].resample(freq).sum().astype(float)

    out = pd.DataFrame({"pnl_money": pnl})
    out["cum_pnl_money"] = out["pnl_money"].cumsum()
    out["nav"] = float(start_equity) + out["cum_pnl_money"]
    out = out.reset_index().rename(columns={"close_time_utc": "date"})
    out["date"] = pd.to_datetime(out["date"], utc=True)
    return out


def build_daily_summary(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty or "close_time_utc" not in trades.columns or "net_sum" not in trades.columns:
        return pd.DataFrame(columns=["date", "day_pnl", "trade_count", "wins", "losses", "avg_trade"])

    d = trades.copy()
    d = d.dropna(subset=["close_time_utc"]).copy()
    if d.empty:
        return pd.DataFrame(columns=["date", "day_pnl", "trade_count", "wins", "losses", "avg_trade"])

    d["date"] = d["close_time_utc"].dt.floor("D")
    out = (
        d.groupby("date", as_index=False)
        .agg(
            day_pnl=("net_sum", "sum"),
            trade_count=("net_sum", "size"),
            wins=("net_sum", lambda s: int((s > 0).sum())),
            losses=("net_sum", lambda s: int((s < 0).sum())),
            avg_trade=("net_sum", "mean"),
        )
        .sort_values("date")
        .reset_index(drop=True)
    )
    return out


def build_month_stats(daily: pd.DataFrame, year: int, month: int) -> Dict[str, object]:
    if daily.empty:
        return {
            "month_pnl": 0.0,
            "trading_days": 0,
            "win_days": 0,
            "loss_days": 0,
            "avg_day": 0.0,
            "best_day": None,
            "worst_day": None,
        }

    x = daily[
        (daily["date"].dt.year == year) &
        (daily["date"].dt.month == month)
    ].copy()

    if x.empty:
        return {
            "month_pnl": 0.0,
            "trading_days": 0,
            "win_days": 0,
            "loss_days": 0,
            "avg_day": 0.0,
            "best_day": None,
            "worst_day": None,
        }

    return {
        "month_pnl": float(x["day_pnl"].sum()),
        "trading_days": int(len(x)),
        "win_days": int((x["day_pnl"] > 0).sum()),
        "loss_days": int((x["day_pnl"] < 0).sum()),
        "avg_day": float(x["day_pnl"].mean()) if len(x) > 0 else 0.0,
        "best_day": None if x.empty else x.loc[x["day_pnl"].idxmax()].to_dict(),
        "worst_day": None if x.empty else x.loc[x["day_pnl"].idxmin()].to_dict(),
    }


def build_weekly_month_summary(daily: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame(columns=["week_label", "week_pnl", "trade_count", "trading_days"])

    x = daily[
        (daily["date"].dt.year == year) &
        (daily["date"].dt.month == month)
    ].copy()

    if x.empty:
        return pd.DataFrame(columns=["week_label", "week_pnl", "trade_count", "trading_days"])

    x["week_start"] = x["date"] - pd.to_timedelta(x["date"].dt.weekday, unit="D")
    x["week_label"] = x["week_start"].dt.strftime("%Y-%m-%d")

    out = (
        x.groupby("week_label", as_index=False)
        .agg(
            week_pnl=("day_pnl", "sum"),
            trade_count=("trade_count", "sum"),
            trading_days=("date", "size"),
        )
        .sort_values("week_label")
        .reset_index(drop=True)
    )
    return out


def build_equity_curve(trades: pd.DataFrame, start_equity: float = START_EQUITY_DEFAULT) -> pd.DataFrame:
    if trades.empty or "close_time_utc" not in trades.columns or "net_sum" not in trades.columns:
        return pd.DataFrame(columns=["close_time_utc", "equity", "drawdown_money"])

    d = trades.copy()
    d["close_time_utc"] = pd.to_datetime(d["close_time_utc"], errors="coerce", utc=True)
    d["net_sum"] = pd.to_numeric(d["net_sum"], errors="coerce").fillna(0.0)
    d = d.dropna(subset=["close_time_utc"]).sort_values("close_time_utc").reset_index(drop=True)

    if d.empty:
        return pd.DataFrame(columns=["close_time_utc", "equity", "drawdown_money"])

    d["equity"] = float(start_equity) + d["net_sum"].cumsum()
    d["rolling_max"] = d["equity"].cummax()
    d["drawdown_money"] = d["equity"] - d["rolling_max"]

    return d[["close_time_utc", "equity", "drawdown_money"]].copy()


# ============================================================
# DATA REPOSITORY
# ============================================================

class StrategyDataRepository:
    def __init__(self, root: Path):
        self.root = root

    def get_accounts(self) -> List[str]:
        return [p.name for p in list_account_dirs(self.root)]

    def get_strategies(self, account_name: str) -> List[str]:
        account_dir = self.root / account_name
        strategies = [p.name for p in list_strategy_dirs(account_dir)]
        return [ACCOUNT_OVERALL_LABEL] + strategies

    def _normalize_bundle(self, trades: pd.DataFrame, kpis: pd.DataFrame, weekly: pd.DataFrame, monthly: pd.DataFrame) -> StrategyBundle:
        if not trades.empty:
            trades = parse_utc_col(trades, "open_time_utc")
            trades = parse_utc_col(trades, "close_time_utc")
            for c in ["net_sum", "volume_in", "entry_price", "exit_price", "close_ticket", "magic"]:
                if c in trades.columns:
                    trades[c] = pd.to_numeric(trades[c], errors="coerce")

        if not weekly.empty:
            weekly = parse_utc_col(weekly, "date")
            for c in ["pnl_money", "cum_pnl_money", "nav"]:
                if c in weekly.columns:
                    weekly[c] = pd.to_numeric(weekly[c], errors="coerce")

        if not monthly.empty:
            monthly = parse_utc_col(monthly, "date")
            for c in ["pnl_money", "cum_pnl_money", "nav"]:
                if c in monthly.columns:
                    monthly[c] = pd.to_numeric(monthly[c], errors="coerce")

        if not kpis.empty:
            for c in [
                "net_pnl", "gross_profit", "gross_loss", "profit_factor", "n_trades", "wins",
                "losses", "win_rate", "avg_trade", "max_drawdown_closed",
            ]:
                if c in kpis.columns:
                    kpis[c] = pd.to_numeric(kpis[c], errors="coerce")

        return StrategyBundle(trades=trades, kpis=kpis, weekly=weekly, monthly=monthly)

    def _load_single_strategy_bundle(self, base: Path) -> StrategyBundle:
        db_path = base / STRATEGY_DB_FILENAME

        trades = safe_read_sqlite_table(db_path, "trades")
        kpis = safe_read_sqlite_table(db_path, "kpis")
        weekly = safe_read_sqlite_table(db_path, "weekly_performance")
        monthly = safe_read_sqlite_table(db_path, "monthly_performance")

        return self._normalize_bundle(trades, kpis, weekly, monthly)

    def _load_account_overall_bundle(self, account_name: str) -> StrategyBundle:
        account_dir = self.root / account_name
        strategy_dirs = list_strategy_dirs(account_dir)

        trades_parts: List[pd.DataFrame] = []

        for sdir in strategy_dirs:
            db_path = sdir / STRATEGY_DB_FILENAME
            trades = safe_read_sqlite_table(db_path, "trades")
            if trades.empty:
                continue

            trades = parse_utc_col(trades, "open_time_utc")
            trades = parse_utc_col(trades, "close_time_utc")
            for c in ["net_sum", "volume_in", "entry_price", "exit_price", "close_ticket", "magic"]:
                if c in trades.columns:
                    trades[c] = pd.to_numeric(trades[c], errors="coerce")

            if "strategy_id" not in trades.columns:
                trades["strategy_id"] = sdir.name

            trades_parts.append(trades)

        if not trades_parts:
            return StrategyBundle(
                trades=pd.DataFrame(),
                kpis=pd.DataFrame(),
                weekly=pd.DataFrame(),
                monthly=pd.DataFrame(),
            )

        trades_all = pd.concat(trades_parts, ignore_index=True)

        if "close_ticket" in trades_all.columns:
            trades_all = trades_all.sort_values(["close_time_utc", "close_ticket"], ascending=[False, False])
            trades_all = trades_all.drop_duplicates(subset=["close_ticket"], keep="first")
        else:
            trades_all = trades_all.sort_values("close_time_utc", ascending=False)

        trades_all = trades_all.reset_index(drop=True)

        trades_for_kpi = trades_all.sort_values("close_time_utc").reset_index(drop=True)
        kpis = compute_kpis_from_trades(trades_for_kpi)
        weekly = compute_perf_from_trades(trades_for_kpi, "W-FRI")
        monthly = compute_perf_from_trades(trades_for_kpi, "ME")

        return StrategyBundle(
            trades=trades_all,
            kpis=kpis,
            weekly=weekly,
            monthly=monthly,
        )

    def load_strategy_bundle(self, account_name: str, strategy_name: str) -> StrategyBundle:
        if strategy_name == ACCOUNT_OVERALL_LABEL:
            return self._load_account_overall_bundle(account_name)

        base = self.root / account_name / "strategies" / strategy_name
        return self._load_single_strategy_bundle(base)

    def build_account_ranking(self, account_name: str) -> pd.DataFrame:
        account_dir = self.root / account_name
        strategy_dirs = list_strategy_dirs(account_dir)
        rows: List[dict] = []

        overall_bundle = self._load_account_overall_bundle(account_name)
        if not overall_bundle.kpis.empty:
            k = overall_bundle.kpis.iloc[0]
            rows.append({
                "strategy_folder": ACCOUNT_OVERALL_LABEL,
                "net_pnl": k.get("net_pnl"),
                "n_trades": k.get("n_trades"),
                "win_rate": k.get("win_rate"),
                "profit_factor": k.get("profit_factor"),
                "max_drawdown_closed": k.get("max_drawdown_closed"),
                "avg_trade": k.get("avg_trade"),
            })

        for sdir in strategy_dirs:
            db_path = sdir / STRATEGY_DB_FILENAME
            kpis = safe_read_sqlite_table(db_path, "kpis")
            if kpis.empty:
                continue

            row = {"strategy_folder": sdir.name}
            for col in [
                "net_pnl",
                "n_trades",
                "win_rate",
                "profit_factor",
                "max_drawdown_closed",
                "avg_trade",
            ]:
                row[col] = kpis.iloc[0][col] if col in kpis.columns else None

            rows.append(row)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        for c in ["net_pnl", "n_trades", "win_rate", "profit_factor", "max_drawdown_closed", "avg_trade"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        return df.sort_values("net_pnl", ascending=False, na_position="last").reset_index(drop=True)


# ============================================================
# UI WIDGETS
# ============================================================

class KpiCard(tk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent, bg=BG_CARD, highlightbackground=BORDER, highlightthickness=1)
        self.configure(height=72)
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

    def set_value(self, value: str, color: Optional[str] = None):
        self.value_var.set(value)
        self.value_label.configure(fg=color or FG_MAIN)


class DayCell(tk.Frame):
    def __init__(self, parent, click_callback=None):
        super().__init__(
            parent,
            bg=BG_CELL_EMPTY,
            highlightbackground=BORDER,
            highlightthickness=1,
            height=108,
            width=120,
            cursor="hand2"
        )
        self.pack_propagate(False)

        self.click_callback = click_callback
        self.day_date: Optional[pd.Timestamp] = None
        self.day_payload: Optional[dict] = None
        self.in_month = False

        self.day_label = tk.Label(self, text="", font=FONT_DAY, bg=BG_CELL_EMPTY, fg=FG_MUTED, anchor="nw")
        self.day_label.pack(anchor="nw", padx=8, pady=(6, 2))

        self.pnl_box = tk.Frame(self, bg=BG_CELL_EMPTY)
        self.pnl_box.pack(fill="x", padx=6, pady=(10, 0))

        self.pnl_label = tk.Label(self.pnl_box, text="", font=("Segoe UI", 9, "bold"), bg=BG_CELL_EMPTY, fg=FG_MAIN, anchor="w")
        self.pnl_label.pack(anchor="w", padx=4, pady=(2, 0))

        self.trades_label = tk.Label(self.pnl_box, text="", font=FONT_DAY_SMALL, bg=BG_CELL_EMPTY, fg=FG_MUTED, anchor="w")
        self.trades_label.pack(anchor="w", padx=4, pady=(0, 2))

        for widget in [self, self.day_label, self.pnl_box, self.pnl_label, self.trades_label]:
            widget.bind("<Button-1>", self._on_click)

    def _on_click(self, _event=None):
        if self.click_callback and self.in_month and self.day_date is not None:
            self.click_callback(self.day_date, self.day_payload)

    def set_empty(self, day_num: int, in_month: bool):
        self.in_month = in_month
        self.day_date = None
        self.day_payload = None

        bg = BG_CELL_EMPTY if in_month else BG_PANEL_2
        fg = FG_MUTED if in_month else "#66707C"

        self.configure(bg=bg)
        self.day_label.configure(text=str(day_num) if day_num > 0 else "", bg=bg, fg=fg)
        self.pnl_box.configure(bg=bg)
        self.pnl_label.configure(text="", bg=bg)
        self.trades_label.configure(text="", bg=bg)

    def set_day(self, day_num: int, day_date: pd.Timestamp, day_pnl: Optional[float], trade_count: int, payload: Optional[dict] = None):
        self.in_month = True
        self.day_date = day_date
        self.day_payload = payload or {}

        bg = day_bg_from_pnl(day_pnl)
        self.configure(bg=bg)
        self.day_label.configure(text=str(day_num), bg=bg, fg=FG_MAIN)
        self.pnl_box.configure(bg=bg)

        if day_pnl is None:
            self.pnl_label.configure(text="", bg=bg)
            self.trades_label.configure(text="", bg=bg)
            return

        self.pnl_label.configure(
            text=fmt_money(day_pnl),
            bg=bg,
            fg=posneg_color(day_pnl),
        )
        self.trades_label.configure(
            text=f"{trade_count} tr",
            bg=bg,
            fg=FG_MUTED,
        )


# ============================================================
# DASHBOARD
# ============================================================

class StrategyPerformanceDashboard(tk.Tk):
    def __init__(self, repo: StrategyDataRepository):
        super().__init__()

        self.repo = repo
        self.title("Strategy Performance Dashboard")
        self.geometry("1720x980")
        self.minsize(1350, 820)
        self.configure(bg=BG_MAIN)

        self.selected_account: Optional[str] = None
        self.selected_strategy: Optional[str] = None
        self.current_bundle: Optional[StrategyBundle] = None
        self.daily_summary: pd.DataFrame = pd.DataFrame()

        self.current_year: Optional[int] = None
        self.current_month: Optional[int] = None

        self.auto_refresh_enabled = tk.BooleanVar(value=AUTO_REFRESH_DEFAULT)
        self._refresh_job: Optional[str] = None
        self.last_refresh_ts: Optional[pd.Timestamp] = None

        self.equity_canvas: Optional[FigureCanvasTkAgg] = None
        self.equity_figure: Optional[Figure] = None
        self.equity_ax = None
        self.dd_ax = None

        self._build_ui()
        self._load_accounts()
        self._schedule_auto_refresh()

    def _build_ui(self):
        root = tk.Frame(self, bg=BG_MAIN)
        root.pack(fill="both", expand=True, padx=12, pady=12)

        topbar = tk.Frame(root, bg=BG_HEADER, height=54, highlightbackground=BORDER, highlightthickness=1)
        topbar.pack(fill="x", pady=(0, 12))
        topbar.pack_propagate(False)

        tk.Label(
            topbar,
            text="STRATEGY PERFORMANCE DASHBOARD",
            font=FONT_TITLE,
            bg=BG_HEADER,
            fg=FG_MAIN,
        ).pack(side="left", padx=14)

        self.info_var = tk.StringVar(value=str(LIVE_PERF_ROOT))
        tk.Label(
            topbar,
            textvariable=self.info_var,
            font=("Segoe UI", 9),
            bg=BG_HEADER,
            fg=FG_MUTED,
        ).pack(side="right", padx=14)

        self.live_status_var = tk.StringVar(value="LIVE: idle")
        tk.Label(
            topbar,
            textvariable=self.live_status_var,
            font=("Segoe UI", 9),
            bg=BG_HEADER,
            fg=FG_MUTED,
        ).pack(side="right", padx=(0, 14))

        controls = tk.Frame(root, bg=BG_MAIN)
        controls.pack(fill="x", pady=(0, 12))

        tk.Label(controls, text="Account", bg=BG_MAIN, fg=FG_MUTED, font=FONT_LABEL).pack(side="left", padx=(0, 6))
        self.account_combo = ttk.Combobox(controls, state="readonly", width=24)
        self.account_combo.pack(side="left", padx=(0, 12))
        self.account_combo.bind("<<ComboboxSelected>>", self._on_account_change)

        tk.Label(controls, text="Strategy", bg=BG_MAIN, fg=FG_MUTED, font=FONT_LABEL).pack(side="left", padx=(0, 6))
        self.strategy_combo = ttk.Combobox(controls, state="readonly", width=56)
        self.strategy_combo.pack(side="left", padx=(0, 12))
        self.strategy_combo.bind("<<ComboboxSelected>>", self._on_strategy_change)

        tk.Button(
            controls,
            text="Refresh",
            command=self._refresh_all,
            bg=BG_BUTTON,
            fg=FG_MAIN,
            activebackground=BG_PANEL_2,
            activeforeground=FG_MAIN,
            relief="flat",
            padx=12,
            pady=6,
            bd=0,
        ).pack(side="left", padx=(0, 12))

        tk.Checkbutton(
            controls,
            text="Auto Refresh",
            variable=self.auto_refresh_enabled,
            command=self._on_toggle_auto_refresh,
            bg=BG_MAIN,
            fg=FG_MAIN,
            activebackground=BG_MAIN,
            activeforeground=FG_MAIN,
            selectcolor=BG_PANEL_2,
            relief="flat",
        ).pack(side="left")

        kpi_row = tk.Frame(root, bg=BG_MAIN)
        kpi_row.pack(fill="x", pady=(0, 12))

        self.card_net = KpiCard(kpi_row, "Net PnL")
        self.card_net.pack(side="left", fill="x", expand=True, padx=4)

        self.card_trades = KpiCard(kpi_row, "Trades")
        self.card_trades.pack(side="left", fill="x", expand=True, padx=4)

        self.card_winrate = KpiCard(kpi_row, "Win Rate")
        self.card_winrate.pack(side="left", fill="x", expand=True, padx=4)

        self.card_pf = KpiCard(kpi_row, "Profit Factor")
        self.card_pf.pack(side="left", fill="x", expand=True, padx=4)

        self.card_dd = KpiCard(kpi_row, "Max DD Closed")
        self.card_dd.pack(side="left", fill="x", expand=True, padx=4)

        main = tk.PanedWindow(root, orient="horizontal", bg=BG_MAIN, sashwidth=6)
        main.pack(fill="both", expand=True)

        left = tk.Frame(main, bg=BG_PANEL, highlightbackground=BORDER, highlightthickness=1)
        right = tk.Frame(main, bg=BG_PANEL, highlightbackground=BORDER, highlightthickness=1)

        main.add(left, minsize=360)
        main.add(right, minsize=900)

        self._build_left_panel(left)
        self._build_right_panel(right)

    def _build_left_panel(self, parent: tk.Frame):
        tk.Label(
            parent,
            text="Account Strategy Ranking",
            font=FONT_SECTION,
            bg=BG_PANEL,
            fg=FG_MAIN,
        ).pack(anchor="w", padx=10, pady=(10, 8))

        cols = ("rank", "strategy", "net_pnl", "n_trades", "win_rate", "pf")
        self.ranking_tree = ttk.Treeview(parent, columns=cols, show="headings", height=18)
        self.ranking_tree.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.ranking_tree.bind("<<TreeviewSelect>>", self._on_ranking_select)

        widths = {
            "rank": 55,
            "strategy": 230,
            "net_pnl": 95,
            "n_trades": 70,
            "win_rate": 80,
            "pf": 70,
        }

        for col in cols:
            self.ranking_tree.heading(col, text=col)
            self.ranking_tree.column(col, width=widths[col], anchor="w")

        summary_box = tk.Frame(parent, bg=BG_PANEL_2, highlightbackground=BORDER, highlightthickness=1)
        summary_box.pack(fill="x", padx=10, pady=(0, 10))

        tk.Label(
            summary_box,
            text="Month / Week Summary",
            font=FONT_SECTION,
            bg=BG_PANEL_2,
            fg=FG_MAIN,
        ).pack(anchor="w", padx=10, pady=(8, 6))

        self.month_summary_text = tk.Text(
            summary_box,
            height=11,
            wrap="word",
            bg=BG_PANEL_2,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            font=("Segoe UI", 9),
        )
        self.month_summary_text.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    def _build_right_panel(self, parent: tk.Frame):
        self.right_notebook = ttk.Notebook(parent)
        self.right_notebook.pack(fill="both", expand=True, padx=10, pady=10)

        self.tab_calendar = tk.Frame(self.right_notebook, bg=BG_PANEL)
        self.tab_equity = tk.Frame(self.right_notebook, bg=BG_PANEL)

        self.right_notebook.add(self.tab_calendar, text="Calendar")
        self.right_notebook.add(self.tab_equity, text="Equity Curve")

        self._build_calendar_tab(self.tab_calendar)
        self._build_equity_tab(self.tab_equity)

    def _build_calendar_tab(self, parent: tk.Frame):
        calendar_shell = tk.Frame(parent, bg=BG_PANEL)
        calendar_shell.pack(fill="both", expand=True)

        nav = tk.Frame(calendar_shell, bg=BG_PANEL)
        nav.pack(fill="x", pady=(0, 10))

        self.month_title_var = tk.StringVar(value="No month selected")
        tk.Label(
            nav,
            textvariable=self.month_title_var,
            font=FONT_TITLE,
            bg=BG_PANEL,
            fg=FG_MAIN,
        ).pack(side="left")

        tk.Button(
            nav,
            text="Today",
            command=self._go_to_latest_month,
            bg=BG_BUTTON,
            fg=FG_MAIN,
            activebackground=BG_PANEL_2,
            activeforeground=FG_MAIN,
            relief="flat",
            padx=12,
            pady=6,
            bd=0,
        ).pack(side="left", padx=(12, 0))

        tk.Button(
            nav,
            text="◀",
            command=self._prev_month,
            bg=BG_BUTTON,
            fg=FG_MAIN,
            activebackground=BG_PANEL_2,
            activeforeground=FG_MAIN,
            relief="flat",
            padx=12,
            pady=6,
            bd=0,
        ).pack(side="right", padx=(6, 0))

        tk.Button(
            nav,
            text="▶",
            command=self._next_month,
            bg=BG_BUTTON,
            fg=FG_MAIN,
            activebackground=BG_PANEL_2,
            activeforeground=FG_MAIN,
            relief="flat",
            padx=12,
            pady=6,
            bd=0,
        ).pack(side="right")

        month_stats_row = tk.Frame(calendar_shell, bg=BG_PANEL)
        month_stats_row.pack(fill="x", pady=(0, 10))

        self.card_month_pnl = KpiCard(month_stats_row, "Monthly Stats")
        self.card_month_pnl.pack(side="left", fill="x", expand=True, padx=4)

        self.card_trading_days = KpiCard(month_stats_row, "Trading Days")
        self.card_trading_days.pack(side="left", fill="x", expand=True, padx=4)

        self.card_best_day = KpiCard(month_stats_row, "Best Day")
        self.card_best_day.pack(side="left", fill="x", expand=True, padx=4)

        self.card_worst_day = KpiCard(month_stats_row, "Worst Day")
        self.card_worst_day.pack(side="left", fill="x", expand=True, padx=4)

        header_row = tk.Frame(calendar_shell, bg=BG_PANEL)
        header_row.pack(fill="x")

        for dayname in ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]:
            lbl = tk.Label(
                header_row,
                text=dayname,
                bg=BG_HEADER,
                fg=FG_MUTED,
                font=("Segoe UI", 9, "bold"),
                padx=8,
                pady=8,
                relief="flat",
                highlightbackground=BORDER,
                highlightthickness=1,
            )
            lbl.pack(side="left", fill="x", expand=True, padx=1, pady=1)

        self.calendar_grid = tk.Frame(calendar_shell, bg=BG_PANEL)
        self.calendar_grid.pack(fill="both", expand=True)

        self.day_cells: List[DayCell] = []
        for r in range(6):
            self.calendar_grid.grid_rowconfigure(r, weight=1)
            for c in range(7):
                self.calendar_grid.grid_columnconfigure(c, weight=1)
                cell = DayCell(self.calendar_grid, click_callback=self._on_day_cell_click)
                cell.grid(row=r, column=c, sticky="nsew", padx=1, pady=1)
                self.day_cells.append(cell)

        trades_section = tk.Frame(parent, bg=BG_PANEL_2, highlightbackground=BORDER, highlightthickness=1)
        trades_section.pack(fill="both", expand=False, pady=(10, 0))

        tk.Label(
            trades_section,
            text="Trade Ledger",
            font=FONT_SECTION,
            bg=BG_PANEL_2,
            fg=FG_MAIN,
        ).pack(anchor="w", padx=10, pady=(8, 6))

        trade_cols = (
            "close_time_utc",
            "symbol",
            "direction",
            "net_sum",
            "entry_price",
            "exit_price",
            "volume_in",
            "magic",
            "strategy_id",
        )

        self.trades_tree = ttk.Treeview(trades_section, columns=trade_cols, show="headings", height=11)
        self.trades_tree.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        trade_widths = {
            "close_time_utc": 170,
            "symbol": 90,
            "direction": 70,
            "net_sum": 90,
            "entry_price": 90,
            "exit_price": 90,
            "volume_in": 80,
            "magic": 70,
            "strategy_id": 130,
        }

        for col in trade_cols:
            self.trades_tree.heading(col, text=col)
            self.trades_tree.column(col, width=trade_widths[col], anchor="w")

    def _build_equity_tab(self, parent: tk.Frame):
        top = tk.Frame(parent, bg=BG_PANEL)
        top.pack(fill="x", pady=(0, 10))

        tk.Label(
            top,
            text="Equity Curve",
            font=FONT_TITLE,
            bg=BG_PANEL,
            fg=FG_MAIN,
        ).pack(side="left", padx=(0, 10))

        tk.Button(
            top,
            text="Open Large Window",
            command=self._open_large_equity_window,
            bg=BG_BUTTON,
            fg=FG_MAIN,
            activebackground=BG_PANEL_2,
            activeforeground=FG_MAIN,
            relief="flat",
            padx=12,
            pady=6,
            bd=0,
        ).pack(side="left")

        self.equity_info_var = tk.StringVar(value="No equity data")
        tk.Label(
            top,
            textvariable=self.equity_info_var,
            font=("Segoe UI", 9),
            bg=BG_PANEL,
            fg=FG_MUTED,
        ).pack(side="right")

        self.equity_stats_row = tk.Frame(parent, bg=BG_PANEL)
        self.equity_stats_row.pack(fill="x", pady=(0, 10))

        self.card_eq_last = KpiCard(self.equity_stats_row, "Current Equity")
        self.card_eq_last.pack(side="left", fill="x", expand=True, padx=4)

        self.card_eq_peak = KpiCard(self.equity_stats_row, "Peak Equity")
        self.card_eq_peak.pack(side="left", fill="x", expand=True, padx=4)

        self.card_eq_dd = KpiCard(self.equity_stats_row, "Current Drawdown")
        self.card_eq_dd.pack(side="left", fill="x", expand=True, padx=4)

        self.card_eq_points = KpiCard(self.equity_stats_row, "Curve Points")
        self.card_eq_points.pack(side="left", fill="x", expand=True, padx=4)

        chart_frame = tk.Frame(parent, bg=BG_PANEL_2, highlightbackground=BORDER, highlightthickness=1)
        chart_frame.pack(fill="both", expand=True)

        self.equity_figure = Figure(figsize=(10, 6), dpi=100, facecolor=BG_PANEL_2)
        self.equity_ax = self.equity_figure.add_subplot(211)
        self.dd_ax = self.equity_figure.add_subplot(212, sharex=self.equity_ax)

        self.equity_canvas = FigureCanvasTkAgg(self.equity_figure, master=chart_frame)
        self.equity_canvas.draw()
        self.equity_canvas.get_tk_widget().pack(fill="both", expand=True)

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
                self._reload_current_view(live=True)
        finally:
            self._schedule_auto_refresh()

    def _on_toggle_auto_refresh(self):
        if self.auto_refresh_enabled.get():
            self.live_status_var.set("LIVE | auto refresh enabled")
        else:
            self.live_status_var.set("LIVE | auto refresh disabled")
        self._schedule_auto_refresh()

    def _load_accounts(self):
        accounts = self.repo.get_accounts()
        self.account_combo["values"] = accounts

        if accounts:
            self.account_combo.current(0)
            self.selected_account = accounts[0]
            self._load_strategies()

    def _load_strategies(self):
        if not self.selected_account:
            return

        strategies = self.repo.get_strategies(self.selected_account)
        self.strategy_combo["values"] = strategies
        self._load_ranking()

        if strategies:
            self.strategy_combo.current(0)
            self.selected_strategy = strategies[0]
            self._load_strategy_dashboard()
        else:
            self.selected_strategy = None
            self._clear_dashboard()

    def _refresh_all(self):
        cur_account = self.account_combo.get()
        cur_strategy = self.strategy_combo.get()

        self._load_accounts()

        accounts = list(self.account_combo["values"])
        if cur_account in accounts:
            self.account_combo.set(cur_account)
            self.selected_account = cur_account
            self._load_strategies()

            strategies = list(self.strategy_combo["values"])
            if cur_strategy in strategies:
                self.strategy_combo.set(cur_strategy)
                self.selected_strategy = cur_strategy
                self._reload_current_view(live=False)

    def _on_account_change(self, _event=None):
        self.selected_account = self.account_combo.get()
        self._load_strategies()

    def _on_strategy_change(self, _event=None):
        self.selected_strategy = self.strategy_combo.get()
        self._load_strategy_dashboard()

    def _on_ranking_select(self, _event=None):
        selected = self.ranking_tree.selection()
        if not selected:
            return

        values = self.ranking_tree.item(selected[0], "values")
        if not values or len(values) < 2:
            return

        strategy_name = values[1]
        strategies = list(self.strategy_combo["values"])
        if strategy_name in strategies:
            self.strategy_combo.set(strategy_name)
            self.selected_strategy = strategy_name
            self._load_strategy_dashboard()

    def _go_to_latest_month(self):
        if self.daily_summary.empty:
            return
        last_date = self.daily_summary["date"].max()
        self.current_year = int(last_date.year)
        self.current_month = int(last_date.month)
        self._render_month()

    def _prev_month(self):
        if self.current_year is None or self.current_month is None:
            return
        y, m = self.current_year, self.current_month
        if m == 1:
            y -= 1
            m = 12
        else:
            m -= 1
        self.current_year, self.current_month = y, m
        self._render_month()

    def _next_month(self):
        if self.current_year is None or self.current_month is None:
            return
        y, m = self.current_year, self.current_month
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
        self.current_year, self.current_month = y, m
        self._render_month()

    def _clear_dashboard(self):
        for card in [
            self.card_net, self.card_trades, self.card_winrate, self.card_pf, self.card_dd,
            self.card_month_pnl, self.card_trading_days, self.card_best_day, self.card_worst_day,
            self.card_eq_last, self.card_eq_peak, self.card_eq_dd, self.card_eq_points,
        ]:
            card.set_value("-")

        self.month_title_var.set("No month selected")
        self.month_summary_text.delete("1.0", "end")
        self.trades_tree.delete(*self.trades_tree.get_children())
        self.ranking_tree.delete(*self.ranking_tree.get_children())
        self.equity_info_var.set("No equity data")

        for idx, cell in enumerate(self.day_cells, start=1):
            cell.set_empty(idx, True)

        self._render_empty_equity_chart()

    def _load_strategy_dashboard(self):
        if not self.selected_account or not self.selected_strategy:
            self._clear_dashboard()
            return

        self.current_year = None
        self.current_month = None
        self._reload_current_view(live=False)

    def _reload_current_view(self, live: bool = False):
        if not self.selected_account or not self.selected_strategy:
            return

        try:
            bundle = self.repo.load_strategy_bundle(self.selected_account, self.selected_strategy)
            self.current_bundle = bundle
            self.daily_summary = build_daily_summary(bundle.trades)

            self._update_kpis(bundle.kpis)
            self._load_trade_table(bundle.trades)
            self._load_ranking()
            self._render_equity_chart()

            if self.current_year is None or self.current_month is None:
                if not self.daily_summary.empty:
                    last_date = self.daily_summary["date"].max()
                    self.current_year = int(last_date.year)
                    self.current_month = int(last_date.month)
                else:
                    now = pd.Timestamp.now(tz="UTC")
                    self.current_year = int(now.year)
                    self.current_month = int(now.month)

            self._render_month()

            self.last_refresh_ts = pd.Timestamp.now(tz="UTC")
            mode = "LIVE" if live else "MANUAL"
            self.live_status_var.set(
                f"{mode} | last refresh {self.last_refresh_ts.strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )
        except Exception as e:
            self.live_status_var.set(f"LIVE ERROR | {e}")

    def _update_kpis(self, kpis: pd.DataFrame):
        if kpis.empty:
            self.card_net.set_value("-")
            self.card_trades.set_value("-")
            self.card_winrate.set_value("-")
            self.card_pf.set_value("-")
            self.card_dd.set_value("-")
            return

        row = kpis.iloc[0]

        net = row.get("net_pnl")
        self.card_net.set_value(fmt_money(net), color=posneg_color(net))
        self.card_trades.set_value(str(int(row.get("n_trades", 0))) if pd.notna(row.get("n_trades")) else "-")
        self.card_winrate.set_value(fmt_pct(row.get("win_rate")) if pd.notna(row.get("win_rate")) else "-")
        self.card_pf.set_value(fmt_float(row.get("profit_factor"), 2) if pd.notna(row.get("profit_factor")) else "-")
        self.card_dd.set_value(fmt_money(row.get("max_drawdown_closed")), color=FG_NEG)

    def _load_ranking(self):
        self.ranking_tree.delete(*self.ranking_tree.get_children())

        if not self.selected_account:
            return

        ranking = self.repo.build_account_ranking(self.selected_account)
        if ranking.empty:
            return

        for idx, row in ranking.iterrows():
            self.ranking_tree.insert(
                "",
                "end",
                values=(
                    idx + 1,
                    row.get("strategy_folder", ""),
                    fmt_money(row.get("net_pnl")),
                    int(row.get("n_trades")) if pd.notna(row.get("n_trades")) else "",
                    fmt_pct(row.get("win_rate")) if pd.notna(row.get("win_rate")) else "",
                    fmt_float(row.get("profit_factor"), 2) if pd.notna(row.get("profit_factor")) else "",
                ),
            )

    def _load_trade_table(self, trades: pd.DataFrame):
        self.trades_tree.delete(*self.trades_tree.get_children())

        if trades.empty:
            return

        d = trades.copy()

        if "close_time_utc" in d.columns:
            d["close_time_utc"] = pd.to_datetime(d["close_time_utc"], errors="coerce", utc=True)
            d = d.sort_values("close_time_utc", ascending=False)

        if "strategy_id" not in d.columns:
            d["strategy_id"] = ""

        d = d.head(MAX_TRADES_IN_TABLE)

        for _, row in d.iterrows():
            self.trades_tree.insert(
                "",
                "end",
                values=(
                    str(row.get("close_time_utc", "")),
                    row.get("symbol", ""),
                    row.get("direction", ""),
                    fmt_money(row.get("net_sum")),
                    fmt_float(row.get("entry_price"), 5) if pd.notna(row.get("entry_price")) else "",
                    fmt_float(row.get("exit_price"), 5) if pd.notna(row.get("exit_price")) else "",
                    fmt_float(row.get("volume_in"), 2) if pd.notna(row.get("volume_in")) else "",
                    row.get("magic", ""),
                    row.get("strategy_id", ""),
                ),
            )

    def _render_month(self):
        if self.current_year is None or self.current_month is None:
            return

        year, month = self.current_year, self.current_month
        self.month_title_var.set(f"{calendar.month_name[month]} {year}")

        month_stats = build_month_stats(self.daily_summary, year, month)
        self.card_month_pnl.set_value(fmt_money(month_stats["month_pnl"]), color=posneg_color(month_stats["month_pnl"]))
        self.card_trading_days.set_value(str(month_stats["trading_days"]))
        self.card_best_day.set_value(
            fmt_money(month_stats["best_day"]["day_pnl"]) if month_stats["best_day"] is not None else "-",
            color=FG_POS,
        )
        self.card_worst_day.set_value(
            fmt_money(month_stats["worst_day"]["day_pnl"]) if month_stats["worst_day"] is not None else "-",
            color=FG_NEG,
        )

        self._render_month_summary_text(year, month, month_stats)
        self._render_calendar_grid(year, month)

    def _render_month_summary_text(self, year: int, month: int, month_stats: Dict[str, object]):
        self.month_summary_text.delete("1.0", "end")

        best_day = month_stats["best_day"]
        worst_day = month_stats["worst_day"]

        lines = [
            f"Monthly Net PnL : {fmt_money(month_stats['month_pnl'])}",
            f"Trading Days    : {month_stats['trading_days']}",
            f"Win Days        : {month_stats['win_days']}",
            f"Loss Days       : {month_stats['loss_days']}",
            f"Avg Daily PnL   : {fmt_money(month_stats['avg_day'])}",
            "",
        ]

        if best_day is not None:
            lines.append(
                f"Best Day        : {best_day['date'].strftime('%Y-%m-%d')} | "
                f"{fmt_money(best_day['day_pnl'])} | {int(best_day['trade_count'])} tr"
            )
        else:
            lines.append("Best Day        : -")

        if worst_day is not None:
            lines.append(
                f"Worst Day       : {worst_day['date'].strftime('%Y-%m-%d')} | "
                f"{fmt_money(worst_day['day_pnl'])} | {int(worst_day['trade_count'])} tr"
            )
        else:
            lines.append("Worst Day       : -")

        lines.append("")
        lines.append("Weekly Summary")
        lines.append("--------------")

        weekly = build_weekly_month_summary(self.daily_summary, year, month)
        if weekly.empty:
            lines.append("No data")
        else:
            for _, row in weekly.iterrows():
                lines.append(
                    f"{row['week_label']} | {fmt_money(row['week_pnl'])} | "
                    f"{int(row['trading_days'])} d | {int(row['trade_count'])} tr"
                )

        self.month_summary_text.insert("1.0", "\n".join(lines))

    def _render_calendar_grid(self, year: int, month: int):
        cal = calendar.Calendar(firstweekday=0)
        weeks = cal.monthdatescalendar(year, month)

        day_map: Dict[pd.Timestamp, dict] = {}
        if not self.daily_summary.empty:
            month_daily = self.daily_summary[
                (self.daily_summary["date"].dt.year == year) &
                (self.daily_summary["date"].dt.month == month)
            ].copy()
            for _, row in month_daily.iterrows():
                day_map[pd.Timestamp(row["date"]).normalize()] = row.to_dict()

        cells = self.day_cells
        for idx, week in enumerate(weeks):
            for jdx, dt_ in enumerate(week):
                cell_index = idx * 7 + jdx
                if cell_index >= len(cells):
                    continue
                cell = cells[cell_index]

                ts = pd.Timestamp(dt_, tz="UTC")
                in_month = (dt_.month == month)

                if not in_month:
                    cell.set_empty(dt_.day, False)
                    continue

                row = day_map.get(ts.normalize())
                if row is None:
                    cell.set_empty(dt_.day, True)
                else:
                    cell.set_day(
                        day_num=dt_.day,
                        day_date=ts,
                        day_pnl=float(row["day_pnl"]),
                        trade_count=int(row["trade_count"]),
                        payload=row,
                    )

        used = len(weeks) * 7
        for idx in range(used, len(cells)):
            cells[idx].set_empty(0, False)

    # ========================================================
    # EQUITY CURVE TAB
    # ========================================================

    def _render_empty_equity_chart(self):
        if self.equity_ax is None or self.dd_ax is None or self.equity_canvas is None:
            return

        self.equity_ax.clear()
        self.dd_ax.clear()

        for ax in [self.equity_ax, self.dd_ax]:
            ax.set_facecolor(BG_PANEL_2)
            ax.tick_params(colors=FG_MUTED)
            for spine in ax.spines.values():
                spine.set_color(BORDER)

        self.equity_ax.text(
            0.5, 0.5, "No equity data",
            color=FG_MUTED, ha="center", va="center", transform=self.equity_ax.transAxes
        )
        self.dd_ax.text(
            0.5, 0.5, "No drawdown data",
            color=FG_MUTED, ha="center", va="center", transform=self.dd_ax.transAxes
        )

        self.equity_canvas.draw_idle()

    def _render_equity_chart(self):
        curve = pd.DataFrame()
        if self.current_bundle is not None:
            curve = build_equity_curve(self.current_bundle.trades, START_EQUITY_DEFAULT)

        if curve.empty:
            self.card_eq_last.set_value("-")
            self.card_eq_peak.set_value("-")
            self.card_eq_dd.set_value("-")
            self.card_eq_points.set_value("-")
            self.equity_info_var.set("No equity data")
            self._render_empty_equity_chart()
            return

        current_equity = float(curve["equity"].iloc[-1])
        peak_equity = float(curve["equity"].max())
        current_dd = float(curve["drawdown_money"].iloc[-1])
        curve_points = int(len(curve))

        self.card_eq_last.set_value(fmt_money(current_equity), color=posneg_color(current_equity - START_EQUITY_DEFAULT))
        self.card_eq_peak.set_value(fmt_money(peak_equity), color=FG_POS)
        self.card_eq_dd.set_value(fmt_money(current_dd), color=FG_NEG if current_dd < 0 else FG_MAIN)
        self.card_eq_points.set_value(str(curve_points))

        first_dt = curve["close_time_utc"].min()
        last_dt = curve["close_time_utc"].max()
        self.equity_info_var.set(
            f"{self.selected_account} | {self.selected_strategy} | "
            f"{first_dt.strftime('%Y-%m-%d')} -> {last_dt.strftime('%Y-%m-%d')}"
        )

        self.equity_ax.clear()
        self.dd_ax.clear()

        self.equity_figure.patch.set_facecolor(BG_PANEL_2)

        for ax in [self.equity_ax, self.dd_ax]:
            ax.set_facecolor(BG_PANEL_2)
            ax.tick_params(colors=FG_MUTED, labelsize=8)
            for spine in ax.spines.values():
                spine.set_color(BORDER)
            ax.grid(True, alpha=0.15)

        self.equity_ax.plot(
            curve["close_time_utc"],
            curve["equity"],
            color="#4DA3FF",
            linewidth=2.0,
        )
        self.equity_ax.set_title("Equity Curve", color=FG_MAIN, fontsize=11)
        self.equity_ax.set_ylabel("Equity", color=FG_MUTED)

        self.dd_ax.fill_between(
            curve["close_time_utc"],
            curve["drawdown_money"],
            0.0,
            color="#7A8798",
            alpha=0.35,
        )
        self.dd_ax.plot(
            curve["close_time_utc"],
            curve["drawdown_money"],
            color="#AAB4C0",
            linewidth=1.0,
        )
        self.dd_ax.set_title("Drawdown (Money)", color=FG_MAIN, fontsize=11)
        self.dd_ax.set_ylabel("DD", color=FG_MUTED)
        self.dd_ax.set_xlabel("Time", color=FG_MUTED)

        self.equity_figure.tight_layout()
        self.equity_canvas.draw_idle()

    def _open_large_equity_window(self):
        curve = pd.DataFrame()
        if self.current_bundle is not None:
            curve = build_equity_curve(self.current_bundle.trades, START_EQUITY_DEFAULT)

        win = tk.Toplevel(self)
        win.title(f"Large Equity Curve | {self.selected_account} | {self.selected_strategy}")
        win.geometry("1400x850")
        win.minsize(1100, 700)
        win.configure(bg=BG_MAIN)

        header = tk.Frame(win, bg=BG_HEADER, highlightbackground=BORDER, highlightthickness=1, height=52)
        header.pack(fill="x", padx=10, pady=10)
        header.pack_propagate(False)

        tk.Label(
            header,
            text="LARGE EQUITY CURVE",
            font=FONT_TITLE,
            bg=BG_HEADER,
            fg=FG_MAIN,
        ).pack(side="left", padx=12)

        tk.Label(
            header,
            text=f"{self.selected_account} | {self.selected_strategy}",
            font=("Segoe UI", 9),
            bg=BG_HEADER,
            fg=FG_MUTED,
        ).pack(side="right", padx=12)

        chart_frame = tk.Frame(win, bg=BG_PANEL_2, highlightbackground=BORDER, highlightthickness=1)
        chart_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        fig = Figure(figsize=(12, 8), dpi=100, facecolor=BG_PANEL_2)
        ax1 = fig.add_subplot(211)
        ax2 = fig.add_subplot(212, sharex=ax1)

        for ax in [ax1, ax2]:
            ax.set_facecolor(BG_PANEL_2)
            ax.tick_params(colors=FG_MUTED, labelsize=9)
            for spine in ax.spines.values():
                spine.set_color(BORDER)
            ax.grid(True, alpha=0.15)

        if curve.empty:
            ax1.text(0.5, 0.5, "No equity data", color=FG_MUTED, ha="center", va="center", transform=ax1.transAxes)
            ax2.text(0.5, 0.5, "No drawdown data", color=FG_MUTED, ha="center", va="center", transform=ax2.transAxes)
        else:
            ax1.plot(curve["close_time_utc"], curve["equity"], color="#4DA3FF", linewidth=2.2)
            ax1.set_title("Equity Curve", color=FG_MAIN, fontsize=12)
            ax1.set_ylabel("Equity", color=FG_MUTED)

            ax2.fill_between(curve["close_time_utc"], curve["drawdown_money"], 0.0, color="#7A8798", alpha=0.35)
            ax2.plot(curve["close_time_utc"], curve["drawdown_money"], color="#AAB4C0", linewidth=1.0)
            ax2.set_title("Drawdown (Money)", color=FG_MAIN, fontsize=12)
            ax2.set_ylabel("DD", color=FG_MUTED)
            ax2.set_xlabel("Time", color=FG_MUTED)

        fig.tight_layout()

        canvas = FigureCanvasTkAgg(fig, master=chart_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

    # ========================================================
    # DAY INSIGHTS
    # ========================================================

    def _get_trades_for_day(self, day_ts: pd.Timestamp) -> pd.DataFrame:
        if self.current_bundle is None or self.current_bundle.trades.empty:
            return pd.DataFrame()

        d = self.current_bundle.trades.copy()
        if "close_time_utc" not in d.columns:
            return pd.DataFrame()

        d["close_time_utc"] = pd.to_datetime(d["close_time_utc"], errors="coerce", utc=True)
        d = d.dropna(subset=["close_time_utc"]).copy()
        if d.empty:
            return pd.DataFrame()

        if "net_sum" in d.columns:
            d["net_sum"] = pd.to_numeric(d["net_sum"], errors="coerce").fillna(0.0)

        day_start = pd.Timestamp(day_ts).tz_convert("UTC").normalize()
        day_end = day_start + pd.Timedelta(days=1)

        out = d[(d["close_time_utc"] >= day_start) & (d["close_time_utc"] < day_end)].copy()
        out = out.sort_values("close_time_utc", ascending=False).reset_index(drop=True)
        return out

    def _build_day_strategy_breakdown(self, day_trades: pd.DataFrame) -> pd.DataFrame:
        if day_trades.empty:
            return pd.DataFrame(columns=["strategy_id", "net_pnl", "trades", "wins", "losses", "avg_trade"])

        d = day_trades.copy()
        if "strategy_id" not in d.columns:
            d["strategy_id"] = self.selected_strategy or ""

        d["net_sum"] = pd.to_numeric(d["net_sum"], errors="coerce").fillna(0.0)

        out = (
            d.groupby("strategy_id", as_index=False)
            .agg(
                net_pnl=("net_sum", "sum"),
                trades=("net_sum", "size"),
                wins=("net_sum", lambda s: int((s > 0).sum())),
                losses=("net_sum", lambda s: int((s < 0).sum())),
                avg_trade=("net_sum", "mean"),
            )
            .sort_values("net_pnl", ascending=False)
            .reset_index(drop=True)
        )
        return out

    def _build_day_symbol_breakdown(self, day_trades: pd.DataFrame) -> pd.DataFrame:
        if day_trades.empty or "symbol" not in day_trades.columns:
            return pd.DataFrame(columns=["symbol", "net_pnl", "trades", "wins", "losses", "avg_trade"])

        d = day_trades.copy()
        d["net_sum"] = pd.to_numeric(d["net_sum"], errors="coerce").fillna(0.0)

        out = (
            d.groupby("symbol", as_index=False)
            .agg(
                net_pnl=("net_sum", "sum"),
                trades=("net_sum", "size"),
                wins=("net_sum", lambda s: int((s > 0).sum())),
                losses=("net_sum", lambda s: int((s < 0).sum())),
                avg_trade=("net_sum", "mean"),
            )
            .sort_values("net_pnl", ascending=False)
            .reset_index(drop=True)
        )
        return out

    def _on_day_cell_click(self, day_ts: pd.Timestamp, payload: Optional[dict] = None):
        day_trades = self._get_trades_for_day(day_ts)
        self._open_day_detail_window(day_ts, day_trades)

    def _open_day_detail_window(self, day_ts: pd.Timestamp, day_trades: pd.DataFrame):
        win = tk.Toplevel(self)
        win.title(f"Day Insights | {day_ts.strftime('%Y-%m-%d')}")
        win.geometry("1320x760")
        win.minsize(1100, 680)
        win.configure(bg=BG_MAIN)

        header = tk.Frame(win, bg=BG_HEADER, highlightbackground=BORDER, highlightthickness=1, height=52)
        header.pack(fill="x", padx=10, pady=10)
        header.pack_propagate(False)

        tk.Label(
            header,
            text=f"DAY INSIGHTS | {day_ts.strftime('%Y-%m-%d')}",
            font=FONT_TITLE,
            bg=BG_HEADER,
            fg=FG_MAIN,
        ).pack(side="left", padx=12)

        scope_txt = f"{self.selected_account} | {self.selected_strategy}"
        tk.Label(
            header,
            text=scope_txt,
            font=("Segoe UI", 9),
            bg=BG_HEADER,
            fg=FG_MUTED,
        ).pack(side="right", padx=12)

        if day_trades.empty:
            box = tk.Frame(win, bg=BG_PANEL, highlightbackground=BORDER, highlightthickness=1)
            box.pack(fill="both", expand=True, padx=10, pady=(0, 10))

            tk.Label(
                box,
                text="Keine Trades an diesem Tag.",
                font=FONT_SECTION,
                bg=BG_PANEL,
                fg=FG_MUTED,
            ).pack(anchor="center", expand=True)
            return

        d = day_trades.copy()
        d["net_sum"] = pd.to_numeric(d["net_sum"], errors="coerce").fillna(0.0)

        day_pnl = float(d["net_sum"].sum())
        trade_count = int(len(d))
        wins = int((d["net_sum"] > 0).sum())
        losses = int((d["net_sum"] < 0).sum())
        avg_trade = float(d["net_sum"].mean()) if trade_count > 0 else 0.0
        best_trade = float(d["net_sum"].max()) if trade_count > 0 else 0.0
        worst_trade = float(d["net_sum"].min()) if trade_count > 0 else 0.0

        kpi_row = tk.Frame(win, bg=BG_MAIN)
        kpi_row.pack(fill="x", padx=10, pady=(0, 10))

        c1 = KpiCard(kpi_row, "Day PnL")
        c1.pack(side="left", fill="x", expand=True, padx=4)
        c1.set_value(fmt_money(day_pnl), color=posneg_color(day_pnl))

        c2 = KpiCard(kpi_row, "Trades")
        c2.pack(side="left", fill="x", expand=True, padx=4)
        c2.set_value(str(trade_count))

        c3 = KpiCard(kpi_row, "Wins / Losses")
        c3.pack(side="left", fill="x", expand=True, padx=4)
        c3.set_value(f"{wins} / {losses}")

        c4 = KpiCard(kpi_row, "Avg Trade")
        c4.pack(side="left", fill="x", expand=True, padx=4)
        c4.set_value(fmt_money(avg_trade), color=posneg_color(avg_trade))

        c5 = KpiCard(kpi_row, "Best Trade")
        c5.pack(side="left", fill="x", expand=True, padx=4)
        c5.set_value(fmt_money(best_trade), color=FG_POS)

        c6 = KpiCard(kpi_row, "Worst Trade")
        c6.pack(side="left", fill="x", expand=True, padx=4)
        c6.set_value(fmt_money(worst_trade), color=FG_NEG)

        main = tk.PanedWindow(win, orient="horizontal", bg=BG_MAIN, sashwidth=6)
        main.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        left = tk.Frame(main, bg=BG_PANEL, highlightbackground=BORDER, highlightthickness=1)
        right = tk.Frame(main, bg=BG_PANEL, highlightbackground=BORDER, highlightthickness=1)
        main.add(left, minsize=360)
        main.add(right, minsize=760)

        tk.Label(
            left,
            text="Strategy Breakdown",
            font=FONT_SECTION,
            bg=BG_PANEL,
            fg=FG_MAIN,
        ).pack(anchor="w", padx=10, pady=(10, 8))

        strat_cols = ("strategy", "net_pnl", "trades", "wins", "losses", "avg_trade")
        strat_tree = ttk.Treeview(left, columns=strat_cols, show="headings", height=10)
        strat_tree.pack(fill="x", padx=10, pady=(0, 10))

        strat_widths = {
            "strategy": 160,
            "net_pnl": 90,
            "trades": 60,
            "wins": 60,
            "losses": 60,
            "avg_trade": 90,
        }
        for col in strat_cols:
            strat_tree.heading(col, text=col)
            strat_tree.column(col, width=strat_widths[col], anchor="w")

        strat_df = self._build_day_strategy_breakdown(day_trades)
        for _, row in strat_df.iterrows():
            strat_tree.insert(
                "",
                "end",
                values=(
                    row.get("strategy_id", ""),
                    fmt_money(row.get("net_pnl")),
                    int(row.get("trades")) if pd.notna(row.get("trades")) else "",
                    int(row.get("wins")) if pd.notna(row.get("wins")) else "",
                    int(row.get("losses")) if pd.notna(row.get("losses")) else "",
                    fmt_money(row.get("avg_trade")),
                ),
            )

        tk.Label(
            left,
            text="Symbol Breakdown",
            font=FONT_SECTION,
            bg=BG_PANEL,
            fg=FG_MAIN,
        ).pack(anchor="w", padx=10, pady=(4, 8))

        symbol_cols = ("symbol", "net_pnl", "trades", "wins", "losses", "avg_trade")
        symbol_tree = ttk.Treeview(left, columns=symbol_cols, show="headings", height=10)
        symbol_tree.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        symbol_widths = {
            "symbol": 100,
            "net_pnl": 90,
            "trades": 60,
            "wins": 60,
            "losses": 60,
            "avg_trade": 90,
        }
        for col in symbol_cols:
            symbol_tree.heading(col, text=col)
            symbol_tree.column(col, width=symbol_widths[col], anchor="w")

        symbol_df = self._build_day_symbol_breakdown(day_trades)
        for _, row in symbol_df.iterrows():
            symbol_tree.insert(
                "",
                "end",
                values=(
                    row.get("symbol", ""),
                    fmt_money(row.get("net_pnl")),
                    int(row.get("trades")) if pd.notna(row.get("trades")) else "",
                    int(row.get("wins")) if pd.notna(row.get("wins")) else "",
                    int(row.get("losses")) if pd.notna(row.get("losses")) else "",
                    fmt_money(row.get("avg_trade")),
                ),
            )

        tk.Label(
            right,
            text="Day Trade Ledger",
            font=FONT_SECTION,
            bg=BG_PANEL,
            fg=FG_MAIN,
        ).pack(anchor="w", padx=10, pady=(10, 8))

        trade_cols = (
            "close_time_utc",
            "strategy_id",
            "symbol",
            "direction",
            "net_sum",
            "entry_price",
            "exit_price",
            "volume_in",
            "magic",
        )
        day_trade_tree = ttk.Treeview(right, columns=trade_cols, show="headings", height=24)
        day_trade_tree.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        trade_widths = {
            "close_time_utc": 170,
            "strategy_id": 120,
            "symbol": 90,
            "direction": 70,
            "net_sum": 90,
            "entry_price": 90,
            "exit_price": 90,
            "volume_in": 80,
            "magic": 80,
        }
        for col in trade_cols:
            day_trade_tree.heading(col, text=col)
            day_trade_tree.column(col, width=trade_widths[col], anchor="w")

        day_trades_show = day_trades.head(MAX_DAY_TRADES_IN_TABLE).copy()
        if "strategy_id" not in day_trades_show.columns:
            day_trades_show["strategy_id"] = self.selected_strategy or ""

        for _, row in day_trades_show.iterrows():
            day_trade_tree.insert(
                "",
                "end",
                values=(
                    str(row.get("close_time_utc", "")),
                    row.get("strategy_id", ""),
                    row.get("symbol", ""),
                    row.get("direction", ""),
                    fmt_money(row.get("net_sum")),
                    fmt_float(row.get("entry_price"), 5) if pd.notna(row.get("entry_price")) else "",
                    fmt_float(row.get("exit_price"), 5) if pd.notna(row.get("exit_price")) else "",
                    fmt_float(row.get("volume_in"), 2) if pd.notna(row.get("volume_in")) else "",
                    row.get("magic", ""),
                ),
            )

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
    if not LIVE_PERF_ROOT.exists():
        raise RuntimeError(f"Performance-Root nicht gefunden: {LIVE_PERF_ROOT}")

    repo = StrategyDataRepository(LIVE_PERF_ROOT)
    app = StrategyPerformanceDashboard(repo)
    app.mainloop()


if __name__ == "__main__":
    main()