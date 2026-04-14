# -*- coding: utf-8 -*-
"""
Standalone KPI Report Dashboard (new system)

Reads per-strategy outputs from kpi_report.py:

Reports/<STRATEGY>/
  - kpis.csv
  - weekly_performance.csv
  - monthly_performance.csv
  - rolling_performance.csv   (NEW; expected columns like:
        window_w, pnl_sum, hit_rate, sharpe_annual_52, pnl_mean, pnl_std
    )

Changes:
  1) Strategies list is ORDERED by "rolling score" (best -> worst).
  2) Rolling table is ORDERED best -> worst (NOT by window).
  3) Uses rolling_performance.csv if present; fallback computes from weekly_performance.csv.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, Optional, List, Tuple

import numpy as np
import pandas as pd

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget,
    QHBoxLayout, QVBoxLayout, QLabel,
    QListWidget, QListWidgetItem, QFrame,
    QTableWidget, QTableWidgetItem, QTabWidget
)
from PyQt6.QtCore import Qt
from pyqtgraph.Qt import QtGui, QtCore
import pyqtgraph as pg


# ----------------------------
# Paths
# ----------------------------

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[0]  # adjust if you place file elsewhere
REPORTS_DIR = PROJECT_ROOT / "Reports"

pg.setConfigOption("background", "#000000")
pg.setConfigOption("foreground", "#f0f0f0")


def discover_strategies(reports_dir: Path = REPORTS_DIR) -> Dict[str, Path]:
    out: Dict[str, Path] = {}
    if not reports_dir.exists():
        return out

    for sub in reports_dir.iterdir():
        if not sub.is_dir():
            continue
        kpis = sub / "kpis.csv"
        wk = sub / "weekly_performance.csv"
        mo = sub / "monthly_performance.csv"
        rol = sub / "rolling_performance.csv"
        if kpis.exists() or wk.exists() or mo.exists() or rol.exists():
            out[sub.name] = sub

    return dict(sorted(out.items()))


# ----------------------------
# Loaders
# ----------------------------

def load_kpis_csv(strategy_dir: Path) -> pd.Series:
    p = strategy_dir / "kpis.csv"
    if not p.exists():
        return pd.Series(dtype="float64")
    try:
        s = pd.read_csv(p).set_index("kpi")["value"]
        return s
    except Exception:
        return pd.Series(dtype="float64")


def _load_perf_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

    # exporter uses index_label="date"
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        df = df.set_index("date")

    for c in ["nav", "pnl_money", "cum_pnl_money"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).astype(float)

    return df


def load_weekly_performance(strategy_dir: Path) -> pd.DataFrame:
    return _load_perf_csv(strategy_dir / "weekly_performance.csv")


def load_monthly_performance(strategy_dir: Path) -> pd.DataFrame:
    return _load_perf_csv(strategy_dir / "monthly_performance.csv")


def load_rolling_performance(strategy_dir: Path) -> pd.DataFrame:
    """
    Expected: rolling_performance.csv with at least:
      window_w, pnl_sum, hit_rate, sharpe_annual_52, pnl_mean, pnl_std
    """
    p = strategy_dir / "rolling_performance.csv"
    if not p.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(p)
    except Exception:
        return pd.DataFrame()

    # normalize column names
    df.columns = [c.strip() for c in df.columns]

    # accept index if exported with index_label
    if "window_w" not in df.columns:
        # try common alternatives
        for cand in ["window", "Window(w)", "Window", "w"]:
            if cand in df.columns:
                df = df.rename(columns={cand: "window_w"})
                break

    # coerce types
    for c in ["window_w", "pnl_sum", "hit_rate", "sharpe_annual_52", "pnl_mean", "pnl_std"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["window_w"]) if "window_w" in df.columns else df
    if "window_w" in df.columns:
        df["window_w"] = df["window_w"].astype(int)

    return df


# ----------------------------
# Rolling KPI computation (weekly) - fallback
# ----------------------------

def compute_weekly_rollups(weekly_df: pd.DataFrame, windows: List[int] = [3, 6, 12]) -> pd.DataFrame:
    """
    Returns a dataframe indexed by window with:
      window_w, pnl_sum, hit_rate, sharpe_annual_52, pnl_mean, pnl_std
    Sharpe computed from weekly returns:
      ret_w = pnl_money / nav_prev
    """
    if weekly_df.empty or "pnl_money" not in weekly_df.columns:
        return pd.DataFrame()

    df = weekly_df.copy().sort_index()
    pnl = df["pnl_money"].astype(float)

    nav_prev = df["nav"].shift(1) if "nav" in df.columns else pd.Series(index=df.index, dtype=float)
    ret = (pnl / nav_prev).replace([np.inf, -np.inf], np.nan)

    rows = []
    for w in windows:
        tail_pnl = pnl.tail(w)
        if len(tail_pnl) == 0:
            continue

        hit = float((tail_pnl > 0).mean()) if len(tail_pnl) else np.nan
        pnl_sum = float(tail_pnl.sum())
        pnl_mean = float(tail_pnl.mean())
        pnl_std = float(tail_pnl.std(ddof=1)) if len(tail_pnl) > 1 else np.nan

        tail_ret = ret.tail(w).dropna()
        if len(tail_ret) > 1 and float(tail_ret.std(ddof=1)) > 0:
            sharpe = float(tail_ret.mean() / tail_ret.std(ddof=1) * np.sqrt(52.0))
        else:
            sharpe = np.nan

        rows.append({
            "window_w": int(w),
            "pnl_sum": pnl_sum,
            "hit_rate": hit,
            "sharpe_annual_52": sharpe,
            "pnl_mean": pnl_mean,
            "pnl_std": pnl_std,
        })

    out = pd.DataFrame(rows)
    return out


# ----------------------------
# Ranking logic (strategies + rolling rows)
# ----------------------------

def _coerce_roll_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    # ensure columns
    need = ["window_w", "pnl_sum", "hit_rate", "sharpe_annual_52", "pnl_mean", "pnl_std"]
    for c in need:
        if c not in out.columns:
            out[c] = np.nan
    out["window_w"] = pd.to_numeric(out["window_w"], errors="coerce").astype("Int64")
    for c in ["pnl_sum", "hit_rate", "sharpe_annual_52", "pnl_mean", "pnl_std"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.dropna(subset=["window_w"])
    out["window_w"] = out["window_w"].astype(int)
    return out


def rank_rolling_rows_best_to_worst(roll_df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort best -> worst.
    Primary: sharpe_annual_52 (desc)
    Secondary: pnl_sum (desc)
    Tertiary: hit_rate (desc)
    """
    df = _coerce_roll_df(roll_df)
    if df.empty:
        return df

    df = df.sort_values(
        by=["sharpe_annual_52", "pnl_sum", "hit_rate", "window_w"],
        ascending=[False, False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)

    df.insert(0, "rank", np.arange(1, len(df) + 1))
    return df


def strategy_rolling_score(roll_df: pd.DataFrame) -> float:
    """
    One scalar score per strategy for ordering the left list.

    Rule:
      - Prefer 12w row. If missing: take best available by sharpe desc.
      - Score = sharpe_annual_52
      - tie-breaker uses pnl_sum (small weight)
    """
    df = _coerce_roll_df(roll_df)
    if df.empty:
        return -1e18

    if (df["window_w"] == 12).any():
        row = df.loc[df["window_w"] == 12].iloc[-1]
    else:
        row = df.sort_values(
            by=["sharpe_annual_52", "pnl_sum", "hit_rate"],
            ascending=[False, False, False],
            kind="mergesort",
        ).iloc[0]

    sh = float(row.get("sharpe_annual_52", np.nan))
    pn = float(row.get("pnl_sum", np.nan))
    if not np.isfinite(sh):
        sh = -1e9
    if not np.isfinite(pn):
        pn = 0.0

    return sh + 1e-6 * pn


# ----------------------------
# Perf tables (pivot)
# ----------------------------

MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def perf_table_monthly_from_weekly(weekly_df: pd.DataFrame) -> pd.DataFrame:
    if weekly_df.empty:
        return pd.DataFrame()
    df = weekly_df.copy()
    idx = df.index
    if not isinstance(idx, pd.DatetimeIndex):
        return pd.DataFrame()

    df["year"] = idx.year
    df["month"] = idx.month

    pt = df.pivot_table(index="year", columns="month", values="pnl_money", aggfunc="sum", fill_value=0.0)
    pt = pt.reindex(columns=list(range(1, 13)), fill_value=0.0).sort_index(ascending=False)

    pt["YTD"] = pt.sum(axis=1)
    pt.columns = MONTH_ABBR + ["YTD"]
    return pt


def perf_table_weekly(weekly_df: pd.DataFrame) -> pd.DataFrame:
    if weekly_df.empty:
        return pd.DataFrame()
    if not isinstance(weekly_df.index, pd.DatetimeIndex):
        return pd.DataFrame()

    df = weekly_df.copy()
    iso = df.index.isocalendar()
    df["year"] = iso["year"].astype(int)
    df["week"] = iso["week"].astype(int)

    pt = df.pivot_table(index="year", columns="week", values="pnl_money", aggfunc="sum", fill_value=0.0)
    weeks = sorted(pt.columns.tolist())
    pt = pt[weeks]

    pt["YTD"] = pt.sum(axis=1)

    pt.columns = [f"W{w:02d}" for w in weeks] + ["YTD"]
    pt = pt.sort_index(ascending=False)
    return pt


# ----------------------------
# Qt helpers
# ----------------------------

def _qitem(text: str, align_right: bool = False) -> QTableWidgetItem:
    it = QTableWidgetItem(text)
    if align_right:
        it.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
    else:
        it.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
    return it


def fill_table_from_df(table: QTableWidget, df: pd.DataFrame, index_name: str = "") -> None:
    table.clear()
    if df is None or df.empty:
        table.setRowCount(0)
        table.setColumnCount(0)
        return

    headers = ([index_name] if index_name else []) + list(df.columns)
    table.setColumnCount(len(headers))
    table.setHorizontalHeaderLabels(headers)
    table.setRowCount(len(df.index))

    for r, idx in enumerate(df.index):
        c0 = 0
        if index_name:
            table.setItem(r, 0, _qitem(str(idx), align_right=False))
            c0 = 1

        for j, col in enumerate(df.columns):
            v = df.iloc[r, j]
            if isinstance(v, (int, np.integer)):
                txt = f"{int(v)}"
            else:
                try:
                    fv = float(v)
                    if np.isnan(fv):
                        txt = "--"
                    else:
                        if col in ["hit_rate"]:
                            txt = f"{fv*100.0:.1f}"
                        else:
                            txt = f"{fv:+.2f}"
                except Exception:
                    txt = str(v)

            item = _qitem(txt, align_right=True)

            # signed color only for numeric, not rank/window
            try:
                fv = float(v)
                if np.isfinite(fv) and col not in ["rank", "window_w"]:
                    if fv > 0:
                        item.setForeground(QtGui.QBrush(QtGui.QColor("#00ff00")))
                    elif fv < 0:
                        item.setForeground(QtGui.QBrush(QtGui.QColor("#ff3300")))
            except Exception:
                pass

            table.setItem(r, j + c0, item)

    table.resizeColumnsToContents()


# ----------------------------
# UI
# ----------------------------

class StrategyView(QWidget):
    def __init__(self, parent=None, reports_dir: Path = REPORTS_DIR):
        super().__init__(parent)
        self.reports_dir = reports_dir

        # load all
        raw_dirs = discover_strategies(self.reports_dir)

        tmp: Dict[str, dict] = {}
        for name, p in raw_dirs.items():
            weekly = load_weekly_performance(p)
            roll = load_rolling_performance(p)
            if roll.empty:
                roll = compute_weekly_rollups(weekly, windows=[3, 6, 12])

            tmp[name] = {
                "name": name,
                "path": p,
                "kpis": load_kpis_csv(p),
                "weekly": weekly,
                "monthly": load_monthly_performance(p),
                "rolling": roll,
            }

        # ORDER strategies by rolling score (best -> worst)
        ordered = sorted(tmp.keys(), key=lambda n: strategy_rolling_score(tmp[n]["rolling"]), reverse=True)
        self.strategies: Dict[str, dict] = {k: tmp[k] for k in ordered}

        main = QHBoxLayout(self)
        main.setContentsMargins(10, 10, 10, 10)
        main.setSpacing(10)

        # left
        left = QFrame()
        left.setObjectName("accountCard")
        ll = QVBoxLayout(left)
        ll.setContentsMargins(8, 8, 8, 8)

        t = QLabel("Strategies (ranked by rolling)")
        t.setStyleSheet("font-size: 14px; font-weight: 600;")
        ll.addWidget(t)

        self.listw = QListWidget()
        for s in self.strategies.keys():
            sc = strategy_rolling_score(self.strategies[s]["rolling"])
            # show score for debug/visibility
            self.listw.addItem(QListWidgetItem(f"{s}   |   score={sc:.3f}"))
        if not self.strategies:
            self.listw.addItem("(no strategies found)")
        ll.addWidget(self.listw)

        main.addWidget(left, 1)

        # right
        right = QFrame()
        right.setObjectName("accountCard")
        rl = QVBoxLayout(right)
        rl.setContentsMargins(8, 8, 8, 8)
        rl.setSpacing(6)

        self.hdr = QLabel("Strategy Detail")
        self.hdr.setStyleSheet("font-size: 16px; font-weight: 600;")
        rl.addWidget(self.hdr)

        self.meta = QLabel("")
        self.meta.setStyleSheet("font-size: 12px; color: #c0c0c0;")
        rl.addWidget(self.meta)

        self.tabs = QTabWidget()
        rl.addWidget(self.tabs, 1)

        # --- Tab Equity
        tab_eq = QWidget()
        eql = QVBoxLayout(tab_eq)
        eql.setContentsMargins(4, 4, 4, 4)
        eql.setSpacing(4)

        self.plot_nav_w = pg.PlotWidget()
        self.plot_nav_w.showGrid(x=True, y=True, alpha=0.2)
        self.plot_nav_w.setLabel("left", "NAV (Weekly)")
        self.plot_nav_w.setLabel("bottom", "Week index")
        self.plot_nav_w.setMinimumHeight(160)
        eql.addWidget(self.plot_nav_w)

        self.plot_nav_m = pg.PlotWidget()
        self.plot_nav_m.showGrid(x=True, y=True, alpha=0.2)
        self.plot_nav_m.setLabel("left", "NAV (Monthly)")
        self.plot_nav_m.setLabel("bottom", "Month index")
        self.plot_nav_m.setMinimumHeight(140)
        eql.addWidget(self.plot_nav_m)

        self.plot_roll = pg.PlotWidget()
        self.plot_roll.showGrid(x=True, y=True, alpha=0.2)
        self.plot_roll.setLabel("left", "Rolling PnL (3/6/12w)")
        self.plot_roll.setLabel("bottom", "Week index")
        self.plot_roll.setMinimumHeight(140)
        eql.addWidget(self.plot_roll)

        self.plot_sharpe = pg.PlotWidget()
        self.plot_sharpe.showGrid(x=True, y=True, alpha=0.2)
        self.plot_sharpe.setLabel("left", "Rolling Sharpe (3/6/12w)")
        self.plot_sharpe.setLabel("bottom", "Week index")
        self.plot_sharpe.setMinimumHeight(140)
        eql.addWidget(self.plot_sharpe)

        self.tabs.addTab(tab_eq, "Equity")

        # --- Tab KPIs
        tab_k = QWidget()
        kl = QVBoxLayout(tab_k)
        kl.setContentsMargins(4, 4, 4, 4)
        kl.setSpacing(6)

        self.table_kpis = QTableWidget()
        self.table_kpis.verticalHeader().setVisible(False)
        self.table_kpis.setShowGrid(False)
        self.table_kpis.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table_kpis.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        self.table_kpis.setMinimumHeight(250)
        kl.addWidget(self.table_kpis, 1)

        lbl = QLabel("Rolling performance (ranked best -> worst)")
        lbl.setStyleSheet("font-size: 12px; font-weight: 600;")
        kl.addWidget(lbl)

        self.table_rollkpis = QTableWidget()
        self.table_rollkpis.verticalHeader().setVisible(False)
        self.table_rollkpis.setShowGrid(False)
        self.table_rollkpis.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table_rollkpis.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        self.table_rollkpis.setMinimumHeight(180)
        kl.addWidget(self.table_rollkpis, 1)

        self.tabs.addTab(tab_k, "KPIs")

        # --- Tab Perf
        tab_p = QWidget()
        pl = QVBoxLayout(tab_p)
        pl.setContentsMargins(4, 4, 4, 4)
        pl.setSpacing(6)

        self.table_month = QTableWidget()
        self.table_month.verticalHeader().setVisible(False)
        self.table_month.setShowGrid(False)
        self.table_month.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table_month.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        self.table_month.setMinimumHeight(140)
        pl.addWidget(self.table_month, 1)

        self.table_week = QTableWidget()
        self.table_week.verticalHeader().setVisible(False)
        self.table_week.setShowGrid(False)
        self.table_week.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table_week.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        self.table_week.setMinimumHeight(160)
        pl.addWidget(self.table_week, 1)

        self.tabs.addTab(tab_p, "Perf Tables")

        main.addWidget(right, 4)

        self.listw.currentRowChanged.connect(self._on_strategy_changed)

        if self.strategies:
            self.listw.setCurrentRow(0)
            self._load_by_index(0)
        else:
            self._show_empty()

    def _show_empty(self):
        self.hdr.setText("Strategy Detail")
        self.meta.setText(f"No data found under:\n{self.reports_dir}")
        for p in [self.plot_nav_w, self.plot_nav_m, self.plot_roll, self.plot_sharpe]:
            p.clear()
        for t in [self.table_kpis, self.table_rollkpis, self.table_month, self.table_week]:
            t.setRowCount(0)
            t.setColumnCount(0)

    def _on_strategy_changed(self, row: int):
        if row < 0 or row >= len(self.strategies):
            return
        self._load_by_index(row)

    def _load_by_index(self, idx: int):
        name = list(self.strategies.keys())[idx]
        rec = self.strategies[name]
        self._update(rec)

    def _update(self, rec: dict):
        name = rec["name"]
        kpis: pd.Series = rec["kpis"]
        weekly: pd.DataFrame = rec["weekly"]
        monthly: pd.DataFrame = rec["monthly"]
        rolling: pd.DataFrame = rec["rolling"]

        self.hdr.setText(f"Strategy: {name}")

        # meta
        def _sf(key: str) -> float:
            try:
                return float(kpis.loc[key]) if (kpis is not None and key in kpis.index) else np.nan
            except Exception:
                return np.nan

        start_eq = _sf("start_equity_money")
        end_eq = _sf("end_equity_money")
        netp = _sf("net_profit_money")
        cagr = _sf("cagr") * 100.0
        sharpe = _sf("sharpe")

        def _fmt(x, f="{:.2f}"):
            return "--" if not np.isfinite(x) else f.format(x)

        score = strategy_rolling_score(rolling)
        self.meta.setText(
            f"StartEq: {_fmt(start_eq)} | EndEq: {_fmt(end_eq)} | Net: {_fmt(netp)} | "
            f"CAGR: {_fmt(cagr)} % | Sharpe: {_fmt(sharpe)} | RollingScore: {_fmt(score, '{:.3f}')}"
        )

        # plots
        self.plot_nav_w.clear()
        if not weekly.empty and "nav" in weekly.columns:
            y = weekly["nav"].values.astype(float)
            x = np.arange(len(y))
            self.plot_nav_w.plot(x, y, pen=pg.mkPen("#00ccff", width=2))

        self.plot_nav_m.clear()
        if not monthly.empty and "nav" in monthly.columns:
            y = monthly["nav"].values.astype(float)
            x = np.arange(len(y))
            self.plot_nav_m.plot(x, y, pen=pg.mkPen("#ffcc00", width=2))

        # rolling pnl + sharpe (computed from weekly series for timeseries plots)
        self.plot_roll.clear()
        self.plot_sharpe.clear()
        if not weekly.empty and "pnl_money" in weekly.columns:
            pnl = weekly["pnl_money"].astype(float)

            for w, color, width in [(3, "#00ff00", 1.8), (6, "#ff9900", 1.6), (12, "#ff33cc", 1.6)]:
                rp = pnl.rolling(w).sum()
                self.plot_roll.plot(np.arange(len(rp)), rp.values, pen=pg.mkPen(color, width=width))

            nav_prev = weekly["nav"].shift(1) if "nav" in weekly.columns else pd.Series(index=weekly.index, dtype=float)
            ret = (pnl / nav_prev).replace([np.inf, -np.inf], np.nan)

            for w, color, width in [(3, "#00ff00", 1.8), (6, "#ff9900", 1.6), (12, "#ff33cc", 1.6)]:
                mu = ret.rolling(w).mean()
                sd = ret.rolling(w).std(ddof=1)
                rs = (mu / sd) * np.sqrt(52.0)
                self.plot_sharpe.plot(np.arange(len(rs)), rs.values, pen=pg.mkPen(color, width=width))

        # KPI table (raw)
        self._fill_kpis_table(kpis)

        # rolling KPI table (FROM CSV if exists; ranked best -> worst)
        ranked_roll = rank_rolling_rows_best_to_worst(rolling)

        # keep only expected columns & stable order
        if not ranked_roll.empty:
            cols = ["rank", "window_w", "pnl_sum", "hit_rate", "sharpe_annual_52", "pnl_mean", "pnl_std"]
            for c in cols:
                if c not in ranked_roll.columns:
                    ranked_roll[c] = np.nan
            ranked_roll = ranked_roll[cols]

        fill_table_from_df(self.table_rollkpis, ranked_roll, index_name="")

        # perf tables
        mtab = perf_table_monthly_from_weekly(weekly)
        wtab = perf_table_weekly(weekly)
        fill_table_from_df(self.table_month, mtab, index_name="Year")
        fill_table_from_df(self.table_week, wtab, index_name="Year")

    def _fill_kpis_table(self, kpis: pd.Series):
        self.table_kpis.clear()
        self.table_kpis.setColumnCount(2)
        self.table_kpis.setHorizontalHeaderLabels(["KPI", "Value"])

        if kpis is None or kpis.empty:
            self.table_kpis.setRowCount(0)
            return

        items = list(kpis.items())
        self.table_kpis.setRowCount(len(items))

        for r, (k, v) in enumerate(items):
            self.table_kpis.setItem(r, 0, _qitem(str(k), align_right=False))

            txt: str
            try:
                fv = float(v)
                if np.isnan(fv):
                    txt = "--"
                else:
                    if any(s in str(k).lower() for s in ["pct", "rate", "cagr", "ann_", "sharpe", "sortino"]):
                        txt = f"{fv:.4f}"
                    else:
                        txt = f"{fv:.2f}"
            except Exception:
                txt = str(v)

            self.table_kpis.setItem(r, 1, _qitem(txt, align_right=True))

        self.table_kpis.resizeColumnsToContents()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("KPI Report Dashboard")
        self.resize(1500, 950)
        self.setCentralWidget(StrategyView())


def apply_global_style(app: QApplication):
    app.setStyleSheet("""
        QMainWindow { background-color: #000000; }
        QWidget {
            background-color: #000000;
            color: #f0f0f0;
            font-family: Segoe UI, Arial;
        }
        QFrame#accountCard {
            background-color: rgba(10, 10, 10, 210);
            border-radius: 4px;
            border: 1px solid #333333;
        }
        QHeaderView::section {
            background-color: #ff9900;
            color: #000000;
            border: none;
            padding: 4px 6px;
            font-size: 11px;
            font-weight: 600;
        }
        QTableWidget {
            background-color: #000000;
            border: none;
            gridline-color: #333333;
            font-size: 11px;
        }
    """)


def main():
    app = QApplication(sys.argv)
    apply_global_style(app)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
