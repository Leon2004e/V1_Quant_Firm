# -*- coding: utf-8 -*-
"""
3.Control_Panel/Pdf_Generator/Pdf_Generator.py

Zweck:
- Professioneller account-zentrierter PDF-Report-Generator
- Hauptreport pro Account mit:
    * Executive Summary
    * Account KPIs
    * Equity / Drawdown
    * Monthly Performance
    * Strategy Attribution
    * Symbol / Direction Breakdown
    * Detailseiten je Strategie
- Datenquelle:
    1.Data_Center/Data/Strategy_Data/Live_Trades_Data/Strategy_Live_Performance/account_*/strategies/*/trades.db

Erwartete Tabellen in trades.db:
- trades
- kpis               (optional, wird notfalls aus Trades berechnet)
- weekly_performance (optional)
- monthly_performance(optional)

Output:
- 1.Data_Center/Data/Strategy_Data/Client_Reports/account_*/<account>_master_report.pdf
"""

from __future__ import annotations

import math
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


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

REPORT_ROOT = (
    PROJECT_ROOT
    / "1.Data_Center"
    / "Data"
    / "Strategy_Data"
    / "Client_Reports"
)

STRATEGY_DB_FILENAME = "trades.db"
ACCOUNT_OVERALL_LABEL = "account_overall"


# ============================================================
# CONFIG
# ============================================================

START_EQUITY_DEFAULT = 100000.0
MAX_RECENT_TRADES = 25
MAX_MONTHS_TABLE = 18
MAX_SYMBOL_ROWS = 12
MAX_STRATEGIES_ATTRIBUTION = 20


# ============================================================
# PDF THEME - CORPORATE CLEAN
# ============================================================

CLR_PAGE_BG = colors.HexColor("#F8FAFC")
CLR_WHITE = colors.white

CLR_NAVY = colors.HexColor("#0F172A")
CLR_SLATE = colors.HexColor("#334155")
CLR_MUTED = colors.HexColor("#64748B")

CLR_BLUE = colors.HexColor("#2563EB")
CLR_BLUE_SOFT = colors.HexColor("#DBEAFE")

CLR_BORDER = colors.HexColor("#E2E8F0")
CLR_GRID = colors.HexColor("#CBD5E1")
CLR_ROW_ALT = colors.HexColor("#F8FAFC")

CLR_GREEN = colors.HexColor("#15803D")
CLR_RED = colors.HexColor("#B91C1C")


# ============================================================
# HELPERS
# ============================================================

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def safe_name(x: object) -> str:
    s = str(x).strip()
    for ch in '<>:"/\\|?*':
        s = s.replace(ch, "_")
    return s[:200] if len(s) > 200 else s


def fmt_money(x: object) -> str:
    try:
        v = float(x)
    except Exception:
        return "-"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:,.2f}"


def fmt_num(x: object, digits: int = 2) -> str:
    try:
        return f"{float(x):,.{digits}f}"
    except Exception:
        return "-"


def fmt_pct_ratio(x: object, digits: int = 2) -> str:
    try:
        return f"{float(x) * 100:.{digits}f}%"
    except Exception:
        return "-"


def fmt_dt(x: object) -> str:
    try:
        ts = pd.to_datetime(x, utc=True)
        if pd.isna(ts):
            return "-"
        return ts.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return "-"


def safe_div(a: float, b: float) -> Optional[float]:
    try:
        if b is None or float(b) == 0:
            return None
        return float(a) / float(b)
    except Exception:
        return None


def safe_read_table(db_path: Path, table_name: str) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame()

    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(db_path)
        return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    except Exception:
        return pd.DataFrame()
    finally:
        if conn is not None:
            conn.close()


def parse_trade_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    d = df.copy()

    for col in ["open_time_utc", "close_time_utc"]:
        if col in d.columns:
            d[col] = pd.to_datetime(d[col], errors="coerce", utc=True)

    numeric_cols = [
        "entry_price",
        "exit_price",
        "price_delta",
        "volume_in",
        "volume_out",
        "profit_sum",
        "swap_sum",
        "commission_sum",
        "net_sum",
        "magic",
        "close_ticket",
    ]
    for col in numeric_cols:
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce")

    if "direction" in d.columns:
        d["direction"] = d["direction"].astype(str).str.upper().str.strip()

    if "symbol" in d.columns:
        d["symbol"] = d["symbol"].astype(str).str.strip()

    if "strategy_id" in d.columns:
        d["strategy_id"] = d["strategy_id"].astype(str).str.strip()

    if "open_time_utc" in d.columns and "close_time_utc" in d.columns:
        d["hold_minutes"] = (
            (d["close_time_utc"] - d["open_time_utc"]).dt.total_seconds() / 60.0
        )

    return d


def parse_perf_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    d = df.copy()
    if "date" in d.columns:
        d["date"] = pd.to_datetime(d["date"], errors="coerce", utc=True)

    for col in ["pnl_money", "cum_pnl_money", "nav"]:
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce")

    return d


def parse_kpi_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    d = df.copy()
    for col in d.columns:
        d[col] = pd.to_numeric(d[col], errors="ignore")
    return d


def streak_lengths(series: pd.Series) -> Tuple[int, int]:
    longest_win = 0
    longest_loss = 0
    cur_win = 0
    cur_loss = 0

    for v in series.fillna(0.0):
        if v > 0:
            cur_win += 1
            cur_loss = 0
        elif v < 0:
            cur_loss += 1
            cur_win = 0
        else:
            cur_win = 0
            cur_loss = 0

        longest_win = max(longest_win, cur_win)
        longest_loss = max(longest_loss, cur_loss)

    return longest_win, longest_loss


# ============================================================
# DATA MODELS
# ============================================================

@dataclass
class StrategyBundle:
    name: str
    trades: pd.DataFrame
    kpis: pd.DataFrame
    weekly: pd.DataFrame
    monthly: pd.DataFrame


@dataclass
class AccountBundle:
    account_name: str
    overall: StrategyBundle
    strategies: List[StrategyBundle]


# ============================================================
# DATA ACCESS
# ============================================================

def list_account_dirs(root: Path) -> List[Path]:
    if not root.exists():
        return []
    return sorted(
        [p for p in root.iterdir() if p.is_dir() and p.name.startswith("account_")],
        key=lambda x: x.name,
    )


def list_strategy_dirs(account_dir: Path) -> List[Path]:
    strategies_dir = account_dir / "strategies"
    if not strategies_dir.exists():
        return []
    return sorted([p for p in strategies_dir.iterdir() if p.is_dir()], key=lambda x: x.name)


def load_strategy_bundle(strategy_dir: Path, start_equity: float = START_EQUITY_DEFAULT) -> StrategyBundle:
    db_path = strategy_dir / STRATEGY_DB_FILENAME

    trades = parse_trade_df(safe_read_table(db_path, "trades"))
    kpis_db = parse_kpi_df(safe_read_table(db_path, "kpis"))
    weekly_db = parse_perf_df(safe_read_table(db_path, "weekly_performance"))
    monthly_db = parse_perf_df(safe_read_table(db_path, "monthly_performance"))

    kpis = compute_kpis_from_trades(trades, start_equity=start_equity)
    if kpis.empty and not kpis_db.empty:
        kpis = kpis_db

    weekly = weekly_db if not weekly_db.empty else compute_perf_from_trades(trades, "W-FRI", start_equity)
    monthly = monthly_db if not monthly_db.empty else compute_perf_from_trades(trades, "ME", start_equity)

    if "strategy_id" not in trades.columns and not trades.empty:
        trades = trades.copy()
        trades["strategy_id"] = strategy_dir.name

    return StrategyBundle(
        name=strategy_dir.name,
        trades=trades,
        kpis=kpis,
        weekly=weekly,
        monthly=monthly,
    )


def build_account_overall_bundle(account_dir: Path, start_equity: float = START_EQUITY_DEFAULT) -> StrategyBundle:
    trades_parts: List[pd.DataFrame] = []

    for sdir in list_strategy_dirs(account_dir):
        bundle = load_strategy_bundle(sdir, start_equity=start_equity)
        if bundle.trades.empty:
            continue

        t = bundle.trades.copy()
        if "strategy_id" not in t.columns:
            t["strategy_id"] = sdir.name
        trades_parts.append(t)

    if not trades_parts:
        return StrategyBundle(
            name=ACCOUNT_OVERALL_LABEL,
            trades=pd.DataFrame(),
            kpis=pd.DataFrame(),
            weekly=pd.DataFrame(),
            monthly=pd.DataFrame(),
        )

    trades = pd.concat(trades_parts, ignore_index=True)

    if "close_ticket" in trades.columns:
        trades = trades.sort_values(["close_time_utc", "close_ticket"], ascending=[False, False])
        trades = trades.drop_duplicates(subset=["close_ticket"], keep="first")
    elif "close_time_utc" in trades.columns:
        trades = trades.sort_values("close_time_utc", ascending=False)

    trades = trades.reset_index(drop=True)
    trades_sorted = trades.sort_values("close_time_utc").reset_index(drop=True)

    kpis = compute_kpis_from_trades(trades_sorted, start_equity=start_equity)
    weekly = compute_perf_from_trades(trades_sorted, "W-FRI", start_equity=start_equity)
    monthly = compute_perf_from_trades(trades_sorted, "ME", start_equity=start_equity)

    return StrategyBundle(
        name=ACCOUNT_OVERALL_LABEL,
        trades=trades_sorted,
        kpis=kpis,
        weekly=weekly,
        monthly=monthly,
    )


def build_account_bundle(account_dir: Path, start_equity: float = START_EQUITY_DEFAULT) -> AccountBundle:
    strategies = [load_strategy_bundle(sdir, start_equity=start_equity) for sdir in list_strategy_dirs(account_dir)]
    overall = build_account_overall_bundle(account_dir, start_equity=start_equity)
    return AccountBundle(
        account_name=account_dir.name,
        overall=overall,
        strategies=strategies,
    )


# ============================================================
# KPI / PERFORMANCE CALC
# ============================================================

def compute_perf_from_trades(
    trades: pd.DataFrame,
    freq: str,
    start_equity: float = START_EQUITY_DEFAULT,
) -> pd.DataFrame:
    if trades.empty or "close_time_utc" not in trades.columns or "net_sum" not in trades.columns:
        return pd.DataFrame()

    d = trades.copy()
    d = d.dropna(subset=["close_time_utc"]).copy()
    if d.empty:
        return pd.DataFrame()

    d["net_sum"] = pd.to_numeric(d["net_sum"], errors="coerce").fillna(0.0)
    d = d.sort_values("close_time_utc").set_index("close_time_utc")

    pnl = d["net_sum"].resample(freq).sum().astype(float)

    out = pd.DataFrame({"pnl_money": pnl})
    out["cum_pnl_money"] = out["pnl_money"].cumsum()
    out["nav"] = float(start_equity) + out["cum_pnl_money"]
    out = out.reset_index().rename(columns={"close_time_utc": "date"})
    out["date"] = pd.to_datetime(out["date"], utc=True)

    running_max = out["nav"].cummax()
    out["drawdown_money"] = out["nav"] - running_max
    out["drawdown_pct"] = out["drawdown_money"] / running_max.replace(0, np.nan)

    return out


def compute_kpis_from_trades(trades: pd.DataFrame, start_equity: float = START_EQUITY_DEFAULT) -> pd.DataFrame:
    if trades.empty or "net_sum" not in trades.columns:
        return pd.DataFrame()

    d = trades.copy()

    if "close_time_utc" in d.columns:
        d = d.sort_values("close_time_utc").reset_index(drop=True)

    d["net_sum"] = pd.to_numeric(d["net_sum"], errors="coerce").fillna(0.0)

    n = int(len(d))
    if n == 0:
        return pd.DataFrame()

    wins_df = d[d["net_sum"] > 0].copy()
    losses_df = d[d["net_sum"] < 0].copy()

    wins = int(len(wins_df))
    losses = int(len(losses_df))

    net_pnl = float(d["net_sum"].sum())
    gross_profit = float(wins_df["net_sum"].sum()) if not wins_df.empty else 0.0
    gross_loss = float(losses_df["net_sum"].sum()) if not losses_df.empty else 0.0

    profit_factor = gross_profit / abs(gross_loss) if gross_loss < 0 else None
    win_rate = wins / n if n > 0 else None
    avg_trade = float(d["net_sum"].mean()) if n > 0 else None
    avg_win = float(wins_df["net_sum"].mean()) if wins > 0 else None
    avg_loss = float(losses_df["net_sum"].mean()) if losses > 0 else None

    payoff_ratio = (avg_win / abs(avg_loss)) if (avg_win is not None and avg_loss is not None and avg_loss != 0) else None
    expectancy = (
        (win_rate * avg_win + (1.0 - win_rate) * avg_loss)
        if (win_rate is not None and avg_win is not None and avg_loss is not None)
        else None
    )

    equity = float(start_equity) + d["net_sum"].cumsum()
    running_max = equity.cummax()
    drawdown_abs = equity - running_max
    drawdown_pct = drawdown_abs / running_max.replace(0, np.nan)

    max_dd_abs = float(drawdown_abs.min()) if len(drawdown_abs) else 0.0
    max_dd_pct = float(drawdown_pct.min()) if len(drawdown_pct.dropna()) else None

    total_return_pct = net_pnl / float(start_equity) if start_equity else None
    recovery_factor = (net_pnl / abs(max_dd_abs)) if max_dd_abs < 0 else None
    calmar_ratio = (
        total_return_pct / abs(max_dd_pct)
        if (total_return_pct is not None and max_dd_pct is not None and max_dd_pct < 0)
        else None
    )

    returns = d["net_sum"] / float(start_equity)
    ret_std = returns.std(ddof=1) if len(returns) > 1 else None
    sharpe = (returns.mean() / ret_std) * math.sqrt(len(returns)) if (ret_std is not None and ret_std > 0) else None

    downside = returns[returns < 0]
    downside_std = downside.std(ddof=1) if len(downside) > 1 else None
    sortino = (
        (returns.mean() / downside_std) * math.sqrt(len(returns))
        if (downside_std is not None and downside_std > 0)
        else None
    )

    best_trade = float(d["net_sum"].max()) if not d.empty else None
    worst_trade = float(d["net_sum"].min()) if not d.empty else None

    if "commission_sum" in d.columns:
        total_commission = float(pd.to_numeric(d["commission_sum"], errors="coerce").fillna(0.0).sum())
    else:
        total_commission = 0.0

    if "swap_sum" in d.columns:
        total_swap = float(pd.to_numeric(d["swap_sum"], errors="coerce").fillna(0.0).sum())
    else:
        total_swap = 0.0

    avg_hold_minutes = None
    if "hold_minutes" in d.columns:
        hm = pd.to_numeric(d["hold_minutes"], errors="coerce").dropna()
        if not hm.empty:
            avg_hold_minutes = float(hm.mean())

    longest_win_streak, longest_loss_streak = streak_lengths(d["net_sum"])

    out = {
        "net_pnl": net_pnl,
        "total_return_pct": total_return_pct,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": profit_factor,
        "payoff_ratio": payoff_ratio,
        "expectancy": expectancy,
        "n_trades": n,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "avg_trade": avg_trade,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "max_drawdown_closed": max_dd_abs,
        "max_drawdown_pct": max_dd_pct,
        "recovery_factor": recovery_factor,
        "calmar_ratio": calmar_ratio,
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "commission_total": total_commission,
        "swap_total": total_swap,
        "avg_hold_minutes": avg_hold_minutes,
        "longest_win_streak": longest_win_streak,
        "longest_loss_streak": longest_loss_streak,
    }

    return pd.DataFrame([out])


# ============================================================
# ANALYSIS TABLES
# ============================================================

def build_strategy_attribution_df(account_bundle: AccountBundle, start_equity: float = START_EQUITY_DEFAULT) -> pd.DataFrame:
    rows = []

    for bundle in account_bundle.strategies:
        trades = bundle.trades.copy()
        kpi = compute_kpis_from_trades(trades, start_equity=start_equity)
        if kpi.empty:
            rows.append({
                "strategy": bundle.name,
                "trades": 0,
                "net_pnl": 0.0,
                "return_pct": None,
                "win_rate": None,
                "profit_factor": None,
                "max_dd_pct": None,
                "avg_trade": None,
            })
            continue

        r = kpi.iloc[0]
        rows.append({
            "strategy": bundle.name,
            "trades": int(r.get("n_trades", 0)) if pd.notna(r.get("n_trades")) else 0,
            "net_pnl": r.get("net_pnl"),
            "return_pct": r.get("total_return_pct"),
            "win_rate": r.get("win_rate"),
            "profit_factor": r.get("profit_factor"),
            "max_dd_pct": r.get("max_drawdown_pct"),
            "avg_trade": r.get("avg_trade"),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["net_pnl", "strategy"], ascending=[False, True]).reset_index(drop=True)
    return df


def build_symbol_breakdown_df(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty or "symbol" not in trades.columns or "net_sum" not in trades.columns:
        return pd.DataFrame()

    d = trades.copy()
    d["net_sum"] = pd.to_numeric(d["net_sum"], errors="coerce").fillna(0.0)
    grouped = d.groupby("symbol", dropna=False).agg(
        trades=("net_sum", "size"),
        net_pnl=("net_sum", "sum"),
        avg_trade=("net_sum", "mean"),
        wins=("net_sum", lambda s: int((s > 0).sum())),
        losses=("net_sum", lambda s: int((s < 0).sum())),
    ).reset_index()

    grouped["win_rate"] = grouped["wins"] / grouped["trades"].replace(0, np.nan)
    grouped = grouped.sort_values("net_pnl", ascending=False).reset_index(drop=True)
    return grouped


def build_direction_breakdown_df(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty or "direction" not in trades.columns or "net_sum" not in trades.columns:
        return pd.DataFrame()

    d = trades.copy()
    d["net_sum"] = pd.to_numeric(d["net_sum"], errors="coerce").fillna(0.0)

    grouped = d.groupby("direction", dropna=False).agg(
        trades=("net_sum", "size"),
        net_pnl=("net_sum", "sum"),
        avg_trade=("net_sum", "mean"),
        wins=("net_sum", lambda s: int((s > 0).sum())),
        losses=("net_sum", lambda s: int((s < 0).sum())),
    ).reset_index()

    grouped["win_rate"] = grouped["wins"] / grouped["trades"].replace(0, np.nan)
    grouped = grouped.sort_values("net_pnl", ascending=False).reset_index(drop=True)
    return grouped


def build_monthly_summary_df(monthly: pd.DataFrame) -> pd.DataFrame:
    if monthly.empty or "date" not in monthly.columns or "pnl_money" not in monthly.columns:
        return pd.DataFrame()

    d = monthly.copy().dropna(subset=["date"]).sort_values("date", ascending=False)
    if d.empty:
        return pd.DataFrame()

    d["month"] = d["date"].dt.strftime("%Y-%m")
    cols = ["month", "pnl_money", "cum_pnl_money", "nav", "drawdown_money", "drawdown_pct"]
    for c in cols:
        if c not in d.columns:
            d[c] = np.nan

    return d[cols].head(MAX_MONTHS_TABLE).reset_index(drop=True)


# ============================================================
# PDF STYLES
# ============================================================

def build_styles():
    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name="ReportTitle",
        parent=styles["Heading1"],
        fontName="Helvetica-Bold",
        fontSize=22,
        leading=26,
        textColor=CLR_NAVY,
        alignment=TA_LEFT,
        spaceAfter=4,
    ))

    styles.add(ParagraphStyle(
        name="SubTitle",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9,
        leading=12,
        textColor=CLR_MUTED,
        alignment=TA_LEFT,
        spaceAfter=6,
    ))

    styles.add(ParagraphStyle(
        name="SectionTitle",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=11,
        leading=13,
        textColor=CLR_NAVY,
        spaceBefore=8,
        spaceAfter=6,
    ))

    styles.add(ParagraphStyle(
        name="BodyText2",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9,
        leading=13,
        textColor=CLR_SLATE,
    ))

    styles.add(ParagraphStyle(
        name="SmallText",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8,
        leading=10,
        textColor=CLR_MUTED,
    ))

    styles.add(ParagraphStyle(
        name="KpiTitle",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=7.5,
        leading=9,
        textColor=CLR_MUTED,
        alignment=TA_CENTER,
    ))

    styles.add(ParagraphStyle(
        name="KpiValue",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=14,
        leading=16,
        textColor=CLR_NAVY,
        alignment=TA_CENTER,
    ))

    return styles


# ============================================================
# TABLE BUILDERS
# ============================================================

def make_table(
    data: List[List[object]],
    col_widths: Optional[List[float]] = None,
    font_size: int = 8,
    align_right_from_col: int = 1,
) -> Table:
    tbl = Table(data, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), CLR_NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), CLR_WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), font_size),

        ("BACKGROUND", (0, 1), (-1, -1), CLR_WHITE),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [CLR_WHITE, CLR_ROW_ALT]),

        ("GRID", (0, 0), (-1, -1), 0.35, CLR_BORDER),
        ("LINEBELOW", (0, 0), (-1, 0), 0.6, CLR_BORDER),

        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
    ]))

    if len(data) > 1 and len(data[0]) > align_right_from_col:
        tbl.setStyle(TableStyle([
            ("ALIGN", (align_right_from_col, 1), (-1, -1), "RIGHT"),
        ]))
    return tbl


def build_kpi_card(title: str, value: str, width: float, height: float, styles) -> Table:
    data = [
        [Paragraph(title, styles["KpiTitle"])],
        [Paragraph(value, styles["KpiValue"])],
    ]
    t = Table(data, colWidths=[width], rowHeights=[height * 0.42, height * 0.58])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), CLR_WHITE),
        ("BOX", (0, 0), (-1, -1), 0.6, CLR_BORDER),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))
    return t


def build_kpi_grid(bundle: StrategyBundle, styles) -> Table:
    if bundle.kpis.empty:
        cards = [
            build_kpi_card("Net PnL", "-", 43 * mm, 20 * mm, styles),
            build_kpi_card("Return %", "-", 43 * mm, 20 * mm, styles),
            build_kpi_card("Max DD %", "-", 43 * mm, 20 * mm, styles),
            build_kpi_card("Trades", "-", 43 * mm, 20 * mm, styles),
        ]
        grid = [[cards[0], cards[1]], [cards[2], cards[3]]]
        return Table(grid, colWidths=[87 * mm, 87 * mm])

    r = bundle.kpis.iloc[0]

    cards = [
        build_kpi_card("Net PnL", fmt_money(r.get("net_pnl")), 42 * mm, 20 * mm, styles),
        build_kpi_card("Return %", fmt_pct_ratio(r.get("total_return_pct")), 42 * mm, 20 * mm, styles),
        build_kpi_card("Max DD %", fmt_pct_ratio(r.get("max_drawdown_pct")), 42 * mm, 20 * mm, styles),
        build_kpi_card("Profit Factor", fmt_num(r.get("profit_factor")), 42 * mm, 20 * mm, styles),
        build_kpi_card("Win Rate", fmt_pct_ratio(r.get("win_rate")), 42 * mm, 20 * mm, styles),
        build_kpi_card("Avg Trade", fmt_money(r.get("avg_trade")), 42 * mm, 20 * mm, styles),
        build_kpi_card("Sharpe", fmt_num(r.get("sharpe_ratio")), 42 * mm, 20 * mm, styles),
        build_kpi_card("Trades", str(int(r.get("n_trades"))) if pd.notna(r.get("n_trades")) else "-", 42 * mm, 20 * mm, styles),
    ]

    grid = [
        [cards[0], cards[1], cards[2], cards[3]],
        [cards[4], cards[5], cards[6], cards[7]],
    ]

    tbl = Table(grid, colWidths=[43 * mm] * 4, rowHeights=[22 * mm, 22 * mm])
    tbl.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 1),
        ("RIGHTPADDING", (0, 0), (-1, -1), 1),
        ("TOPPADDING", (0, 0), (-1, -1), 1),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
    ]))
    return tbl


def build_meta_table(account_name: str, bundle: StrategyBundle) -> Table:
    trade_count = int(len(bundle.trades)) if not bundle.trades.empty else 0

    first_trade = "-"
    last_trade = "-"
    if not bundle.trades.empty and "close_time_utc" in bundle.trades.columns:
        valid = bundle.trades["close_time_utc"].dropna()
        if not valid.empty:
            first_trade = fmt_dt(valid.min())
            last_trade = fmt_dt(valid.max())

    data = [
        ["Field", "Value"],
        ["Account", account_name],
        ["Scope", bundle.name],
        ["Trades Loaded", str(trade_count)],
        ["First Closed Trade", first_trade],
        ["Last Closed Trade", last_trade],
    ]
    return make_table(data, [70 * mm, 105 * mm])


def build_extended_kpi_table(bundle: StrategyBundle) -> Table:
    if bundle.kpis.empty:
        return make_table([["Metric", "Value"], ["No KPIs", "-"]], [85 * mm, 90 * mm])

    r = bundle.kpis.iloc[0]
    rows = [
        ["Metric", "Value"],
        ["Net PnL", fmt_money(r.get("net_pnl"))],
        ["Return %", fmt_pct_ratio(r.get("total_return_pct"))],
        ["Gross Profit", fmt_money(r.get("gross_profit"))],
        ["Gross Loss", fmt_money(r.get("gross_loss"))],
        ["Profit Factor", fmt_num(r.get("profit_factor"))],
        ["Payoff Ratio", fmt_num(r.get("payoff_ratio"))],
        ["Expectancy", fmt_money(r.get("expectancy"))],
        ["Win Rate", fmt_pct_ratio(r.get("win_rate"))],
        ["Avg Trade", fmt_money(r.get("avg_trade"))],
        ["Avg Win", fmt_money(r.get("avg_win"))],
        ["Avg Loss", fmt_money(r.get("avg_loss"))],
        ["Max Drawdown", fmt_money(r.get("max_drawdown_closed"))],
        ["Max Drawdown %", fmt_pct_ratio(r.get("max_drawdown_pct"))],
        ["Recovery Factor", fmt_num(r.get("recovery_factor"))],
        ["Calmar Ratio", fmt_num(r.get("calmar_ratio"))],
        ["Sharpe Ratio", fmt_num(r.get("sharpe_ratio"))],
        ["Sortino Ratio", fmt_num(r.get("sortino_ratio"))],
        ["Best Trade", fmt_money(r.get("best_trade"))],
        ["Worst Trade", fmt_money(r.get("worst_trade"))],
        ["Commission Total", fmt_money(r.get("commission_total"))],
        ["Swap Total", fmt_money(r.get("swap_total"))],
        ["Avg Hold Minutes", fmt_num(r.get("avg_hold_minutes"))],
        ["Longest Win Streak", fmt_num(r.get("longest_win_streak"), 0)],
        ["Longest Loss Streak", fmt_num(r.get("longest_loss_streak"), 0)],
    ]
    return make_table(rows, [85 * mm, 90 * mm])


def build_recent_trades_table(trades: pd.DataFrame) -> Table:
    if trades.empty:
        return make_table([["Close Time", "Strategy", "Symbol", "Dir", "Net", "Volume"], ["-", "-", "-", "-", "-", "-"]])

    d = trades.copy()
    if "close_time_utc" in d.columns:
        d = d.sort_values("close_time_utc", ascending=False)
    d = d.head(MAX_RECENT_TRADES)

    rows = [["Close Time", "Strategy", "Symbol", "Dir", "Net", "Volume"]]
    for _, r in d.iterrows():
        rows.append([
            fmt_dt(r.get("close_time_utc")),
            str(r.get("strategy_id", "")),
            str(r.get("symbol", "")),
            str(r.get("direction", "")),
            fmt_money(r.get("net_sum")),
            fmt_num(r.get("volume_in")),
        ])

    return make_table(rows, [42 * mm, 34 * mm, 28 * mm, 16 * mm, 28 * mm, 20 * mm], font_size=7)


def build_monthly_table(monthly: pd.DataFrame) -> Table:
    d = build_monthly_summary_df(monthly)
    if d.empty:
        return make_table([["Month", "PnL", "CumPnL", "NAV", "DD Money"], ["-", "-", "-", "-", "-"]])

    rows = [["Month", "PnL", "CumPnL", "NAV", "DD Money"]]
    for _, r in d.iterrows():
        rows.append([
            r["month"],
            fmt_money(r.get("pnl_money")),
            fmt_money(r.get("cum_pnl_money")),
            fmt_money(r.get("nav")),
            fmt_money(r.get("drawdown_money")),
        ])

    return make_table(rows, [30 * mm, 35 * mm, 35 * mm, 35 * mm, 30 * mm])


def build_strategy_attribution_table(df: pd.DataFrame) -> Table:
    if df.empty:
        return make_table([["Strategy", "Trades", "Net PnL", "Return %", "Win Rate", "PF", "Max DD %"], ["-", "-", "-", "-", "-", "-", "-"]])

    d = df.head(MAX_STRATEGIES_ATTRIBUTION).copy()
    rows = [["Strategy", "Trades", "Net PnL", "Return %", "Win Rate", "PF", "Max DD %"]]
    for _, r in d.iterrows():
        rows.append([
            str(r.get("strategy", "")),
            str(int(r.get("trades", 0))) if pd.notna(r.get("trades")) else "-",
            fmt_money(r.get("net_pnl")),
            fmt_pct_ratio(r.get("return_pct")),
            fmt_pct_ratio(r.get("win_rate")),
            fmt_num(r.get("profit_factor")),
            fmt_pct_ratio(r.get("max_dd_pct")),
        ])

    return make_table(rows, [50 * mm, 18 * mm, 28 * mm, 24 * mm, 24 * mm, 18 * mm, 22 * mm], font_size=7)


def build_symbol_breakdown_table(df: pd.DataFrame) -> Table:
    if df.empty:
        return make_table([["Symbol", "Trades", "Net PnL", "Avg Trade", "Win Rate"], ["-", "-", "-", "-", "-"]])

    d = df.head(MAX_SYMBOL_ROWS).copy()
    rows = [["Symbol", "Trades", "Net PnL", "Avg Trade", "Win Rate"]]
    for _, r in d.iterrows():
        rows.append([
            str(r.get("symbol", "")),
            str(int(r.get("trades", 0))) if pd.notna(r.get("trades")) else "-",
            fmt_money(r.get("net_pnl")),
            fmt_money(r.get("avg_trade")),
            fmt_pct_ratio(r.get("win_rate")),
        ])

    return make_table(rows, [45 * mm, 20 * mm, 35 * mm, 35 * mm, 30 * mm])


def build_direction_breakdown_table(df: pd.DataFrame) -> Table:
    if df.empty:
        return make_table([["Direction", "Trades", "Net PnL", "Avg Trade", "Win Rate"], ["-", "-", "-", "-", "-"]])

    rows = [["Direction", "Trades", "Net PnL", "Avg Trade", "Win Rate"]]
    for _, r in df.iterrows():
        rows.append([
            str(r.get("direction", "")),
            str(int(r.get("trades", 0))) if pd.notna(r.get("trades")) else "-",
            fmt_money(r.get("net_pnl")),
            fmt_money(r.get("avg_trade")),
            fmt_pct_ratio(r.get("win_rate")),
        ])

    return make_table(rows, [45 * mm, 20 * mm, 35 * mm, 35 * mm, 30 * mm])


# ============================================================
# CHARTS
# ============================================================

def _tmp_png(prefix: str) -> Path:
    fd, tmp_name = tempfile.mkstemp(prefix=prefix, suffix=".png")
    os.close(fd)
    return Path(tmp_name)


def save_equity_drawdown_chart(
    bundle: StrategyBundle,
    title: str,
    start_equity: float = START_EQUITY_DEFAULT
) -> Optional[Path]:
    if bundle.trades.empty or "net_sum" not in bundle.trades.columns or "close_time_utc" not in bundle.trades.columns:
        return None

    d = bundle.trades.copy()
    d = d.dropna(subset=["close_time_utc"]).sort_values("close_time_utc").copy()
    if d.empty:
        return None

    d["net_sum"] = pd.to_numeric(d["net_sum"], errors="coerce").fillna(0.0)
    d["equity"] = float(start_equity) + d["net_sum"].cumsum()
    d["running_max"] = d["equity"].cummax()

    # Drawdown in MONEY, nicht in %
    d["drawdown_money"] = d["equity"] - d["running_max"]

    out = _tmp_png("equity_drawdown_")

    fig, (ax1, ax2) = plt.subplots(
        2, 1,
        figsize=(10, 5.8),
        sharex=True,
        gridspec_kw={"height_ratios": [3.2, 1.3]}
    )

    fig.patch.set_facecolor("white")
    ax1.set_facecolor("white")
    ax2.set_facecolor("white")

    ax1.plot(d["close_time_utc"], d["equity"], color="#2563EB", linewidth=2.0)
    ax1.set_title(title, fontsize=11, color="#0F172A")
    ax1.set_ylabel("Equity", color="#334155")
    ax1.grid(True, alpha=0.18)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax1.spines["left"].set_color("#CBD5E1")
    ax1.spines["bottom"].set_color("#CBD5E1")
    ax1.tick_params(colors="#64748B", labelsize=8)

    ax2.fill_between(
        d["close_time_utc"],
        d["drawdown_money"].fillna(0.0).values,
        0.0,
        color="#94A3B8",
        alpha=0.35
    )
    ax2.plot(d["close_time_utc"], d["drawdown_money"], color="#64748B", linewidth=1.2)
    ax2.set_ylabel("DD Money", color="#334155")
    ax2.set_xlabel("Time", color="#334155")
    ax2.grid(True, alpha=0.18)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    ax2.spines["left"].set_color("#CBD5E1")
    ax2.spines["bottom"].set_color("#CBD5E1")
    ax2.tick_params(colors="#64748B", labelsize=8)

    plt.tight_layout()
    plt.savefig(out, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out


def save_monthly_bar_chart(monthly: pd.DataFrame, title: str) -> Optional[Path]:
    if monthly.empty or "date" not in monthly.columns or "pnl_money" not in monthly.columns:
        return None

    d = monthly.copy().dropna(subset=["date"]).sort_values("date")
    if d.empty:
        return None

    x = d["date"].dt.strftime("%Y-%m")
    y = pd.to_numeric(d["pnl_money"], errors="coerce").fillna(0.0)

    out = _tmp_png("monthly_bar_")

    fig, ax = plt.subplots(figsize=(10, 3.8))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    bar_colors = ["#2563EB" if v >= 0 else "#B91C1C" for v in y]
    ax.bar(x, y, color=bar_colors, alpha=0.9)

    ax.set_title(title, fontsize=11, color="#0F172A")
    ax.set_xlabel("Month", color="#334155")
    ax.set_ylabel("PnL", color="#334155")
    ax.grid(True, axis="y", alpha=0.18)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#CBD5E1")
    ax.spines["bottom"].set_color("#CBD5E1")
    ax.tick_params(colors="#64748B", labelsize=8)

    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(out, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out


def save_strategy_contribution_chart(df: pd.DataFrame, title: str) -> Optional[Path]:
    if df.empty or "strategy" not in df.columns or "net_pnl" not in df.columns:
        return None

    d = df.head(12).sort_values("net_pnl", ascending=True).copy()
    out = _tmp_png("strategy_contrib_")

    fig, ax = plt.subplots(figsize=(9.5, 4.2))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    vals = pd.to_numeric(d["net_pnl"], errors="coerce").fillna(0.0)
    colors_bar = ["#2563EB" if v >= 0 else "#B91C1C" for v in vals]
    ax.barh(d["strategy"], vals, color=colors_bar, alpha=0.9)

    ax.set_title(title, fontsize=11, color="#0F172A")
    ax.set_xlabel("Net PnL", color="#334155")
    ax.set_ylabel("Strategy", color="#334155")
    ax.grid(True, axis="x", alpha=0.18)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#CBD5E1")
    ax.spines["bottom"].set_color("#CBD5E1")
    ax.tick_params(colors="#64748B", labelsize=8)

    plt.tight_layout()
    plt.savefig(out, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out


def save_symbol_contribution_chart(df: pd.DataFrame, title: str) -> Optional[Path]:
    if df.empty or "symbol" not in df.columns or "net_pnl" not in df.columns:
        return None

    d = df.head(10).sort_values("net_pnl", ascending=True).copy()
    out = _tmp_png("symbol_contrib_")

    fig, ax = plt.subplots(figsize=(9.5, 4.0))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    vals = pd.to_numeric(d["net_pnl"], errors="coerce").fillna(0.0)
    colors_bar = ["#2563EB" if v >= 0 else "#B91C1C" for v in vals]
    ax.barh(d["symbol"], vals, color=colors_bar, alpha=0.9)

    ax.set_title(title, fontsize=11, color="#0F172A")
    ax.set_xlabel("Net PnL", color="#334155")
    ax.set_ylabel("Symbol", color="#334155")
    ax.grid(True, axis="x", alpha=0.18)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#CBD5E1")
    ax.spines["bottom"].set_color("#CBD5E1")
    ax.tick_params(colors="#64748B", labelsize=8)

    plt.tight_layout()
    plt.savefig(out, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out


# ============================================================
# HEADER / FOOTER
# ============================================================

def add_header_footer(canvas, doc):
    canvas.saveState()

    canvas.setStrokeColor(CLR_BORDER)
    canvas.setLineWidth(0.6)
    canvas.line(15 * mm, 14 * mm, 195 * mm, 14 * mm)

    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(CLR_MUTED)
    canvas.drawString(
        15 * mm,
        9 * mm,
        f"Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )
    canvas.drawRightString(195 * mm, 9 * mm, f"{doc.page}")

    canvas.restoreState()


# ============================================================
# STORY HELPERS
# ============================================================

def add_image_if_exists(story: List, img_path: Optional[Path], width_mm: float, height_mm: float) -> None:
    if img_path is not None and img_path.exists():
        story.append(Image(str(img_path), width=width_mm * mm, height=height_mm * mm))


def build_account_executive_summary(account_bundle: AccountBundle) -> str:
    bundle = account_bundle.overall

    if bundle.kpis.empty:
        return "No trade data available for this account."

    r = bundle.kpis.iloc[0]
    net_pnl = fmt_money(r.get("net_pnl"))
    return_pct = fmt_pct_ratio(r.get("total_return_pct"))
    max_dd = fmt_pct_ratio(r.get("max_drawdown_pct"))
    pf = fmt_num(r.get("profit_factor"))
    win_rate = fmt_pct_ratio(r.get("win_rate"))
    n_trades = int(r.get("n_trades")) if pd.notna(r.get("n_trades")) else 0

    best_strategy = "-"
    worst_strategy = "-"

    attr = build_strategy_attribution_df(account_bundle)
    if not attr.empty:
        best_strategy = f"{attr.iloc[0]['strategy']} ({fmt_money(attr.iloc[0]['net_pnl'])})"
        worst_strategy = f"{attr.iloc[-1]['strategy']} ({fmt_money(attr.iloc[-1]['net_pnl'])})"

    text = (
        f"<b>Account Summary:</b> Total closed-trade result is <b>{net_pnl}</b> "
        f"with a total return of <b>{return_pct}</b>. "
        f"Maximum closed-trade drawdown is <b>{max_dd}</b>. "
        f"Trade quality shows Profit Factor <b>{pf}</b> and Win Rate <b>{win_rate}</b> "
        f"across <b>{n_trades}</b> trades. "
        f"Best contributing strategy: <b>{best_strategy}</b>. "
        f"Weakest strategy: <b>{worst_strategy}</b>."
    )
    return text


# ============================================================
# PDF GENERATION
# ============================================================

def generate_account_master_pdf(
    account_bundle: AccountBundle,
    out_path: Path,
    start_equity: float = START_EQUITY_DEFAULT,
) -> None:
    ensure_dir(out_path.parent)
    styles = build_styles()
    story: List = []
    temp_images: List[Path] = []

    account_name = account_bundle.account_name
    overall = account_bundle.overall

    title = f"{account_name} Performance Report"
    subtitle = "Account overview, attribution, risk metrics, and strategy detail"

    story.append(Paragraph(title, styles["ReportTitle"]))
    story.append(Paragraph(subtitle, styles["SubTitle"]))
    story.append(Spacer(1, 5 * mm))

    story.append(build_meta_table(account_name, overall))
    story.append(Spacer(1, 6 * mm))

    story.append(Paragraph("Executive KPI Dashboard", styles["SectionTitle"]))
    story.append(build_kpi_grid(overall, styles))
    story.append(Spacer(1, 6 * mm))

    story.append(Paragraph("Executive Summary", styles["SectionTitle"]))
    story.append(Paragraph(build_account_executive_summary(account_bundle), styles["BodyText2"]))
    story.append(Spacer(1, 6 * mm))

    eq_dd_img = save_equity_drawdown_chart(overall, f"Account Equity and Drawdown | {account_name}", start_equity)
    if eq_dd_img:
        temp_images.append(eq_dd_img)
        story.append(Paragraph("Account Equity and Drawdown", styles["SectionTitle"]))
        add_image_if_exists(story, eq_dd_img, 178, 100)
        story.append(Spacer(1, 5 * mm))

    story.append(PageBreak())
    story.append(Paragraph("Monthly Performance", styles["SectionTitle"]))

    monthly_img = save_monthly_bar_chart(overall.monthly, f"Monthly PnL | {account_name}")
    if monthly_img:
        temp_images.append(monthly_img)
        add_image_if_exists(story, monthly_img, 178, 72)
        story.append(Spacer(1, 5 * mm))

    story.append(build_monthly_table(overall.monthly))
    story.append(Spacer(1, 6 * mm))

    attr_df = build_strategy_attribution_df(account_bundle, start_equity=start_equity)
    story.append(Paragraph("Strategy Attribution", styles["SectionTitle"]))

    contrib_img = save_strategy_contribution_chart(attr_df, f"Strategy Contribution | {account_name}")
    if contrib_img:
        temp_images.append(contrib_img)
        add_image_if_exists(story, contrib_img, 178, 74)
        story.append(Spacer(1, 5 * mm))

    story.append(build_strategy_attribution_table(attr_df))

    story.append(PageBreak())
    story.append(Paragraph("Portfolio Breakdown", styles["SectionTitle"]))

    symbol_df = build_symbol_breakdown_df(overall.trades)
    symbol_img = save_symbol_contribution_chart(symbol_df, f"Symbol Contribution | {account_name}")
    if symbol_img:
        temp_images.append(symbol_img)
        add_image_if_exists(story, symbol_img, 178, 72)
        story.append(Spacer(1, 5 * mm))

    story.append(Paragraph("Top Symbols", styles["SectionTitle"]))
    story.append(build_symbol_breakdown_table(symbol_df))
    story.append(Spacer(1, 6 * mm))

    story.append(Paragraph("Direction Breakdown", styles["SectionTitle"]))
    story.append(build_direction_breakdown_table(build_direction_breakdown_df(overall.trades)))
    story.append(Spacer(1, 6 * mm))

    story.append(Paragraph("Recent Closed Trades", styles["SectionTitle"]))
    story.append(build_recent_trades_table(overall.trades))

    for bundle in account_bundle.strategies:
        story.append(PageBreak())
        story.append(Paragraph(f"Strategy Detail | {bundle.name}", styles["ReportTitle"]))
        story.append(Paragraph(f"Account: {account_name}", styles["SubTitle"]))
        story.append(Spacer(1, 4 * mm))

        story.append(build_meta_table(account_name, bundle))
        story.append(Spacer(1, 6 * mm))

        story.append(Paragraph("Strategy KPI Dashboard", styles["SectionTitle"]))
        story.append(build_kpi_grid(bundle, styles))
        story.append(Spacer(1, 6 * mm))

        strategy_img = save_equity_drawdown_chart(
            bundle,
            f"Equity and Drawdown | {account_name} | {bundle.name}",
            start_equity=start_equity,
        )
        if strategy_img:
            temp_images.append(strategy_img)
            story.append(Paragraph("Equity and Drawdown", styles["SectionTitle"]))
            add_image_if_exists(story, strategy_img, 178, 96)
            story.append(Spacer(1, 5 * mm))

        strategy_monthly_img = save_monthly_bar_chart(
            bundle.monthly,
            f"Monthly PnL | {account_name} | {bundle.name}",
        )
        if strategy_monthly_img:
            temp_images.append(strategy_monthly_img)
            story.append(Paragraph("Monthly Performance", styles["SectionTitle"]))
            add_image_if_exists(story, strategy_monthly_img, 178, 68)
            story.append(Spacer(1, 5 * mm))

        story.append(Paragraph("Extended KPIs", styles["SectionTitle"]))
        story.append(build_extended_kpi_table(bundle))
        story.append(Spacer(1, 6 * mm))

        story.append(Paragraph("Recent Trades", styles["SectionTitle"]))
        story.append(build_recent_trades_table(bundle.trades))

    doc = SimpleDocTemplate(
        str(out_path),
        pagesize=A4,
        leftMargin=15 * mm,
        rightMargin=15 * mm,
        topMargin=15 * mm,
        bottomMargin=18 * mm,
        title=f"{account_name} Performance Report",
        author="OpenAI / ChatGPT",
    )

    doc.build(story, onFirstPage=add_header_footer, onLaterPages=add_header_footer)

    for p in temp_images:
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass


# ============================================================
# JOB LOGIC
# ============================================================

def generate_reports_for_account(account_dir: Path, start_equity: float = START_EQUITY_DEFAULT) -> List[Path]:
    generated: List[Path] = []

    account_bundle = build_account_bundle(account_dir, start_equity=start_equity)
    account_name = account_bundle.account_name

    out_path = REPORT_ROOT / account_name / f"{safe_name(account_name)}_master_report.pdf"
    generate_account_master_pdf(
        account_bundle=account_bundle,
        out_path=out_path,
        start_equity=start_equity,
    )
    generated.append(out_path)

    return generated


def main() -> None:
    ensure_dir(REPORT_ROOT)

    account_dirs = list_account_dirs(LIVE_PERF_ROOT)
    if not account_dirs:
        raise RuntimeError(f"Keine account_* Ordner gefunden unter: {LIVE_PERF_ROOT}")

    all_generated: List[Path] = []

    for account_dir in account_dirs:
        try:
            generated = generate_reports_for_account(account_dir, start_equity=START_EQUITY_DEFAULT)
            all_generated.extend(generated)
            print(f"[OK] {account_dir.name}: {len(generated)} Master-Report(s) generiert")
        except Exception as e:
            print(f"[WARN] Report-Generierung fehlgeschlagen für {account_dir.name}: {e}")

    print(f"[DONE] Reports generiert: {len(all_generated)}")
    for p in all_generated:
        print(f"  - {p}")


if __name__ == "__main__":
    main()