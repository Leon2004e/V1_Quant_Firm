# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


# ============================================================
# CONFIG / PATHS
# ============================================================

INITIAL_CAPITAL = 10_000.0

# Script: FTMO/2.Analyse_Center/Backtest_Analyse/Basic_KPI.py
FTMO_ROOT = Path(__file__).resolve().parents[2]

BACKTEST_ROOT = FTMO_ROOT / "1.Data_Center" / "Data" / "Strategy_Data" / "Backtest_Trades_Data"
REPORT_ROOT = FTMO_ROOT / "2.Analyse_Center" / "Reports" / "Backtests"
SUMMARY_DIR = REPORT_ROOT / "_summary"


# ============================================================
# UTIL
# ============================================================

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _safe_div(a: float, b: float) -> float:
    try:
        if b is None or float(b) == 0.0:
            return np.nan
        return float(a) / float(b)
    except Exception:
        return np.nan


def extract_strategy_from_filename(stem: str) -> str:
    # listOfTrades_0.18430_USDJPY_37_BUY -> Strategy_0.18430
    parts = stem.split("_")
    if len(parts) >= 2:
        return f"Strategy_{parts[1]}"
    return "Strategy_Unknown"


def consecutive_streaks(sign_series: pd.Series) -> Tuple[int, int]:
    """
    sign_series: +1 (win), -1 (loss), 0 (breakeven)
    returns: (max_consecutive_wins, max_consecutive_losses)
    """
    max_w = 0
    max_l = 0
    cur_w = 0
    cur_l = 0
    for v in sign_series.tolist():
        if v > 0:
            cur_w += 1
            cur_l = 0
        elif v < 0:
            cur_l += 1
            cur_w = 0
        else:
            cur_w = 0
            cur_l = 0
        max_w = max(max_w, cur_w)
        max_l = max(max_l, cur_l)
    return int(max_w), int(max_l)


# ============================================================
# LOAD + CLEAN
# ============================================================

def load_trades_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]

    required = ["Open time", "Close time", "P/L in money"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns {missing} in {path.name}")

    df["Open time"] = pd.to_datetime(df["Open time"], dayfirst=True, errors="coerce")
    df["Close time"] = pd.to_datetime(df["Close time"], dayfirst=True, errors="coerce")
    df["P/L in money"] = pd.to_numeric(df["P/L in money"], errors="coerce")

    if "Comm/Swap" in df.columns:
        df["Comm/Swap"] = pd.to_numeric(df["Comm/Swap"], errors="coerce")
    if "P/L in pips" in df.columns:
        df["P/L in pips"] = pd.to_numeric(df["P/L in pips"], errors="coerce")
    if "P/L in %" in df.columns:
        df["P/L in %"] = pd.to_numeric(df["P/L in %"], errors="coerce")

    df = df.dropna(subset=["Close time", "P/L in money"]).sort_values("Close time").reset_index(drop=True)
    return df


# ============================================================
# EQUITY / DRAWDOWN + ENRICH
# ============================================================

def add_equity_and_dd(df: pd.DataFrame, initial_capital: float) -> pd.DataFrame:
    d = df.copy()

    d["cum_pl_money"] = d["P/L in money"].cumsum()
    d["equity"] = initial_capital + d["cum_pl_money"]
    d["hwm"] = d["equity"].cummax()
    d["dd"] = d["equity"] - d["hwm"]
    d["dd_pct"] = np.where(d["hwm"] > 0, (d["dd"] / d["hwm"]) * 100.0, 0.0)

    # time in trade
    dt = (d["Close time"] - d["Open time"])
    d["time_in_trade_min"] = (dt.dt.total_seconds() / 60.0).round(2)

    return d


# ============================================================
# ADVANCED METRICS (no SL/TP needed)
# ============================================================

def equity_daily_returns(d: pd.DataFrame) -> pd.Series:
    eq = d.set_index("Close time")["equity"].sort_index()
    daily = eq.resample("1D").last().dropna()
    return daily.pct_change().dropna()


def dd_duration_stats(d: pd.DataFrame) -> Dict[str, float]:
    """
    max_dd_duration_days: longest underwater duration
    time_to_recover_days: time from max-dd point to recover that HWM (NaN if not recovered)
    """
    x = d.sort_values("Close time").reset_index(drop=True)
    under = (x["equity"] < x["hwm"]).astype(int)

    if under.sum() == 0:
        return {"max_dd_duration_days": 0.0, "time_to_recover_days": 0.0}

    t = x["Close time"]

    max_dur = 0.0
    cur_start = None
    for i, u in enumerate(under.tolist()):
        if u == 1 and cur_start is None:
            cur_start = t.iloc[i]
        if u == 0 and cur_start is not None:
            dur = (t.iloc[i] - cur_start).total_seconds() / 86400.0
            max_dur = max(max_dur, dur)
            cur_start = None
    if cur_start is not None:
        dur = (t.iloc[-1] - cur_start).total_seconds() / 86400.0
        max_dur = max(max_dur, dur)

    dd_idx = int(x["dd"].idxmin())
    hwm_at_dd = float(x.loc[dd_idx, "hwm"])
    t_dd = x.loc[dd_idx, "Close time"]
    recovered = x[(x["Close time"] > t_dd) & (x["equity"] >= hwm_at_dd)]
    ttr = ((recovered["Close time"].iloc[0] - t_dd).total_seconds() / 86400.0) if len(recovered) else np.nan

    return {"max_dd_duration_days": float(max_dur), "time_to_recover_days": float(ttr)}


def ulcer_index(d: pd.DataFrame) -> float:
    ddp = d["dd_pct"].fillna(0.0).to_numpy()
    return float(np.sqrt(np.mean(ddp * ddp)))


def equity_trend_r2(d: pd.DataFrame) -> float:
    """
    R^2 of linear regression equity ~ time_index
    """
    y = d["equity"].to_numpy(dtype=float)
    if len(y) < 30:
        return np.nan
    x = np.arange(len(y), dtype=float)

    x_mean = x.mean()
    y_mean = y.mean()
    cov = np.mean((x - x_mean) * (y - y_mean))
    var = np.mean((x - x_mean) ** 2)
    if var == 0:
        return np.nan
    beta = cov / var
    alpha = y_mean - beta * x_mean

    y_hat = alpha + beta * x
    ss_res = np.sum((y - y_hat) ** 2)
    ss_tot = np.sum((y - y_mean) ** 2)
    return float(1.0 - ss_res / ss_tot) if ss_tot > 0 else np.nan


# ============================================================
# KPI
# ============================================================

def compute_kpis(d: pd.DataFrame, initial_capital: float) -> Dict[str, object]:
    n = int(len(d))
    pnl = d["P/L in money"].astype(float)

    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]

    gross_profit = float(wins.sum()) if len(wins) else 0.0
    gross_loss = float(abs(losses.sum())) if len(losses) else 0.0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else np.nan

    net_profit = float(pnl.sum()) if n else 0.0
    final_equity = float(d["equity"].iloc[-1]) if n else float(initial_capital)

    winrate = (len(wins) / n * 100.0) if n else 0.0
    avg_win = float(wins.mean()) if len(wins) else 0.0
    avg_loss = float(losses.mean()) if len(losses) else 0.0
    expectancy = float(pnl.mean()) if n else 0.0

    max_dd = float(d["dd"].min()) if n else 0.0
    max_dd_pct = float(d["dd_pct"].min()) if n else 0.0

    start = d["Close time"].min() if n else pd.NaT
    end = d["Close time"].max() if n else pd.NaT
    days = float((end - start).total_seconds() / 86400.0) if (pd.notna(start) and pd.notna(end)) else np.nan

    ret_pct = (net_profit / initial_capital * 100.0) if initial_capital > 0 else np.nan

    # distribution
    best_trade = float(pnl.max()) if n else np.nan
    worst_trade = float(pnl.min()) if n else np.nan
    median_trade = float(pnl.median()) if n else np.nan
    std_trade = float(pnl.std(ddof=0)) if n else np.nan
    skew_trade = float(pnl.skew()) if n >= 3 else np.nan
    kurt_trade = float(pnl.kurtosis()) if n >= 4 else np.nan

    win_loss_ratio = _safe_div(avg_win, abs(avg_loss)) if avg_loss != 0 else np.nan

    # streaks
    sign = np.sign(pnl).replace({np.nan: 0.0})
    max_w_streak, max_l_streak = consecutive_streaks(sign)

    # worst-k
    sorted_losses = pnl.sort_values()
    worst5_sum = float(sorted_losses.head(5).sum()) if n >= 5 else float(sorted_losses.sum()) if n else np.nan
    worst10pct = int(max(1, np.floor(0.10 * n))) if n else 0
    worst10pct_mean = float(sorted_losses.head(worst10pct).mean()) if worst10pct > 0 else np.nan

    # exposure proxy
    if np.isfinite(days) and days > 0 and "time_in_trade_min" in d.columns:
        total_in_trade_days = float(d["time_in_trade_min"].fillna(0.0).sum() / 60.0 / 24.0)
        exposure_pct = _safe_div(total_in_trade_days, days) * 100.0
        avg_time_in_trade_min = float(d["time_in_trade_min"].mean())
    else:
        total_in_trade_days = np.nan
        exposure_pct = np.nan
        avg_time_in_trade_min = np.nan

    # equity daily returns
    rets = equity_daily_returns(d) if n else pd.Series(dtype=float)
    if len(rets) >= 10:
        mu_d = float(rets.mean())
        vol_d = float(rets.std(ddof=0))
        downside_d = float(rets[rets < 0].std(ddof=0)) if (rets < 0).any() else 0.0

        ann_return = (1.0 + mu_d) ** 252 - 1.0
        ann_vol = vol_d * np.sqrt(252)

        sharpe = _safe_div(ann_return, ann_vol)
        sortino = _safe_div(ann_return, downside_d * np.sqrt(252)) if downside_d > 0 else np.nan

        var_95 = float(np.quantile(rets, 0.05))
        cvar_95 = float(rets[rets <= var_95].mean()) if (rets <= var_95).any() else np.nan

        q95 = float(np.quantile(rets, 0.95))
        q05 = float(np.quantile(rets, 0.05))
        tail_ratio = _safe_div(abs(q95), abs(q05)) if q05 != 0 else np.nan
    else:
        ann_return = ann_vol = sharpe = sortino = var_95 = cvar_95 = tail_ratio = np.nan

    # CAGR / rates
    if np.isfinite(days) and days > 0 and initial_capital > 0 and final_equity > 0:
        years = days / 365.25
        cagr = (final_equity / initial_capital) ** (1.0 / years) - 1.0 if years > 0 else np.nan
        profit_per_day = net_profit / days
        trades_per_day = n / days
    else:
        cagr = profit_per_day = trades_per_day = np.nan

    # drawdown/path
    ui = ulcer_index(d) if n else np.nan
    dd_stats = dd_duration_stats(d) if n else {"max_dd_duration_days": np.nan, "time_to_recover_days": np.nan}
    r2 = equity_trend_r2(d)

    # efficiency ratios
    return_dd_ratio = _safe_div(ret_pct, abs(max_dd_pct)) if (np.isfinite(ret_pct) and max_dd_pct != 0) else np.nan
    recovery_factor = _safe_div(net_profit, abs(max_dd)) if max_dd != 0 else np.nan
    calmar = _safe_div(cagr, abs(max_dd_pct) / 100.0) if (np.isfinite(cagr) and max_dd_pct != 0) else np.nan

    # gain-to-pain
    if len(rets) >= 10:
        g2p = _safe_div(rets.sum(), rets[rets < 0].abs().sum()) if (rets < 0).any() else np.nan
    else:
        g2p = np.nan

    return {
        # core
        "trades": n,
        "net_profit": round(net_profit, 2),
        "ret_%": round(float(ret_pct), 4) if np.isfinite(ret_pct) else None,
        "final_equity": round(final_equity, 2),
        "winrate_%": round(winrate, 2),
        "profit_factor": None if np.isnan(profit_factor) else round(float(profit_factor), 4),
        "expectancy": round(expectancy, 4),
        "avg_win": round(avg_win, 4),
        "avg_loss": round(avg_loss, 4),
        "win_loss_ratio": None if np.isnan(win_loss_ratio) else round(float(win_loss_ratio), 4),

        # dd
        "max_dd": round(max_dd, 2),
        "max_dd_%": round(max_dd_pct, 4),

        # time
        "days": round(float(days), 2) if np.isfinite(days) else None,
        "profit_per_day": round(float(profit_per_day), 6) if np.isfinite(profit_per_day) else None,
        "trades_per_day": round(float(trades_per_day), 6) if np.isfinite(trades_per_day) else None,
        "cagr_%": round(float(cagr) * 100.0, 6) if np.isfinite(cagr) else None,

        # equity-return risk
        "ann_return_%": round(float(ann_return) * 100.0, 6) if np.isfinite(ann_return) else None,
        "ann_vol_%": round(float(ann_vol) * 100.0, 6) if np.isfinite(ann_vol) else None,
        "sharpe": round(float(sharpe), 4) if np.isfinite(sharpe) else None,
        "sortino": round(float(sortino), 4) if np.isfinite(sortino) else None,
        "VaR_95_daily_%": round(float(var_95) * 100.0, 6) if np.isfinite(var_95) else None,
        "CVaR_95_daily_%": round(float(cvar_95) * 100.0, 6) if np.isfinite(cvar_95) else None,
        "tail_ratio": round(float(tail_ratio), 4) if np.isfinite(tail_ratio) else None,

        # stability/path
        "ulcer_index": round(float(ui), 6) if np.isfinite(ui) else None,
        "max_dd_duration_days": round(float(dd_stats["max_dd_duration_days"]), 6) if np.isfinite(dd_stats["max_dd_duration_days"]) else None,
        "time_to_recover_days": round(float(dd_stats["time_to_recover_days"]), 6) if np.isfinite(dd_stats["time_to_recover_days"]) else None,
        "equity_trend_r2": round(float(r2), 6) if np.isfinite(r2) else None,

        # distribution/extremes
        "best_trade": round(float(best_trade), 6) if np.isfinite(best_trade) else None,
        "worst_trade": round(float(worst_trade), 6) if np.isfinite(worst_trade) else None,
        "median_trade": round(float(median_trade), 6) if np.isfinite(median_trade) else None,
        "std_trade": round(float(std_trade), 6) if np.isfinite(std_trade) else None,
        "skew_trade": round(float(skew_trade), 6) if np.isfinite(skew_trade) else None,
        "kurt_trade": round(float(kurt_trade), 6) if np.isfinite(kurt_trade) else None,
        "max_consecutive_wins": max_w_streak,
        "max_consecutive_losses": max_l_streak,
        "worst5_sum": round(float(worst5_sum), 6) if np.isfinite(worst5_sum) else None,
        "worst10pct_mean": round(float(worst10pct_mean), 6) if np.isfinite(worst10pct_mean) else None,

        # efficiency proxies
        "return_dd_ratio": round(float(return_dd_ratio), 6) if np.isfinite(return_dd_ratio) else None,
        "recovery_factor": round(float(recovery_factor), 6) if np.isfinite(recovery_factor) else None,
        "calmar": round(float(calmar), 6) if np.isfinite(calmar) else None,
        "gain_to_pain": round(float(g2p), 6) if np.isfinite(g2p) else None,

        # exposure proxy
        "avg_time_in_trade_min": round(float(avg_time_in_trade_min), 4) if np.isfinite(avg_time_in_trade_min) else None,
        "total_time_in_trade_days": round(float(total_in_trade_days), 6) if np.isfinite(total_in_trade_days) else None,
        "exposure_%": round(float(exposure_pct), 6) if np.isfinite(exposure_pct) else None,

        # period
        "from": str(start) if pd.notna(start) else "",
        "to": str(end) if pd.notna(end) else "",
    }


# ============================================================
# SAVE (per run)
# ============================================================

def save_run_outputs(run_dir: Path, file: Path, d: pd.DataFrame, kpis: Dict[str, object]) -> None:
    ensure_dir(run_dir)

    out_trades = run_dir / "trades_enriched.csv"
    d.to_csv(out_trades, index=False)

    out_kpis = run_dir / "kpis.json"
    with open(out_kpis, "w", encoding="utf-8") as f:
        json.dump(kpis, f, indent=2)

    meta = {"source_file": str(file), "rows": int(len(d)), "columns": list(d.columns)}
    out_meta = run_dir / "meta.json"
    with open(out_meta, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)


def save_run_plots(run_dir: Path, d: pd.DataFrame, title: str) -> None:
    import matplotlib.pyplot as plt

    ensure_dir(run_dir)

    fig = plt.figure(figsize=(12, 5))
    ax = fig.add_subplot(1, 1, 1)
    ax.plot(d["Close time"], d["equity"])
    ax.set_title(title)
    ax.set_ylabel("Equity")
    ax.set_xlabel("Close time")
    fig.tight_layout()
    fig.savefig(run_dir / "equity.png", dpi=160)
    plt.close(fig)

    fig = plt.figure(figsize=(12, 3))
    ax = fig.add_subplot(1, 1, 1)
    ax.fill_between(d["Close time"], d["dd"], 0)
    ax.set_title(title + " | Drawdown")
    ax.set_ylabel("Drawdown")
    ax.set_xlabel("Close time")
    fig.tight_layout()
    fig.savefig(run_dir / "drawdown.png", dpi=160)
    plt.close(fig)


# ============================================================
# RANKING (STRONG FILTERS + SCORE)
# ============================================================

def add_ranking(summary: pd.DataFrame) -> pd.DataFrame:
    df = summary.copy()

    # numeric coercion
    num_cols = [
        "net_profit", "ret_%", "profit_factor", "max_dd_%", "winrate_%", "expectancy",
        "sharpe", "sortino", "calmar", "ulcer_index", "CVaR_95_daily_%", "equity_trend_r2",
        "return_dd_ratio", "gain_to_pain", "trades", "days", "max_consecutive_losses",
        "profit_per_day", "cagr_%", "ann_vol_%"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # ----------------------------
    # HARD FILTERS (tunable)
    # ----------------------------
    MIN_TRADES = 400
    MIN_DAYS = 120

    MIN_PF = 1.25
    MIN_EXPECTANCY = 1.0

    MIN_RET_PCT = 8.0
    MIN_NET_PROFIT = 800.0
    MIN_PROFIT_PER_DAY = 0.5

    MAX_DD_PCT_FLOOR = -8.0
    MAX_ULCER = 2.0

    MIN_CVAR95 = -1.0  # CVaR_95_daily_% (negative). Require >= -1.0 (less negative is better)

    MIN_R2 = 0.85
    MAX_LOSS_STREAK = 12

    # tiny dd deception rule: if abs(dd)<0.5 require ret>=3
    df["tiny_dd_flag"] = df["max_dd_%"].abs() < 0.5
    df["pass_tiny_dd"] = (~df["tiny_dd_flag"]) | (df["ret_%"] >= 3.0)

    df["pass_min_trades"] = df["trades"] >= MIN_TRADES
    df["pass_days"] = df["days"].fillna(0) >= MIN_DAYS

    df["pass_pf"] = df["profit_factor"] >= MIN_PF
    df["pass_expectancy"] = df["expectancy"] >= MIN_EXPECTANCY

    df["pass_ret"] = df["ret_%"] >= MIN_RET_PCT
    df["pass_profit"] = df["net_profit"] >= MIN_NET_PROFIT
    df["pass_profit_per_day"] = df["profit_per_day"] >= MIN_PROFIT_PER_DAY

    df["pass_dd"] = df["max_dd_%"] >= MAX_DD_PCT_FLOOR
    df["pass_ulcer"] = df["ulcer_index"] <= MAX_ULCER

    df["pass_cvar"] = df["CVaR_95_daily_%"].notna() & (df["CVaR_95_daily_%"] >= MIN_CVAR95)

    df["pass_r2"] = df["equity_trend_r2"] >= MIN_R2
    df["pass_streak"] = df["max_consecutive_losses"] <= MAX_LOSS_STREAK

    df["is_candidate"] = (
        df["pass_min_trades"] &
        df["pass_days"] &
        df["pass_pf"] &
        df["pass_expectancy"] &
        df["pass_ret"] &
        df["pass_profit"] &
        df["pass_profit_per_day"] &
        df["pass_dd"] &
        df["pass_ulcer"] &
        df["pass_cvar"] &
        df["pass_r2"] &
        df["pass_streak"] &
        df["pass_tiny_dd"]
    )

    # ----------------------------
    # SCORE (robust z-score)
    # ----------------------------
    def rz(s: pd.Series) -> pd.Series:
        x = pd.to_numeric(s, errors="coerce")
        med = x.median()
        mad = (x - med).abs().median()
        denom = 1.4826 * mad if (mad is not None and mad > 0) else np.nan
        return (x - med) / denom

    # positives
    z_calmar = rz(df["calmar"])
    z_sharpe = rz(df["sharpe"])
    z_pf = rz(df["profit_factor"])
    z_exp = rz(df["expectancy"])
    z_pday = rz(df["profit_per_day"])
    z_r2 = rz(df["equity_trend_r2"])
    z_g2p = rz(df["gain_to_pain"])

    # penalties
    z_ulcer_pen = rz(df["ulcer_index"])
    z_cvar_pen = rz(df["CVaR_95_daily_%"].abs())
    z_dd_pen = rz(df["max_dd_%"].abs())
    z_streak_pen = rz(df["max_consecutive_losses"])

    df["score"] = (
        0.22 * z_calmar +
        0.14 * z_sharpe +
        0.10 * z_pf +
        0.10 * z_exp +
        0.10 * z_pday +
        0.08 * z_r2 +
        0.06 * z_g2p
        - 0.14 * z_ulcer_pen
        - 0.10 * z_cvar_pen
        - 0.10 * z_dd_pen
        - 0.06 * z_streak_pen
    )

    df.loc[~df["is_candidate"], "score"] = np.nan
    df["rank_score"] = df["score"].rank(ascending=False, method="dense")

    # diagnostics
    df["fail_reason"] = ""
    checks = [
        ("min_trades", "pass_min_trades"),
        ("min_days", "pass_days"),
        ("pf", "pass_pf"),
        ("expectancy", "pass_expectancy"),
        ("ret", "pass_ret"),
        ("net_profit", "pass_profit"),
        ("profit_per_day", "pass_profit_per_day"),
        ("dd", "pass_dd"),
        ("ulcer", "pass_ulcer"),
        ("cvar", "pass_cvar"),
        ("r2", "pass_r2"),
        ("loss_streak", "pass_streak"),
        ("tiny_dd_rule", "pass_tiny_dd"),
    ]
    for label, col in checks:
        df.loc[~df[col], "fail_reason"] = df.loc[~df[col], "fail_reason"] + (label + ";")

    return df


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    ap = argparse.ArgumentParser(description="Backtest KPI analysis (no SL/TP) + strong filtering + ranking.")
    ap.add_argument("--root", type=str, default=str(BACKTEST_ROOT), help="Backtest folder.")
    ap.add_argument("--pattern", type=str, default="listOfTrades_*.csv", help="Glob pattern.")
    ap.add_argument("--initial-capital", type=float, default=INITIAL_CAPITAL, help="Initial capital.")
    ap.add_argument("--top", type=int, default=20, help="Top N print.")
    ap.add_argument("--one", type=str, default="", help="Analyze only one file (filename or full path).")
    ap.add_argument("--save-runs", action="store_true", help="Save per-run outputs (csv/json).")
    ap.add_argument("--save-plots", action="store_true", help="Save equity.png and drawdown.png per run.")
    ap.add_argument("--save-summary", action="store_true", help="Save summary tables.")
    args = ap.parse_args()

    root = Path(args.root).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Backtest folder not found: {root}")

    ensure_dir(REPORT_ROOT)
    ensure_dir(SUMMARY_DIR)

    if args.one.strip():
        p = Path(args.one)
        files = [p if p.is_absolute() else (root / args.one)]
    else:
        files = sorted(root.glob(args.pattern))

    if not files:
        print(f"[INFO] No files found: {root} / {args.pattern}")
        return

    rows: List[Dict[str, object]] = []

    print(f"[INFO] BACKTEST_ROOT: {root}")
    print(f"[INFO] REPORT_ROOT  : {REPORT_ROOT}")

    for f in files:
        try:
            df = load_trades_csv(f)
            if df.empty:
                print(f"[SKIP] empty: {f.name}")
                continue

            d = add_equity_and_dd(df, args.initial_capital)
            kpis = compute_kpis(d, args.initial_capital)

            strategy = extract_strategy_from_filename(f.stem)
            run_name = f.stem

            row = {"strategy": strategy, "run": run_name, "file": str(f), **kpis}
            rows.append(row)

            print(
                f"[OK] {strategy} | {f.name} | "
                f"trades={kpis['trades']} net={kpis['net_profit']} "
                f"PF={kpis['profit_factor'] if kpis['profit_factor'] is not None else 'NA'} "
                f"MaxDD%={kpis['max_dd_%']}"
            )

            if args.save_runs or args.save_plots:
                run_dir = REPORT_ROOT / strategy / run_name
                if args.save_runs:
                    save_run_outputs(run_dir, f, d, kpis)
                if args.save_plots:
                    save_run_plots(run_dir, d, title=f"{strategy} | {run_name}")

        except Exception as e:
            print(f"[WARN] failed {f.name}: {e}")

    if not rows:
        print("[INFO] No valid files processed.")
        return

    summary = pd.DataFrame(rows)
    ranked = add_ranking(summary)

    cols_basic = [
        "strategy", "run", "trades", "days",
        "net_profit", "ret_%", "profit_factor", "max_dd_%", "winrate_%", "expectancy",
        "sharpe", "sortino", "calmar", "ulcer_index", "CVaR_95_daily_%", "equity_trend_r2",
        "profit_per_day", "return_dd_ratio", "gain_to_pain", "max_consecutive_losses",
    ]

    print("\n=== TOP by net_profit ===")
    tmp = ranked.sort_values("net_profit", ascending=False, na_position="last").reset_index(drop=True)
    print(tmp[cols_basic].head(int(args.top)).to_string(index=False))

    print("\n=== TOP by SCORE (candidates only) ===")
    tmp2 = ranked.sort_values("score", ascending=False, na_position="last").reset_index(drop=True)
    cols_score = ["rank_score", "score"] + cols_basic + ["is_candidate", "fail_reason"]
    print(tmp2[cols_score].head(int(args.top)).to_string(index=False))

    if args.save_summary:
        out_full = SUMMARY_DIR / "summary_backtests_full.csv"
        out_rank = SUMMARY_DIR / "summary_backtests_ranked.csv"
        out_top_score = SUMMARY_DIR / f"top{int(args.top)}_by_score.csv"
        out_top_profit = SUMMARY_DIR / f"top{int(args.top)}_by_net_profit.csv"

        summary.to_csv(out_full, index=False)
        ranked.to_csv(out_rank, index=False)
        tmp2[cols_score].head(int(args.top)).to_csv(out_top_score, index=False)
        tmp[cols_basic].head(int(args.top)).to_csv(out_top_profit, index=False)

        try:
            ranked.to_parquet(SUMMARY_DIR / "summary_backtests_ranked.parquet", index=False)
        except Exception:
            pass

        filt_path = SUMMARY_DIR / "ranking_filters.json"
        filters = {
            "MIN_TRADES": 400,
            "MIN_DAYS": 120,
            "MIN_PF": 1.25,
            "MIN_EXPECTANCY": 1.0,
            "MIN_RET_PCT": 8.0,
            "MIN_NET_PROFIT": 800.0,
            "MIN_PROFIT_PER_DAY": 0.5,
            "MAX_DD_PCT_FLOOR": -8.0,
            "MAX_ULCER": 2.0,
            "MIN_CVAR95": -1.0,
            "MIN_R2": 0.85,
            "MAX_LOSS_STREAK": 12,
            "tiny_dd_rule": "if abs(max_dd_% ) < 0.5 then require ret_% >= 3.0",
            "note": "non-candidates get NaN score and rank",
        }
        with open(filt_path, "w", encoding="utf-8") as f:
            json.dump(filters, f, indent=2)

        print(f"\n[WRITE] {out_full}")
        print(f"[WRITE] {out_rank}")
        print(f"[WRITE] {out_top_profit}")
        print(f"[WRITE] {out_top_score}")
        print(f"[WRITE] {filt_path}")


if __name__ == "__main__":
    main()