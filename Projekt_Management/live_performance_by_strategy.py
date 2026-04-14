# -*- coding: utf-8 -*-
# live_performance_by_strategy.py
# OUTPUT: ONLY account folders, and inside ONLY strategies/ (no CSVs at account root)
# FILTER: FIXED cutoff-date (UTC)
# STRATEGY_ID: extracted from comment "Strategy X.Y.Z" and forward-filled per (account_id, position_id)
# SIDE: derived from MT5 deal type (1=BUY, 0=SELL), stabilized per position via entry==0
# Strategy folder name: <strategy_id>_<symbol>_<magic>_<side>
# No UNMAPPED/UNKNOWN folders (skipped if strategy_id or side missing)
#
# NEW:
#  - exports weekly_performance.csv + monthly_performance.csv per strategy bucket
#  - NAV is synthesized: nav = start_equity + cum_pnl_money
#  - CLI: --start-equity (float, default 100000)

from __future__ import annotations

import argparse
import os
import re
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd


DEALS_COLS = [
    "account_id", "ticket", "order", "position_id", "symbol", "type", "entry",
    "volume", "price", "profit", "swap", "commission", "magic", "comment", "time"
]

ORDERS_COLS = [
    "account_id", "ticket", "time_setup", "time_done", "symbol", "type", "state",
    "volume_initial", "volume_current", "price_open", "price_current", "sl", "tp",
    "magic", "comment"
]


# ----------------------------
# Helpers
# ----------------------------
def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def _sanitize_name(s: str) -> str:
    s = str(s).strip()
    s = re.sub(r"[<>:\"/\\|?*\x00-\x1F]", "_", s)
    s = re.sub(r"\s+", "_", s)
    return s[:200] if len(s) > 200 else s


def detect_mt5_root(preferred: str = "mt5_logs") -> str:
    cwd = os.getcwd()
    cand = os.path.join(cwd, preferred)
    if os.path.isdir(cand):
        return cand
    if os.path.isdir(preferred):
        return preferred
    for name in os.listdir(cwd):
        p = os.path.join(cwd, name)
        if os.path.isdir(p) and name.lower() == "mt5_logs":
            return p
    raise FileNotFoundError(f"mt5_logs nicht gefunden. Erwartet z.B.: {cand} | working dir: {cwd}")


def parse_cutoff_utc(date_str: str) -> pd.Timestamp:
    return pd.to_datetime(date_str, errors="raise", utc=True)


# ----------------------------
# Parsing Strategy + Side
# ----------------------------
_STRAT_RE = re.compile(r"Strategy\s+([0-9]+(?:\.[0-9]+)*)", re.IGNORECASE)


def extract_strategy_from_comment(c: str) -> Optional[str]:
    m = _STRAT_RE.search(str(c))
    return m.group(1) if m else None


def side_from_mt5_type(t) -> Optional[str]:
    # Based on your observed data: 1=BUY, 0=SELL
    try:
        t = int(t)
    except Exception:
        return None
    if t == 1:
        return "BUY"
    if t == 0:
        return "SELL"
    return None


def stabilize_strategy_and_side(deals: pd.DataFrame) -> pd.DataFrame:
    """
    Vectorized stabilization per (account_id, position_id):
      - strategy_id: first non-null strategy found in comment within the position
      - side: prefer side from entry==0 rows within the position, else first non-null side in the position
    """
    d = deals.copy()
    gk = ["account_id", "position_id"]

    d["strategy_id_comment"] = d["comment"].apply(extract_strategy_from_comment)
    d["side_raw"] = d["type"].apply(side_from_mt5_type)

    # strategy: first non-null in group (transform keeps index)
    d["strategy_id"] = d.groupby(gk)["strategy_id_comment"].transform("first")

    # side: prefer entry==0 side
    entry_side = d["side_raw"].where(d["entry"] == 0)
    side_pref = entry_side.groupby([d[gk[0]], d[gk[1]]]).transform("first")

    # fallback: first non-null side in group
    side_fallback = d.groupby(gk)["side_raw"].transform("first")
    d["side"] = side_pref.fillna(side_fallback)

    d = d.drop(columns=["strategy_id_comment", "side_raw"], errors="ignore")
    return d


# ----------------------------
# Registry (optional fallback)
# ----------------------------
def load_registry(path: str) -> pd.DataFrame:
    reg = pd.read_csv(path)
    if "magic" not in reg.columns or "strategy_id" not in reg.columns:
        raise ValueError("Registry must have columns: magic,strategy_id")
    reg = reg.copy()
    reg["magic"] = pd.to_numeric(reg["magic"], errors="coerce").astype("Int64")
    reg["strategy_id"] = reg["strategy_id"].astype(str).fillna("")
    reg = reg[reg["strategy_id"].str.len() > 0]
    return reg


def make_registry_template(deals: pd.DataFrame, out_path: str) -> pd.DataFrame:
    magics = (
        deals.dropna(subset=["magic"])
        .loc[:, ["magic"]]
        .drop_duplicates()
        .sort_values("magic")
        .reset_index(drop=True)
    )
    magics["strategy_id"] = ""
    magics["risk_bucket"] = ""
    magics["notes"] = ""
    magics.to_csv(out_path, index=False)
    return magics


# ----------------------------
# IO + Normalization
# ----------------------------
def read_deals_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    missing = [c for c in DEALS_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {path}: {missing}")

    df = df[DEALS_COLS].copy()

    for c in ["account_id", "ticket", "order", "position_id", "magic", "type", "entry"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    for c in ["volume", "price", "profit", "swap", "commission"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True)

    for c in ["symbol", "comment"]:
        df[c] = df[c].astype(str).fillna("")

    df["net"] = df["profit"] + df["swap"] + df["commission"]

    # stabilize (fixes missing strategy on closes + side consistency)
    df = stabilize_strategy_and_side(df)
    return df


def read_orders_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    missing = [c for c in ORDERS_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {path}: {missing}")

    df = df[ORDERS_COLS].copy()

    for c in ["account_id", "ticket", "magic"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    for c in ["time_setup", "time_done"]:
        df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)

    for c in ["symbol", "type", "state", "comment"]:
        df[c] = df[c].astype(str).fillna("")

    return df


def load_all_deals_and_orders(mt5_root: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    deals_frames = []
    orders_frames = []

    for name in os.listdir(mt5_root):
        if not name.lower().startswith("account_"):
            continue
        acc_dir = os.path.join(mt5_root, name)
        if not os.path.isdir(acc_dir):
            continue

        deals_path = os.path.join(acc_dir, "deals.csv")
        orders_path = os.path.join(acc_dir, "orders.csv")

        if os.path.exists(deals_path):
            deals_frames.append(read_deals_csv(deals_path))
        if os.path.exists(orders_path):
            orders_frames.append(read_orders_csv(orders_path))

    deals = (
        pd.concat(deals_frames, ignore_index=True)
        if deals_frames
        else pd.DataFrame(columns=DEALS_COLS + ["net", "strategy_id", "side"])
    )
    orders = (
        pd.concat(orders_frames, ignore_index=True)
        if orders_frames
        else pd.DataFrame(columns=ORDERS_COLS)
    )

    if not deals.empty:
        deals = deals.dropna(subset=["time", "account_id", "ticket", "position_id"])

    if not orders.empty:
        orders = orders.dropna(subset=["account_id", "ticket"])

    return deals, orders


# ----------------------------
# Date filtering (FIXED cutoff date)
# ----------------------------
def filter_from_cutoff(deals: pd.DataFrame, orders: pd.DataFrame, cutoff: pd.Timestamp) -> Tuple[pd.DataFrame, pd.DataFrame]:
    d = deals.copy()
    if not d.empty:
        d = d[d["time"] >= cutoff].copy()

    o = orders.copy()
    if not o.empty:
        t = o["time_done"].copy().fillna(o["time_setup"])
        o = o[t >= cutoff].copy()

    return d, o


# ----------------------------
# Trades (position-level aggregation)
# ----------------------------
def build_trades_from_positions(deals: pd.DataFrame) -> pd.DataFrame:
    d = deals.dropna(subset=["account_id", "position_id", "time"]).copy()
    if d.empty:
        return pd.DataFrame()

    d = d.sort_values(["account_id", "position_id", "time", "ticket"])

    rows: List[dict] = []
    for (acc, pos), g in d.groupby(["account_id", "position_id"], sort=False):
        rows.append({
            "account_id": int(acc),
            "position_id": int(pos),
            "strategy_id": g["strategy_id"].iloc[0],
            "symbol": g["symbol"].iloc[0],
            "magic": int(g["magic"].iloc[0]) if pd.notna(g["magic"].iloc[0]) else None,
            "side": g["side"].iloc[0],
            "open_time_utc": g["time"].iloc[0],
            "close_time_utc": g["time"].iloc[-1],
            "net_pnl": float(g["net"].sum()),
            "gross_pnl": float(g["profit"].sum()),
            "swap": float(g["swap"].sum()),
            "commission": float(g["commission"].sum()),
            "n_deals": int(len(g)),
        })

    trades = pd.DataFrame(rows)
    if trades.empty:
        return trades

    return trades.sort_values(["account_id", "strategy_id", "symbol", "magic", "side", "open_time_utc"]).reset_index(drop=True)


# ----------------------------
# KPI
# ----------------------------
def kpis_from_trades(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    g = trades.groupby(["strategy_id", "symbol", "magic", "side"], dropna=False)
    out = g.agg(
        net_pnl=("net_pnl", "sum"),
        gross_pnl=("gross_pnl", "sum"),
        swap=("swap", "sum"),
        commission=("commission", "sum"),
        n_trades=("position_id", "count"),
        avg_trade=("net_pnl", "mean"),
        first_time=("open_time_utc", "min"),
        last_time=("close_time_utc", "max"),
    ).reset_index()
    return out.sort_values("net_pnl", ascending=False).reset_index(drop=True)


# ----------------------------
# NEW: Weekly/Monthly performance exports (from deals)
# ----------------------------
def perf_from_deals(deals_sub: pd.DataFrame, freq: str, start_equity: float) -> pd.DataFrame:
    """
    Build performance table from deals:
      columns: date, nav, pnl_money, cum_pnl_money
    NAV is synthesized: nav = start_equity + cum_pnl_money

    freq:
      - weekly: 'W-FRI' (week ends Friday)
      - monthly: 'M' (month end)
    """
    if deals_sub is None or deals_sub.empty:
        return pd.DataFrame()

    d = deals_sub.dropna(subset=["time"]).copy()
    d = d.sort_values("time")
    d = d.set_index("time")

    if "net" not in d.columns:
        d["net"] = (
            pd.to_numeric(d.get("profit", 0), errors="coerce").fillna(0.0)
            + pd.to_numeric(d.get("swap", 0), errors="coerce").fillna(0.0)
            + pd.to_numeric(d.get("commission", 0), errors="coerce").fillna(0.0)
        )

    pnl = d["net"].resample(freq).sum().astype(float)

    out = pd.DataFrame({"pnl_money": pnl})
    out["cum_pnl_money"] = out["pnl_money"].cumsum()
    out["nav"] = float(start_equity) + out["cum_pnl_money"]

    out = out.reset_index().rename(columns={"time": "date"})
    out["date"] = pd.to_datetime(out["date"], utc=True)
    return out


def export_perf_files(bucket_path: str, deals_sub: pd.DataFrame, start_equity: float):
    """
    Writes weekly_performance.csv and monthly_performance.csv into bucket folder.
    """
    if deals_sub is None or deals_sub.empty:
        return

    wk = perf_from_deals(deals_sub, freq="W-FRI", start_equity=start_equity)
    mo = perf_from_deals(deals_sub, freq="M", start_equity=start_equity)

    if not wk.empty:
        wk.to_csv(os.path.join(bucket_path, "weekly_performance.csv"), index=False)
    if not mo.empty:
        mo.to_csv(os.path.join(bucket_path, "monthly_performance.csv"), index=False)


# ----------------------------
# Export ONLY accounts -> ONLY strategies folder inside
# ----------------------------
def export_only_accounts(outdir: str, deals: pd.DataFrame, trades: pd.DataFrame, start_equity: float):
    base = os.path.join(outdir, "accounts")
    _ensure_dir(base)

    if deals.empty:
        raise RuntimeError("Keine Deals nach Filter. Kein Export.")

    account_ids = sorted([int(x) for x in deals["account_id"].dropna().unique().tolist()])

    for acc in account_ids:
        d_acc = deals[deals["account_id"] == acc].copy()
        t_acc = trades[trades["account_id"] == acc].copy() if not trades.empty else pd.DataFrame()

        # only valid buckets (no UNMAPPED/UNKNOWN)
        if t_acc.empty:
            continue

        valid = t_acc.dropna(subset=["strategy_id", "side"]).copy()
        valid = valid[
            (valid["strategy_id"].astype(str).str.len() > 0) &
            (valid["side"].astype(str).str.len() > 0)
        ]
        if valid.empty:
            continue

        # create account folder only if it has at least one valid strategy bucket
        acc_dir = os.path.join(base, f"{acc}")
        _ensure_dir(acc_dir)

        strat_base = os.path.join(acc_dir, "strategies")
        _ensure_dir(strat_base)

        for (sid, sym, mag, side), _ in valid.groupby(["strategy_id", "symbol", "magic", "side"]):
            folder = f"{_sanitize_name(sid)}_{_sanitize_name(sym)}_{mag}_{_sanitize_name(side)}"
            p = os.path.join(strat_base, folder)
            _ensure_dir(p)

            d_sub = d_acc[
                (d_acc["strategy_id"] == sid) &
                (d_acc["symbol"] == sym) &
                (d_acc["magic"] == mag) &
                (d_acc["side"] == side)
            ].copy()
            if not d_sub.empty:
                d_sub.sort_values("time").to_csv(os.path.join(p, "deals.csv"), index=False)

            t_sub = valid[
                (valid["strategy_id"] == sid) &
                (valid["symbol"] == sym) &
                (valid["magic"] == mag) &
                (valid["side"] == side)
            ].copy()
            if not t_sub.empty:
                t_sub.to_csv(os.path.join(p, "trades.csv"), index=False)
                k_sub = kpis_from_trades(t_sub)
                k_sub.to_csv(os.path.join(p, "kpis.csv"), index=False)

            # NEW: weekly/monthly performance
            if not d_sub.empty:
                export_perf_files(p, d_sub, start_equity=start_equity)


# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mt5-root", default="mt5_logs")
    ap.add_argument("--outdir", default="out_live")
    ap.add_argument("--cutoff-date", default="2026-01-11")
    ap.add_argument("--registry", default="strategy_registry.csv")
    ap.add_argument("--make-registry", action="store_true")

    # NEW
    ap.add_argument("--start-equity", type=float, default=100000.0)

    args = ap.parse_args()

    mt5_root = detect_mt5_root(args.mt5_root)
    deals_all, orders_all = load_all_deals_and_orders(mt5_root)

    if deals_all.empty:
        raise RuntimeError(f"Keine deals.csv Daten gefunden unter: {mt5_root}")

    # registry template on all deals (optional)
    if args.make_registry:
        make_registry_template(deals_all, args.registry)
        print(f"Registry template written: {args.registry}")
        return

    cutoff = parse_cutoff_utc(args.cutoff_date)
    deals, _orders = filter_from_cutoff(deals_all, orders_all, cutoff)

    if deals.empty:
        raise RuntimeError(f"Keine Deals ab cutoff-date gefunden: {cutoff} (UTC)")

    # optional: fill missing strategy_id from registry by magic
    if os.path.exists(args.registry):
        reg = load_registry(args.registry)
        if not reg.empty:
            deals = deals.merge(
                reg[["magic", "strategy_id"]].rename(columns={"strategy_id": "strategy_id_reg"}),
                on="magic",
                how="left",
            )
            m = deals["strategy_id"].isna() | (deals["strategy_id"].astype(str).str.len() == 0)
            deals.loc[m, "strategy_id"] = deals.loc[m, "strategy_id_reg"]
            deals = deals.drop(columns=["strategy_id_reg"], errors="ignore")
            # restabilize per position after registry fill
            deals = deals.sort_values(["account_id", "position_id", "time", "ticket"])
            deals["strategy_id"] = deals.groupby(["account_id", "position_id"])["strategy_id"].transform("first")

    trades = build_trades_from_positions(deals)

    # output only account folders, and inside only strategies/
    export_only_accounts(args.outdir, deals, trades, start_equity=args.start_equity)

    s = deals.groupby("account_id")["net"].sum().sort_values(ascending=False)
    print("Account Net PnL (from cutoff-date:", cutoff, "UTC):")
    print(s.to_string())


if __name__ == "__main__":
    main()
