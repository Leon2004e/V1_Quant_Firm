# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Data_Center/Data_Operations/Trades/Trade_Logger_Loop/Trade_Logger_FTMO_LIVE_540136824.py

Zweck:
- Startet fixes MT5-Terminal (optional portable)
- Verbindet sich, loggt ein
- Baut aus MT5 DEAL-Historie ein "Closed Trades"-Ledger (nur geschlossene Trades; pro position_id)
- Speichert inkrementell in:
    <FTMO_ROOT>/Data_Center/Data/Trades/Raw/Live/Strategy_Live_Performance/account_<login>_<account_type>/closed_trades.db
- Polling im Loop
- Robuster gegen:
    - längere offene Trades
    - wiederholte Polls
    - bestehende DBs
    - stille Ignore-Probleme
- Mit Debug-Logging
"""

from __future__ import annotations

import os
import time
import sqlite3
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import MetaTrader5 as mt5
import numpy as np
import pandas as pd


# ============================================================
# CONFIG
# ============================================================

FIXED_TERMINAL_EXE = Path(
    r"C:\Users\Leon\Desktop\Terminals\MetaTrader 5 - Kopie - Kopie - Kopie (8) - Kopie - Kopie - Kopie - Kopie\terminal64.exe"
)

MT5_LOGIN = int(os.getenv("MT5_LOGIN", "540136824"))
MT5_PASSWORD = os.getenv("MT5_PASSWORD", "bXRS1Jl@")
MT5_SERVER = os.getenv("MT5_SERVER", "FTMO-Server4")

PORTABLE = True
START_TERMINAL = True

INIT_TIMEOUT_MS = 20000
STARTUP_WAIT_SECONDS = 5
INIT_RETRIES = 8
RETRY_SLEEP_SECONDS = 2

POLL_SECONDS = 60
POLL_OVERLAP_SECONDS = 300
INITIAL_LOOKBACK_DAYS = 365

POSITION_REBUILD_LOOKBACK_DAYS = 365

DEBUG = True
DEBUG_SHOW_LAST_ROWS = 10

OUTPUT_BASE = (
    Path("Data_Center")
    / "Data"
    / "Trades"
    / "Raw"
    / "Live"
)
DB_FILENAME = "closed_trades.db"


# ============================================================
# ROOT
# ============================================================

def find_ftmo_root(start: Path) -> Path:
    cur = start.resolve()
    for p in [cur] + list(cur.parents):
        if (p / "Data_Center").exists():
            return p
    raise RuntimeError(
        f"FTMO-Root nicht gefunden (kein 'Data_Center' in Parents). Start={start}"
    )


ROOT = find_ftmo_root(Path(__file__))


# ============================================================
# GENERIC HELPERS
# ============================================================

def log_info(msg: str) -> None:
    print(f"[INFO] {msg}")


def log_warn(msg: str) -> None:
    print(f"[WARN] {msg}")


def log_debug(msg: str) -> None:
    if DEBUG:
        print(f"[DEBUG] {msg}")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def to_utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ts_iso_from_unix(sec: int) -> str:
    return datetime.fromtimestamp(int(sec), tz=timezone.utc).isoformat(timespec="seconds")


# ============================================================
# MT5 TERMINAL / CONNECTION
# ============================================================

def get_terminal_exe() -> Path:
    if not FIXED_TERMINAL_EXE.exists():
        raise RuntimeError(f"terminal64.exe nicht gefunden: {FIXED_TERMINAL_EXE}")
    return FIXED_TERMINAL_EXE


def start_terminal(exe: Path) -> None:
    args = [str(exe)]
    if PORTABLE:
        args.append("/portable")

    subprocess.Popen(
        args,
        cwd=str(exe.parent),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
    )


def get_account_type(server: str) -> str:
    s = str(server).strip().lower()
    if "demo" in s:
        return "DEMO"
    return "LIVE"


def connect_mt5(exe: Path) -> Dict[str, Any]:
    if not exe.exists():
        raise RuntimeError(f"terminal64.exe nicht gefunden: {exe}")

    if not MT5_PASSWORD:
        raise RuntimeError("MT5_PASSWORD fehlt")

    mt5.shutdown()

    if START_TERMINAL:
        start_terminal(exe)
        time.sleep(STARTUP_WAIT_SECONDS)

    last_err = None

    for i in range(1, INIT_RETRIES + 1):
        if mt5.initialize(path=str(exe), portable=PORTABLE, timeout=INIT_TIMEOUT_MS):
            break
        last_err = mt5.last_error()
        log_warn(f"initialize failed ({i}/{INIT_RETRIES}): {last_err}")
        time.sleep(RETRY_SLEEP_SECONDS)
    else:
        raise RuntimeError(f"initialize failed: {last_err}")

    for i in range(1, INIT_RETRIES + 1):
        if mt5.login(MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER):
            break
        last_err = mt5.last_error()
        log_warn(f"login failed ({i}/{INIT_RETRIES}): {last_err}")
        time.sleep(RETRY_SLEEP_SECONDS)
    else:
        mt5.shutdown()
        raise RuntimeError(f"login failed: {last_err}")

    acc = mt5.account_info()
    if acc is None:
        err = mt5.last_error()
        mt5.shutdown()
        raise RuntimeError(f"account_info failed: {err}")

    server = str(getattr(acc, "server", MT5_SERVER) or MT5_SERVER)
    account_type = get_account_type(server)

    print(f"[OK] Connected: {acc.login} | {acc.name} | {server} | {account_type}")
    return {
        "login": int(acc.login),
        "name": str(acc.name),
        "server": server,
        "account_type": account_type,
    }


# ============================================================
# SQLITE HELPERS
# ============================================================

def ensure_column_exists(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    definition: str,
) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = {row[1] for row in cur.fetchall()}
    if column not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        conn.commit()


def init_db(path: Path) -> sqlite3.Connection:
    ensure_dir(path.parent)
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS closed_trades (
        account_id INTEGER NOT NULL,
        position_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,

        open_time_utc TEXT NOT NULL,
        close_time_utc TEXT NOT NULL,

        entry_price REAL,
        exit_price REAL,
        price_delta REAL,

        volume_in REAL NOT NULL,
        volume_out REAL NOT NULL,

        profit_sum REAL NOT NULL,
        swap_sum REAL NOT NULL,
        commission_sum REAL NOT NULL,
        net_sum REAL NOT NULL,

        magic INTEGER NOT NULL,
        comment_last TEXT,
        account_type TEXT,
        server TEXT,

        close_ticket INTEGER PRIMARY KEY
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_closed_trades_account_close_time
    ON closed_trades(account_id, close_time_utc)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_closed_trades_symbol_close_time
    ON closed_trades(symbol, close_time_utc)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_closed_trades_position_id
    ON closed_trades(position_id)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS logger_state (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)

    conn.commit()

    ensure_column_exists(conn, "closed_trades", "account_type", "TEXT")
    ensure_column_exists(conn, "closed_trades", "server", "TEXT")

    return conn


def state_get(conn: sqlite3.Connection, key: str) -> Optional[str]:
    cur = conn.cursor()
    cur.execute("SELECT value FROM logger_state WHERE key = ?", (key,))
    row = cur.fetchone()
    return str(row[0]) if row and row[0] is not None else None


def state_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO logger_state(key, value)
    VALUES(?, ?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """, (key, value))
    conn.commit()


def db_count_rows(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM closed_trades")
    row = cur.fetchone()
    return int(row[0]) if row else 0


def db_max_close_time(conn: sqlite3.Connection) -> Optional[str]:
    cur = conn.cursor()
    cur.execute("SELECT MAX(close_time_utc) FROM closed_trades")
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0])


def db_existing_close_tickets(conn: sqlite3.Connection, tickets: List[int]) -> Set[int]:
    if not tickets:
        return set()

    placeholders = ",".join(["?"] * len(tickets))
    cur = conn.cursor()
    cur.execute(
        f"SELECT close_ticket FROM closed_trades WHERE close_ticket IN ({placeholders})",
        [int(t) for t in tickets]
    )
    rows = cur.fetchall()
    return {int(r[0]) for r in rows}


def db_upsert_closed_rows(conn: sqlite3.Connection, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    payload = []
    for r in rows:
        payload.append((
            int(r["account_id"]),
            int(r["position_id"]),
            str(r["symbol"]),
            str(r["direction"]),
            str(r["open_time_utc"]),
            str(r["close_time_utc"]),
            float(r["entry_price"]) if r["entry_price"] != "" else None,
            float(r["exit_price"]) if r["exit_price"] != "" else None,
            float(r["price_delta"]) if r["price_delta"] != "" else None,
            float(r["volume_in"]),
            float(r["volume_out"]),
            float(r["profit_sum"]),
            float(r["swap_sum"]),
            float(r["commission_sum"]),
            float(r["net_sum"]),
            int(r["magic"]),
            str(r["comment_last"]),
            str(r["account_type"]),
            str(r["server"]),
            int(r["close_ticket"]),
        ))

    cur = conn.cursor()
    before = conn.total_changes

    cur.executemany("""
    INSERT INTO closed_trades (
        account_id,
        position_id,
        symbol,
        direction,
        open_time_utc,
        close_time_utc,
        entry_price,
        exit_price,
        price_delta,
        volume_in,
        volume_out,
        profit_sum,
        swap_sum,
        commission_sum,
        net_sum,
        magic,
        comment_last,
        account_type,
        server,
        close_ticket
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(close_ticket) DO UPDATE SET
        account_id=excluded.account_id,
        position_id=excluded.position_id,
        symbol=excluded.symbol,
        direction=excluded.direction,
        open_time_utc=excluded.open_time_utc,
        close_time_utc=excluded.close_time_utc,
        entry_price=excluded.entry_price,
        exit_price=excluded.exit_price,
        price_delta=excluded.price_delta,
        volume_in=excluded.volume_in,
        volume_out=excluded.volume_out,
        profit_sum=excluded.profit_sum,
        swap_sum=excluded.swap_sum,
        commission_sum=excluded.commission_sum,
        net_sum=excluded.net_sum,
        magic=excluded.magic,
        comment_last=excluded.comment_last,
        account_type=excluded.account_type,
        server=excluded.server
    """, payload)

    conn.commit()
    changed = conn.total_changes - before
    return int(changed)


# ============================================================
# DEAL HELPERS
# ============================================================

def deals_to_df(deals) -> pd.DataFrame:
    if not deals:
        return pd.DataFrame()

    rows = []
    for d in deals:
        rows.append({
            "ticket": int(d.ticket),
            "order": int(d.order),
            "position_id": int(d.position_id),
            "symbol": str(d.symbol),
            "type": int(d.type),
            "entry": int(d.entry),
            "volume": float(d.volume),
            "price": float(d.price),
            "profit": float(d.profit),
            "swap": float(d.swap),
            "commission": float(d.commission),
            "magic": int(d.magic),
            "comment": str(d.comment),
            "time": int(d.time),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    return df.sort_values(["position_id", "time", "ticket"]).reset_index(drop=True)


def _vwap(prices: np.ndarray, vols: np.ndarray) -> float:
    v = np.asarray(vols, dtype=float)
    p = np.asarray(prices, dtype=float)
    s = float(np.sum(v))
    if not np.isfinite(s) or s <= 0:
        return float(np.nan)
    return float(np.sum(p * v) / s)


def build_closed_trade_from_position_deals(
    g: pd.DataFrame,
    account_id: int,
) -> Optional[Dict[str, Any]]:
    if g.empty:
        return None

    exit_entries = {
        getattr(mt5, "DEAL_ENTRY_OUT", 1),
        getattr(mt5, "DEAL_ENTRY_OUT_BY", 2),
    }
    in_entries = {getattr(mt5, "DEAL_ENTRY_IN", 0)}

    deal_buy = getattr(mt5, "DEAL_TYPE_BUY", 0)
    deal_sell = getattr(mt5, "DEAL_TYPE_SELL", 1)

    g = g.sort_values(["time", "ticket"]).reset_index(drop=True)

    g_in = g[g["entry"].isin(in_entries)]
    g_out = g[g["entry"].isin(exit_entries)]

    if g_out.empty:
        return None
    if g_in.empty:
        return None

    pos_id = int(g["position_id"].iloc[0])
    symbol = str(g["symbol"].iloc[-1])
    magic = int(g["magic"].iloc[-1]) if "magic" in g.columns else 0

    first_type = int(g_in["type"].iloc[0])
    direction = "BUY" if first_type == deal_buy else ("SELL" if first_type == deal_sell else str(first_type))

    open_time = int(g_in["time"].min())
    close_time = int(g_out["time"].max())
    close_ticket = int(g_out.sort_values(["time", "ticket"]).iloc[-1]["ticket"])

    entry_price = _vwap(g_in["price"].to_numpy(), g_in["volume"].to_numpy())
    exit_price = _vwap(g_out["price"].to_numpy(), g_out["volume"].to_numpy())

    vol_in = float(g_in["volume"].sum())
    vol_out = float(g_out["volume"].sum())

    profit = float(g["profit"].sum())
    swap = float(g["swap"].sum())
    commission = float(g["commission"].sum())
    net = profit + swap + commission

    price_delta = (
        float(exit_price - entry_price)
        if (np.isfinite(entry_price) and np.isfinite(exit_price))
        else np.nan
    )

    return {
        "account_id": int(account_id),
        "position_id": int(pos_id),
        "symbol": symbol,
        "direction": direction,
        "open_time_utc": ts_iso_from_unix(open_time),
        "close_time_utc": ts_iso_from_unix(close_time),
        "entry_price": float(entry_price) if np.isfinite(entry_price) else "",
        "exit_price": float(exit_price) if np.isfinite(exit_price) else "",
        "price_delta": float(price_delta) if np.isfinite(price_delta) else "",
        "volume_in": vol_in,
        "volume_out": vol_out,
        "profit_sum": profit,
        "swap_sum": swap,
        "commission_sum": commission,
        "net_sum": net,
        "magic": magic,
        "comment_last": str(g["comment"].iloc[-1]) if "comment" in g.columns else "",
        "close_ticket": int(close_ticket),
    }


def build_closed_trades_from_deals(df: pd.DataFrame, account_id: int) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    for _, g in df.groupby("position_id", sort=False):
        row = build_closed_trade_from_position_deals(g, account_id)
        if row is not None:
            rows.append(row)

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    return out.sort_values(["close_time_utc", "position_id"]).reset_index(drop=True)


def extract_recent_exit_position_ids(df: pd.DataFrame) -> List[int]:
    if df.empty:
        return []

    exit_entries = {
        getattr(mt5, "DEAL_ENTRY_OUT", 1),
        getattr(mt5, "DEAL_ENTRY_OUT_BY", 2),
    }
    g_out = df[df["entry"].isin(exit_entries)]
    if g_out.empty:
        return []

    ids = sorted({int(x) for x in g_out["position_id"].dropna().tolist() if int(x) > 0})
    return ids


def fetch_deals_range(dt_from: datetime, dt_to: datetime) -> pd.DataFrame:
    deals = mt5.history_deals_get(dt_from, dt_to) or []
    log_debug(f"history_deals_get dt_from={dt_from.isoformat()} dt_to={dt_to.isoformat()} deals={len(deals)}")
    return deals_to_df(deals)


# ============================================================
# LOGGER
# ============================================================

class MT5ClosedTradeLogger:
    def __init__(self, root: Path, account_id: int, account_type: str, server: str):
        self.account_id = int(account_id)
        self.account_type = str(account_type).upper().strip()
        self.server = str(server)

        self.dir = root / OUTPUT_BASE / f"account_{self.account_id}_{self.account_type}"
        ensure_dir(self.dir)

        self.db_path = self.dir / DB_FILENAME
        self.conn = init_db(self.db_path)

        state_last_poll = state_get(self.conn, "last_poll_utc")
        if state_last_poll:
            try:
                self.last_poll_dt = datetime.fromisoformat(state_last_poll)
                if self.last_poll_dt.tzinfo is None:
                    self.last_poll_dt = self.last_poll_dt.replace(tzinfo=timezone.utc)
            except Exception:
                self.last_poll_dt = to_utc_now() - timedelta(seconds=POLL_OVERLAP_SECONDS)
        else:
            self.last_poll_dt = to_utc_now() - timedelta(seconds=POLL_OVERLAP_SECONDS)

    def persist_state(self) -> None:
        state_set(self.conn, "last_poll_utc", self.last_poll_dt.isoformat())

    def backfill(self, lookback_days: int) -> None:
        dt_to = to_utc_now()
        dt_from = dt_to - timedelta(days=int(lookback_days))

        df = fetch_deals_range(dt_from, dt_to)
        closed = build_closed_trades_from_deals(df, self.account_id)

        log_debug(f"backfill closed_trades_found={len(closed)}")
        if not closed.empty:
            log_debug(str(closed[["position_id", "symbol", "open_time_utc", "close_time_utc", "close_ticket"]].tail(DEBUG_SHOW_LAST_ROWS)))

        self._append_new_closed(closed)

        self.last_poll_dt = dt_to - timedelta(seconds=POLL_OVERLAP_SECONDS)
        self.persist_state()

    def poll_once(self) -> int:
        dt_to = to_utc_now()
        dt_from = self.last_poll_dt - timedelta(seconds=POLL_OVERLAP_SECONDS)

        df_recent = fetch_deals_range(dt_from, dt_to)
        recent_exit_pos_ids = extract_recent_exit_position_ids(df_recent)

        log_debug(f"recent_exit_position_ids={len(recent_exit_pos_ids)}")
        if recent_exit_pos_ids:
            log_debug(f"recent_exit_position_ids_last={recent_exit_pos_ids[-DEBUG_SHOW_LAST_ROWS:]}")

        rebuilt_rows: List[Dict[str, Any]] = []

        if recent_exit_pos_ids:
            hist_from = dt_to - timedelta(days=POSITION_REBUILD_LOOKBACK_DAYS)
            df_hist = fetch_deals_range(hist_from, dt_to)

            if not df_hist.empty:
                df_hist = df_hist[df_hist["position_id"].isin(recent_exit_pos_ids)].copy()
                log_debug(f"rebuild_history_rows_for_exit_positions={len(df_hist)}")

                for pos_id, g in df_hist.groupby("position_id", sort=False):
                    row = build_closed_trade_from_position_deals(g, self.account_id)
                    if row is not None:
                        rebuilt_rows.append(row)

        rebuilt_df = pd.DataFrame(rebuilt_rows)
        log_debug(f"rebuilt_closed_trades={len(rebuilt_df)}")
        if not rebuilt_df.empty:
            log_debug(str(rebuilt_df[["position_id", "symbol", "open_time_utc", "close_time_utc", "close_ticket"]].tail(DEBUG_SHOW_LAST_ROWS)))

        changed = self._append_new_closed(rebuilt_df)

        self.last_poll_dt = dt_to
        self.persist_state()
        return changed

    def _append_new_closed(self, closed_df: pd.DataFrame) -> int:
        if closed_df is None or closed_df.empty:
            log_debug("rows_to_upsert=0")
            return 0

        rows: List[Dict[str, Any]] = []
        for _, r in closed_df.iterrows():
            row = {k: r[k] for k in closed_df.columns}
            row["account_type"] = self.account_type
            row["server"] = self.server
            rows.append(row)

        log_debug(f"rows_to_upsert={len(rows)}")

        tickets = [int(r["close_ticket"]) for r in rows]
        existing_before = db_existing_close_tickets(self.conn, tickets)
        log_debug(f"existing_close_tickets_before={len(existing_before)}")

        changed = db_upsert_closed_rows(self.conn, rows)

        if changed > 0:
            print(f"[DB][{self.account_type}][{self.server}] changed={changed} -> {self.db_path}")
        else:
            log_debug("upsert produced no db changes")

        return changed

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    exe = get_terminal_exe()
    preview_account_type = get_account_type(MT5_SERVER)

    log_info(f"terminal64.exe = {exe}")
    log_info(f"ROOT = {ROOT.resolve()}")
    log_info(
        "OUTPUT_DIR = "
        + str((ROOT / OUTPUT_BASE / f"account_{MT5_LOGIN}_{preview_account_type}").resolve())
    )

    logger: Optional[MT5ClosedTradeLogger] = None

    try:
        acc_info = connect_mt5(exe)
        account_id = int(acc_info["login"])
        account_type = str(acc_info["account_type"])
        server = str(acc_info["server"])

        logger = MT5ClosedTradeLogger(
            ROOT,
            account_id=account_id,
            account_type=account_type,
            server=server,
        )

        try:
            logger.backfill(INITIAL_LOOKBACK_DAYS)
            print(
                f"[OK] Backfill done. Folder: {logger.dir.resolve()} | "
                f"account_type={account_type} | server={server}"
            )
            print(f"[OK] DB rows: {db_count_rows(logger.conn)}")
            print(f"[OK] DB max close_time_utc: {db_max_close_time(logger.conn)}")
        except Exception as e:
            log_warn(f"Backfill failed: {e}")

        log_info(
            f"Polling every {POLL_SECONDS}s. "
            f"Account={account_id} | Type={account_type} | Server={server}. Stop with CTRL+C."
        )

        while True:
            loop_ts = to_utc_now().isoformat(timespec="seconds")
            try:
                changed = logger.poll_once()
                max_close = db_max_close_time(logger.conn)
                if changed == 0:
                    print(
                        f"[LOOP] {loop_ts} | no db changes | "
                        f"account_type={account_type} | server={server} | "
                        f"max_close_time_utc={max_close}"
                    )
                else:
                    print(
                        f"[LOOP] {loop_ts} | db_changed={changed} | "
                        f"account_type={account_type} | server={server} | "
                        f"max_close_time_utc={max_close}"
                    )
            except Exception as e:
                log_warn(f"poll failed: {e}")
            time.sleep(float(POLL_SECONDS))

    except KeyboardInterrupt:
        log_info("Stopping...")
    finally:
        mt5.shutdown()
        if logger is not None:
            logger.close()


if __name__ == "__main__":
    main()