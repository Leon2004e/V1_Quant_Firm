# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Data_Center/Data_Operations/Market/Spread_Logger_Loop/Spread_Data_Management.py

Zweck:
- Startet fixes MT5-Terminal (optional portable)
- Loggt ein
- Loggt live Spreads (bid/ask) kontinuierlich für definierte Symbole
- Speichert inkrementell in SQLite pro Symbol
- Speichert sowohl UTC als auch lokale Zeit mit
- Führt bestehende SQLite-Dateien automatisch auf das neue Schema hoch

Speichert in neuer Struktur:
  Quant_Structure/FTMO/Data_Center/Data/Spreads/<symbol>.db

Tabellenstruktur:
  table: spreads

Wichtig:
- Kein zweiter zusätzlicher 'Spreads'-Unterordner
- Output direkt in:
    Data_Center/Data/Spreads/
- Zeit wird doppelt gespeichert:
    - time_utc
    - time_local
"""

from __future__ import annotations

import os
import time
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional

import MetaTrader5 as mt5


# ============================================================
# PROJECT PATHS
# ============================================================

def find_ftmo_root(start: Path) -> Path:
    """
    Erwartet FTMO-Root:
        Quant_Structure/FTMO

    Erkennung:
    - FTMO-Root enthält 'Data_Center'
    - und in Data_Center liegt 'Data_Operations'
    """
    cur = start.resolve()
    for p in [cur] + list(cur.parents):
        if (p / "Data_Center").exists() and (p / "Data_Center" / "Data_Operations").exists():
            return p
    raise RuntimeError(
        f"FTMO-Root nicht gefunden. Erwartet Root mit "
        f"'Data_Center' und 'Data_Center/Data_Operations'. Start={start}"
    )


SCRIPT_PATH = Path(__file__).resolve()
FTMO_ROOT = find_ftmo_root(SCRIPT_PATH)

OUT_DIR = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Spreads"
)


# ============================================================
# KONFIG
# ============================================================

FIXED_MT5_DIR = Path(
    r"C:\Users\Leon\Desktop\Terminals\FTMO\MetaTrader 5 - Kopie - Kopie - Kopie (10) - Kopie - Kopie - Kopie - Kopie"
)

MT5_LOGIN = int(os.getenv("MT5_LOGIN", "540130486"))
MT5_PASSWORD = os.getenv("MT5_PASSWORD", "T4b*5J2si")
MT5_SERVER = os.getenv("MT5_SERVER", "FTMO-Server4")

PORTABLE = True
START_TERMINAL = True

INIT_TIMEOUT_MS = 20000
STARTUP_WAIT_SECONDS = 5
INIT_RETRIES = 8
RETRY_SLEEP_SECONDS = 2

POLL_SECONDS = 1.0

SYMBOLS: List[str] = [
    "AUDJPY", "AUDUSD", "EURGBP", "EURUSD", "GBPUSD", "GBPJPY",
    "NZDUSD", "US500.cash", "USDCAD", "USDCHF", "USDJPY", "USOIL.cash", "XAUUSD"
]


# ============================================================
# MT5 TERMINAL / CONNECTION
# ============================================================

def find_terminal64(root: Path) -> Path:
    exe = root / "terminal64.exe"
    if exe.exists():
        return exe

    hits = list(root.rglob("terminal64.exe"))
    if not hits:
        raise RuntimeError(f"terminal64.exe nicht gefunden unter: {root}")

    hits.sort(key=lambda p: p.stat().st_size, reverse=True)
    return hits[0]


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


def connect_mt5(exe: Path) -> int:
    if not exe.exists():
        raise RuntimeError(f"terminal64.exe nicht gefunden: {exe}")

    if not MT5_PASSWORD:
        raise RuntimeError("MT5_PASSWORD fehlt (ENV setzen)")

    mt5.shutdown()

    if START_TERMINAL:
        start_terminal(exe)
        time.sleep(STARTUP_WAIT_SECONDS)

    last_err = None
    for i in range(1, INIT_RETRIES + 1):
        if mt5.initialize(path=str(exe), portable=PORTABLE, timeout=INIT_TIMEOUT_MS):
            break
        last_err = mt5.last_error()
        print(f"[WARN] initialize failed ({i}/{INIT_RETRIES}): {last_err}")
        time.sleep(RETRY_SLEEP_SECONDS)
    else:
        raise RuntimeError(f"initialize failed: {last_err}")

    for i in range(1, INIT_RETRIES + 1):
        if mt5.login(MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER):
            break
        last_err = mt5.last_error()
        print(f"[WARN] login failed ({i}/{INIT_RETRIES}): {last_err}")
        time.sleep(RETRY_SLEEP_SECONDS)
    else:
        mt5.shutdown()
        raise RuntimeError(f"login failed: {last_err}")

    acc = mt5.account_info()
    if acc is None:
        err = mt5.last_error()
        mt5.shutdown()
        raise RuntimeError(f"account_info failed: {err}")

    print(f"[OK] Connected: {acc.login} | {acc.name} | {MT5_SERVER}")
    return int(acc.login)


# ============================================================
# IO HELPERS
# ============================================================

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def safe_symbol_filename(symbol: str) -> str:
    return symbol.replace("/", "_") + ".db"


def spread_db_path(out_dir: Path, symbol: str) -> Path:
    return out_dir / safe_symbol_filename(symbol)


def table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    rows = cur.fetchall()
    return [str(r[1]) for r in rows]


def ensure_spreads_table_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS spreads (
        time_utc TEXT NOT NULL,
        time_local TEXT,
        symbol TEXT NOT NULL,
        bid REAL NOT NULL,
        ask REAL NOT NULL,
        spread REAL NOT NULL,
        spread_points REAL,
        digits INTEGER NOT NULL,
        trade_mode INTEGER NOT NULL,
        PRIMARY KEY (time_utc, symbol)
    )
    """)

    cols = table_columns(conn, "spreads")

    if "time_local" not in cols:
        cur.execute("ALTER TABLE spreads ADD COLUMN time_local TEXT")
        conn.commit()

        cur.execute("""
            UPDATE spreads
            SET time_local = time_utc
            WHERE time_local IS NULL
        """)
        conn.commit()

        print("[INFO] SQLite migration applied: added column 'time_local'")

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_spreads_time_utc
    ON spreads(time_utc)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_spreads_time_local
    ON spreads(time_local)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_spreads_symbol_time
    ON spreads(symbol, time_utc)
    """)

    conn.commit()


def init_symbol_db(path: Path) -> sqlite3.Connection:
    ensure_dir(path.parent)
    conn = sqlite3.connect(path)
    ensure_spreads_table_schema(conn)
    return conn


def db_insert_spread_row(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    cur = conn.cursor()
    cur.execute("""
    INSERT OR IGNORE INTO spreads (
        time_utc,
        time_local,
        symbol,
        bid,
        ask,
        spread,
        spread_points,
        digits,
        trade_mode
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        str(row["time_utc"]),
        str(row["time_local"]),
        str(row["symbol"]),
        float(row["bid"]),
        float(row["ask"]),
        float(row["spread"]),
        float(row["spread_points"]) if row["spread_points"] != "" else None,
        int(row["digits"]),
        int(row["trade_mode"]),
    ))
    conn.commit()


# ============================================================
# SPREAD SAMPLING
# ============================================================

def ensure_symbol_selected(symbol: str) -> None:
    info = mt5.symbol_info(symbol)
    if info is None:
        raise RuntimeError(f"Symbol nicht gefunden: {symbol}")
    if not info.visible:
        if not mt5.symbol_select(symbol, True):
            raise RuntimeError(f"symbol_select failed: {symbol} / {mt5.last_error()}")


def sample_spread(symbol: str) -> Optional[Dict[str, Any]]:
    ensure_symbol_selected(symbol)

    info = mt5.symbol_info(symbol)
    if info is None:
        return None

    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return None

    bid = float(getattr(tick, "bid", 0.0))
    ask = float(getattr(tick, "ask", 0.0))
    if bid <= 0.0 or ask <= 0.0:
        return None

    point = float(getattr(info, "point", 0.0)) or 0.0
    digits = int(getattr(info, "digits", 0))
    trade_mode = int(getattr(info, "trade_mode", 0))

    spread = ask - bid
    spread_points = (spread / point) if point > 0 else None

    utc_now = datetime.now(timezone.utc)
    local_now = datetime.now().astimezone()

    return {
        "time_utc": utc_now.isoformat(timespec="seconds"),
        "time_local": local_now.isoformat(timespec="seconds"),
        "symbol": symbol,
        "bid": bid,
        "ask": ask,
        "spread": spread,
        "spread_points": float(spread_points) if spread_points is not None else "",
        "digits": digits,
        "trade_mode": trade_mode,
    }


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    ensure_dir(OUT_DIR)

    exe = find_terminal64(FIXED_MT5_DIR)
    print(f"[INFO] terminal64.exe = {exe}")
    print(f"[INFO] FTMO_ROOT      = {FTMO_ROOT.resolve()}")
    print(f"[INFO] OUT_DIR        = {OUT_DIR.resolve()}")

    _account_id = connect_mt5(exe)

    db_conns: Dict[str, sqlite3.Connection] = {}
    for sym in SYMBOLS:
        db_path = spread_db_path(OUT_DIR, sym)
        db_conns[sym] = init_symbol_db(db_path)
        print(f"[INFO] DB ready      = {db_path}")

    print(f"[INFO] Spread logging started: symbols={len(SYMBOLS)} poll={POLL_SECONDS}s")

    try:
        while True:
            for sym in SYMBOLS:
                try:
                    row = sample_spread(sym)
                    if row is None:
                        continue
                    db_insert_spread_row(db_conns[sym], row)
                except Exception as e:
                    print(f"[WARN] {sym} sample failed: {e}")

            time.sleep(float(POLL_SECONDS))

    except KeyboardInterrupt:
        print("[INFO] Stopping...")
    finally:
        mt5.shutdown()
        for conn in db_conns.values():
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()