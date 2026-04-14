# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Data_Center/Data_Operations/Market/Ohcl_Logger_Loop/Ohcl_Loader.py

Zweck:
- Lädt OHLC-Historie aus MetaTrader 5
- Speichert pro Symbol/Timeframe eine Parquet-Datei
- Baut unvollständige Historien automatisch neu auf
- Aktualisiert bestehende Dateien inkrementell
- Schreibt zusätzlich Metadaten direkt in die Parquet-Datei:
    - from_utc
    - to_utc
    - rows
    - symbol
    - timeframe
    - written_at_utc
    - required_history_start_utc
- Q und Y werden aus MN1 resampled

Neue Output-Struktur:
Quant_Structure/FTMO/Data_Center/Data/Ohcl/Raw/<TF>/<SYMBOL>.parquet
Quant_Structure/FTMO/Data_Center/Data/Ohcl/Raw/summary.json

Wichtig:
- Kein zusätzlicher Unterordner OHLC unter Raw
- Raw ist direkt die Wurzel für die Timeframe-Ordner
- MT5 wird über einen festen terminal64.exe-Pfad gestartet
- Keine automatische Suche nach Terminals
- Historie soll mindestens ab REQUIRED_HISTORY_START_UTC vorliegen
- Existierende, zu kurze Historien werden automatisch vollständig neu aufgebaut
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import MetaTrader5 as mt5
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


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
        f"'Data_Center' und 'Data_Center/Data_Operations'. Start: {start}"
    )


SCRIPT_PATH = Path(__file__).resolve()
FTMO_ROOT = find_ftmo_root(SCRIPT_PATH)

OUT_DIR = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Ohcl"
    / "Raw"
)


# ============================================================
# KONFIG
# ============================================================

TERMINAL_EXE_PATH = Path(
    r"C:\Users\Leon\Desktop\Terminals\MetaTrader 5 - Kopie - Kopie - Kopie (19) - Kopie - Kopie - Kopie - Kopie - Kopie - Kopie\terminal64.exe"
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

SYMBOLS: List[str] = [
    "AUDJPY", "AUDUSD", "EURGBP", "EURUSD", "GBPUSD", "GBPJPY",
    "NZDUSD", "US500.cash", "USDCAD", "USDCHF", "USDJPY", "USOIL.cash", "XAUUSD"
]

BASE_TIMEFRAMES: List[str] = ["M1", "M5", "M15", "H1", "H4", "H8", "H12", "D1", "W1", "MN1"]
DERIVED_TIMEFRAMES: List[str] = ["Q", "Y"]

CHUNK_DAYS = 180
SLEEP_BETWEEN_CALLS = 0.05

UPDATE_EVERY_SECONDS = 30.0
SAFETY_LOOKBACK_BARS = 5
TIME_COL = "time"

REQUIRED_HISTORY_START_UTC = pd.Timestamp("2022-01-01 00:00:00+00:00")

TF_MAP = {
    "M1": mt5.TIMEFRAME_M1,
    "M2": mt5.TIMEFRAME_M2,
    "M3": mt5.TIMEFRAME_M3,
    "M4": mt5.TIMEFRAME_M4,
    "M5": mt5.TIMEFRAME_M5,
    "M6": mt5.TIMEFRAME_M6,
    "M10": mt5.TIMEFRAME_M10,
    "M12": mt5.TIMEFRAME_M12,
    "M15": mt5.TIMEFRAME_M15,
    "M20": mt5.TIMEFRAME_M20,
    "M30": mt5.TIMEFRAME_M30,
    "H1": mt5.TIMEFRAME_H1,
    "H2": mt5.TIMEFRAME_H2,
    "H3": mt5.TIMEFRAME_H3,
    "H4": mt5.TIMEFRAME_H4,
    "H6": mt5.TIMEFRAME_H6,
    "H8": mt5.TIMEFRAME_H8,
    "H12": mt5.TIMEFRAME_H12,
    "D1": mt5.TIMEFRAME_D1,
    "W1": mt5.TIMEFRAME_W1,
    "MN1": mt5.TIMEFRAME_MN1,
}


# ============================================================
# MT5 Terminal / Connection
# ============================================================

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
# Helpers / IO
# ============================================================

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def safe_symbol_filename(symbol: str) -> str:
    return symbol.replace("/", "_") + ".parquet"


def ohlc_path(out_dir: Path, symbol: str, tf: str) -> Path:
    return out_dir / tf / safe_symbol_filename(symbol)


def normalize_ohlc_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "time", "open", "high", "low", "close",
            "tick_volume", "spread", "real_volume",
            "symbol", "timeframe"
        ])

    out = df.copy()

    keep = ["time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume"]
    for c in keep:
        if c not in out.columns:
            out[c] = 0

    if "symbol" not in out.columns:
        out["symbol"] = ""
    if "timeframe" not in out.columns:
        out["timeframe"] = ""

    out["time"] = pd.to_datetime(out["time"], utc=True, errors="coerce")
    out = out.dropna(subset=["time"])

    for c in ["open", "high", "low", "close", "tick_volume", "spread", "real_volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out["symbol"] = out["symbol"].astype(str)
    out["timeframe"] = out["timeframe"].astype(str)

    out = (
        out[["time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume", "symbol", "timeframe"]]
        .drop_duplicates(subset=["time"], keep="last")
        .sort_values("time")
        .reset_index(drop=True)
    )
    return out


def build_parquet_metadata(df: pd.DataFrame, symbol: str, tf: str) -> Dict[bytes, bytes]:
    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    if df is None or df.empty:
        meta = {
            "symbol": symbol,
            "timeframe": tf,
            "rows": "0",
            "from_utc": "",
            "to_utc": "",
            "written_at_utc": now_utc,
            "required_history_start_utc": REQUIRED_HISTORY_START_UTC.isoformat(),
        }
    else:
        time_min = pd.to_datetime(df["time"].min(), utc=True)
        time_max = pd.to_datetime(df["time"].max(), utc=True)
        meta = {
            "symbol": symbol,
            "timeframe": tf,
            "rows": str(int(len(df))),
            "from_utc": time_min.isoformat(),
            "to_utc": time_max.isoformat(),
            "written_at_utc": now_utc,
            "required_history_start_utc": REQUIRED_HISTORY_START_UTC.isoformat(),
        }

    return {str(k).encode("utf-8"): str(v).encode("utf-8") for k, v in meta.items()}


def atomic_write_df_parquet(df: pd.DataFrame, path: Path, symbol: str, tf: str) -> None:
    ensure_dir(path.parent)

    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=path.stem + "_",
        suffix=".tmp",
        dir=str(path.parent),
    )
    os.close(tmp_fd)
    tmp_path = Path(tmp_name)

    try:
        norm = normalize_ohlc_df(df)
        table = pa.Table.from_pandas(norm, preserve_index=False)

        existing_meta = table.schema.metadata or {}
        new_meta = build_parquet_metadata(norm, symbol=symbol, tf=tf)
        merged_meta = dict(existing_meta)
        merged_meta.update(new_meta)

        table = table.replace_schema_metadata(merged_meta)
        pq.write_table(table, tmp_path)
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


def atomic_write_json(obj: dict, path: Path) -> None:
    ensure_dir(path.parent)

    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=path.stem + "_",
        suffix=".tmp",
        dir=str(path.parent),
    )
    os.close(tmp_fd)
    tmp_path = Path(tmp_name)

    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2, ensure_ascii=False)
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


def ensure_symbol_selected(symbol: str) -> None:
    info = mt5.symbol_info(symbol)
    if info is None:
        raise RuntimeError(f"Symbol nicht gefunden: {symbol}")
    if not info.visible:
        if not mt5.symbol_select(symbol, True):
            raise RuntimeError(f"symbol_select failed: {symbol} / {mt5.last_error()}")


def get_parquet_metadata(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}

    try:
        pf = pq.ParquetFile(path)
        raw = pf.schema_arrow.metadata or {}
        return {
            k.decode("utf-8", errors="ignore"): v.decode("utf-8", errors="ignore")
            for k, v in raw.items()
        }
    except Exception:
        return {}


# ============================================================
# Data fetch
# ============================================================

def chunked_ranges(dt_from: datetime, dt_to: datetime, chunk_days: int) -> List[Tuple[datetime, datetime]]:
    out: List[Tuple[datetime, datetime]] = []
    cur = dt_from
    step = timedelta(days=int(chunk_days))
    while cur < dt_to:
        nxt = min(dt_to, cur + step)
        out.append((cur, nxt))
        cur = nxt
    return out


def fetch_rates_range(symbol: str, tf: str, dt_from: datetime, dt_to: datetime) -> pd.DataFrame:
    ensure_symbol_selected(symbol)

    tf_enum = TF_MAP.get(tf)
    if tf_enum is None:
        raise ValueError(f"Unbekannter Timeframe: {tf}")

    rates = mt5.copy_rates_range(symbol, tf_enum, dt_from, dt_to)
    if rates is None:
        err = mt5.last_error()
        raise RuntimeError(f"copy_rates_range failed: {symbol} {tf} | {err}")

    df = pd.DataFrame(rates)
    if df.empty:
        return df

    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    keep = ["time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume"]
    for c in keep:
        if c not in df.columns:
            df[c] = 0
    df = df[keep].copy()
    df["symbol"] = symbol
    df["timeframe"] = tf
    return normalize_ohlc_df(df)


def dump_full_history(symbol: str, tf: str, dt_from: datetime, dt_to: datetime) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []

    for a, b in chunked_ranges(dt_from, dt_to, CHUNK_DAYS):
        df = fetch_rates_range(symbol, tf, a, b)
        if not df.empty:
            parts.append(df)
        time.sleep(SLEEP_BETWEEN_CALLS)

    if not parts:
        return pd.DataFrame()

    out = pd.concat(parts, ignore_index=True)
    out = normalize_ohlc_df(out)
    return out


def read_existing_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_parquet(path)
    return normalize_ohlc_df(df)


def fetch_last_n_bars(symbol: str, tf: str, n: int) -> pd.DataFrame:
    ensure_symbol_selected(symbol)

    tf_enum = TF_MAP[tf]
    rates = mt5.copy_rates_from_pos(symbol, tf_enum, 0, int(n))
    if rates is None:
        err = mt5.last_error()
        raise RuntimeError(f"copy_rates_from_pos failed: {symbol} {tf} | {err}")

    df = pd.DataFrame(rates)
    if df.empty:
        return df

    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    keep = ["time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume"]
    for c in keep:
        if c not in df.columns:
            df[c] = 0
    df = df[keep].copy()
    df["symbol"] = symbol
    df["timeframe"] = tf
    return normalize_ohlc_df(df)


def merge_and_dedup(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    if existing is None or existing.empty:
        out = new.copy()
    elif new is None or new.empty:
        out = existing.copy()
    else:
        out = pd.concat([existing, new], ignore_index=True)

    return normalize_ohlc_df(out)


# ============================================================
# Resample (MN1 -> Q/Y)
# ============================================================

def resample_ohlc_from_mn1(mn1_df: pd.DataFrame, rule: str) -> pd.DataFrame:
    if mn1_df is None or mn1_df.empty:
        return pd.DataFrame()

    d = normalize_ohlc_df(mn1_df).copy()
    d = d.set_index("time")

    agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "tick_volume": "sum",
        "real_volume": "sum",
        "spread": "mean",
    }

    out = d.resample(rule).agg(agg)
    out = out.dropna(subset=["open", "high", "low", "close"]).reset_index()
    return out


def write_qy_from_mn1(symbol: str, summary_sym: Dict[str, object]) -> None:
    mn1_path = ohlc_path(OUT_DIR, symbol, "MN1")
    if not mn1_path.exists():
        summary_sym["Q"] = {"status": "skipped", "reason": "MN1_missing"}
        summary_sym["Y"] = {"status": "skipped", "reason": "MN1_missing"}
        return

    mn1 = read_existing_parquet(mn1_path)
    if mn1.empty:
        summary_sym["Q"] = {"status": "empty"}
        summary_sym["Y"] = {"status": "empty"}
        return

    for c in ["tick_volume", "real_volume", "spread"]:
        if c not in mn1.columns:
            mn1[c] = 0

    q = resample_ohlc_from_mn1(mn1, "Q")
    y = resample_ohlc_from_mn1(mn1, "Y")

    if not q.empty:
        q["symbol"] = symbol
        q["timeframe"] = "Q"
        q = normalize_ohlc_df(q)
        qp = ohlc_path(OUT_DIR, symbol, "Q")
        atomic_write_df_parquet(q, qp, symbol=symbol, tf="Q")
        summary_sym["Q"] = {
            "status": "ok",
            "rows": int(len(q)),
            "from_utc": str(pd.to_datetime(q["time"], utc=True).min()),
            "to_utc": str(pd.to_datetime(q["time"], utc=True).max()),
            "file": str(qp),
            "parquet_metadata": get_parquet_metadata(qp),
        }
    else:
        summary_sym["Q"] = {"status": "empty"}

    if not y.empty:
        y["symbol"] = symbol
        y["timeframe"] = "Y"
        y = normalize_ohlc_df(y)
        yp = ohlc_path(OUT_DIR, symbol, "Y")
        atomic_write_df_parquet(y, yp, symbol=symbol, tf="Y")
        summary_sym["Y"] = {
            "status": "ok",
            "rows": int(len(y)),
            "from_utc": str(pd.to_datetime(y["time"], utc=True).min()),
            "to_utc": str(pd.to_datetime(y["time"], utc=True).max()),
            "file": str(yp),
            "parquet_metadata": get_parquet_metadata(yp),
        }
    else:
        summary_sym["Y"] = {"status": "empty"}


# ============================================================
# Update / Rebuild logic
# ============================================================

def needs_full_rebuild(existing: pd.DataFrame) -> Tuple[bool, str]:
    if existing is None or existing.empty:
        return True, "missing_or_empty"

    if "time" not in existing.columns:
        return True, "time_column_missing"

    tmin = pd.to_datetime(existing["time"].min(), utc=True, errors="coerce")
    tmax = pd.to_datetime(existing["time"].max(), utc=True, errors="coerce")

    if pd.isna(tmin) or pd.isna(tmax):
        return True, "invalid_time_range"

    if tmin > REQUIRED_HISTORY_START_UTC:
        return True, f"history_too_short:{tmin.isoformat()}"

    return False, "ok"


def initial_full_dump(symbol: str, tf: str, now: datetime) -> pd.DataFrame:
    dt_from = REQUIRED_HISTORY_START_UTC.to_pydatetime()
    return dump_full_history(symbol, tf, dt_from, now)


def incremental_update(symbol: str, tf: str, now: datetime) -> pd.DataFrame:
    path = ohlc_path(OUT_DIR, symbol, tf)
    existing = read_existing_parquet(path)

    rebuild, rebuild_reason = needs_full_rebuild(existing)
    if rebuild:
        print(f"[INFO] rebuild required {symbol} {tf}: {rebuild_reason}")
        return initial_full_dump(symbol, tf, now)

    last_time = pd.to_datetime(existing["time"].max(), utc=True)
    tail = fetch_last_n_bars(symbol, tf, SAFETY_LOOKBACK_BARS)

    dt_from = last_time.to_pydatetime().replace(tzinfo=timezone.utc) - timedelta(days=3)
    rng = fetch_rates_range(symbol, tf, dt_from, now)

    merged = merge_and_dedup(existing, rng)
    merged = merge_and_dedup(merged, tail)
    return merged


# ============================================================
# MAIN LOOP
# ============================================================

def main() -> None:
    ensure_dir(OUT_DIR)

    print(f"[INFO] FTMO_ROOT                  = {FTMO_ROOT}")
    print(f"[INFO] OUT_DIR                    = {OUT_DIR}")
    print(f"[INFO] TERMINAL_EXE               = {TERMINAL_EXE_PATH}")
    print(f"[INFO] REQUIRED_HISTORY_START_UTC = {REQUIRED_HISTORY_START_UTC}")

    if not TERMINAL_EXE_PATH.exists():
        raise RuntimeError(f"terminal64.exe nicht gefunden: {TERMINAL_EXE_PATH}")

    exe = TERMINAL_EXE_PATH
    account_id = connect_mt5(exe)

    summary: Dict[str, Dict[str, object]] = {
        sym: {
            "meta": {
                "account_id": account_id,
                "required_history_start_utc": REQUIRED_HISTORY_START_UTC.isoformat(),
            }
        }
        for sym in SYMBOLS
    }

    sum_path = OUT_DIR / "summary.json"

    try:
        now = datetime.now(timezone.utc)

        # ----------------------------------------------------
        # INITIAL PASS
        # ----------------------------------------------------
        for sym in SYMBOLS:
            for tf in BASE_TIMEFRAMES:
                try:
                    p = ohlc_path(OUT_DIR, sym, tf)
                    existing = read_existing_parquet(p) if p.exists() else pd.DataFrame()

                    rebuild, reason = needs_full_rebuild(existing)

                    if rebuild:
                        df = initial_full_dump(sym, tf, now)
                        if df.empty:
                            summary[sym][tf] = {
                                "status": "empty",
                                "reason": reason,
                            }
                            continue

                        atomic_write_df_parquet(df, p, symbol=sym, tf=tf)
                        meta = get_parquet_metadata(p)

                        summary[sym][tf] = {
                            "status": "rebuilt",
                            "reason": reason,
                            "rows": int(len(df)),
                            "from_utc": str(df["time"].min()),
                            "to_utc": str(df["time"].max()),
                            "file": str(p),
                            "parquet_metadata": meta,
                        }
                        print(f"[OK] init/rebuild {sym} {tf}: rows={len(df)} -> {p}")
                    else:
                        meta = get_parquet_metadata(p)
                        summary[sym][tf] = {
                            "status": "exists",
                            "rows": int(len(existing)),
                            "from_utc": str(existing["time"].min()),
                            "to_utc": str(existing["time"].max()),
                            "file": str(p),
                            "parquet_metadata": meta,
                        }
                        print(
                            f"[OK] exists {sym} {tf}: "
                            f"rows={len(existing)} from={existing['time'].min()} to={existing['time'].max()}"
                        )

                except Exception as e:
                    summary[sym][tf] = {"status": "error", "error": str(e)}
                    print(f"[WARN] init {sym} {tf} failed: {e}")

            try:
                write_qy_from_mn1(sym, summary[sym])
            except Exception as e:
                summary[sym]["Q"] = {"status": "error", "error": str(e)}
                summary[sym]["Y"] = {"status": "error", "error": str(e)}
                print(f"[WARN] init {sym} Q/Y failed: {e}")

        atomic_write_json(summary, sum_path)
        print(f"[DONE] Initial summary -> {sum_path.resolve()}")

        # ----------------------------------------------------
        # LOOP
        # ----------------------------------------------------
        while True:
            loop_now = datetime.now(timezone.utc)

            for sym in SYMBOLS:
                for tf in BASE_TIMEFRAMES:
                    try:
                        p = ohlc_path(OUT_DIR, sym, tf)
                        df_new = incremental_update(sym, tf, loop_now)

                        if df_new.empty:
                            summary[sym][tf] = {
                                "status": "empty",
                                "updated_at_utc": loop_now.isoformat(timespec="seconds"),
                            }
                            continue

                        atomic_write_df_parquet(df_new, p, symbol=sym, tf=tf)
                        meta = get_parquet_metadata(p)

                        summary[sym][tf] = {
                            "status": "ok",
                            "rows": int(len(df_new)),
                            "from_utc": str(df_new["time"].min()),
                            "to_utc": str(df_new["time"].max()),
                            "file": str(p),
                            "updated_at_utc": loop_now.isoformat(timespec="seconds"),
                            "parquet_metadata": meta,
                        }

                    except Exception as e:
                        summary[sym][tf] = {
                            "status": "error",
                            "error": str(e),
                            "updated_at_utc": loop_now.isoformat(timespec="seconds"),
                        }
                        print(f"[WARN] update {sym} {tf} failed: {e}")

                try:
                    write_qy_from_mn1(sym, summary[sym])
                except Exception as e:
                    summary[sym]["Q"] = {"status": "error", "error": str(e)}
                    summary[sym]["Y"] = {"status": "error", "error": str(e)}
                    print(f"[WARN] update {sym} Q/Y failed: {e}")

            atomic_write_json(summary, sum_path)
            print(f"[LOOP] updated_at_utc={loop_now.isoformat(timespec='seconds')} -> summary.json")

            time.sleep(UPDATE_EVERY_SECONDS)

    except KeyboardInterrupt:
        print("[INFO] Stopping...")
        atomic_write_json(summary, sum_path)
    finally:
        mt5.shutdown()


if __name__ == "__main__":
    main()