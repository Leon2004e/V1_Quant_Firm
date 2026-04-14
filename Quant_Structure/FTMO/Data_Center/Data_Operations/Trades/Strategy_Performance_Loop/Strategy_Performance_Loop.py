# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Data_Center/Data_Operations/Trades/Strategy_Performance_Loop/Strategy_Performance_Loop.py

Zweck:
- Überwacht laufend alle account_<id>_<type>/closed_trades.db Dateien unter:
    Data_Center/Data/Trades/Raw/Live/
- Sortiert Closed Trades sauber in Strategie-Buckets
- Nutzt Strategy_EA-Dateinamen als primäre Mapping-Quelle:
    <symbol>_<magic>_<strategy_id>_<side>_<timeframe>.mq5
- Exportiert pro Strategie in SQLite nach:
    Data_Center/Data/Trades/Featured/Live/account_<id>_<type>/<bucket>/trades.db

trades.db enthält:
  - trades
  - kpis
  - weekly_performance
  - monthly_performance

Wichtig:
- Input = Raw Closed Trades
- Output = organisierte / strategiebezogene Trades
- Keine MT5-Verbindung nötig
"""

from __future__ import annotations

import json
import os
import re
import time
import sqlite3
import tempfile
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


# ============================================================
# PROJECT ROOT / PATHS
# ============================================================

def find_project_root(start: Path) -> Path:
    cur = start.resolve()
    for p in [cur] + list(cur.parents):
        if (p / "Data_Center").exists():
            return p
    raise RuntimeError(f"Projekt-Root nicht gefunden (kein 'Data_Center' in Parents). Start={start}")


SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = find_project_root(SCRIPT_PATH)

INPUT_LIVE_ROOT = (
    PROJECT_ROOT
    / "Data_Center"
    / "Data"
    / "Trades"
    / "Raw"
    / "Live"
)

OUTPUT_LIVE_ROOT = (
    PROJECT_ROOT
    / "Data_Center"
    / "Data"
    / "Trades"
    / "Featured"
    / "Live"
)

STRATEGY_EA_ROOT = (
    PROJECT_ROOT
    / "Data_Center"
    / "Data"
    / "Strategy"
    / "Strategy_EA"
)

RUNTIME_DIR = (
    PROJECT_ROOT
    / "Data_Center"
    / "Data_Operations"
    / "Trades"
    / "Strategy_Performance_Loop"
    / "runtime"
)

STATE_FILE = RUNTIME_DIR / "state.json"

REGISTRY_FILE = (
    PROJECT_ROOT
    / "Data_Center"
    / "Data"
    / "Strategy"
    / "Strategy_Profile"
    / "strategy_registry.csv"
)


# ============================================================
# CONFIG
# ============================================================

POLL_SECONDS = 30.0
START_EQUITY = 100000.0

GLOBAL_CUTOFF_DATE_UTC = pd.Timestamp("2026-02-22 00:00:00", tz="UTC")

INPUT_DB_FILENAME = "closed_trades.db"
INPUT_TABLE_NAME = "closed_trades"

OUTPUT_DB_FILENAME = "trades.db"

TRADES_REQUIRED_COLS = [
    "account_id",
    "position_id",
    "symbol",
    "direction",
    "open_time_utc",
    "close_time_utc",
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
    "comment_last",
    "close_ticket",
]

_STRAT_COMMENT_RE = re.compile(r"Strategy\s+([0-9]+(?:\.[0-9]+)*)", re.IGNORECASE)

_EA_FILENAME_RE = re.compile(
    r"^(?P<symbol>[^_]+)_(?P<magic>\d+)_(?P<strategy_id>.+)_(?P<side>BUY|SELL|BOTH)_(?P<timeframe>[A-Z0-9]+)\.mq5$",
    re.IGNORECASE,
)


# ============================================================
# HELPERS
# ============================================================

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sanitize_name(s: object) -> str:
    x = str(s).strip()
    x = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", x)
    x = re.sub(r"\s+", "_", x)
    return x[:200] if len(x) > 200 else x


def atomic_write_json(obj: dict, path: Path) -> None:
    ensure_dir(path.parent)
    fd, tmp_name = tempfile.mkstemp(prefix=path.stem + "_", suffix=".tmp", dir=str(path.parent))
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2)
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


def parse_strategy_from_comment(comment: object) -> Optional[str]:
    m = _STRAT_COMMENT_RE.search(str(comment))
    return m.group(1).strip() if m else None


def parse_time_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def safe_float_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0).astype(float)


def safe_int_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def apply_global_cutoff_filter(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    d = df.copy()
    d["close_time_utc"] = pd.to_datetime(d["close_time_utc"], errors="coerce", utc=True)
    d = d.dropna(subset=["close_time_utc"])
    d = d[d["close_time_utc"] >= GLOBAL_CUTOFF_DATE_UTC].copy()
    return d.reset_index(drop=True)


def ts_to_str(ts: Optional[pd.Timestamp]) -> str:
    if ts is None or pd.isna(ts):
        return ""
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat()


def get_measurement_day_utc() -> pd.Timestamp:
    now_utc = datetime.now(timezone.utc)
    return pd.Timestamp(now_utc.date(), tz="UTC")


def get_strategy_effective_window(trades: pd.DataFrame) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    if trades.empty:
        return None, None

    close_times = pd.to_datetime(trades["close_time_utc"], errors="coerce", utc=True).dropna()
    if close_times.empty:
        return None, None

    strategy_start = close_times.min()
    strategy_end = close_times.max()
    return strategy_start, strategy_end


def build_bucket_date_range_for_measurement(trades: pd.DataFrame, measurement_day_utc: pd.Timestamp) -> Tuple[str, str]:
    start_ts, _ = get_strategy_effective_window(trades)

    if start_ts is None or pd.isna(start_ts):
        return "unknown", measurement_day_utc.strftime("%Y-%m-%d")

    start_day = pd.Timestamp(start_ts).tz_convert("UTC").normalize()
    measurement_day = pd.Timestamp(measurement_day_utc).tz_convert("UTC").normalize()

    if measurement_day < start_day:
        measurement_day = start_day

    return start_day.strftime("%Y-%m-%d"), measurement_day.strftime("%Y-%m-%d")


def bucket_folder_name(
    symbol: str,
    magic: object,
    strategy_id: str,
    side: str,
    start_date: str,
    end_date: str,
) -> str:
    return (
        f"{sanitize_name(symbol)}_"
        f"{sanitize_name(magic)}_"
        f"{sanitize_name(strategy_id)}_"
        f"{sanitize_name(side)}_"
        f"{sanitize_name(start_date)}_to_{sanitize_name(end_date)}"
    )


# ============================================================
# STATE
# ============================================================

@dataclass
class AccountState:
    account_id: int
    last_max_close_ticket: Optional[int] = None
    last_rows: int = 0
    last_updated_utc: Optional[str] = None


def load_state() -> Dict[str, AccountState]:
    ensure_dir(RUNTIME_DIR)
    if not STATE_FILE.exists():
        return {}

    try:
        raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

    out: Dict[str, AccountState] = {}
    for account_id, payload in raw.items():
        try:
            out[account_id] = AccountState(**payload)
        except Exception:
            pass
    return out


def save_state(state: Dict[str, AccountState]) -> None:
    payload = {k: asdict(v) for k, v in state.items()}
    atomic_write_json(payload, STATE_FILE)


# ============================================================
# REGISTRY / EA MAPPING
# ============================================================

def load_registry(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["magic", "strategy_id"])

    reg = pd.read_csv(path)
    if "magic" not in reg.columns or "strategy_id" not in reg.columns:
        return pd.DataFrame(columns=["magic", "strategy_id"])

    reg = reg.copy()
    reg["magic"] = pd.to_numeric(reg["magic"], errors="coerce").astype("Int64")
    reg["strategy_id"] = reg["strategy_id"].astype(str).fillna("").str.strip()
    reg = reg[reg["strategy_id"].str.len() > 0].copy()
    reg = reg.drop_duplicates(subset=["magic"], keep="last").reset_index(drop=True)
    return reg[["magic", "strategy_id"]]


def build_strategy_ea_mapping(strategy_ea_root: Path) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []

    if not strategy_ea_root.exists():
        return pd.DataFrame(columns=["strategy_id", "symbol", "magic", "side", "timeframe", "filename"])

    for symbol_dir in strategy_ea_root.iterdir():
        if not symbol_dir.is_dir():
            continue

        for p in symbol_dir.glob("*.mq5"):
            m = _EA_FILENAME_RE.match(p.name)
            if not m:
                continue

            rows.append(
                {
                    "strategy_id": str(m.group("strategy_id")).strip(),
                    "symbol": str(m.group("symbol")).strip(),
                    "magic": int(m.group("magic")),
                    "side": str(m.group("side")).strip().upper(),
                    "timeframe": str(m.group("timeframe")).strip().upper(),
                    "filename": p.name,
                }
            )

    if not rows:
        return pd.DataFrame(columns=["strategy_id", "symbol", "magic", "side", "timeframe", "filename"])

    df = pd.DataFrame(rows)
    df["magic"] = pd.to_numeric(df["magic"], errors="coerce").astype("Int64")
    df["strategy_id"] = df["strategy_id"].astype(str).fillna("").str.strip()
    df["symbol"] = df["symbol"].astype(str).fillna("").str.strip()
    df["side"] = df["side"].astype(str).fillna("").str.strip().str.upper()
    df["timeframe"] = df["timeframe"].astype(str).fillna("").str.strip().str.upper()

    expanded_rows: List[Dict[str, object]] = []
    for _, row in df.iterrows():
        side = str(row["side"]).upper()
        row_dict = row.to_dict()

        if side == "BOTH":
            for actual_side in ["BUY", "SELL"]:
                r = dict(row_dict)
                r["side"] = actual_side
                expanded_rows.append(r)
        else:
            expanded_rows.append(row_dict)

    df = pd.DataFrame(expanded_rows)

    df = df.drop_duplicates(subset=["symbol", "magic", "side"], keep="first").reset_index(drop=True)
    return df


# ============================================================
# SQLITE HELPERS
# ============================================================

def read_closed_trades_db(path: Path, table_name: str = INPUT_TABLE_NAME) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=TRADES_REQUIRED_COLS)

    conn = None
    try:
        conn = sqlite3.connect(path)
        query = f"""
        SELECT
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
            close_ticket
        FROM {table_name}
        """
        df = pd.read_sql_query(query, conn)
    finally:
        if conn is not None:
            conn.close()

    missing = [c for c in TRADES_REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {path}:{table_name}: {missing}")

    df = df[TRADES_REQUIRED_COLS].copy()

    for c in ["account_id", "position_id", "magic", "close_ticket"]:
        df[c] = safe_int_series(df[c])

    for c in [
        "entry_price", "exit_price", "price_delta",
        "volume_in", "volume_out",
        "profit_sum", "swap_sum", "commission_sum", "net_sum",
    ]:
        df[c] = safe_float_series(df[c])

    df["open_time_utc"] = parse_time_utc(df["open_time_utc"])
    df["close_time_utc"] = parse_time_utc(df["close_time_utc"])

    for c in ["symbol", "direction", "comment_last"]:
        df[c] = df[c].astype(str).fillna("").str.strip()

    df["direction"] = df["direction"].str.upper()

    df = df.dropna(subset=["account_id", "position_id", "close_ticket", "close_time_utc"]).copy()
    df = df.sort_values(["close_time_utc", "position_id", "close_ticket"]).reset_index(drop=True)

    return df


def sqlite_write_table_replace(db_path: Path, table_name: str, df: pd.DataFrame) -> None:
    ensure_dir(db_path.parent)
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.commit()
    finally:
        if conn is not None:
            conn.close()


def sqlite_create_indexes_for_strategy_db(db_path: Path) -> None:
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_close_time ON trades(close_time_utc)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_close_ticket ON trades(close_ticket)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_magic ON trades(magic)")

        cur.execute("CREATE INDEX IF NOT EXISTS idx_weekly_date ON weekly_performance(date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_monthly_date ON monthly_performance(date)")

        conn.commit()
    finally:
        if conn is not None:
            conn.close()


# ============================================================
# IO / NORMALIZATION
# ============================================================

def find_account_dirs(root: Path) -> List[Path]:
    if not root.exists():
        return []
    out = []
    for p in root.iterdir():
        if p.is_dir() and p.name.startswith("account_"):
            out.append(p)
    out.sort(key=lambda x: x.name)
    return out


def enrich_strategy_ids(df: pd.DataFrame, reg: pd.DataFrame, ea_map: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()

    if not ea_map.empty:
        d = d.merge(
            ea_map[["strategy_id", "symbol", "magic", "side"]].rename(columns={"side": "direction"}),
            on=["symbol", "magic", "direction"],
            how="left",
            suffixes=("", "_ea"),
        )
        d["strategy_id_from_ea"] = d["strategy_id"]
    else:
        d["strategy_id_from_ea"] = None

    d["strategy_id_from_comment"] = d["comment_last"].apply(parse_strategy_from_comment)

    if not reg.empty:
        d = d.merge(
            reg.rename(columns={"strategy_id": "strategy_id_reg"}),
            on="magic",
            how="left",
        )
    else:
        d["strategy_id_reg"] = None

    d["strategy_id"] = d["strategy_id_from_ea"]

    missing = d["strategy_id"].isna() | (d["strategy_id"].astype(str).str.strip().isin(["", "nan", "None"]))
    d.loc[missing, "strategy_id"] = d.loc[missing, "strategy_id_from_comment"]

    missing = d["strategy_id"].isna() | (d["strategy_id"].astype(str).str.strip().isin(["", "nan", "None"]))
    d.loc[missing, "strategy_id"] = d.loc[missing, "strategy_id_reg"]

    d["strategy_id"] = d["strategy_id"].astype(str).replace(["nan", "None"], "").str.strip()

    d = d.drop(
        columns=["strategy_id_from_ea", "strategy_id_from_comment", "strategy_id_reg"],
        errors="ignore",
    )

    return d


# ============================================================
# KPI / PERFORMANCE
# ============================================================

def bucket_trades(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out.sort_values(["close_time_utc", "position_id", "close_ticket"]).reset_index(drop=True)
    return out


def kpis_from_trades(
    trades: pd.DataFrame,
    global_cutoff_utc: pd.Timestamp,
    measurement_day_utc: pd.Timestamp,
) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()

    net = trades["net_sum"].astype(float)
    wins = int((net > 0).sum())
    losses = int((net < 0).sum())
    n = int(len(trades))

    gross_profit = float(net[net > 0].sum()) if wins > 0 else 0.0
    gross_loss = float(net[net < 0].sum()) if losses > 0 else 0.0
    total_net = float(net.sum())
    avg_trade = float(net.mean()) if n > 0 else 0.0
    win_rate = float(wins / n) if n > 0 else 0.0
    profit_factor = float(gross_profit / abs(gross_loss)) if gross_loss < 0 else None

    eq = net.cumsum()
    roll_max = eq.cummax()
    dd = eq - roll_max
    max_drawdown = float(dd.min()) if len(dd) else 0.0

    first_open_time = pd.to_datetime(trades["open_time_utc"], utc=True, errors="coerce").min()
    first_close_time = pd.to_datetime(trades["close_time_utc"], utc=True, errors="coerce").min()
    last_close_time = pd.to_datetime(trades["close_time_utc"], utc=True, errors="coerce").max()

    strategy_live_start_utc, strategy_last_trade_close_utc = get_strategy_effective_window(trades)

    out = pd.DataFrame([
        {
            "base_cutoff_utc": ts_to_str(global_cutoff_utc),
            "measurement_day_utc": ts_to_str(measurement_day_utc),
            "strategy_live_start_utc": ts_to_str(strategy_live_start_utc if strategy_live_start_utc is not None else first_close_time),
            "strategy_live_end_utc": ts_to_str(measurement_day_utc),
            "strategy_last_trade_close_utc": ts_to_str(strategy_last_trade_close_utc if strategy_last_trade_close_utc is not None else last_close_time),
            "first_open_time_utc": ts_to_str(first_open_time) if pd.notna(first_open_time) else "",
            "first_close_time_utc": ts_to_str(first_close_time) if pd.notna(first_close_time) else "",
            "last_close_time_utc": ts_to_str(last_close_time) if pd.notna(last_close_time) else "",
            "net_pnl": total_net,
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "profit_factor": profit_factor if profit_factor is not None else "",
            "n_trades": n,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "avg_trade": avg_trade,
            "max_drawdown_closed": max_drawdown,
            "swap_sum": float(trades["swap_sum"].sum()),
            "commission_sum": float(trades["commission_sum"].sum()),
            "volume_in_sum": float(trades["volume_in"].sum()),
            "volume_out_sum": float(trades["volume_out"].sum()),
        }
    ])
    return out


def perf_from_trades(trades: pd.DataFrame, freq: str, start_equity: float) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()

    d = trades.copy()
    d["close_time_utc"] = pd.to_datetime(d["close_time_utc"], utc=True, errors="coerce")
    d = d.dropna(subset=["close_time_utc"]).sort_values("close_time_utc")
    d = d.set_index("close_time_utc")

    pnl = d["net_sum"].resample(freq).sum().astype(float)

    out = pd.DataFrame({"pnl_money": pnl})
    out["cum_pnl_money"] = out["pnl_money"].cumsum()
    out["nav"] = float(start_equity) + out["cum_pnl_money"]
    out = out.reset_index().rename(columns={"close_time_utc": "date"})
    out["date"] = pd.to_datetime(out["date"], utc=True)
    return out


# ============================================================
# EXPORT
# ============================================================

def export_strategy_db(
    bucket_dir: Path,
    trades: pd.DataFrame,
    start_equity: float,
    measurement_day_utc: pd.Timestamp,
) -> None:
    ensure_dir(bucket_dir)

    db_path = bucket_dir / OUTPUT_DB_FILENAME

    trades_out = trades.copy()
    kpis = kpis_from_trades(
        trades_out,
        global_cutoff_utc=GLOBAL_CUTOFF_DATE_UTC,
        measurement_day_utc=measurement_day_utc,
    )
    weekly = perf_from_trades(trades_out, freq="W-FRI", start_equity=start_equity)
    monthly = perf_from_trades(trades_out, freq="ME", start_equity=start_equity)

    sqlite_write_table_replace(db_path, "trades", trades_out)
    sqlite_write_table_replace(db_path, "kpis", kpis)
    sqlite_write_table_replace(db_path, "weekly_performance", weekly)
    sqlite_write_table_replace(db_path, "monthly_performance", monthly)

    sqlite_create_indexes_for_strategy_db(db_path)


def export_account_strategies(
    output_account_dir: Path,
    df: pd.DataFrame,
    start_equity: float,
    measurement_day_utc: pd.Timestamp,
) -> Tuple[int, int]:
    if df.empty:
        return 0, 0

    strategies_dir = output_account_dir
    ensure_dir(strategies_dir)

    valid = df.copy()

    mask_valid = (
        valid["strategy_id"].astype(str).str.len() > 0
    ) & (
        valid["direction"].astype(str).str.len() > 0
    )

    skipped_count = int((~mask_valid).sum())
    valid = valid[mask_valid].copy()

    if valid.empty:
        return 0, skipped_count

    bucket_count = 0

    grouped = valid.groupby(["strategy_id", "symbol", "magic", "direction"], dropna=False, sort=True)
    for (strategy_id, symbol, magic, side), g in grouped:
        trades = bucket_trades(g)

        start_date, end_date = build_bucket_date_range_for_measurement(
            trades=trades,
            measurement_day_utc=measurement_day_utc,
        )

        bucket_dir = strategies_dir / bucket_folder_name(
            symbol=symbol,
            magic=magic,
            strategy_id=strategy_id,
            side=side,
            start_date=start_date,
            end_date=end_date,
        )

        export_strategy_db(
            bucket_dir=bucket_dir,
            trades=trades,
            start_equity=start_equity,
            measurement_day_utc=measurement_day_utc,
        )
        bucket_count += 1

    return bucket_count, skipped_count


# ============================================================
# LOOP
# ============================================================

def process_account_dir(
    input_account_dir: Path,
    output_root: Path,
    reg: pd.DataFrame,
    ea_map: pd.DataFrame,
    state: Dict[str, AccountState],
    measurement_day_utc: pd.Timestamp,
) -> None:
    closed_path = input_account_dir / INPUT_DB_FILENAME
    if not closed_path.exists():
        return

    df = read_closed_trades_db(closed_path)
    if df.empty:
        return

    rows_before = len(df)
    df = apply_global_cutoff_filter(df)
    rows_after = len(df)

    if df.empty:
        print(
            f"[INFO] {input_account_dir.name}: keine Trades ab Basisdatum {GLOBAL_CUTOFF_DATE_UTC} "
            f"(rows_before={rows_before}, rows_after={rows_after})"
        )
        return

    df = enrich_strategy_ids(df, reg, ea_map)

    account_id = int(df["account_id"].dropna().iloc[0])
    output_account_dir = output_root / input_account_dir.name
    ensure_dir(output_account_dir)

    bucket_count, skipped_count = export_account_strategies(
        output_account_dir=output_account_dir,
        df=df,
        start_equity=START_EQUITY,
        measurement_day_utc=measurement_day_utc,
    )

    max_close_ticket = pd.to_numeric(df["close_ticket"], errors="coerce").dropna()
    max_ticket_val = int(max_close_ticket.max()) if not max_close_ticket.empty else None

    matched = int((df["strategy_id"].astype(str).str.len() > 0).sum())
    total = int(len(df))

    state[str(account_id)] = AccountState(
        account_id=account_id,
        last_max_close_ticket=max_ticket_val,
        last_rows=total,
        last_updated_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )

    print(
        f"[OK] account={account_id} rows_before={rows_before} rows_after={rows_after} "
        f"matched={matched} skipped={skipped_count} buckets={bucket_count} "
        f"max_close_ticket={max_ticket_val} measurement_day_utc={measurement_day_utc.strftime('%Y-%m-%d')} "
        f"output_dir={output_account_dir}"
    )


def main() -> None:
    ensure_dir(RUNTIME_DIR)
    ensure_dir(OUTPUT_LIVE_ROOT)

    print(f"[INFO] PROJECT_ROOT = {PROJECT_ROOT}")
    print(f"[INFO] INPUT_LIVE_ROOT = {INPUT_LIVE_ROOT}")
    print(f"[INFO] OUTPUT_LIVE_ROOT = {OUTPUT_LIVE_ROOT}")
    print(f"[INFO] STRATEGY_EA_ROOT = {STRATEGY_EA_ROOT}")
    print(f"[INFO] STATE_FILE = {STATE_FILE}")
    print(f"[INFO] REGISTRY_FILE = {REGISTRY_FILE if REGISTRY_FILE.exists() else 'not found'}")
    print(f"[INFO] POLL_SECONDS = {POLL_SECONDS}")
    print(f"[INFO] START_EQUITY = {START_EQUITY}")
    print(f"[INFO] GLOBAL_CUTOFF_DATE_UTC = {GLOBAL_CUTOFF_DATE_UTC}")
    print(f"[INFO] INPUT_DB_FILENAME = {INPUT_DB_FILENAME}")
    print(f"[INFO] OUTPUT_DB_FILENAME = {OUTPUT_DB_FILENAME}")

    state = load_state()

    try:
        while True:
            measurement_day_utc = get_measurement_day_utc()

            reg = load_registry(REGISTRY_FILE)
            ea_map = build_strategy_ea_mapping(STRATEGY_EA_ROOT)

            if ea_map.empty:
                print("[WARN] Kein Strategy_EA Mapping gefunden.")
            else:
                print(f"[INFO] Strategy_EA mappings loaded: {len(ea_map)}")

            account_dirs = find_account_dirs(INPUT_LIVE_ROOT)

            if not account_dirs:
                print("[WARN] Keine account_* Ordner gefunden.")
            else:
                for input_account_dir in account_dirs:
                    try:
                        process_account_dir(
                            input_account_dir=input_account_dir,
                            output_root=OUTPUT_LIVE_ROOT,
                            reg=reg,
                            ea_map=ea_map,
                            state=state,
                            measurement_day_utc=measurement_day_utc,
                        )
                    except Exception as e:
                        print(f"[WARN] account processing failed: {input_account_dir.name} | {e}")

            save_state(state)
            print(
                f"[LOOP] updated_at_utc={datetime.now(timezone.utc).isoformat(timespec='seconds')} "
                f"measurement_day_utc={measurement_day_utc.strftime('%Y-%m-%d')}"
            )
            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        print("[INFO] Stopping...")
        save_state(state)


if __name__ == "__main__":
    main()