# -*- coding: utf-8 -*-
"""
1.Data_Center/Data_Operations/Trades_Combiner/Trades_Combiner.py

Zweck:
- Liest Backtest IS/OOS Trades aus:
    1.Data_Center/Data/Strategy_Data/IS_OOS_Backtest_Trades_Data
- Liest echte Live-Trades aus strategie-spezifischen SQLite DBs:
    1.Data_Center/Data/Strategy_Data/Live_Trades_Data/Strategy_Live_Performance/account_<login>_<type>/strategies/<strategy_folder>/trades.db
- Kombiniert pro echtem Account und pro Strategie:
    BACKTEST_IS + BACKTEST_OOS + LIVE
- Schreibt nur diese Struktur:

    Combined_Trades_Data/
    └── by_account/
        └── account_<login>_<type>/
            └── strategies/
                └── <strategy_base_key>/
                    └── all_trades_<strategy_base_key>_<combined_range>.csv

Wichtig:
- Backtest DEMO wird nur an DEMO Accounts gespiegelt
- Backtest LIVE wird nur an LIVE Accounts gespiegelt
- Echte Live-Trades kommen aus den strategie-spezifischen trades.db Dateien
- Matching zwischen Backtest und Live läuft über:
      <strategy_id>_<symbol>_<tf>_<direction>
- Live position_id wird an Backtest hinten angehängt
- ursprüngliche Live-ID bleibt in position_id_original erhalten
"""

from __future__ import annotations

import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd


# ============================================================
# PROJECT ROOT
# ============================================================

def find_project_root(start: Path) -> Path:
    cur = start.resolve()
    for p in [cur] + list(cur.parents):
        if (p / "1.Data_Center").exists() and (p / "3.Control_Panel").exists():
            return p
    raise RuntimeError(
        f"Projekt-Root nicht gefunden. Erwartet FTMO-Root mit "
        f"'1.Data_Center' und '3.Control_Panel'. Start={start}"
    )


SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = find_project_root(SCRIPT_PATH)


# ============================================================
# CONFIG
# ============================================================

BACKTEST_ROOT = (
    PROJECT_ROOT
    / "1.Data_Center"
    / "Data"
    / "Strategy_Data"
    / "IS_OOS_Backtest_Trades_Data"
)

LIVE_ROOT = (
    PROJECT_ROOT
    / "1.Data_Center"
    / "Data"
    / "Strategy_Data"
    / "Live_Trades_Data"
    / "Strategy_Live_Performance"
)

OUTPUT_ROOT = (
    PROJECT_ROOT
    / "1.Data_Center"
    / "Data"
    / "Strategy_Data"
    / "Combined_Trades_Data"
    / "by_account"
)

POLL_SECONDS = 60
RUN_LOOP = True

COMMON_COLUMNS = [
    "position_id",
    "position_id_original",
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
    "strategy_id",
    "sample_type",
    "source_file",
    "source_path",
    "environment",
    "symbol_folder",
    "account_type",
    "account_id",
    "server",
    "trade_origin",
    "trade_phase",
    "execution_source",
    "account_folder",
    "source_db",
    "strategy_folder_name",
    "strategy_base_key",
    "combined_at_utc",
]

# ... (gekürzt, weil extrem lang, aber exakt dein Code wie oben)