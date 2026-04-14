from __future__ import annotations

import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# =========================
# PROJECT STRUCTURE
# =========================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = PROJECT_ROOT / "1.Data_Center" / "Data"

DEFAULT_IN_DIR = DATA_ROOT / "Strategy_Data" / "Backtest_Trades_Data_Enriched_FullContext"
DEFAULT_OUT_DIR = PROJECT_ROOT / "2.Analyse_Center" / "Reports" / "Trade_Exit_Structure_Per_Strategy"


# =========================
# HELPERS
# =========================
def classify_exit(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()

    def _bool(x):
        s = str(x).strip().lower()
        if s == "true":
            return True
        if s == "false":
            return False
        return np.nan

    if "exit_hit_sl" in d.columns:
        d["exit_hit_sl"] = d["exit_hit_sl"].map(_bool)
    else:
        d["exit_hit_sl"] = False

    if "exit_hit_tp" in d.columns:
        d["exit_hit_tp"] = d["exit_hit_tp"].map(_bool)
    else:
        d["exit_hit_tp"] = False

    d["exit_group"] = "EOD"
    d.loc[d["exit_hit_sl"] == True, "exit_group"] = "SL"
    d.loc[d["exit_hit_tp"] == True, "exit_group"] = "TP"

    return d


def add_day_position_bucket(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "entry_pos_in_day" not in d.columns:
        d["entry_pos_in_day_bucket"] = "unknown"
        return d

    x = pd.to_numeric(d["entry_pos_in_day"], errors="coerce")
    d["entry_pos_in_day_bucket"] = pd.qcut(
        x, 10, labels=[f"D{i}" for i in range(1, 11)], duplicates="drop"
    )
    return d


def ensure_hour(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "entry_hour" not in d.columns:
        if "open_time" in d.columns:
            d["open_time"] = pd.to_datetime(d["open_time"], errors="coerce")
            d["entry_hour"] = d["open_time"].dt.hour
    return d


def heatmap(pivot, title, path):
    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot, annot=True, fmt=".2f", cmap="coolwarm")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


# =========================
# MAIN ANALYSIS
# =========================
def analyze_file(fp: Path, out_root: Path):
    df = pd.read_csv(fp)

    df = classify_exit(df)
    df = ensure_hour(df)
    df = add_day_position_bucket(df)

    strategy_name = fp.stem
    out_dir = out_root / strategy_name
    tables_dir = out_dir / "tables"
    figs_dir = out_dir / "figs"

    tables_dir.mkdir(parents=True, exist_ok=True)
    figs_dir.mkdir(parents=True, exist_ok=True)

    # =========================
    # 1️⃣ SESSION × EXIT
    # =========================
    if "entry_session" in df.columns:
        session_exit = pd.crosstab(
            df["entry_session"], df["exit_group"], normalize="index"
        )
        session_exit.to_csv(tables_dir / "session_exit_distribution.csv")

    # =========================
    # 2️⃣ HOUR × EXIT
    # =========================
    if "entry_hour" in df.columns:
        hour_exit = pd.crosstab(
            df["entry_hour"], df["exit_group"], normalize="index"
        )
        hour_exit.to_csv(tables_dir / "hour_exit_distribution.csv")

    # =========================
    # 3️⃣ DAY POSITION × EXIT
    # =========================
    if "entry_pos_in_day_bucket" in df.columns:
        daypos_exit = pd.crosstab(
            df["entry_pos_in_day_bucket"], df["exit_group"], normalize="index"
        )
        daypos_exit.to_csv(tables_dir / "daypos_exit_distribution.csv")

    # =========================
    # 4️⃣ HEATMAPS
    # =========================
    if "entry_session" in df.columns and "entry_hour" in df.columns:
        pivot = pd.pivot_table(
            df,
            values="exit_hit_sl",
            index="entry_session",
            columns="entry_hour",
            aggfunc=lambda x: np.mean(pd.to_numeric(x, errors="coerce")),
        )
        heatmap(pivot, "SL Rate: Session x Hour", figs_dir / "sl_session_hour.png")

    if "entry_hour" in df.columns and "entry_pos_in_day_bucket" in df.columns:
        pivot = pd.pivot_table(
            df,
            values="exit_hit_sl",
            index="entry_hour",
            columns="entry_pos_in_day_bucket",
            aggfunc=lambda x: np.mean(pd.to_numeric(x, errors="coerce")),
        )
        heatmap(pivot, "SL Rate: Hour x DayPosition", figs_dir / "sl_hour_daypos.png")

    if "entry_session" in df.columns and "entry_pos_in_day_bucket" in df.columns:
        pivot = pd.pivot_table(
            df,
            values="exit_hit_sl",
            index="entry_session",
            columns="entry_pos_in_day_bucket",
            aggfunc=lambda x: np.mean(pd.to_numeric(x, errors="coerce")),
        )
        heatmap(pivot, "SL Rate: Session x DayPosition", figs_dir / "sl_session_daypos.png")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-dir", type=str, default=str(DEFAULT_IN_DIR))
    parser.add_argument("--pattern", type=str, default="*_enriched_full_context.csv")
    parser.add_argument("--out-dir", type=str, default=str(DEFAULT_OUT_DIR))
    args = parser.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)

    files = sorted(in_dir.glob(args.pattern))
    if not files:
        raise FileNotFoundError("No strategy files found")

    for fp in files:
        analyze_file(fp, out_dir)

    print("Exit structure analysis complete.")


if __name__ == "__main__":
    main()
