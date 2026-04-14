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
DEFAULT_OUT_DIR = PROJECT_ROOT / "2.Analyse_Center" / "Reports" / "Trade_Exit_Structure_TP_Per_Strategy"


# =========================
# HELPERS
# =========================
def _to_bool_or_nan(x):
    s = str(x).strip().lower()
    if s == "true":
        return True
    if s == "false":
        return False
    return np.nan


def classify_exit(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()

    # normalize exit_hit_sl / exit_hit_tp
    if "exit_hit_sl" in d.columns:
        d["exit_hit_sl"] = d["exit_hit_sl"].map(_to_bool_or_nan)
    else:
        d["exit_hit_sl"] = np.nan

    if "exit_hit_tp" in d.columns:
        d["exit_hit_tp"] = d["exit_hit_tp"].map(_to_bool_or_nan)
    else:
        d["exit_hit_tp"] = np.nan

    # build mutually exclusive exit_group
    d["exit_group"] = "EOD"

    # priority: SL first, then TP (if both true, TP will overwrite SL; change order if you want)
    d.loc[d["exit_hit_sl"] == True, "exit_group"] = "SL"
    d.loc[d["exit_hit_tp"] == True, "exit_group"] = "TP"

    return d


def add_day_position_bucket(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "entry_pos_in_day" not in d.columns:
        d["entry_pos_in_day_bucket"] = "unknown"
        return d

    x = pd.to_numeric(d["entry_pos_in_day"], errors="coerce")
    # if not enough unique values, qcut may fail -> fallback to NaN/unknown
    try:
        d["entry_pos_in_day_bucket"] = pd.qcut(
            x,
            10,
            labels=[f"D{i}" for i in range(1, 11)],
            duplicates="drop",
        )
    except Exception:
        d["entry_pos_in_day_bucket"] = "unknown"
    return d


def ensure_hour(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "entry_hour" not in d.columns:
        if "open_time" in d.columns:
            d["open_time"] = pd.to_datetime(d["open_time"], errors="coerce")
            d["entry_hour"] = d["open_time"].dt.hour
    return d


def _heatmap_ok(pivot: pd.DataFrame) -> bool:
    if pivot is None:
        return False
    if pivot.empty:
        return False
    # must contain at least one finite number
    arr = pivot.to_numpy()
    if arr.size == 0:
        return False
    if not np.isfinite(arr).any():
        return False
    return True


def heatmap(pivot: pd.DataFrame, title: str, path: Path) -> None:
    if not _heatmap_ok(pivot):
        return
    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot, annot=True, fmt=".2f", cmap="coolwarm")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def pivot_rate(df: pd.DataFrame, value_col: str, index_col: str, columns_col: str) -> pd.DataFrame:
    """
    Computes mean rate for a boolean-ish column.
    Converts to 0/1 and ignores NaNs.
    """
    tmp = df[[value_col, index_col, columns_col]].copy()

    # convert to 0/1; NaN stays NaN
    b = tmp[value_col].map(_to_bool_or_nan)
    tmp["__v"] = np.where(b == True, 1.0, np.where(b == False, 0.0, np.nan))

    # remove rows with missing index/columns
    tmp = tmp.dropna(subset=[index_col, columns_col])

    pv = pd.pivot_table(
        tmp,
        values="__v",
        index=index_col,
        columns=columns_col,
        aggfunc="mean",
        observed=False,  # silence warning + keep old behavior
    )

    # Ensure numeric dtype
    pv = pv.apply(pd.to_numeric, errors="coerce")
    return pv


# =========================
# MAIN ANALYSIS
# =========================
def analyze_file(fp: Path, out_root: Path) -> None:
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

    # -------------------------
    # 3-way distributions (EOD/SL/TP)
    # -------------------------
    if "entry_session" in df.columns:
        pd.crosstab(df["entry_session"], df["exit_group"], normalize="index").to_csv(
            tables_dir / "session_exit_distribution_3way.csv"
        )

    if "entry_hour" in df.columns:
        pd.crosstab(df["entry_hour"], df["exit_group"], normalize="index").to_csv(
            tables_dir / "hour_exit_distribution_3way.csv"
        )

    if "entry_pos_in_day_bucket" in df.columns:
        pd.crosstab(df["entry_pos_in_day_bucket"], df["exit_group"], normalize="index").to_csv(
            tables_dir / "daypos_exit_distribution_3way.csv"
        )

    # -------------------------
    # TP vs NOT_TP rate tables
    # -------------------------
    is_tp = df["exit_hit_tp"].map(_to_bool_or_nan) == True
    df["tp_group"] = np.where(is_tp, "TP", "NOT_TP")

    if "entry_session" in df.columns:
        pd.crosstab(df["entry_session"], df["tp_group"], normalize="index").to_csv(
            tables_dir / "session_tp_rate.csv"
        )

    if "entry_hour" in df.columns:
        pd.crosstab(df["entry_hour"], df["tp_group"], normalize="index").to_csv(
            tables_dir / "hour_tp_rate.csv"
        )

    if "entry_pos_in_day_bucket" in df.columns:
        pd.crosstab(df["entry_pos_in_day_bucket"], df["tp_group"], normalize="index").to_csv(
            tables_dir / "daypos_tp_rate.csv"
        )

    # -------------------------
    # TP heatmaps (skip if empty/all-NaN)
    # -------------------------
    if "entry_session" in df.columns and "entry_hour" in df.columns:
        pv = pivot_rate(df, "exit_hit_tp", "entry_session", "entry_hour")
        heatmap(pv, "TP Rate: Session x Hour", figs_dir / "tp_session_hour.png")

    if "entry_hour" in df.columns and "entry_pos_in_day_bucket" in df.columns:
        pv = pivot_rate(df, "exit_hit_tp", "entry_hour", "entry_pos_in_day_bucket")
        heatmap(pv, "TP Rate: Hour x DayPosition", figs_dir / "tp_hour_daypos.png")

    if "entry_session" in df.columns and "entry_pos_in_day_bucket" in df.columns:
        pv = pivot_rate(df, "exit_hit_tp", "entry_session", "entry_pos_in_day_bucket")
        heatmap(pv, "TP Rate: Session x DayPosition", figs_dir / "tp_session_daypos.png")


def main() -> None:
    parser = argparse.ArgumentParser(description="TP structure per strategy (crosstabs + TP heatmaps).")
    parser.add_argument("--in-dir", type=str, default=str(DEFAULT_IN_DIR))
    parser.add_argument("--pattern", type=str, default="*_enriched_full_context.csv")
    parser.add_argument("--out-dir", type=str, default=str(DEFAULT_OUT_DIR))
    args = parser.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(in_dir.glob(args.pattern))
    if not files:
        raise FileNotFoundError(f"No files matching {args.pattern} in {in_dir}")

    for fp in files:
        analyze_file(fp, out_dir)

    print(f"[OK] TP structure reports written to: {out_dir}")


if __name__ == "__main__":
    main()
