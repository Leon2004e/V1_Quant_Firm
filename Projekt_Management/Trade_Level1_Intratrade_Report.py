# FTMO/2.Analyse_Center/Backtest_Analyse/Trade_Level1_Per_Strategy.py
#
# Zweck
# - Bewertet JEDE Strategie-Datei einzeln (keine Pools/Buckets über mehrere Strategien)
# - Input: Backtest_Trades_Data_Enriched_FullContext/*.csv  (eine Datei = eine Strategie)
# - Output:
#   2.Analyse_Center/Reports/Trade_Level1_Per_Strategy/<run_id>/
#     ├── strategies/<strategy_name>/tables/*.csv
#     ├── strategies/<strategy_name>/figs/*.png (optional)
#     └── master_level1_summary.csv  (1 Zeile pro Strategie)
#
# Update:
# - exit_hit_tp / exit_hit_sl werden ROBUST neu klassifiziert:
#   - exit_type (tp/sl) (optional)
#   - close_price nahe init_tp_price/init_sl_price (Tick-Toleranz)
#   - Intratrade-touch über highest_high_price/lowest_low_price (wenn vorhanden)
# - Danach wird is_eod_exit korrekt berechnet.

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# =========================
# PROJECT STRUCTURE
# =========================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = PROJECT_ROOT / "1.Data_Center" / "Data"

DEFAULT_IN_DIR = DATA_ROOT / "Strategy_Data" / "Backtest_Trades_Data_Enriched_FullContext"
DEFAULT_OUT_DIR = PROJECT_ROOT / "2.Analyse_Center" / "Reports" / "Trade_Level1_Per_Strategy"


# =========================
# CONFIG
# =========================
TP_LEVELS = [0.5, 1.0, 1.5, 2.0]
DEFAULT_NEAR_SL_THRESHOLD_R = 0.8
MIN_TRADES_PER_STRATEGY = 5  # setz auf 10 wenn du strenger sein willst


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


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def summarize_series(x: pd.Series) -> Dict[str, object]:
    z = pd.to_numeric(x, errors="coerce").dropna().astype(float)
    if len(z) == 0:
        return {"n": 0, "mean": None, "median": None, "p10": None, "p90": None, "std": None, "min": None, "max": None}
    return {
        "n": int(len(z)),
        "mean": float(z.mean()),
        "median": float(z.median()),
        "p10": float(np.percentile(z, 10)),
        "p90": float(np.percentile(z, 90)),
        "std": float(z.std(ddof=1)) if len(z) > 1 else 0.0,
        "min": float(z.min()),
        "max": float(z.max()),
    }


def hit_rate_from_mfe(df: pd.DataFrame, tp_r: float) -> float:
    x = pd.to_numeric(df.get("mfe_r"), errors="coerce").dropna().astype(float)
    if len(x) == 0:
        return np.nan
    return float((x >= tp_r).mean())


def time_to_threshold(df: pd.DataFrame, threshold_r: float) -> Dict[str, object]:
    # approximiert über time_to_mfe_minutes bei Trades mit mfe_r >= threshold
    if "time_to_mfe_minutes" not in df.columns or "mfe_r" not in df.columns:
        return {"n": 0, "mean_min": None, "median_min": None, "p90_min": None}

    mfe = pd.to_numeric(df["mfe_r"], errors="coerce")
    ttm = pd.to_numeric(df["time_to_mfe_minutes"], errors="coerce")
    mask = mfe.notna() & (mfe >= threshold_r) & ttm.notna()
    t = ttm[mask].astype(float)

    if len(t) == 0:
        return {"n": 0, "mean_min": None, "median_min": None, "p90_min": None}
    return {
        "n": int(len(t)),
        "mean_min": float(t.mean()),
        "median_min": float(t.median()),
        "p90_min": float(np.percentile(t, 90)),
    }


def reclassify_exit_hits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Überschreibt exit_hit_tp/exit_hit_sl robust anhand:
    - exit_type (falls vorhanden)
    - close_price ~ init_{tp/sl}_price
    - intratrade-touch über highest_high_price / lowest_low_price

    Erwartete Spalten (optional, je nach Data):
      type, close_price
      init_tp_price, init_sl_price
      highest_high_price, lowest_low_price
      tick_size (für Toleranz)
      exit_type
    """
    d = df.copy()

    if not {"type", "close_price"}.issubset(set(d.columns)):
        return d

    t = d["type"].astype(str).str.upper()
    closep = _num(d["close_price"])

    init_tp = _num(d["init_tp_price"]) if "init_tp_price" in d.columns else pd.Series(np.nan, index=d.index)
    init_sl = _num(d["init_sl_price"]) if "init_sl_price" in d.columns else pd.Series(np.nan, index=d.index)

    hh = _num(d["highest_high_price"]) if "highest_high_price" in d.columns else pd.Series(np.nan, index=d.index)
    ll = _num(d["lowest_low_price"]) if "lowest_low_price" in d.columns else pd.Series(np.nan, index=d.index)

    # Toleranz: bevorzugt tick_size, sonst fallback
    if "tick_size" in d.columns:
        tick = _num(d["tick_size"]).abs()
        tol = (tick * 0.6).replace(0.0, np.nan)
    else:
        tol = pd.Series(np.nan, index=d.index)
    tol = tol.fillna(1e-5)

    # exit_type (optional)
    if "exit_type" in d.columns:
        et = d["exit_type"].astype(str).str.lower()
        tp_by_type = et.eq("tp")
        sl_by_type = et.eq("sl")
    else:
        tp_by_type = pd.Series(False, index=d.index)
        sl_by_type = pd.Series(False, index=d.index)

    is_buy = t.str.contains("BUY")
    is_sell = t.str.contains("SELL")

    # close near level
    tp_by_close = init_tp.notna() & closep.notna() & ((closep - init_tp).abs() <= tol)
    sl_by_close = init_sl.notna() & closep.notna() & ((closep - init_sl).abs() <= tol)

    # path touch
    tp_by_touch = pd.Series(False, index=d.index)
    sl_by_touch = pd.Series(False, index=d.index)

    # BUY: TP if HH >= TP, SL if LL <= SL
    tp_by_touch = tp_by_touch | (is_buy & init_tp.notna() & hh.notna() & (hh >= (init_tp - tol)))
    sl_by_touch = sl_by_touch | (is_buy & init_sl.notna() & ll.notna() & (ll <= (init_sl + tol)))

    # SELL: TP if LL <= TP, SL if HH >= SL
    tp_by_touch = tp_by_touch | (is_sell & init_tp.notna() & ll.notna() & (ll <= (init_tp + tol)))
    sl_by_touch = sl_by_touch | (is_sell & init_sl.notna() & hh.notna() & (hh >= (init_sl - tol)))

    d["exit_hit_tp"] = (tp_by_type | tp_by_close | tp_by_touch)
    d["exit_hit_sl"] = (sl_by_type | sl_by_close | sl_by_touch)

    # Konflikte: beides True -> entscheide nach close-nähe
    both = d["exit_hit_tp"] & d["exit_hit_sl"]
    if both.any():
        tp_dist = (closep - init_tp).abs()
        sl_dist = (closep - init_sl).abs()
        choose_tp = tp_dist <= sl_dist
        d.loc[both & choose_tp, "exit_hit_sl"] = False
        d.loc[both & ~choose_tp, "exit_hit_tp"] = False

    return d


def ensure_is_eod_exit(df: pd.DataFrame) -> pd.DataFrame:
    # 1) Exit-Hits robust rekonstruieren
    d = reclassify_exit_hits(df)

    # 2) normalize (falls strings)
    if "exit_hit_sl" in d.columns:
        d["exit_hit_sl"] = d["exit_hit_sl"].map(_to_bool_or_nan)
    else:
        d["exit_hit_sl"] = np.nan

    if "exit_hit_tp" in d.columns:
        d["exit_hit_tp"] = d["exit_hit_tp"].map(_to_bool_or_nan)
    else:
        d["exit_hit_tp"] = np.nan

    sl = d["exit_hit_sl"]
    tp = d["exit_hit_tp"]

    # primär: EOD = weder SL noch TP
    d["is_eod_exit"] = (sl == False) & (tp == False)

    # fallback über exit_type (optional)
    if "exit_type" in d.columns:
        et = d["exit_type"].astype(str).str.lower()
        is_time_exit = et.isin(["time", "eod", "close", "other"])
        d["is_eod_exit"] = d["is_eod_exit"] | (is_time_exit & sl.isna() & tp.isna())

    return d


def compute_level1_tables(df: pd.DataFrame, near_sl_threshold_r: float) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    d = ensure_is_eod_exit(df)

    rows_overall = []

    # 1.1 Realisierte Performance
    rows_overall += [
        {"section": "1.1_realized", "metric": "n_trades", "value": int(len(d))},
        {"section": "1.1_realized", "metric": "n_r_valid", "value": int(pd.to_numeric(d.get("r_multiple_price"), errors="coerce").notna().sum()) if "r_multiple_price" in d.columns else 0},
        {"section": "1.1_realized", "metric": "mean_r", "value": summarize_series(d.get("r_multiple_price", pd.Series(dtype=float)))["mean"]},
        {"section": "1.1_realized", "metric": "median_r", "value": summarize_series(d.get("r_multiple_price", pd.Series(dtype=float)))["median"]},
        {"section": "1.1_realized", "metric": "sl_hit_rate", "value": float((d["exit_hit_sl"] == True).mean()) if "exit_hit_sl" in d.columns else np.nan},
        {"section": "1.1_realized", "metric": "tp_hit_rate", "value": float((d["exit_hit_tp"] == True).mean()) if "exit_hit_tp" in d.columns else np.nan},
        {"section": "1.1_realized", "metric": "eod_exit_rate", "value": float(d["is_eod_exit"].mean())},
        {"section": "1.1_realized", "metric": "mean_trade_duration_min", "value": summarize_series(d.get("trade_duration_minutes", pd.Series(dtype=float)))["mean"]},
    ]

    # 1.2 Path Efficiency
    for col in ["mfe_r", "mae_r", "edge_leakage_r", "mfe_price", "mae_price", "time_to_mfe_minutes", "time_to_mae_minutes"]:
        if col in d.columns:
            s = summarize_series(d[col])
            rows_overall += [
                {"section": "1.2_path", "metric": f"{col}_mean", "value": s["mean"]},
                {"section": "1.2_path", "metric": f"{col}_p10", "value": s["p10"]},
                {"section": "1.2_path", "metric": f"{col}_p90", "value": s["p90"]},
            ]

    # 1.3 Exit-Effizienz
    for col in ["exit_dist_to_tp_r", "exit_dist_to_sl_r", "realized_vs_day_range"]:
        if col in d.columns:
            s = summarize_series(d[col])
            rows_overall += [
                {"section": "1.3_exit_eff", "metric": f"{col}_mean", "value": s["mean"]},
                {"section": "1.3_exit_eff", "metric": f"{col}_p10", "value": s["p10"]},
                {"section": "1.3_exit_eff", "metric": f"{col}_p90", "value": s["p90"]},
            ]

    overall_table = pd.DataFrame(rows_overall)

    # Hypothetische TP-Level (nur aus MFE, ohne Re-Backtest)
    tp_rows = []
    if "mfe_r" in d.columns:
        for tp_r in TP_LEVELS:
            tstats = time_to_threshold(d, tp_r)
            tp_rows.append({
                "tp_r": tp_r,
                "hyp_hit_rate_from_mfe": hit_rate_from_mfe(d, tp_r),
                "n_time_samples": tstats["n"],
                "time_to_hit_mean_min": tstats["mean_min"],
                "time_to_hit_median_min": tstats["median_min"],
                "time_to_hit_p90_min": tstats["p90_min"],
            })
    tp_table = pd.DataFrame(tp_rows)

    # 1.4 Stop-Relevanz (mae_r ist i.d.R. adverse magnitude positiv)
    stop_rows = []
    if "mae_r" in d.columns:
        mae = pd.to_numeric(d["mae_r"], errors="coerce").dropna().astype(float)
        stop_rows += [
            {"metric": "mae_r_mean", "value": float(mae.mean()) if len(mae) else None},
            {"metric": "mae_r_median", "value": float(mae.median()) if len(mae) else None},
            {"metric": "share_mae_ge_1R", "value": float((mae >= 1.0).mean()) if len(mae) else None},
            {"metric": f"share_mae_ge_{near_sl_threshold_r}R", "value": float((mae >= near_sl_threshold_r).mean()) if len(mae) else None},
        ]

        # Zeit bis SL-Nähe (approx) über time_to_mae_minutes bei mae_r >= threshold
        if "time_to_mae_minutes" in d.columns:
            t = pd.to_numeric(d["time_to_mae_minutes"], errors="coerce")
            mask = t.notna() & (pd.to_numeric(d["mae_r"], errors="coerce") >= near_sl_threshold_r)
            tt = t[mask].dropna().astype(float)
            stop_rows += [
                {"metric": "time_to_near_sl_n", "value": int(len(tt))},
                {"metric": "time_to_near_sl_mean_min", "value": float(tt.mean()) if len(tt) else None},
                {"metric": "time_to_near_sl_median_min", "value": float(tt.median()) if len(tt) else None},
                {"metric": "time_to_near_sl_p90_min", "value": float(np.percentile(tt, 90)) if len(tt) else None},
            ]

    stop_table = pd.DataFrame(stop_rows)

    # Split EOD vs non-EOD
    split_rows = []
    for label, g in [("ALL", d), ("EOD_ONLY", d[d["is_eod_exit"] == True]), ("NON_EOD", d[d["is_eod_exit"] == False])]:
        if len(g) == 0:
            continue
        split_rows.append({
            "segment": label,
            "n": int(len(g)),
            "mean_r": summarize_series(g.get("r_multiple_price", pd.Series(dtype=float)))["mean"],
            "mean_mfe_r": summarize_series(g.get("mfe_r", pd.Series(dtype=float)))["mean"],
            "mean_mae_r": summarize_series(g.get("mae_r", pd.Series(dtype=float)))["mean"],
            "mean_leakage_r": summarize_series(g.get("edge_leakage_r", pd.Series(dtype=float)))["mean"],
            "tp_hit_rate": float((g["exit_hit_tp"] == True).mean()) if "exit_hit_tp" in g.columns else np.nan,
            "sl_hit_rate": float((g["exit_hit_sl"] == True).mean()) if "exit_hit_sl" in g.columns else np.nan,
            "mean_duration_min": summarize_series(g.get("trade_duration_minutes", pd.Series(dtype=float)))["mean"],
        })
    split_table = pd.DataFrame(split_rows)

    return overall_table, tp_table, stop_table, split_table


def make_figs(df: pd.DataFrame, figs_dir: Path) -> None:
    figs_dir.mkdir(parents=True, exist_ok=True)
    for col in ["r_multiple_price", "mfe_r", "mae_r", "edge_leakage_r", "trade_duration_minutes"]:
        if col not in df.columns:
            continue
        x = pd.to_numeric(df[col], errors="coerce").dropna().astype(float)
        if len(x) < 5:
            continue
        plt.figure()
        plt.hist(x.values, bins=50)
        plt.title(col)
        plt.xlabel(col)
        plt.ylabel("count")
        plt.tight_layout()
        plt.savefig(figs_dir / f"hist_{col}.png", dpi=140)
        plt.close()


def safe_name(s: str) -> str:
    return str(s).replace("/", "_").replace("\\", "_").replace(" ", "_")


def strategy_label_for_file(fp: Path, df: pd.DataFrame) -> str:
    # Primär: Dateiname (eine Datei = eine Strategie)
    base = fp.stem
    # Optional: add symbol/strategy_id wenn eindeutig vorhanden
    sid = None
    sym = None
    if "strategy_id" in df.columns:
        u = df["strategy_id"].dropna().astype(str).unique()
        if len(u) == 1:
            sid = u[0]
    if "symbol" in df.columns:
        u = df["symbol"].dropna().astype(str).str.upper().unique()
        if len(u) == 1:
            sym = u[0]
    tags = []
    if sid:
        tags.append(f"sid_{sid}")
    if sym:
        tags.append(f"sym_{sym}")
    if tags:
        return safe_name(base + "__" + "__".join(tags))
    return safe_name(base)


def build_master_row(
    label: str,
    fp: Path,
    df: pd.DataFrame,
    overall: pd.DataFrame,
    tp_table: pd.DataFrame,
    stop_table: pd.DataFrame,
) -> Dict[str, object]:
    d = ensure_is_eod_exit(df)

    def get_overall(metric: str) -> Optional[float]:
        sub = overall[(overall["metric"] == metric)]
        if len(sub) == 0:
            return None
        v = sub.iloc[0]["value"]
        return None if pd.isna(v) else float(v)

    def get_tp_hit(tp_r: float) -> Optional[float]:
        if tp_table is None or len(tp_table) == 0:
            return None
        sub = tp_table[tp_table["tp_r"] == tp_r]
        if len(sub) == 0:
            return None
        v = sub.iloc[0]["hyp_hit_rate_from_mfe"]
        return None if pd.isna(v) else float(v)

    def get_stop(metric: str) -> Optional[float]:
        if stop_table is None or len(stop_table) == 0:
            return None
        sub = stop_table[stop_table["metric"] == metric]
        if len(sub) == 0:
            return None
        v = sub.iloc[0]["value"]
        return None if pd.isna(v) else float(v)

    sid = None
    if "strategy_id" in d.columns:
        u = d["strategy_id"].dropna().astype(str).unique()
        if len(u) == 1:
            sid = u[0]

    sym = None
    if "symbol" in d.columns:
        u = d["symbol"].dropna().astype(str).str.upper().unique()
        if len(u) == 1:
            sym = u[0]

    return {
        "strategy_name": label,
        "file": fp.name,
        "strategy_id": sid,
        "symbol": sym,
        "n": int(len(d)),
        "mean_r": get_overall("mean_r"),
        "median_r": get_overall("median_r"),
        "tp_hit_rate": get_overall("tp_hit_rate"),
        "sl_hit_rate": get_overall("sl_hit_rate"),
        "eod_exit_rate": get_overall("eod_exit_rate"),
        "mean_mfe_r": get_overall("mfe_r_mean"),
        "mean_mae_r": get_overall("mae_r_mean"),
        "mean_leakage_r": get_overall("edge_leakage_r_mean"),
        "mean_duration_min": get_overall("mean_trade_duration_min"),
        "hyp_tp_hit_0_5R": get_tp_hit(0.5),
        "hyp_tp_hit_1_0R": get_tp_hit(1.0),
        "hyp_tp_hit_1_5R": get_tp_hit(1.5),
        "hyp_tp_hit_2_0R": get_tp_hit(2.0),
        "share_mae_ge_1R": get_stop("share_mae_ge_1R"),
    }


# =========================
# MAIN
# =========================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compute Level-1 Intratrade reports per strategy file (no pooling).")
    p.add_argument("--in-dir", type=str, default=str(DEFAULT_IN_DIR))
    p.add_argument("--pattern", type=str, default="*_enriched_full_context.csv")
    p.add_argument("--out-dir", type=str, default=str(DEFAULT_OUT_DIR))
    p.add_argument("--near-sl-threshold-r", type=float, default=DEFAULT_NEAR_SL_THRESHOLD_R)
    p.add_argument("--min-trades", type=int, default=MIN_TRADES_PER_STRATEGY)
    p.add_argument("--no-figs", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    in_dir = Path(args.in_dir)
    out_base = Path(args.out_dir)

    if not in_dir.exists():
        raise FileNotFoundError(f"Input dir not found: {in_dir}")

    files = sorted(in_dir.glob(args.pattern))
    if not files:
        raise FileNotFoundError(f"No files matching {args.pattern} in {in_dir}")

    run_id = f"per_strategy_{len(files)}files"
    run_dir = out_base / run_id
    strat_root = run_dir / "strategies"
    run_dir.mkdir(parents=True, exist_ok=True)
    strat_root.mkdir(parents=True, exist_ok=True)

    master_rows: List[Dict[str, object]] = []
    skipped: List[Tuple[str, str]] = []

    for fp in files:
        try:
            df = pd.read_csv(fp)
        except Exception as e:
            skipped.append((fp.name, f"read_error: {type(e).__name__}: {e}"))
            continue

        if len(df) < args.min_trades:
            skipped.append((fp.name, f"too_few_trades: n={len(df)} < {args.min_trades}"))
            continue

        label = strategy_label_for_file(fp, df)
        out_dir = strat_root / label
        tables_dir = out_dir / "tables"
        figs_dir = out_dir / "figs"
        tables_dir.mkdir(parents=True, exist_ok=True)
        out_dir.mkdir(parents=True, exist_ok=True)

        overall, tp_table, stop_table, split_table = compute_level1_tables(df, args.near_sl_threshold_r)

        overall.to_csv(tables_dir / "level1_overall.csv", index=False)
        tp_table.to_csv(tables_dir / "level1_hypothetical_tp_from_mfe.csv", index=False)
        stop_table.to_csv(tables_dir / "level1_stop_relevance.csv", index=False)
        split_table.to_csv(tables_dir / "level1_split_eod.csv", index=False)

        if not args.no_figs:
            make_figs(df, figs_dir)

        master_rows.append(build_master_row(label, fp, df, overall, tp_table, stop_table))

    master = pd.DataFrame(master_rows)
    if len(master):
        master = master.sort_values(["n", "mean_r"], ascending=[False, False])

    master.to_csv(run_dir / "master_level1_summary.csv", index=False)

    if skipped:
        pd.DataFrame(skipped, columns=["file", "reason"]).to_csv(run_dir / "skipped_files.csv", index=False)

    print(f"[OK] Per-strategy Level1 reports written to: {run_dir}")
    print(f"[OK] Master summary: {run_dir / 'master_level1_summary.csv'}")
    if skipped:
        print(f"[WARN] Skipped files list: {run_dir / 'skipped_files.csv'}")


if __name__ == "__main__":
    main()
