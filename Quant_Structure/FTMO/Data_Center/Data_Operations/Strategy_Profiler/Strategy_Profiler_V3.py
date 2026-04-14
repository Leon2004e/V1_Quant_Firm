#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ============================================================
# Utils
# ============================================================

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def remove_comments(text: str) -> str:
    text = re.sub(r'//.*', '', text)
    text = re.sub(r'/\*.*?\*/', '', text, flags=re.S)
    return text


def clean_ws(x: str) -> str:
    return re.sub(r'\s+', ' ', x.strip())


def try_parse_number(x: str) -> Any:
    xs = x.strip()
    if xs.lower() == "true":
        return True
    if xs.lower() == "false":
        return False
    if (xs.startswith('"') and xs.endswith('"')) or (xs.startswith("'") and xs.endswith("'")):
        return xs[1:-1]
    try:
        if "." in xs:
            return float(xs)
        return int(xs)
    except Exception:
        return xs


def split_top_level_args(s: str) -> List[str]:
    out = []
    cur = []
    depth = 0
    in_str = False
    quote = ""

    for ch in s:
        if in_str:
            cur.append(ch)
            if ch == quote:
                in_str = False
            continue

        if ch in ('"', "'"):
            in_str = True
            quote = ch
            cur.append(ch)
            continue

        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1

        if ch == "," and depth == 0:
            out.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)

    if cur:
        out.append("".join(cur).strip())

    return out


def find_default_data_root(script_path: Path) -> Optional[Path]:
    candidates = []
    for parent in [script_path.parent, *script_path.parents]:
        candidates.extend([
            parent / "Data",
            parent / "Data_Center" / "Data",
        ])
    for c in candidates:
        if c.exists() and c.is_dir():
            return c.resolve()
    return None


def derive_paths(data_root: Path) -> Dict[str, Path]:
    return {
        "strategy_ea_root": data_root / "Strategy" / "Strategy_EA",
        "indicator_root": data_root / "Indicators",
        "profile_root": data_root / "Strategy" / "Strategy_Profile_V2",
    }


# ============================================================
# Filename parsing
# ============================================================

def parse_strategy_filename(path: Path) -> Dict[str, Any]:
    stem = path.stem
    parts = stem.split("_")
    out = {
        "file_name": path.name,
        "file_stem": stem,
        "path": str(path),
        "symbol_from_filename": None,
        "strategy_number_from_filename": None,
        "strategy_name_from_filename": None,
        "side_from_filename": None,
        "timeframe_from_filename": None,
    }

    if len(parts) >= 5:
        out["symbol_from_filename"] = parts[0]
        out["strategy_number_from_filename"] = parts[1]
        out["strategy_name_from_filename"] = parts[2]
        out["side_from_filename"] = parts[3]
        out["timeframe_from_filename"] = parts[4]

    return out


# ============================================================
# Inputs
# ============================================================

def parse_inputs(text: str) -> List[Dict[str, Any]]:
    inputs: List[Dict[str, Any]] = []
    pattern = re.compile(
        r'\binput\s+([A-Za-z_][A-Za-z0-9_]*)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?);'
    )

    for m in pattern.finditer(text):
        input_type = m.group(1).strip()
        name = m.group(2).strip()
        default_raw = m.group(3).strip()
        inputs.append({
            "type": input_type,
            "name": name,
            "default_raw": default_raw,
            "default": try_parse_number(default_raw),
        })

    return inputs


# ============================================================
# Timeframe helpers
# ============================================================

def map_mt5_timeframe(value: Any) -> Optional[str]:
    mapping = {
        1: "M1",
        5: "M5",
        15: "M15",
        30: "M30",
        60: "H1",
        240: "H4",
        480: "H8",
        720: "H12",
        1440: "D1",
        10080: "W1",
        43200: "MN1",
    }
    if isinstance(value, int):
        return mapping.get(value)
    if isinstance(value, str):
        try:
            return mapping.get(int(value))
        except Exception:
            if value in mapping.values():
                return value
    return None


# ============================================================
# Function / indicator extraction
# ============================================================

def extract_sqx_functions(text: str) -> List[str]:
    return sorted(set(re.findall(r'\b(sq[A-Za-z0-9_]+)\s*\(', text)))


def discover_indicator_files(indicator_root: Path) -> Dict[str, Path]:
    out: Dict[str, Path] = {}
    if not indicator_root.exists():
        return out
    for fp in indicator_root.rglob("*.mq5"):
        out[fp.stem] = fp
    return out


PYTHON_SUPPORTED_INDICATORS = {
    "SqATR",
    "SqADX",
    "SqSuperTrend",
    "SqIchimoku",
    "SqWPR",
    "SqParabolicSAR",
    "SqBearsPower",
    "SqHullMovingAverage",
    "SqFibo",
    "SqHighestInRange",
    "SqLowestInRange",
    "SqVWAP",
    "SqLinReg",
    "SqCCI",
    # standard-like
    "iMA",
    "iMACD",
    "iStdDev",
}


# ============================================================
# Expression parsing
# ============================================================

def normalize_expression(expr: str) -> str:
    x = expr
    x = x.replace("NULL", '"CurrentSymbol"')
    x = x.replace("correctSymbol(Subchart1Symbol)", '"Subchart1Symbol"')
    x = x.replace("TFMigrate(Subchart1Timeframe)", '"Subchart1Timeframe"')
    return x


def resolve_token(token: str, inputs_map: Dict[str, Any]) -> Any:
    tok = token.strip()
    if tok in inputs_map:
        return inputs_map[tok]
    return try_parse_number(tok)


def detect_expression_type(expr: str) -> str:
    expr = expr.strip()
    if expr.startswith("iCustom("):
        return "iCustom"
    if expr.startswith("iMA("):
        return "iMA"
    if expr.startswith("iMACD("):
        return "iMACD"
    if expr.startswith("iStdDev("):
        return "iStdDev"
    if expr == "255":
        return "constant"
    return "unknown"


def parse_expression_details(expr: str, inputs_map: Dict[str, Any]) -> Dict[str, Any]:
    expr_type = detect_expression_type(expr)
    out: Dict[str, Any] = {
        "expr_type": expr_type,
        "source": None,
        "timeframe_ref": None,
        "timeframe_resolved": None,
        "params": [],
        "raw_args": [],
    }

    if expr_type == "constant":
        out["source"] = "constant"
        out["params"] = [255]
        return out

    m = re.search(r'\((.*)\)', expr, re.S)
    if not m:
        return out

    raw_args = split_top_level_args(m.group(1))
    out["raw_args"] = raw_args

    if expr_type == "iCustom":
        # iCustom(symbol, timeframe, "IndicatorName", params...)
        if len(raw_args) >= 3:
            out["timeframe_ref"] = raw_args[1].strip()
            out["source"] = str(resolve_token(raw_args[2], inputs_map))
            out["params"] = [resolve_token(x, inputs_map) for x in raw_args[3:]]

    elif expr_type == "iMA":
        # iMA(symbol, timeframe, period, ma_shift, ma_method, applied_price)
        if len(raw_args) >= 6:
            out["timeframe_ref"] = raw_args[1].strip()
            out["source"] = "iMA"
            out["params"] = [
                resolve_token(raw_args[2], inputs_map),
                resolve_token(raw_args[3], inputs_map),
                resolve_token(raw_args[4], inputs_map),
                resolve_token(raw_args[5], inputs_map),
            ]

    elif expr_type == "iMACD":
        # iMACD(symbol, timeframe, fast, slow, signal, applied_price)
        if len(raw_args) >= 6:
            out["timeframe_ref"] = raw_args[1].strip()
            out["source"] = "iMACD"
            out["params"] = [
                resolve_token(raw_args[2], inputs_map),
                resolve_token(raw_args[3], inputs_map),
                resolve_token(raw_args[4], inputs_map),
                resolve_token(raw_args[5], inputs_map),
            ]

    elif expr_type == "iStdDev":
        # iStdDev(symbol, timeframe, period, ma_shift, ma_method, applied_price)
        if len(raw_args) >= 6:
            out["timeframe_ref"] = raw_args[1].strip()
            out["source"] = "iStdDev"
            out["params"] = [
                resolve_token(raw_args[2], inputs_map),
                resolve_token(raw_args[3], inputs_map),
                resolve_token(raw_args[4], inputs_map),
                resolve_token(raw_args[5], inputs_map),
            ]

    out["timeframe_resolved"] = resolve_timeframe_ref(out["timeframe_ref"], inputs_map)
    return out


def resolve_timeframe_ref(ref: Optional[str], inputs_map: Dict[str, Any]) -> Optional[str]:
    if ref is None:
        return None
    ref = ref.strip()
    if ref == "0":
        return None
    if ref in inputs_map:
        return map_mt5_timeframe(inputs_map[ref])
    if "Subchart1Timeframe" in ref:
        return map_mt5_timeframe(inputs_map.get("Subchart1Timeframe"))
    return map_mt5_timeframe(try_parse_number(ref))


# ============================================================
# Indicator buffers
# ============================================================

def parse_indicator_buffer_assignments(text: str, inputs_map: Dict[str, Any], indicator_file_map: Dict[str, Path]) -> List[Dict[str, Any]]:
    buffers: List[Dict[str, Any]] = []

    pattern_calls = re.compile(
        r'([A-Z0-9_]+)\s*\[\s*(\d+)\s*\]\s*=\s*(iCustom|iMA|iMACD|iStdDev)\s*\((.*?)\)\s*;',
        re.S
    )

    for m in pattern_calls.finditer(text):
        buffer_name = m.group(1).strip()
        buffer_index = int(m.group(2))
        fn = m.group(3).strip()
        args = m.group(4).strip()
        expr = f"{fn}({args})"

        details = parse_expression_details(expr, inputs_map)
        source = details["source"]

        indicator_file = None
        mq5_found = False
        if isinstance(source, str) and source in indicator_file_map:
            indicator_file = str(indicator_file_map[source])
            mq5_found = True

        python_supported = bool(source in PYTHON_SUPPORTED_INDICATORS)
        if source in {"iMA", "iMACD", "iStdDev"}:
            python_supported = True

        support_status = "supported" if python_supported else ("mq5_found_but_not_supported" if mq5_found else "unknown")

        buffers.append({
            "buffer_name": buffer_name,
            "buffer_index": buffer_index,
            "expression": expr,
            "expression_normalized": normalize_expression(expr),
            "expr_type": details["expr_type"],
            "source": source,
            "timeframe_ref": details["timeframe_ref"],
            "timeframe_resolved": details["timeframe_resolved"],
            "params": details["params"],
            "raw_args": details["raw_args"],
            "indicator_file": indicator_file,
            "mq5_found": mq5_found,
            "python_supported": python_supported,
            "support_status": support_status,
        })

    pattern_const = re.compile(r'([A-Z0-9_]+)\s*\[\s*(\d+)\s*\]\s*=\s*(255)\s*;')
    for m in pattern_const.finditer(text):
        buffers.append({
            "buffer_name": m.group(1).strip(),
            "buffer_index": int(m.group(2)),
            "expression": "255",
            "expression_normalized": "255",
            "expr_type": "constant",
            "source": "constant",
            "timeframe_ref": None,
            "timeframe_resolved": None,
            "params": [255],
            "raw_args": [],
            "indicator_file": None,
            "mq5_found": False,
            "python_supported": True,
            "support_status": "supported",
        })

    seen = set()
    out = []
    for b in buffers:
        key = (b["buffer_name"], b["buffer_index"], b["expression"])
        if key not in seen:
            out.append(b)
            seen.add(key)
    return out


# ============================================================
# Rules
# ============================================================

def normalize_condition(cond: Optional[str]) -> Optional[str]:
    if not cond:
        return None
    x = cond
    x = x.replace("NULL", '"CurrentSymbol"')
    x = re.sub(r'\s+', ' ', x)
    return x.strip()


def parse_order_action(block: str) -> Dict[str, Any]:
    order_type = None
    sl_expr = None
    pt_expr = None

    m_order = re.search(r'ORDER_TYPE_(BUY|SELL)', block)
    if m_order:
        order_type = f"ORDER_TYPE_{m_order.group(1)}"

    m_sl = re.search(r'sl_expr\s*=\s*(.*?);', block, re.S)
    if m_sl:
        sl_expr = clean_ws(m_sl.group(1))

    m_pt = re.search(r'pt_expr\s*=\s*(.*?);', block, re.S)
    if m_pt:
        pt_expr = clean_ws(m_pt.group(1))

    return {
        "order_type": order_type,
        "sl_expr": sl_expr,
        "pt_expr": pt_expr,
        "raw": clean_ws(block),
    }


def parse_rules(text: str) -> List[Dict[str, Any]]:
    rules: List[Dict[str, Any]] = []

    mapping = [
        ("Trading signals", "signals"),
        ("Long entry rule", "long_entry"),
        ("Short entry rule", "short_entry"),
        ("Long exit rule", "long_exit"),
        ("Short exit rule", "short_exit"),
        ("Rule 1", "generic"),
        ("Rule 2", "generic"),
        ("Rule 3", "generic"),
    ]

    for label, rule_type in mapping:
        idx = text.find(label)
        if idx == -1:
            continue

        next_idx = len(text)
        for other_label, _ in mapping:
            if other_label == label:
                continue
            oi = text.find(other_label, idx + len(label))
            if oi != -1:
                next_idx = min(next_idx, oi)

        body = text[idx:next_idx]
        body_clean = clean_ws(body)

        cond = None
        m_cond = re.search(r'(?:if\s*\((.*?)\))', body, re.S)
        if m_cond:
            cond = clean_ws(m_cond.group(1))

        action = parse_order_action(body)

        rules.append({
            "name": label,
            "type": rule_type,
            "condition_raw": cond,
            "condition_normalized": normalize_condition(cond),
            "body_raw": body_clean,
            "actions": [action] if any(action.values()) else [],
            "functions_used": extract_sqx_functions(body_clean),
        })

    return rules


def extract_signal_assignments(body_raw: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key in ["LongEntrySignal", "ShortEntrySignal", "LongExitSignal", "ShortExitSignal"]:
        m = re.search(rf'{key}\s*=\s*(.*?);', body_raw, re.S)
        if m:
            out[key] = clean_ws(m.group(1))
    return out


# ============================================================
# Indicator profile
# ============================================================

def profile_indicator_file(path: Path) -> Dict[str, Any]:
    raw = read_text(path)
    text = remove_comments(raw)
    inputs = parse_inputs(text)

    return {
        "profile_type": "indicator_profile",
        "file_name": path.name,
        "file_stem": path.stem,
        "path": str(path),
        "inputs": inputs,
        "sqx_functions": extract_sqx_functions(text),
        "uses_oncalculate": "OnCalculate" in text,
        "uses_copybuffer": "CopyBuffer(" in text,
        "uses_icustom": "iCustom(" in text,
        "uses_ima": "iMA(" in text,
    }


# ============================================================
# EA profile
# ============================================================

def extract_backtest_context(fname_meta: Dict[str, Any], inputs_map: Dict[str, Any], indicator_buffers: List[Dict[str, Any]], text: str) -> Dict[str, Any]:
    timeframes: List[str] = []

    primary_tf = fname_meta.get("timeframe_from_filename")
    if primary_tf:
        timeframes.append(primary_tf)

    for b in indicator_buffers:
        tf = b.get("timeframe_resolved")
        if tf and tf not in timeframes:
            timeframes.append(tf)

    sub_tf = map_mt5_timeframe(inputs_map.get("Subchart1Timeframe"))
    if sub_tf and sub_tf not in timeframes:
        timeframes.append(sub_tf)

    return {
        "symbol": fname_meta.get("symbol_from_filename"),
        "timeframes": timeframes,
        "uses_subchart1": "Subchart1Timeframe" in inputs_map or "Subchart1Symbol" in inputs_map,
        "has_money_management": bool(inputs_map.get("UseMoneyManagement", False)),
        "has_time_filters": bool(inputs_map.get("LimitTimeRange", False)),
        "has_eod_exit": bool(inputs_map.get("ExitAtEndOfDay", False)),
        "has_friday_exit": bool(inputs_map.get("ExitOnFriday", False)),
        "source_contains_sq_functions": bool(re.search(r'\bsq[A-Za-z0-9_]+\s*\(', text)),
    }


def profile_ea_file(path: Path, indicator_file_map: Dict[str, Path]) -> Dict[str, Any]:
    raw = read_text(path)
    text = remove_comments(raw)

    fname_meta = parse_strategy_filename(path)
    inputs = parse_inputs(text)
    inputs_map = {x["name"]: x["default"] for x in inputs}
    indicator_buffers = parse_indicator_buffer_assignments(text, inputs_map, indicator_file_map)
    rules = parse_rules(text)
    backtest_context = extract_backtest_context(fname_meta, inputs_map, indicator_buffers, text)

    used_indicators = sorted(set(
        x["source"] for x in indicator_buffers
        if isinstance(x.get("source"), str) and x["source"] not in {"constant", "iMA", "iMACD", "iStdDev"}
    ))

    support_summary = {
        "total_buffers": len(indicator_buffers),
        "supported_buffers": sum(1 for x in indicator_buffers if x["python_supported"]),
        "unsupported_buffers": sum(1 for x in indicator_buffers if not x["python_supported"]),
        "all_supported": all(x["python_supported"] for x in indicator_buffers),
    }

    profile = {
        "profile_type": "ea_profile",
        **fname_meta,
        "inputs": inputs,
        "indicator_buffers": indicator_buffers,
        "rules": rules,
        "backtest_context": backtest_context,
        "sqx_functions": extract_sqx_functions(text),
        "used_indicators": used_indicators,
        "support_summary": support_summary,
    }
    return profile


# ============================================================
# Normalized / compiled
# ============================================================

def build_normalized_strategy(profile: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "profile_type": "normalized_strategy",
        "strategy_id": profile["file_stem"],
        "strategy_name": profile.get("strategy_name_from_filename"),
        "symbol": profile.get("symbol_from_filename"),
        "direction": profile.get("side_from_filename"),
        "primary_timeframe": profile.get("timeframe_from_filename"),
        "inputs": profile.get("inputs", []),
        "rules": profile.get("rules", []),
        "indicator_buffers": profile.get("indicator_buffers", []),
        "metadata": {
            "backtest_context": profile.get("backtest_context", {}),
            "sqx_functions": profile.get("sqx_functions", []),
            "used_indicators": profile.get("used_indicators", []),
            "support_summary": profile.get("support_summary", {}),
        },
    }


def compile_time_filters(inputs: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "limit_time_range": bool(inputs.get("LimitTimeRange", False)),
        "signal_time_range_from": inputs.get("SignalTimeRangeFrom"),
        "signal_time_range_to": inputs.get("SignalTimeRangeTo"),
        "max_trades_per_day": inputs.get("MaxTradesPerDay"),
        "dont_trade_on_weekends": bool(inputs.get("DontTradeOnWeekends", False)),
        "exit_at_end_of_day": bool(inputs.get("ExitAtEndOfDay", False)),
        "eod_exit_time": inputs.get("EODExitTime"),
        "exit_on_friday": bool(inputs.get("ExitOnFriday", False)),
        "friday_exit_time": inputs.get("FridayExitTime"),
        "trade_in_session_hours_only": bool(inputs.get("tradeInSessionHoursOnly", False)),
    }


def simplify_rule(rule: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": rule["name"],
        "type": rule["type"],
        "condition_raw": rule.get("condition_raw"),
        "condition_normalized": rule.get("condition_normalized"),
        "functions_used": rule.get("functions_used", []),
        "actions": rule.get("actions", []),
    }


def classify_risk_expr(expr: Optional[str]) -> Dict[str, Any]:
    if not expr:
        return {"type": None}

    x = expr.strip()

    m = re.search(r'([A-Za-z0-9_.]+)\s*\*\s*sqGetIndicatorValue\(([A-Za-z0-9_]+),\s*(\d+)\)', x)
    if m:
        return {
            "type": "indicator_multiple",
            "coef": m.group(1),
            "indicator_ref": m.group(2),
            "shift": int(m.group(3)),
        }

    m = re.search(r'ORDER_TYPE_[A-Z]+,\s*openPrice,\s*\d+,\s*([A-Za-z0-9_.]+)\)', x)
    if m:
        return {
            "type": "fixed_points",
            "value_ref": m.group(1),
        }

    return {
        "type": "raw",
        "expr": x,
    }


def build_compiled_strategy(profile: Dict[str, Any]) -> Dict[str, Any]:
    inputs = {x["name"]: x["default"] for x in profile["inputs"]}
    rules = profile["rules"]

    signal_rule = next((r for r in rules if r["type"] == "signals"), None)
    long_entry = next((r for r in rules if r["type"] == "long_entry"), None)
    short_entry = next((r for r in rules if r["type"] == "short_entry"), None)
    long_exit = next((r for r in rules if r["type"] == "long_exit"), None)
    short_exit = next((r for r in rules if r["type"] == "short_exit"), None)
    generic_rules = [r for r in rules if r["type"] == "generic"]

    compiled_buffers = []
    for b in profile["indicator_buffers"]:
        compiled_buffers.append({
            "id": b["buffer_name"],
            "buffer_index": b["buffer_index"],
            "expression": b["expression"],
            "expr_type": b["expr_type"],
            "source": b["source"],
            "timeframe": b["timeframe_resolved"] or profile.get("timeframe_from_filename"),
            "params": b["params"],
            "mq5_found": b["mq5_found"],
            "indicator_file": b["indicator_file"],
            "python_supported": b["python_supported"],
            "support_status": b["support_status"],
        })

    risk_models = []
    for r in [long_entry, short_entry, *generic_rules]:
        if not r:
            continue
        acts = r.get("actions", [])
        if not acts:
            continue
        act = acts[0]
        risk_models.append({
            "rule_name": r["name"],
            "order_type": act.get("order_type"),
            "sl_expr": act.get("sl_expr"),
            "pt_expr": act.get("pt_expr"),
            "sl_model": classify_risk_expr(act.get("sl_expr")),
            "pt_model": classify_risk_expr(act.get("pt_expr")),
        })

    compiled = {
        "profile_type": "compiled_strategy",
        "strategy_id": profile["file_stem"],
        "symbol": profile["symbol_from_filename"],
        "direction": profile["side_from_filename"],
        "primary_timeframe": profile["timeframe_from_filename"],
        "secondary_timeframes": profile["backtest_context"]["timeframes"][1:] if profile["backtest_context"]["timeframes"] else [],
        "inputs": inputs,
        "time_filters": compile_time_filters(inputs),
        "buffers": compiled_buffers,
        "entry": {
            "mode": "signals_block" if signal_rule else ("generic_rules" if generic_rules else "unknown"),
            "signals_block": extract_signal_assignments(signal_rule["body_raw"]) if signal_rule else {},
            "long_rule": simplify_rule(long_entry) if long_entry else None,
            "short_rule": simplify_rule(short_entry) if short_entry else None,
            "generic_rules": [simplify_rule(x) for x in generic_rules],
        },
        "exit": {
            "signals_block": extract_signal_assignments(signal_rule["body_raw"]) if signal_rule else {},
            "long_exit_rule": simplify_rule(long_exit) if long_exit else None,
            "short_exit_rule": simplify_rule(short_exit) if short_exit else None,
        },
        "risk": {
            "models": risk_models,
        },
        "functions_used": profile["sqx_functions"],
        "used_indicators": profile["used_indicators"],
        "support_summary": profile["support_summary"],
    }

    return compiled


# ============================================================
# Runner
# ============================================================

def discover_mq5_files(root: Path) -> List[Path]:
    if not root.exists():
        return []
    return sorted(root.rglob("*.mq5"))


def run_profiler(
    data_root: Path,
    strategy_ea_root: Path,
    indicator_root: Path,
    profile_root: Path,
) -> Dict[str, Any]:
    ea_profiles_root = profile_root / "ea_profiles"
    indicator_profiles_root = profile_root / "indicator_profiles"
    normalized_root = profile_root / "normalized_strategies"
    compiled_root = profile_root / "compiled_strategies"

    for p in [ea_profiles_root, indicator_profiles_root, normalized_root, compiled_root]:
        ensure_dir(p)

    indicator_file_map = discover_indicator_files(indicator_root)

    ea_files = discover_mq5_files(strategy_ea_root)
    indicator_files = discover_mq5_files(indicator_root)

    ea_results = []
    indicator_results = []
    errors = []

    for fp in indicator_files:
        try:
            prof = profile_indicator_file(fp)
            out_path = indicator_profiles_root / f"{fp.stem}.json"
            save_json(out_path, prof)
            indicator_results.append({
                "file": str(fp),
                "profile": str(out_path),
            })
        except Exception as exc:
            errors.append({
                "type": "indicator_profile",
                "file": str(fp),
                "error": repr(exc),
            })

    for fp in ea_files:
        try:
            prof = profile_ea_file(fp, indicator_file_map)

            symbol = prof.get("symbol_from_filename") or "UNKNOWN"
            ea_profile_path = ea_profiles_root / symbol / f"{fp.stem}.json"
            normalized_path = normalized_root / symbol / f"{fp.stem}.json"
            compiled_path = compiled_root / symbol / f"{fp.stem}.compiled.json"

            save_json(ea_profile_path, prof)
            save_json(normalized_path, build_normalized_strategy(prof))
            save_json(compiled_path, build_compiled_strategy(prof))

            ea_results.append({
                "file": str(fp),
                "ea_profile": str(ea_profile_path),
                "normalized_strategy": str(normalized_path),
                "compiled_strategy": str(compiled_path),
                "symbol": symbol,
                "strategy_id": fp.stem,
                "all_supported": prof["support_summary"]["all_supported"],
            })

        except Exception as exc:
            errors.append({
                "type": "ea_profile",
                "file": str(fp),
                "error": repr(exc),
            })

    index = {
        "meta": {
            "data_root": str(data_root),
            "strategy_ea_root": str(strategy_ea_root),
            "indicator_root": str(indicator_root),
            "profile_root": str(profile_root),
        },
        "summary": {
            "ea_files": len(ea_files),
            "indicator_files": len(indicator_files),
            "ea_profiles_created": len(ea_results),
            "indicator_profiles_created": len(indicator_results),
            "errors": len(errors),
        },
        "ea_results": ea_results,
        "indicator_results": indicator_results,
        "errors": errors,
    }

    save_json(profile_root / "_index.json", index)
    return index


# ============================================================
# CLI
# ============================================================

def main() -> int:
    parser = argparse.ArgumentParser(description="Strategy Profiler V4 for MT5/SQX")
    parser.add_argument("--data-root", type=str, default=None)
    parser.add_argument("--strategy-ea-root", type=str, default=None)
    parser.add_argument("--indicator-root", type=str, default=None)
    parser.add_argument("--profile-root", type=str, default=None)
    args = parser.parse_args()

    script_path = Path(__file__).resolve()

    if args.data_root:
        data_root = Path(args.data_root).expanduser().resolve()
    else:
        data_root = find_default_data_root(script_path)

    if data_root is None or not data_root.exists():
        print("FEHLER: Data root konnte nicht gefunden werden.", file=sys.stderr)
        return 1

    paths = derive_paths(data_root)

    strategy_ea_root = Path(args.strategy_ea_root).expanduser().resolve() if args.strategy_ea_root else paths["strategy_ea_root"]
    indicator_root = Path(args.indicator_root).expanduser().resolve() if args.indicator_root else paths["indicator_root"]
    profile_root = Path(args.profile_root).expanduser().resolve() if args.profile_root else paths["profile_root"]

    if not strategy_ea_root.exists():
        print(f"FEHLER: Strategy_EA root nicht gefunden: {strategy_ea_root}", file=sys.stderr)
        return 1

    if not indicator_root.exists():
        print(f"FEHLER: Indicator root nicht gefunden: {indicator_root}", file=sys.stderr)
        return 1

    ensure_dir(profile_root)

    index = run_profiler(
        data_root=data_root,
        strategy_ea_root=strategy_ea_root,
        indicator_root=indicator_root,
        profile_root=profile_root,
    )

    print("FERTIG")
    print(f"Data root:          {data_root}")
    print(f"Strategy EA root:   {strategy_ea_root}")
    print(f"Indicator root:     {indicator_root}")
    print(f"Profile root:       {profile_root}")
    print(f"EA files:           {index['summary']['ea_files']}")
    print(f"Indicator files:    {index['summary']['indicator_files']}")
    print(f"EA profiles:        {index['summary']['ea_profiles_created']}")
    print(f"Indicator profiles: {index['summary']['indicator_profiles_created']}")
    print(f"Errors:             {index['summary']['errors']}")
    print(f"Index:              {profile_root / '_index.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())