import json
import re
from pathlib import Path


# =========================
# NEUE PROJEKT-STRUKTUR
# =========================

def find_ftmo_root(start: Path) -> Path:
    cur = start.resolve()
    for p in [cur] + list(cur.parents):
        if (p / "Data_Center").exists() and (p / "Dashboards").exists():
            return p
    raise RuntimeError(
        f"FTMO-Root nicht gefunden. Erwartet Root mit 'Data_Center' und 'Dashboards'. Start={start}"
    )


SCRIPT_PATH = Path(__file__).resolve()
FTMO_ROOT = find_ftmo_root(SCRIPT_PATH)

EA_DIR = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Strategy"
    / "Strategy_EA"
)

OUT_DIR = (
    FTMO_ROOT
    / "Data_Center"
    / "Data"
    / "Strategy"
    / "Strategy_Profile"
)


# =========================
# REGEX
# =========================

RE_INPUT = re.compile(
    r"""^\s*input\s+(?P<type>\w+)\s+(?P<name>\w+)\s*=\s*(?P<value>[^;]+)\s*;""",
    re.MULTILINE,
)

RE_SQATR = re.compile(
    r"""iCustom\(\s*NULL\s*,\s*(?P<tf>\d+)\s*,\s*["']SqATR["']\s*,\s*(?P<period>\d+)\s*\)""",
    re.IGNORECASE,
)

RE_SQATR_ANY = re.compile(r"""["']SqATR["']""", re.IGNORECASE)

RE_STRATEGY_ID = re.compile(
    r"""^\d+(?:[._]\d+)+$"""
)

RE_NEW_FILENAME = re.compile(
    r"""^(?P<symbol>.+?)_(?P<variant>\d+)_(?P<strategy_id>\d+(?:[._]\d+)+)_(?P<side>BUY|SELL|BOTH)_(?P<timeframe>[A-Za-z0-9.]+)$""",
    re.IGNORECASE,
)


# =========================
# HELPERS
# =========================

def strip_quotes(v: str) -> str:
    v = v.strip()
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        return v[1:-1]
    return v


def parse_value(type_name: str, raw_value: str):
    type_name = type_name.lower()
    value = strip_quotes(raw_value.split("//")[0].strip())

    if type_name == "bool":
        return value.lower() == "true"

    if type_name in ["int", "long", "short", "uint", "ulong"]:
        try:
            return int(float(value))
        except ValueError:
            return value

    if type_name in ["double", "float"]:
        try:
            return float(value)
        except ValueError:
            return value

    return value


def parse_inputs(text: str):
    inputs = {}
    for match in RE_INPUT.finditer(text):
        name = match.group("name")
        value = parse_value(match.group("type"), match.group("value"))
        inputs[name] = value
    return inputs


def normalize_side(side: str) -> str:
    s = str(side).upper()
    if s in {"BUY", "SELL", "BOTH"}:
        return s
    return "unknown"


def normalize_strategy_id(raw_sid: str) -> str:
    return raw_sid.replace("_", ".")


def sanitize_filename(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', "_", name)


def fmt_num(value):
    if value is None:
        return None

    try:
        f = float(value)
    except (TypeError, ValueError):
        return str(value)

    if f.is_integer():
        return str(int(f))

    return str(f).replace(".", "")


def safe_read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        try:
            return path.read_text(errors="ignore")
        except Exception:
            return ""


# =========================
# FILENAME PARSER
# NEUES FORMAT:
# SYMBOL_VARIANT_STRATEGYID_SIDE_TIMEFRAME
# z.B.
# AUDJPY_1_3.14.146_BUY_M15
# =========================

def infer_from_filename(path: Path):
    stem = path.stem
    parent_symbol = path.parent.name

    result = {
        "strategy_id": "unknown",
        "symbol": parent_symbol if parent_symbol else "unknown",
        "side": "unknown",
        "variant_number": None,
        "magic_number": None,
        "timeframe": None,
        "date_block": None,
    }

    m = RE_NEW_FILENAME.fullmatch(stem)
    if m:
        symbol = m.group("symbol")
        variant = m.group("variant")
        strategy_id = m.group("strategy_id")
        side = m.group("side")
        timeframe = m.group("timeframe")

        result["symbol"] = symbol
        result["variant_number"] = int(variant) if str(variant).isdigit() else None
        result["strategy_id"] = normalize_strategy_id(strategy_id) if RE_STRATEGY_ID.fullmatch(strategy_id) else "unknown"
        result["side"] = normalize_side(side)
        result["timeframe"] = str(timeframe).upper()
        return result

    parts = stem.split("_")

    # Fallback-Logik für unklare Dateinamen
    # sucht Strategy-ID, Side, Timeframe flexibel
    for token in parts:
        if RE_STRATEGY_ID.fullmatch(token):
            result["strategy_id"] = normalize_strategy_id(token)
            break

    for token in parts:
        ns = normalize_side(token)
        if ns != "unknown":
            result["side"] = ns
            break

    # Variante häufig an Position 1
    if len(parts) >= 2 and parts[1].isdigit():
        result["variant_number"] = int(parts[1])

    # Timeframe meist letztes Token
    if len(parts) >= 1:
        last_token = parts[-1].upper()
        if re.fullmatch(r"[MHDW]\d+|MN\d*|[A-Z0-9.]+", last_token):
            result["timeframe"] = last_token

    # Symbol aus Filename bevorzugen, sonst Ordnername
    if len(parts) >= 1 and parts[0]:
        result["symbol"] = parts[0]

    return result


# =========================
# ATR DETECTION
# =========================

def parse_atr_spec(text: str):
    m = RE_SQATR.search(text)
    if m:
        return {
            "base": "SqATR",
            "atr_period": int(m.group("period")),
            "atr_timeframe_code": int(m.group("tf")),
            "atr_shift": 1,
        }

    if RE_SQATR_ANY.search(text):
        return {
            "base": "SqATR",
            "atr_period": None,
            "atr_timeframe_code": None,
            "atr_shift": 1,
        }

    return None


# =========================
# SIGNAL DETECTION
# =========================

def detect_signal_family(inputs: dict):
    families = []

    if any(k in inputs for k in ["EMAPeriod1"]):
        families.append("EMA")

    if any(k in inputs for k in [
        "MACDFast1", "MACDSlow1", "MACDSmooth1",
        "MACDMainFast1", "MACDMainSlow1", "MACDMainSmooth1",
        "MACDMainCrossZroFst1", "MACDMainCrossZroSlw1", "MACDMainCrossZroSmt1"
    ]):
        families.append("MACD")

    if any(k in inputs for k in ["ADXLowerPeriod1", "ADXHigherPeriod1"]):
        families.append("ADX")

    if any(k in inputs for k in ["BollingerBandsPrd1"]):
        families.append("BB")

    if any(k in inputs for k in [
        "OSMAChangesFastEMA1", "OSMAChangesSlowEMA1", "OSMAChangesSgnPrd1",
        "OSMAChangesFastEMA2", "OSMAChangesSlowEMA2", "OSMAChangesSgnPrd2",
        "OSMAChangesSgnPrd3"
    ]):
        families.append("OSMA")

    if any(k in inputs for k in ["MTATRPeriod1", "MTATRPeriod2"]):
        families.append("ATR")

    if any(k in inputs for k in ["WoodiesZLRPeriod1"]):
        families.append("WZLR")

    return families


def build_signal_label(signal_family):
    if not signal_family:
        return "UNKNOWN"
    return "_".join(signal_family)


def build_time_label(inputs: dict, identity: dict):
    enabled = inputs.get("LimitTimeRange")
    from_time = inputs.get("SignalTimeRangeFrom")
    to_time = inputs.get("SignalTimeRangeTo")

    if enabled and from_time and to_time:
        return f"{str(from_time).replace(':', '')}_{str(to_time).replace(':', '')}"

    timeframe = identity.get("timeframe")
    if timeframe:
        return str(timeframe)

    return "FULLDAY"


# =========================
# EXIT / RISK
# =========================

def classify_exit_profile(fixed_sl, fixed_tp, sl_coef, tp_coef, trailing_coef, risk_model):
    if fixed_sl is not None:
        sl_type = "fixed_pips"
    elif sl_coef is not None:
        if risk_model.get("type") == "coef_atr":
            sl_type = "atr_coef"
        else:
            sl_type = "dynamic_unknown"
    else:
        sl_type = "unknown"

    if fixed_tp is not None:
        tp_type = "fixed_pips"
    elif tp_coef is not None:
        tp_type = "coef"
    else:
        tp_type = "none"

    if trailing_coef is not None:
        trailing_type = "coef_trailing"
    else:
        trailing_type = "none"

    if sl_type == "fixed_pips" and tp_type == "fixed_pips":
        exit_profile = "fixed_sl_fixed_tp"
    elif sl_type == "fixed_pips" and tp_type == "coef":
        exit_profile = "fixed_sl_dynamic_tp"
    elif sl_type in ["atr_coef", "dynamic_unknown"] and tp_type == "coef" and trailing_type == "none":
        exit_profile = "dynamic_sl_dynamic_tp"
    elif tp_type == "coef" and trailing_type == "coef_trailing" and sl_type == "unknown":
        exit_profile = "dynamic_tp_trailing"
    elif tp_type == "coef" and trailing_type == "coef_trailing" and sl_type in ["atr_coef", "dynamic_unknown"]:
        exit_profile = "dynamic_sl_dynamic_tp_trailing"
    elif trailing_type == "coef_trailing" and tp_type == "none":
        exit_profile = "trailing_only"
    else:
        exit_profile = "unknown_exit_profile"

    return {
        "sl_type": sl_type,
        "tp_type": tp_type,
        "trailing_type": trailing_type,
        "exit_profile": exit_profile,
    }


def build_exit_label(fixed_sl, fixed_tp, sl_coef, tp_coef, trailing_coef, risk_model):
    if fixed_sl is not None and fixed_tp is not None:
        return f"FIXED_SL{fmt_num(fixed_sl)}_TP{fmt_num(fixed_tp)}"

    if fixed_sl is not None and tp_coef is not None and trailing_coef is None:
        return f"FIXED_SL{fmt_num(fixed_sl)}_DYNAMIC_TP{fmt_num(tp_coef)}"

    if fixed_sl is not None and tp_coef is not None and trailing_coef is not None:
        return (
            f"FIXED_SL{fmt_num(fixed_sl)}_"
            f"DYNAMIC_TP{fmt_num(tp_coef)}_"
            f"TRAIL{fmt_num(trailing_coef)}"
        )

    if sl_coef is not None and tp_coef is not None and trailing_coef is None:
        if risk_model.get("type") == "coef_atr":
            return f"ATR_SL{fmt_num(sl_coef)}_TP{fmt_num(tp_coef)}"
        return f"DYN_SL{fmt_num(sl_coef)}_TP{fmt_num(tp_coef)}"

    if sl_coef is not None and tp_coef is not None and trailing_coef is not None:
        if risk_model.get("type") == "coef_atr":
            return f"ATR_SL{fmt_num(sl_coef)}_TP{fmt_num(tp_coef)}_TRAIL{fmt_num(trailing_coef)}"
        return f"DYN_SL{fmt_num(sl_coef)}_TP{fmt_num(tp_coef)}_TRAIL{fmt_num(trailing_coef)}"

    if tp_coef is not None and trailing_coef is not None:
        return f"DYNAMIC_TP{fmt_num(tp_coef)}_TRAIL{fmt_num(trailing_coef)}"

    if trailing_coef is not None:
        return f"TRAIL{fmt_num(trailing_coef)}"

    if fixed_sl is not None and fixed_tp is None:
        return f"FIXED_SL{fmt_num(fixed_sl)}"

    if sl_coef is not None and tp_coef is None:
        if risk_model.get("type") == "coef_atr":
            return f"ATR_SL{fmt_num(sl_coef)}"
        return f"DYN_SL{fmt_num(sl_coef)}"

    return "UNKNOWN_EXIT"


def build_quality_checks(profile: dict, classification: dict):
    money_management = profile.get("money_management", {})
    fixed = profile.get("trade_parameters", {}).get("fixed", {})
    filters = profile.get("filters", {})
    risk_model = profile.get("risk_model", {})

    risk_percent = money_management.get("risk_percent")
    fixed_sl = fixed.get("stop_loss_pips")

    has_defined_stop_loss = classification["sl_type"] in ["fixed_pips", "atr_coef", "dynamic_unknown"]
    has_defined_take_profit_or_exit = (
        classification["tp_type"] != "none"
        or classification["trailing_type"] != "none"
        or filters.get("exit_at_end_of_day", {}).get("enabled") is True
        or filters.get("exit_on_friday", {}).get("enabled") is True
    )

    uses_aggressive_mm = False
    if risk_percent is not None:
        try:
            uses_aggressive_mm = float(risk_percent) >= 5.0
        except Exception:
            uses_aggressive_mm = False

    has_parser_uncertainty = risk_model.get("type") in ["unknown", "dynamic_unknown"]

    has_tight_stop = False
    if fixed_sl is not None:
        try:
            symbol = profile.get("identity", {}).get("symbol", "")
            if "JPY" in symbol and float(fixed_sl) <= 10:
                has_tight_stop = True
            elif symbol.endswith(".cash"):
                has_tight_stop = False
            elif float(fixed_sl) <= 5:
                has_tight_stop = True
        except Exception:
            has_tight_stop = False

    return {
        "has_defined_stop_loss": has_defined_stop_loss,
        "has_defined_take_profit_or_exit": has_defined_take_profit_or_exit,
        "uses_aggressive_mm": uses_aggressive_mm,
        "has_parser_uncertainty": has_parser_uncertainty,
        "has_tight_stop": has_tight_stop,
        "weekend_protection": filters.get("dont_trade_on_weekends", {}).get("enabled") is True,
        "eod_exit": filters.get("exit_at_end_of_day", {}).get("enabled") is True,
        "friday_exit": filters.get("exit_on_friday", {}).get("enabled") is True,
    }


# =========================
# NAME BUILDER
# NEUE BASIS:
# SYMBOL_VARIANT_STRATEGYID_SIDE_TIMEFRAME
# =========================

def build_base_name(identity: dict) -> str:
    parts = []

    symbol = identity.get("symbol", "unknown")
    variant_number = identity.get("variant_number")
    strategy_id = identity.get("strategy_id", "unknown")
    side = identity.get("side", "unknown")
    timeframe = identity.get("timeframe")

    parts.append(symbol)

    if variant_number is not None:
        parts.append(str(variant_number))

    parts.append(strategy_id)
    parts.append(side)

    if timeframe:
        parts.append(str(timeframe))

    return "_".join([str(x) for x in parts if x is not None and str(x) != ""])


def build_display_name(identity: dict, exit_label: str) -> str:
    return f"{build_base_name(identity)}__{exit_label}"


def build_extended_display_name(identity: dict, exit_label: str, signal_label: str, time_label: str) -> str:
    base = build_base_name(identity)
    return f"{base}__{exit_label}__SIG_{signal_label}__TIME_{time_label}"


# =========================
# PROFILE BUILDER
# =========================

def build_profile(ea_path: Path):
    text = safe_read_text(ea_path)
    inputs = parse_inputs(text)
    info = infer_from_filename(ea_path)

    fixed_sl = inputs.get("StopLoss1")
    fixed_tp = inputs.get("ProfitTarget1")

    sl_coef = inputs.get("StopLossCoef1")
    tp_coef = inputs.get("ProfitTargetCoef1")
    trailing_coef = inputs.get("TrailingStopCoef1")

    atr_spec = parse_atr_spec(text)

    if fixed_sl is not None:
        risk_model = {
            "type": "fixed_pips",
            "stop_loss_pips": float(fixed_sl),
        }
    elif sl_coef is not None:
        if atr_spec and atr_spec.get("base") == "SqATR":
            risk_model = {
                "type": "coef_atr",
                "stop_loss_coef": float(sl_coef),
                "take_profit_coef": float(tp_coef) if tp_coef is not None else None,
                "atr": atr_spec,
            }
        else:
            risk_model = {
                "type": "dynamic_unknown",
                "stop_loss_coef": float(sl_coef),
                "take_profit_coef": float(tp_coef) if tp_coef is not None else None,
                "note": "Coef-based exits detected but ATR/base not identified from code.",
            }
    else:
        risk_model = {
            "type": "unknown"
        }

    classification = classify_exit_profile(
        fixed_sl=fixed_sl,
        fixed_tp=fixed_tp,
        sl_coef=sl_coef,
        tp_coef=tp_coef,
        trailing_coef=trailing_coef,
        risk_model=risk_model,
    )

    exit_label = build_exit_label(
        fixed_sl=fixed_sl,
        fixed_tp=fixed_tp,
        sl_coef=sl_coef,
        tp_coef=tp_coef,
        trailing_coef=trailing_coef,
        risk_model=risk_model,
    )

    signal_family = detect_signal_family(inputs)
    signal_label = build_signal_label(signal_family)
    time_label = build_time_label(inputs, info)

    base_name = build_base_name(info)
    display_name = build_display_name(info, exit_label)
    extended_display_name = build_extended_display_name(
        identity=info,
        exit_label=exit_label,
        signal_label=signal_label,
        time_label=time_label,
    )

    profile = {
        "schema_version": "2.0",
        "source": {
            "ea_file": ea_path.name,
            "ea_path": ea_path.as_posix(),
            "ea_extension": ea_path.suffix.lower(),
        },
        "identity": info,
        "profile_naming": {
            "base_name": base_name,
            "exit_label": exit_label,
            "signal_label": signal_label,
            "time_label": time_label,
            "display_name": display_name,
            "extended_display_name": extended_display_name,
        },
        "trade_parameters": {
            "fixed": {
                "stop_loss_pips": float(fixed_sl) if fixed_sl is not None else None,
                "take_profit_pips": float(fixed_tp) if fixed_tp is not None else None,
            },
            "dynamic_coef": {
                "stop_loss_coef": float(sl_coef) if sl_coef is not None else None,
                "take_profit_coef": float(tp_coef) if tp_coef is not None else None,
                "trailing_stop_coef": float(trailing_coef) if trailing_coef is not None else None,
            },
        },
        "risk_model": risk_model,
        "classification": {
            **classification,
            "signal_family": signal_family,
            "overlap_key": f"{info.get('symbol', 'unknown')}_{info.get('side', 'unknown')}",
        },
        "money_management": {
            "enabled": inputs.get("UseMoneyManagement"),
            "risk_percent": inputs.get("mmRiskPercent"),
            "fixed_lot": inputs.get("FixedLotSize"),
            "initial_capital": inputs.get("InitialCapital"),
            "mm_stoploss_pips": inputs.get("mmStopLossPips") or inputs.get("mmStopLoss"),
            "mm_max_lots": inputs.get("mmMaxLots"),
            "mm_lots_if_no_mm": inputs.get("mmLotsIfNoMM"),
        },
        "signal_parameters": {
            "MACD": {
                "fast": inputs.get("MACDFast1") or inputs.get("MACDMainFast1") or inputs.get("MACDMainCrossZroFst1"),
                "slow": inputs.get("MACDSlow1") or inputs.get("MACDMainSlow1") or inputs.get("MACDMainCrossZroSlw1"),
                "signal": inputs.get("MACDSmooth1") or inputs.get("MACDMainSmooth1") or inputs.get("MACDMainCrossZroSmt1"),
            }
        },
        "filters": {
            "dont_trade_on_weekends": {
                "enabled": inputs.get("DontTradeOnWeekends"),
                "friday_close_time": inputs.get("FridayCloseTime"),
                "sunday_open_time": inputs.get("SundayOpenTime"),
            },
            "exit_at_end_of_day": {
                "enabled": inputs.get("ExitAtEndOfDay"),
                "time": inputs.get("EODExitTime") or inputs.get("EndOfDayExitTime"),
            },
            "exit_on_friday": {
                "enabled": inputs.get("ExitOnFriday"),
                "time": inputs.get("FridayExitTime"),
            },
            "limit_time_range": {
                "enabled": inputs.get("LimitTimeRange"),
                "from": inputs.get("SignalTimeRangeFrom"),
                "to": inputs.get("SignalTimeRangeTo"),
            },
            "max_trades_per_day": inputs.get("MaxTradesPerDay"),
        },
        "checks": {},
        "raw_inputs": inputs,
    }

    profile["checks"] = build_quality_checks(profile, profile["classification"])
    return profile


# =========================
# OUTPUT PATH
# verhindert Überschreiben bei Namenskollision
# =========================

def make_unique_output_path(symbol_dir: Path, desired_filename: str) -> Path:
    target = symbol_dir / desired_filename
    if not target.exists():
        return target

    stem = target.stem
    suffix = target.suffix
    counter = 2

    while True:
        candidate = symbol_dir / f"{stem}__DUP{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


# =========================
# MAIN
# =========================

def main():
    if not EA_DIR.exists():
        raise FileNotFoundError(f"EA directory not found: {EA_DIR}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    ea_files = list(EA_DIR.rglob("*.mq5")) + list(EA_DIR.rglob("*.ex5"))

    if not ea_files:
        print("No EA files found.")
        return

    print(f"FTMO_ROOT : {FTMO_ROOT}")
    print(f"EA_DIR    : {EA_DIR}")
    print(f"OUT_DIR   : {OUT_DIR}")
    print(f"Found {len(ea_files)} EA files.\n")

    ok_count = 0
    err_count = 0

    for ea_file in sorted(ea_files):
        try:
            profile = build_profile(ea_file)
            identity = profile["identity"]

            symbol = identity.get("symbol", "unknown") or "unknown"
            symbol_dir = OUT_DIR / symbol
            symbol_dir.mkdir(parents=True, exist_ok=True)

            filename = sanitize_filename(f"{profile['profile_naming']['extended_display_name']}.json")
            output_path = make_unique_output_path(symbol_dir, filename)

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(profile, f, indent=2, ensure_ascii=False)

            print(
                f"[OK] {ea_file.name} "
                f"| symbol={identity.get('symbol')} "
                f"| variant={identity.get('variant_number')} "
                f"| sid={identity.get('strategy_id')} "
                f"| side={identity.get('side')} "
                f"| tf={identity.get('timeframe')} "
                f"-> {output_path.relative_to(OUT_DIR)}"
            )
            ok_count += 1

        except Exception as e:
            print(f"[ERROR] {ea_file.name}: {e}")
            err_count += 1

    print(f"\nFinished. OK={ok_count}, ERROR={err_count}")


if __name__ == "__main__":
    main()