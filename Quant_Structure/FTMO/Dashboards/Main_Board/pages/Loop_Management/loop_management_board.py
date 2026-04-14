# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Dashboards/Main_Board/pages/Loop_Management/loop_management_board.py

Zweck:
- Eingebettetes Loop Management Panel für das Main Board
- Kann zusätzlich weiterhin standalone gestartet werden
- Einheitlicher FTMO-Stil passend zu Main Board / Code Station / Knowledge Board
- Robuste Script-Auflösung
- Echter Auto-Restart

Wichtig:
- Für Einbettung im Main Board existiert:
    class LoopManagementPanel(tk.Frame)
- Für Standalone existiert zusätzlich:
    class LoopManagementBoardApp(tk.Tk)
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import tkinter as tk
from tkinter import ttk, messagebox


# ============================================================
# PATH HELPERS
# ============================================================

def find_ftmo_root(start: Path) -> Path:
    cur = start.resolve()
    for p in [cur] + list(cur.parents):
        if (p / "Data_Center").exists() and (p / "Dashboards").exists():
            return p
    raise RuntimeError(
        f"FTMO-Root nicht gefunden. Erwartet FTMO-Root mit 'Data_Center' und 'Dashboards'. Start={start}"
    )


SCRIPT_PATH = Path(__file__).resolve()
FTMO_ROOT = find_ftmo_root(SCRIPT_PATH)

DASHBOARDS_DIR = FTMO_ROOT / "Dashboards"
LOOP_MGMT_DIR = SCRIPT_PATH.parent
RUNTIME_DIR = LOOP_MGMT_DIR / "runtime"
STATE_FILE = RUNTIME_DIR / "loop_state.json"
LOG_DIR = RUNTIME_DIR / "logs"


# ============================================================
# THEME
# ============================================================

BG_APP = "#0A1118"
BG_TOP = "#0E1721"
BG_SURFACE = "#101B27"
BG_CARD = "#142131"
BG_CARD_HOVER = "#1A2A3D"
BG_EDITOR = "#0F1722"
BG_INPUT = "#0F1A26"
BG_STATUS = "#0C141D"

BG_BUTTON = "#2563EB"
BG_BUTTON_HOVER = "#3B82F6"
BG_BUTTON_PRESSED = "#1D4ED8"

BG_BUTTON_SECONDARY = "#223246"
BG_BUTTON_SECONDARY_HOVER = "#2C415A"
BG_BUTTON_SECONDARY_PRESSED = "#1C2B3C"

BG_BADGE = "#132130"
BG_BADGE_OPEN = "#10281D"
BG_BADGE_WARN = "#2C2110"

FG_MAIN = "#EAF2F9"
FG_MUTED = "#93A4B5"
FG_SUBTLE = "#708396"
FG_WHITE = "#FFFFFF"
FG_ACCENT = "#60A5FA"
FG_POS = "#22C55E"
FG_WARN = "#F59E0B"
FG_NEG = "#EF4444"

BORDER = "#223244"
DIVIDER = "#1B2A39"

FONT_TITLE = ("Segoe UI", 18, "bold")
FONT_TOP = ("Segoe UI", 10)
FONT_SECTION = ("Segoe UI", 11, "bold")
FONT_LABEL = ("Segoe UI", 9)
FONT_TEXT = ("Segoe UI", 10)
FONT_SMALL = ("Segoe UI", 8)
FONT_BADGE = ("Segoe UI", 8, "bold")
FONT_BUTTON = ("Segoe UI", 9, "bold")
FONT_MONO = ("Consolas", 10)


# ============================================================
# HELPERS
# ============================================================

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def find_script_by_name(project_root: Path, filename: str) -> Optional[Path]:
    hits = list(project_root.rglob(filename))
    if not hits:
        return None

    hits_sorted = sorted(
        hits,
        key=lambda p: (
            "Data_Center" not in str(p),
            "Data_Operations" not in str(p),
            len(str(p)),
        ),
    )
    return hits_sorted[0]


def resolve_script_path(loop_cfg: Dict[str, Any]) -> Path:
    raw_script_path = loop_cfg.get("script_path")
    if raw_script_path:
        p = Path(raw_script_path)
        if not p.is_absolute():
            p = FTMO_ROOT / p
        if p.exists():
            return p.resolve()

    for candidate in loop_cfg.get("script_candidates", []) or []:
        p = Path(candidate)
        if not p.is_absolute():
            p = FTMO_ROOT / p
        if p.exists():
            return p.resolve()

    script_name = str(loop_cfg.get("script_name", "")).strip()
    if script_name:
        found = find_script_by_name(FTMO_ROOT, script_name)
        if found is not None and found.exists():
            return found.resolve()

    raise FileNotFoundError(
        f"Script nicht gefunden für loop_id={loop_cfg.get('id')} | "
        f"script_path={raw_script_path} | "
        f"script_name={loop_cfg.get('script_name')}"
    )


def shorten_path(s: str, max_len: int = 78) -> str:
    if len(s) <= max_len:
        return s
    return "..." + s[-(max_len - 3):]


def format_epoch(ts: Optional[float]) -> str:
    if not ts:
        return ""
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
    except Exception:
        return ""


def format_uptime(started_at_epoch: Optional[float], running: bool) -> str:
    if not started_at_epoch or not running:
        return ""
    elapsed = max(0, int(time.time() - started_at_epoch))
    hh = elapsed // 3600
    mm = (elapsed % 3600) // 60
    ss = elapsed % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


# ============================================================
# LOOP CONFIG
# ============================================================

LOOP_DEFINITIONS: List[Dict[str, object]] = [
    {
        "id": "spread_logger",
        "label": "Spread Logger",
        "script_name": "Spread_Data_Management.py",
        "script_candidates": [
            "Data_Center/Data_Operations/Market/Spread_Logger_Loop/Spread_Data_Management.py",
            "Data_Center/Data_Operations/Market/Spread_Logger/Spread_Data_Management.py",
        ],
        "cwd": None,
        "env": {},
        "auto_restart": False,
    },
    {
        "id": "ohlc_loader",
        "label": "OHLC Loader",
        "script_name": "Ohcl_Loader.py",
        "script_candidates": [
            "Data_Center/Data_Operations/Market/Ohcl_Logger_Loop/Ohcl_Loader.py",
            "Data_Center/Data_Operations/Market/Ohcl_Generator_Loop/Ohcl_Loader.py",
            "Data_Center/Data_Operations/Market/Ohcl_Loader.py",
        ],
        "cwd": None,
        "env": {},
        "auto_restart": False,
    },
    {
        "id": "strategy_performance_loop",
        "label": "Strategy Perf",
        "script_name": "Strategy_Performance_Loop.py",
        "script_candidates": [
            "Data_Center/Data_Operations/Trades/Strategy_Performance_Loop/Strategy_Performance_Loop.py",
            "Data_Center/Data_Operations/Strategy_Performance_Loop/Strategy_Performance_Loop.py",
        ],
        "cwd": None,
        "env": {},
        "auto_restart": False,
    },
    {
        "id": "trades_combiner",
        "label": "Trades Combiner",
        "script_name": "Trades_Combiner.py",
        "script_candidates": [
            "Data_Center/Data_Operations/Trades/Trades_Combiner/Trades_Combiner.py",
            "Data_Center/Data_Operations/Trades_Combiner/Trades_Combiner.py",
        ],
        "cwd": None,
        "env": {},
        "auto_restart": False,
    },
    {
        "id": "trade_logger_live_530164208",
        "label": "LIVE 530164208",
        "script_name": "Trade_Logger_FTMO_LIVE_530164208.py",
        "script_candidates": [
            "Data_Center/Data_Operations/Trades/Trade_Logger_Loop/Trade_Logger_FTMO_LIVE_530164208.py",
        ],
        "cwd": None,
        "env": {},
        "auto_restart": False,
    },
    {
        "id": "trade_logger_live_540130486",
        "label": "LIVE 540130486",
        "script_name": "Trade_Logger_FTMO_LIVE_540130486.py",
        "script_candidates": [
            "Data_Center/Data_Operations/Trades/Trade_Logger_Loop/Trade_Logger_FTMO_LIVE_540130486.py",
        ],
        "cwd": None,
        "env": {},
        "auto_restart": False,
    },
    {
        "id": "trade_logger_live_540136817",
        "label": "LIVE 540136817",
        "script_name": "Trade_Logger_FTMO_LIVE_540136817.py",
        "script_candidates": [
            "Data_Center/Data_Operations/Trades/Trade_Logger_Loop/Trade_Logger_FTMO_LIVE_540136817.py",
        ],
        "cwd": None,
        "env": {},
        "auto_restart": False,
    },
    {
        "id": "trade_logger_live_540136824",
        "label": "LIVE 540136824",
        "script_name": "Trade_Logger_FTMO_LIVE_540136824.py",
        "script_candidates": [
            "Data_Center/Data_Operations/Trades/Trade_Logger_Loop/Trade_Logger_FTMO_LIVE_540136824.py",
        ],
        "cwd": None,
        "env": {},
        "auto_restart": False,
    },
    {
        "id": "trade_logger_demo_1",
        "label": "DEMO 1",
        "script_name": "Trade_Logger_FTMO_DEMO_1.py",
        "script_candidates": [
            "Data_Center/Data_Operations/Trades/Trade_Logger_Loop/Trade_Logger_FTMO_DEMO_1.py",
        ],
        "cwd": None,
        "env": {},
        "auto_restart": False,
    },
    {
        "id": "trade_logger_demo_2",
        "label": "DEMO 2",
        "script_name": "Trade_Logger_FTMO_DEMO_2.py",
        "script_candidates": [
            "Data_Center/Data_Operations/Trades/Trade_Logger_Loop/Trade_Logger_FTMO_DEMO_2.py",
        ],
        "cwd": None,
        "env": {},
        "auto_restart": False,
    },
]


# ============================================================
# DATA MODELS
# ============================================================

@dataclass
class LoopState:
    loop_id: str
    pid: Optional[int] = None
    running: bool = False
    started_at_epoch: Optional[float] = None
    last_exit_code: Optional[int] = None
    auto_restart: bool = False
    log_file: Optional[str] = None
    script_path: Optional[str] = None


# ============================================================
# FS / JSON
# ============================================================

def load_state() -> Dict[str, LoopState]:
    ensure_dir(RUNTIME_DIR)
    ensure_dir(LOG_DIR)

    if not STATE_FILE.exists():
        return {}

    try:
        raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

    out: Dict[str, LoopState] = {}
    for loop_id, payload in raw.items():
        try:
            out[loop_id] = LoopState(**payload)
        except Exception:
            out[loop_id] = LoopState(loop_id=loop_id)
    return out


def save_state(state: Dict[str, LoopState]) -> None:
    ensure_dir(RUNTIME_DIR)
    payload = {k: asdict(v) for k, v in state.items()}
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


# ============================================================
# PROCESS HELPERS
# ============================================================

def is_pid_running(pid: Optional[int]) -> bool:
    if pid is None or pid <= 0:
        return False

    try:
        if os.name == "nt":
            import ctypes

            kernel32 = ctypes.windll.kernel32
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
            if not handle:
                return False
            try:
                code = ctypes.c_ulong()
                kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
                STILL_ACTIVE = 259
                return code.value == STILL_ACTIVE
            finally:
                kernel32.CloseHandle(handle)
        else:
            os.kill(pid, 0)
            return True
    except Exception:
        return False


def build_log_file(loop_id: str) -> Path:
    ensure_dir(LOG_DIR)
    ts = time.strftime("%Y%m%d_%H%M%S")
    return LOG_DIR / f"{loop_id}_{ts}.log"


def start_loop_process(loop_cfg: Dict[str, object], existing_state: LoopState) -> LoopState:
    script_path = resolve_script_path(loop_cfg)

    if existing_state.pid and is_pid_running(existing_state.pid):
        raise RuntimeError(f"Loop läuft bereits mit PID {existing_state.pid}")

    env = os.environ.copy()
    env.update({str(k): str(v) for k, v in dict(loop_cfg.get("env", {})).items()})

    cwd = loop_cfg.get("cwd")
    cwd_path = Path(cwd) if cwd else script_path.parent

    log_file = build_log_file(str(loop_cfg["id"]))

    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

    log_handle = open(log_file, "a", encoding="utf-8", buffering=1)

    try:
        proc = subprocess.Popen(
            [sys.executable, str(script_path)],
            cwd=str(cwd_path),
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
        )
    finally:
        try:
            log_handle.close()
        except Exception:
            pass

    return LoopState(
        loop_id=str(loop_cfg["id"]),
        pid=int(proc.pid),
        running=True,
        started_at_epoch=time.time(),
        last_exit_code=None,
        auto_restart=bool(loop_cfg.get("auto_restart", False)),
        log_file=str(log_file),
        script_path=str(script_path),
    )


def stop_pid(pid: int, timeout_sec: float = 8.0) -> Optional[int]:
    if not is_pid_running(pid):
        return 0

    if os.name == "nt":
        try:
            os.kill(pid, signal.CTRL_BREAK_EVENT)
        except Exception:
            pass
    else:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass

    t0 = time.time()
    while time.time() - t0 < timeout_sec:
        if not is_pid_running(pid):
            return 0
        time.sleep(0.25)

    if os.name == "nt":
        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        except Exception:
            pass
    else:
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass

    time.sleep(0.5)
    return 0 if not is_pid_running(pid) else None


# ============================================================
# MANAGER
# ============================================================

class LoopManager:
    def __init__(self, loop_definitions: List[Dict[str, object]]):
        self.loop_definitions = {str(x["id"]): x for x in loop_definitions}
        self.state: Dict[str, LoopState] = load_state()
        self._bootstrap_missing_state()
        self.refresh_runtime_state(save=True)

    def _bootstrap_missing_state(self) -> None:
        for loop_id, cfg in self.loop_definitions.items():
            if loop_id not in self.state:
                script_path_str = ""
                try:
                    script_path_str = str(resolve_script_path(cfg))
                except Exception:
                    script_path_str = str(cfg.get("script_name", ""))

                self.state[loop_id] = LoopState(
                    loop_id=loop_id,
                    pid=None,
                    running=False,
                    started_at_epoch=None,
                    last_exit_code=None,
                    auto_restart=bool(cfg.get("auto_restart", False)),
                    script_path=script_path_str,
                )

    def refresh_runtime_state(self, save: bool = False) -> None:
        changed = False

        for loop_id, st in self.state.items():
            alive = is_pid_running(st.pid)

            if st.running and not alive:
                st.running = False
                st.pid = None
                if st.last_exit_code is None:
                    st.last_exit_code = 0
                changed = True

                if st.auto_restart:
                    try:
                        new_state = start_loop_process(self.loop_definitions[loop_id], st)
                        self.state[loop_id] = new_state
                        changed = True
                    except Exception as e:
                        print(f"[WARN] auto_restart failed for {loop_id}: {e}")

            elif alive and not st.running:
                st.running = True
                changed = True

        if save or changed:
            save_state(self.state)

    def start_loop(self, loop_id: str) -> None:
        cfg = self.loop_definitions[loop_id]
        new_state = start_loop_process(cfg, self.state[loop_id])
        self.state[loop_id] = new_state
        save_state(self.state)

    def stop_loop(self, loop_id: str) -> None:
        st = self.state[loop_id]
        if not st.pid:
            st.running = False
            save_state(self.state)
            return

        stop_pid(st.pid)
        st.running = False
        st.pid = None
        st.last_exit_code = 0
        save_state(self.state)

    def restart_loop(self, loop_id: str) -> None:
        self.stop_loop(loop_id)
        time.sleep(0.5)
        self.start_loop(loop_id)

    def start_all(self) -> None:
        for loop_id in self.loop_definitions:
            try:
                self.refresh_runtime_state(save=False)
                if not self.state[loop_id].running:
                    self.start_loop(loop_id)
            except Exception as e:
                print(f"[WARN] start_all {loop_id} failed: {e}")

    def stop_all(self) -> None:
        for loop_id in self.loop_definitions:
            try:
                if self.state[loop_id].running:
                    self.stop_loop(loop_id)
            except Exception as e:
                print(f"[WARN] stop_all {loop_id} failed: {e}")

    def set_auto_restart(self, loop_id: str, value: bool) -> None:
        self.state[loop_id].auto_restart = bool(value)
        save_state(self.state)

    def get_summary(self) -> Dict[str, int]:
        self.refresh_runtime_state(save=False)
        total = len(self.loop_definitions)
        running = sum(1 for st in self.state.values() if st.running)
        stopped = total - running
        return {"total": total, "running": running, "stopped": stopped}

    def get_rows(self) -> List[Dict[str, str]]:
        self.refresh_runtime_state(save=False)
        rows: List[Dict[str, str]] = []

        for loop_id, cfg in self.loop_definitions.items():
            st = self.state[loop_id]

            try:
                resolved_script = str(resolve_script_path(cfg))
            except Exception:
                resolved_script = str(st.script_path or cfg.get("script_name", ""))

            rows.append(
                {
                    "id": loop_id,
                    "label": str(cfg.get("label", loop_id)),
                    "status": "RUNNING" if st.running else "STOPPED",
                    "pid": str(st.pid or ""),
                    "started_at": format_epoch(st.started_at_epoch),
                    "uptime": format_uptime(st.started_at_epoch, st.running),
                    "auto_restart": "ON" if st.auto_restart else "OFF",
                    "script_path": resolved_script,
                    "log_file": str(st.log_file or ""),
                }
            )
        return rows


# ============================================================
# REUSABLE UI
# ============================================================

class Badge(tk.Frame):
    def __init__(self, parent, text: str, fg: str = FG_MAIN, bg: str = BG_BADGE):
        super().__init__(parent, bg=bg, bd=0, highlightthickness=0)
        self.label = tk.Label(
            self,
            text=text,
            font=FONT_BADGE,
            bg=bg,
            fg=fg,
            padx=10,
            pady=5,
        )
        self.label.pack()

    def set(self, text: str, fg: Optional[str] = None, bg: Optional[str] = None):
        self.label.configure(text=text)
        if fg is not None:
            self.label.configure(fg=fg)
        if bg is not None:
            self.configure(bg=bg)
            self.label.configure(bg=bg)


class FlatActionButton(tk.Frame):
    def __init__(
        self,
        parent,
        text: str,
        command,
        bg_normal: str,
        bg_hover: str,
        bg_pressed: str,
        fg: str,
        width: int = 90,
        height: int = 32,
    ):
        super().__init__(parent, bg=bg_normal, width=width, height=height, bd=0, highlightthickness=0)
        self.command = command
        self.bg_normal = bg_normal
        self.bg_hover = bg_hover
        self.bg_pressed = bg_pressed

        self.pack_propagate(False)

        self.label = tk.Label(
            self,
            text=text,
            bg=bg_normal,
            fg=fg,
            font=FONT_BUTTON,
            cursor="hand2",
        )
        self.label.pack(fill="both", expand=True)

        for w in (self, self.label):
            w.bind("<Enter>", self._on_enter)
            w.bind("<Leave>", self._on_leave)
            w.bind("<ButtonPress-1>", self._on_press)
            w.bind("<ButtonRelease-1>", self._on_release)

    def _set_bg(self, bg: str):
        self.configure(bg=bg)
        self.label.configure(bg=bg)

    def _on_enter(self, _event=None):
        self._set_bg(self.bg_hover)

    def _on_leave(self, _event=None):
        self._set_bg(self.bg_normal)

    def _on_press(self, _event=None):
        self._set_bg(self.bg_pressed)

    def _on_release(self, _event=None):
        self._set_bg(self.bg_hover)
        try:
            self.command()
        except Exception:
            pass


class PanelFrame(tk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent, bg=BG_SURFACE, highlightbackground=BORDER, highlightthickness=1)
        self.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)

        header = tk.Frame(self, bg=BG_TOP, height=38)
        header.grid(row=0, column=0, sticky="ew")
        header.grid_propagate(False)

        tk.Label(
            header,
            text=title,
            font=FONT_SECTION,
            bg=BG_TOP,
            fg=FG_WHITE,
        ).pack(side="left", padx=12)

        self.body = tk.Frame(self, bg=BG_SURFACE)
        self.body.grid(row=1, column=0, sticky="nsew")


# ============================================================
# EMBEDDED PANEL
# ============================================================

class LoopManagementPanel(tk.Frame):
    REFRESH_MS = 1500

    def __init__(
        self,
        parent,
        manager: Optional[LoopManager] = None,
        app=None,
        ftmo_root: Optional[Path] = None,
        root_path: Optional[Path] = None,
        **_kwargs,
    ):
        super().__init__(parent, bg=BG_APP)

        self.app = app
        self.ftmo_root = ftmo_root
        self.root_path = root_path

        self.manager = manager or LoopManager(LOOP_DEFINITIONS)
        self.selected_loop_id: Optional[str] = None
        self.compact_mode = tk.BooleanVar(value=True)
        self._refresh_job: Optional[str] = None

        self.top_info_var = tk.StringVar(value=shorten_path(str(FTMO_ROOT), 110))
        self.status_bar_var = tk.StringVar(value="Ready")

        self._configure_ttk()
        self._build_ui()
        self._refresh_table()
        self._schedule_refresh()

    # ========================================================
    # STYLE
    # ========================================================

    def _configure_ttk(self) -> None:
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure(
            "Treeview",
            background=BG_EDITOR,
            fieldbackground=BG_EDITOR,
            foreground=FG_MAIN,
            rowheight=22,
            borderwidth=0,
            relief="flat",
            font=FONT_LABEL,
        )
        style.map(
            "Treeview",
            background=[("selected", BG_CARD_HOVER)],
            foreground=[("selected", FG_WHITE)],
        )

        style.configure(
            "Treeview.Heading",
            background=BG_TOP,
            foreground=FG_WHITE,
            relief="flat",
            borderwidth=0,
            font=FONT_LABEL,
        )
        style.map(
            "Treeview.Heading",
            background=[("active", BG_TOP)],
            foreground=[("active", FG_WHITE)],
        )

        style.configure(
            "Vertical.TScrollbar",
            background=BG_TOP,
            troughcolor=BG_EDITOR,
            bordercolor=BG_EDITOR,
            arrowcolor=FG_MUTED,
            darkcolor=BG_TOP,
            lightcolor=BG_TOP,
        )
        style.configure(
            "Horizontal.TScrollbar",
            background=BG_TOP,
            troughcolor=BG_EDITOR,
            bordercolor=BG_EDITOR,
            arrowcolor=FG_MUTED,
            darkcolor=BG_TOP,
            lightcolor=BG_TOP,
        )

        style.configure(
            "TCheckbutton",
            background=BG_APP,
            foreground=FG_MAIN,
            font=FONT_LABEL,
        )
        style.map(
            "TCheckbutton",
            foreground=[("active", FG_WHITE)],
            background=[("active", BG_APP)],
        )

    # ========================================================
    # UI
    # ========================================================

    def _build_ui(self) -> None:
        self.rowconfigure(2, weight=1)
        self.columnconfigure(0, weight=1)

        topbar = tk.Frame(self, bg=BG_TOP, height=54, highlightbackground=BORDER, highlightthickness=1)
        topbar.grid(row=0, column=0, sticky="ew", padx=12, pady=(12, 10))
        topbar.grid_propagate(False)
        topbar.columnconfigure(1, weight=1)

        tk.Label(
            topbar,
            text="Loop Management",
            font=FONT_TITLE,
            bg=BG_TOP,
            fg=FG_WHITE,
        ).grid(row=0, column=0, sticky="w", padx=14)

        tk.Label(
            topbar,
            textvariable=self.top_info_var,
            font=FONT_TOP,
            bg=BG_TOP,
            fg=FG_MUTED,
        ).grid(row=0, column=1, sticky="e", padx=14)

        toolbar = tk.Frame(self, bg=BG_APP)
        toolbar.grid(row=1, column=0, sticky="ew", padx=12, pady=(0, 10))
        toolbar.columnconfigure(1, weight=1)

        left_info = tk.Frame(toolbar, bg=BG_APP)
        left_info.grid(row=0, column=0, sticky="w")

        tk.Label(
            left_info,
            text="Operations",
            font=FONT_LABEL,
            bg=BG_APP,
            fg=FG_MUTED,
        ).pack(side="left", padx=(0, 8))

        ttk.Checkbutton(
            left_info,
            text="Compact",
            variable=self.compact_mode,
            command=self._apply_compact_mode,
        ).pack(side="left")

        btns = tk.Frame(toolbar, bg=BG_APP)
        btns.grid(row=0, column=2, sticky="e")

        def pbtn(text, cmd, width=90):
            b = FlatActionButton(
                btns,
                text=text,
                command=cmd,
                bg_normal=BG_BUTTON,
                bg_hover=BG_BUTTON_HOVER,
                bg_pressed=BG_BUTTON_PRESSED,
                fg=FG_WHITE,
                width=width,
                height=32,
            )
            b.pack(side="left", padx=(0, 6))
            return b

        def sbtn(text, cmd, width=90):
            b = FlatActionButton(
                btns,
                text=text,
                command=cmd,
                bg_normal=BG_BUTTON_SECONDARY,
                bg_hover=BG_BUTTON_SECONDARY_HOVER,
                bg_pressed=BG_BUTTON_SECONDARY_PRESSED,
                fg=FG_MAIN,
                width=width,
                height=32,
            )
            b.pack(side="left", padx=(0, 6))
            return b

        sbtn("Refresh", self._refresh_table, 84)
        pbtn("Start All", self._start_all, 94)
        sbtn("Stop All", self._stop_all, 90)

        content = tk.PanedWindow(self, orient="vertical", sashwidth=6, bg=BG_APP, bd=0)
        content.grid(row=2, column=0, sticky="nsew", padx=12, pady=(0, 12))

        top = PanelFrame(content, "Loops")
        bottom = PanelFrame(content, "Details")

        content.add(top, minsize=340)
        content.add(bottom, minsize=200)

        self._build_top_panel(top.body)
        self._build_bottom_panel(bottom.body)

        footer = tk.Frame(self, bg=BG_STATUS, height=30, highlightbackground=BORDER, highlightthickness=1)
        footer.grid(row=3, column=0, sticky="ew", padx=12, pady=(0, 12))
        footer.grid_propagate(False)

        tk.Label(
            footer,
            textvariable=self.status_bar_var,
            font=FONT_LABEL,
            bg=BG_STATUS,
            fg=FG_MUTED,
        ).pack(side="left", padx=10)

    def _build_top_panel(self, parent: tk.Frame) -> None:
        parent.rowconfigure(2, weight=1)
        parent.columnconfigure(0, weight=1)

        kpi_row = tk.Frame(parent, bg=BG_SURFACE)
        kpi_row.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 8))
        kpi_row.columnconfigure(3, weight=1)

        self.badge_total = Badge(kpi_row, "0 Total", fg=FG_MAIN, bg=BG_BADGE)
        self.badge_total.grid(row=0, column=0, sticky="w", padx=(0, 8))

        self.badge_running = Badge(kpi_row, "0 Running", fg=FG_POS, bg=BG_BADGE_OPEN)
        self.badge_running.grid(row=0, column=1, sticky="w", padx=(0, 8))

        self.badge_stopped = Badge(kpi_row, "0 Stopped", fg=FG_WARN, bg=BG_BADGE_WARN)
        self.badge_stopped.grid(row=0, column=2, sticky="w", padx=(0, 8))

        tk.Label(
            kpi_row,
            text=shorten_path(str(FTMO_ROOT), 90),
            font=FONT_LABEL,
            bg=BG_SURFACE,
            fg=FG_SUBTLE,
            anchor="e",
        ).grid(row=0, column=3, sticky="e")

        divider = tk.Frame(parent, bg=DIVIDER, height=1)
        divider.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 8))

        shell = tk.Frame(parent, bg=BG_SURFACE)
        shell.grid(row=2, column=0, sticky="nsew", padx=10, pady=(0, 10))
        shell.rowconfigure(0, weight=1)
        shell.columnconfigure(0, weight=1)

        columns = ("label", "status", "pid", "uptime", "auto_restart")
        self.tree = ttk.Treeview(shell, columns=columns, show="headings")
        self.tree.grid(row=0, column=0, sticky="nsew")
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        specs = [
            ("label", 300, "loop"),
            ("status", 110, "status"),
            ("pid", 90, "pid"),
            ("uptime", 100, "uptime"),
            ("auto_restart", 120, "auto_restart"),
        ]
        for col, width, title in specs:
            self.tree.heading(col, text=title)
            self.tree.column(col, width=width, anchor="w", stretch=(col == "label"))

        yscroll = ttk.Scrollbar(shell, orient="vertical", command=self.tree.yview)
        yscroll.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=yscroll.set)

    def _build_bottom_panel(self, parent: tk.Frame) -> None:
        parent.rowconfigure(2, weight=1)
        parent.columnconfigure(0, weight=1)

        action_row = tk.Frame(parent, bg=BG_SURFACE)
        action_row.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 8))
        action_row.columnconfigure(2, weight=1)

        btns = tk.Frame(action_row, bg=BG_SURFACE)
        btns.grid(row=0, column=0, sticky="w")

        def pbtn(text, cmd, width=90):
            b = FlatActionButton(
                btns,
                text=text,
                command=cmd,
                bg_normal=BG_BUTTON,
                bg_hover=BG_BUTTON_HOVER,
                bg_pressed=BG_BUTTON_PRESSED,
                fg=FG_WHITE,
                width=width,
                height=32,
            )
            b.pack(side="left", padx=(0, 6))
            return b

        def sbtn(text, cmd, width=90):
            b = FlatActionButton(
                btns,
                text=text,
                command=cmd,
                bg_normal=BG_BUTTON_SECONDARY,
                bg_hover=BG_BUTTON_SECONDARY_HOVER,
                bg_pressed=BG_BUTTON_SECONDARY_PRESSED,
                fg=FG_MAIN,
                width=width,
                height=32,
            )
            b.pack(side="left", padx=(0, 6))
            return b

        pbtn("Start", self._start_selected, 78)
        sbtn("Stop", self._stop_selected, 78)
        sbtn("Restart", self._restart_selected, 90)
        sbtn("Logs", self._open_log_folder, 78)

        self.auto_restart_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            action_row,
            text="Auto Restart",
            variable=self.auto_restart_var,
            command=self._toggle_auto_restart_selected,
        ).grid(row=0, column=1, sticky="w", padx=(12, 0))

        divider = tk.Frame(parent, bg=DIVIDER, height=1)
        divider.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 8))

        shell = tk.Frame(parent, bg=BG_SURFACE)
        shell.grid(row=2, column=0, sticky="nsew", padx=10, pady=(0, 10))
        shell.rowconfigure(0, weight=1)
        shell.columnconfigure(0, weight=1)

        self.detail_text = tk.Text(
            shell,
            wrap="word",
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground=BORDER,
            highlightcolor=BORDER,
            font=FONT_MONO,
            bg=BG_EDITOR,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            padx=12,
            pady=12,
        )
        self.detail_text.grid(row=0, column=0, sticky="nsew")

        yscroll = ttk.Scrollbar(shell, orient="vertical", command=self.detail_text.yview)
        yscroll.grid(row=0, column=1, sticky="ns")
        self.detail_text.configure(yscrollcommand=yscroll.set)

    # ========================================================
    # UI STATE
    # ========================================================

    def _apply_compact_mode(self) -> None:
        style = ttk.Style()
        if self.compact_mode.get():
            style.configure("Treeview", rowheight=22, font=FONT_LABEL)
            style.configure("Treeview.Heading", font=FONT_LABEL, padding=(6, 5))
        else:
            style.configure("Treeview", rowheight=28, font=("Segoe UI", 10))
            style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"), padding=(8, 7))

    def _schedule_refresh(self) -> None:
        if self._refresh_job is not None:
            try:
                self.after_cancel(self._refresh_job)
            except Exception:
                pass
        self._refresh_job = self.after(self.REFRESH_MS, self._periodic_refresh)

    def _periodic_refresh(self) -> None:
        self._refresh_table()
        self._schedule_refresh()

    def _set_status(self, text: str) -> None:
        self.status_bar_var.set(text)
        self.top_info_var.set(shorten_path(str(FTMO_ROOT), 110))

    # ========================================================
    # DATA RENDER
    # ========================================================

    def _refresh_kpis(self) -> None:
        s = self.manager.get_summary()
        self.badge_total.set(f"{s['total']} Total", fg=FG_MAIN, bg=BG_BADGE)
        self.badge_running.set(f"{s['running']} Running", fg=FG_POS, bg=BG_BADGE_OPEN)
        self.badge_stopped.set(f"{s['stopped']} Stopped", fg=FG_WARN, bg=BG_BADGE_WARN)

    def _refresh_table(self) -> None:
        self._refresh_kpis()

        rows = self.manager.get_rows()
        current_selection = self.selected_loop_id
        self.tree.delete(*self.tree.get_children())

        for row in rows:
            status = row["status"]
            tags = ("running",) if status == "RUNNING" else ("stopped",)

            self.tree.insert(
                "",
                "end",
                iid=row["id"],
                values=(
                    row["label"],
                    row["status"],
                    row["pid"],
                    row["uptime"],
                    row["auto_restart"],
                ),
                tags=tags,
            )

        self.tree.tag_configure("running", foreground=FG_POS)
        self.tree.tag_configure("stopped", foreground=FG_WARN)

        if current_selection and current_selection in self.tree.get_children():
            self.tree.selection_set(current_selection)
            self.tree.focus(current_selection)
            self._render_details(current_selection)

        self._set_status("Refreshed")

    def _on_select(self, _event=None) -> None:
        selected = self.tree.selection()
        if not selected:
            self.selected_loop_id = None
            return
        self.selected_loop_id = selected[0]
        self._render_details(self.selected_loop_id)

    def _render_details(self, loop_id: str) -> None:
        cfg = self.manager.loop_definitions[loop_id]
        st = self.manager.state[loop_id]

        self.auto_restart_var.set(bool(st.auto_restart))

        try:
            resolved_script = resolve_script_path(cfg)
        except Exception:
            resolved_script = Path(str(st.script_path or cfg.get("script_name", "")))

        cwd = cfg.get("cwd", "") or resolved_script.parent

        lines = [
            f"LOOP_ID      : {loop_id}",
            f"LABEL        : {cfg.get('label', '')}",
            f"STATUS       : {'RUNNING' if st.running else 'STOPPED'}",
            f"PID          : {st.pid}",
            f"AUTO_RESTART : {st.auto_restart}",
            f"STARTED_AT   : {format_epoch(st.started_at_epoch)}",
            f"LAST_EXIT    : {st.last_exit_code}",
            "",
            f"SCRIPT_PATH  : {resolved_script}",
            f"CWD          : {cwd}",
            f"LOG_FILE     : {st.log_file or ''}",
            f"ENV          : {cfg.get('env', {})}",
        ]

        self.detail_text.delete("1.0", "end")
        self.detail_text.insert("1.0", "\n".join(lines))

    # ========================================================
    # ACTIONS
    # ========================================================

    def _run_action(self, action, success_msg: Optional[str] = None) -> None:
        def worker():
            try:
                action()
                self.after(0, self._refresh_table)
                if success_msg:
                    self.after(0, lambda: self._set_detail_message(success_msg))
            except Exception as e:
                err_msg = str(e)
                self.after(0, lambda msg=err_msg: messagebox.showerror("Fehler", msg))

        threading.Thread(target=worker, daemon=True).start()

    def _set_detail_message(self, msg: str) -> None:
        self.detail_text.delete("1.0", "end")
        self.detail_text.insert("1.0", msg)
        self._set_status(msg)

    def _require_selection(self) -> str:
        if not self.selected_loop_id:
            raise RuntimeError("Kein Loop ausgewählt")
        return self.selected_loop_id

    def _start_selected(self) -> None:
        loop_id = self._require_selection()
        self._run_action(lambda: self.manager.start_loop(loop_id), f"GESTARTET: {loop_id}")

    def _stop_selected(self) -> None:
        loop_id = self._require_selection()
        self._run_action(lambda: self.manager.stop_loop(loop_id), f"GESTOPPT: {loop_id}")

    def _restart_selected(self) -> None:
        loop_id = self._require_selection()
        self._run_action(lambda: self.manager.restart_loop(loop_id), f"NEU GESTARTET: {loop_id}")

    def _toggle_auto_restart_selected(self) -> None:
        loop_id = self._require_selection()
        value = self.auto_restart_var.get()
        self.manager.set_auto_restart(loop_id, value)
        self._refresh_table()
        self._render_details(loop_id)
        self._set_status(f"AUTO RESTART {'ON' if value else 'OFF'}: {loop_id}")

    def _start_all(self) -> None:
        self._run_action(self.manager.start_all, "ALLE LOOPS GESTARTET")

    def _stop_all(self) -> None:
        self._run_action(self.manager.stop_all, "ALLE LOOPS GESTOPPT")

    def _open_log_folder(self) -> None:
        ensure_dir(LOG_DIR)
        try:
            if sys.platform == "darwin":
                subprocess.Popen(["open", str(LOG_DIR)])
            elif os.name == "nt":
                os.startfile(str(LOG_DIR))  # type: ignore[attr-defined]
            else:
                subprocess.Popen(["xdg-open", str(LOG_DIR)])
            self._set_status("Log folder opened")
        except Exception as e:
            messagebox.showerror("Fehler", f"Log-Folder konnte nicht geöffnet werden: {e}")

    def destroy(self) -> None:
        if self._refresh_job is not None:
            try:
                self.after_cancel(self._refresh_job)
            except Exception:
                pass
            self._refresh_job = None
        super().destroy()


# ============================================================
# STANDALONE APP WRAPPER
# ============================================================

class LoopManagementBoardApp(tk.Tk):
    def __init__(self, manager: Optional[LoopManager] = None):
        super().__init__()
        self.title("Loop Management Board")
        self.geometry("1600x980")
        self.minsize(1200, 760)
        self.configure(bg=BG_APP)

        self.panel = LoopManagementPanel(self, manager=manager)
        self.panel.pack(fill="both", expand=True)

    def destroy(self) -> None:
        try:
            self.panel.destroy()
        except Exception:
            pass
        super().destroy()


# ============================================================
# MAIN
# ============================================================

def validate_loop_definitions() -> None:
    seen = set()
    for cfg in LOOP_DEFINITIONS:
        loop_id = str(cfg["id"])
        if loop_id in seen:
            raise RuntimeError(f"Doppelte loop id: {loop_id}")
        seen.add(loop_id)

        try:
            script_path = resolve_script_path(cfg)
            print(f"[OK] {loop_id} -> {script_path}")
        except Exception as e:
            print(f"[WARN] Script existiert noch nicht / nicht gefunden: {loop_id} | {e}")


def main() -> None:
    ensure_dir(RUNTIME_DIR)
    ensure_dir(LOG_DIR)
    validate_loop_definitions()

    manager = LoopManager(LOOP_DEFINITIONS)
    app = LoopManagementBoardApp(manager=manager)
    app.mainloop()


if __name__ == "__main__":
    main()