# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Dashboards/Main_Board/main_board.py

Cleaner Launcher Version
- Keine Sidebar
- Kein System Overview
- Kein Current Structure
- Fokus auf Header + Module Grid + Footer
- Eigene Flat-Buttons statt tk.Button
- Robusteres Modul-Laden
- Erweiterte Modulstruktur für Quant-Research / Analytics / Risk / Performance
"""

from __future__ import annotations

import importlib.util
import sys
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import messagebox
from typing import Callable, Dict, List, Optional, Tuple


# ============================================================
# PATHS
# ============================================================

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

DASHBOARDS_ROOT = FTMO_ROOT / "Dashboards"
MAIN_BOARD_DIR = DASHBOARDS_ROOT / "Main_Board"
PAGES_DIR = MAIN_BOARD_DIR / "pages"
RUNTIME_DIR = MAIN_BOARD_DIR / "runtime"

# Existing
STRATEGY_DASHBOARD_PATH = PAGES_DIR / "Strategy" / "strategy_dashboard.py"
LOOP_DASHBOARD_PATH = PAGES_DIR / "Loop_Management" / "loop_management_board.py"
MARKET_DASHBOARD_PATH = PAGES_DIR / "Market" / "market_watch_dashboard.py"
VISUAL_FOLDER_DASHBOARD_PATH = PAGES_DIR / "Visual_Folder" / "Visual_Folder.py"
CODE_STATION_DASHBOARD_PATH = PAGES_DIR / "Code_Station" / "Code_Station.py"
KNOWLEDGE_BOARD_PATH = PAGES_DIR / "Knowledge_Board" / "knowledge_board.py"

# New
STRATEGY_ANALYTICS_DASHBOARD_PATH = PAGES_DIR / "Strategy_Analytics" / "strategy_analytics_dashboard.py"
REGIME_DASHBOARD_PATH = PAGES_DIR / "Regime" / "Regime_dashboard.py"
ROBUSTNESS_DASHBOARD_PATH = PAGES_DIR / "Robustness" / "robustness_dashboard.py"
PORTFOLIO_DASHBOARD_PATH = PAGES_DIR / "Portfolio" / "portfolio_dashboard.py"
RISK_MODELING_DASHBOARD_PATH = PAGES_DIR / "Risk_Modeling" / "risk_modeling_dashboard.py"
SELECTION_DASHBOARD_PATH = PAGES_DIR / "Strategy_Selection" / "selection_dashboard.py"
PERFORMANCE_DASHBOARD_PATH = PAGES_DIR / "Performance" / "strategy_performance_dashboard.py"


# ============================================================
# THEME
# ============================================================

BG_APP = "#0A1118"
BG_TOP = "#0E1721"
BG_SURFACE = "#101B27"
BG_CARD = "#142131"
BG_CARD_HOVER = "#1A2A3D"
BG_BADGE = "#132130"
BG_BADGE_OPEN = "#10281D"
BG_BADGE_WARN = "#2C2110"
BG_BADGE_PLANNED = "#2A2115"

BG_BUTTON = "#2563EB"
BG_BUTTON_HOVER = "#3B82F6"
BG_BUTTON_PRESSED = "#1D4ED8"

BG_BUTTON_SECONDARY = "#223246"
BG_BUTTON_SECONDARY_HOVER = "#2C415A"
BG_BUTTON_SECONDARY_PRESSED = "#1C2B3C"

FG_MAIN = "#EAF2F9"
FG_MUTED = "#93A4B5"
FG_SUBTLE = "#708396"
FG_WHITE = "#FFFFFF"
FG_ACCENT = "#60A5FA"
FG_POS = "#22C55E"
FG_WARN = "#F59E0B"
FG_NEG = "#EF4444"
FG_PLANNED = "#D2AA62"

BORDER = "#223244"
DIVIDER = "#1B2A39"

FONT_TITLE = ("Segoe UI", 24, "bold")
FONT_SUBTITLE = ("Segoe UI", 10)
FONT_SECTION = ("Segoe UI", 11, "bold")
FONT_CARD_TITLE = ("Segoe UI", 11, "bold")
FONT_CARD_SUB = ("Segoe UI", 9)
FONT_TEXT = ("Segoe UI", 10)
FONT_LABEL = ("Segoe UI", 9)
FONT_SMALL = ("Segoe UI", 8)
FONT_BADGE = ("Segoe UI", 8, "bold")
FONT_BUTTON = ("Segoe UI", 9, "bold")


# ============================================================
# HELPERS
# ============================================================

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def shorten_path(s: str, max_len: int = 100) -> str:
    if len(s) <= max_len:
        return s
    return "..." + s[-(max_len - 3):]


def load_module_from_path(module_name: str, file_path: Path):
    if not file_path.exists():
        raise FileNotFoundError(f"Datei nicht gefunden: {file_path}")

    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Modul konnte nicht geladen werden: {file_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def make_divider(parent, pady=(0, 0)):
    tk.Frame(parent, bg=DIVIDER, height=1).pack(fill="x", pady=pady)


# ============================================================
# SPEC
# ============================================================

@dataclass
class ModuleSpec:
    key: str
    title: str
    subtitle: str
    short_desc: str
    icon: str
    path: Optional[Path]
    panel_class_name: Optional[str]
    module_name_for_import: Optional[str]
    geometry: str = "1500x900"
    minsize: Tuple[int, int] = (1100, 700)
    placeholder: bool = False
    alt_panel_class_names: Optional[List[str]] = None


# ============================================================
# UI BASICS
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


class SoftPanel(tk.Frame):
    def __init__(self, parent, bg=BG_SURFACE, padx=18, pady=18):
        super().__init__(parent, bg=bg, bd=0, highlightthickness=1, highlightbackground=BORDER)
        self.inner = tk.Frame(self, bg=bg)
        self.inner.pack(fill="both", expand=True, padx=padx, pady=pady)


class FlatActionButton(tk.Frame):
    def __init__(
        self,
        parent,
        text: str,
        command: Callable[[], None],
        bg_normal: str,
        bg_hover: str,
        bg_pressed: str,
        fg: str,
        width: int = 86,
        height: int = 34,
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


# ============================================================
# MODULE CARD
# ============================================================

class ModuleCard(tk.Frame):
    def __init__(
        self,
        parent,
        title: str,
        subtitle: str,
        short_desc: str,
        icon: str,
        open_command: Callable[[], None],
        status_getter: Callable[[], Tuple[str, str]],
        is_placeholder: bool = False,
    ):
        super().__init__(parent, bg=BG_CARD, bd=0, highlightthickness=1, highlightbackground=BORDER)

        self.open_command = open_command
        self.status_getter = status_getter
        self.is_placeholder = is_placeholder

        self.columnconfigure(0, weight=1)

        self.head = tk.Frame(self, bg=BG_CARD)
        self.head.grid(row=0, column=0, sticky="ew", padx=18, pady=(16, 10))
        self.head.columnconfigure(1, weight=1)

        self.icon_label = tk.Label(
            self.head,
            text=icon,
            font=("Segoe UI Symbol", 18),
            bg=BG_CARD,
            fg=FG_ACCENT if not is_placeholder else FG_PLANNED,
            width=2,
        )
        self.icon_label.grid(row=0, column=0, sticky="nw", padx=(0, 8))

        self.txt = tk.Frame(self.head, bg=BG_CARD)
        self.txt.grid(row=0, column=1, sticky="ew")

        self.title_label = tk.Label(
            self.txt,
            text=title,
            font=FONT_CARD_TITLE,
            bg=BG_CARD,
            fg=FG_WHITE,
            anchor="w",
        )
        self.title_label.pack(anchor="w")

        self.subtitle_label = tk.Label(
            self.txt,
            text=subtitle,
            font=FONT_CARD_SUB,
            bg=BG_CARD,
            fg=FG_SUBTLE,
            anchor="w",
        )
        self.subtitle_label.pack(anchor="w", pady=(2, 0))

        self.status_badge = Badge(self.head, "CLOSED", fg=FG_MUTED, bg=BG_BADGE)
        self.status_badge.grid(row=0, column=2, sticky="ne")

        self.desc_label = tk.Label(
            self,
            text=short_desc,
            font=FONT_LABEL,
            bg=BG_CARD,
            fg=FG_MUTED,
            justify="left",
            wraplength=280,
            anchor="w",
        )
        self.desc_label.grid(row=1, column=0, sticky="ew", padx=18, pady=(0, 16))

        self.btn_row = tk.Frame(self, bg=BG_CARD)
        self.btn_row.grid(row=2, column=0, sticky="w", padx=18, pady=(0, 16))

        if is_placeholder:
            self.open_btn = FlatActionButton(
                self.btn_row,
                text="Planned",
                command=self.open_command,
                bg_normal=BG_BUTTON_SECONDARY,
                bg_hover=BG_BUTTON_SECONDARY_HOVER,
                bg_pressed=BG_BUTTON_SECONDARY_PRESSED,
                fg=FG_MAIN,
                width=92,
                height=34,
            )
        else:
            self.open_btn = FlatActionButton(
                self.btn_row,
                text="Open",
                command=self.open_command,
                bg_normal=BG_BUTTON,
                bg_hover=BG_BUTTON_HOVER,
                bg_pressed=BG_BUTTON_PRESSED,
                fg=FG_WHITE,
                width=86,
                height=34,
            )

        self.open_btn.pack(side="left")

        for widget in (
            self,
            self.head,
            self.txt,
            self.icon_label,
            self.title_label,
            self.subtitle_label,
            self.desc_label,
        ):
            widget.bind("<Double-1>", lambda _e: self.open_command())
            widget.bind("<Enter>", self._on_enter)
            widget.bind("<Leave>", self._on_leave)

        self.refresh_status()

    def _on_enter(self, _event=None):
        self._set_card_bg(BG_CARD_HOVER)

    def _on_leave(self, _event=None):
        self._set_card_bg(BG_CARD)
        self.refresh_status()

    def _set_card_bg(self, bg: str):
        self.configure(bg=bg)
        self.head.configure(bg=bg)
        self.txt.configure(bg=bg)
        self.icon_label.configure(bg=bg)
        self.title_label.configure(bg=bg)
        self.subtitle_label.configure(bg=bg)
        self.desc_label.configure(bg=bg)
        self.btn_row.configure(bg=bg)

    def refresh_status(self):
        text, color = self.status_getter()

        badge_bg = BG_BADGE
        if text == "OPEN":
            badge_bg = BG_BADGE_OPEN
        elif text == "ERROR":
            badge_bg = BG_BADGE_WARN
        elif text == "PLANNED":
            badge_bg = BG_BADGE_PLANNED

        self.status_badge.set(text=text, fg=color, bg=badge_bg)


# ============================================================
# MODULE WINDOW
# ============================================================

class ModuleWindow(tk.Toplevel):
    def __init__(self, app: "FTMOMainBoard", spec: ModuleSpec):
        super().__init__(app)
        self.app = app
        self.spec = spec
        self.loaded_ok = False
        self.load_error: Optional[str] = None
        self.panel = None
        self.external_app = None

        self.title(spec.title)
        self.geometry(spec.geometry)
        self.minsize(*spec.minsize)
        self.configure(bg=BG_APP)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        self._build_shell()
        self._build_content()

    def _build_shell(self):
        top = tk.Frame(self, bg=BG_TOP, height=54, highlightthickness=1, highlightbackground=BORDER)
        top.pack(fill="x", padx=12, pady=(12, 10))
        top.pack_propagate(False)
        top.columnconfigure(1, weight=1)

        tk.Label(
            top,
            text=self.spec.title,
            font=("Segoe UI", 17, "bold"),
            bg=BG_TOP,
            fg=FG_WHITE,
        ).grid(row=0, column=0, sticky="w", padx=14)

        self.info_var = tk.StringVar(value="")
        tk.Label(
            top,
            textvariable=self.info_var,
            font=FONT_LABEL,
            bg=BG_TOP,
            fg=FG_MUTED,
        ).grid(row=0, column=1, sticky="e", padx=14)

        self.content = tk.Frame(self, bg=BG_APP)
        self.content.pack(fill="both", expand=True, padx=12, pady=(0, 12))

    def _build_placeholder(self, desc: str):
        body = SoftPanel(self.content, bg=BG_SURFACE, padx=22, pady=22)
        body.pack(fill="both", expand=True)

        tk.Label(
            body.inner,
            text=self.spec.title,
            font=FONT_SECTION,
            bg=BG_SURFACE,
            fg=FG_WHITE,
        ).pack(anchor="w")

        make_divider(body.inner, pady=(10, 14))

        tk.Label(
            body.inner,
            text=desc,
            font=FONT_TEXT,
            bg=BG_SURFACE,
            fg=FG_MUTED,
            justify="left",
            wraplength=1100,
        ).pack(anchor="w")

    def _build_error(self, err: str):
        body = SoftPanel(self.content, bg=BG_SURFACE, padx=22, pady=22)
        body.pack(fill="both", expand=True)

        tk.Label(
            body.inner,
            text="Module could not be loaded",
            font=FONT_SECTION,
            bg=BG_SURFACE,
            fg=FG_WHITE,
        ).pack(anchor="w")

        make_divider(body.inner, pady=(10, 14))

        txt = tk.Text(
            body.inner,
            wrap="word",
            bg=BG_SURFACE,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            bd=0,
            highlightthickness=0,
            font=FONT_TEXT,
        )
        txt.pack(fill="both", expand=True)
        txt.insert(
            "1.0",
            "\n".join([
                f"Title: {self.spec.title}",
                f"Path: {self.spec.path}",
                "",
                "Error:",
                err,
            ])
        )
        txt.configure(state="disabled")

    def _resolve_panel_class(self, module):
        names_to_try: List[str] = []

        if self.spec.panel_class_name:
            names_to_try.append(self.spec.panel_class_name)

        if self.spec.alt_panel_class_names:
            for n in self.spec.alt_panel_class_names:
                if n not in names_to_try:
                    names_to_try.append(n)

        for n in names_to_try:
            if hasattr(module, n):
                return getattr(module, n), n

        return None, None

    def _instantiate_panel(self, module, host):
        panel_cls, found_name = self._resolve_panel_class(module)

        # Strategy special case
        if panel_cls is not None and found_name == "StrategyDashboardPanel":
            repo_cls = getattr(module, "StrategyProfileRepository", None)
            root_path = getattr(module, "STRATEGY_PROFILE_ROOT", None)
            if repo_cls is not None and root_path is not None:
                repo = repo_cls(root_path)
                return panel_cls(host, repo=repo)
            return panel_cls(host)

        # Performance special case: class is a tk.Tk app, not a Frame panel
        if panel_cls is not None and found_name == "StrategyPerformanceDashboard":
            repo_cls = getattr(module, "StrategyDataRepository", None)
            live_root = getattr(module, "LIVE_PERF_ROOT", None)
            if repo_cls is None or live_root is None:
                raise RuntimeError("Performance dashboard requires StrategyDataRepository and LIVE_PERF_ROOT")
            repo = repo_cls(live_root)

            # embed wrapper
            wrapper = tk.Frame(host, bg=BG_APP)
            wrapper.pack(fill="both", expand=True)

            info = tk.Text(
                wrapper,
                wrap="word",
                bg=BG_SURFACE,
                fg=FG_MAIN,
                insertbackground=FG_MAIN,
                relief="flat",
                bd=0,
                highlightthickness=1,
                font=FONT_TEXT,
            )
            info.pack(fill="both", expand=True, padx=12, pady=12)
            info.insert(
                "1.0",
                "\n".join([
                    "Das Performance-Dashboard ist als eigenständige tk.Tk-App gebaut.",
                    "Es kann nicht direkt als Frame in ein Toplevel eingebettet werden.",
                    "",
                    "Die Moduldatei wurde korrekt gefunden.",
                    f"Root: {live_root}",
                    "",
                    "Empfehlung:",
                    "- Entweder die Klasse auf tk.Frame umbauen",
                    "- oder dieses Modul separat starten",
                ])
            )
            info.configure(state="disabled")

            # We still instantiate repo so module validation is real
            self.external_app = {"repo": repo}
            return wrapper

        if panel_cls is not None:
            attempts = [
                lambda: panel_cls(host),
                lambda: panel_cls(host, app=self.app),
                lambda: panel_cls(host, ftmo_root=FTMO_ROOT),
                lambda: panel_cls(host, root_path=FTMO_ROOT),
            ]
            last_err = None
            for a in attempts:
                try:
                    return a()
                except Exception as e:
                    last_err = e
            raise RuntimeError(f"Konstruktion fehlgeschlagen: {last_err}")

        if hasattr(module, "build_panel") and callable(module.build_panel):
            attempts = [
                lambda: module.build_panel(host),
                lambda: module.build_panel(host, app=self.app),
                lambda: module.build_panel(host, ftmo_root=FTMO_ROOT),
            ]
            last_err = None
            for a in attempts:
                try:
                    return a()
                except Exception as e:
                    last_err = e
            raise RuntimeError(f"build_panel(...) fehlgeschlagen: {last_err}")

        raise AttributeError(
            f"Keine passende Panel-Klasse gefunden. Erwartet: "
            f"{self.spec.panel_class_name} oder {self.spec.alt_panel_class_names or []}"
        )

    def _build_content(self):
        for child in self.content.winfo_children():
            child.destroy()

        if self.spec.placeholder:
            self.loaded_ok = True
            self.info_var.set("Planned module")
            self._build_placeholder(self.spec.short_desc)
            return

        try:
            assert self.spec.path is not None
            assert self.spec.module_name_for_import is not None

            module = load_module_from_path(self.spec.module_name_for_import, self.spec.path)

            host = tk.Frame(self.content, bg=BG_APP)
            host.pack(fill="both", expand=True)

            self.panel = self._instantiate_panel(module, host)

            if isinstance(self.panel, tk.Widget) and self.panel.master is host and not self.panel.winfo_manager():
                self.panel.pack(fill="both", expand=True)

            self.loaded_ok = True
            self.info_var.set(shorten_path(str(self.spec.path), 110))

        except Exception as e:
            self.loaded_ok = False
            self.load_error = str(e)
            self.info_var.set("Load error")
            self._build_error(str(e))

    def _on_close(self):
        try:
            if self.panel is not None and hasattr(self.panel, "destroy"):
                self.panel.destroy()
        except Exception:
            pass
        self.app.unregister_window(self.spec.key)
        self.destroy()


# ============================================================
# HOME
# ============================================================

class HomePage(tk.Frame):
    def __init__(self, parent, app: "FTMOMainBoard"):
        super().__init__(parent, bg=BG_APP)
        self.app = app
        self.cards: Dict[str, ModuleCard] = {}
        self.footer_var = tk.StringVar(value="-")
        self._build_ui()

    def _build_ui(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        header = tk.Frame(self, bg=BG_APP)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 22))
        header.columnconfigure(0, weight=1)

        left = tk.Frame(header, bg=BG_APP)
        left.grid(row=0, column=0, sticky="w")

        tk.Label(
            left,
            text="FTMO Main Board",
            font=FONT_TITLE,
            bg=BG_APP,
            fg=FG_WHITE,
        ).pack(anchor="w")

        tk.Label(
            left,
            text="Launcher / Control Hub",
            font=FONT_SUBTITLE,
            bg=BG_APP,
            fg=FG_SUBTLE,
        ).pack(anchor="w", pady=(4, 0))

        right = tk.Frame(header, bg=BG_APP)
        right.grid(row=0, column=1, sticky="e")

        self.badge_home = Badge(right, "Home Ready", fg=FG_ACCENT, bg=BG_BADGE)
        self.badge_home.pack(side="left", padx=(0, 8))

        self.badge_modules = Badge(right, "0 Modules", fg=FG_MAIN, bg=BG_BADGE)
        self.badge_modules.pack(side="left", padx=(0, 8))

        self.badge_open = Badge(right, "0 Open", fg=FG_MAIN, bg=BG_BADGE)
        self.badge_open.pack(side="left")

        main = SoftPanel(self, bg=BG_SURFACE, padx=20, pady=20)
        main.grid(row=1, column=0, sticky="nsew")

        top_row = tk.Frame(main.inner, bg=BG_SURFACE)
        top_row.pack(fill="x")

        tk.Label(
            top_row,
            text="Modules",
            font=FONT_SECTION,
            bg=BG_SURFACE,
            fg=FG_WHITE,
        ).pack(side="left")

        tk.Label(
            top_row,
            text="Open a module to launch a separate workspace window",
            font=FONT_LABEL,
            bg=BG_SURFACE,
            fg=FG_SUBTLE,
        ).pack(side="right")

        make_divider(main.inner, pady=(12, 18))

        grid = tk.Frame(main.inner, bg=BG_SURFACE)
        grid.pack(fill="both", expand=True)

        for c in range(3):
            grid.columnconfigure(c, weight=1)

        ordered_keys = [
            "strategy",
            "strategy_analytics",
            "selection",

            "market",
            "regime",
            "robustness",

            "portfolio",
            "risk_modeling",
            "performance",

            "loop_management",
            "visual_folder",
            "code_station",

            "knowledge_board",
            "spread",
        ]

        for idx, key in enumerate(ordered_keys):
            spec = self.app.module_specs[key]
            card = ModuleCard(
                grid,
                title=spec.title,
                subtitle=spec.subtitle,
                short_desc=spec.short_desc,
                icon=spec.icon,
                open_command=lambda k=key: self.app.open_module(k),
                status_getter=lambda k=key: self.app.get_module_status(k),
                is_placeholder=spec.placeholder,
            )
            r, c = divmod(idx, 3)
            card.grid(row=r, column=c, sticky="nsew", padx=8, pady=8)
            self.cards[key] = card

        footer = tk.Frame(self, bg=BG_APP, height=34)
        footer.grid(row=2, column=0, sticky="ew", pady=(14, 0))
        footer.pack_propagate(False)

        tk.Label(
            footer,
            textvariable=self.footer_var,
            font=FONT_LABEL,
            bg=BG_APP,
            fg=FG_SUBTLE,
            anchor="w",
        ).pack(side="left")

    def refresh(self):
        open_count = sum(1 for w in self.app.module_windows.values() if w.winfo_exists())
        total_count = len(self.app.module_specs)

        self.badge_modules.set(f"{total_count} Modules", fg=FG_MAIN, bg=BG_BADGE)
        self.badge_open.set(f"{open_count} Open", fg=FG_MAIN, bg=BG_BADGE)

        self.footer_var.set(
            f"Root: {shorten_path(str(FTMO_ROOT), 80)}   ·   Refresh: {self.app.REFRESH_MS // 1000}s   ·   Open Windows: {open_count}"
        )

        for card in self.cards.values():
            card.refresh_status()


# ============================================================
# APP
# ============================================================

class FTMOMainBoard(tk.Tk):
    REFRESH_MS = 5000

    def __init__(self):
        super().__init__()

        ensure_dir(RUNTIME_DIR)

        self.title("FTMO Main Board")
        self.geometry("1560x920")
        self.minsize(1280, 780)
        self.configure(bg=BG_APP)

        self.module_windows: Dict[str, ModuleWindow] = {}
        self.module_specs: Dict[str, ModuleSpec] = self._build_module_specs()

        self._build_ui()
        self.home_page.refresh()
        self.after(self.REFRESH_MS, self._tick)

    def _build_module_specs(self) -> Dict[str, ModuleSpec]:
        return {
            "strategy": ModuleSpec(
                key="strategy",
                title="Strategy",
                subtitle="Research / Profile / Selection",
                short_desc="Profiles, setup and selection.",
                icon="◫",
                path=STRATEGY_DASHBOARD_PATH,
                panel_class_name="StrategyDashboardPanel",
                module_name_for_import="ftmo_strategy_dashboard_module",
                geometry="1860x1080",
                minsize=(1400, 900),
            ),
            "strategy_analytics": ModuleSpec(
                key="strategy_analytics",
                title="Strategy Analytics",
                subtitle="Equity / Metrics / Diagnostics",
                short_desc="Detailed strategy analytics and equity curves.",
                icon="◨",
                path=STRATEGY_ANALYTICS_DASHBOARD_PATH,
                panel_class_name="StrategyAnalyticsDashboardPanel",
                module_name_for_import="ftmo_strategy_analytics_module",
                geometry="1860x1080",
                minsize=(1400, 900),
            ),
            "selection": ModuleSpec(
                key="selection",
                title="Selection",
                subtitle="Ranking / Approval",
                short_desc="Final strategy selection and deployment ranking.",
                icon="✓",
                path=SELECTION_DASHBOARD_PATH,
                panel_class_name="SelectionDashboardPanel",
                module_name_for_import="ftmo_selection_dashboard_module",
                geometry="1800x1000",
                minsize=(1400, 850),
            ),
            "market": ModuleSpec(
                key="market",
                title="Market",
                subtitle="Watch / Session Moves",
                short_desc="Market monitor and quick access.",
                icon="◌",
                path=MARKET_DASHBOARD_PATH,
                panel_class_name="MarketWatchPanel",
                module_name_for_import="ftmo_market_watch_module",
                geometry="1540x940",
                minsize=(1200, 760),
            ),
            "regime": ModuleSpec(
                key="regime",
                title="Regime",
                subtitle="Market State Analysis",
                short_desc="Regime detection and dependency analysis.",
                icon="◔",
                path=REGIME_DASHBOARD_PATH,
                panel_class_name="RegimeDashboardPanel",
                module_name_for_import="ftmo_regime_dashboard_module",
                geometry="1900x1080",
                minsize=(1450, 900),
            ),
            "robustness": ModuleSpec(
                key="robustness",
                title="Robustness",
                subtitle="Monte Carlo / Stress Tests",
                short_desc="Monte Carlo, shuffle, bootstrap and noise robustness.",
                icon="◈",
                path=ROBUSTNESS_DASHBOARD_PATH,
                panel_class_name="RobustnessDashboardPanel",
                module_name_for_import="ftmo_robustness_dashboard_module",
                geometry="1800x1000",
                minsize=(1400, 850),
            ),
            "portfolio": ModuleSpec(
                key="portfolio",
                title="Portfolio",
                subtitle="Allocation / Correlation",
                short_desc="Portfolio construction, diversification and fit.",
                icon="◍",
                path=PORTFOLIO_DASHBOARD_PATH,
                panel_class_name="PortfolioDashboardPanel",
                module_name_for_import="ftmo_portfolio_dashboard_module",
                geometry="1900x1080",
                minsize=(1450, 900),
            ),
            "risk_modeling": ModuleSpec(
                key="risk_modeling",
                title="Risk Modeling",
                subtitle="Sizing / Risk Engine",
                short_desc="Risk budgets, lot sizing and capital constraints.",
                icon="⚠",
                path=RISK_MODELING_DASHBOARD_PATH,
                panel_class_name="RiskModelingDashboardPanel",
                module_name_for_import="ftmo_risk_modeling_dashboard_module",
                geometry="1900x1080",
                minsize=(1450, 900),
            ),
            "performance": ModuleSpec(
                key="performance",
                title="Performance",
                subtitle="Live Performance / Calendar",
                short_desc="Monthly calendar, day insights, equity curve and live account performance.",
                icon="◴",
                path=PERFORMANCE_DASHBOARD_PATH,
                panel_class_name="StrategyPerformanceDashboard",
                module_name_for_import="ftmo_performance_dashboard_module",
                geometry="1720x980",
                minsize=(1350, 820),
            ),
            "loop_management": ModuleSpec(
                key="loop_management",
                title="Loop Management",
                subtitle="Runtime / Monitoring",
                short_desc="Loops, jobs and controls.",
                icon="↻",
                path=LOOP_DASHBOARD_PATH,
                panel_class_name="LoopManagementPanel",
                module_name_for_import="ftmo_loop_management_module",
                geometry="1600x980",
                minsize=(1200, 800),
            ),
            "visual_folder": ModuleSpec(
                key="visual_folder",
                title="Visual Folder",
                subtitle="Structure / Export",
                short_desc="Folder explorer and export.",
                icon="☷",
                path=VISUAL_FOLDER_DASHBOARD_PATH,
                panel_class_name="FolderExplorerPanel",
                module_name_for_import="ftmo_visual_folder_module",
                geometry="1650x960",
                minsize=(1250, 760),
            ),
            "code_station": ModuleSpec(
                key="code_station",
                title="Code Station",
                subtitle="Editor / Run / Console",
                short_desc="Internal editing workspace.",
                icon="⌘",
                path=CODE_STATION_DASHBOARD_PATH,
                panel_class_name="CodeStationPanel",
                module_name_for_import="ftmo_code_station_module",
                geometry="1820x1050",
                minsize=(1350, 860),
            ),
            "knowledge_board": ModuleSpec(
                key="knowledge_board",
                title="Knowledge Board",
                subtitle="Docs / Planning / Notes",
                short_desc="Internal documentation hub.",
                icon="☰",
                path=KNOWLEDGE_BOARD_PATH,
                panel_class_name="KnowledgeBoardPanel",
                module_name_for_import="ftmo_knowledge_board_module",
                geometry="1700x980",
                minsize=(1300, 820),
                alt_panel_class_names=[
                    "KnowledgeBoardPanel",
                    "KnowledgeBoard",
                    "KnowledgePanel",
                    "KnowledgeBoardFrame",
                    "MainPanel",
                    "MainFrame",
                ],
            ),
            "spread": ModuleSpec(
                key="spread",
                title="Spread",
                subtitle="Planned",
                short_desc="Spread and broker analytics.",
                icon="⇄",
                path=None,
                panel_class_name=None,
                module_name_for_import=None,
                geometry="1200x760",
                minsize=(900, 600),
                placeholder=True,
            ),
        }

    def _build_ui(self):
        root = tk.Frame(self, bg=BG_APP)
        root.pack(fill="both", expand=True)

        self.home_page = HomePage(root, self)
        self.home_page.pack(fill="both", expand=True, padx=22, pady=22)

    def get_module_status(self, key: str) -> Tuple[str, str]:
        spec = self.module_specs[key]

        if spec.placeholder:
            return ("PLANNED", FG_PLANNED)

        win = self.module_windows.get(key)
        if win is None or not win.winfo_exists():
            return ("CLOSED", FG_MUTED)
        if win.loaded_ok:
            return ("OPEN", FG_POS)
        return ("ERROR", FG_WARN)

    def unregister_window(self, key: str):
        self.module_windows.pop(key, None)
        self.home_page.refresh()

    def open_module(self, key: str):
        spec = self.module_specs[key]

        existing = self.module_windows.get(key)
        if existing is not None and existing.winfo_exists():
            try:
                existing.deiconify()
                existing.lift()
                existing.focus_force()
            except Exception:
                pass
            self.home_page.refresh()
            return

        try:
            win = ModuleWindow(self, spec)
            self.module_windows[key] = win
            self.home_page.refresh()
        except Exception as e:
            messagebox.showerror("Modulfehler", f"{spec.title} konnte nicht geöffnet werden.\n\n{e}")

    def _tick(self):
        try:
            dead_keys = []
            for key, win in self.module_windows.items():
                if not win.winfo_exists():
                    dead_keys.append(key)

            for key in dead_keys:
                self.module_windows.pop(key, None)

            self.home_page.refresh()
        finally:
            self.after(self.REFRESH_MS, self._tick)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    app = FTMOMainBoard()
    app.mainloop()


if __name__ == "__main__":
    main()