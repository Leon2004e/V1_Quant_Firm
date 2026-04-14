# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Dashboards/Main_Board/pages/Code_Station/Code_Station.py

Zweck:
- Interne Code Station für das FTMO Main Board
- VS-Code-artiger Aufbau
- Links: Explorer
- Mitte: Editor mit Line Numbers + Syntax Highlighting
- Rechts: File Info + Code Index
- Unten: Console / Run Output
- Einheitlicher Stil passend zur Home Page
- CSV/Tabellen-Dateien werden spaltenweise farblich markiert

Einbettung im Main Board:
    panel_class_name="CodeStationPanel"

Standalone:
    python Quant_Structure/FTMO/Dashboards/Main_Board/pages/Code_Station/Code_Station.py
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, List, Tuple
import builtins
import csv
import io
import keyword
import re
import shutil
import subprocess
import sys
import threading
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog


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
DEFAULT_WORKSPACE_ROOT = FTMO_ROOT


# ============================================================
# THEME
# ============================================================

BG_APP = "#0A1118"
BG_TOP = "#0E1721"
BG_SURFACE = "#101B27"
BG_CARD = "#142131"
BG_CARD_HOVER = "#1A2A3D"

BG_EDITOR = "#0F1722"
BG_EDITOR_2 = "#111C29"
BG_CONSOLE = "#0D1621"
BG_INPUT = "#0F1A26"
BG_STATUS = "#0C141D"

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

# syntax
FG_SYNTAX_KEYWORD = "#C792EA"
FG_SYNTAX_STRING = "#ECC48D"
FG_SYNTAX_COMMENT = "#5C6B7A"
FG_SYNTAX_NUMBER = "#F78C6C"
FG_SYNTAX_DEFCLASS = "#82AAFF"
FG_SYNTAX_BUILTIN = "#4FC1FF"
FG_SYNTAX_OPERATOR = "#89DDFF"
FG_SYNTAX_DECORATOR = "#FFCB6B"
FG_SYNTAX_JSON_KEY = "#7FDBCA"
FG_SYNTAX_BOOL = "#FF5370"
FG_SYNTAX_TAG = "#F07178"
FG_SYNTAX_ATTR = "#C3E88D"
FG_SYNTAX_HEADING = "#82AAFF"
FG_SYNTAX_MD_EM = "#C792EA"

FG_LINE_NUM = "#4F6070"

CSV_COLORS = [
    "#EAF2F9",
    "#7FDBCA",
    "#C3E88D",
    "#ECC48D",
    "#C792EA",
    "#82AAFF",
    "#F78C6C",
    "#89DDFF",
]

BORDER = "#223244"
DIVIDER = "#1B2A39"

FONT_TITLE = ("Segoe UI", 18, "bold")
FONT_TOP = ("Segoe UI", 10)
FONT_SECTION = ("Segoe UI", 11, "bold")
FONT_LABEL = ("Segoe UI", 9)
FONT_TEXT = ("Segoe UI", 10)
FONT_SMALL = ("Segoe UI", 8)
FONT_BUTTON = ("Segoe UI", 9, "bold")
FONT_MONO = ("Consolas", 10)


# ============================================================
# HELPERS
# ============================================================

TEXT_FILE_EXTENSIONS = {
    ".py", ".txt", ".md", ".json", ".yaml", ".yml", ".csv", ".ini", ".cfg",
    ".toml", ".sql", ".js", ".ts", ".html", ".css", ".xml", ".log", ".env",
    ".mq5", ".mqh", ".bat", ".ps1", ".r", ".cpp", ".c", ".h", ".java"
}

CODE_INDEX_EXTENSIONS = {
    ".py", ".mq5", ".mqh", ".json", ".yaml", ".yml", ".sql", ".js", ".ts",
    ".html", ".css", ".xml", ".ini", ".cfg", ".toml", ".txt", ".md", ".csv"
}

PY_BUILTINS = set(dir(builtins))


def is_text_file(path: Path) -> bool:
    return path.suffix.lower() in TEXT_FILE_EXTENSIONS or path.name.lower() in {
        "dockerfile", ".gitignore", ".env"
    }


def is_code_index_file(path: Path) -> bool:
    return path.is_file() and (
        path.suffix.lower() in CODE_INDEX_EXTENSIONS
        or path.name.lower() in {"dockerfile", ".gitignore", ".env"}
    )


def safe_rel(path: Path, root: Path) -> str:
    try:
        rel = path.resolve().relative_to(root.resolve())
        s = rel.as_posix()
        return s if s else "."
    except Exception:
        return "."


def make_iid(rel_path: str) -> str:
    return rel_path if rel_path else "."


def within_root(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


def list_dir(path: Path, ignore_hidden: bool = True):
    entries = list(path.iterdir())
    if ignore_hidden:
        entries = [e for e in entries if not e.name.startswith(".")]
    entries.sort(key=lambda p: (not p.is_dir(), p.name.lower()))
    return entries


# ============================================================
# REUSABLE UI
# ============================================================

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
        width: int = 92,
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


class LineNumberCanvas(tk.Canvas):
    def __init__(self, parent, text_widget: tk.Text):
        super().__init__(
            parent,
            width=52,
            bg=BG_EDITOR_2,
            highlightthickness=0,
            bd=0,
        )
        self.text_widget = text_widget

    def redraw(self):
        self.delete("all")
        i = self.text_widget.index("@0,0")
        while True:
            dline = self.text_widget.dlineinfo(i)
            if dline is None:
                break
            y = dline[1]
            line_number = i.split(".")[0]
            self.create_text(
                42,
                y + 2,
                anchor="ne",
                text=line_number,
                fill=FG_LINE_NUM,
                font=FONT_MONO,
            )
            i = self.text_widget.index(f"{i}+1line")


# ============================================================
# PANEL
# ============================================================

class CodeStationPanel(tk.Frame):
    HIGHLIGHT_DELAY_MS = 120

    def __init__(self, parent):
        super().__init__(parent, bg=BG_APP)

        self.workspace_root = DEFAULT_WORKSPACE_ROOT.resolve()
        self.current_file: Optional[Path] = None
        self.editor_dirty = False
        self.ignore_hidden = tk.BooleanVar(value=True)

        self.process: Optional[subprocess.Popen] = None
        self.process_thread: Optional[threading.Thread] = None

        self.code_index_files: List[Path] = []
        self._highlight_job = None

        self._configure_ttk()
        self._build_ui()
        self._configure_editor_tags()
        self._load_tree()
        self._rebuild_code_index()
        self._update_status("Ready")

    # ========================================================
    # STYLE
    # ========================================================

    def _configure_ttk(self):
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
            rowheight=24,
            borderwidth=0,
            relief="flat",
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

    # ========================================================
    # UI
    # ========================================================

    def _build_ui(self):
        self.rowconfigure(2, weight=1)
        self.columnconfigure(0, weight=1)

        topbar = tk.Frame(self, bg=BG_TOP, height=54, highlightbackground=BORDER, highlightthickness=1)
        topbar.grid(row=0, column=0, sticky="ew", padx=12, pady=(12, 10))
        topbar.grid_propagate(False)
        topbar.columnconfigure(1, weight=1)

        tk.Label(
            topbar,
            text="Code Station",
            font=FONT_TITLE,
            bg=BG_TOP,
            fg=FG_WHITE,
        ).grid(row=0, column=0, sticky="w", padx=14)

        self.top_info_var = tk.StringVar(value=str(self.workspace_root))
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

        tk.Label(
            toolbar,
            text="Workspace",
            font=FONT_LABEL,
            bg=BG_APP,
            fg=FG_MUTED,
        ).grid(row=0, column=0, sticky="w", padx=(0, 8))

        self.workspace_var = tk.StringVar(value=str(self.workspace_root))
        self.workspace_entry = tk.Entry(
            toolbar,
            textvariable=self.workspace_var,
            bg=BG_INPUT,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground=BORDER,
            highlightcolor=BORDER,
        )
        self.workspace_entry.grid(row=0, column=1, sticky="ew", padx=(0, 10), ipady=7)

        btns = tk.Frame(toolbar, bg=BG_APP)
        btns.grid(row=0, column=2, sticky="e")

        def pbtn(text, cmd, width=92):
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

        def sbtn(text, cmd, width=96):
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

        sbtn("Reload", self._reload_workspace, 84)
        pbtn("Save", self._save_current_file, 78)
        pbtn("Run", self._run_current_file, 78)
        sbtn("Stop", self._stop_process, 78)
        sbtn("New File", self._new_file, 88)
        sbtn("New Folder", self._new_folder, 100)
        sbtn("Delete", self._delete_selected, 82)
        sbtn("Copy Path", self._copy_current_path, 96)
        sbtn("Refresh Index", self._rebuild_code_index, 112)
        sbtn("Clear Console", self._clear_console, 112)

        self.hidden_btn = FlatActionButton(
            btns,
            text="Hidden: OFF",
            command=self._toggle_hidden,
            bg_normal=BG_BUTTON_SECONDARY,
            bg_hover=BG_BUTTON_SECONDARY_HOVER,
            bg_pressed=BG_BUTTON_SECONDARY_PRESSED,
            fg=FG_MAIN,
            width=110,
            height=32,
        )
        self.hidden_btn.pack(side="left")

        vertical = tk.PanedWindow(self, orient="vertical", sashwidth=6, bg=BG_APP, bd=0)
        vertical.grid(row=2, column=0, sticky="nsew", padx=12, pady=(0, 12))

        top_content = tk.PanedWindow(vertical, orient="horizontal", sashwidth=6, bg=BG_APP, bd=0)
        vertical.add(top_content, minsize=500)

        left = PanelFrame(top_content, "Explorer")
        center = PanelFrame(top_content, "Editor")
        right = PanelFrame(top_content, "Inspector")

        top_content.add(left, minsize=300)
        top_content.add(center, minsize=760)
        top_content.add(right, minsize=360)

        console_frame = PanelFrame(vertical, "Console")
        vertical.add(console_frame, minsize=180)

        self._build_tree_side(left.body)
        self._build_editor_side(center.body)
        self._build_right_side(right.body)
        self._build_console_side(console_frame.body)

        footer = tk.Frame(self, bg=BG_STATUS, height=30, highlightbackground=BORDER, highlightthickness=1)
        footer.grid(row=3, column=0, sticky="ew", padx=12, pady=(0, 12))
        footer.grid_propagate(False)

        self.status_var = tk.StringVar(value="Ready")
        tk.Label(
            footer,
            textvariable=self.status_var,
            font=FONT_LABEL,
            bg=BG_STATUS,
            fg=FG_MUTED,
        ).pack(side="left", padx=10)

    def _build_tree_side(self, parent: tk.Frame):
        parent.rowconfigure(1, weight=1)
        parent.columnconfigure(0, weight=1)

        top = tk.Frame(parent, bg=BG_SURFACE)
        top.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 8))
        top.columnconfigure(0, weight=1)

        tk.Label(
            top,
            text="Workspace Explorer",
            font=FONT_LABEL,
            bg=BG_SURFACE,
            fg=FG_SUBTLE,
        ).grid(row=0, column=0, sticky="w")

        shell = tk.Frame(parent, bg=BG_SURFACE)
        shell.grid(row=1, column=0, sticky="nsew", padx=10, pady=(0, 10))
        shell.rowconfigure(0, weight=1)
        shell.columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(shell, show="tree")
        self.tree.grid(row=0, column=0, sticky="nsew")

        scrollbar_y = ttk.Scrollbar(shell, orient="vertical", command=self.tree.yview)
        scrollbar_y.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=scrollbar_y.set)

        self.tree.bind("<<TreeviewOpen>>", self._on_tree_open)
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)
        self.tree.bind("<Double-1>", self._on_tree_double_click)

    def _build_editor_side(self, parent: tk.Frame):
        parent.rowconfigure(1, weight=1)
        parent.columnconfigure(0, weight=1)

        self.editor_title_var = tk.StringVar(value="No file open")
        header = tk.Frame(parent, bg=BG_SURFACE)
        header.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 8))
        header.columnconfigure(0, weight=1)

        tk.Label(
            header,
            textvariable=self.editor_title_var,
            font=FONT_LABEL,
            bg=BG_SURFACE,
            fg=FG_SUBTLE,
        ).grid(row=0, column=0, sticky="w")

        shell = tk.Frame(parent, bg=BG_SURFACE)
        shell.grid(row=1, column=0, sticky="nsew", padx=10, pady=(0, 10))
        shell.rowconfigure(0, weight=1)
        shell.columnconfigure(1, weight=1)

        self.line_numbers = LineNumberCanvas(shell, None)
        self.line_numbers.grid(row=0, column=0, sticky="ns")

        self.editor = tk.Text(
            shell,
            wrap="none",
            bg=BG_EDITOR,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground=BORDER,
            highlightcolor=BORDER,
            font=FONT_MONO,
            undo=True,
            padx=12,
            pady=12,
            tabs=("4c",),
        )
        self.editor.grid(row=0, column=1, sticky="nsew")
        self.line_numbers.text_widget = self.editor

        scroll_y = ttk.Scrollbar(shell, orient="vertical", command=self._on_editor_yscroll)
        scroll_y.grid(row=0, column=2, sticky="ns")
        self.editor.configure(yscrollcommand=lambda first, last: self._editor_yscroll_set(scroll_y, first, last))

        scroll_x = ttk.Scrollbar(shell, orient="horizontal", command=self.editor.xview)
        scroll_x.grid(row=1, column=1, sticky="ew")
        self.editor.configure(xscrollcommand=scroll_x.set)

        self.editor.bind("<<Modified>>", self._on_editor_modified)
        self.editor.bind("<KeyRelease>", self._schedule_highlight)
        self.editor.bind("<ButtonRelease-1>", self._redraw_line_numbers)
        self.editor.bind("<MouseWheel>", self._redraw_line_numbers)
        self.editor.bind("<Configure>", self._redraw_line_numbers)

    def _build_right_side(self, parent: tk.Frame):
        parent.rowconfigure(4, weight=1)
        parent.columnconfigure(0, weight=1)

        tk.Label(
            parent,
            text="File Info",
            font=FONT_LABEL,
            bg=BG_SURFACE,
            fg=FG_SUBTLE,
        ).grid(row=0, column=0, sticky="w", padx=10, pady=(10, 8))

        self.info_text = tk.Text(
            parent,
            wrap="word",
            height=10,
            bg=BG_EDITOR_2,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground=BORDER,
            highlightcolor=BORDER,
            font=FONT_TEXT,
            padx=10,
            pady=10,
        )
        self.info_text.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 10))
        self.info_text.configure(state="disabled")

        search_row = tk.Frame(parent, bg=BG_SURFACE)
        search_row.grid(row=2, column=0, sticky="ew", padx=10, pady=(0, 8))
        search_row.columnconfigure(1, weight=1)

        tk.Label(
            search_row,
            text="Code Search",
            font=FONT_LABEL,
            bg=BG_SURFACE,
            fg=FG_SUBTLE,
        ).grid(row=0, column=0, sticky="w", padx=(0, 8))

        self.code_search_var = tk.StringVar(value="")
        self.code_search_entry = tk.Entry(
            search_row,
            textvariable=self.code_search_var,
            bg=BG_INPUT,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground=BORDER,
            highlightcolor=BORDER,
        )
        self.code_search_entry.grid(row=0, column=1, sticky="ew", ipady=6)
        self.code_search_entry.bind("<KeyRelease>", self._on_code_search_change)

        tk.Label(
            parent,
            text="Code Index",
            font=FONT_LABEL,
            bg=BG_SURFACE,
            fg=FG_SUBTLE,
        ).grid(row=3, column=0, sticky="w", padx=10, pady=(4, 8))

        list_shell = tk.Frame(parent, bg=BG_SURFACE)
        list_shell.grid(row=4, column=0, sticky="nsew", padx=10, pady=(0, 10))
        list_shell.rowconfigure(0, weight=1)
        list_shell.columnconfigure(0, weight=1)

        self.code_list = ttk.Treeview(
            list_shell,
            columns=("relpath",),
            show="headings",
            height=18,
        )
        self.code_list.grid(row=0, column=0, sticky="nsew")

        self.code_list.heading("relpath", text="relative_path")
        self.code_list.column("relpath", anchor="w", width=320)

        code_scroll_y = ttk.Scrollbar(list_shell, orient="vertical", command=self.code_list.yview)
        code_scroll_y.grid(row=0, column=1, sticky="ns")
        self.code_list.configure(yscrollcommand=code_scroll_y.set)

        self.code_list.bind("<Double-1>", self._on_code_list_double_click)

    def _build_console_side(self, parent: tk.Frame):
        parent.rowconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)

        shell = tk.Frame(parent, bg=BG_SURFACE)
        shell.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        shell.rowconfigure(0, weight=1)
        shell.columnconfigure(0, weight=1)

        self.console = tk.Text(
            shell,
            wrap="none",
            bg=BG_CONSOLE,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground=BORDER,
            highlightcolor=BORDER,
            font=FONT_MONO,
            padx=12,
            pady=12,
        )
        self.console.grid(row=0, column=0, sticky="nsew")

        scroll_y = ttk.Scrollbar(shell, orient="vertical", command=self.console.yview)
        scroll_y.grid(row=0, column=1, sticky="ns")
        self.console.configure(yscrollcommand=scroll_y.set)

        scroll_x = ttk.Scrollbar(shell, orient="horizontal", command=self.console.xview)
        scroll_x.grid(row=1, column=0, sticky="ew")
        self.console.configure(xscrollcommand=scroll_x.set)

        self.console.configure(state="disabled")

    # ========================================================
    # EDITOR SCROLL / LINE NUMBERS
    # ========================================================

    def _on_editor_yscroll(self, *args):
        self.editor.yview(*args)
        self._redraw_line_numbers()

    def _editor_yscroll_set(self, scrollbar, first, last):
        scrollbar.set(first, last)
        self._redraw_line_numbers()

    def _redraw_line_numbers(self, _event=None):
        self.after_idle(self.line_numbers.redraw)

    # ========================================================
    # SYNTAX TAGS
    # ========================================================

    def _configure_editor_tags(self):
        text = self.editor

        base_tags = {
            "keyword": FG_SYNTAX_KEYWORD,
            "string": FG_SYNTAX_STRING,
            "comment": FG_SYNTAX_COMMENT,
            "number": FG_SYNTAX_NUMBER,
            "defclass": FG_SYNTAX_DEFCLASS,
            "builtin": FG_SYNTAX_BUILTIN,
            "operator": FG_SYNTAX_OPERATOR,
            "decorator": FG_SYNTAX_DECORATOR,
            "json_key": FG_SYNTAX_JSON_KEY,
            "bool": FG_SYNTAX_BOOL,
            "tag": FG_SYNTAX_TAG,
            "attr": FG_SYNTAX_ATTR,
            "heading": FG_SYNTAX_HEADING,
            "emph": FG_SYNTAX_MD_EM,
        }

        for name, color in base_tags.items():
            text.tag_configure(name, foreground=color)

        for i, color in enumerate(CSV_COLORS):
            text.tag_configure(f"csv_col_{i}", foreground=color)

    def _clear_syntax_tags(self):
        for tag in self.editor.tag_names():
            if (
                tag in {
                    "keyword", "string", "comment", "number", "defclass",
                    "builtin", "operator", "decorator", "json_key",
                    "bool", "tag", "attr", "heading", "emph"
                }
                or tag.startswith("csv_col_")
            ):
                self.editor.tag_remove(tag, "1.0", "end")

    def _schedule_highlight(self, _event=None):
        if self._highlight_job is not None:
            try:
                self.after_cancel(self._highlight_job)
            except Exception:
                pass
        self._highlight_job = self.after(self.HIGHLIGHT_DELAY_MS, self._apply_syntax_highlighting)

    def _apply_tag_range(self, tag: str, line_no: int, start_col: int, end_col: int):
        self.editor.tag_add(tag, f"{line_no}.{start_col}", f"{line_no}.{end_col}")

    def _iter_text_lines(self) -> List[str]:
        return self.editor.get("1.0", "end-1c").splitlines()

    def _apply_syntax_highlighting(self):
        self._highlight_job = None
        self._clear_syntax_tags()

        if self.current_file is None:
            self._redraw_line_numbers()
            return

        ext = self.current_file.suffix.lower()
        name = self.current_file.name.lower()
        lines = self._iter_text_lines()

        try:
            if ext == ".py":
                self._highlight_python(lines)
            elif ext in {".json"}:
                self._highlight_json(lines)
            elif ext in {".yaml", ".yml"}:
                self._highlight_yaml(lines)
            elif ext in {".sql"}:
                self._highlight_sql(lines)
            elif ext in {".js", ".ts"}:
                self._highlight_js_ts(lines)
            elif ext in {".html", ".xml"}:
                self._highlight_html_xml(lines)
            elif ext in {".css"}:
                self._highlight_css(lines)
            elif ext in {".md"}:
                self._highlight_markdown(lines)
            elif ext in {".csv"}:
                self._highlight_csv(lines)
            elif name in {".env", "dockerfile", ".gitignore"}:
                self._highlight_simple_strings_numbers(lines)
            else:
                self._highlight_simple_strings_numbers(lines)
        finally:
            self._redraw_line_numbers()

    # ========================================================
    # HIGHLIGHTERS
    # ========================================================

    def _highlight_python(self, lines: List[str]):
        triple_pat = re.compile(r"('{3}|\"{3})")
        token_word = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")
        number_pat = re.compile(r"\b\d+(\.\d+)?\b")
        decorator_pat = re.compile(r"(^|\s)@[\w\.]+")
        operator_pat = re.compile(r"==|!=|<=|>=|:=|[-+*/%=<>]")

        in_triple: Optional[str] = None

        for i, line in enumerate(lines, start=1):
            if in_triple:
                end_idx = line.find(in_triple)
                if end_idx == -1:
                    self._apply_tag_range("string", i, 0, len(line))
                    continue
                self._apply_tag_range("string", i, 0, end_idx + 3)
                line_rest_start = end_idx + 3
                in_triple = None
            else:
                line_rest_start = 0

            scan = line[line_rest_start:]
            triple_match = triple_pat.search(scan)
            if triple_match:
                start = line_rest_start + triple_match.start()
                delim = triple_match.group(1)
                end = line.find(delim, start + 3)
                if end == -1:
                    self._apply_tag_range("string", i, start, len(line))
                    in_triple = delim
                    scan_before = line[:start]
                else:
                    self._apply_tag_range("string", i, start, end + 3)
                    scan_before = line[:start] + " " * (end + 3 - start) + line[end + 3:]
            else:
                scan_before = line

            comment_idx = self._find_python_comment_index(scan_before)
            code_part = scan_before if comment_idx == -1 else scan_before[:comment_idx]
            if comment_idx != -1:
                self._apply_tag_range("comment", i, comment_idx, len(line))

            for m in re.finditer(r"""('([^'\\]|\\.)*'|"([^"\\]|\\.)*")""", code_part):
                self._apply_tag_range("string", i, m.start(), m.end())

            masked = re.sub(r"""('([^'\\]|\\.)*'|"([^"\\]|\\.)*")""", lambda m: " " * (m.end() - m.start()), code_part)

            for m in decorator_pat.finditer(masked):
                start = m.start()
                at_pos = masked.find("@", start, m.end())
                if at_pos >= 0:
                    self._apply_tag_range("decorator", i, at_pos, m.end())

            for m in number_pat.finditer(masked):
                self._apply_tag_range("number", i, m.start(), m.end())

            for m in operator_pat.finditer(masked):
                self._apply_tag_range("operator", i, m.start(), m.end())

            for m in re.finditer(r"\b(def|class)\s+([A-Za-z_][A-Za-z0-9_]*)", masked):
                kw_start, kw_end = m.start(1), m.end(1)
                name_start, name_end = m.start(2), m.end(2)
                self._apply_tag_range("keyword", i, kw_start, kw_end)
                self._apply_tag_range("defclass", i, name_start, name_end)

            for m in token_word.finditer(masked):
                word = m.group(0)
                if word in keyword.kwlist:
                    self._apply_tag_range("keyword", i, m.start(), m.end())
                elif word in {"True", "False", "None"}:
                    self._apply_tag_range("bool", i, m.start(), m.end())
                elif word in PY_BUILTINS:
                    self._apply_tag_range("builtin", i, m.start(), m.end())

    def _find_python_comment_index(self, line: str) -> int:
        in_single = False
        in_double = False
        escaped = False
        for idx, ch in enumerate(line):
            if escaped:
                escaped = False
                continue
            if ch == "\\":
                escaped = True
                continue
            if ch == "'" and not in_double:
                in_single = not in_single
                continue
            if ch == '"' and not in_single:
                in_double = not in_double
                continue
            if ch == "#" and not in_single and not in_double:
                return idx
        return -1

    def _highlight_json(self, lines: List[str]):
        key_pat = re.compile(r'"([^"\\]|\\.)*"\s*:')
        string_pat = re.compile(r'"([^"\\]|\\.)*"')
        number_pat = re.compile(r"\b-?\d+(\.\d+)?([eE][+-]?\d+)?\b")
        bool_pat = re.compile(r"\b(true|false|null)\b", re.IGNORECASE)

        for i, line in enumerate(lines, start=1):
            for m in key_pat.finditer(line):
                key_end = line.find(":", m.start(), m.end())
                if key_end != -1:
                    self._apply_tag_range("json_key", i, m.start(), key_end)

            for m in string_pat.finditer(line):
                self._apply_tag_range("string", i, m.start(), m.end())

            for m in number_pat.finditer(line):
                self._apply_tag_range("number", i, m.start(), m.end())

            for m in bool_pat.finditer(line):
                self._apply_tag_range("bool", i, m.start(), m.end())

    def _highlight_yaml(self, lines: List[str]):
        key_pat = re.compile(r"^(\s*[-]?\s*)([A-Za-z0-9_\-\.\"']+)\s*:")
        string_pat = re.compile(r"""("([^"\\]|\\.)*"|'([^'\\]|\\.)*')""")
        number_pat = re.compile(r"\b-?\d+(\.\d+)?\b")
        bool_pat = re.compile(r"\b(true|false|null|yes|no|on|off)\b", re.IGNORECASE)
        comment_pat = re.compile(r"#.*$")

        for i, line in enumerate(lines, start=1):
            m = key_pat.search(line)
            if m:
                self._apply_tag_range("json_key", i, m.start(2), m.end(2))

            for s in string_pat.finditer(line):
                self._apply_tag_range("string", i, s.start(), s.end())

            for n in number_pat.finditer(line):
                self._apply_tag_range("number", i, n.start(), n.end())

            for b in bool_pat.finditer(line):
                self._apply_tag_range("bool", i, b.start(), b.end())

            c = comment_pat.search(line)
            if c:
                self._apply_tag_range("comment", i, c.start(), c.end())

    def _highlight_sql(self, lines: List[str]):
        sql_keywords = {
            "select", "from", "where", "join", "left", "right", "inner", "outer",
            "group", "by", "order", "having", "limit", "insert", "into", "update",
            "delete", "create", "table", "view", "drop", "alter", "and", "or",
            "not", "null", "as", "case", "when", "then", "else", "end", "distinct",
            "union", "all", "on", "values", "set"
        }
        word_pat = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")
        number_pat = re.compile(r"\b\d+(\.\d+)?\b")
        string_pat = re.compile(r"""('([^'\\]|\\.)*')""")
        comment_pat = re.compile(r"--.*$")

        for i, line in enumerate(lines, start=1):
            c = comment_pat.search(line)
            code_part = line
            if c:
                self._apply_tag_range("comment", i, c.start(), c.end())
                code_part = line[:c.start()]

            for s in string_pat.finditer(code_part):
                self._apply_tag_range("string", i, s.start(), s.end())

            masked = re.sub(string_pat, lambda m: " " * (m.end() - m.start()), code_part)

            for n in number_pat.finditer(masked):
                self._apply_tag_range("number", i, n.start(), n.end())

            for w in word_pat.finditer(masked):
                if w.group(0).lower() in sql_keywords:
                    self._apply_tag_range("keyword", i, w.start(), w.end())

    def _highlight_js_ts(self, lines: List[str]):
        kw = {
            "function", "return", "const", "let", "var", "if", "else", "for", "while",
            "switch", "case", "break", "continue", "class", "new", "import", "export",
            "from", "async", "await", "try", "catch", "finally", "throw", "extends",
            "interface", "type"
        }
        word_pat = re.compile(r"\b[A-Za-z_$][A-Za-z0-9_$]*\b")
        number_pat = re.compile(r"\b\d+(\.\d+)?\b")
        string_pat = re.compile(r"""('([^'\\]|\\.)*'|"([^"\\]|\\.)*"|`([^`\\]|\\.)*`)""")
        comment_pat = re.compile(r"//.*$")

        for i, line in enumerate(lines, start=1):
            c = comment_pat.search(line)
            code_part = line
            if c:
                self._apply_tag_range("comment", i, c.start(), c.end())
                code_part = line[:c.start()]

            for s in string_pat.finditer(code_part):
                self._apply_tag_range("string", i, s.start(), s.end())

            masked = re.sub(string_pat, lambda m: " " * (m.end() - m.start()), code_part)

            for n in number_pat.finditer(masked):
                self._apply_tag_range("number", i, n.start(), n.end())

            for m in re.finditer(r"\b(class|function)\s+([A-Za-z_$][A-Za-z0-9_$]*)", masked):
                self._apply_tag_range("keyword", i, m.start(1), m.end(1))
                self._apply_tag_range("defclass", i, m.start(2), m.end(2))

            for w in word_pat.finditer(masked):
                if w.group(0) in kw:
                    self._apply_tag_range("keyword", i, w.start(), w.end())

    def _highlight_html_xml(self, lines: List[str]):
        tag_pat = re.compile(r"</?([A-Za-z0-9:_-]+)")
        attr_pat = re.compile(r"\s([A-Za-z_:][-A-Za-z0-9_:.]*)\s*=")
        string_pat = re.compile(r'"[^"]*"|\'[^\']*\'')

        for i, line in enumerate(lines, start=1):
            for m in tag_pat.finditer(line):
                self._apply_tag_range("tag", i, m.start(), m.end())

            for a in attr_pat.finditer(line):
                self._apply_tag_range("attr", i, a.start(1), a.end(1))

            for s in string_pat.finditer(line):
                self._apply_tag_range("string", i, s.start(), s.end())

    def _highlight_css(self, lines: List[str]):
        prop_pat = re.compile(r"\b([A-Za-z-]+)\s*:")
        number_pat = re.compile(r"\b\d+(\.\d+)?(px|em|rem|%)?\b")
        color_pat = re.compile(r"#[0-9a-fA-F]{3,8}\b")
        comment_pat = re.compile(r"/\*.*?\*/")

        for i, line in enumerate(lines, start=1):
            for c in comment_pat.finditer(line):
                self._apply_tag_range("comment", i, c.start(), c.end())

            for p in prop_pat.finditer(line):
                self._apply_tag_range("attr", i, p.start(1), p.end(1))

            for n in number_pat.finditer(line):
                self._apply_tag_range("number", i, n.start(), n.end())

            for c in color_pat.finditer(line):
                self._apply_tag_range("string", i, c.start(), c.end())

    def _highlight_markdown(self, lines: List[str]):
        emph_pat = re.compile(r"(\*\*.*?\*\*|__.*?__|\*.*?\*|_.*?_)")
        code_pat = re.compile(r"`[^`]+`")
        link_pat = re.compile(r"\[[^\]]+\]\([^)]+\)")

        for i, line in enumerate(lines, start=1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                self._apply_tag_range("heading", i, 0, len(line))

            for m in emph_pat.finditer(line):
                self._apply_tag_range("emph", i, m.start(), m.end())

            for m in code_pat.finditer(line):
                self._apply_tag_range("string", i, m.start(), m.end())

            for m in link_pat.finditer(line):
                self._apply_tag_range("builtin", i, m.start(), m.end())

    def _highlight_csv(self, lines: List[str]):
        for i, line in enumerate(lines, start=1):
            try:
                row = next(csv.reader([line]))
            except Exception:
                continue

            idx = 0
            col = 0
            while idx < len(line) and col < len(row):
                cell = row[col]
                tag = f"csv_col_{col % len(CSV_COLORS)}"

                if cell == "":
                    comma_pos = line.find(",", idx)
                    if comma_pos == idx:
                        idx += 1
                        col += 1
                        continue

                if idx < len(line) and line[idx] == '"':
                    end_idx = idx + 1
                    escaped = False
                    while end_idx < len(line):
                        ch = line[end_idx]
                        if ch == '"' and not escaped:
                            if end_idx + 1 < len(line) and line[end_idx + 1] == '"':
                                end_idx += 2
                                continue
                            end_idx += 1
                            break
                        escaped = False
                        end_idx += 1
                    self._apply_tag_range(tag, i, idx, min(end_idx, len(line)))
                    idx = end_idx
                else:
                    next_comma = line.find(",", idx)
                    end_idx = len(line) if next_comma == -1 else next_comma
                    self._apply_tag_range(tag, i, idx, end_idx)
                    idx = end_idx

                if idx < len(line) and line[idx] == ",":
                    self._apply_tag_range("operator", i, idx, idx + 1)
                    idx += 1

                col += 1

    def _highlight_simple_strings_numbers(self, lines: List[str]):
        string_pat = re.compile(r"""("([^"\\]|\\.)*"|'([^'\\]|\\.)*')""")
        number_pat = re.compile(r"\b\d+(\.\d+)?\b")
        comment_pat = re.compile(r"#.*$")

        for i, line in enumerate(lines, start=1):
            for s in string_pat.finditer(line):
                self._apply_tag_range("string", i, s.start(), s.end())
            for n in number_pat.finditer(line):
                self._apply_tag_range("number", i, n.start(), n.end())
            c = comment_pat.search(line)
            if c:
                self._apply_tag_range("comment", i, c.start(), c.end())

    # ========================================================
    # STATUS / CONSOLE
    # ========================================================

    def _update_status(self, msg: str):
        self.status_var.set(msg)
        self.top_info_var.set(str(self.workspace_root))

    def _append_console(self, text: str):
        self.console.configure(state="normal")
        self.console.insert("end", text)
        self.console.see("end")
        self.console.configure(state="disabled")

    def _clear_console(self):
        self.console.configure(state="normal")
        self.console.delete("1.0", "end")
        self.console.configure(state="disabled")
        self._update_status("Console cleared")

    # ========================================================
    # TREE
    # ========================================================

    def _load_tree(self):
        self.tree.delete(*self.tree.get_children())
        self.tree.insert("", "end", iid=".", text=".", open=True)
        self._populate_tree_node(".", Path("."))

    def _populate_tree_node(self, parent_iid: str, rel_path: Path):
        abs_dir = (self.workspace_root / rel_path).resolve()
        if not abs_dir.exists() or not abs_dir.is_dir():
            return

        for child in self.tree.get_children(parent_iid):
            self.tree.delete(child)

        try:
            entries = list_dir(abs_dir, ignore_hidden=not self.ignore_hidden.get())
        except Exception as e:
            self.tree.insert(parent_iid, "end", text=f"ERROR: {e}", iid=f"{parent_iid}::__error__")
            return

        for entry in entries:
            child_rel = safe_rel(entry, self.workspace_root)
            iid = make_iid(child_rel)

            if entry.is_dir():
                self.tree.insert(parent_iid, "end", iid=iid, text=entry.name, open=False)
                self.tree.insert(iid, "end", iid=f"{iid}::__placeholder__", text="...")
            else:
                self.tree.insert(parent_iid, "end", iid=iid, text=entry.name)

    def _ensure_loaded(self, iid: str):
        children = self.tree.get_children(iid)
        if len(children) == 1 and children[0].endswith("::__placeholder__"):
            rel = Path("." if iid == "." else iid)
            self._populate_tree_node(iid, rel)

    def _selected_path(self) -> Optional[Path]:
        sel = self.tree.selection()
        if not sel:
            return None
        iid = sel[0]
        if "::__" in iid:
            return None
        rel = Path("." if iid == "." else iid)
        p = (self.workspace_root / rel).resolve()
        if not within_root(p, self.workspace_root):
            return None
        return p

    def _on_tree_open(self, _event=None):
        iid = self.tree.focus()
        if iid:
            self._ensure_loaded(iid)

    def _on_tree_select(self, _event=None):
        self._refresh_info_panel()

    def _on_tree_double_click(self, _event=None):
        path = self._selected_path()
        if path and path.is_file():
            self._open_file(path)

    # ========================================================
    # CODE INDEX
    # ========================================================

    def _iter_code_files(self, root: Path) -> List[Path]:
        out: List[Path] = []
        try:
            for p in root.rglob("*"):
                try:
                    if self.ignore_hidden.get():
                        if any(part.startswith(".") for part in p.parts):
                            continue
                    if is_code_index_file(p):
                        out.append(p)
                except Exception:
                    continue
        except Exception:
            pass

        out.sort(key=lambda x: safe_rel(x, root).lower())
        return out

    def _rebuild_code_index(self):
        self.code_index_files = self._iter_code_files(self.workspace_root)
        self._load_code_list()
        self._update_status(f"Code index rebuilt: {len(self.code_index_files)} files")

    def _load_code_list(self):
        self.code_list.delete(*self.code_list.get_children())

        needle = self.code_search_var.get().strip().lower()

        for p in self.code_index_files:
            rel = safe_rel(p, self.workspace_root)
            if needle and needle not in rel.lower() and needle not in p.name.lower():
                continue
            self.code_list.insert("", "end", iid=rel, values=(rel,))

    def _on_code_search_change(self, _event=None):
        self._load_code_list()

    def _on_code_list_double_click(self, _event=None):
        sel = self.code_list.selection()
        if not sel:
            return

        rel = sel[0]
        path = (self.workspace_root / rel).resolve()
        if path.exists() and path.is_file():
            self._open_file(path)

    # ========================================================
    # FILE INFO
    # ========================================================

    def _refresh_info_panel(self):
        path = self._selected_path()

        lines = []
        if path is None:
            lines.append("No selection.")
        else:
            lines.append(f"path      : {path}")
            lines.append(f"relative  : {safe_rel(path, self.workspace_root)}")
            lines.append(f"type      : {'dir' if path.is_dir() else 'file'}")

            if path.exists():
                try:
                    stat = path.stat()
                    lines.append(f"size      : {stat.st_size} bytes")
                except Exception:
                    lines.append("size      : -")

            if path.is_file():
                lines.append(f"suffix    : {path.suffix}")
                lines.append(f"text_file : {is_text_file(path)}")
                lines.append(f"runnable  : {path.suffix.lower() == '.py'}")

        lines.append("")
        lines.append(f"workspace_root : {self.workspace_root}")
        lines.append(f"indexed_files  : {len(self.code_index_files)}")

        self.info_text.configure(state="normal")
        self.info_text.delete("1.0", "end")
        self.info_text.insert("1.0", "\n".join(lines))
        self.info_text.configure(state="disabled")

    # ========================================================
    # EDITOR
    # ========================================================

    def _open_file(self, path: Path):
        if not within_root(path, self.workspace_root):
            self._update_status("Open blocked: path outside workspace")
            return

        if not path.exists() or not path.is_file():
            self._update_status(f"File not found: {path}")
            return

        if not is_text_file(path):
            self._update_status(f"Unsupported file type: {path.name}")
            return

        if self.editor_dirty and self.current_file is not None:
            answer = messagebox.askyesnocancel(
                "Unsaved Changes",
                "Aktuelle Datei hat ungespeicherte Änderungen. Erst speichern?",
            )
            if answer is None:
                return
            if answer is True:
                if not self._save_current_file():
                    return

        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            try:
                text = path.read_text(encoding="latin-1")
            except Exception as e:
                self._update_status(f"Open failed: {e}")
                return
        except Exception as e:
            self._update_status(f"Open failed: {e}")
            return

        self.editor.delete("1.0", "end")
        self.editor.insert("1.0", text)
        self.editor.edit_modified(False)

        self.current_file = path
        self.editor_dirty = False
        self._refresh_editor_title()
        self._refresh_info_panel()
        self._highlight_code_list_for_path(path)
        self._update_status(f"Opened: {path.name}")
        self._apply_syntax_highlighting()
        self._redraw_line_numbers()

    def _highlight_code_list_for_path(self, path: Path):
        rel = safe_rel(path, self.workspace_root)
        if self.code_list.exists(rel):
            self.code_list.selection_set(rel)
            self.code_list.focus(rel)
            self.code_list.see(rel)

    def _refresh_editor_title(self):
        if self.current_file is None:
            self.editor_title_var.set("No file open")
            return

        mark = " *" if self.editor_dirty else ""
        self.editor_title_var.set(f"{self.current_file.name}{mark}")

    def _on_editor_modified(self, _event=None):
        try:
            modified = self.editor.edit_modified()
        except Exception:
            modified = False

        if modified:
            self.editor_dirty = True
            self._refresh_editor_title()
            self.editor.edit_modified(False)
            self._schedule_highlight()

    def _save_current_file(self) -> bool:
        if self.current_file is None:
            self._update_status("No file open")
            return False

        if not within_root(self.current_file, self.workspace_root):
            self._update_status("Save blocked: file outside workspace")
            return False

        try:
            text = self.editor.get("1.0", "end-1c")
            self.current_file.write_text(text, encoding="utf-8")
            self.editor_dirty = False
            self._refresh_editor_title()
            self._refresh_info_panel()
            self._rebuild_code_index()
            self._highlight_code_list_for_path(self.current_file)
            self._update_status(f"Saved: {self.current_file.name}")
            self._apply_syntax_highlighting()
            return True
        except Exception as e:
            self._update_status(f"Save failed: {e}")
            return False

    # ========================================================
    # RUN / STOP
    # ========================================================

    def _run_current_file(self):
        if self.current_file is None:
            self._update_status("No file open")
            return

        if self.current_file.suffix.lower() != ".py":
            self._update_status("Run currently only supports .py files")
            return

        if self.process is not None and self.process.poll() is None:
            self._update_status("A process is already running")
            return

        if self.editor_dirty:
            ok = messagebox.askyesno(
                "Unsaved Changes",
                "Datei hat ungespeicherte Änderungen. Vor Ausführung speichern?",
            )
            if ok:
                if not self._save_current_file():
                    return

        self._clear_console()
        self._append_console(f"> RUN {self.current_file}\n\n")

        try:
            self.process = subprocess.Popen(
                [sys.executable, str(self.current_file)],
                cwd=str(self.current_file.parent),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except Exception as e:
            self._append_console(f"START FAILED: {e}\n")
            self._update_status(f"Run failed: {e}")
            self.process = None
            return

        self._update_status(f"Running: {self.current_file.name}")

        def reader():
            proc = self.process
            if proc is None or proc.stdout is None:
                return

            try:
                for line in proc.stdout:
                    self.after(0, lambda ln=line: self._append_console(ln))
            except Exception as e:
                self.after(0, lambda: self._append_console(f"\nREAD ERROR: {e}\n"))
            finally:
                try:
                    return_code = proc.wait(timeout=1)
                except Exception:
                    return_code = None

                def done():
                    if return_code is None:
                        self._append_console("\nPROCESS FINISHED\n")
                        self._update_status("Process finished")
                    elif return_code == 0:
                        self._append_console(f"\nPROCESS EXITED WITH CODE {return_code}\n")
                        self._update_status("Process finished successfully")
                    else:
                        self._append_console(f"\nPROCESS EXITED WITH CODE {return_code}\n")
                        self._update_status(f"Process failed with code {return_code}")

                    self.process = None

                self.after(0, done)

        self.process_thread = threading.Thread(target=reader, daemon=True)
        self.process_thread.start()

    def _stop_process(self):
        if self.process is None or self.process.poll() is not None:
            self._update_status("No running process")
            return

        try:
            self.process.terminate()
            self._append_console("\n> STOP REQUESTED\n")
            self._update_status("Stop requested")
        except Exception as e:
            self._append_console(f"\nSTOP FAILED: {e}\n")
            self._update_status(f"Stop failed: {e}")

    # ========================================================
    # ACTIONS
    # ========================================================

    def _reload_workspace(self):
        new_root = Path(self.workspace_var.get()).expanduser().resolve()
        if not new_root.exists() or not new_root.is_dir():
            self._update_status(f"Invalid workspace: {new_root}")
            return

        if not within_root(new_root, FTMO_ROOT) and new_root != FTMO_ROOT:
            self._update_status("Workspace blocked: outside FTMO root")
            return

        self.workspace_root = new_root
        self.current_file = None
        self.editor_dirty = False
        self.editor.delete("1.0", "end")
        self._clear_syntax_tags()
        self._refresh_editor_title()
        self._load_tree()
        self._rebuild_code_index()
        self._refresh_info_panel()
        self._redraw_line_numbers()
        self._update_status(f"Workspace loaded: {self.workspace_root}")

    def _toggle_hidden(self):
        self.ignore_hidden.set(not self.ignore_hidden.get())
        self.hidden_btn.label.configure(
            text="Hidden: ON" if self.ignore_hidden.get() else "Hidden: OFF"
        )
        self._load_tree()
        self._rebuild_code_index()
        self._update_status("Explorer and index reloaded")

    def _new_file(self):
        base = self._selected_path()
        if base is None:
            base = self.workspace_root
        if base.is_file():
            base = base.parent

        name = simpledialog.askstring("New File", "Dateiname:")
        if not name:
            return

        target = (base / name).resolve()
        if not within_root(target, self.workspace_root):
            self._update_status("Create blocked: outside workspace")
            return

        if target.exists():
            self._update_status("File already exists")
            return

        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("", encoding="utf-8")
            self._load_tree()
            self._rebuild_code_index()
            self._update_status(f"File created: {target.name}")
        except Exception as e:
            self._update_status(f"Create file failed: {e}")

    def _new_folder(self):
        base = self._selected_path()
        if base is None:
            base = self.workspace_root
        if base.is_file():
            base = base.parent

        name = simpledialog.askstring("New Folder", "Ordnername:")
        if not name:
            return

        target = (base / name).resolve()
        if not within_root(target, self.workspace_root):
            self._update_status("Create blocked: outside workspace")
            return

        if target.exists():
            self._update_status("Folder already exists")
            return

        try:
            target.mkdir(parents=True, exist_ok=True)
            self._load_tree()
            self._rebuild_code_index()
            self._update_status(f"Folder created: {target.name}")
        except Exception as e:
            self._update_status(f"Create folder failed: {e}")

    def _delete_selected(self):
        path = self._selected_path()
        if path is None or path == self.workspace_root:
            self._update_status("Nothing deletable selected")
            return

        if not within_root(path, self.workspace_root):
            self._update_status("Delete blocked: outside workspace")
            return

        ok = messagebox.askyesno("Delete", f"Wirklich löschen?\n\n{path}")
        if not ok:
            return

        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()

            if self.current_file is not None and self.current_file == path:
                self.current_file = None
                self.editor_dirty = False
                self.editor.delete("1.0", "end")
                self._clear_syntax_tags()
                self._refresh_editor_title()

            self._load_tree()
            self._rebuild_code_index()
            self._refresh_info_panel()
            self._redraw_line_numbers()
            self._update_status(f"Deleted: {path.name}")
        except Exception as e:
            self._update_status(f"Delete failed: {e}")

    def _copy_current_path(self):
        path = self.current_file or self._selected_path()
        if path is None:
            self._update_status("No path available")
            return

        self.clipboard_clear()
        self.clipboard_append(str(path))
        self._update_status(f"Copied path: {path}")

    def destroy(self):
        try:
            if self.process is not None and self.process.poll() is None:
                self.process.terminate()
        except Exception:
            pass
        super().destroy()


# ============================================================
# STANDALONE
# ============================================================

class CodeStationDashboard(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Code Station")
        self.geometry("1850x1100")
        self.minsize(1350, 850)
        self.configure(bg=BG_APP)

        self.panel = CodeStationPanel(self)
        self.panel.pack(fill="both", expand=True)

        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _on_close(self):
        try:
            self.panel.destroy()
        except Exception:
            pass
        self.destroy()


def main():
    app = CodeStationDashboard()
    app.mainloop()


if __name__ == "__main__":
    main()