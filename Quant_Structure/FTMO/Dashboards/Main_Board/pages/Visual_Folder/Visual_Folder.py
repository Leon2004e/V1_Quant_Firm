# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Dashboards/Main_Board/pages/Visual_Folder/Visual_Folder.py

Zweck:
- Visual Folder Explorer als Tkinter-Panel für das FTMO Main Board
- Links: interaktiver Folder Tree
- Rechts: sichtbarer ASCII-Tree als Nebenfenster
- One-click Copy
- One-click Export
- Hidden-Dateien optional
- Root frei wählbar
- Einheitlicher FTMO-Stil

Einbettung im Main Board:
    panel_class_name="FolderExplorerPanel"

Standalone:
    python Quant_Structure/FTMO/Dashboards/Main_Board/pages/Visual_Folder/Visual_Folder.py
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import tkinter as tk
from tkinter import ttk


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

DEFAULT_ROOT = FTMO_ROOT
DEFAULT_OUTPUT_PATH = FTMO_ROOT / "Dashboards" / "Main_Board" / "runtime" / "visible_tree.txt"
DEFAULT_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)


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
# MODEL
# ============================================================

@dataclass
class VisibleNode:
    name: str
    rel_posix: str
    kind: str  # "dir" | "file"
    children: Optional[List["VisibleNode"]] = None


def build_visible_tree(
    root: Path,
    rel: Path,
    expanded_paths: set[str],
    ignore_hidden: bool = True,
) -> VisibleNode:
    abs_path = (root / rel).resolve()
    if not abs_path.exists():
        raise FileNotFoundError(str(abs_path))
    if not abs_path.is_dir():
        raise NotADirectoryError(str(abs_path))

    entries = list(abs_path.iterdir())
    if ignore_hidden:
        entries = [e for e in entries if not e.name.startswith(".")]

    entries.sort(key=lambda p: (not p.is_dir(), p.name.lower()))

    rel_posix = rel.as_posix()
    children: List[VisibleNode] = []

    for e in entries:
        child_rel = rel / e.name
        child_rel_posix = child_rel.as_posix()

        if e.is_dir():
            if child_rel_posix in expanded_paths:
                children.append(
                    build_visible_tree(
                        root=root,
                        rel=child_rel,
                        expanded_paths=expanded_paths,
                        ignore_hidden=ignore_hidden,
                    )
                )
            else:
                children.append(
                    VisibleNode(
                        name=e.name,
                        rel_posix=child_rel_posix,
                        kind="dir",
                        children=[],
                    )
                )
        else:
            children.append(
                VisibleNode(
                    name=e.name,
                    rel_posix=child_rel_posix,
                    kind="file",
                    children=None,
                )
            )

    node_name = "." if rel_posix == "." else rel.name
    return VisibleNode(name=node_name, rel_posix=rel_posix, kind="dir", children=children)


def render_ascii_tree(node: VisibleNode) -> str:
    lines: List[str] = []

    def rec(n: VisibleNode, prefix: str = "", is_last: bool = True, is_root: bool = False):
        if is_root:
            lines.append(".")
        else:
            connector = "└── " if is_last else "├── "
            lines.append(prefix + connector + n.name)

        kids = n.children or []
        if not kids:
            return

        next_prefix = prefix + ("    " if is_last else "│   ")
        for i, c in enumerate(kids):
            rec(c, next_prefix, i == (len(kids) - 1), is_root=False)

    rec(node, prefix="", is_last=True, is_root=True)
    return "\n".join(lines) + "\n"


# ============================================================
# HELPERS
# ============================================================

def make_iid(rel_path: str) -> str:
    return rel_path if rel_path else "."


def safe_rel(path: Path, root: Path) -> str:
    try:
        rel = path.resolve().relative_to(root.resolve())
        s = rel.as_posix()
        return s if s else "."
    except Exception:
        return "."


def list_dir(path: Path, ignore_hidden: bool) -> List[Path]:
    entries = list(path.iterdir())
    if ignore_hidden:
        entries = [e for e in entries if not e.name.startswith(".")]
    entries.sort(key=lambda p: (not p.is_dir(), p.name.lower()))
    return entries


def shorten_path(s: str, max_len: int = 100) -> str:
    if len(s) <= max_len:
        return s
    return "..." + s[-(max_len - 3):]


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
# PANEL
# ============================================================

class FolderExplorerPanel(tk.Frame):
    def __init__(
        self,
        parent,
        app=None,
        ftmo_root: Optional[Path] = None,
        root_path: Optional[Path] = None,
        **_kwargs,
    ):
        super().__init__(parent, bg=BG_APP)

        self.app = app
        self.ftmo_root = ftmo_root
        self.injected_root_path = root_path

        self.root_path = Path(DEFAULT_ROOT).resolve()
        self.output_path = Path(DEFAULT_OUTPUT_PATH).resolve()
        self.ignore_hidden = tk.BooleanVar(value=True)
        self.expanded_paths: set[str] = {"."}

        self._configure_ttk()
        self._build_ui()
        self._load_tree()
        self._refresh_ascii_preview()

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

    # ========================================================
    # UI
    # ========================================================

    def _build_ui(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)

        topbar = tk.Frame(self, bg=BG_TOP, height=54, highlightbackground=BORDER, highlightthickness=1)
        topbar.grid(row=0, column=0, sticky="ew", padx=12, pady=(12, 10))
        topbar.grid_propagate(False)
        topbar.columnconfigure(1, weight=1)

        tk.Label(
            topbar,
            text="Visual Folder",
            font=FONT_TITLE,
            bg=BG_TOP,
            fg=FG_WHITE,
        ).grid(row=0, column=0, sticky="w", padx=14)

        self.top_info_var = tk.StringVar(value=str(self.root_path))
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
        toolbar.columnconfigure(3, weight=1)

        tk.Label(
            toolbar,
            text="Root",
            font=FONT_LABEL,
            bg=BG_APP,
            fg=FG_MUTED,
        ).grid(row=0, column=0, sticky="w", padx=(0, 8))

        self.root_var = tk.StringVar(value=str(self.root_path))
        self.root_entry = tk.Entry(
            toolbar,
            textvariable=self.root_var,
            bg=BG_INPUT,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground=BORDER,
            highlightcolor=BORDER,
            font=FONT_TEXT,
        )
        self.root_entry.grid(row=0, column=1, sticky="ew", padx=(0, 10), ipady=7)

        tk.Label(
            toolbar,
            text="Output",
            font=FONT_LABEL,
            bg=BG_APP,
            fg=FG_MUTED,
        ).grid(row=0, column=2, sticky="w", padx=(0, 8))

        self.output_var = tk.StringVar(value=str(self.output_path))
        self.output_entry = tk.Entry(
            toolbar,
            textvariable=self.output_var,
            bg=BG_INPUT,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground=BORDER,
            highlightcolor=BORDER,
            font=FONT_TEXT,
        )
        self.output_entry.grid(row=0, column=3, sticky="ew", padx=(0, 10), ipady=7)

        btns = tk.Frame(toolbar, bg=BG_APP)
        btns.grid(row=0, column=4, sticky="e")

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
        self.hidden_btn.pack(side="left", padx=(0, 6))

        sbtn("Reload", self._reload_from_input, 84)
        sbtn("Expand All", self._expand_all, 96)
        sbtn("Collapse All", self._collapse_all, 104)
        sbtn("Copy", self._copy_ascii_to_clipboard, 78)
        pbtn("Export", self._export_ascii, 82)

        content = tk.PanedWindow(self, orient="horizontal", sashwidth=6, bg=BG_APP, bd=0)
        content.grid(row=2, column=0, sticky="nsew", padx=12, pady=(0, 12))

        left = PanelFrame(content, "Folder Tree")
        right = PanelFrame(content, "Visible Tree Output")

        content.add(left, minsize=420)
        content.add(right, minsize=520)

        self._build_tree_side(left.body)
        self._build_preview_side(right.body)

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
        parent.rowconfigure(2, weight=1)
        parent.columnconfigure(0, weight=1)

        top = tk.Frame(parent, bg=BG_SURFACE)
        top.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 8))
        top.columnconfigure(0, weight=1)

        tk.Label(
            top,
            text="Interactive tree view",
            font=FONT_LABEL,
            bg=BG_SURFACE,
            fg=FG_SUBTLE,
        ).grid(row=0, column=0, sticky="w")

        self.tree_badge = Badge(top, "0 Nodes", fg=FG_MAIN, bg=BG_BADGE)
        self.tree_badge.grid(row=0, column=1, sticky="e")

        divider = tk.Frame(parent, bg=DIVIDER, height=1)
        divider.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 8))

        shell = tk.Frame(parent, bg=BG_SURFACE)
        shell.grid(row=2, column=0, sticky="nsew", padx=10, pady=(0, 10))
        shell.rowconfigure(0, weight=1)
        shell.columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(shell, show="tree")
        self.tree.grid(row=0, column=0, sticky="nsew")

        tree_scroll_y = ttk.Scrollbar(shell, orient="vertical", command=self.tree.yview)
        tree_scroll_y.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=tree_scroll_y.set)

        self.tree.bind("<<TreeviewOpen>>", self._on_tree_open)
        self.tree.bind("<<TreeviewClose>>", self._on_tree_close)
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)

    def _build_preview_side(self, parent: tk.Frame):
        parent.rowconfigure(2, weight=1)
        parent.columnconfigure(0, weight=1)

        top = tk.Frame(parent, bg=BG_SURFACE)
        top.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 8))
        top.columnconfigure(0, weight=1)

        tk.Label(
            top,
            text="ASCII preview",
            font=FONT_LABEL,
            bg=BG_SURFACE,
            fg=FG_SUBTLE,
        ).grid(row=0, column=0, sticky="w")

        self.output_badge = Badge(top, "Preview", fg=FG_ACCENT, bg=BG_BADGE)
        self.output_badge.grid(row=0, column=1, sticky="e")

        divider = tk.Frame(parent, bg=DIVIDER, height=1)
        divider.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 8))

        shell = tk.Frame(parent, bg=BG_SURFACE)
        shell.grid(row=2, column=0, sticky="nsew", padx=10, pady=(0, 10))
        shell.rowconfigure(0, weight=1)
        shell.columnconfigure(0, weight=1)

        self.preview = tk.Text(
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
            padx=12,
            pady=12,
        )
        self.preview.grid(row=0, column=0, sticky="nsew")

        scroll_y = ttk.Scrollbar(shell, orient="vertical", command=self.preview.yview)
        scroll_y.grid(row=0, column=1, sticky="ns")
        self.preview.configure(yscrollcommand=scroll_y.set)

        scroll_x = ttk.Scrollbar(shell, orient="horizontal", command=self.preview.xview)
        scroll_x.grid(row=1, column=0, sticky="ew")
        self.preview.configure(xscrollcommand=scroll_x.set)

        self.preview.configure(state="disabled")

    # ========================================================
    # TREE LOGIC
    # ========================================================

    def _load_tree(self):
        self.tree.delete(*self.tree.get_children())

        root_iid = make_iid(".")
        self.tree.insert("", "end", iid=root_iid, text=".", open=True)
        self._populate_tree_node(root_iid, Path("."))

        if "." not in self.expanded_paths:
            self.expanded_paths.add(".")

        self._apply_expanded_state()
        self._update_tree_badge()
        self.top_info_var.set(shorten_path(str(self.root_path), 110))
        self.status_var.set("Tree loaded")

    def _populate_tree_node(self, parent_iid: str, rel_path: Path):
        abs_dir = (self.root_path / rel_path).resolve()
        if not abs_dir.exists() or not abs_dir.is_dir():
            return

        for child in self.tree.get_children(parent_iid):
            self.tree.delete(child)

        try:
            entries = list_dir(abs_dir, ignore_hidden=not self.ignore_hidden.get())
        except PermissionError:
            self.tree.insert(parent_iid, "end", text="Permission denied", iid=f"{parent_iid}::__denied__")
            return
        except Exception as e:
            self.tree.insert(parent_iid, "end", text=f"Error: {e}", iid=f"{parent_iid}::__error__")
            return

        for entry in entries:
            child_rel = safe_rel(entry, self.root_path)
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

    def _apply_expanded_state(self):
        ordered = sorted(self.expanded_paths, key=lambda x: x.count("/"))
        for rel in ordered:
            iid = make_iid(rel)
            if self.tree.exists(iid):
                self._ensure_loaded(iid)
                self.tree.item(iid, open=True)

    def _on_tree_open(self, _event=None):
        sel = self.tree.focus()
        if not sel:
            return
        self._ensure_loaded(sel)
        self.expanded_paths.add(sel)
        self._refresh_ascii_preview()
        self._update_tree_badge()

    def _on_tree_close(self, _event=None):
        sel = self.tree.focus()
        if not sel:
            return
        if sel != ".":
            self.expanded_paths.discard(sel)
        self._refresh_ascii_preview()
        self._update_tree_badge()

    def _on_tree_select(self, _event=None):
        sel = self.tree.selection()
        if not sel:
            return
        picked = sel[0]
        self.status_var.set(f"Selected: {picked}")

    def _collect_all_dirs(self, rel: Path = Path(".")) -> List[str]:
        abs_dir = (self.root_path / rel).resolve()
        out: List[str] = []
        if not abs_dir.exists() or not abs_dir.is_dir():
            return out

        try:
            entries = list_dir(abs_dir, ignore_hidden=not self.ignore_hidden.get())
        except Exception:
            return out

        for entry in entries:
            if entry.is_dir():
                child_rel = safe_rel(entry, self.root_path)
                out.append(child_rel)
                out.extend(self._collect_all_dirs(Path(child_rel)))
        return out

    def _count_visible_tree_nodes(self) -> int:
        def rec(items):
            total = 0
            for iid in items:
                total += 1
                total += rec(self.tree.get_children(iid))
            return total
        return rec(self.tree.get_children(""))

    def _update_tree_badge(self):
        count = self._count_visible_tree_nodes()
        self.tree_badge.set(f"{count} Nodes", fg=FG_MAIN, bg=BG_BADGE)

    def _expand_all(self):
        all_dirs = self._collect_all_dirs(Path("."))
        self.expanded_paths = {"."} | set(all_dirs)
        self._load_tree()
        self._refresh_ascii_preview()
        self.status_var.set("All folders expanded")

    def _collapse_all(self):
        self.expanded_paths = {"."}
        self._load_tree()
        self._refresh_ascii_preview()
        self.status_var.set("All folders collapsed")

    # ========================================================
    # OUTPUT
    # ========================================================

    def _refresh_ascii_preview(self):
        try:
            visible = build_visible_tree(
                root=self.root_path,
                rel=Path("."),
                expanded_paths=set(self.expanded_paths),
                ignore_hidden=not self.ignore_hidden.get(),
            )
            txt = render_ascii_tree(visible)
            line_count = max(0, len(txt.splitlines()))
            self.output_badge.set(f"{line_count} Lines", fg=FG_ACCENT, bg=BG_BADGE)
        except Exception as e:
            txt = f"ERROR: {e}\n"
            self.output_badge.set("Error", fg=FG_WARN, bg=BG_BADGE_WARN)

        self.preview.configure(state="normal")
        self.preview.delete("1.0", "end")
        self.preview.insert("1.0", txt)
        self.preview.configure(state="disabled")

    def _copy_ascii_to_clipboard(self):
        txt = self.preview.get("1.0", "end-1c")
        self.clipboard_clear()
        self.clipboard_append(txt)
        self.status_var.set("Visible tree copied to clipboard")

    def _export_ascii(self):
        try:
            self.output_path = Path(self.output_var.get()).expanduser().resolve()
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            txt = self.preview.get("1.0", "end-1c") + "\n"
            self.output_path.write_text(txt, encoding="utf-8")
            self.status_var.set(f"Exported: {self.output_path}")
            self.top_info_var.set(shorten_path(str(self.root_path), 110))
        except Exception as e:
            self.status_var.set(f"Export failed: {e}")

    # ========================================================
    # CONTROLS
    # ========================================================

    def _reload_from_input(self):
        try:
            new_root = Path(self.root_var.get()).expanduser().resolve()
            if not new_root.exists() or not new_root.is_dir():
                self.status_var.set(f"Invalid root: {new_root}")
                return

            self.root_path = new_root
            self.output_path = Path(self.output_var.get()).expanduser().resolve()
            self.output_path.parent.mkdir(parents=True, exist_ok=True)

            self.expanded_paths = {"."}
            self._load_tree()
            self._refresh_ascii_preview()
            self.status_var.set(f"Reloaded: {self.root_path}")
        except Exception as e:
            self.status_var.set(f"Reload failed: {e}")

    def _toggle_hidden(self):
        self.ignore_hidden.set(not self.ignore_hidden.get())
        self.hidden_btn.label.configure(
            text="Hidden: ON" if self.ignore_hidden.get() else "Hidden: OFF"
        )
        self._load_tree()
        self._refresh_ascii_preview()
        self.status_var.set("Visibility updated")

    def destroy(self):
        super().destroy()


# ============================================================
# STANDALONE
# ============================================================

class FolderExplorerDashboard(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Visual Folder")
        self.geometry("1650x980")
        self.minsize(1250, 760)
        self.configure(bg=BG_APP)

        self.panel = FolderExplorerPanel(self)
        self.panel.pack(fill="both", expand=True)

        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _on_close(self):
        try:
            self.panel.destroy()
        except Exception:
            pass
        self.destroy()


def main():
    app = FolderExplorerDashboard()
    app.mainloop()


if __name__ == "__main__":
    main()