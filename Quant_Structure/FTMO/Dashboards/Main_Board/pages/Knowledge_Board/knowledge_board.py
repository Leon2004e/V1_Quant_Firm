# -*- coding: utf-8 -*-
"""
Quant_Structure/FTMO/Dashboards/Main_Board/pages/Knowledge_Board/knowledge_board.py

Knowledge Board
- reduzierte FTMO-integrierbare Wissensseite
- Sidebar + Editor
- Notion-artige To-do-Zeilen mit Checkbox-Syntax
- kein Cursor-Sprung beim Schreiben
- besser lesbarer Editor
- Standalone startbar
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog


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

KNOWLEDGE_DIR = SCRIPT_PATH.parent
RUNTIME_DIR = KNOWLEDGE_DIR / "runtime"

WORKSPACE_DIR = FTMO_ROOT / "Data_Center" / "Workspace"
PAGES_DIR = WORKSPACE_DIR / "pages"
INDEX_FILE = WORKSPACE_DIR / "index.json"


# ============================================================
# THEME
# ============================================================

BG_APP = "#0B1118"
BG_TOP = "#0E1722"
BG_SURFACE = "#101923"
BG_SURFACE_2 = "#0F1A26"
BG_CARD = "#132131"
BG_EDITOR = "#0E1721"
BG_EDITOR_INNER = "#111C28"
BG_STATUS = "#0B131C"
BG_INPUT = "#122131"

BG_BUTTON = "#2563EB"
BG_BUTTON_HOVER = "#3B82F6"
BG_BUTTON_PRESSED = "#1D4ED8"

BG_BUTTON_SECONDARY = "#1A2A3B"
BG_BUTTON_SECONDARY_HOVER = "#22364C"
BG_BUTTON_SECONDARY_PRESSED = "#132131"

FG_MAIN = "#EAF2F9"
FG_MUTED = "#98AABC"
FG_SUBTLE = "#73879A"
FG_WHITE = "#FFFFFF"
FG_ACCENT = "#7AB8FF"
FG_TODO = "#DCEBFA"
FG_TODO_DONE = "#7D90A2"

BORDER = "#213244"
DIVIDER = "#192735"

FONT_TITLE = ("Segoe UI", 18, "bold")
FONT_TOP = ("Segoe UI", 10)
FONT_SECTION = ("Segoe UI", 11, "bold")
FONT_LABEL = ("Segoe UI", 9)
FONT_BUTTON = ("Segoe UI", 9, "bold")
FONT_TEXT = ("Segoe UI", 12)
FONT_EDITOR = ("Segoe UI", 13)
FONT_EDITOR_TITLE = ("Segoe UI", 16, "bold")


# ============================================================
# HELPERS
# ============================================================

TODO_UNCHECKED = "☐"
TODO_CHECKED = "☑"
INDENT = "    "


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def shorten_path(s: str, max_len: int = 100) -> str:
    if len(s) <= max_len:
        return s
    return "..." + s[-(max_len - 3):]


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def count_open_todos(text: str) -> int:
    return sum(1 for line in text.splitlines() if line.lstrip().startswith(TODO_UNCHECKED + " "))


def count_done_todos(text: str) -> int:
    return sum(1 for line in text.splitlines() if line.lstrip().startswith(TODO_CHECKED + " "))


def is_todo_line(line: str) -> bool:
    stripped = line.lstrip()
    return stripped.startswith(TODO_UNCHECKED + " ") or stripped.startswith(TODO_CHECKED + " ")


def todo_indent(line: str) -> str:
    return line[: len(line) - len(line.lstrip(" "))]


def toggle_todo_text(line: str) -> str:
    stripped = line.lstrip(" ")
    indent = todo_indent(line)

    if stripped.startswith(TODO_UNCHECKED + " "):
        return indent + TODO_CHECKED + stripped[1:]
    if stripped.startswith(TODO_CHECKED + " "):
        return indent + TODO_UNCHECKED + stripped[1:]
    return line


# ============================================================
# DATA MODEL
# ============================================================

@dataclass
class KnowledgePage:
    page_id: str
    title: str
    category: str
    status: str
    tags: List[str]
    due_date: str
    parent: str
    content: str
    sort_order: int
    created_at: str
    updated_at: str

    @classmethod
    def create(cls, page_id: str, title: str, sort_order: int, parent: str = "") -> "KnowledgePage":
        now = now_iso()
        return cls(
            page_id=page_id,
            title=title,
            category="note",
            status="active",
            tags=[],
            due_date="",
            parent=parent,
            content="",
            sort_order=sort_order,
            created_at=now,
            updated_at=now,
        )


# ============================================================
# REPOSITORY
# ============================================================

class KnowledgeRepository:
    def __init__(self) -> None:
        ensure_dir(RUNTIME_DIR)
        ensure_dir(WORKSPACE_DIR)
        ensure_dir(PAGES_DIR)

        if not INDEX_FILE.exists():
            self._write_index({"pages": []})

        self._migrate_index_if_needed()
        self._rebuild_index_from_files_if_missing()
        self._deduplicate_index()
        self.normalize_sort_order()

    def _read_index(self) -> Dict:
        try:
            payload = json.loads(INDEX_FILE.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return {"pages": []}
            payload.setdefault("pages", [])
            return payload
        except Exception:
            return {"pages": []}

    def _write_index(self, payload: Dict) -> None:
        payload = payload if isinstance(payload, dict) else {"pages": []}
        payload.setdefault("pages", [])
        INDEX_FILE.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def _migrate_index_if_needed(self) -> None:
        index_data = self._read_index()
        changed = False

        for i, item in enumerate(index_data.get("pages", [])):
            if "sort_order" not in item:
                item["sort_order"] = i
                changed = True

        if changed:
            self._write_index(index_data)

    def _rebuild_index_from_files_if_missing(self) -> None:
        index_data = self._read_index()
        indexed_ids = {str(item.get("page_id", "")) for item in index_data.get("pages", [])}
        changed = False

        for page_file in sorted(PAGES_DIR.glob("*.json")):
            try:
                payload = json.loads(page_file.read_text(encoding="utf-8"))
                page_id = str(payload.get("page_id", page_file.stem))
                if page_id in indexed_ids:
                    continue

                index_data.setdefault("pages", []).append({
                    "page_id": page_id,
                    "title": str(payload.get("title", page_id)),
                    "category": str(payload.get("category", "note")),
                    "status": str(payload.get("status", "active")),
                    "parent": str(payload.get("parent", "")),
                    "sort_order": len(index_data["pages"]),
                    "updated_at": str(payload.get("updated_at", "")),
                })
                indexed_ids.add(page_id)
                changed = True
            except Exception:
                continue

        if changed:
            self._write_index(index_data)

    def _deduplicate_index(self) -> None:
        index_data = self._read_index()
        seen = set()
        deduped = []
        changed = False

        for item in index_data.get("pages", []):
            page_id = str(item.get("page_id", ""))
            if not page_id:
                changed = True
                continue
            if page_id in seen:
                changed = True
                continue
            seen.add(page_id)
            deduped.append(item)

        if changed:
            index_data["pages"] = deduped
            self._write_index(index_data)

    def page_path(self, page_id: str) -> Path:
        return PAGES_DIR / f"{page_id}.json"

    def list_pages(self) -> List[Dict]:
        self._deduplicate_index()
        pages = self._read_index().get("pages", [])
        pages.sort(key=lambda x: (int(x.get("sort_order", 0)), str(x.get("title", "")).lower()))
        return pages

    def next_page_id(self) -> str:
        existing_ids = {str(item.get("page_id", "")) for item in self.list_pages()}
        n = 1
        while True:
            page_id = f"page_{n:04d}"
            if page_id not in existing_ids and not self.page_path(page_id).exists():
                return page_id
            n += 1

    def next_sort_order(self) -> int:
        pages = self.list_pages()
        if not pages:
            return 0
        return max(int(x.get("sort_order", 0)) for x in pages) + 1

    def load_page(self, page_id: str) -> KnowledgePage:
        payload = json.loads(self.page_path(page_id).read_text(encoding="utf-8"))
        if "sort_order" not in payload:
            payload["sort_order"] = 0

        payload.setdefault("category", "note")
        payload.setdefault("status", "active")
        payload.setdefault("tags", [])
        payload.setdefault("due_date", "")
        payload.setdefault("parent", "")
        payload.setdefault("content", "")
        payload.setdefault("created_at", now_iso())
        payload.setdefault("updated_at", now_iso())

        return KnowledgePage(**payload)

    def save_page(self, page: KnowledgePage) -> None:
        page.updated_at = now_iso()

        self.page_path(page.page_id).write_text(
            json.dumps(asdict(page), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        index_data = self._read_index()
        pages = index_data.setdefault("pages", [])
        found = False

        for item in pages:
            if str(item.get("page_id")) == page.page_id:
                item["title"] = page.title
                item["category"] = page.category
                item["status"] = page.status
                item["parent"] = page.parent
                item["sort_order"] = page.sort_order
                item["updated_at"] = page.updated_at
                found = True
                break

        if not found:
            pages.append({
                "page_id": page.page_id,
                "title": page.title,
                "category": page.category,
                "status": page.status,
                "parent": page.parent,
                "sort_order": page.sort_order,
                "updated_at": page.updated_at,
            })

        self._write_index(index_data)
        self._deduplicate_index()

    def create_page(self, title: str, parent: str = "") -> KnowledgePage:
        page = KnowledgePage.create(
            page_id=self.next_page_id(),
            title=title,
            sort_order=self.next_sort_order(),
            parent=parent,
        )
        self.save_page(page)
        self.normalize_sort_order()
        return page

    def delete_page(self, page_id: str) -> None:
        path = self.page_path(page_id)
        if path.exists():
            path.unlink()

        index_data = self._read_index()
        index_data["pages"] = [
            x for x in index_data.get("pages", [])
            if str(x.get("page_id")) != page_id
        ]
        self._write_index(index_data)
        self._deduplicate_index()
        self.normalize_sort_order()

    def normalize_sort_order(self) -> None:
        index_data = self._read_index()
        pages = index_data.get("pages", [])
        pages.sort(key=lambda x: int(x.get("sort_order", 0)))

        for i, item in enumerate(pages):
            item["sort_order"] = i
            try:
                path = self.page_path(str(item["page_id"]))
                if path.exists():
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    payload["sort_order"] = i
                    path.write_text(
                        json.dumps(payload, indent=2, ensure_ascii=False),
                        encoding="utf-8",
                    )
            except Exception:
                pass

        self._write_index(index_data)

    def move_page_up(self, page_id: str) -> None:
        index_data = self._read_index()
        pages = index_data.get("pages", [])
        pages.sort(key=lambda x: int(x.get("sort_order", 0)))

        idx = next((i for i, p in enumerate(pages) if str(p.get("page_id")) == page_id), None)
        if idx is None or idx == 0:
            return

        pages[idx]["sort_order"], pages[idx - 1]["sort_order"] = (
            pages[idx - 1]["sort_order"],
            pages[idx]["sort_order"],
        )
        self._write_index({"pages": pages})
        self.normalize_sort_order()

    def move_page_down(self, page_id: str) -> None:
        index_data = self._read_index()
        pages = index_data.get("pages", [])
        pages.sort(key=lambda x: int(x.get("sort_order", 0)))

        idx = next((i for i, p in enumerate(pages) if str(p.get("page_id")) == page_id), None)
        if idx is None or idx >= len(pages) - 1:
            return

        pages[idx]["sort_order"], pages[idx + 1]["sort_order"] = (
            pages[idx + 1]["sort_order"],
            pages[idx]["sort_order"],
        )
        self._write_index({"pages": pages})
        self.normalize_sort_order()


# ============================================================
# UI HELPERS
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

        header = tk.Frame(self, bg=BG_TOP, height=40)
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
# MAIN PANEL
# ============================================================

class KnowledgeBoardPanel(tk.Frame):
    AUTOSAVE_MS = 1200

    def __init__(
        self,
        parent,
        repo: Optional[KnowledgeRepository] = None,
        app=None,
        ftmo_root: Optional[Path] = None,
        root_path: Optional[Path] = None,
        **_kwargs,
    ):
        super().__init__(parent, bg=BG_APP)

        self.app = app
        self.ftmo_root = ftmo_root
        self.root_path = root_path

        self.repo = repo or KnowledgeRepository()

        self.current_page_id: Optional[str] = None
        self._autosave_job: Optional[str] = None
        self._loading_page = False
        self._last_saved_title = ""
        self._last_saved_content = ""

        self.search_var = tk.StringVar()
        self.title_var = tk.StringVar()

        self.top_info_var = tk.StringVar(value=shorten_path(str(WORKSPACE_DIR), 120))
        self.status_bar_var = tk.StringVar(value="Workspace ready")

        self._configure_ttk()
        self._build_ui()
        self._bind_events()
        self._load_pages()

    # --------------------------------------------------------
    # STYLE
    # --------------------------------------------------------

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
            rowheight=30,
            borderwidth=0,
            relief="flat",
            font=("Segoe UI", 10),
        )
        style.map(
            "Treeview",
            background=[("selected", "#1A2D43")],
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

        style.configure(
            "Vertical.TScrollbar",
            background=BG_TOP,
            troughcolor=BG_EDITOR,
            bordercolor=BG_EDITOR,
            arrowcolor=FG_MUTED,
            darkcolor=BG_TOP,
            lightcolor=BG_TOP,
        )

    # --------------------------------------------------------
    # BUILD UI
    # --------------------------------------------------------

    def _build_ui(self):
        self.rowconfigure(2, weight=1)
        self.columnconfigure(0, weight=1)

        topbar = tk.Frame(self, bg=BG_TOP, height=56, highlightbackground=BORDER, highlightthickness=1)
        topbar.grid(row=0, column=0, sticky="ew", padx=12, pady=(12, 10))
        topbar.grid_propagate(False)
        topbar.columnconfigure(1, weight=1)

        tk.Label(
            topbar,
            text="Knowledge Board",
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

        tk.Label(
            toolbar,
            text="Pages",
            font=FONT_LABEL,
            bg=BG_APP,
            fg=FG_MUTED,
        ).grid(row=0, column=0, sticky="w", padx=(0, 8))

        search_entry = tk.Entry(
            toolbar,
            textvariable=self.search_var,
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
        search_entry.grid(row=0, column=1, sticky="ew", padx=(0, 10), ipady=8)
        search_entry.bind("<KeyRelease>", lambda _e: self._load_pages())

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
                height=34,
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
                height=34,
            )
            b.pack(side="left", padx=(0, 6))
            return b

        pbtn("New Page", self._new_page, 96)
        pbtn("Save", self._save_current_page, 78)
        sbtn("To-do", self._insert_todo_line, 82)
        sbtn("Toggle", self._toggle_current_todo, 82)
        sbtn("Delete", self._delete_current_page, 82)
        sbtn("Move Up", self._move_current_up, 90)
        sbtn("Move Down", self._move_current_down, 104)

        content = tk.PanedWindow(self, orient="horizontal", sashwidth=6, bg=BG_APP, bd=0)
        content.grid(row=2, column=0, sticky="nsew", padx=12, pady=(0, 12))

        left = PanelFrame(content, "Sidebar")
        center = PanelFrame(content, "Editor")

        content.add(left, minsize=320)
        content.add(center, minsize=980)

        self._build_left(left.body)
        self._build_center(center.body)

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

    def _build_left(self, parent: tk.Frame):
        parent.rowconfigure(2, weight=1)
        parent.columnconfigure(0, weight=1)

        top = tk.Frame(parent, bg=BG_SURFACE)
        top.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 8))
        top.columnconfigure(0, weight=1)

        tk.Label(
            top,
            text="Your Pages",
            font=FONT_LABEL,
            bg=BG_SURFACE,
            fg=FG_SUBTLE,
        ).grid(row=0, column=0, sticky="w")

        self.pages_info_label = tk.Label(
            top,
            text="0 pages",
            font=FONT_LABEL,
            bg=BG_SURFACE,
            fg=FG_MUTED,
        )
        self.pages_info_label.grid(row=0, column=1, sticky="e")

        divider = tk.Frame(parent, bg=DIVIDER, height=1)
        divider.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 8))

        shell = tk.Frame(parent, bg=BG_SURFACE)
        shell.grid(row=2, column=0, sticky="nsew", padx=10, pady=(0, 10))
        shell.rowconfigure(0, weight=1)
        shell.columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(
            shell,
            columns=("meta",),
            show="tree headings",
            selectmode="browse",
        )
        self.tree.grid(row=0, column=0, sticky="nsew")

        self.tree.heading("#0", text="Page")
        self.tree.heading("meta", text="Open / Done")
        self.tree.column("#0", anchor="w", width=240, stretch=True)
        self.tree.column("meta", anchor="center", width=90, stretch=False)

        yscroll = ttk.Scrollbar(shell, orient="vertical", command=self.tree.yview)
        yscroll.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=yscroll.set)

        self.tree.bind("<<TreeviewSelect>>", self._on_select_page)

    def _build_center(self, parent: tk.Frame):
        parent.rowconfigure(3, weight=1)
        parent.columnconfigure(0, weight=1)

        head = tk.Frame(parent, bg=BG_SURFACE)
        head.grid(row=0, column=0, sticky="ew", padx=14, pady=(12, 8))
        head.columnconfigure(1, weight=1)

        tk.Label(
            head,
            text="Title",
            font=FONT_LABEL,
            bg=BG_SURFACE,
            fg=FG_SUBTLE,
        ).grid(row=0, column=0, sticky="w", padx=(0, 8))

        title_entry = tk.Entry(
            head,
            textvariable=self.title_var,
            bg=BG_INPUT,
            fg=FG_MAIN,
            insertbackground=FG_MAIN,
            relief="flat",
            bd=0,
            highlightthickness=1,
            highlightbackground=BORDER,
            highlightcolor=BORDER,
            font=FONT_EDITOR_TITLE,
        )
        title_entry.grid(row=0, column=1, sticky="ew", ipady=10)

        helper = tk.Label(
            parent,
            text="Shortcuts: Ctrl+L = To-do, Ctrl+Enter = Toggle, Tab = Einrücken, Backspace am Zeilenanfang = Ausrücken",
            font=FONT_LABEL,
            bg=BG_SURFACE,
            fg=FG_SUBTLE,
            anchor="w",
        )
        helper.grid(row=1, column=0, sticky="ew", padx=16, pady=(0, 8))

        divider = tk.Frame(parent, bg=DIVIDER, height=1)
        divider.grid(row=2, column=0, sticky="ew", padx=14, pady=(0, 10))

        editor_outer = tk.Frame(
            parent,
            bg=BG_EDITOR_INNER,
            highlightbackground=BORDER,
            highlightthickness=1,
            bd=0,
        )
        editor_outer.grid(row=3, column=0, sticky="nsew", padx=14, pady=(0, 14))
        editor_outer.rowconfigure(0, weight=1)
        editor_outer.columnconfigure(0, weight=1)

        self.editor = tk.Text(
            editor_outer,
            wrap="word",
            undo=True,
            autoseparators=True,
            maxundo=-1,
            bg=BG_EDITOR,
            fg=FG_MAIN,
            insertbackground=FG_WHITE,
            insertwidth=2,
            selectbackground="#244A73",
            selectforeground=FG_WHITE,
            relief="flat",
            bd=0,
            highlightthickness=0,
            font=FONT_EDITOR,
            padx=26,
            pady=24,
            spacing1=5,
            spacing2=7,
            spacing3=7,
        )
        self.editor.grid(row=0, column=0, sticky="nsew")

        yscroll = ttk.Scrollbar(editor_outer, orient="vertical", command=self.editor.yview)
        yscroll.grid(row=0, column=1, sticky="ns")
        self.editor.configure(yscrollcommand=yscroll.set)

        self.editor.tag_configure("todo_open", foreground=FG_TODO, lmargin1=18, lmargin2=42)
        self.editor.tag_configure("todo_done", foreground=FG_TODO_DONE, overstrike=1, lmargin1=18, lmargin2=42)
        self.editor.tag_configure("normal_text", foreground=FG_MAIN, lmargin1=6, lmargin2=6)

    # --------------------------------------------------------
    # EVENTS
    # --------------------------------------------------------

    def _bind_events(self):
        self.title_var.trace_add("write", self._queue_autosave_var)

        self.editor.bind("<KeyRelease>", self._on_editor_key_release)
        self.editor.bind("<Return>", self._handle_return)
        self.editor.bind("<Tab>", self._indent_selection_or_line)
        self.editor.bind("<BackSpace>", self._handle_backspace_outdent)
        self.editor.bind("<Control-Return>", self._toggle_current_todo_event)
        self.editor.bind("<Control-KP_Enter>", self._toggle_current_todo_event)
        self.editor.bind("<Control-l>", self._insert_todo_event)
        self.editor.bind("<ButtonRelease-1>", self._on_editor_click_release)

    # --------------------------------------------------------
    # PAGE LIST
    # --------------------------------------------------------

    def _page_meta_text(self, page: KnowledgePage) -> str:
        open_count = count_open_todos(page.content)
        done_count = count_done_todos(page.content)
        if open_count == 0 and done_count == 0:
            return "-"
        return f"{open_count}/{done_count}"

    def _load_pages(self):
        query = self.search_var.get().strip().lower()
        selected = self.current_page_id

        self.tree.delete(*self.tree.get_children())
        pages = self.repo.list_pages()
        visible_count = 0

        for meta in pages:
            page_id = str(meta.get("page_id", ""))
            title = str(meta.get("title", "Untitled"))

            if not page_id:
                continue
            if query and query not in title.lower():
                continue

            try:
                page = self.repo.load_page(page_id)
                meta_text = self._page_meta_text(page)
            except Exception:
                meta_text = "-"

            self.tree.insert(
                "",
                "end",
                iid=page_id,
                text=title,
                values=(meta_text,),
            )
            visible_count += 1

        self.pages_info_label.configure(text=f"{visible_count} pages")

        if selected and self.tree.exists(selected):
            self.tree.selection_set(selected)
            self.tree.focus(selected)
            self.tree.see(selected)

    def _refresh_sidebar_row(self, page: KnowledgePage):
        if not self.tree.exists(page.page_id):
            self._load_pages()
            return

        self.tree.item(
            page.page_id,
            text=page.title,
            values=(self._page_meta_text(page),),
        )

    # --------------------------------------------------------
    # LOAD / SAVE
    # --------------------------------------------------------

    def _on_select_page(self, _event=None):
        selected = self.tree.selection()
        if not selected:
            return
        self._load_page(selected[0])

    def _load_page(self, page_id: str):
        try:
            page = self.repo.load_page(page_id)
        except Exception as e:
            self._set_status(f"Load failed: {e}")
            return

        self._loading_page = True
        try:
            self.current_page_id = page.page_id
            self.title_var.set(page.title)

            self.editor.delete("1.0", "end")
            self.editor.insert("1.0", page.content)

            self._last_saved_title = page.title
            self._last_saved_content = page.content

            self._refresh_editor_tags()
            self.editor.mark_set("insert", "1.0")
            self.editor.see("insert")
        finally:
            self._loading_page = False

        self._set_status(f"Open page: {page.title}")

    def _build_page_from_ui(self) -> KnowledgePage:
        if not self.current_page_id:
            raise RuntimeError("No page selected.")

        page = self.repo.load_page(self.current_page_id)
        page.title = self.title_var.get().strip() or "Untitled"
        page.content = self.editor.get("1.0", "end-1c")
        return page

    def _save_current_page(self):
        if not self.current_page_id:
            messagebox.showinfo("Info", "No page selected.")
            return

        try:
            page = self._build_page_from_ui()
            self.repo.save_page(page)
            self._last_saved_title = page.title
            self._last_saved_content = page.content
            self._refresh_sidebar_row(page)
            self._set_status(f"Saved: {page.title}")
        except Exception as e:
            self._set_status(f"Save failed: {e}")

    def _save_current_page_silent(self):
        if not self.current_page_id:
            return

        page = self._build_page_from_ui()
        self.repo.save_page(page)
        self._last_saved_title = page.title
        self._last_saved_content = page.content
        self._refresh_sidebar_row(page)

    # --------------------------------------------------------
    # PAGE ACTIONS
    # --------------------------------------------------------

    def _new_page(self):
        title = simpledialog.askstring("New Page", "Titel der neuen Seite:", parent=self)
        if not title:
            return

        clean_title = title.strip()
        if not clean_title:
            return

        page = self.repo.create_page(title=clean_title)
        self._load_pages()

        if self.tree.exists(page.page_id):
            self.tree.selection_set(page.page_id)
            self.tree.focus(page.page_id)
            self.tree.see(page.page_id)

        self._load_page(page.page_id)

    def _delete_current_page(self):
        if not self.current_page_id:
            self._set_status("No page selected")
            return

        try:
            page = self.repo.load_page(self.current_page_id)
        except Exception as e:
            self._set_status(f"Delete failed: {e}")
            return

        ok = messagebox.askyesno("Delete", f"Seite wirklich löschen?\n\n{page.title}")
        if not ok:
            return

        self.repo.delete_page(self.current_page_id)
        self.current_page_id = None

        self._loading_page = True
        try:
            self.title_var.set("")
            self.editor.delete("1.0", "end")
            self._last_saved_title = ""
            self._last_saved_content = ""
        finally:
            self._loading_page = False

        self._load_pages()
        self._set_status("Page deleted")

    def _move_current_up(self):
        if not self.current_page_id:
            self._set_status("No page selected")
            return

        self.repo.move_page_up(self.current_page_id)
        current = self.current_page_id
        self._load_pages()
        if self.tree.exists(current):
            self.tree.selection_set(current)
            self.tree.focus(current)
            self.tree.see(current)
        self._set_status("Page moved up")

    def _move_current_down(self):
        if not self.current_page_id:
            self._set_status("No page selected")
            return

        self.repo.move_page_down(self.current_page_id)
        current = self.current_page_id
        self._load_pages()
        if self.tree.exists(current):
            self.tree.selection_set(current)
            self.tree.focus(current)
            self.tree.see(current)
        self._set_status("Page moved down")

    # --------------------------------------------------------
    # AUTOSAVE
    # --------------------------------------------------------

    def _queue_autosave_var(self, *_args):
        self._queue_autosave()

    def _on_editor_key_release(self, _event=None):
        if self._loading_page:
            return
        self._refresh_editor_tags()
        self._queue_autosave()

    def _queue_autosave(self, _event=None):
        if self._loading_page or self.current_page_id is None:
            return

        if self._autosave_job is not None:
            try:
                self.after_cancel(self._autosave_job)
            except Exception:
                pass

        self._autosave_job = self.after(self.AUTOSAVE_MS, self._autosave)

    def _autosave(self):
        self._autosave_job = None
        try:
            if self.current_page_id is None:
                return

            title_now = self.title_var.get().strip() or "Untitled"
            content_now = self.editor.get("1.0", "end-1c")

            if title_now == self._last_saved_title and content_now == self._last_saved_content:
                return

            self._save_current_page_silent()
            self._set_status(f"Autosaved: {title_now}")
        except Exception as e:
            self._set_status(f"Autosave error: {e}")

    # --------------------------------------------------------
    # TODO / EDITOR HELPERS
    # --------------------------------------------------------

    def _insert_todo_event(self, _event=None):
        self._insert_todo_line()
        return "break"

    def _toggle_current_todo_event(self, _event=None):
        self._toggle_current_todo()
        return "break"

    def _current_line_bounds(self):
        idx = self.editor.index("insert")
        line_start = f"{idx} linestart"
        line_end = f"{idx} lineend"
        return line_start, line_end

    def _current_line_text(self) -> str:
        line_start, line_end = self._current_line_bounds()
        return self.editor.get(line_start, line_end)

    def _replace_current_line(self, new_text: str):
        line_start, line_end = self._current_line_bounds()
        self.editor.delete(line_start, line_end)
        self.editor.insert(line_start, new_text)

    def _insert_todo_line(self):
        if self.current_page_id is None:
            return

        try:
            has_selection = bool(self.editor.tag_ranges("sel"))
        except Exception:
            has_selection = False

        if has_selection:
            start = self.editor.index("sel.first")
            end = self.editor.index("sel.last")
            start_line = int(start.split(".")[0])
            end_line = int(end.split(".")[0])

            for line_no in range(start_line, end_line + 1):
                line_start = f"{line_no}.0"
                line_end = f"{line_no}.end"
                text = self.editor.get(line_start, line_end)
                if not is_todo_line(text):
                    self.editor.insert(line_start, f"{TODO_UNCHECKED} ")

            self._refresh_editor_tags()
            self._queue_autosave()
            return

        line_text = self._current_line_text()
        if not line_text.strip():
            self._replace_current_line(f"{TODO_UNCHECKED} ")
            self.editor.mark_set("insert", "insert lineend")
        else:
            self.editor.insert("insert linestart", f"{TODO_UNCHECKED} ")

        self._refresh_editor_tags()
        self._queue_autosave()
        self.editor.focus_set()

    def _toggle_current_todo(self):
        if self.current_page_id is None:
            return

        try:
            has_selection = bool(self.editor.tag_ranges("sel"))
        except Exception:
            has_selection = False

        if has_selection:
            start = self.editor.index("sel.first")
            end = self.editor.index("sel.last")
            start_line = int(start.split(".")[0])
            end_line = int(end.split(".")[0])

            for line_no in range(start_line, end_line + 1):
                line_start = f"{line_no}.0"
                line_end = f"{line_no}.end"
                text = self.editor.get(line_start, line_end)
                if is_todo_line(text):
                    self.editor.delete(line_start, line_end)
                    self.editor.insert(line_start, toggle_todo_text(text))

            self._refresh_editor_tags()
            self._queue_autosave()
            return

        text = self._current_line_text()
        if not is_todo_line(text):
            return

        line_start, line_end = self._current_line_bounds()
        self.editor.delete(line_start, line_end)
        self.editor.insert(line_start, toggle_todo_text(text))
        self._refresh_editor_tags()
        self._queue_autosave()
        self.editor.focus_set()

    def _handle_return(self, _event=None):
        line_text = self._current_line_text()
        current_insert = self.editor.index("insert")

        if is_todo_line(line_text):
            indent = todo_indent(line_text)
            stripped = line_text.lstrip(" ")

            if stripped in (f"{TODO_UNCHECKED} ", f"{TODO_CHECKED} "):
                self._replace_current_line(indent)
                self.editor.mark_set("insert", f"{current_insert} linestart + {len(indent)}c")
                self._refresh_editor_tags()
                self._queue_autosave()
                return "break"

            self.editor.insert("insert", f"\n{indent}{TODO_UNCHECKED} ")
            self._refresh_editor_tags()
            self._queue_autosave()
            return "break"

        self.editor.insert("insert", "\n")
        self._refresh_editor_tags()
        self._queue_autosave()
        return "break"

    def _indent_selection_or_line(self, _event=None):
        try:
            has_selection = bool(self.editor.tag_ranges("sel"))
        except Exception:
            has_selection = False

        if has_selection:
            start = self.editor.index("sel.first")
            end = self.editor.index("sel.last")
            start_line = int(start.split(".")[0])
            end_line = int(end.split(".")[0])

            for line_no in range(start_line, end_line + 1):
                self.editor.insert(f"{line_no}.0", INDENT)
        else:
            self.editor.insert("insert linestart", INDENT)

        self._refresh_editor_tags()
        self._queue_autosave()
        return "break"

    def _handle_backspace_outdent(self, _event=None):
        insert_idx = self.editor.index("insert")
        line_no, col_no = insert_idx.split(".")
        col_no = int(col_no)
        line_start = f"{line_no}.0"
        line_text = self.editor.get(line_start, f"{line_no}.end")

        if col_no == 0:
            return None

        before_cursor = line_text[:col_no]

        if before_cursor.isspace():
            remove_n = min(len(INDENT), len(before_cursor))
            self.editor.delete(f"{insert_idx} - {remove_n}c", insert_idx)
            self._refresh_editor_tags()
            self._queue_autosave()
            return "break"

        return None

    def _on_editor_click_release(self, event=None):
        if event is not None:
            idx = self.editor.index(f"@{event.x},{event.y}")
            line_start = f"{idx} linestart"
            line_end = f"{idx} lineend"
            line_text = self.editor.get(line_start, line_end)

            if is_todo_line(line_text):
                stripped = line_text.lstrip(" ")
                indent_len = len(line_text) - len(stripped)
                col = int(idx.split(".")[1])

                # Klick im Checkbox-Bereich
                if indent_len <= col <= indent_len + 2:
                    self.editor.mark_set("insert", idx)
                    self._toggle_current_todo()
                    return "break"

        self.after_idle(self._refresh_editor_tags)
        return None

    # --------------------------------------------------------
    # VISUAL TAGGING
    # --------------------------------------------------------

    def _refresh_editor_tags(self):
        self.editor.tag_remove("todo_open", "1.0", "end")
        self.editor.tag_remove("todo_done", "1.0", "end")
        self.editor.tag_remove("normal_text", "1.0", "end")

        total_lines = int(self.editor.index("end-1c").split(".")[0])

        for line_no in range(1, total_lines + 1):
            line_start = f"{line_no}.0"
            line_end = f"{line_no}.end"
            text = self.editor.get(line_start, line_end)

            if text.lstrip().startswith(TODO_UNCHECKED + " "):
                self.editor.tag_add("todo_open", line_start, line_end)
            elif text.lstrip().startswith(TODO_CHECKED + " "):
                self.editor.tag_add("todo_done", line_start, line_end)
            else:
                self.editor.tag_add("normal_text", line_start, line_end)

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    def _set_status(self, text: str):
        self.status_bar_var.set(text)
        self.top_info_var.set(shorten_path(str(WORKSPACE_DIR), 120))

    def destroy(self):
        if self._autosave_job is not None:
            try:
                self.after_cancel(self._autosave_job)
            except Exception:
                pass
            self._autosave_job = None
        super().destroy()


# ============================================================
# STANDALONE APP
# ============================================================

class KnowledgeBoardApp(tk.Tk):
    def __init__(self, repo: Optional[KnowledgeRepository] = None):
        super().__init__()
        self.title("Knowledge Board")
        self.geometry("1700x980")
        self.minsize(1250, 760)
        self.configure(bg=BG_APP)

        self.panel = KnowledgeBoardPanel(self, repo=repo)
        self.panel.pack(fill="both", expand=True)

    def destroy(self):
        try:
            self.panel.destroy()
        except Exception:
            pass
        super().destroy()


def main() -> None:
    app = KnowledgeBoardApp()
    app.mainloop()


if __name__ == "__main__":
    main()