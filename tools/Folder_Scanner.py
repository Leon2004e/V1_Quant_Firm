# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Button, Footer, Header, Input, Static, Tree
from textual.widgets.tree import TreeNode


# ===========================
# Textual API drift helpers
# ===========================
def wipe_children(node: TreeNode) -> None:
    """Remove all children from a TreeNode across Textual versions."""
    if hasattr(node, "remove_children"):
        node.remove_children()  # type: ignore[attr-defined]
        return

    ch = node.children
    if hasattr(ch, "clear"):
        ch.clear()  # type: ignore[attr-defined]
        return

    for child in list(ch):
        if hasattr(child, "remove"):
            child.remove()  # type: ignore[attr-defined]
        elif hasattr(node, "remove"):
            node.remove(child)  # type: ignore[misc]


def first_child(node: TreeNode):
    try:
        return next(iter(node.children), None)
    except Exception:
        return None


# ===========================
# Visible Tree Snapshot Model
# ===========================
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
    """
    Build EXACT visible tree:
    - list only direct children of rel
    - recurse ONLY into folders that are expanded (present in expanded_paths)
    """
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
    """Render VisibleNode tree as ASCII tree."""
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


# ===========================
# Path helpers
# ===========================
def project_root_from_tools(script_path: Path, levels_up_from_tools: int = 1) -> Path:
    """
    Script: <project>/tools/Visual_Folder.py
    tools_dir = <project>/tools
    levels_up_from_tools=1 => <project>   (DEFAULT)
    levels_up_from_tools=2 => parent(<project>)
    """
    tools_dir = script_path.resolve().parent
    p = tools_dir
    n = max(1, int(levels_up_from_tools))
    for _ in range(n):
        p = p.parent
    return p


def default_output_path(script_path: Path) -> Path:
    """
    Always write export to tools/output/visible_tree.txt (independent from scanned root).
    """
    tools_dir = script_path.resolve().parent
    out_dir = tools_dir / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / "visible_tree.txt"


# ===========================
# UI App
# ===========================
class TreeExplorer(App):
    CSS = """
    Screen { layout: vertical; }
    #top { height: 3; }
    #main { height: 1fr; }
    #status { height: 3; }
    Tree { width: 1fr; height: 1fr; }
    #right { width: 60; }
    Input { width: 1fr; }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("e", "export_visible_tree", "Export visible tree"),
        ("r", "reload_tree", "Reload"),
    ]

    def __init__(self, root: Path, out_path: Optional[Path] = None):
        super().__init__()
        self.root = root.resolve()
        self.expanded_paths: set[str] = set()
        self._flags = {"ignore_hidden": True}
        self.out_path = (out_path or default_output_path(Path(__file__))).resolve()
        self.out_path.parent.mkdir(parents=True, exist_ok=True)

    def compose(self) -> ComposeResult:
        yield Header()

        with Horizontal(id="top"):
            yield Static(f"Root: {self.root}", id="root_label")
            yield Input(value=str(self.root), placeholder="Root path...", id="root_input")
            yield Button("Reload (r)", id="reload_btn")

        with Horizontal(id="main"):
            yield Tree("📁 .", id="tree")

            with Vertical(id="right"):
                yield Static("Options:", id="opt_label")
                yield Button("ignore hidden: ON", id="opt_hidden")
                yield Static("", id="export_hint")
                yield Button("Export visible tree (e)", id="export_btn")
                yield Static("", id="out_label")

        yield Static("Ready.", id="status")
        yield Footer()

    def on_mount(self) -> None:
        self.expanded_paths = {"."}
        self.query_one("#out_label", Static).update(f"Output: {self.out_path}")
        self._reload_tree_content()

    def _set_status(self, msg: str) -> None:
        self.query_one("#status", Static).update(msg)

    def _tree(self) -> Tree:
        return self.query_one("#tree", Tree)

    def _populate_node(self, node: TreeNode, rel: Path) -> None:
        abs_dir = self.root / rel
        if not abs_dir.exists() or not abs_dir.is_dir():
            return

        try:
            entries = list(abs_dir.iterdir())
        except PermissionError:
            node.add("⛔ Permission denied", data={"rel": rel.as_posix()})
            return

        if self._flags["ignore_hidden"]:
            entries = [e for e in entries if not e.name.startswith(".")]
        entries.sort(key=lambda p: (not p.is_dir(), p.name.lower()))

        for e in entries:
            child_rel = rel / e.name
            if e.is_dir():
                child = node.add(f"{e.name}", data={"rel": child_rel.as_posix()})
                child.add("…", data={"placeholder": True})
            else:
                node.add(f"{e.name}", data={"rel": child_rel.as_posix()})

    def _reload_tree_content(self) -> None:
        tree = self._tree()
        tree.root.label = "."
        tree.root.data = {"rel": "."}
        wipe_children(tree.root)
        self._populate_node(tree.root, Path("."))
        tree.root.expand()
        self._set_status("Tree loaded.")

    def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:
        node = event.node
        rel = str(node.data.get("rel", "."))

        fc = first_child(node)
        if fc is not None and getattr(fc, "data", None) and fc.data.get("placeholder"):
            wipe_children(node)
            self._populate_node(node, Path(rel))

        self.expanded_paths.add(rel)

    def on_tree_node_collapsed(self, event: Tree.NodeCollapsed) -> None:
        node = event.node
        rel = str(node.data.get("rel", "."))
        if rel != ".":
            self.expanded_paths.discard(rel)

    def action_reload_tree(self) -> None:
        root_input = self.query_one("#root_input", Input).value.strip()
        p = Path(root_input).expanduser().resolve()
        if not p.exists() or not p.is_dir():
            self._set_status(f"Invalid root: {p}")
            return

        self.root = p
        self.expanded_paths = {"."}
        self.query_one("#root_label", Static).update(f"Root: {self.root}")
        self._reload_tree_content()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id
        if bid == "reload_btn":
            self.action_reload_tree()
        elif bid == "export_btn":
            self.action_export_visible_tree()
        elif bid == "opt_hidden":
            self._flags["ignore_hidden"] = not self._flags["ignore_hidden"]
            event.button.label = "ignore hidden: ON" if self._flags["ignore_hidden"] else "ignore hidden: OFF"
            self._reload_tree_content()

    def action_export_visible_tree(self) -> None:
        try:
            visible = build_visible_tree(
                root=self.root,
                rel=Path("."),
                expanded_paths=set(self.expanded_paths),
                ignore_hidden=self._flags["ignore_hidden"],
            )
            txt = render_ascii_tree(visible)

            self.out_path.parent.mkdir(parents=True, exist_ok=True)
            self.out_path.write_text(txt, encoding="utf-8")

            self.query_one("#out_label", Static).update(f"Output: {self.out_path}")
            self._set_status(f"Exported: {self.out_path}")
        except Exception as e:
            self._set_status(f"Export failed: {e}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Visual folder explorer (Textual) + export visible tree.")
    p.add_argument(
        "root",
        nargs="?",
        default="",
        help="Root folder to browse. Default: project root (parent of tools/).",
    )
    p.add_argument(
        "--project-up",
        type=int,
        default=1,
        help="If root not given: go N levels up from tools/ to select project root. Default: 1.",
    )
    p.add_argument(
        "--out",
        type=str,
        default="",
        help="Export output file path. Default: tools/output/visible_tree.txt",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    script_path = Path(__file__).resolve()

    # Default root = parent of tools/ (project root) even if script runs from tools/
    if args.root.strip():
        root = Path(args.root).expanduser().resolve()
    else:
        root = project_root_from_tools(script_path, levels_up_from_tools=int(args.project_up))

    out_path = Path(args.out).expanduser().resolve() if args.out.strip() else default_output_path(script_path)

    if not root.exists() or not root.is_dir():
        raise FileNotFoundError(f"Invalid root: {root}")

    TreeExplorer(root=root, out_path=out_path).run()


if __name__ == "__main__":
    main()