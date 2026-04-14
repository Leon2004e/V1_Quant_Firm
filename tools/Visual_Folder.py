# -*- coding: utf-8 -*-
"""
diagram_tree_pro.py
Corporate folder diagram viewer for large project structures.

Features
- LEFT -> RIGHT diagram
- clean corporate UI
- smart auto collapse
- folders-first overview
- files toggle in frontend
- folder metrics (dirs/files/direct children/depth)
- optional child compression with "_more (...)"
- sidebar hotspots
- search + focus
- overlay support (virtual nodes)
- output independent from scanned root

Usage examples
--------------
1) Nur Ordner, geringe Tiefe:
   python diagram_tree_pro.py --root /path/to/project --max-depth 4 --folders-only

2) Mit Dateien:
   python diagram_tree_pro.py --root /path/to/project --max-depth 5

3) Auto-root:
   python diagram_tree_pro.py --auto-root --title "Quant Platform"

4) Mehr Verdichtung:
   python diagram_tree_pro.py --root /path/to/project --max-depth 6 --max-children 10 --folders-only
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


DEFAULT_IGNORE_DIRS = {
    ".git", ".svn", ".hg",
    "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache",
    ".idea", ".vscode",
    "venv", ".venv", "env", ".env",
    "node_modules",
    "dist", "build",
    ".next", ".turbo",
    ".DS_Store",
}

DEFAULT_IGNORE_FILES_REGEX = [
    r".*\.pyc$",
    r".*\.pyo$",
    r".*\.log$",
    r".*\.tmp$",
    r".*\.bak$",
]

DEFAULT_INCLUDE_EXTS = {
    ".py", ".md", ".txt", ".json", ".csv", ".xlsx", ".yml", ".yaml",
    ".ipynb", ".toml", ".ini", ".cfg", ".sql", ".sh", ".parquet"
}

ROOT_MARKERS = ["1.Data_Center", "1_Data_Center", "2.Analyse_Center", "3.Control_Panel"]

DEFAULT_COLLAPSE_HINT_DIRS = {
    "archive", "archives", "legacy", "old", "deprecated", "tmp", "temp",
    "logs", "exports", "results", "snapshots", "checkpoints", "cache",
    "raw_exports", "debug", "artifacts"
}

DEFAULT_PRIORITY_HINT_DIRS = {
    "data", "research", "alpha", "signals", "features", "models", "portfolio",
    "execution", "risk", "monitoring", "reporting", "backtest", "backtests",
    "live", "prod", "src", "pipelines", "infra", "core", "ops"
}


@dataclass
class Node:
    name: str
    path: str
    is_dir: bool
    children: List["Node"] = field(default_factory=list)

    # metrics
    file_count: int = 0
    dir_count: int = 0
    depth: int = 0
    direct_children_count: int = 0

    # display hints
    collapsed_hint: bool = False
    priority_hint: bool = False
    hidden_children_count: int = 0
    is_more_node: bool = False


def should_ignore_dir(p: Path, ignore_dirs: Set[str]) -> bool:
    return p.name in ignore_dirs


def should_ignore_file(p: Path, ignore_file_patterns: List[re.Pattern]) -> bool:
    return any(rx.match(p.name) for rx in ignore_file_patterns)


def has_any_marker(p: Path) -> bool:
    return any((p / marker).exists() for marker in ROOT_MARKERS)


def find_best_root(start: Path, max_search_depth: int = 4) -> Optional[Path]:
    start = start.resolve()
    if has_any_marker(start):
        return start

    frontier: List[Tuple[Path, int]] = [(start, 0)]
    seen: Set[Path] = {start}

    while frontier:
        cur, d = frontier.pop(0)
        if d >= max_search_depth:
            continue

        try:
            for child in cur.iterdir():
                if not child.is_dir():
                    continue
                if child in seen:
                    continue
                seen.add(child)

                if should_ignore_dir(child, DEFAULT_IGNORE_DIRS):
                    continue

                if has_any_marker(child):
                    return child

                frontier.append((child, d + 1))
        except Exception:
            continue

    return None


def build_tree(
    root: Path,
    base: Path,
    max_depth: int,
    include_files: bool,
    include_exts: Set[str],
    ignore_dirs: Set[str],
    ignore_file_patterns: List[re.Pattern],
) -> Node:
    root = root.resolve()
    base = base.resolve()

    def rel(p: Path) -> str:
        try:
            return str(p.resolve().relative_to(base)).replace("\\", "/")
        except Exception:
            return p.name

    def walk(cur: Path, depth: int) -> Node:
        node = Node(
            name=cur.name,
            path=rel(cur),
            is_dir=cur.is_dir(),
            children=[],
            depth=depth,
        )

        if node.name.lower() in DEFAULT_COLLAPSE_HINT_DIRS:
            node.collapsed_hint = True
        if node.name.lower() in DEFAULT_PRIORITY_HINT_DIRS:
            node.priority_hint = True

        if depth >= max_depth or not cur.is_dir():
            return node

        try:
            entries = sorted(cur.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
        except Exception:
            return node

        for e in entries:
            if e.is_dir():
                if should_ignore_dir(e, ignore_dirs):
                    continue

                child = walk(e, depth + 1)

                if e.name.lower() in DEFAULT_COLLAPSE_HINT_DIRS:
                    child.collapsed_hint = True
                if e.name.lower() in DEFAULT_PRIORITY_HINT_DIRS:
                    child.priority_hint = True

                node.children.append(child)
            else:
                if not include_files:
                    continue
                if should_ignore_file(e, ignore_file_patterns):
                    continue
                if include_exts and e.suffix.lower() not in include_exts:
                    continue

                child = Node(
                    name=e.name,
                    path=rel(e),
                    is_dir=False,
                    children=[],
                    depth=depth + 1,
                )
                node.children.append(child)

        node.direct_children_count = len(node.children)
        return node

    return walk(root, 0)


def enrich_counts(node: Node, depth: int = 0) -> Node:
    node.depth = depth

    if not node.is_dir:
        node.file_count = 1
        node.dir_count = 0
        node.direct_children_count = 0
        return node

    total_files = 0
    total_dirs = 0
    node.direct_children_count = len(node.children)

    for child in node.children:
        enrich_counts(child, depth + 1)
        if child.is_dir:
            total_dirs += 1 + child.dir_count
            total_files += child.file_count
        else:
            total_files += 1

    node.file_count = total_files
    node.dir_count = total_dirs

    if len(node.children) > 18:
        node.collapsed_hint = True

    return node


def compress_children(node: Node, max_children: int) -> Node:
    if not node.is_dir:
        return node

    for child in node.children:
        compress_children(child, max_children)

    if len(node.children) > max_children:
        visible = node.children[:max_children]
        hidden = node.children[max_children:]

        more_node = Node(
            name=f"_more ({len(hidden)} hidden)",
            path=f"{node.path}/_more",
            is_dir=True,
            children=hidden,
            collapsed_hint=True,
            is_more_node=True,
            hidden_children_count=len(hidden),
        )
        enrich_counts(more_node, node.depth + 1)

        node.children = visible + [more_node]
        node.hidden_children_count = len(hidden)

    node.direct_children_count = len(node.children)
    return node


def collect_hotspots(node: Node, stats: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    if stats is None:
        stats = []

    if node.is_dir:
        stats.append({
            "name": node.name,
            "path": node.path,
            "depth": node.depth,
            "file_count": node.file_count,
            "dir_count": node.dir_count,
            "direct_children_count": node.direct_children_count,
            "collapsed_hint": node.collapsed_hint,
            "priority_hint": node.priority_hint,
            "is_more_node": node.is_more_node,
        })

    for child in node.children:
        collect_hotspots(child, stats)

    return stats


def node_to_dict(n: Node) -> Dict[str, Any]:
    return {
        "name": n.name,
        "path": n.path,
        "is_dir": n.is_dir,
        "file_count": n.file_count,
        "dir_count": n.dir_count,
        "depth": n.depth,
        "direct_children_count": n.direct_children_count,
        "collapsed_hint": n.collapsed_hint,
        "priority_hint": n.priority_hint,
        "hidden_children_count": n.hidden_children_count,
        "is_more_node": n.is_more_node,
        "children": [node_to_dict(c) for c in n.children],
    }


HTML_TEMPLATE = r"""<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Corporate Structure Viewer</title>
  <style>
    :root{
      --bg:#0b1220;
      --bg2:#0f172a;
      --panel:#111b31;
      --panel2:#14213d;
      --panel3:#0e1a30;
      --text:#e5e7eb;
      --muted:#94a3b8;
      --line:#23314f;
      --line2:#2d3f66;
      --accent:#4f8cff;
      --accent2:#7aa2ff;
      --danger:#ff6b6b;
      --success:#34d399;
      --warn:#f59e0b;
      --folder:#10213f;
      --file:#151c2d;
      --shadow:0 8px 24px rgba(0,0,0,0.25);
      --radius:14px;
    }

    *{ box-sizing:border-box; }

    html,body{
      margin:0;
      height:100%;
      background:
        radial-gradient(circle at 15% 20%, rgba(79,140,255,0.08), transparent 30%),
        linear-gradient(180deg, #0a1020 0%, #0b1220 100%);
      color:var(--text);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
    }

    .app{
      display:grid;
      grid-template-columns: 340px 1fr;
      grid-template-rows: auto 1fr;
      height:100vh;
      gap:0;
    }

    .topbar{
      grid-column:1 / span 2;
      display:flex;
      flex-wrap:wrap;
      gap:10px;
      align-items:center;
      padding:12px 14px;
      border-bottom:1px solid var(--line);
      background:linear-gradient(180deg, rgba(20,33,61,0.94), rgba(11,18,32,0.96));
      backdrop-filter: blur(10px);
      position:sticky;
      top:0;
      z-index:20;
    }

    .brand{
      display:flex;
      align-items:center;
      gap:10px;
      min-width:220px;
      padding-right:8px;
    }

    .brandMark{
      width:12px;
      height:12px;
      border-radius:999px;
      background:linear-gradient(180deg, var(--accent2), var(--accent));
      box-shadow:0 0 18px rgba(79,140,255,0.55);
      flex:0 0 auto;
    }

    .brandText{
      display:flex;
      flex-direction:column;
      line-height:1.1;
    }

    .brandText .kicker{
      font-size:11px;
      color:var(--muted);
      text-transform:uppercase;
      letter-spacing:0.08em;
    }

    .brandText .title{
      font-size:14px;
      font-weight:700;
      color:var(--text);
      max-width:340px;
      overflow:hidden;
      text-overflow:ellipsis;
      white-space:nowrap;
    }

    .controls{
      display:flex;
      flex-wrap:wrap;
      gap:8px;
      align-items:center;
      flex:1;
    }

    .input, .select, .btn{
      border:1px solid var(--line2);
      background:linear-gradient(180deg, rgba(17,27,49,0.96), rgba(14,26,48,0.96));
      color:var(--text);
      border-radius:12px;
      height:38px;
      padding:0 12px;
      outline:none;
      box-shadow:var(--shadow);
    }

    .input{
      min-width:220px;
      flex:1;
    }

    .btn{
      cursor:pointer;
      font-size:12px;
      font-weight:600;
      letter-spacing:0.01em;
    }

    .btn:hover{
      border-color:var(--accent);
    }

    .btn.active{
      border-color:var(--accent);
      box-shadow:0 0 0 1px rgba(79,140,255,0.22), var(--shadow);
    }

    .badge{
      height:38px;
      display:inline-flex;
      align-items:center;
      padding:0 12px;
      border:1px solid var(--line2);
      border-radius:12px;
      color:var(--muted);
      background:rgba(15,23,42,0.8);
      font-size:12px;
      white-space:nowrap;
    }

    .sidebar{
      border-right:1px solid var(--line);
      background:linear-gradient(180deg, rgba(15,23,42,0.92), rgba(11,18,32,0.96));
      overflow:auto;
      padding:14px;
    }

    .panel{
      background:linear-gradient(180deg, rgba(17,27,49,0.75), rgba(14,26,48,0.75));
      border:1px solid var(--line);
      border-radius:16px;
      padding:12px;
      margin-bottom:12px;
      box-shadow:var(--shadow);
    }

    .panel h3{
      margin:0 0 10px 0;
      font-size:12px;
      text-transform:uppercase;
      letter-spacing:0.08em;
      color:var(--muted);
      font-weight:700;
    }

    .kv{
      display:grid;
      grid-template-columns: 1fr auto;
      gap:6px 10px;
      font-size:12px;
    }

    .kv .k{ color:var(--muted); }
    .kv .v{ color:var(--text); font-weight:600; }

    .hotspotList{
      display:flex;
      flex-direction:column;
      gap:8px;
    }

    .hotspot{
      padding:10px 10px;
      border:1px solid var(--line);
      border-radius:12px;
      background:rgba(16,33,63,0.5);
      cursor:pointer;
      transition:120ms ease;
    }

    .hotspot:hover{
      border-color:var(--accent);
      transform:translateY(-1px);
    }

    .hotspot .name{
      font-size:12px;
      font-weight:700;
      color:var(--text);
      overflow:hidden;
      text-overflow:ellipsis;
      white-space:nowrap;
    }

    .hotspot .path{
      font-size:11px;
      color:var(--muted);
      overflow:hidden;
      text-overflow:ellipsis;
      white-space:nowrap;
      margin-top:2px;
    }

    .hotspot .meta{
      display:flex;
      gap:10px;
      flex-wrap:wrap;
      margin-top:7px;
      font-size:11px;
      color:var(--muted);
    }

    .stage{
      position:relative;
      overflow:hidden;
    }

    #wrap{
      height:100%;
      width:100%;
    }

    svg{
      width:100%;
      height:100%;
      display:block;
      background:
        radial-gradient(circle at 30% 10%, rgba(79,140,255,0.06), transparent 22%),
        linear-gradient(180deg, #0b1220 0%, #0a1020 100%);
      touch-action:none;
    }

    .grid line{
      stroke:rgba(61,81,122,0.15);
      stroke-width:1;
    }

    .link{
      fill:none;
      stroke:#31425f;
      stroke-width:1.2px;
      opacity:0.95;
    }

    .node rect{
      stroke:var(--line2);
      stroke-width:1px;
      rx:12;
      ry:12;
      filter: drop-shadow(0 6px 14px rgba(0,0,0,0.18));
    }

    .node.folder rect{
      fill:linear-gradient(180deg, #112242, #0f1e38);
    }

    .node.file rect{
      fill:#141c2e;
    }

    .node.priority rect{
      stroke:#4568a8;
    }

    .node.more rect{
      stroke:var(--warn);
      stroke-dasharray:5 3;
    }

    .node.highlight rect{
      stroke:var(--warn);
      stroke-width:2px;
    }

    .node.selected rect{
      stroke:var(--accent);
      stroke-width:2px;
    }

    .node text{
      fill:var(--text);
      pointer-events:none;
    }

    .node .labelMain{
      font-size:12px;
      font-weight:700;
    }

    .node .labelSub{
      font-size:10px;
      fill:var(--muted);
    }

    .node .toggle, .node .plus, .node .trash{
      pointer-events:all;
    }

    .node .toggle circle, .node .plus circle, .node .trash circle{
      fill:#0f172a;
      stroke:var(--line2);
      stroke-width:1px;
    }

    .node .toggle:hover circle, .node .plus:hover circle{
      stroke:var(--accent);
    }

    .node .trash:hover circle{
      stroke:var(--danger);
    }

    .node .toggle text, .node .plus text, .node .trash text{
      font-size:11px;
      font-weight:800;
      text-anchor:middle;
      dy:0.35em;
      fill:var(--text);
      pointer-events:none;
    }

    .foreign{
      overflow:visible;
    }

    .inlineEditor{
      display:flex;
      gap:6px;
      align-items:center;
      background:linear-gradient(180deg, rgba(17,27,49,0.98), rgba(14,26,48,0.98));
      border:1px solid var(--line2);
      border-radius:12px;
      padding:8px;
      box-shadow:var(--shadow);
    }

    .inlineEditor input, .inlineEditor select{
      background:#0f172a;
      color:var(--text);
      border:1px solid var(--line2);
      border-radius:10px;
      padding:7px 8px;
      outline:none;
      font-size:12px;
    }

    .inlineEditor button{
      height:34px;
      border-radius:10px;
      border:1px solid var(--line2);
      background:#0f172a;
      color:var(--text);
      cursor:pointer;
      padding:0 10px;
      font-size:12px;
      font-weight:700;
    }

    .inlineEditor button:hover{
      border-color:var(--accent);
    }

    .footnote{
      font-size:11px;
      color:var(--muted);
      line-height:1.4;
    }

    @media (max-width: 1200px){
      .app{
        grid-template-columns: 290px 1fr;
      }
    }

    @media (max-width: 980px){
      .app{
        grid-template-columns: 1fr;
        grid-template-rows: auto auto 1fr;
      }
      .topbar{
        grid-column:1;
      }
      .sidebar{
        border-right:none;
        border-bottom:1px solid var(--line);
        max-height:280px;
      }
    }
  </style>
</head>
<body>
<div class="app">
  <div class="topbar">
    <div class="brand">
      <div class="brandMark"></div>
      <div class="brandText">
        <div class="kicker">Structure Intelligence</div>
        <div class="title" id="title">Corporate Structure Viewer</div>
      </div>
    </div>

    <div class="controls">
      <input id="q" class="input" type="text" placeholder="Search path or module..." />
      <button id="fit" class="btn">Fit</button>
      <button id="compact" class="btn">Compact</button>
      <button id="expand" class="btn">Expand</button>
      <button id="collapse" class="btn">Collapse</button>
      <button id="foldersOnlyBtn" class="btn active">Folders Only</button>
      <button id="largeOnlyBtn" class="btn">Large Nodes</button>
      <button id="exportOverlay" class="btn">Export Overlay</button>
      <label>
        <input id="importOverlay" type="file" accept="application/json" style="display:none;">
        <button id="importBtn" class="btn" type="button">Import Overlay</button>
      </label>
      <button id="resetOverlay" class="btn">Reset Overlay</button>
      <span class="badge" id="stats"></span>
    </div>
  </div>

  <aside class="sidebar">
    <div class="panel">
      <h3>Overview</h3>
      <div class="kv">
        <div class="k">Root</div><div class="v" id="ovRoot">-</div>
        <div class="k">Total folders</div><div class="v" id="ovDirs">-</div>
        <div class="k">Total files</div><div class="v" id="ovFiles">-</div>
        <div class="k">Rendered nodes</div><div class="v" id="ovRendered">-</div>
        <div class="k">Overlay nodes</div><div class="v" id="ovOverlay">-</div>
      </div>
    </div>

    <div class="panel">
      <h3>Hotspots · Largest Folders</h3>
      <div class="hotspotList" id="largestList"></div>
    </div>

    <div class="panel">
      <h3>Hotspots · Widest Branches</h3>
      <div class="hotspotList" id="widestList"></div>
    </div>

    <div class="panel">
      <h3>Notes</h3>
      <div class="footnote">
        Große Strukturen werden automatisch verdichtet. Standardmäßig werden Dateien in der
        Grafik ausgeblendet, damit zuerst Modul- und Domänenstruktur sichtbar bleibt.
      </div>
    </div>
  </aside>

  <main class="stage">
    <div id="wrap">
      <svg id="svg"></svg>
    </div>
  </main>
</div>

<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>
<script>
const BASE_DATA = __DATA__;
const HOTSPOTS = __HOTSPOTS__;

// -------------------- Overlay
const OVERLAY_KEY = "corp_structure_overlay_v2";

function loadOverlay(){
  try{
    const raw = localStorage.getItem(OVERLAY_KEY);
    if(!raw) return {nodes:[]};
    const o = JSON.parse(raw);
    if(!o || !Array.isArray(o.nodes)) return {nodes:[]};
    return o;
  }catch(e){
    return {nodes:[]};
  }
}

function saveOverlay(o){
  localStorage.setItem(OVERLAY_KEY, JSON.stringify(o));
}

let overlay = loadOverlay();
let undoStack = [];

function snapshotOverlay(){
  return JSON.parse(JSON.stringify(overlay || {nodes:[]}));
}

function pushUndo(){
  undoStack.push(snapshotOverlay());
  if(undoStack.length > 50) undoStack.shift();
}

function undo(){
  if(!undoStack.length) return;
  overlay = undoStack.pop();
  saveOverlay(overlay);
  rebuildRoot();
  update(root);
}

// -------------------- Data merge
function ensureChild(parentNode, name, isDir){
  parentNode.children = parentNode.children || [];
  const hit = parentNode.children.find(c => c.name === name && !!c.is_dir === !!isDir);
  if(hit) return hit;

  const child = {
    name,
    path:"",
    is_dir:isDir,
    file_count:isDir?0:1,
    dir_count:0,
    depth:(parentNode.depth || 0) + 1,
    direct_children_count:0,
    collapsed_hint:false,
    priority_hint:false,
    hidden_children_count:0,
    is_more_node:false,
    children:isDir?[]:[]
  };
  parentNode.children.push(child);
  parentNode.children.sort((a,b) => {
    if(a.is_dir !== b.is_dir) return a.is_dir ? -1 : 1;
    return (a.name || "").localeCompare(b.name || "");
  });
  parentNode.direct_children_count = parentNode.children.length;
  return child;
}

function insertVirtualPath(baseData, path, type){
  const parts = String(path || "").split("/").filter(Boolean);
  if(!parts.length) return;

  let cur = baseData;
  let curPath = ".";

  for(let i=0;i<parts.length;i++){
    const isLast = i === parts.length - 1;
    const wantDir = isLast ? (type === "dir") : true;
    const name = parts[i];
    const nxt = ensureChild(cur, name, wantDir);
    curPath = (curPath === "." ? name : (curPath + "/" + name));
    nxt.path = curPath;
    cur = nxt;
  }
}

function recalcMetrics(node, depth=0){
  node.depth = depth;

  if(!node.is_dir){
    node.file_count = 1;
    node.dir_count = 0;
    node.direct_children_count = 0;
    return node;
  }

  let files = 0;
  let dirs = 0;
  node.children = node.children || [];
  node.direct_children_count = node.children.length;

  node.children.forEach(child => {
    recalcMetrics(child, depth + 1);
    if(child.is_dir){
      dirs += 1 + (child.dir_count || 0);
      files += (child.file_count || 0);
    }else{
      files += 1;
    }
  });

  node.file_count = files;
  node.dir_count = dirs;
  return node;
}

function applyOverlayToData(baseData, overlayObj){
  const out = JSON.parse(JSON.stringify(baseData));
  (overlayObj.nodes || []).forEach(n => {
    if(!n || !n.path || !n.type) return;
    const p = String(n.path).replace(/\\/g,"/").replace(/^\.\/+/,"").trim();
    if(!p) return;
    insertVirtualPath(out, p, n.type);
  });
  recalcMetrics(out, 0);
  return out;
}

function addOverlayNode(type, fullPath){
  pushUndo();
  const path = String(fullPath).replace(/\\/g,"/").replace(/^\.\/+/,"").trim();
  if(!path) return;

  overlay.nodes = overlay.nodes || [];
  overlay.nodes.push({type, path});

  const seen = new Set();
  overlay.nodes = overlay.nodes.filter(n => {
    const k = `${n.type}|${n.path}`;
    if(seen.has(k)) return false;
    seen.add(k);
    return true;
  });

  saveOverlay(overlay);
  rebuildRoot();
  update(root);
}

function deleteOverlayPrefix(prefixPath){
  const prefix = String(prefixPath).replace(/\\/g,"/").replace(/^\.\/+/,"").trim();
  if(!prefix) return;

  const before = (overlay.nodes||[]).length;
  overlay.nodes = (overlay.nodes||[]).filter(n => {
    const p = String(n.path||"").replace(/\\/g,"/").replace(/^\.\/+/,"").trim();
    return !(p === prefix || p.startsWith(prefix + "/"));
  });

  if((overlay.nodes||[]).length === before) return;
  saveOverlay(overlay);
  rebuildRoot();
  update(root);
}

function isOverlayPath(path){
  const p = String(path || "").replace(/\\/g,"/").replace(/^\.\/+/,"").trim();
  return (overlay.nodes || []).some(n => {
    const np = String(n.path || "").replace(/\\/g,"/").replace(/^\.\/+/,"").trim();
    return np === p;
  });
}

// -------------------- View state
let foldersOnly = true;
let largeOnly = false;
let compactMode = false;
let selectedPath = null;
let editorOpenFor = null;

function filterTreeForView(node){
  const copy = JSON.parse(JSON.stringify(node));

  function keep(n){
    if(!n.is_dir){
      return !foldersOnly;
    }

    if(largeOnly){
      const isLarge = (n.file_count || 0) >= 20 || (n.dir_count || 0) >= 8 || (n.direct_children_count || 0) >= 10 || n.depth <= 1 || n.priority_hint;
      if(!isLarge && n.depth > 1){
        return false;
      }
    }

    n.children = (n.children || []).filter(keep);
    return true;
  }

  keep(copy);
  recalcMetrics(copy, 0);
  return copy;
}

// -------------------- D3 setup
const svg = d3.select("#svg");
const defs = svg.append("defs");

const gMain = svg.append("g");
const gGrid = gMain.append("g").attr("class", "grid");
const gLinks = gMain.append("g");
const gNodes = gMain.append("g");

const width = () => svg.node().clientWidth;
const height = () => svg.node().clientHeight;

const zoom = d3.zoom()
  .scaleExtent([0.15, 4])
  .on("zoom", (event) => gMain.attr("transform", event.transform));

svg.call(zoom);
svg.on("dblclick.zoom", null);
svg.on("click", () => closeInlineEditor());

// -------------------- Build root
let rootData = applyOverlayToData(BASE_DATA, overlay);
rootData = filterTreeForView(rootData);
let root = d3.hierarchy(rootData);

document.getElementById("title").textContent = BASE_DATA.name || "Corporate Structure Viewer";

function rebuildRoot(){
  rootData = applyOverlayToData(BASE_DATA, overlay);
  rootData = filterTreeForView(rootData);
  root = d3.hierarchy(rootData);

  document.getElementById("title").textContent = rootData.name || "Corporate Structure Viewer";
  refreshOverview(rootData);
  closeInlineEditor();
  applyInitialCollapse();
  refreshSidebar(rootData);
}

// -------------------- Tree helpers
function collapseAll(d){
  if(d.children){
    d._children = d.children;
    d._children.forEach(collapseAll);
    d.children = null;
  }
}

function expandAll(d){
  if(d._children){
    d.children = d._children;
    d._children = null;
  }
  if(d.children) d.children.forEach(expandAll);
}

function toggleNode(d){
  if(d.children){
    d._children = d.children;
    d.children = null;
  }else if(d._children){
    d.children = d._children;
    d._children = null;
  }
}

function smartCollapse(d, level=0){
  const visibleChildren = (d.children || []).length;
  const isHuge = visibleChildren > 10;
  const isDeep = level >= 2;
  const hint = !!d.data.collapsed_hint;
  const priority = !!d.data.priority_hint;
  const isMore = !!d.data.is_more_node;

  if(d.children){
    d.children.forEach(c => smartCollapse(c, level + 1));
  }

  if(d.children && !priority){
    if((isHuge && level >= 1) || hint || isDeep || isMore){
      d._children = d.children;
      d.children = null;
    }
  }
}

function applyInitialCollapse(){
  if(!root) return;
  expandAll(root);
  smartCollapse(root, 0);
}

// -------------------- Layout
const nodeH = 42;
const paddingX = 12;

function computeLayout(){
  const w = width();
  const h = height();

  let depthSpacing = 280;
  let rowSpacing = 82;

  if(w < 1500) depthSpacing = 250;
  if(w < 1200) depthSpacing = 220;
  if(w < 900)  depthSpacing = 190;
  if(w < 700)  depthSpacing = 165;

  if(h < 900) rowSpacing = 74;
  if(h < 760) rowSpacing = 66;
  if(h < 620) rowSpacing = 58;

  if(compactMode){
    depthSpacing = Math.max(145, Math.floor(depthSpacing * 0.84));
    rowSpacing = Math.max(44, Math.floor(rowSpacing * 0.84));
  }

  return {depthSpacing, rowSpacing};
}

function nodeWidth(d){
  const w = width();
  const baseText = `${d.data.name || ""} ${(d.data.file_count || 0)} ${(d.data.dir_count || 0)}`;
  const base = Math.max(190, baseText.length * 6.6 + 80);
  const cap = w < 900 ? 350 : 470;
  return Math.min(cap, base);
}

function drawGrid(){
  const w = width();
  const h = height();
  const step = 80;

  const linesX = d3.range(0, w + step, step);
  const linesY = d3.range(0, h + step, step);

  const vx = gGrid.selectAll("line.v").data(linesX, d => d);
  vx.enter().append("line").attr("class","v")
    .merge(vx)
    .attr("x1", d => d)
    .attr("x2", d => d)
    .attr("y1", 0)
    .attr("y2", h);
  vx.exit().remove();

  const hy = gGrid.selectAll("line.h").data(linesY, d => d);
  hy.enter().append("line").attr("class","h")
    .merge(hy)
    .attr("x1", 0)
    .attr("x2", w)
    .attr("y1", d => d)
    .attr("y2", d => d);
  hy.exit().remove();
}

// -------------------- Inline editor
function closeInlineEditor(){
  gNodes.selectAll("foreignObject.inlineEditorFO").remove();
  editorOpenFor = null;
}

function openInlineEditor(d){
  if(!d.data.is_dir) return;

  if(editorOpenFor === d.data.path){
    closeInlineEditor();
    return;
  }

  closeInlineEditor();
  editorOpenFor = d.data.path;

  const w = 330, h = 50;
  const nx = d.y;
  const ny = d.x;

  const fo = gNodes.append("foreignObject")
    .attr("class","inlineEditorFO foreign")
    .attr("x", nx + (nodeWidth(d)/2) + 18)
    .attr("y", ny - h/2)
    .attr("width", w)
    .attr("height", h);

  const div = fo.append("xhtml:div").attr("class","inlineEditor");
  div.on("mousedown", (ev)=> ev.stopPropagation());
  div.on("click", (ev)=> ev.stopPropagation());

  div.append("select")
    .attr("id","edType")
    .html(`<option value="dir">folder</option><option value="file">file</option>`);

  const inp = div.append("input")
    .attr("id","edName")
    .attr("placeholder","Name, e.g. signal_engine or engine.py")
    .node();

  const addBtn = div.append("button").text("Add").node();
  const cancelBtn = div.append("button").text("X").node();

  function commit(){
    const type = fo.select("#edType").node().value;
    const name = String(fo.select("#edName").node().value || "").trim();
    if(!name) return;

    const parent = (d.data.path === "." ? "" : d.data.path);
    const childPath = parent ? `${parent}/${name}` : name;

    addOverlayNode(type, childPath);
    closeInlineEditor();
  }

  addBtn.addEventListener("click", (e)=>{ e.stopPropagation(); commit(); });
  cancelBtn.addEventListener("click", (e)=>{ e.stopPropagation(); closeInlineEditor(); });

  inp.addEventListener("keydown", (e) => {
    if(e.key === "Enter"){ e.stopPropagation(); commit(); }
    if(e.key === "Escape"){ e.stopPropagation(); closeInlineEditor(); }
  });

  inp.focus();
}

// -------------------- Rendering
function update(source){
  drawGrid();

  const layout = computeLayout();
  const tree = d3.tree().nodeSize([layout.rowSpacing, layout.depthSpacing]);
  tree(root);

  const nodes = root.descendants();
  const links = root.links();

  const linkGen = d3.linkHorizontal()
    .x(d => d.y)
    .y(d => d.x);

  const link = gLinks.selectAll("path.link").data(links, d => d.target.data.path);

  link.enter()
      .append("path")
      .attr("class","link")
    .merge(link)
      .attr("d", linkGen);

  link.exit().remove();

  const node = gNodes.selectAll("g.node").data(nodes, d => d.data.path);

  const nodeEnter = node.enter()
    .append("g")
    .attr("class", d => {
      const classes = ["node", d.data.is_dir ? "folder" : "file"];
      if(d.data.priority_hint) classes.push("priority");
      if(d.data.is_more_node) classes.push("more");
      return classes.join(" ");
    })
    .attr("transform", d => `translate(${d.y},${d.x})`)
    .style("cursor","pointer")
    .on("click", (event, d) => {
      event.stopPropagation();
      closeInlineEditor();
      selectedPath = d.data.path;
      update(d);
    });

  nodeEnter.append("rect")
    .attr("x", d => -nodeWidth(d)/2)
    .attr("y", -nodeH/2)
    .attr("width", d => nodeWidth(d))
    .attr("height", nodeH);

  const toggle = nodeEnter.filter(d => d.data.is_dir)
    .append("g")
    .attr("class","toggle")
    .attr("transform", d => `translate(${-nodeWidth(d)/2 + 16}, 0)`)
    .on("click", (event, d) => {
      event.stopPropagation();
      closeInlineEditor();
      toggleNode(d);
      update(d);
    });

  toggle.append("circle").attr("r", 11);
  toggle.append("text")
    .attr("class","toggleText")
    .text(d => (d.children ? "▾" : (d._children ? "▸" : "•")));

  const txt = nodeEnter.append("g").attr("class","labelWrap");

  txt.append("text")
    .attr("class","labelMain")
    .attr("x", d => -nodeWidth(d)/2 + 34)
    .attr("y", -3)
    .attr("text-anchor","start")
    .text(d => {
      const icon = d.data.is_dir ? "📁" : "📄";
      return `${icon} ${d.data.name || ""}`;
    });

  txt.append("text")
    .attr("class","labelSub")
    .attr("x", d => -nodeWidth(d)/2 + 34)
    .attr("y", 13)
    .attr("text-anchor","start")
    .text(d => {
      if(!d.data.is_dir){
        return d.data.path || "";
      }
      const dirs = d.data.dir_count || 0;
      const files = d.data.file_count || 0;
      const children = d.data.direct_children_count || 0;
      const tags = [];
      tags.push(`${dirs} dirs`);
      tags.push(`${files} files`);
      tags.push(`${children} children`);
      if(d.data.priority_hint) tags.push("priority");
      if(d.data.hidden_children_count) tags.push(`${d.data.hidden_children_count} hidden`);
      return tags.join(" · ");
    });

  nodeEnter.append("title")
    .text(d => {
      if(d.data.is_dir){
        return `${d.data.path}\nDirs: ${d.data.dir_count || 0}\nFiles: ${d.data.file_count || 0}\nChildren: ${d.data.direct_children_count || 0}`;
      }
      return d.data.path || d.data.name;
    });

  const trash = nodeEnter
    .filter(d => d.data.is_dir && isOverlayPath(d.data.path))
    .append("g")
    .attr("class","trash")
    .attr("transform", d => `translate(${nodeWidth(d)/2 - 46}, 0)`)
    .on("click", (event, d) => {
      event.stopPropagation();
      closeInlineEditor();
      const ok = confirm(`Delete virtual node and children?\n${d.data.path}`);
      if(!ok) return;
      pushUndo();
      deleteOverlayPrefix(d.data.path);
    });

  trash.append("circle").attr("r", 11);
  trash.append("text").text("🗑");

  const plus = nodeEnter.filter(d => d.data.is_dir)
    .append("g")
    .attr("class","plus")
    .attr("transform", d => `translate(${nodeWidth(d)/2 - 16}, 0)`)
    .on("click", (event, d) => {
      event.stopPropagation();
      openInlineEditor(d);
    });

  plus.append("circle").attr("r", 11);
  plus.append("text").text("+");

  const nodeMerged = node.merge(nodeEnter)
    .attr("class", d => {
      const classes = ["node", d.data.is_dir ? "folder" : "file"];
      if(d.data.priority_hint) classes.push("priority");
      if(d.data.is_more_node) classes.push("more");
      if(selectedPath && d.data.path === selectedPath) classes.push("selected");
      return classes.join(" ");
    })
    .attr("transform", d => `translate(${d.y},${d.x})`);

  nodeMerged.select("rect")
    .attr("x", d => -nodeWidth(d)/2)
    .attr("y", -nodeH/2)
    .attr("width", d => nodeWidth(d))
    .attr("height", nodeH);

  nodeMerged.selectAll("g.toggle text.toggleText")
    .text(d => (d.children ? "▾" : (d._children ? "▸" : "•")));

  nodeMerged.selectAll("g.toggle")
    .attr("transform", d => `translate(${-nodeWidth(d)/2 + 16}, 0)`);

  nodeMerged.selectAll("g.trash")
    .attr("transform", d => `translate(${nodeWidth(d)/2 - 46}, 0)`);

  nodeMerged.selectAll("g.plus")
    .attr("transform", d => `translate(${nodeWidth(d)/2 - 16}, 0)`);

  nodeMerged.selectAll("text.labelMain")
    .attr("x", d => -nodeWidth(d)/2 + 34)
    .text(d => {
      const icon = d.data.is_dir ? "📁" : "📄";
      return `${icon} ${d.data.name || ""}`;
    });

  nodeMerged.selectAll("text.labelSub")
    .attr("x", d => -nodeWidth(d)/2 + 34)
    .text(d => {
      if(!d.data.is_dir){
        return d.data.path || "";
      }
      const dirs = d.data.dir_count || 0;
      const files = d.data.file_count || 0;
      const children = d.data.direct_children_count || 0;
      const tags = [];
      tags.push(`${dirs} dirs`);
      tags.push(`${files} files`);
      tags.push(`${children} children`);
      if(d.data.priority_hint) tags.push("priority");
      if(d.data.hidden_children_count) tags.push(`${d.data.hidden_children_count} hidden`);
      return tags.join(" · ");
    });

  node.exit().remove();

  document.getElementById("stats").textContent =
    `${nodes.length} rendered · overlay ${(overlay.nodes||[]).length}`;
  document.getElementById("ovRendered").textContent = String(nodes.length);
  document.getElementById("ovOverlay").textContent = String((overlay.nodes||[]).length);
}

// -------------------- Fit
function fitToScreen(){
  const prev = gMain.attr("transform");
  gMain.attr("transform", null);
  const bbox = gMain.node().getBBox();
  gMain.attr("transform", prev);

  const w = width();
  const h = height();
  if(!bbox || bbox.width === 0 || bbox.height === 0) return;

  const margin = 36;
  const sx = (w - 2*margin) / bbox.width;
  const sy = (h - 2*margin) / bbox.height;
  const scale = Math.min(2.4, Math.max(0.15, Math.min(sx, sy)));

  const cx = bbox.x + bbox.width/2;
  const cy = bbox.y + bbox.height/2;

  const tx = (w/2) - scale * cx;
  const ty = (h/2) - scale * cy;

  const t = d3.zoomIdentity.translate(tx, ty).scale(scale);
  svg.transition().duration(240).call(zoom.transform, t);
}

// -------------------- Search
function clearHighlights(){
  gNodes.selectAll("g.node").classed("highlight", false);
}

function expandPathTo(d){
  let cur = d;
  while(cur){
    if(cur._children){
      cur.children = cur._children;
      cur._children = null;
    }
    cur = cur.parent;
  }
}

function doSearch(q){
  q = (q||"").toLowerCase().trim();
  clearHighlights();

  if(!q){
    update(root);
    return;
  }

  const matches = [];
  function walkAll(d){
    const name = (d.data.name||"").toLowerCase();
    const path = (d.data.path||"").toLowerCase();
    if(name.includes(q) || path.includes(q)) matches.push(d);

    const kids = (d.children||[]).concat(d._children||[]);
    kids.forEach(walkAll);
  }

  walkAll(root);
  matches.slice(0, 50).forEach(expandPathTo);
  update(root);

  gNodes.selectAll("g.node").each(function(d){
    const name = (d.data.name||"").toLowerCase();
    const path = (d.data.path||"").toLowerCase();
    if(name.includes(q) || path.includes(q)){
      d3.select(this).classed("highlight", true);
    }
  });

  if(matches.length){
    selectedPath = matches[0].data.path;
    update(root);
    focusNodeByPath(matches[0].data.path);
  }
}

// -------------------- Focus helpers
function findNodeByPath(targetPath){
  let found = null;
  root.each(d => {
    if(d.data.path === targetPath){
      found = d;
    }
  });
  return found;
}

function focusNodeByPath(targetPath){
  const d = findNodeByPath(targetPath);
  if(!d) return;

  expandPathTo(d);
  update(root);

  const w = width();
  const h = height();
  const current = d3.zoomTransform(svg.node());
  const scale = current.k || 1;

  const tx = w * 0.45 - d.y * scale;
  const ty = h * 0.50 - d.x * scale;
  const t = d3.zoomIdentity.translate(tx, ty).scale(scale);

  svg.transition().duration(220).call(zoom.transform, t);
}

// -------------------- Sidebar
function byLargest(a,b){
  return (b.file_count + b.dir_count) - (a.file_count + a.dir_count);
}

function byWidest(a,b){
  return (b.direct_children_count || 0) - (a.direct_children_count || 0);
}

function renderHotspotList(elId, items){
  const el = document.getElementById(elId);
  el.innerHTML = "";

  items.forEach(item => {
    const div = document.createElement("div");
    div.className = "hotspot";
    div.innerHTML = `
      <div class="name">${escapeHtml(item.name || "(root)")}</div>
      <div class="path">${escapeHtml(item.path || ".")}</div>
      <div class="meta">
        <span>${item.dir_count || 0} dirs</span>
        <span>${item.file_count || 0} files</span>
        <span>${item.direct_children_count || 0} children</span>
      </div>
    `;
    div.addEventListener("click", () => {
      selectedPath = item.path;
      focusPathInCurrentRoot(item.path);
    });
    el.appendChild(div);
  });
}

function flattenDirs(data){
  const out = [];
  function walk(n){
    if(n.is_dir) out.push(n);
    (n.children || []).forEach(walk);
  }
  walk(data);
  return out;
}

function refreshSidebar(data){
  const dirs = flattenDirs(data).filter(x => x.path !== "." && !x.is_more_node);
  const largest = [...dirs].sort(byLargest).slice(0, 10);
  const widest = [...dirs].sort(byWidest).slice(0, 10);

  renderHotspotList("largestList", largest);
  renderHotspotList("widestList", widest);
}

function focusPathInCurrentRoot(targetPath){
  function walkAll(d){
    if(d.data.path === targetPath){
      expandPathTo(d);
    }
  }
  root.each(walkAll);
  update(root);
  focusNodeByPath(targetPath);
}

// -------------------- Overview
function refreshOverview(data){
  document.getElementById("ovRoot").textContent = data.name || "-";
  document.getElementById("ovDirs").textContent = String(data.dir_count || 0);
  document.getElementById("ovFiles").textContent = String(data.file_count || 0);
}

function escapeHtml(s){
  return String(s)
    .replaceAll("&","&amp;")
    .replaceAll("<","&lt;")
    .replaceAll(">","&gt;")
    .replaceAll('"',"&quot;");
}

// -------------------- Buttons
document.getElementById("fit").addEventListener("click", () => {
  closeInlineEditor();
  fitToScreen();
});

document.getElementById("compact").addEventListener("click", (e) => {
  compactMode = !compactMode;
  e.currentTarget.classList.toggle("active", compactMode);
  closeInlineEditor();
  update(root);
});

document.getElementById("expand").addEventListener("click", () => {
  closeInlineEditor();
  expandAll(root);
  update(root);
});

document.getElementById("collapse").addEventListener("click", () => {
  closeInlineEditor();
  applyInitialCollapse();
  update(root);
});

document.getElementById("foldersOnlyBtn").addEventListener("click", (e) => {
  foldersOnly = !foldersOnly;
  e.currentTarget.classList.toggle("active", foldersOnly);
  rebuildRoot();
  update(root);
  setTimeout(fitToScreen, 80);
});

document.getElementById("largeOnlyBtn").addEventListener("click", (e) => {
  largeOnly = !largeOnly;
  e.currentTarget.classList.toggle("active", largeOnly);
  rebuildRoot();
  update(root);
  setTimeout(fitToScreen, 80);
});

document.getElementById("exportOverlay").addEventListener("click", () => {
  closeInlineEditor();
  const blob = new Blob([JSON.stringify(overlay, null, 2)], {type:"application/json"});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "overlay.json";
  a.click();
  URL.revokeObjectURL(url);
});

document.getElementById("importBtn").addEventListener("click", () => {
  closeInlineEditor();
  document.getElementById("importOverlay").click();
});

document.getElementById("importOverlay").addEventListener("change", (ev) => {
  closeInlineEditor();
  const file = ev.target.files && ev.target.files[0];
  if(!file) return;

  const reader = new FileReader();
  reader.onload = () => {
    try{
      const obj = JSON.parse(reader.result);
      if(!obj || !Array.isArray(obj.nodes)){
        throw new Error("Invalid overlay format. Expected {nodes:[...]}");
      }
      pushUndo();
      overlay = obj;
      saveOverlay(overlay);
      rebuildRoot();
      update(root);
      setTimeout(fitToScreen, 80);
    }catch(e){
      alert("Import failed: " + e.message);
    }
  };
  reader.readAsText(file);
});

document.getElementById("resetOverlay").addEventListener("click", () => {
  closeInlineEditor();
  pushUndo();
  overlay = {nodes:[]};
  saveOverlay(overlay);
  rebuildRoot();
  update(root);
  setTimeout(fitToScreen, 80);
});

// -------------------- Search
const qEl = document.getElementById("q");
let tSearch = null;
qEl.addEventListener("input", () => {
  clearTimeout(tSearch);
  tSearch = setTimeout(() => doSearch(qEl.value), 140);
});

// -------------------- Keyboard
window.addEventListener("keydown", (e) => {
  if((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "z"){
    e.preventDefault();
    closeInlineEditor();
    undo();
  }
  if(e.key === "Escape"){
    closeInlineEditor();
  }
});

// -------------------- Resize
let resizeTimer = null;
window.addEventListener("resize", () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => {
    update(root);
    fitToScreen();
  }, 160);
});

// -------------------- Init
refreshOverview(rootData);
refreshSidebar(rootData);
applyInitialCollapse();
update(root);
setTimeout(fitToScreen, 120);
</script>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Corporate folder diagram viewer for large project structures.")
    p.add_argument("--root", type=str, default="", help="Root folder to scan. Default: cwd.")
    p.add_argument("--auto-root", action="store_true", help="Auto-detect best root below cwd using known markers.")
    p.add_argument("--out-dir", type=str, default="", help="Output dir. Default: <script>/output/diagram_tree")
    p.add_argument("--title", type=str, default="Quant Platform", help="Diagram title.")
    p.add_argument("--max-depth", type=int, default=6, help="Max scan depth.")
    p.add_argument("--folders-only", action="store_true", help="Only folders in initial dataset.")
    p.add_argument("--include-exts", type=str, default=",".join(sorted(DEFAULT_INCLUDE_EXTS)), help="Included file extensions.")
    p.add_argument("--max-children", type=int, default=12, help="Max visible direct children per node before compression.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    root = Path(args.root).expanduser().resolve() if args.root.strip() else Path.cwd().resolve()

    if args.auto_root:
        best = find_best_root(root, max_search_depth=4)
        if best is not None:
            root = best.resolve()

    if not root.exists():
        raise FileNotFoundError(f"root not found: {root}")

    script_dir = Path(__file__).resolve().parent
    default_out_dir = script_dir / "output" / "diagram_tree"
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir.strip() else default_out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    ignore_patterns = [re.compile(x) for x in DEFAULT_IGNORE_FILES_REGEX]
    include_files = not args.folders_only

    include_exts: Set[str] = set()
    if include_files:
        s = args.include_exts.strip()
        if s:
            include_exts = {
                e.strip().lower() if e.strip().startswith(".") else "." + e.strip().lower()
                for e in s.split(",") if e.strip()
            }

    tree = build_tree(
        root=root,
        base=root,
        max_depth=int(args.max_depth),
        include_files=include_files,
        include_exts=include_exts,
        ignore_dirs=set(DEFAULT_IGNORE_DIRS),
        ignore_file_patterns=ignore_patterns,
    )

    tree.name = args.title
    tree.path = "."
    tree = enrich_counts(tree)
    tree = compress_children(tree, max_children=max(3, int(args.max_children)))
    tree = enrich_counts(tree)

    data = node_to_dict(tree)
    hotspots = collect_hotspots(tree)

    json_path = out_dir / "tree.json"
    hotspots_path = out_dir / "hotspots.json"
    html_path = out_dir / "diagram.html"

    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    hotspots_path.write_text(json.dumps(hotspots, ensure_ascii=False, indent=2), encoding="utf-8")

    html = HTML_TEMPLATE.replace("__DATA__", json.dumps(data, ensure_ascii=False))
    html = html.replace("__HOTSPOTS__", json.dumps(hotspots, ensure_ascii=False))
    html_path.write_text(html, encoding="utf-8")

    print(f"[INFO] root         : {root}")
    print(f"[INFO] out_dir      : {out_dir}")
    print(f"[INFO] max_depth    : {args.max_depth}")
    print(f"[INFO] max_children : {args.max_children}")
    print(f"[OK] JSON          : {json_path}")
    print(f"[OK] HOTSPOTS      : {hotspots_path}")
    print(f"[OK] DIAGRAM       : {html_path}")


if __name__ == "__main__":
    main()