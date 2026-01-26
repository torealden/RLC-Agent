#!/usr/bin/env python3
"""
generate_folder_viz_readable.py

Generate a readable, collapsible folder-tree visualization (HTML) from a
tree-style text listing.

Key features:
- Left-to-right collapsible tree (high readability)
- Hover tooltip shows:
    * kind (FOLDER/FILE)
    * node name
    * full path
    * note (from inline '# ...' comments)
- "note" badge on nodes that have notes
- Legend explaining stroke colors (Folder vs File)
- Search box to highlight/jump to a node
- Zoom/pan

Usage:
  python generate_folder_viz_readable.py --in folder_tree.txt --out folder_tree_readable.html

If --in is omitted, the script uses the embedded example tree.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Tuple


EMBEDDED_TREE = r"""
Folder Structure
  RLC-Agent/
  │
  ├── README.md                      # Project overview & quick start
  ├── LLM_SETUP_PLAN.md             # Setup guide (keep at root)
  ├── requirements.txt
  ├── .env                          # Single source of credentials
  ├── .env.example
  ├── .gitignore
  │
  ├── src/                          # ALL application code
  │   ├── main.py                   # CLI entry point
  │   ├── agents/                   # AI agent implementations
  │   │   ├── core/                 # Master, memory, verification agents
  │   │   ├── collectors/           # Data collection agents (consolidate)
  │   │   ├── analysis/             # Fundamental, price, spread analyzers
  │   │   ├── reporting/            # Report generation agents
  │   │   └── integration/          # Email, calendar, Notion agents
  │   ├── orchestrators/            # Workflow coordinators
  │   ├── services/                 # Shared services
  │   │   ├── api/                  # External API clients
  │   │   ├── database/             # DB config, loaders, schema
  │   │   └── document/             # Document builders, RAG
  │   ├── scheduler/                # Consolidated scheduler (merge rlc_scheduler)
  │   │   ├── agents/               # Scheduled task agents
  │   │   ├── tasks/                # Task definitions
  │   │   └── runner.py             # Main scheduler
  │   ├── tools/                    # LLM tools & utilities
  │   └── utils/                    # Config, helpers
  │
  ├── database/                     # Database artifacts
  │   ├── schemas/                  # SQL schema files (001-009)
  │   ├── migrations/               # Schema migrations
  │   ├── views/                    # SQL views
  │   └── queries/                  # Reusable queries
  │
  ├── config/                       # Configuration (single location)
  │   ├── data_sources.csv          # Master data source list
  │   ├── weather_locations.json
  │   └── schedules.json            # Task schedules
  │
  ├── data/                         # Runtime data (gitignored mostly)
  │   ├── raw/                      # Raw downloaded data
  │   ├── processed/                # Transformed data
  │   ├── cache/                    # API response cache
  │   └── exports/                  # PowerBI exports, CSVs
  │
  ├── output/                       # Generated outputs
  │   ├── reports/                  # Generated Word reports
  │   ├── visualizations/           # Chart images
  │   └── logs/                     # Application logs
  │
  ├── tests/                        # Test suite
  │   ├── unit/
  │   ├── integration/
  │   └── fixtures/
  │
  ├── docs/                         # Documentation
  │   ├── architecture/             # System design docs
  │   ├── setup/                    # Installation guides
  │   ├── api/                      # API documentation
  │   └── runbooks/                 # Operational guides
  │
  ├── domain_knowledge/             # Agricultural economist knowledge base
  │   ├── balance_sheets/           # Excel models (current Models/)
  │   │   ├── biofuels/
  │   │   ├── feed_grains/
  │   │   ├── food_grains/
  │   │   ├── oilseeds/
  │   │   └── fats_greases/
  │   ├── sample_reports/           # Reference reports (current report_samples/)
  │   ├── operator_guides/          # Silver Operators docs
  │   └── llm_context/              # LLM training context
  │
  ├── scripts/                      # Standalone utility scripts
  │   ├── data_ingestion/           # One-time data loads
  │   ├── maintenance/              # DB maintenance, cleanup
  │   └── deployment/               # Deploy scripts (merge deployment/)
  │
  ├── dashboards/                   # Visualization assets
  │   ├── powerbi/                  # PowerBI files
  │   └── templates/                # Dashboard templates
  │
  └── archive/                      # Historical/inactive items
      ├── presentations/            # Old conference presentations
      └── deprecated_code/          # Old implementations for reference
""".strip("\n")


@dataclass
class Node:
    name: str
    note: str = ""
    kind: str = "folder"  # folder | file
    children: List["Node"] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = {"name": self.name, "kind": self.kind, "note": self.note or ""}
        if self.children:
            d["children"] = [c.to_dict() for c in self.children]
        return d


TREE_LINE_RE = re.compile(
    r"""
^(?P<prefix>.*?)
(?:(?P<marker>├──|└──)\s*)?
(?P<label>[^#\n\r]+?)
(?:\s+\#\s*(?P<note>.*))?
$
""",
    re.VERBOSE,
)


def infer_depth(prefix: str) -> int:
    """
    Estimate depth from the tree prefix. Your tree uses an initial 2-space offset,
    then 4-space blocks per nesting level.
    """
    normalized = prefix.replace("│", " ").replace("├", " ").replace("└", " ").replace("─", " ")
    leading = len(normalized) - len(normalized.lstrip(" "))
    if leading <= 2:
        return 0
    return max(0, (leading - 2) // 4)


def classify_kind(label: str) -> str:
    return "folder" if label.strip().endswith("/") else "file"


def clean_label(label: str) -> str:
    label = label.strip()
    if label.endswith("/"):
        label = label[:-1]
    return label


def is_structural_garbage(label: str) -> bool:
    """
    True if label is only tree/connector glyphs (e.g., '|', '-', '│') and should be ignored.
    """
    stripped = label.strip()
    if not stripped:
        return True
    tree_chars = set("│|-─└┘├┤┬┴┼")
    return all(ch in tree_chars for ch in stripped)


def parse_tree(text: str) -> Node:
    lines = [ln.rstrip("\n\r") for ln in text.splitlines()]

    # Find the first "RootFolder/" line
    root_name = None
    root_idx = None
    for i, ln in enumerate(lines):
        s = ln.strip()
        if s.lower() == "folder structure":
            continue
        if s.endswith("/") and "──" not in s:
            root_name = clean_label(s)
            root_idx = i
            break
    if root_name is None:
        root_name = "root"
        root_idx = 0

    root = Node(name=root_name, kind="folder")
    stack: List[Tuple[int, Node]] = [(0, root)]

    for ln in lines[root_idx + 1 :]:
        if not ln.strip():
            continue

        # Quick skips for connector-only lines
        if ln.strip() in {"│", "|", "-", "─"}:
            continue

        m = TREE_LINE_RE.match(ln)
        if not m:
            continue

        label_raw = (m.group("label") or "").rstrip()
        note = (m.group("note") or "").strip()
        prefix = (m.group("prefix") or "")

        cleaned = clean_label(label_raw)

        # Skip headings and structural artifacts
        if cleaned.lower() == "folder structure":
            continue
        if is_structural_garbage(cleaned):
            continue

        depth = infer_depth(prefix)
        kind = classify_kind(label_raw)
        name = cleaned

        node = Node(name=name, note=note, kind=kind)

        while stack and stack[-1][0] > depth:
            stack.pop()
        if not stack:
            stack = [(0, root)]

        parent = stack[-1][1]
        parent.children.append(node)

        if kind == "folder":
            stack.append((depth + 1, node))

    return root


HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Folder Structure (Readable Tree)</title>
  <style>
    body {
      margin: 0;
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
      background: #0b0f14;
      color: #e6edf3;
    }
    header {
      padding: 12px 16px;
      border-bottom: 1px solid rgba(255,255,255,0.08);
      display: flex;
      gap: 14px;
      align-items: center;
      justify-content: space-between;
    }
    .title {
      font-size: 16px;
      font-weight: 650;
      letter-spacing: 0.2px;
    }
    .hint {
      font-size: 12px;
      opacity: 0.75;
      margin-top: 2px;
    }
    .left {
      display: flex;
      flex-direction: column;
      gap: 2px;
    }
    .right {
      display: flex;
      gap: 10px;
      align-items: center;
      font-size: 12px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    input[type="text"] {
      background: rgba(255,255,255,0.06);
      border: 1px solid rgba(255,255,255,0.12);
      color: #e6edf3;
      border-radius: 10px;
      padding: 8px 10px;
      min-width: 320px;
      outline: none;
    }
    button {
      background: rgba(255,255,255,0.06);
      border: 1px solid rgba(255,255,255,0.12);
      color: #e6edf3;
      border-radius: 10px;
      padding: 8px 10px;
      cursor: pointer;
    }
    button:hover {
      background: rgba(255,255,255,0.10);
    }
    #viz {
      width: 100vw;
      height: calc(100vh - 62px);
    }
    .link {
      fill: none;
      stroke: rgba(230,237,243,0.22);
      stroke-width: 1.2px;
    }
    .node circle {
      fill: #0b0f14;
      stroke-width: 2px;
    }
    .node text {
      font-size: 13px;
      dominant-baseline: middle;
      fill: rgba(230,237,243,0.95);
    }
    .badge {
      font-size: 10px;
      fill: rgba(230,237,243,0.75);
    }
    .highlight text {
      font-weight: 750;
      fill: #ffffff;
    }
    .highlight circle {
      stroke-width: 3px;
    }
    .tooltip {
      position: absolute;
      pointer-events: none;
      background: rgba(15, 23, 42, 0.94);
      border: 1px solid rgba(255,255,255,0.14);
      border-radius: 12px;
      padding: 10px 12px;
      font-size: 12px;
      max-width: 520px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.45);
      line-height: 1.35;
      display: none;
    }
    .tooltip .k {
      opacity: 0.65;
      font-size: 11px;
      margin-bottom: 4px;
    }
    .tooltip .n {
      font-weight: 750;
      margin-bottom: 6px;
      word-break: break-word;
    }
    .tooltip .p {
      opacity: 0.85;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      font-size: 11px;
      margin-bottom: 6px;
      word-break: break-word;
    }
    .tooltip .note {
      opacity: 0.95;
      word-break: break-word;
    }

    /* Legend */
    .legend {
      display: inline-flex;
      gap: 12px;
      align-items: center;
      padding: 6px 10px;
      border: 1px solid rgba(255,255,255,0.12);
      border-radius: 10px;
      background: rgba(255,255,255,0.04);
    }
    .legend-item {
      display: inline-flex;
      gap: 8px;
      align-items: center;
      font-size: 12px;
      opacity: 0.95;
      white-space: nowrap;
    }
    .legend-dot {
      width: 12px;
      height: 12px;
      border-radius: 999px;
      background: #0b0f14;
      border: 2px solid rgba(255,255,255,0.6);
      box-sizing: border-box;
    }
    .legend-dot.folder {
      border-color: rgba(125, 211, 252, 0.95);
    }
    .legend-dot.file {
      border-color: rgba(167, 139, 250, 0.95);
    }
  </style>
</head>
<body>
  <header>
    <div class="left">
      <div class="title">RLC-Agent Folder Map (Readable Tree)</div>
      <div class="hint">Click a node to expand/collapse. Hover for notes. Drag to pan. Scroll to zoom.</div>
    </div>
    <div class="right">
      <div class="legend" aria-label="Legend">
        <div class="legend-item">
          <span class="legend-dot folder" aria-hidden="true"></span>
          <span>Folder</span>
        </div>
        <div class="legend-item">
          <span class="legend-dot file" aria-hidden="true"></span>
          <span>File</span>
        </div>
      </div>

      <input id="search" type="text" placeholder="Search node name (e.g., scheduler, database, README)..." />
      <button id="expandAll">Expand all</button>
      <button id="collapseAll">Collapse all</button>
    </div>
  </header>

  <svg id="viz"></svg>
  <div id="tooltip" class="tooltip"></div>

  <script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
  <script>
    const data = __DATA_JSON__;

    // Build full-path strings for tooltips + search
    function addPaths(node, parentPath="") {
      const here = parentPath ? `${parentPath}/${node.name}` : node.name;
      node.path = here;
      if (node.children) node.children.forEach(ch => addPaths(ch, here));
    }
    addPaths(data, "");

    const svg = d3.select("#viz");
    const width = svg.node().clientWidth;
    const height = svg.node().clientHeight;

    const gRoot = svg
      .attr("viewBox", [0, 0, width, height])
      .append("g");

    const zoom = d3.zoom()
      .scaleExtent([0.4, 3.5])
      .on("zoom", (event) => gRoot.attr("transform", event.transform));

    svg.call(zoom);

    const tip = d3.select("#tooltip");

    const folderStroke = "rgba(125, 211, 252, 0.95)";
    const fileStroke   = "rgba(167, 139, 250, 0.95)";

    // d3 tree layout (left-to-right)
    const dx = 22;   // vertical spacing
    const dy = 260;  // horizontal spacing per depth (readability)
    const tree = d3.tree().nodeSize([dx, dy]);

    const root = d3.hierarchy(data);
    root.x0 = height / 2;
    root.y0 = 30;

    // Stable ids for transitions + search
    root.each(d => { d.id = d.data.path; });

    function collapse(d) {
      if (d.children) {
        d._children = d.children;
        d._children.forEach(collapse);
        d.children = null;
      }
    }

    function expand(d) {
      if (d._children) {
        d.children = d._children;
        d._children = null;
      }
      if (d.children) d.children.forEach(expand);
    }

    function diagonal(d) {
      return `M${d.source.y},${d.source.x}
              C${(d.source.y + d.target.y) / 2},${d.source.x}
               ${(d.source.y + d.target.y) / 2},${d.target.x}
               ${d.target.y},${d.target.x}`;
    }

    function escapeHtml(s) {
      return String(s)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    }

    function update(source) {
      const duration = 250;

      tree(root);
      const nodes = root.descendants();
      const links = root.links();

      // Dynamic height to reduce overlap when expanded
      let left = root;
      let right = root;
      root.eachBefore(n => {
        if (n.x < left.x) left = n;
        if (n.x > right.x) right = n;
      });

      const marginTop = 20;
      const marginBottom = 20;
      const marginLeft = 20;

      const newHeight = right.x - left.x + marginTop + marginBottom + 40;
      svg.attr("viewBox", [0, 0, width, newHeight]);

      // Normalize positions
      nodes.forEach(d => {
        d.y = d.depth * dy + marginLeft;
        d.x = d.x - left.x + marginTop;
      });

      // Links
      const link = gRoot.selectAll("path.link")
        .data(links, d => d.target.id);

      link.join(
        enter => enter.append("path")
          .attr("class", "link")
          .attr("d", () => {
            const o = {x: source.x0, y: source.y0};
            return diagonal({source: o, target: o});
          }),
        update => update,
        exit => exit.transition().duration(duration)
          .attr("d", () => {
            const o = {x: source.x, y: source.y};
            return diagonal({source: o, target: o});
          })
          .remove()
      ).transition().duration(duration)
        .attr("d", diagonal);

      // Nodes
      const node = gRoot.selectAll("g.node")
        .data(nodes, d => d.id);

      const nodeEnter = node.enter().append("g")
        .attr("class", "node")
        .attr("transform", () => `translate(${source.y0},${source.x0})`)
        .on("click", (event, d) => {
          if (d.children) {
            d._children = d.children;
            d.children = null;
          } else {
            d.children = d._children;
            d._children = null;
          }
          update(d);
        })
        .on("mousemove", (event, d) => {
          const note = (d.data.note || "").trim();
          tip.style("display", "block")
            .style("left", (event.pageX + 14) + "px")
            .style("top", (event.pageY + 14) + "px")
            .html(`
              <div class="k">${(d.data.kind || "node").toUpperCase()}</div>
              <div class="n">${escapeHtml(d.data.name || "")}</div>
              <div class="p">${escapeHtml(d.data.path || "")}</div>
              <div class="note">${note ? escapeHtml(note) : "<span style='opacity:0.65'>No note provided</span>"}</div>
            `);
        })
        .on("mouseleave", () => tip.style("display", "none"));

      nodeEnter.append("circle")
        .attr("r", 6)
        .attr("stroke", d => (d.data.kind === "folder" ? folderStroke : fileStroke));

      nodeEnter.append("text")
        .attr("x", 12)
        .attr("dy", 0)
        .text(d => d.data.name);

      nodeEnter.append("text")
        .attr("class", "badge")
        .attr("x", 12)
        .attr("dy", 14)
        .text(d => (d.data.note && d.data.note.trim().length ? "note" : ""));

      const nodeUpdate = nodeEnter.merge(node);

      nodeUpdate.transition().duration(duration)
        .attr("transform", d => `translate(${d.y},${d.x})`);

      nodeUpdate.select("circle")
        .attr("stroke", d => (d.data.kind === "folder" ? folderStroke : fileStroke));

      node.exit().transition().duration(duration)
        .attr("transform", () => `translate(${source.y},${source.x})`)
        .remove();

      // Stash for transitions
      nodes.forEach(d => {
        d.x0 = d.x;
        d.y0 = d.y;
      });
    }

    // Search + highlight
    const searchEl = document.getElementById("search");

    function clearHighlights() {
      gRoot.selectAll("g.node").classed("highlight", false);
    }

    function findNodes(term) {
      term = term.trim().toLowerCase();
      if (!term) return [];
      return root.descendants().filter(d => (d.data.name || "").toLowerCase().includes(term));
    }

    function revealNode(d) {
      let p = d.parent;
      while (p) {
        if (p._children) {
          p.children = p._children;
          p._children = null;
        }
        p = p.parent;
      }
    }

    searchEl.addEventListener("input", () => {
      const term = searchEl.value;
      clearHighlights();
      if (!term.trim()) return;

      const matches = findNodes(term);
      if (!matches.length) return;

      matches.forEach(revealNode);
      update(root);

      setTimeout(() => {
        gRoot.selectAll("g.node")
          .filter(d => matches.some(m => m.id === d.id))
          .classed("highlight", true);

        const first = matches[0];
        const tx = 40 - first.y;
        const ty = 80 - first.x;
        svg.transition().duration(250).call(zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(1.0));
      }, 20);
    });

    // Expand/Collapse All
    document.getElementById("expandAll").addEventListener("click", () => {
      expand(root);
      update(root);
    });

    document.getElementById("collapseAll").addEventListener("click", () => {
      if (root.children) root.children.forEach(collapse);
      update(root);
    });

    // Initial render
    update(root);
  </script>
</body>
</html>
"""


def write_html(root: Node, out_path: Path) -> None:
    data_json = json.dumps(root.to_dict(), ensure_ascii=False)
    out_path.write_text(HTML_TEMPLATE.replace("__DATA_JSON__", data_json), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", type=str, default="", help="Path to text file containing folder tree")
    ap.add_argument(
        "--out",
        dest="outfile",
        type=str,
        default="folder_tree_readable.html",
        help="Output HTML path",
    )
    args = ap.parse_args()

    text = Path(args.infile).read_text(encoding="utf-8", errors="replace") if args.infile else EMBEDDED_TREE
    root = parse_tree(text)

    out_path = Path(args.outfile).resolve()
    write_html(root, out_path)

    print(f"Wrote: {out_path}")
    print("Open it in a browser. Use search to jump/highlight. Click nodes to collapse/expand.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
