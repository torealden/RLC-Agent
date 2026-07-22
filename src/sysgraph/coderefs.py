"""Step 5 -- code to relation edges, resolve-or-flag.

Design section 1 measured 1,188 naive schema-qualified references in the repo, of which 539
resolved against the live catalog and 649 did not. Building without validation therefore
yields a graph that is half fiction. So every reference here is checked against the catalog,
and one that does not resolve is *still stored* -- marked unresolved, excluded from default
answers, and surfaced in a report. 649 dead references are themselves a finding.

Confidence is a statement about the parser, not about the code:
  0.90  extracted from a Python string literal via AST, with an unambiguous SQL verb in front
  0.70  same, from a .sql or .bas file (no AST, but the verb is still there)
  0.40  a bare mention with no verb context -- a name in a docstring, a log line, an f-string
        fragment. Stored, labelled intent='mention', and worth exactly what it looks like.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

SCHEMAS = (
    "bronze|silver|gold|core|reference|reports|audit|sales|risk|meta|config|public|"
    "sandbox_reference|sys"
)
RELN = re.compile(rf"\b({SCHEMAS})\.([a-zA-Z_][a-zA-Z0-9_]*)")

WRITE_CTX = re.compile(
    r"(insert\s+into|update|delete\s+from|truncate\s+(table\s+)?|merge\s+into|"
    r"create\s+(or\s+replace\s+)?(table|view|materialized\s+view)\s+(if\s+not\s+exists\s+)?|"
    r"drop\s+(table|view|materialized\s+view)\s+(if\s+exists\s+)?|copy\s+|refresh\s+materialized\s+view\s+)$",
    re.I,
)
READ_CTX = re.compile(r"(from|join|using)\s+$", re.I)

SKIP_DIRS = {".git", "__pycache__", "node_modules", ".venv", "venv"}


def refs_in_text(text: str) -> list[tuple[str, str, str]]:
    """-> [(relation_key, intent, snippet)] where intent is 'write' | 'read' | 'mention'."""
    out = []
    for m in RELN.finditer(text):
        rel = f"{m.group(1)}.{m.group(2)}"
        before = text[max(0, m.start() - 60): m.start()]
        # collapse newlines so a verb on the previous line still counts
        before_flat = re.sub(r"\s+", " ", before)
        if WRITE_CTX.search(before_flat):
            intent = "write"
        elif READ_CTX.search(before_flat):
            intent = "read"
        else:
            intent = "mention"
        snippet = re.sub(r"\s+", " ", text[max(0, m.start() - 40): m.end() + 20]).strip()
        out.append((rel, intent, snippet))
    return out


def _python_string_refs(path: Path) -> tuple[list[tuple[str, str, str, int]], bool]:
    """AST pass: only string constants, so a variable named `gold_view` is not a reference.
    Returns (refs, ast_ok)."""
    src = path.read_text(encoding="utf-8", errors="replace")
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return [], False
    refs = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            for rel, intent, snip in refs_in_text(node.value):
                refs.append((rel, intent, snip, getattr(node, "lineno", 0)))
    return refs, True


def extract(conn, store, live_relations: set[str]) -> dict:
    stats = {
        "py_files_scanned": 0, "py_ast_ok": 0, "py_ast_failed": 0,
        "sql_files_scanned": 0, "bas_files_scanned": 0,
        "refs_total": 0, "refs_resolved": 0, "refs_unresolved": 0,
        "edges_read": 0, "edges_write": 0, "edges_mention": 0,
    }
    unresolved_seen: set[str] = set()

    def emit(node_key: str, rel: str, intent: str, snippet: str, line: int, method: str, conf: float):
        stats["refs_total"] += 1
        resolved = rel in live_relations
        if resolved:
            stats["refs_resolved"] += 1
        else:
            stats["refs_unresolved"] += 1
            if rel not in unresolved_seen:
                unresolved_seen.add(rel)
                store.add_node(
                    "db_relation", rel, label=rel,
                    properties={"phantom": True, "note": "referenced in code, absent from catalog"},
                    extraction_method=method, confidence=0.40,
                    resolution_status="unresolved",
                )
        edge_type = "WRITES" if intent == "write" else "READS"
        # Direction convention (design 5.2): edges follow the data.
        src, tgt = (node_key, rel) if intent == "write" else (rel, node_key)
        store.add_edge(
            src, edge_type, tgt,
            properties={"intent": intent},
            evidence={"line": line, "snippet": snippet[:180], "refs": 1},
            extraction_method=method,
            confidence=conf if intent != "mention" else 0.40,
            resolution_status="resolved" if resolved else "unresolved",
        )
        stats["edges_" + ("write" if intent == "write" else "read" if intent == "read" else "mention")] += 1

    for rel_path, node_key, kind in _iter_tracked(store):
        path = ROOT / rel_path
        if not path.exists():
            continue

        if kind == "py":
            stats["py_files_scanned"] += 1
            refs, ok = _python_string_refs(path)
            if ok:
                stats["py_ast_ok"] += 1
                for r, intent, snip, line in refs:
                    emit(node_key, r, intent, snip, line, "python_ast",
                         0.90 if intent != "mention" else 0.40)
            else:
                stats["py_ast_failed"] += 1
                text = path.read_text(encoding="utf-8", errors="replace")
                for r, intent, snip in refs_in_text(text):
                    emit(node_key, r, intent, snip, 0, "regex", 0.40)
        else:
            stats["sql_files_scanned" if kind == "sql" else "bas_files_scanned"] += 1
            text = path.read_text(encoding="utf-8", errors="replace")
            method = "sql_parse" if kind == "sql" else "vba_parse"
            for r, intent, snip in refs_in_text(text):
                emit(node_key, r, intent, snip, 0, method,
                     0.70 if intent != "mention" else 0.40)

    stats["distinct_unresolved_relations"] = len(unresolved_seen)
    stats["unresolved_relations"] = sorted(unresolved_seen)
    return stats


def _iter_tracked(store):
    """The nodes repo.py already created, so the two extractors cannot disagree about which
    files exist."""
    for key, node in list(store._nodes.items()):  # noqa: SLF001 -- same package, same scan
        if node.node_type == "sql_script":
            yield key[len("repo:"):], key, "sql"
        elif node.node_type == "repo_file" and not node.properties.get("phantom"):
            ext = node.properties.get("ext")
            if ext == ".py":
                yield key[len("repo:"):], key, "py"
            elif ext == ".bas":
                yield key[len("repo:"):], key, "bas"
