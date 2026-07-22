"""Buffered writer for sys.node / sys.edge.

Extractors call add_node() / add_edge() with natural keys and never touch node ids. The store
resolves keys to ids at flush time, which is also where the two invariants live:

  * an edge endpoint that was never added as a node is a bug in the extractor, not a silent
    no-op -- flush raises rather than dropping the edge;
  * nodes and edges are upserted, and `last_seen_scan` is advanced. Nothing is deleted. A node
    that disappears from the repo simply stops having its last_seen_scan bumped, which turns
    "what vanished between scans" into a query (design 5.3).
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from psycopg2.extras import execute_values

from src.services.database.db_config import get_connection
from src.sysgraph import EXTRACTOR_VERSION


@dataclass
class _Node:
    node_type: str
    node_key: str
    label: str | None
    properties: dict
    lifecycle: str
    extraction_method: str
    confidence: float
    resolution_status: str


@dataclass
class _Edge:
    source_key: str
    target_key: str
    edge_type: str
    properties: dict
    evidence: dict
    extraction_method: str
    confidence: float
    resolution_status: str


class GraphStore:
    """Accumulates nodes and edges in memory, writes them once per flush()."""

    def __init__(self, conn, scan_id: int):
        self.conn = conn
        self.scan_id = scan_id
        self._nodes: dict[str, _Node] = {}
        self._edges: dict[tuple[str, str, str], _Edge] = {}
        self._ids: dict[str, int] = {}
        self.stats: dict[str, Any] = {}

    # -- accumulation ------------------------------------------------------

    def add_node(
        self,
        node_type: str,
        node_key: str,
        *,
        label: str | None = None,
        properties: dict | None = None,
        lifecycle: str = "unknown",
        extraction_method: str = "regex",
        confidence: float = 1.00,
        resolution_status: str = "resolved",
    ) -> str:
        existing = self._nodes.get(node_key)
        if existing is not None:
            # Same key seen twice: keep the higher-confidence description, merge properties.
            if confidence > existing.confidence:
                existing.extraction_method = extraction_method
                existing.confidence = confidence
                existing.resolution_status = resolution_status
                if label:
                    existing.label = label
            if lifecycle != "unknown":
                existing.lifecycle = lifecycle
            if properties:
                existing.properties.update(properties)
            return node_key

        self._nodes[node_key] = _Node(
            node_type=node_type,
            node_key=node_key,
            label=label or node_key,
            properties=properties or {},
            lifecycle=lifecycle,
            extraction_method=extraction_method,
            confidence=confidence,
            resolution_status=resolution_status,
        )
        return node_key

    def add_edge(
        self,
        source_key: str,
        edge_type: str,
        target_key: str,
        *,
        properties: dict | None = None,
        evidence: dict | None = None,
        extraction_method: str = "regex",
        confidence: float = 1.00,
        resolution_status: str = "resolved",
    ) -> None:
        k = (source_key, target_key, edge_type)
        prior = self._edges.get(k)
        if prior is not None:
            if confidence > prior.confidence:
                prior.confidence = confidence
                prior.extraction_method = extraction_method
                prior.resolution_status = resolution_status
            if evidence:
                prior.evidence = _merge_evidence(prior.evidence, evidence)
            if properties:
                prior.properties.update(properties)
            return

        self._edges[k] = _Edge(
            source_key=source_key,
            target_key=target_key,
            edge_type=edge_type,
            properties=properties or {},
            evidence=evidence or {},
            extraction_method=extraction_method,
            confidence=confidence,
            resolution_status=resolution_status,
        )

    def has_node(self, node_key: str) -> bool:
        return node_key in self._nodes

    # -- write -------------------------------------------------------------

    def flush(self) -> dict[str, int]:
        cur = self.conn.cursor()

        node_rows = [
            (
                n.node_type, n.node_key, n.label, json.dumps(n.properties),
                n.lifecycle, n.extraction_method, n.confidence, n.resolution_status,
                self.scan_id, self.scan_id,
            )
            for n in self._nodes.values()
        ]
        if node_rows:
            execute_values(
                cur,
                """
                INSERT INTO sys.node
                    (node_type, node_key, label, properties, lifecycle,
                     extraction_method, confidence, resolution_status,
                     first_seen_scan, last_seen_scan)
                VALUES %s
                ON CONFLICT (node_key) DO UPDATE SET
                    node_type         = EXCLUDED.node_type,
                    label             = EXCLUDED.label,
                    properties        = EXCLUDED.properties,
                    lifecycle         = CASE WHEN EXCLUDED.lifecycle = 'unknown'
                                             THEN sys.node.lifecycle ELSE EXCLUDED.lifecycle END,
                    extraction_method = EXCLUDED.extraction_method,
                    confidence        = EXCLUDED.confidence,
                    resolution_status = EXCLUDED.resolution_status,
                    last_seen_scan    = EXCLUDED.last_seen_scan
                """,
                node_rows,
                page_size=1000,
            )

        # Resolve every key we might need, including ones written by an earlier flush.
        keys = set(self._nodes) | {k for e in self._edges.values() for k in (e.source_key, e.target_key)}
        if keys:
            cur.execute(
                "SELECT node_key, node_id FROM sys.node WHERE node_key = ANY(%s)", (list(keys),)
            )
            for row in cur.fetchall():
                self._ids[row["node_key"]] = row["node_id"]

        edge_rows = []
        for e in self._edges.values():
            src = self._ids.get(e.source_key)
            tgt = self._ids.get(e.target_key)
            if src is None or tgt is None:
                missing = e.source_key if src is None else e.target_key
                raise RuntimeError(
                    f"edge {e.edge_type} {e.source_key} -> {e.target_key} references a node that "
                    f"was never added: {missing!r}. Extractors must add_node() before add_edge(); "
                    f"unresolved references get an explicit unresolved node, never a dropped edge."
                )
            edge_rows.append(
                (
                    src, tgt, e.edge_type, json.dumps(e.properties), json.dumps(e.evidence),
                    e.extraction_method, e.confidence, e.resolution_status,
                    self.scan_id, self.scan_id,
                )
            )
        if edge_rows:
            execute_values(
                cur,
                """
                INSERT INTO sys.edge
                    (source_node_id, target_node_id, edge_type, properties, evidence,
                     extraction_method, confidence, resolution_status,
                     first_seen_scan, last_seen_scan)
                VALUES %s
                ON CONFLICT (source_node_id, target_node_id, edge_type) DO UPDATE SET
                    properties        = EXCLUDED.properties,
                    evidence          = EXCLUDED.evidence,
                    extraction_method = EXCLUDED.extraction_method,
                    confidence        = EXCLUDED.confidence,
                    resolution_status = EXCLUDED.resolution_status,
                    last_seen_scan    = EXCLUDED.last_seen_scan
                """,
                edge_rows,
                page_size=1000,
            )

        written = {"nodes": len(node_rows), "edges": len(edge_rows)}
        self._nodes.clear()
        self._edges.clear()
        self.conn.commit()
        return written


def _merge_evidence(a: dict, b: dict) -> dict:
    """Evidence accumulates rather than overwrites -- two call sites for the same edge are
    both worth keeping, and `cells`/`refs` counters are how blast radius gets sized."""
    out = dict(a)
    for k, v in b.items():
        if k in out and isinstance(out[k], int) and isinstance(v, int):
            out[k] = out[k] + v
        elif k in out and isinstance(out[k], list) and isinstance(v, list):
            out[k] = (out[k] + v)[:20]
        else:
            out.setdefault(k, v)
    return out


def open_scan(conn, mode: str = "full") -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO sys.scan (git_sha, extractor_version, scan_mode)
        VALUES (%s, %s, %s) RETURNING scan_id
        """,
        (_git_sha(), EXTRACTOR_VERSION, mode),
    )
    scan_id = cur.fetchone()["scan_id"]
    conn.commit()
    return scan_id


def close_scan(conn, scan_id: int, status: str, stats: dict, failure_reason: str | None = None):
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE sys.scan
           SET finished_at = now(), status = %s, stats = %s, failure_reason = %s
         WHERE scan_id = %s
        """,
        (status, json.dumps(stats, default=str), failure_reason, scan_id),
    )
    conn.commit()


def record_check(conn, scan_id: int, name: str, binding: bool, passed: bool, detail: dict):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO sys.check_result (scan_id, check_name, binding, passed, detail)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (scan_id, check_name) DO UPDATE
            SET passed = EXCLUDED.passed, detail = EXCLUDED.detail
        """,
        (scan_id, name, binding, passed, json.dumps(detail, default=str)),
    )
    conn.commit()


def _git_sha() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True, text=True, timeout=10,
        ).stdout.strip() or None
    except Exception:
        return None
