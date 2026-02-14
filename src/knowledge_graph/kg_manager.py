"""
Knowledge Graph Manager

Clean Python interface to the analyst knowledge graph (core.kg_node,
core.kg_edge, core.kg_context). Used by MCP tools, future enrichment
agents, and CLI tools.

Usage:
    from src.knowledge_graph import KGManager

    kg = KGManager()
    stats = kg.get_stats()
    nodes = kg.search_nodes(node_type='commodity')
    context = kg.get_enriched_context('corn')
"""

import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, date
from decimal import Decimal

logger = logging.getLogger(__name__)


def _serialize(obj):
    """Convert non-serializable types to JSON-safe values."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def _clean_row(row: dict) -> dict:
    """Ensure all values in a row dict are JSON-serializable."""
    return {k: _serialize(v) for k, v in row.items()}


class KGManager:
    """
    Read-only interface to the RLC Knowledge Graph.

    All methods return plain dicts/lists (JSON-serializable).
    Each call opens and closes its own DB connection.
    """

    def _get_connection(self):
        from src.services.database.db_config import get_connection
        return get_connection()

    # ------------------------------------------------------------------
    # search_nodes
    # ------------------------------------------------------------------
    def search_nodes(
        self,
        node_type: Optional[str] = None,
        label_pattern: Optional[str] = None,
        key_pattern: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Search kg_node by type, label (ILIKE), or key pattern.

        Returns list of dicts: id, node_type, node_key, label, properties.
        """
        conditions = []
        params = []

        if node_type:
            conditions.append("node_type = %s")
            params.append(node_type)
        if label_pattern:
            conditions.append("label ILIKE %s")
            params.append(f"%{label_pattern}%")
        if key_pattern:
            conditions.append("node_key ILIKE %s")
            params.append(f"%{key_pattern}%")

        where = " AND ".join(conditions) if conditions else "TRUE"
        sql = f"""
            SELECT id, node_type, node_key, label, properties
            FROM core.kg_node
            WHERE {where}
            ORDER BY node_type, label
            LIMIT %s
        """
        params.append(limit)

        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, tuple(params))
            return [_clean_row(dict(row)) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # get_node
    # ------------------------------------------------------------------
    def get_node(self, node_key: str) -> Optional[Dict[str, Any]]:
        """Get a single node by its node_key. Returns None if not found."""
        sql = """
            SELECT id, node_type, node_key, label, properties
            FROM core.kg_node
            WHERE node_key = %s
        """
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, (node_key,))
            row = cur.fetchone()
            return _clean_row(dict(row)) if row else None

    # ------------------------------------------------------------------
    # get_node_context
    # ------------------------------------------------------------------
    def get_node_context(self, node_key: str) -> List[Dict[str, Any]]:
        """Get all kg_context entries for a node (by node_key)."""
        sql = """
            SELECT c.id, c.context_type, c.context_key, c.context_value,
                   c.applicable_when, c.source, c.last_updated
            FROM core.kg_context c
            JOIN core.kg_node n ON n.id = c.node_id
            WHERE n.node_key = %s
            ORDER BY c.context_type, c.context_key
        """
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, (node_key,))
            return [_clean_row(dict(row)) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # get_node_edges
    # ------------------------------------------------------------------
    def get_node_edges(
        self,
        node_key: str,
        edge_type: Optional[str] = None,
        direction: str = 'both',
    ) -> List[Dict[str, Any]]:
        """
        Get edges from/to a node, enriched with connected node labels.

        Args:
            node_key: The node to query edges for
            edge_type: Optional filter by edge type (CAUSES, LEADS, etc.)
            direction: 'outgoing', 'incoming', or 'both' (default)

        Returns:
            List of edge dicts with source/target node info.
        """
        type_filter = ""
        base_params = []
        if edge_type:
            type_filter = "AND e.edge_type = %s"
            base_params = [edge_type]

        queries = []
        params = []

        if direction in ('outgoing', 'both'):
            queries.append(f"""
                SELECT e.id, e.edge_type, e.weight, e.properties,
                       e.confidence, e.created_by,
                       'outgoing' AS direction,
                       sn.node_key AS source_key, sn.label AS source_label,
                       tn.node_key AS target_key, tn.label AS target_label
                FROM core.kg_edge e
                JOIN core.kg_node sn ON sn.id = e.source_node_id
                JOIN core.kg_node tn ON tn.id = e.target_node_id
                WHERE sn.node_key = %s {type_filter}
            """)
            params.extend([node_key] + base_params)

        if direction in ('incoming', 'both'):
            queries.append(f"""
                SELECT e.id, e.edge_type, e.weight, e.properties,
                       e.confidence, e.created_by,
                       'incoming' AS direction,
                       sn.node_key AS source_key, sn.label AS source_label,
                       tn.node_key AS target_key, tn.label AS target_label
                FROM core.kg_edge e
                JOIN core.kg_node sn ON sn.id = e.source_node_id
                JOIN core.kg_node tn ON tn.id = e.target_node_id
                WHERE tn.node_key = %s {type_filter}
            """)
            params.extend([node_key] + base_params)

        if not queries:
            return []

        sql = " UNION ALL ".join(queries) + " ORDER BY edge_type, direction"

        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, tuple(params))
            return [_clean_row(dict(row)) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # get_enriched_context
    # ------------------------------------------------------------------
    def get_enriched_context(self, node_key: str) -> Optional[Dict[str, Any]]:
        """
        The main LLM query: node + all contexts + all edges.

        This is the "what would an analyst think about this?" query.
        Returns None if node_key does not exist.
        """
        node = self.get_node(node_key)
        if node is None:
            return None

        contexts = self.get_node_context(node_key)
        edges = self.get_node_edges(node_key, direction='both')

        return {
            "node": node,
            "contexts": contexts,
            "edges": edges,
            "summary": {
                "context_count": len(contexts),
                "edge_count": len(edges),
                "context_types": sorted(set(c["context_type"] for c in contexts)),
                "edge_types": sorted(set(e["edge_type"] for e in edges)),
            }
        }

    # ------------------------------------------------------------------
    # get_stats
    # ------------------------------------------------------------------
    def get_stats(self) -> Dict[str, Any]:
        """Node/edge/context counts grouped by type."""
        with self._get_connection() as conn:
            cur = conn.cursor()

            # Totals
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM core.kg_node) AS total_nodes,
                    (SELECT COUNT(*) FROM core.kg_edge) AS total_edges,
                    (SELECT COUNT(*) FROM core.kg_context) AS total_contexts
            """)
            totals = dict(cur.fetchone())

            # Nodes by type
            cur.execute("""
                SELECT node_type, COUNT(*) AS count
                FROM core.kg_node
                GROUP BY node_type
                ORDER BY count DESC
            """)
            nodes_by_type = [dict(row) for row in cur.fetchall()]

            # Edges by type
            cur.execute("""
                SELECT edge_type, COUNT(*) AS count
                FROM core.kg_edge
                GROUP BY edge_type
                ORDER BY count DESC
            """)
            edges_by_type = [dict(row) for row in cur.fetchall()]

            # Contexts by type
            cur.execute("""
                SELECT context_type, COUNT(*) AS count
                FROM core.kg_context
                GROUP BY context_type
                ORDER BY count DESC
            """)
            contexts_by_type = [dict(row) for row in cur.fetchall()]

            return {
                "totals": _clean_row(totals),
                "nodes_by_type": nodes_by_type,
                "edges_by_type": edges_by_type,
                "contexts_by_type": contexts_by_type,
            }
