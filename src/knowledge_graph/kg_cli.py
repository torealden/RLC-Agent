"""
Knowledge Graph CLI — Manual entry tool for KG nodes, edges, and contexts.

Provides a command-line interface for adding, viewing, and searching
the knowledge graph. Useful for ad-hoc additions without writing SQL.

Usage:
    python -m src.knowledge_graph.kg_cli search <query>
    python -m src.knowledge_graph.kg_cli show <node_key>
    python -m src.knowledge_graph.kg_cli add-node <node_key> <label> <node_type>
    python -m src.knowledge_graph.kg_cli add-edge <source_key> <target_key> <edge_type> [--mechanism "..."]
    python -m src.knowledge_graph.kg_cli add-context <node_key> <context_type> <context_key> <json_value>
    python -m src.knowledge_graph.kg_cli stats
    python -m src.knowledge_graph.kg_cli list-types
"""

import argparse
import json
import logging
import sys
from typing import Optional

logger = logging.getLogger(__name__)


def _get_connection():
    from src.services.database.db_config import get_connection
    return get_connection()


def _get_kg():
    from src.knowledge_graph.kg_manager import KGManager
    return KGManager()


def cmd_stats(args):
    """Show KG statistics."""
    with _get_connection() as conn:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) as cnt FROM core.kg_node")
        nodes = cur.fetchone()['cnt']

        cur.execute("SELECT COUNT(*) as cnt FROM core.kg_edge")
        edges = cur.fetchone()['cnt']

        cur.execute("SELECT COUNT(*) as cnt FROM core.kg_context")
        contexts = cur.fetchone()['cnt']

        cur.execute("SELECT COUNT(*) as cnt FROM core.kg_source")
        sources = cur.fetchone()['cnt']

        print(f"Knowledge Graph Stats:")
        print(f"  Nodes:    {nodes}")
        print(f"  Edges:    {edges}")
        print(f"  Contexts: {contexts}")
        print(f"  Sources:  {sources}")

        # Breakdown by source
        cur.execute("""
            SELECT source, context_type, COUNT(*) as cnt
            FROM core.kg_context
            GROUP BY source, context_type
            ORDER BY source, context_type
        """)
        print(f"\nContext breakdown:")
        for r in cur.fetchall():
            print(f"  {r['source'] or 'null':12s} {r['context_type']:20s} {r['cnt']:3d}")

        # Node types
        cur.execute("""
            SELECT node_type, COUNT(*) as cnt
            FROM core.kg_node
            GROUP BY node_type
            ORDER BY cnt DESC
        """)
        print(f"\nNode types:")
        for r in cur.fetchall():
            print(f"  {r['node_type']:25s} {r['cnt']:3d}")


def cmd_search(args):
    """Search nodes by label or key."""
    query = args.query
    kg = _get_kg()

    # Search both label and key
    by_label = kg.search_nodes(label_pattern=query)
    by_key = kg.search_nodes(key_pattern=query)

    # Merge results, deduplicate by node_key
    seen = set()
    results = []
    for n in by_label + by_key:
        if n['node_key'] not in seen:
            seen.add(n['node_key'])
            results.append(n)

    if not results:
        print(f"No nodes matching '{query}'")
        return

    print(f"Found {len(results)} nodes matching '{query}':")
    for n in results:
        print(f"  [{n['node_type']:20s}] {n['node_key']:40s} {n['label']}")


def cmd_show(args):
    """Show full details for a node."""
    kg = _get_kg()
    enriched = kg.get_enriched_context(args.node_key)

    if enriched is None:
        print(f"Node not found: {args.node_key}")
        return

    node = enriched['node']
    print(f"Node: {node['node_key']}")
    print(f"  Label: {node['label']}")
    print(f"  Type:  {node['node_type']}")
    if node.get('properties'):
        print(f"  Props: {json.dumps(node['properties'], indent=4)}")

    contexts = enriched.get('contexts', [])
    if contexts:
        print(f"\nContexts ({len(contexts)}):")
        for ctx in contexts:
            cv = ctx.get('context_value', {})
            preview = json.dumps(cv)
            if len(preview) > 120:
                preview = preview[:120] + '...'
            print(f"  [{ctx['context_type']:20s}] {ctx['context_key']:40s}")
            print(f"    source={ctx.get('source', '?')}  when={ctx.get('applicable_when', '?')}")
            print(f"    {preview}")

    edges = enriched.get('edges', [])
    if edges:
        print(f"\nEdges ({len(edges)}):")
        for e in edges:
            src = e.get('source_label', e.get('source_key', '?'))
            tgt = e.get('target_label', e.get('target_key', '?'))
            print(f"  {src} --{e['edge_type']}--> {tgt}")
            if e.get('properties', {}).get('mechanism'):
                mech = e['properties']['mechanism']
                if len(mech) > 100:
                    mech = mech[:100] + '...'
                print(f"    mechanism: {mech}")


def cmd_add_node(args):
    """Add a new node to the KG."""
    properties = {}
    if args.properties:
        try:
            properties = json.loads(args.properties)
        except json.JSONDecodeError:
            print(f"Error: --properties must be valid JSON")
            sys.exit(1)

    with _get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO core.kg_node (node_key, label, node_type, properties)
            VALUES (%s, %s, %s, %s::jsonb)
            ON CONFLICT (node_key) DO UPDATE SET
                label = EXCLUDED.label,
                node_type = EXCLUDED.node_type,
                properties = EXCLUDED.properties
            RETURNING id, (xmax = 0) AS was_inserted
        """, (args.node_key, args.label, args.node_type, json.dumps(properties)))
        row = cur.fetchone()
        conn.commit()
        action = "Created" if row['was_inserted'] else "Updated"
        print(f"{action} node: {args.node_key} (id={row['id']})")


def cmd_add_edge(args):
    """Add an edge between two nodes."""
    properties = {}
    if args.mechanism:
        properties['mechanism'] = args.mechanism

    with _get_connection() as conn:
        cur = conn.cursor()

        # Look up source and target node IDs
        cur.execute("SELECT id FROM core.kg_node WHERE node_key = %s", (args.source_key,))
        src = cur.fetchone()
        if not src:
            print(f"Error: source node not found: {args.source_key}")
            sys.exit(1)

        cur.execute("SELECT id FROM core.kg_node WHERE node_key = %s", (args.target_key,))
        tgt = cur.fetchone()
        if not tgt:
            print(f"Error: target node not found: {args.target_key}")
            sys.exit(1)

        cur.execute("""
            INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, properties)
            VALUES (%s, %s, %s, %s::jsonb)
            RETURNING id
        """, (src['id'], tgt['id'], args.edge_type, json.dumps(properties)))
        row = cur.fetchone()
        conn.commit()
        print(f"Created edge: {args.source_key} --{args.edge_type}--> {args.target_key} (id={row['id']})")


def cmd_add_context(args):
    """Add a context entry to a node."""
    try:
        context_value = json.loads(args.json_value)
    except json.JSONDecodeError:
        print(f"Error: json_value must be valid JSON")
        sys.exit(1)

    kg = _get_kg()
    result = kg.upsert_context(
        node_key=args.node_key,
        context_type=args.context_type,
        context_key=args.context_key,
        context_value=context_value,
        applicable_when=args.when or 'always',
        source=args.source or 'manual',
    )
    print(f"{result['action'].title()} context: {args.context_key} on {args.node_key} (id={result['context_id']})")


def cmd_list_types(args):
    """List all node types and edge types in the KG."""
    with _get_connection() as conn:
        cur = conn.cursor()

        cur.execute("""
            SELECT node_type, COUNT(*) as cnt
            FROM core.kg_node GROUP BY node_type ORDER BY cnt DESC
        """)
        print("Node types:")
        for r in cur.fetchall():
            print(f"  {r['node_type']:25s} ({r['cnt']})")

        cur.execute("""
            SELECT edge_type, COUNT(*) as cnt
            FROM core.kg_edge GROUP BY edge_type ORDER BY cnt DESC
        """)
        print("\nEdge types:")
        for r in cur.fetchall():
            print(f"  {r['edge_type']:25s} ({r['cnt']})")

        cur.execute("""
            SELECT context_type, COUNT(*) as cnt
            FROM core.kg_context GROUP BY context_type ORDER BY cnt DESC
        """)
        print("\nContext types:")
        for r in cur.fetchall():
            print(f"  {r['context_type']:25s} ({r['cnt']})")


def main():
    parser = argparse.ArgumentParser(description='Knowledge Graph CLI')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # stats
    subparsers.add_parser('stats', help='Show KG statistics')

    # search
    p = subparsers.add_parser('search', help='Search nodes')
    p.add_argument('query', help='Search term')

    # show
    p = subparsers.add_parser('show', help='Show node details')
    p.add_argument('node_key', help='Node key to display')

    # add-node
    p = subparsers.add_parser('add-node', help='Add a node')
    p.add_argument('node_key', help='Unique node key')
    p.add_argument('label', help='Display label')
    p.add_argument('node_type', help='Node type (commodity, data_series, model, etc.)')
    p.add_argument('--properties', help='JSON properties', default=None)

    # add-edge
    p = subparsers.add_parser('add-edge', help='Add an edge')
    p.add_argument('source_key', help='Source node key')
    p.add_argument('target_key', help='Target node key')
    p.add_argument('edge_type', help='Edge type (CAUSES, PREDICTS, etc.)')
    p.add_argument('--mechanism', help='Mechanism description', default=None)

    # add-context
    p = subparsers.add_parser('add-context', help='Add context to a node')
    p.add_argument('node_key', help='Node key')
    p.add_argument('context_type', help='Context type (expert_rule, risk_threshold, etc.)')
    p.add_argument('context_key', help='Context key')
    p.add_argument('json_value', help='Context value as JSON string')
    p.add_argument('--when', help='Applicable when condition', default='always')
    p.add_argument('--source', help='Source (manual, computed, etc.)', default='manual')

    # list-types
    subparsers.add_parser('list-types', help='List all node/edge/context types')

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        'stats': cmd_stats,
        'search': cmd_search,
        'show': cmd_show,
        'add-node': cmd_add_node,
        'add-edge': cmd_add_edge,
        'add-context': cmd_add_context,
        'list-types': cmd_list_types,
    }

    commands[args.command](args)


if __name__ == '__main__':
    main()
