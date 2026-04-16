"""
Backfill core.kg_provenance from source_doc fields already embedded in
kg_context.context_value JSONB. Every context that names a source_doc
but has no provenance row gets one — fixing the empty join table.

Also backfills edges (properties->>'source_key' convention) where applicable.
"""

import json
import os
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / '.env')


def connect():
    return psycopg2.connect(
        host=os.environ['RLC_PG_HOST'],
        port=os.environ.get('RLC_PG_PORT', 5432),
        database=os.environ.get('RLC_PG_DATABASE', 'rlc_commodities'),
        user=os.environ['RLC_PG_USER'],
        password=os.environ['RLC_PG_PASSWORD'],
        sslmode='require',
    )


def main():
    conn = connect()
    cur = conn.cursor()

    # Load all sources into a lookup
    cur.execute("SELECT id, source_key FROM core.kg_source")
    sources_by_key = {}
    for row in cur.fetchall():
        src_id, src_key = row
        sources_by_key[src_key] = src_id
        # Also map the gdoc_ prefix variant used in HOBO contexts
        if src_key.startswith('gdoc_'):
            sources_by_key[src_key[5:]] = src_id
    print(f"Loaded {len(sources_by_key)} source keys")

    # Contexts with source_doc
    cur.execute("""
        SELECT c.id, c.node_id, c.context_key, c.context_value->>'source_doc' AS sd
        FROM core.kg_context c
        WHERE c.context_value ? 'source_doc'
    """)
    ctx_rows = cur.fetchall()
    print(f"Contexts referencing source_doc: {len(ctx_rows)}")

    ctx_linked = 0
    ctx_unmatched = set()
    for ctx_id, node_id, ctx_key, sd in ctx_rows:
        if sd in sources_by_key:
            cur.execute("""
                INSERT INTO core.kg_provenance
                    (entity_type, entity_id, source_id, extracted_at, extraction_method,
                     source_excerpt, source_confidence)
                VALUES ('context', %s, %s, NOW(), 'backfill_from_source_doc', %s, 0.85)
                ON CONFLICT DO NOTHING
            """, (ctx_id, sources_by_key[sd], f'Backfilled from context_value.source_doc: {sd}'))
            ctx_linked += 1
        else:
            ctx_unmatched.add(sd)

    print(f"  Linked: {ctx_linked}")
    print(f"  Unmatched source_doc values: {sorted(ctx_unmatched)}")

    # Edges with properties->>'source_doc' or 'source_key'
    cur.execute("""
        SELECT e.id, e.properties->>'source_doc' AS sd, e.properties->>'source_key' AS sk
        FROM core.kg_edge e
        WHERE e.properties ? 'source_doc' OR e.properties ? 'source_key'
    """)
    edge_rows = cur.fetchall()
    print(f"Edges referencing source_doc/source_key: {len(edge_rows)}")

    edge_linked = 0
    edge_unmatched = set()
    for e_id, sd, sk in edge_rows:
        key = sd or sk
        if key in sources_by_key:
            cur.execute("""
                INSERT INTO core.kg_provenance
                    (entity_type, entity_id, source_id, extracted_at, extraction_method,
                     source_excerpt, source_confidence)
                VALUES ('edge', %s, %s, NOW(), 'backfill_from_source_doc', %s, 0.85)
                ON CONFLICT DO NOTHING
            """, (e_id, sources_by_key[key], f'Backfilled: {key}'))
            edge_linked += 1
        else:
            edge_unmatched.add(key)

    print(f"  Linked: {edge_linked}")
    print(f"  Unmatched: {sorted(edge_unmatched)}")

    conn.commit()

    # Final audit
    cur.execute("""SELECT entity_type, COUNT(*) FROM core.kg_provenance GROUP BY entity_type ORDER BY entity_type""")
    print("\nkg_provenance row counts:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]}")

    cur.execute("""SELECT COUNT(*) FROM core.kg_provenance""")
    total = cur.fetchone()[0]
    cur.execute("""SELECT COUNT(*) FROM core.kg_context""")
    ctx_total = cur.fetchone()[0]
    print(f"\nTotal provenance rows: {total}")
    print(f"Coverage: {total / ctx_total * 100:.1f}% of contexts now have at least one provenance row (plus node + edge rows)")

    conn.close()


if __name__ == '__main__':
    main()
