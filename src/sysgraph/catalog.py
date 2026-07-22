"""Step 2 -- catalog extractor: relations, and the view dependency edges pg_depend gives free.

This is the only extractor whose output is exact. Everything downstream is parsed, mined or
declared; this one is read straight out of the catalog at confidence 1.00, and check 8.1
holds it to zero drift in both directions.
"""

from __future__ import annotations

SKIP_SCHEMAS = ("pg_catalog", "information_schema", "pg_toast")

# Schemas whose contents are part of the system under study. `sandbox_reference` and `public`
# are included deliberately: 51 and 10 live relations respectively that the design's section-1
# survey missed entirely, and a graph that quietly omits them would answer blast-radius
# questions with a number that looks complete and is not.
RELATION_SQL = """
SELECT c.relnamespace::regnamespace::text AS schema_name,
       c.relname                          AS relation_name,
       CASE c.relkind WHEN 'r' THEN 'table'
                      WHEN 'v' THEN 'view'
                      WHEN 'm' THEN 'matview'
                      WHEN 'f' THEN 'foreign'
                      WHEN 'p' THEN 'partitioned'
                      ELSE c.relkind::text END AS kind,
       obj_description(c.oid, 'pg_class')  AS comment,
       c.oid                               AS oid
  FROM pg_class c
 WHERE c.relkind IN ('r','v','m','f','p')
   AND c.relnamespace::regnamespace::text NOT IN %(skip)s
   AND c.relnamespace::regnamespace::text NOT LIKE 'pg_%%'
"""

# View -> its source relations, straight from the rewrite rules. Self-references are excluded
# (a view always depends on itself through its own rewrite rule).
DEPEND_SQL = """
SELECT DISTINCT
       src.relnamespace::regnamespace::text || '.' || src.relname AS source_key,
       tgt.relnamespace::regnamespace::text || '.' || tgt.relname AS target_key
  FROM pg_depend d
  JOIN pg_rewrite r  ON d.objid = r.oid
  JOIN pg_class   tgt ON r.ev_class = tgt.oid
  JOIN pg_class   src ON d.refobjid = src.oid
 WHERE d.classid    = 'pg_rewrite'::regclass
   AND d.refclassid = 'pg_class'::regclass
   AND d.deptype    = 'n'
   AND r.ev_class  <> d.refobjid
   AND src.relnamespace::regnamespace::text NOT LIKE 'pg_%%'
   AND tgt.relnamespace::regnamespace::text NOT LIKE 'pg_%%'
"""

COLUMN_SQL = """
SELECT c.relnamespace::regnamespace::text AS schema_name,
       c.relname                          AS relation_name,
       a.attname                          AS column_name,
       format_type(a.atttypid, a.atttypmod) AS data_type,
       a.attnum
  FROM pg_class c
  JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
 WHERE c.relkind IN ('r','v','m','f','p')
   AND c.relnamespace::regnamespace::text NOT IN %(skip)s
   AND c.relnamespace::regnamespace::text NOT LIKE 'pg_%%'
"""


def extract(conn, store) -> dict:
    cur = conn.cursor()

    # --- columns first, so each relation node can carry its column list ---
    cur.execute(COLUMN_SQL, {"skip": SKIP_SCHEMAS})
    cols: dict[str, list[str]] = {}
    for r in cur.fetchall():
        cols.setdefault(f"{r['schema_name']}.{r['relation_name']}", []).append(r["column_name"])

    cur.execute(RELATION_SQL, {"skip": SKIP_SCHEMAS})
    relations = cur.fetchall()
    for r in relations:
        key = f"{r['schema_name']}.{r['relation_name']}"
        store.add_node(
            "db_relation",
            key,
            label=key,
            properties={
                "schema": r["schema_name"],
                "kind": r["kind"],
                "comment": r["comment"],
                "columns": sorted(cols.get(key, [])),
                "column_count": len(cols.get(key, [])),
            },
            extraction_method="pg_catalog",
            confidence=1.00,
        )

    live_keys = {f"{r['schema_name']}.{r['relation_name']}" for r in relations}

    # --- DERIVES_FROM: stored source -> dependent view, i.e. the way data flows.
    # The edge type reads backwards against its own arrow on purpose; the direction
    # convention (design 5.2) is fixed for the whole graph so traversal never special-cases.
    cur.execute(DEPEND_SQL)
    dep_rows = cur.fetchall()
    n_dep = 0
    for r in dep_rows:
        if r["source_key"] not in live_keys or r["target_key"] not in live_keys:
            continue
        store.add_edge(
            r["source_key"], "DERIVES_FROM", r["target_key"],
            extraction_method="pg_catalog", confidence=1.00,
            evidence={"via": "pg_rewrite"},
        )
        n_dep += 1

    return {
        "relations": len(relations),
        "columns_seen": sum(len(v) for v in cols.values()),
        "derives_from_edges": n_dep,
        "live_relation_keys": len(live_keys),
    }
