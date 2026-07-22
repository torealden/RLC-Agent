-- 146_sys_system_graph.sql
-- The system graph: lineage across code, database and the spreadsheet estate.
-- Design: docs/specs/system_knowledge_graph_design_v1.md (D1-D6, R1-R4).
--
-- This is NOT the analyst knowledge graph. That lives in core.kg_* -- 436 hand-curated,
-- irreplaceable nodes. This schema holds ~20k machine-extracted nodes that are rebuilt from
-- scratch on every scan. Ruling D1: they never share a table, because one buggy rebuild
-- script must not be able to delete a year of curation. The single seam between them is
-- sys.declaration rows with predicate='SERVES' (design section 6).
--
-- Two shape decisions worth reading before you change anything here:
--
--   1. first_seen_scan / last_seen_scan instead of delete-and-reload. A node absent from the
--      current scan is not removed; its last_seen_scan simply stops advancing. "What
--      disappeared between scans" becomes a query, and rebuilds are non-destructive.
--
--   2. sys.declaration is never written by a scan. Every human ruling -- this workbook is
--      canonical, this series serves that analyst node -- lives here and is replayed onto the
--      graph after each rebuild. Same instinct as D1, one level down.

CREATE SCHEMA IF NOT EXISTS sys;

COMMENT ON SCHEMA sys IS
    'System graph: machine-extracted lineage over code, catalog and workbooks. Rebuilt every '
    'scan. Distinct from core.kg_* (hand-curated analyst knowledge graph) -- see '
    'docs/specs/system_knowledge_graph_design_v1.md D1.';


-- ---------------------------------------------------------------------------
-- sys.scan -- one row per extraction run
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sys.scan (
    scan_id           bigserial PRIMARY KEY,
    started_at        timestamptz NOT NULL DEFAULT now(),
    finished_at       timestamptz,
    git_sha           text,
    extractor_version text        NOT NULL,
    scan_mode         text        NOT NULL DEFAULT 'full',   -- full | cheap | expensive
    status            text        NOT NULL DEFAULT 'running', -- running | ok | failed
    failure_reason    text,
    stats             jsonb       NOT NULL DEFAULT '{}'::jsonb
);

COMMENT ON TABLE sys.scan IS
    'One row per extraction run. R3: the cheap half (catalog + repo inventory) runs nightly; '
    'the expensive half (openpyxl over the workbook estate) is hash-gated and rides along.';


-- ---------------------------------------------------------------------------
-- sys.node
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sys.node (
    node_id           bigserial PRIMARY KEY,
    node_type         text NOT NULL,
    node_key          text NOT NULL,
    label             text,
    properties        jsonb NOT NULL DEFAULT '{}'::jsonb,

    -- D4: lifecycle LABELS, resolution_status may EXCLUDE. Different claims, never one code path.
    lifecycle         text NOT NULL DEFAULT 'unknown',   -- canonical | superseded | archive | backup | unknown
    extraction_method text NOT NULL,                     -- pg_catalog | sql_parse | python_ast | vba_parse
                                                         -- | xlsx_formula | xlsx_extlink | regex | declared
    confidence        numeric(3,2) NOT NULL DEFAULT 1.00,
    resolution_status text NOT NULL DEFAULT 'resolved',  -- resolved | unresolved | ambiguous

    first_seen_scan   bigint NOT NULL REFERENCES sys.scan(scan_id),
    last_seen_scan    bigint NOT NULL REFERENCES sys.scan(scan_id),

    CONSTRAINT sys_node_key_uq UNIQUE (node_key),
    CONSTRAINT sys_node_type_ck CHECK (node_type IN (
        'db_relation','db_column','data_series','repo_file','vba_module','vba_procedure',
        'sql_script','scheduled_job','workbook','worksheet','sheet_block','flat_file_series',
        'deliverable','external_source')),
    CONSTRAINT sys_node_lifecycle_ck CHECK (lifecycle IN
        ('canonical','superseded','archive','backup','unknown')),
    CONSTRAINT sys_node_resolution_ck CHECK (resolution_status IN
        ('resolved','unresolved','ambiguous')),

    -- Check section 8.2, mechanically: node identity is never a cell address (D3).
    -- Excel positions rot and the oilseed folder alone holds 2.5M formula cells. Cell
    -- addresses may live in edge.evidence as observations; never in a key.
    CONSTRAINT sys_node_no_positional_key_ck CHECK (
        node_key !~ '#\$?[A-Z]{1,3}\$?[0-9]+$'
    )
);

CREATE INDEX IF NOT EXISTS sys_node_type_idx      ON sys.node (node_type);
CREATE INDEX IF NOT EXISTS sys_node_lastseen_idx  ON sys.node (last_seen_scan);
CREATE INDEX IF NOT EXISTS sys_node_lifecycle_idx ON sys.node (lifecycle) WHERE lifecycle <> 'unknown';
CREATE INDEX IF NOT EXISTS sys_node_props_idx     ON sys.node USING gin (properties);

COMMENT ON COLUMN sys.node.lifecycle IS
    'R4: for code, zero inbound edges is strong evidence of death. For a workbook it is nearly '
    'no evidence -- a human opens it by double-clicking. So workbooks are never marked dead on '
    'graph evidence alone. Lifecycle LABELS a query result; it must never shrink one.';
COMMENT ON COLUMN sys.node.resolution_status IS
    'unresolved = this reference may point at nothing. Excluded from default answers, kept for '
    'the drift report. Distinct from lifecycle -- see D4.';


-- ---------------------------------------------------------------------------
-- sys.edge -- direction convention: edges point the way DATA FLOWS.
-- 'file READS relation' runs against the flow, so it is stored relation -> file with
-- edge_type='READS'. Downstream traversal is then always "follow edges forward" with no
-- per-type special-casing. Written down once (design 5.2); do not re-litigate.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sys.edge (
    edge_id           bigserial PRIMARY KEY,
    source_node_id    bigint NOT NULL REFERENCES sys.node(node_id) ON DELETE CASCADE,
    target_node_id    bigint NOT NULL REFERENCES sys.node(node_id) ON DELETE CASCADE,
    edge_type         text NOT NULL,
    properties        jsonb NOT NULL DEFAULT '{}'::jsonb,
    evidence          jsonb NOT NULL DEFAULT '{}'::jsonb,   -- file, line, formula excerpt, cell sample

    extraction_method text NOT NULL,
    confidence        numeric(3,2) NOT NULL DEFAULT 1.00,
    resolution_status text NOT NULL DEFAULT 'resolved',

    first_seen_scan   bigint NOT NULL REFERENCES sys.scan(scan_id),
    last_seen_scan    bigint NOT NULL REFERENCES sys.scan(scan_id),

    CONSTRAINT sys_edge_uq UNIQUE (source_node_id, target_node_id, edge_type),
    CONSTRAINT sys_edge_type_ck CHECK (edge_type IN (
        'DERIVES_FROM','HAS_SERIES','READS','WRITES','EMITS','SCHEDULED_AS','INVOKES',
        'LINKS_TO','BINDS_TO','PUBLISHED_IN','SOURCED_FROM','SUPERSEDED_BY','DEPLOYED_AS',
        'DEFINES')),
    CONSTRAINT sys_edge_resolution_ck CHECK (resolution_status IN
        ('resolved','unresolved','ambiguous'))
);

CREATE INDEX IF NOT EXISTS sys_edge_source_idx   ON sys.edge (source_node_id, edge_type);
CREATE INDEX IF NOT EXISTS sys_edge_target_idx   ON sys.edge (target_node_id, edge_type);
CREATE INDEX IF NOT EXISTS sys_edge_type_idx     ON sys.edge (edge_type);
CREATE INDEX IF NOT EXISTS sys_edge_lastseen_idx ON sys.edge (last_seen_scan);

COMMENT ON TABLE sys.edge IS
    'Edges point in the direction data flows. READS is stored relation -> consumer.';
COMMENT ON COLUMN sys.edge.edge_type IS
    'DEPLOYED_AS and DEFINES are session-2 additions to design 5.2. DEPLOYED_AS: a .bas file in '
    'git -> the module actually embedded in a workbook. Measured 2026-07-21: 7 of 8 shared '
    'modules had drifted textually and TradeUpdaterSQL exists as 6 distinct forks under one '
    'name, so git source and embedded instance cannot be the same node. DEFINES: module -> '
    'procedure.';


-- ---------------------------------------------------------------------------
-- sys.declaration -- human rulings. NEVER written by a scan.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sys.declaration (
    declaration_id  bigserial PRIMARY KEY,
    subject_key     text NOT NULL,
    predicate       text NOT NULL,   -- SERVES | IS_CANONICAL | SUPERSEDED_BY | PUBLISHED_IN
                                     -- | LIFECYCLE | READS | WRITES | ARCHIVE_OK
    object_key      text,
    ruled_by        text NOT NULL,
    ruled_at        timestamptz NOT NULL DEFAULT now(),
    rationale       text NOT NULL,
    retired_at      timestamptz,

    CONSTRAINT sys_declaration_uq UNIQUE (subject_key, predicate, object_key)
);

CREATE INDEX IF NOT EXISTS sys_declaration_subject_idx ON sys.declaration (subject_key)
    WHERE retired_at IS NULL;

COMMENT ON TABLE sys.declaration IS
    'Human rulings, replayed onto the graph after every rebuild. A scan may read this table; '
    'a scan may never write it. Review these as carefully as any ruling doc -- an assumption '
    'written down once and inherited forever without re-verification is the exact mechanism '
    'behind several of the 2026-07-21 corrections (design section 10).';


-- ---------------------------------------------------------------------------
-- sys.check_result -- section 8 assertions, one row per check per scan.
-- The durable fix is an assertion that runs every time, not a fact someone remembered once.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sys.check_result (
    check_id     bigserial PRIMARY KEY,
    scan_id      bigint NOT NULL REFERENCES sys.scan(scan_id) ON DELETE CASCADE,
    check_name   text NOT NULL,
    binding      boolean NOT NULL,       -- binding checks fail the scan; others only report
    passed       boolean NOT NULL,
    detail       jsonb NOT NULL DEFAULT '{}'::jsonb,
    checked_at   timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT sys_check_uq UNIQUE (scan_id, check_name)
);


-- ---------------------------------------------------------------------------
-- sys.workbook_hash -- R3's gate. The expensive openpyxl pass only touches workbooks
-- whose content hash moved (typically 2-3 per week out of ~470), which is why the
-- cheap/expensive split replaces the calendar question entirely.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sys.workbook_hash (
    workbook_path  text PRIMARY KEY,
    content_sha256 text NOT NULL,
    size_bytes     bigint,
    mtime          timestamptz,
    last_scanned   bigint REFERENCES sys.scan(scan_id),
    scan_duration_ms integer,
    scan_error     text
);


-- ---------------------------------------------------------------------------
-- Views
-- ---------------------------------------------------------------------------

-- Current graph = whatever the newest completed scan saw.
CREATE OR REPLACE VIEW sys.v_current_scan AS
SELECT scan_id FROM sys.scan WHERE status = 'ok' ORDER BY scan_id DESC LIMIT 1;

CREATE OR REPLACE VIEW sys.v_node AS
SELECT n.* FROM sys.node n, sys.v_current_scan c WHERE n.last_seen_scan = c.scan_id;

CREATE OR REPLACE VIEW sys.v_edge AS
SELECT e.* FROM sys.edge e, sys.v_current_scan c WHERE e.last_seen_scan = c.scan_id;

-- Edges in readable form. resolution_status is exposed, never pre-filtered here --
-- callers decide, and trace_series must be able to prove check 8.7 (identical results with
-- and without lifecycle filtering).
CREATE OR REPLACE VIEW sys.v_edge_named AS
SELECT e.edge_id,
       s.node_type AS source_type, s.node_key AS source_key, s.lifecycle AS source_lifecycle,
       e.edge_type,
       t.node_type AS target_type, t.node_key AS target_key, t.lifecycle AS target_lifecycle,
       e.confidence, e.extraction_method, e.resolution_status, e.evidence, e.properties,
       e.last_seen_scan
FROM sys.v_edge e
JOIN sys.node s ON s.node_id = e.source_node_id
JOIN sys.node t ON t.node_id = e.target_node_id;

-- R1: the cleanup deliverable. Candidates, never conclusions -- and for workbooks the
-- claim is only ever "no CODE references this", which R4 says is nearly no evidence at all.
CREATE OR REPLACE VIEW sys.v_no_inbound AS
SELECT n.node_type, n.node_key, n.lifecycle, n.properties,
       CASE WHEN n.node_type IN ('workbook','worksheet')
            THEN 'no-code-references'      -- a human opens these by double-clicking
            ELSE 'no-inbound-edges'
       END AS claim
FROM sys.v_node n
WHERE NOT EXISTS (SELECT 1 FROM sys.v_edge e WHERE e.target_node_id = n.node_id)
  AND n.node_type IN ('repo_file','sql_script','vba_module','workbook');


-- Cross-graph union (design section 6). Storage separate, access unified.
CREATE OR REPLACE VIEW sys.v_graph AS
SELECT 'sys'::text AS graph, node_key AS key, node_type AS type, label, properties
FROM sys.v_node
UNION ALL
SELECT 'analyst'::text, node_key, node_type, label, COALESCE(properties, '{}'::jsonb)
FROM core.kg_node;


-- R2: point at the replacement so nobody reads the dead model by accident. Retirement is
-- decided as part of the R1 cleanup pass, not in isolation.
COMMENT ON TABLE audit.lineage_edge IS
    'SUPERSEDED by sys.edge (2026-07-21). Runtime-instrumented lineage: every script had to '
    'call audit.add_lineage_edge() voluntarily, and across 636 Python files adoption was ~0 -- '
    '29 edges here against 428 that pg_depend gives away for free. Do not build on this. '
    'Retirement is an item on the R1 cleanup list, see docs/specs/system_knowledge_graph_design_v1.md.';
