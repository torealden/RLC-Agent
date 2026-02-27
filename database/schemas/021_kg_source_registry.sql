-- ============================================================================
-- RLC-Agent Knowledge Graph: Source Registry & Provenance Tracking
-- ============================================================================
-- File: 021_kg_source_registry.sql (renumbered from 014 to follow schema sequence)
-- Purpose: Track which documents have been processed, what was extracted,
--          and link every KG entry back to its source material.
--          Enables incremental processing -- never re-process unchanged docs.
-- Depends: 020_knowledge_graph.sql (kg_node, kg_edge, kg_context)
-- Source:  Claude KG extraction pipeline
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. SOURCE REGISTRY: Every document we process gets a row here FIRST
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.kg_source (
    id              SERIAL PRIMARY KEY,

    -- Identity
    source_key      TEXT NOT NULL UNIQUE,     -- Stable identifier: gdrive file ID, file path hash, etc.
    source_type     TEXT NOT NULL,            -- 'gdrive_doc', 'gdrive_pdf', 'local_file', 'url', 'notion_page'
    title           TEXT NOT NULL,            -- Document title / filename

    -- Location
    location_uri    TEXT,                     -- Full URI: Google Drive URL, file path, Notion URL
    folder_path     TEXT,                     -- Logical folder: 'Reports/Weekly/2024' for organization

    -- Content fingerprint (detect changes)
    content_hash    TEXT,                     -- SHA-256 of content at last processing
    word_count      INTEGER,                 -- Approximate size

    -- Metadata
    document_date   DATE,                    -- When the report was written (not when we processed it)
    author          TEXT DEFAULT 'tore',     -- Who wrote it
    commodities     TEXT[],                  -- Commodities discussed: {'corn','soybeans','soy_oil'}
    topics          TEXT[],                  -- Topics: {'cftc','export_sales','balance_sheet','biofuels'}
    document_type   TEXT,                    -- 'weekly_report', 'daily_note', 'article', 'research', 'third_party'

    -- Processing status
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending | processing | completed | failed | skipped
    first_processed TIMESTAMPTZ,             -- When we first extracted from this doc
    last_processed  TIMESTAMPTZ,             -- Most recent processing run
    processing_notes TEXT,                   -- Any issues, partial extractions, etc.

    -- Extraction counts (quick summary without joining)
    nodes_extracted   INTEGER DEFAULT 0,
    edges_extracted   INTEGER DEFAULT 0,
    contexts_extracted INTEGER DEFAULT 0,

    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_kg_source_status ON core.kg_source(status);
CREATE INDEX IF NOT EXISTS idx_kg_source_type ON core.kg_source(source_type);
CREATE INDEX IF NOT EXISTS idx_kg_source_date ON core.kg_source(document_date DESC);
CREATE INDEX IF NOT EXISTS idx_kg_source_commodities ON core.kg_source USING GIN(commodities);
CREATE INDEX IF NOT EXISTS idx_kg_source_topics ON core.kg_source USING GIN(topics);


-- ---------------------------------------------------------------------------
-- 2. PROVENANCE: Links every KG entry to the source(s) it came from
-- ---------------------------------------------------------------------------
-- A single relationship might be mentioned in 15 different reports over 3 years.
-- That's signal -- it means it's a core part of the analytical framework.

CREATE TABLE IF NOT EXISTS core.kg_provenance (
    id              SERIAL PRIMARY KEY,

    -- What was extracted
    entity_type     TEXT NOT NULL,            -- 'node', 'edge', 'context'
    entity_id       INTEGER NOT NULL,         -- FK to kg_node.id, kg_edge.id, or kg_context.id

    -- Where it came from
    source_id       INTEGER NOT NULL REFERENCES core.kg_source(id),

    -- Extraction details
    extracted_at    TIMESTAMPTZ DEFAULT NOW(),
    extraction_method TEXT DEFAULT 'llm',     -- 'llm', 'manual', 'computed'

    -- The actual text that supported this extraction (for auditability)
    source_excerpt  TEXT,                     -- The relevant passage from the document

    -- Confidence from this particular source
    source_confidence NUMERIC DEFAULT 0.8,   -- How clearly this doc supports the extraction

    UNIQUE(entity_type, entity_id, source_id)  -- One provenance link per entity per source
);

CREATE INDEX IF NOT EXISTS idx_kg_provenance_entity ON core.kg_provenance(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_kg_provenance_source ON core.kg_provenance(source_id);


-- ---------------------------------------------------------------------------
-- 3. PROCESSING BATCHES: Group processing runs for tracking
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.kg_processing_batch (
    id              SERIAL PRIMARY KEY,
    batch_name      TEXT NOT NULL,            -- 'initial_load_2026-02-14', 'weekly_update_2026-02-21'
    started_at      TIMESTAMPTZ DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          TEXT DEFAULT 'running',   -- running | completed | failed | partial

    -- Counts
    sources_queued    INTEGER DEFAULT 0,
    sources_processed INTEGER DEFAULT 0,
    sources_skipped   INTEGER DEFAULT 0,      -- Already processed, no changes
    sources_failed    INTEGER DEFAULT 0,

    -- KG changes in this batch
    nodes_created     INTEGER DEFAULT 0,
    nodes_updated     INTEGER DEFAULT 0,
    edges_created     INTEGER DEFAULT 0,
    edges_updated     INTEGER DEFAULT 0,
    contexts_created  INTEGER DEFAULT 0,
    contexts_updated  INTEGER DEFAULT 0,

    notes           TEXT
);


-- ---------------------------------------------------------------------------
-- 4. ADD PROVENANCE COLUMNS to existing KG tables
-- ---------------------------------------------------------------------------
-- How many independent sources support this node/edge/context?
-- Higher = more core to the analytical framework
ALTER TABLE core.kg_node ADD COLUMN IF NOT EXISTS source_count INTEGER DEFAULT 0;
ALTER TABLE core.kg_edge ADD COLUMN IF NOT EXISTS source_count INTEGER DEFAULT 0;
ALTER TABLE core.kg_context ADD COLUMN IF NOT EXISTS source_count INTEGER DEFAULT 0;

-- When was this last reinforced by a new source?
ALTER TABLE core.kg_node ADD COLUMN IF NOT EXISTS last_reinforced TIMESTAMPTZ;
ALTER TABLE core.kg_edge ADD COLUMN IF NOT EXISTS last_reinforced TIMESTAMPTZ;
ALTER TABLE core.kg_context ADD COLUMN IF NOT EXISTS last_reinforced TIMESTAMPTZ;


-- ---------------------------------------------------------------------------
-- 5. VIEWS: Operational queries for the processing pipeline
-- ---------------------------------------------------------------------------

-- What needs processing?
CREATE OR REPLACE VIEW core.kg_sources_pending AS
SELECT id, source_key, title, source_type, location_uri, document_date
FROM core.kg_source
WHERE status = 'pending'
ORDER BY document_date DESC NULLS LAST;

-- What's been processed, grouped by commodity?
CREATE OR REPLACE VIEW core.kg_source_coverage AS
SELECT
    unnest(commodities) AS commodity,
    COUNT(*) AS total_sources,
    COUNT(*) FILTER (WHERE status = 'completed') AS processed,
    COUNT(*) FILTER (WHERE status = 'pending') AS pending,
    MIN(document_date) AS earliest_doc,
    MAX(document_date) AS latest_doc
FROM core.kg_source
GROUP BY unnest(commodities)
ORDER BY total_sources DESC;

-- Which KG entries are most well-supported? (core analytical framework)
CREATE OR REPLACE VIEW core.kg_confidence_ranking AS
SELECT
    p.entity_type,
    p.entity_id,
    CASE
        WHEN p.entity_type = 'node' THEN n.label
        WHEN p.entity_type = 'edge' THEN et.label || ' -> ' || COALESCE(tt.label, '?')
        WHEN p.entity_type = 'context' THEN c.context_type || ': ' || c.context_key
    END AS description,
    COUNT(DISTINCT p.source_id) AS source_count,
    AVG(p.source_confidence) AS avg_confidence,
    MIN(s.document_date) AS first_mentioned,
    MAX(s.document_date) AS last_mentioned,
    (MAX(s.document_date) - MIN(s.document_date))::numeric / 365.25 AS span_years
FROM core.kg_provenance p
JOIN core.kg_source s ON s.id = p.source_id
LEFT JOIN core.kg_node n ON p.entity_type = 'node' AND n.id = p.entity_id
LEFT JOIN core.kg_edge e ON p.entity_type = 'edge' AND e.id = p.entity_id
LEFT JOIN core.kg_node et ON e.source_node_id = et.id
LEFT JOIN core.kg_node tt ON e.target_node_id = tt.id
LEFT JOIN core.kg_context c ON p.entity_type = 'context' AND c.id = p.entity_id
GROUP BY p.entity_type, p.entity_id, n.label, et.label, tt.label, c.context_type, c.context_key
ORDER BY source_count DESC, avg_confidence DESC;

-- Has a document changed since we last processed it?
CREATE OR REPLACE VIEW core.kg_sources_changed AS
SELECT id, source_key, title, content_hash, last_processed
FROM core.kg_source
WHERE status = 'completed'
  AND content_hash IS NOT NULL
ORDER BY last_processed ASC;


-- ---------------------------------------------------------------------------
-- 6. HELPER FUNCTION: Register a new source (idempotent)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION core.register_kg_source(
    p_source_key TEXT,
    p_source_type TEXT,
    p_title TEXT,
    p_location_uri TEXT DEFAULT NULL,
    p_folder_path TEXT DEFAULT NULL,
    p_document_date DATE DEFAULT NULL,
    p_document_type TEXT DEFAULT NULL,
    p_commodities TEXT[] DEFAULT NULL,
    p_topics TEXT[] DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    v_id INTEGER;
BEGIN
    INSERT INTO core.kg_source (
        source_key, source_type, title, location_uri, folder_path,
        document_date, document_type, commodities, topics
    ) VALUES (
        p_source_key, p_source_type, p_title, p_location_uri, p_folder_path,
        p_document_date, p_document_type, p_commodities, p_topics
    )
    ON CONFLICT (source_key) DO UPDATE SET
        title = EXCLUDED.title,
        location_uri = EXCLUDED.location_uri,
        updated_at = NOW()
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$ LANGUAGE plpgsql;


-- ---------------------------------------------------------------------------
-- 7. HELPER FUNCTION: Update source counts after extraction
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION core.update_kg_source_counts()
RETURNS TRIGGER AS $$
BEGIN
    -- Update source_count on the target KG table
    IF NEW.entity_type = 'node' THEN
        UPDATE core.kg_node SET
            source_count = (SELECT COUNT(DISTINCT source_id) FROM core.kg_provenance WHERE entity_type = 'node' AND entity_id = NEW.entity_id),
            last_reinforced = NOW()
        WHERE id = NEW.entity_id;
    ELSIF NEW.entity_type = 'edge' THEN
        UPDATE core.kg_edge SET
            source_count = (SELECT COUNT(DISTINCT source_id) FROM core.kg_provenance WHERE entity_type = 'edge' AND entity_id = NEW.entity_id),
            last_reinforced = NOW()
        WHERE id = NEW.entity_id;
    ELSIF NEW.entity_type = 'context' THEN
        UPDATE core.kg_context SET
            source_count = (SELECT COUNT(DISTINCT source_id) FROM core.kg_provenance WHERE entity_type = 'context' AND entity_id = NEW.entity_id),
            last_reinforced = NOW()
        WHERE id = NEW.entity_id;
    END IF;

    -- Update extraction counts on the source
    UPDATE core.kg_source SET
        nodes_extracted = (SELECT COUNT(*) FROM core.kg_provenance WHERE source_id = NEW.source_id AND entity_type = 'node'),
        edges_extracted = (SELECT COUNT(*) FROM core.kg_provenance WHERE source_id = NEW.source_id AND entity_type = 'edge'),
        contexts_extracted = (SELECT COUNT(*) FROM core.kg_provenance WHERE source_id = NEW.source_id AND entity_type = 'context'),
        updated_at = NOW()
    WHERE id = NEW.source_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_kg_provenance_counts
    AFTER INSERT ON core.kg_provenance
    FOR EACH ROW EXECUTE FUNCTION core.update_kg_source_counts();


-- ---------------------------------------------------------------------------
-- Verification
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    RAISE NOTICE '021_kg_source_registry.sql executed successfully:';
    RAISE NOTICE '  - core.kg_source: table created';
    RAISE NOTICE '  - core.kg_provenance: table created';
    RAISE NOTICE '  - core.kg_processing_batch: table created';
    RAISE NOTICE '  - source_count + last_reinforced columns added to KG tables';
    RAISE NOTICE '  - 4 views created: kg_sources_pending, kg_source_coverage, kg_confidence_ranking, kg_sources_changed';
    RAISE NOTICE '  - 2 functions created: register_kg_source, update_kg_source_counts';
    RAISE NOTICE '  - Trigger created: trg_kg_provenance_counts';
END $$;
