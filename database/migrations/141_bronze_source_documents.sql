-- Migration 141: bronze.source_documents — generalized document registry
-- =============================================================================
-- The shared spine for non-API / PDF (and other file) sources. Bespoke
-- ACQUIRERS (one per source: ia_dnr_titlev, nass_gccp, ers_*, ...) register
-- every fetched document here; a generalized PARSE/QC/LOAD spine reads
-- parse_status='pending' rows, routes by doc_type to the right extractor
-- (regex / pdfplumber-positional / local-LLM best-of-N / vision), and writes
-- results to the appropriate bronze table (e.g. bronze.state_air_permits).
--
-- This is the inbox/queue + provenance + dedup layer. It does NOT replace the
-- parsed-output tables; it tracks "what we have and what still needs parsing"
-- across all sources, so a nightly job can find work and never re-fetch or
-- re-parse unchanged documents.
-- =============================================================================

CREATE TABLE IF NOT EXISTS bronze.source_documents (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT NOT NULL,            -- 'ia_dnr_titlev', 'nass_gccp', ...
    source_key      TEXT,                     -- facility_id / report_id / natural key
    doc_type        TEXT NOT NULL,            -- 'title_v_permit', 'gccp_report', ...
    title           TEXT,
    url             TEXT,
    local_path      TEXT,
    sha256          TEXT,                     -- content hash for change detection/dedup
    page_count      INTEGER,
    published_date  DATE,                     -- the document's own date (release/issue)
    vintage         DATE,                     -- when the source last revised it
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- parse lifecycle
    parse_status    TEXT NOT NULL DEFAULT 'pending',   -- pending|parsing|parsed|failed|skipped
    parse_method    TEXT,                     -- 'regex'|'pdfplumber'|'ollama_bestofn'|'vision'
    parse_model     TEXT,                     -- e.g. 'qwen3-coder:30b'
    parse_confidence TEXT,                    -- high|medium|low (QC gate)
    parsed_at       TIMESTAMPTZ,
    output_ref      TEXT,                     -- path to extraction JSON or bronze ref
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    notes           TEXT
);

-- One row per (source, source_key, content hash): re-fetching unchanged content
-- is idempotent; a NEW sha256 for the same key = a revised document (new row,
-- old vintage preserved).
CREATE UNIQUE INDEX IF NOT EXISTS source_documents_uniq
    ON bronze.source_documents (source, COALESCE(source_key, ''), COALESCE(sha256, ''));
CREATE INDEX IF NOT EXISTS idx_source_documents_status
    ON bronze.source_documents (parse_status, doc_type);
CREATE INDEX IF NOT EXISTS idx_source_documents_source
    ON bronze.source_documents (source);
