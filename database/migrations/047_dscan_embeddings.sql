-- =============================================================================
-- Migration 047: silver.dscan_embeddings
--
-- Vector embeddings of the dscan-cataloged D-drive archive (~25K files,
-- 12,910 with extracted text + LLM-suggested topics).
--
-- Powers semantic search ("find files related to soybean meal pricing")
-- and is the foundation for later RAG over the historical archive.
--
-- Embeddings produced by nomic-embed-text on the 4060 laptop.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.dscan_embeddings (
    id                 BIGSERIAL PRIMARY KEY,
    rel_path           TEXT NOT NULL UNIQUE,    -- relative path from dscan inventory
    file_name          TEXT,
    ext                TEXT,
    size_bytes         BIGINT,
    topic              TEXT,                    -- from dscan_rename_suggest
    suggested_name     TEXT,
    embedded_text_len  INTEGER,                 -- chars of input fed to embedder
    embedding_model    TEXT NOT NULL,           -- 'nomic-embed-text'
    embedding          REAL[],                  -- 768-dim vector for nomic-embed-text
    embedded_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dscan_embeddings_topic
    ON silver.dscan_embeddings(topic);
CREATE INDEX IF NOT EXISTS idx_dscan_embeddings_ext
    ON silver.dscan_embeddings(ext);

COMMENT ON TABLE silver.dscan_embeddings IS
    'Vector embeddings of the D-drive rat-hole archive (cataloged via dscan). '
    'Produced by nomic-embed-text on the 4060 laptop background grinder. '
    'Used for semantic search over historical work artifacts.';
