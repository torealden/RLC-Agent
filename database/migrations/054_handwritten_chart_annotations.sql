-- Migration 054: Handwritten chart annotation tables
--
-- Date: 2026-05-06
--
-- Why:
--   User has years of price-chart scans annotated by hand with the
--   market-moving event/news behind each major price move (e.g., SN15
--   chart May 2014 - Jul 2015 has ~15 annotations: "Higher than
--   Expected avg", "Funds Established Record net short", "Wet weather"
--   reversing "Favorable weather", etc.).
--
--   Each annotation is a dated event with a polarity sign and rough
--   topic tag — the same schema the Market Field calibration script
--   wants for tuning ALPHA/BETA/GAMMA/EPS. Plus the data feeds the KG
--   as historical event nodes that complement the existing
--   special_situations/ markdown files.
--
-- Schema:
--   bronze.handwritten_chart_annotation — raw extraction output, one
--     row per annotation per chart-page. Preserves verbatim text plus
--     provenance (source_file + page) for re-extraction.
--   silver.market_event_annotation — cleaned + topic-mapped. Joins
--     to the Market Field topic taxonomy via topic_key.

CREATE TABLE IF NOT EXISTS bronze.handwritten_chart_annotation (
    id                      BIGSERIAL PRIMARY KEY,
    source_file             TEXT NOT NULL,
    source_file_hash        TEXT,                       -- sha256 of pdf bytes for dedup
    page_number             INTEGER NOT NULL DEFAULT 1,
    -- Chart metadata extracted from the page
    chart_contract          TEXT,                       -- e.g. 'SN15', 'CN15', 'WN15'
    chart_commodity         TEXT,                       -- 'soybeans', 'corn', 'wheat'
    chart_period_start      DATE,
    chart_period_end        DATE,
    chart_source            TEXT,                       -- watermark like 'DTN ProphetX'
    -- The annotation itself
    annotation_index        INTEGER NOT NULL,           -- ordering within the page
    verbatim_text           TEXT NOT NULL,
    position_on_chart       TEXT,                       -- 'top' / 'bottom' / 'diagonal' / 'header'
    approximate_date        DATE,                       -- best-effort date from chart position
    approximate_date_label  TEXT,                       -- string the model read off the x-axis
    -- Numbers from top/bottom strips of the chart
    raw_number              NUMERIC,                    -- e.g. 3927
    raw_deviation           NUMERIC,                    -- e.g. +14
    -- Provenance
    extractor_model         TEXT NOT NULL,              -- 'claude-sonnet-4-6'
    extractor_version       TEXT NOT NULL,              -- 'v1', 'v2' for re-runs
    raw_response            JSONB,                      -- full LLM JSON for debugging
    extracted_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (source_file_hash, page_number, annotation_index, extractor_version)
);

CREATE INDEX IF NOT EXISTS idx_chart_annotation_date
    ON bronze.handwritten_chart_annotation (approximate_date);
CREATE INDEX IF NOT EXISTS idx_chart_annotation_contract
    ON bronze.handwritten_chart_annotation (chart_contract);

COMMENT ON TABLE bronze.handwritten_chart_annotation IS
'Raw LLM extractions from handwritten-annotated price-chart scans. One row per annotation per page. Re-extraction keyed by (file_hash, page, idx, extractor_version) so model-version upgrades produce a new generation alongside the old.';


CREATE TABLE IF NOT EXISTS silver.market_event_annotation (
    id                      BIGSERIAL PRIMARY KEY,
    bronze_id               BIGINT NOT NULL REFERENCES bronze.handwritten_chart_annotation(id) ON DELETE CASCADE,
    market_id               TEXT NOT NULL,              -- 'us_oilseed_crush' for soybean charts
    event_date              DATE NOT NULL,
    event_text              TEXT NOT NULL,              -- cleaned verbatim
    topic_key               TEXT NOT NULL,              -- maps to reference.market_topic_taxonomy
    estimated_polarity      NUMERIC,                    -- -1 to +1
    estimated_intensity     NUMERIC,                    -- 0 to 1
    propagation_scope       TEXT,                       -- 'national' / 'regional' / 'local' / 'facility'
    confidence              NUMERIC,
    cleaned_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (bronze_id)
);

CREATE INDEX IF NOT EXISTS idx_market_event_annotation_date
    ON silver.market_event_annotation (event_date);
CREATE INDEX IF NOT EXISTS idx_market_event_annotation_topic
    ON silver.market_event_annotation (topic_key);

COMMENT ON TABLE silver.market_event_annotation IS
'Cleaned + topic-mapped market-moving events derived from handwritten chart scans. Feeds Market Field calibration (clients/market_field_calibration_events.json) and KG event-node ingestion.';
