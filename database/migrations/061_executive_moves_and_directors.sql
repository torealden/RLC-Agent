-- Migration 061: weak inter-company links via executive moves + board overlap
-- Date: 2026-05-09
--
-- Two new structured tables:
--   silver.executive_move        — person leaves company A, joins company B
--   silver.director_appointment  — person sits on company X's board for years Y-Z
--
-- Plus a view that auto-derives the cross-company "shared_director"
-- links from director_appointment, so the FIC and downstream queries
-- don't need to compute them ad-hoc.
--
-- Source pattern (Slice 2 + 3 of the FIC roadmap):
--   - executive_move rows come from the news classifier when an
--     article mentions a personnel change with a prior employer.
--     Manual entry is also supported (source_type='manual').
--   - director_appointment rows come from extracting DEF 14A proxy
--     statements via the local LLM. The SEC EDGAR puller adds DEF 14A
--     to its default forms; an extractor parses the directors section.

BEGIN;

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- =============================================================================
-- silver.executive_move
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.executive_move (
    id                       BIGSERIAL PRIMARY KEY,

    -- The person
    person_name              TEXT NOT NULL,            -- as stated in source
    person_normalized        TEXT NOT NULL,            -- 'donald.smith' style
    role                     TEXT,                     -- e.g. 'CFO', 'CEO', 'VP Origination'

    -- The move
    from_operator            TEXT,                     -- prior employer (string match)
    from_operator_ticker     TEXT,                     -- if public
    to_operator              TEXT,                     -- new employer
    to_operator_ticker       TEXT,
    event_date               DATE,                     -- when move took effect
    announced_date           DATE,                     -- when news/filing announced

    -- Provenance
    source_type              TEXT NOT NULL,            -- news_article | sec_filing | manual
    source_id                TEXT,                     -- article_id_hash | accession | NULL
    source_url               TEXT,
    confidence               NUMERIC(3, 2),            -- 0.0-1.0 from extractor

    notes                    TEXT,
    extracted_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    extracted_by             TEXT NOT NULL DEFAULT 'system',

    CONSTRAINT exec_move_source_type_chk CHECK (
        source_type IN ('news_article', 'sec_filing', 'manual', 'press_release', 'linkedin')
    )
);

CREATE INDEX IF NOT EXISTS idx_exec_move_from
    ON silver.executive_move (LOWER(from_operator));
CREATE INDEX IF NOT EXISTS idx_exec_move_to
    ON silver.executive_move (LOWER(to_operator));
CREATE INDEX IF NOT EXISTS idx_exec_move_person
    ON silver.executive_move (person_normalized);
CREATE INDEX IF NOT EXISTS idx_exec_move_event_date
    ON silver.executive_move (event_date DESC);

COMMENT ON TABLE silver.executive_move IS
'Inter-company personnel moves: a person leaves operator A and joins '
'operator B. Drives the executive_move weak edge in '
'reference.facility_edge_weights.';


-- =============================================================================
-- silver.director_appointment
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.director_appointment (
    id                       BIGSERIAL PRIMARY KEY,

    person_name              TEXT NOT NULL,
    person_normalized        TEXT NOT NULL,
    bio_summary              TEXT,                     -- extracted bio paragraph

    operator                 TEXT NOT NULL,
    operator_ticker          TEXT,                     -- NULL if private (rare for boards)
    role                     TEXT,                     -- director | chair | lead_independent | audit_committee_chair | ...
    is_independent           BOOLEAN,
    is_chair                 BOOLEAN GENERATED ALWAYS AS (LOWER(role) = 'chair') STORED,

    appointed_date           DATE,
    departed_date            DATE,                     -- NULL if still serving
    last_disclosed_year      INTEGER,                  -- year of the DEF 14A this came from

    -- Provenance
    source_form              TEXT NOT NULL,            -- usually 'DEF 14A'
    source_filing_accession  TEXT,
    source_filing_url        TEXT,
    extracted_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    extracted_by             TEXT NOT NULL DEFAULT 'system'
);

CREATE INDEX IF NOT EXISTS idx_director_operator
    ON silver.director_appointment (LOWER(operator));
CREATE INDEX IF NOT EXISTS idx_director_ticker
    ON silver.director_appointment (operator_ticker);
CREATE INDEX IF NOT EXISTS idx_director_person
    ON silver.director_appointment (person_normalized);
CREATE INDEX IF NOT EXISTS idx_director_active
    ON silver.director_appointment (LOWER(operator))
    WHERE departed_date IS NULL;

COMMENT ON TABLE silver.director_appointment IS
'Board membership extracted from DEF 14A proxy statements. Person X '
'served on operator Y board appointed_date through departed_date. '
'Cross-references via gold.cross_company_director_links view.';


-- =============================================================================
-- gold.cross_company_director_links
-- =============================================================================
-- A view that auto-derives "these two companies share a director" links.
-- Excludes self-joins; orders the operator pair so each link appears once.

CREATE OR REPLACE VIEW gold.cross_company_director_links AS
SELECT
    LEAST(LOWER(d1.operator), LOWER(d2.operator))    AS operator_a,
    GREATEST(LOWER(d1.operator), LOWER(d2.operator)) AS operator_b,
    d1.operator                                       AS operator_a_display,
    d2.operator                                       AS operator_b_display,
    d1.person_normalized,
    d1.person_name                                    AS person_name_a,
    d2.person_name                                    AS person_name_b,
    d1.role                                           AS role_a,
    d2.role                                           AS role_b,
    d1.last_disclosed_year                            AS year_a,
    d2.last_disclosed_year                            AS year_b,
    -- Both currently serving = stronger signal
    (d1.departed_date IS NULL AND d2.departed_date IS NULL) AS both_active
FROM silver.director_appointment d1
JOIN silver.director_appointment d2
  ON d1.person_normalized = d2.person_normalized
 AND LOWER(d1.operator) < LOWER(d2.operator);

COMMENT ON VIEW gold.cross_company_director_links IS
'Pairs of operators that share a board member. Driver for shared_director '
'weak edges in reference.facility_edge_weights.';


-- =============================================================================
-- gold.executive_moves_recent
-- =============================================================================
-- Recent moves, joined with whatever ticker info we have, for FIC display.

CREATE OR REPLACE VIEW gold.executive_moves_recent AS
SELECT
    id,
    person_name,
    role,
    from_operator,
    from_operator_ticker,
    to_operator,
    to_operator_ticker,
    event_date,
    announced_date,
    source_type,
    source_url,
    confidence,
    notes,
    extracted_at
FROM silver.executive_move
WHERE COALESCE(event_date, announced_date, extracted_at::date)
      > CURRENT_DATE - INTERVAL '730 days'
ORDER BY COALESCE(event_date, announced_date, extracted_at::date) DESC;

COMMENT ON VIEW gold.executive_moves_recent IS
'Executive moves announced/effective in the last 24 months.';

COMMIT;
