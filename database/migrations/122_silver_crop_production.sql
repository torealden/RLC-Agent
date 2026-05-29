-- Migration 122: silver.crop_production + silver.crop_progress_condition
--
-- Long/tidy crop production tracking with full revision history. Replaces
-- bronze.nass_production, bronze.nass_acreage, silver.nass_production_summary
-- and bronze.nass_state_yields as the canonical state-level production
-- layer (deprecation handled in mig 123 after data migration is verified).
--
-- Design choices (per Tore 2026-05-29):
--   - Store BOTH all-classes rollup and broken-out classes (class='all_classes'
--     + per-class rows). Lets downstream pick either cut.
--   - release_date IS in the natural key. Append-only — every NASS release
--     gets its own row even for the same reference_period. Enables analysis
--     of how forecasts revise as the season progresses.
--   - Companion silver.crop_progress_condition: schema stub only, no
--     data load in this migration.
--   - District/county extensibility built in (asd_code, county_ansi columns
--     ready, unused in v1).

BEGIN;

-- =============================================================
-- silver.crop_production — one row = one number for one (crop, geography,
-- statistic, vintage). Long/tidy. Append-only on release_date.
-- =============================================================
CREATE TABLE IF NOT EXISTS silver.crop_production (
    id                  BIGSERIAL PRIMARY KEY,

    -- WHAT
    commodity           VARCHAR(40)  NOT NULL,           -- normalized: soybeans, corn, wheat, sorghum, canola, sunflower, cotton, peanuts
    class               VARCHAR(40)  NOT NULL,           -- 'all_classes' OR a NASS class: 'grain', 'silage', 'winter', 'spring', 'durum', 'oil_type', 'confection', 'upland', 'pima', 'runner', 'spanish', 'virginia_valencia'
    statistic           VARCHAR(40)  NOT NULL,           -- 'area_planted', 'area_harvested', 'yield', 'production', 'production_value'

    -- WHEN (crop / forecast vintage)
    crop_year           INTEGER      NOT NULL,
    reference_period    VARCHAR(60)  NOT NULL,           -- NASS reference_period_desc: 'YEAR', 'YEAR - AUG FORECAST', etc.
    is_forecast         BOOLEAN      NOT NULL DEFAULT FALSE,
    source_report       VARCHAR(50),                     -- 'Prospective Plantings', 'Acreage', 'Crop Production', 'Annual Crop Production Summary'
    release_date        DATE         NOT NULL,           -- derived from NASS load_time

    -- WHERE
    agg_level           VARCHAR(20)  NOT NULL,           -- 'NATIONAL', 'STATE', 'DISTRICT', 'COUNTY'
    state_alpha         VARCHAR(2)   NOT NULL DEFAULT '',-- e.g., 'IA', '' for NATIONAL
    state_fips          VARCHAR(2)   NOT NULL,           -- '99' for NATIONAL, '19' for IA, etc.
    asd_code            VARCHAR(4)   NOT NULL DEFAULT '',-- agricultural statistics district — DISTRICT level, '' otherwise
    county_ansi         VARCHAR(5)   NOT NULL DEFAULT '',-- COUNTY level, '' otherwise

    -- VALUE
    value               NUMERIC,                         -- NULL if NASS returned (D) suppressed or (NA)
    unit                VARCHAR(40)  NOT NULL,           -- acres, bu, lb, cwt, 480-lb bales, tons, $, bu/acre, lb/acre, etc. NEVER bury units in column names.
    cv_pct              NUMERIC,                         -- coefficient of variation, when NASS provides

    -- TRACEABILITY
    short_desc          TEXT         NOT NULL,           -- carry NASS short_desc verbatim
    load_ts             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    -- Natural key constraint (append-only on release_date)
    CONSTRAINT crop_production_natural_key UNIQUE (
        commodity, class, statistic, agg_level, state_fips,
        crop_year, reference_period, release_date,
        asd_code, county_ansi
    )
);

-- Indexes per Tore's spec: on the natural key (above) and on
-- (commodity, state_fips, crop_year) for common time-series queries
CREATE INDEX IF NOT EXISTS idx_crop_prod_commodity_state_year
    ON silver.crop_production (commodity, state_fips, crop_year DESC);

CREATE INDEX IF NOT EXISTS idx_crop_prod_release
    ON silver.crop_production (release_date DESC);

CREATE INDEX IF NOT EXISTS idx_crop_prod_forecast
    ON silver.crop_production (commodity, crop_year, is_forecast)
    WHERE is_forecast = TRUE;

COMMENT ON TABLE  silver.crop_production IS 'Long/tidy crop production tracking with full revision history. Append-only on release_date.';
COMMENT ON COLUMN silver.crop_production.class IS 'NASS class normalized. "all_classes" for the NASS-emitted rollup; specific class names where NASS splits (e.g., wheat winter/spring/durum, peanut runner/spanish/virginia_valencia).';
COMMENT ON COLUMN silver.crop_production.reference_period IS 'NASS reference_period_desc verbatim. "YEAR" = annual estimate; "YEAR - AUG FORECAST" et al = monthly Crop Production forecasts.';
COMMENT ON COLUMN silver.crop_production.release_date IS 'Date the row was released by NASS (derived from QuickStats load_time). In the natural key — every release of the same reference_period appends a new row, enabling vintage analysis of how forecasts revise.';
COMMENT ON COLUMN silver.crop_production.source_report IS 'Derived: Prospective Plantings, Acreage, Crop Production (monthly), Annual Crop Production Summary. Inferred from statistic + reference_period + release_date.';


-- =============================================================
-- silver.crop_progress_condition — SCHEMA STUB. No data load.
-- Populate in a follow-up migration once natural-key convention is agreed.
-- =============================================================
CREATE TABLE IF NOT EXISTS silver.crop_progress_condition (
    id                  BIGSERIAL PRIMARY KEY,

    commodity           VARCHAR(40)  NOT NULL,
    class               VARCHAR(40)  NOT NULL DEFAULT 'all_classes',

    -- WHEN (week of observation)
    crop_year           INTEGER      NOT NULL,
    week_ending         DATE         NOT NULL,

    -- WHERE
    agg_level           VARCHAR(20)  NOT NULL,
    state_alpha         VARCHAR(2),
    state_fips          VARCHAR(2)   NOT NULL,

    -- WHAT
    statistic           VARCHAR(60)  NOT NULL,         -- 'planted_pct', 'emerged_pct', 'silking_pct', 'harvested_pct',
                                                       -- 'condition_excellent_pct', 'condition_good_pct', 'condition_fair_pct',
                                                       -- 'condition_poor_pct', 'condition_very_poor_pct',
                                                       -- 'good_excellent_pct' (derived)
    value               NUMERIC,
    unit                VARCHAR(20)  NOT NULL DEFAULT 'pct',

    -- TRACEABILITY
    short_desc          TEXT         NOT NULL,
    release_date        DATE,
    load_ts             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT crop_progress_natural_key UNIQUE (
        commodity, class, statistic, agg_level, state_fips,
        crop_year, week_ending
    )
);

CREATE INDEX IF NOT EXISTS idx_crop_progress_commodity_state_week
    ON silver.crop_progress_condition (commodity, state_fips, week_ending DESC);

COMMENT ON TABLE silver.crop_progress_condition IS 'STUB: schema for weekly NASS crop progress + condition ratings. Natural key on (commodity, class, statistic, agg_level, state_fips, crop_year, week_ending). No data loaded yet — populated by follow-up migration.';


COMMIT;

-- Verification:
-- SELECT table_name FROM information_schema.tables
-- WHERE table_schema = 'silver' AND table_name LIKE 'crop_%' ORDER BY table_name;
--
-- After data migration, mig 123 will rename:
--   bronze.nass_production       -> bronze.nass_production_deprecated_20260529
--   bronze.nass_acreage          -> bronze.nass_acreage_deprecated_20260529
--   silver.nass_production_summary -> silver.nass_production_summary_deprecated_20260529
--   bronze.nass_state_yields     -> bronze.nass_state_yields_deprecated_20260529
