-- 167_wasde_historical_bronze.sql
--
-- bronze.wasde_historical: USDA OCE "Consolidated Historical WASDE Report Data"
-- (every WASDE report as published at release time, Apr 2010 -> present).
-- Source archive: data/raw/wasde_historical/ via scripts/download_wasde_historical.py.
-- Loader: scripts/load_wasde_historical.py (one-shot backfill, idempotent upsert;
-- forward collection stays with the mig-166 PSD/WASDE vintage collector).
-- Spec: docs/specs/wasde_vintage_backfill_v1.md.
--
-- CSV columns verbatim (report_date is the "July 2026" label, not a date; the
-- parseable date is release_date). Values stay in WASDE-published source units
-- (US grains Million Bushels, world tables MMT at 2 dp) — conversion happens at
-- transform, never here. market_year is verbatim and contains source typos
-- ("2010/011"); normalize downstream.
--
-- Uniqueness: the spec's proposed 7-column key is NOT unique in the real data
-- (reliability-appendix rows collide; Proj./Est. rows for the same MY collide).
-- The 10-column key below was verified duplicate-free across the two consolidated
-- CSVs + 2026-07 (617k rows) on 2026-08-03; the loader asserts it per file.

BEGIN;

CREATE TABLE IF NOT EXISTS bronze.wasde_historical (
    id                     bigserial PRIMARY KEY,
    wasde_number           integer     NOT NULL,
    report_date            text        NOT NULL,  -- label, e.g. 'July 2026'
    report_title           text        NOT NULL,
    attribute              text        NOT NULL,
    reliability_projection text        NOT NULL DEFAULT '',
    commodity              text        NOT NULL,
    region                 text        NOT NULL,
    market_year            text        NOT NULL DEFAULT '',  -- '' = reliability appendix row
    proj_est_flag          text        NOT NULL DEFAULT '',
    annual_quarter_flag    text        NOT NULL DEFAULT '',
    value                  numeric,
    unit                   text        NOT NULL DEFAULT '',
    release_date           date        NOT NULL,
    release_time           text,
    forecast_year          integer,
    forecast_month         integer,
    source_file            text        NOT NULL,
    loaded_at              timestamptz NOT NULL DEFAULT now(),
    last_touched_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT wasde_historical_natural_key UNIQUE
        (wasde_number, report_title, attribute, reliability_projection,
         commodity, region, market_year, proj_est_flag, annual_quarter_flag, unit)
);

CREATE INDEX IF NOT EXISTS idx_wasde_hist_commodity_region_my
    ON bronze.wasde_historical (commodity, region, market_year);
CREATE INDEX IF NOT EXISTS idx_wasde_hist_release_date
    ON bronze.wasde_historical (release_date);

COMMENT ON TABLE bronze.wasde_historical IS
'USDA OCE historical WASDE report data as published at release time, WASDE #481 (Apr 2010) '
'onward. CSV columns verbatim, source units, market_year verbatim incl. source typos. '
'Month gaps 2013-10 / 2019-01 / 2025-10 are government shutdowns (report-number sequence '
'is contiguous across them — those reports never existed). Backfill-only source; live '
'WASDE vintages come from the PSD collector (mig 166). See docs/specs/wasde_vintage_backfill_v1.md.';

COMMIT;
