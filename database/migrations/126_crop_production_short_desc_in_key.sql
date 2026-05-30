-- Migration 126: Add short_desc to silver.crop_production natural key
--
-- The v1 natural key was:
--   (commodity, class, statistic, agg_level, state_fips, crop_year,
--    reference_period, release_date, asd_code, county_ansi)
--
-- This collided when NASS returns multiple short_descs sharing the same
-- statisticcat_desc / reference_period / release_date. Example: for
-- soybean AREA PLANTED 2025 YEAR - JUN ACREAGE rel=2025-06-30, NASS
-- publishes BOTH:
--   SOYBEANS - ACRES PLANTED                                    = 83,380,000 acres
--   SOYBEANS, FOLLOWING ANOTHER CROP (DOUBLE CROPPED)
--     - AREA PLANTED, MEASURED IN PCT                           = 6 PCT
--
-- Both rows had identical natural keys under the v1 design, so the
-- second insert (whichever it was per batch order) silently
-- ON CONFLICT DO UPDATE-d over the first. The canonical ACRES value
-- was being overwritten with the percentage.
--
-- Fix: add short_desc to the natural key so each NASS short_desc gets
-- its own row.

BEGIN;

ALTER TABLE silver.crop_production
    DROP CONSTRAINT crop_production_natural_key;

ALTER TABLE silver.crop_production
    ADD CONSTRAINT crop_production_natural_key UNIQUE (
        commodity, class, statistic, short_desc,
        agg_level, state_fips, crop_year,
        reference_period, release_date,
        asd_code, county_ansi
    );

COMMIT;

-- Verification:
-- SELECT pg_get_constraintdef(oid) FROM pg_constraint
-- WHERE conname = 'crop_production_natural_key';
--
-- After this migration, re-run the collector for all commodities
-- to repopulate any rows that were silently overwritten. The collector
-- is idempotent (ON CONFLICT DO UPDATE on the new wider key).
