-- Migration 031: Canadian Grain Commission bronze tables
-- GSW (Grain Statistics Weekly) and Exports from licensed facilities
-- Created: 2026-03-11

BEGIN;

-- =====================================================
-- 1. CGC Grain Statistics Weekly (deliveries, stocks,
--    shipments, exports, receipts — weekly by grain/region)
-- =====================================================
CREATE TABLE IF NOT EXISTS bronze.canada_cgc_weekly (
    id              SERIAL PRIMARY KEY,
    crop_year       TEXT NOT NULL,              -- '2025-2026'
    grain_week      INTEGER NOT NULL,           -- 1-52
    week_ending     DATE NOT NULL,
    worksheet       TEXT NOT NULL,              -- 'Summary', 'Terminal Stocks', etc.
    metric          TEXT NOT NULL,              -- 'Deliveries', 'Stocks', 'Exports', etc.
    period          TEXT NOT NULL,              -- 'Current Week' or 'Crop Year'
    grain           TEXT NOT NULL,              -- 'Wheat', 'Canola', 'Barley', etc.
    grade           TEXT NOT NULL DEFAULT '',    -- grain grade (often blank)
    region          TEXT NOT NULL DEFAULT '',   -- 'Alberta', 'Saskatchewan', etc.
    ktonnes         NUMERIC(12,4),             -- value in thousand tonnes
    commodity       TEXT,                       -- normalized name
    source          TEXT DEFAULT 'CGC_GSW',
    collected_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (crop_year, grain_week, worksheet, metric, period, grain, grade, region)
);

CREATE INDEX IF NOT EXISTS idx_cgc_weekly_grain ON bronze.canada_cgc_weekly (grain, week_ending);
CREATE INDEX IF NOT EXISTS idx_cgc_weekly_metric ON bronze.canada_cgc_weekly (metric, grain);
CREATE INDEX IF NOT EXISTS idx_cgc_weekly_date ON bronze.canada_cgc_weekly (week_ending);

-- =====================================================
-- 2. CGC Exports from Licensed Facilities (monthly
--    exports by grain, grade, destination, elevator type)
-- =====================================================
CREATE TABLE IF NOT EXISTS bronze.canada_cgc_exports (
    id              SERIAL PRIMARY KEY,
    year            INTEGER NOT NULL,
    month           TEXT NOT NULL,              -- 'August', 'September', etc.
    grain           TEXT NOT NULL,              -- 'Wheat', 'Canola', etc.
    grade           TEXT NOT NULL DEFAULT '',    -- 'No.1 CW RS', 'FEED', etc.
    ktonnes         NUMERIC(12,4),
    elevator        TEXT NOT NULL DEFAULT '',   -- 'TERMINALS', 'PRIMARY'
    region          TEXT NOT NULL DEFAULT '',   -- 'Vancouver', 'Prince Rupert', etc.
    global_region   TEXT NOT NULL DEFAULT '',   -- 'Asia', 'Africa', etc.
    destination     TEXT NOT NULL DEFAULT '',   -- 'Japan', 'China P.R.', etc.
    commodity       TEXT,                       -- normalized name
    source          TEXT DEFAULT 'CGC_EXPORTS',
    collected_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (year, month, grain, grade, elevator, region, destination)
);

CREATE INDEX IF NOT EXISTS idx_cgc_exports_grain ON bronze.canada_cgc_exports (grain, year);
CREATE INDEX IF NOT EXISTS idx_cgc_exports_dest ON bronze.canada_cgc_exports (destination, year);
CREATE INDEX IF NOT EXISTS idx_cgc_exports_year_month ON bronze.canada_cgc_exports (year, month);

COMMIT;
