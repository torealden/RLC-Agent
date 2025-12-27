-- ============================================================================
-- EXPORT INSPECTIONS TABLE
-- Migration: 004_export_inspections.sql
--
-- Stores weekly export inspection data from FGIS
-- Source: https://fgisonline.ams.usda.gov/ExportGrainReport/
-- ============================================================================

-- Weekly export inspections (detailed records)
CREATE TABLE IF NOT EXISTS export_inspections (
    id BIGSERIAL PRIMARY KEY,
    week_ending DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    destination VARCHAR(100) NOT NULL,

    -- Quantities
    pounds NUMERIC,
    metric_tons NUMERIC,
    thousand_bushels NUMERIC,

    -- Classification
    marketing_year VARCHAR(10),
    port VARCHAR(50),
    grade VARCHAR(50),
    commodity_class VARCHAR(50),

    -- Quality metrics (optional, from detailed records)
    moisture_avg NUMERIC,
    test_weight NUMERIC,
    protein_avg NUMERIC,
    oil_avg NUMERIC,

    -- Metadata
    source_file VARCHAR(100),
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_export_inspection
        UNIQUE (week_ending, commodity, destination, port, grade)
);

-- Weekly aggregated totals by commodity
CREATE TABLE IF NOT EXISTS export_inspections_weekly (
    id BIGSERIAL PRIMARY KEY,
    week_ending DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,

    -- Totals
    total_thousand_bushels NUMERIC,
    total_metric_tons NUMERIC,

    -- Comparisons
    prior_week_thousand_bushels NUMERIC,
    week_over_week_change NUMERIC,
    prior_year_thousand_bushels NUMERIC,
    year_over_year_change NUMERIC,

    -- Running totals
    marketing_year VARCHAR(10),
    my_cumulative_thousand_bushels NUMERIC,

    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_weekly_inspection
        UNIQUE (week_ending, commodity)
);

-- Monthly totals by destination
CREATE TABLE IF NOT EXISTS export_inspections_monthly (
    id BIGSERIAL PRIMARY KEY,
    month_start DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    destination VARCHAR(100) NOT NULL,

    -- Totals
    total_thousand_bushels NUMERIC,
    total_metric_tons NUMERIC,
    total_million_bushels NUMERIC,

    -- Classification
    marketing_year VARCHAR(10),
    destination_region VARCHAR(50),

    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_monthly_inspection
        UNIQUE (month_start, commodity, destination)
);

-- Marketing year totals by destination
CREATE TABLE IF NOT EXISTS export_inspections_yearly (
    id BIGSERIAL PRIMARY KEY,
    marketing_year VARCHAR(10) NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    destination VARCHAR(100) NOT NULL,

    -- Totals
    total_million_bushels NUMERIC,
    total_mmt NUMERIC,  -- Million metric tons

    -- Share of total
    pct_of_total NUMERIC,

    destination_region VARCHAR(50),

    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_yearly_inspection
        UNIQUE (marketing_year, commodity, destination)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_export_insp_week ON export_inspections(week_ending);
CREATE INDEX IF NOT EXISTS idx_export_insp_commodity ON export_inspections(commodity);
CREATE INDEX IF NOT EXISTS idx_export_insp_destination ON export_inspections(destination);
CREATE INDEX IF NOT EXISTS idx_export_insp_my ON export_inspections(marketing_year);

CREATE INDEX IF NOT EXISTS idx_weekly_insp_week ON export_inspections_weekly(week_ending);
CREATE INDEX IF NOT EXISTS idx_weekly_insp_commodity ON export_inspections_weekly(commodity);

CREATE INDEX IF NOT EXISTS idx_monthly_insp_month ON export_inspections_monthly(month_start);
CREATE INDEX IF NOT EXISTS idx_monthly_insp_dest ON export_inspections_monthly(destination);

CREATE INDEX IF NOT EXISTS idx_yearly_insp_my ON export_inspections_yearly(marketing_year);
CREATE INDEX IF NOT EXISTS idx_yearly_insp_dest ON export_inspections_yearly(destination);
