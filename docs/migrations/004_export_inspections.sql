-- ============================================================================
-- EXPORT INSPECTIONS TABLE
-- Migration: 004_export_inspections.sql
--
-- Stores weekly export inspection data from FGIS
-- Source: https://fgisonline.ams.usda.gov/ExportGrainReport/
-- ============================================================================

-- Weekly export inspections (detailed records)
-- Contains all individual inspection records with quality metrics
CREATE TABLE IF NOT EXISTS export_inspections (
    id BIGSERIAL PRIMARY KEY,
    week_ending DATE NOT NULL,
    cert_date DATE,  -- Actual inspection date
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

    -- Quality Metrics - Critical for Analysis
    -- Moisture (affects pricing, grading, storage)
    moisture_avg NUMERIC,
    moisture_high NUMERIC,
    moisture_low NUMERIC,

    -- Test Weight (density indicator, bushels/lb)
    test_weight NUMERIC,

    -- Protein (affects meal value)
    protein_avg NUMERIC,
    protein_high NUMERIC,
    protein_low NUMERIC,

    -- Oil (affects oil extraction value)
    oil_avg NUMERIC,
    oil_high NUMERIC,
    oil_low NUMERIC,

    -- Damage metrics
    total_damage_avg NUMERIC,
    heat_damage_avg NUMERIC,
    foreign_material_avg NUMERIC,

    -- Splits (for soybeans)
    splits_avg NUMERIC,

    -- Dockage
    dockage_avg NUMERIC,

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

-- ============================================================================
-- REFERENCE STATISTICS TABLES
-- Pre-calculated statistics that the LLM uses for analysis
-- ============================================================================

-- Weekly quality metrics by destination (weighted averages)
CREATE TABLE IF NOT EXISTS export_quality_weekly (
    id BIGSERIAL PRIMARY KEY,
    week_ending DATE NOT NULL,
    commodity VARCHAR(50) NOT NULL,
    destination VARCHAR(100),  -- NULL means all destinations

    -- Volume for weighting
    total_thousand_bushels NUMERIC,
    sample_count INTEGER,

    -- Weighted average quality metrics
    moisture_wavg NUMERIC,
    test_weight_wavg NUMERIC,
    protein_wavg NUMERIC,
    oil_wavg NUMERIC,
    damage_wavg NUMERIC,
    splits_wavg NUMERIC,

    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_quality_weekly
        UNIQUE (week_ending, commodity, destination)
);

-- Reference statistics for comparisons
-- These are pre-calculated for LLM analysis
CREATE TABLE IF NOT EXISTS export_reference_stats (
    id BIGSERIAL PRIMARY KEY,
    commodity VARCHAR(50) NOT NULL,
    destination VARCHAR(100),  -- NULL means total
    stat_type VARCHAR(50) NOT NULL,  -- e.g., '5yr_avg', 'pre_trade_war_avg', 'ytd_total'
    period_type VARCHAR(20) NOT NULL,  -- 'weekly', 'monthly', 'yearly'

    -- For time-series stats
    reference_date DATE,  -- Week ending, month start, or MY start
    marketing_year VARCHAR(10),

    -- Values
    thousand_bushels NUMERIC,
    million_bushels NUMERIC,
    pct_of_total NUMERIC,
    pct_change_yoy NUMERIC,  -- Year-over-year change

    -- Metadata
    calculation_notes TEXT,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_ref_stat
        UNIQUE (commodity, destination, stat_type, period_type, reference_date)
);

-- Pre-trade war baseline (for China analysis)
-- Example: Store 5-year average before tariffs for comparison
CREATE TABLE IF NOT EXISTS export_baseline_stats (
    id BIGSERIAL PRIMARY KEY,
    commodity VARCHAR(50) NOT NULL,
    destination VARCHAR(100) NOT NULL,
    baseline_name VARCHAR(100) NOT NULL,  -- e.g., 'pre_trade_war_5yr_avg'
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,

    -- Baseline values
    avg_weekly_thousand_bushels NUMERIC,
    avg_monthly_thousand_bushels NUMERIC,
    avg_yearly_million_bushels NUMERIC,
    pct_of_total_exports NUMERIC,

    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_baseline
        UNIQUE (commodity, destination, baseline_name)
);

-- Indexes for reference stats
CREATE INDEX IF NOT EXISTS idx_quality_weekly_date ON export_quality_weekly(week_ending);
CREATE INDEX IF NOT EXISTS idx_quality_weekly_commodity ON export_quality_weekly(commodity);
CREATE INDEX IF NOT EXISTS idx_ref_stats_commodity ON export_reference_stats(commodity);
CREATE INDEX IF NOT EXISTS idx_ref_stats_type ON export_reference_stats(stat_type);
CREATE INDEX IF NOT EXISTS idx_baseline_commodity ON export_baseline_stats(commodity);
