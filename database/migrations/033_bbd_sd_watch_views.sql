-- ============================================================================
-- Migration 033: BBD Supply-Demand Watch (gold layer)
-- ============================================================================
-- Powers section 06 of the weekly report: a unified monthly view of US BBD
-- (biomass-based diesel) feedstock supply, demand, stocks, and trade flows
-- with M/M and Y/Y deltas for ending-stocks-trajectory analysis.
--
-- Architecture:
--   silver.bbd_feedstock_dim    — canonical feedstock reference (code, name, category)
--   silver.bbd_feedstock_monthly — normalized monthly facts in MIL LBS
--                                  (production / stocks / food_industrial_use /
--                                   biofuel_use / imports / exports)
--   gold.bbd_sd_watch           — silver + computed M/M, Y/Y, 12-mo trailing
--   gold.bbd_sd_pivot           — wide pivot for the Excel dashboard panel
--
-- Unit convention (per user 2026-04-25):
--   - Oils, fats, greases (the FEEDSTOCK side): mil lbs
--   - Oilseeds & grains (the SEED side, e.g., soybean crush volume): mil bu
--   Each row in the silver/gold views carries its own value_unit column.
--   NASS oil data normalized via raw_value / 1e6 to bypass the per-commodity
--   conversion_factor variation (mil lbs vs '000 lbs') from prior migrations.
--   EIA feedstock data is already in mil lbs.
--   Census trade kg → mil lbs via × 0.0022046.
--   NASS oilseed crush from bronze.nass_processing (commodity_desc=SOYBEANS etc.)
--     when raw is BU: pass through; when LB: convert to BU using crush yield (60 lb/bu).
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- 1. Canonical feedstock dimension
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.bbd_feedstock_dim (
    feedstock_code      VARCHAR(40) PRIMARY KEY,
    feedstock_name      VARCHAR(100) NOT NULL,
    feedstock_category  VARCHAR(40)  NOT NULL,  -- veg_oil / animal_fat / waste_oil / other
    -- aliases used by source systems
    nass_class_desc     VARCHAR(40),  -- bronze.nass_processing.class_desc when commodity_desc='OIL'
    eia_feedstock_name  VARCHAR(60),  -- bronze.eia_feedstock_monthly.feedstock_name
    crush_attr_commodity VARCHAR(40), -- silver.crush_attribute_reference.commodity
    census_hs_codes     TEXT[],        -- HS codes used for trade lookup
    is_bbd_feedstock    BOOLEAN DEFAULT TRUE,
    sort_order          INTEGER
);

INSERT INTO silver.bbd_feedstock_dim
    (feedstock_code, feedstock_name, feedstock_category, nass_class_desc,
     eia_feedstock_name, crush_attr_commodity, census_hs_codes, sort_order) VALUES
    ('SBO',      'Soybean Oil',       'veg_oil',    'SOYBEAN',     'Soybean Oil',         'soybeans',   ARRAY['1507']::TEXT[],            10),
    ('CANO',     'Canola Oil',        'veg_oil',    'CANOLA',      'Canola Oil',          'canola',     ARRAY['151411','151419','151491','151499']::TEXT[], 20),
    ('SUNO',     'Sunflower Oil',     'veg_oil',    'SUNFLOWER',   NULL,                   'sunflower',  ARRAY['151211','151219']::TEXT[], 30),
    ('CSO',      'Cottonseed Oil',    'veg_oil',    'COTTONSEED',  NULL,                   'cottonseed', ARRAY['1512']::TEXT[],            40),
    ('CO',       'Corn Oil (food)',   'veg_oil',    'CORN',        NULL,                   'corn',       ARRAY['151521']::TEXT[],          50),
    ('DCO',      'Distillers Corn Oil','veg_oil',   NULL,           'Corn Oil',            NULL,         ARRAY['151521']::TEXT[],          51),
    ('PNUT',     'Peanut Oil',        'veg_oil',    'PEANUT',      NULL,                   'peanut',     ARRAY['1508']::TEXT[],            60),
    ('PMO',      'Palm Oil',          'veg_oil',    'PALM',        NULL,                   'palm',       ARRAY['1511']::TEXT[],            70),
    ('PKO',      'Palm Kernel Oil',   'veg_oil',    'PALM KERNEL', NULL,                   'palm_kernel',ARRAY['151321','151329']::TEXT[], 80),
    ('CNO',      'Coconut Oil',       'veg_oil',    'COCONUT',     NULL,                   'coconut',    ARRAY['151311','151319']::TEXT[], 90),
    ('SAFF',     'Safflower Oil',     'veg_oil',    'SAFFLOWER',   NULL,                   'safflower',  ARRAY['151911']::TEXT[],         100),
    ('TALLOW',   'Tallow (all)',      'animal_fat', NULL,           'Tallow',              NULL,         ARRAY['150200']::TEXT[],         200),
    ('EBFT',     'Edible Beef Tallow','animal_fat', NULL,           NULL,                   NULL,         ARRAY['150200']::TEXT[],         201),
    ('IBFT',     'Inedible Beef Tallow','animal_fat',NULL,          NULL,                   NULL,         ARRAY['150200']::TEXT[],         202),
    ('LARD',     'Lard',              'animal_fat', NULL,           NULL,                   NULL,         ARRAY['150100']::TEXT[],         210),
    ('CWG',      'Choice White Grease','animal_fat',NULL,           'White Grease',         NULL,         ARRAY['150100','151800']::TEXT[],220),
    ('PFAT',     'Poultry Fat',       'animal_fat', NULL,           'Poultry',              NULL,         ARRAY['150190']::TEXT[],         230),
    ('YG',       'Yellow Grease',     'waste_oil',  NULL,           'Yellow Grease',        NULL,         ARRAY['151800']::TEXT[],         300),
    ('UCO',      'Used Cooking Oil',  'waste_oil',  NULL,           'Other Recycled',       NULL,         ARRAY['151800']::TEXT[],         310),
    ('OVO',      'Other Vegetable Oil','veg_oil',   NULL,           'Other Vegetable Oil',  NULL,         NULL,                            900),
    ('OWASTE',   'Other Waste',       'waste_oil',  NULL,           'Other Waste',          NULL,         NULL,                            910)
ON CONFLICT (feedstock_code) DO UPDATE SET
    feedstock_name = EXCLUDED.feedstock_name,
    feedstock_category = EXCLUDED.feedstock_category,
    nass_class_desc = EXCLUDED.nass_class_desc,
    eia_feedstock_name = EXCLUDED.eia_feedstock_name,
    crush_attr_commodity = EXCLUDED.crush_attr_commodity,
    census_hs_codes = EXCLUDED.census_hs_codes,
    sort_order = EXCLUDED.sort_order;

-- ----------------------------------------------------------------------------
-- 2. Per-commodity unit + yield reference for seed → oil conversion
-- ----------------------------------------------------------------------------
-- Bushels per short ton (2,000 lbs / lbs-per-bushel) for major oilseeds.
-- Used to convert NASS soybean/canola crush (reported in TONS) → mil bu.
CREATE TABLE IF NOT EXISTS silver.bbd_seed_unit_ref (
    nass_commodity_desc VARCHAR(40) PRIMARY KEY,
    feedstock_code      VARCHAR(40) NOT NULL,
    lbs_per_bushel      NUMERIC(6,2) NOT NULL,
    bu_per_short_ton    NUMERIC(8,4) GENERATED ALWAYS AS (2000.0 / lbs_per_bushel) STORED
);

INSERT INTO silver.bbd_seed_unit_ref (nass_commodity_desc, feedstock_code, lbs_per_bushel) VALUES
    ('SOYBEANS', 'SBO',  60),
    ('CANOLA',   'CANO', 50),
    ('SUNFLOWER','SUNO', 24),  -- 24 lb/bu by USDA convention (sometimes 32 for confection)
    ('CORN',     'CO',   56),
    ('PEANUTS',  'PNUT', 28)   -- in-shell; shelled basis differs
ON CONFLICT (nass_commodity_desc) DO UPDATE SET
    feedstock_code = EXCLUDED.feedstock_code,
    lbs_per_bushel = EXCLUDED.lbs_per_bushel;

-- ----------------------------------------------------------------------------
-- 3. Normalized monthly facts (silver)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW silver.bbd_feedstock_monthly AS
WITH nass_attrs_wide AS (
    -- Pivot NASS oil attributes to one row per (feedstock, period) so we can
    -- pick the RIGHT attribute per metric (avoids double-counting crude+refined).
    -- All values normalized to mil lbs via raw_value / 1e6.
    SELECT
        d.feedstock_code,
        m.year, m.month,
        make_date(m.year, m.month, 1) AS period,
        SUM(CASE WHEN m.attribute_code = 'crude_oil_production'           THEN m.raw_value END) / 1e6 AS crude_oil_production,
        SUM(CASE WHEN m.attribute_code = 'refined_oil_production'         THEN m.raw_value END) / 1e6 AS refined_oil_production,
        SUM(CASE WHEN m.attribute_code IN ('crude_oil_stocks','crude_oil_stocks_total','crude_oil_crusher_stocks')
                 THEN m.raw_value END) / 1e6 AS crude_stocks_total,
        SUM(CASE WHEN m.attribute_code IN ('refined_oil_stocks','refined_oil_stocks_total')
                 THEN m.raw_value END) / 1e6 AS refined_stocks_total,
        SUM(CASE WHEN m.attribute_code = 'oil_offsite_stocks'             THEN m.raw_value END) / 1e6 AS offsite_stocks,
        SUM(CASE WHEN m.attribute_code = 'refined_oil_edible_use'         THEN m.raw_value END) / 1e6 AS refined_edible_use,
        SUM(CASE WHEN m.attribute_code = 'refined_oil_inedible_use'       THEN m.raw_value END) / 1e6 AS refined_inedible_use,
        SUM(CASE WHEN m.attribute_code = 'refined_oil_further_processing' THEN m.raw_value END) / 1e6 AS refined_further_processing
    FROM gold.nass_crush_mapped m
    JOIN silver.bbd_feedstock_dim d ON d.crush_attr_commodity = m.commodity
    WHERE m.raw_value IS NOT NULL AND m.raw_unit = 'LB'
    GROUP BY d.feedstock_code, m.year, m.month
),
nass_metrics AS (
    -- Pick exactly ONE attribute per metric per commodity:
    --   production     = crude_oil_production (the actual new oil from crush);
    --                    fall back to refined_oil_production if crude not reported
    --                    (true for canola/coconut/palm where NASS only publishes refined).
    --   stocks         = sum of distinct inventory categories (crude + refined)
    --   food_industrial_use = edible + inedible if both reported (soybeans);
    --                    else refined_further_processing as a single "consumption" proxy
    --                    (true for minor oils where NASS doesn't break out edible vs inedible).
    SELECT feedstock_code, year, month, period,
           'production'::VARCHAR AS metric,
           COALESCE(crude_oil_production, refined_oil_production) AS value
    FROM nass_attrs_wide
    WHERE COALESCE(crude_oil_production, refined_oil_production) IS NOT NULL
    UNION ALL
    SELECT feedstock_code, year, month, period,
           'stocks',
           COALESCE(crude_stocks_total, 0) + COALESCE(refined_stocks_total, 0)
    FROM nass_attrs_wide
    WHERE COALESCE(crude_stocks_total, refined_stocks_total) IS NOT NULL
    UNION ALL
    SELECT feedstock_code, year, month, period,
           'food_industrial_use',
           CASE
             WHEN refined_edible_use IS NOT NULL OR refined_inedible_use IS NOT NULL
                  THEN COALESCE(refined_edible_use, 0) + COALESCE(refined_inedible_use, 0)
             ELSE refined_further_processing
           END
    FROM nass_attrs_wide
    WHERE refined_edible_use IS NOT NULL
       OR refined_inedible_use IS NOT NULL
       OR refined_further_processing IS NOT NULL
),
nass_aggregated AS (
    SELECT feedstock_code, year, month, period, metric, value,
           'mil lbs'::VARCHAR AS value_unit,
           NULL::TEXT          AS detail,
           'NASS_FATS_OILS'::VARCHAR AS source
    FROM nass_metrics
),
nass_seed_crush AS (
    -- Seed-level crush volume in MIL BU (per user 2026-04-25: bushels for oilseeds/grains).
    -- NASS reports SOYBEANS/CANOLA crush in TONS → convert via bu_per_short_ton.
    SELECT
        ref.feedstock_code,
        bp.year,
        bp.month,
        make_date(bp.year, bp.month, 1) AS period,
        'crush_seed'::VARCHAR AS metric,
        bp.short_desc AS detail,
        (bp.value * ref.bu_per_short_ton) / 1e6 AS value,
        'mil bu'::VARCHAR AS value_unit,
        'NASS_CRUSH'::VARCHAR AS source
    FROM bronze.nass_processing bp
    JOIN silver.bbd_seed_unit_ref ref ON bp.commodity_desc = ref.nass_commodity_desc
    WHERE bp.statisticcat_desc = 'CRUSHED'
      AND bp.unit_desc = 'TONS'
      AND bp.value IS NOT NULL
      AND bp.month IS NOT NULL
),
eia_biofuel_use AS (
    -- Feedstock consumed by biofuel plants (already mil lbs in the EIA bronze table)
    SELECT
        d.feedstock_code,
        e.year,
        e.month,
        make_date(e.year, e.month, 1) AS period,
        CASE
            WHEN e.plant_type = 'biodiesel'        THEN 'biofuel_use_bd'
            WHEN e.plant_type = 'renewable_diesel' THEN 'biofuel_use_rd'
            WHEN e.plant_type = 'total'            THEN 'biofuel_use_total'
            ELSE 'biofuel_use_other'
        END AS metric,
        e.plant_type AS detail,
        e.quantity_mil_lbs AS value,
        'mil lbs'::VARCHAR AS value_unit,
        'EIA_FEEDSTOCK' AS source
    FROM bronze.eia_feedstock_monthly e
    JOIN silver.bbd_feedstock_dim d ON d.eia_feedstock_name = e.feedstock_name
    WHERE e.is_withheld = FALSE
      AND e.is_no_data  = FALSE
      AND e.quantity_mil_lbs IS NOT NULL
),
census_trade_facts AS (
    -- Imports / exports from Census trade by HS-6 match against feedstock HS list
    -- silver.census_trade_monthly stores quantity_display in '000 Pounds';
    -- divide by 1000 to express in mil lbs.
    SELECT
        d.feedstock_code,
        ct.year,
        ct.month,
        make_date(ct.year, ct.month, 1) AS period,
        CASE WHEN ct.flow ILIKE 'import%' THEN 'imports' ELSE 'exports' END AS metric,
        ct.hs_code_6 AS detail,
        SUM(ct.quantity_display) / 1000.0 AS value,   -- '000 lbs' -> mil lbs
        'mil lbs'::VARCHAR AS value_unit,
        'CENSUS_TRADE' AS source
    FROM silver.census_trade_monthly ct
    JOIN silver.bbd_feedstock_dim d
      ON ct.hs_code_6 = ANY(d.census_hs_codes)
      OR SUBSTRING(ct.hs_code_6 FROM 1 FOR 4) = ANY(d.census_hs_codes)
    WHERE ct.quantity_display > 0
      AND ct.is_regional_total = FALSE
    GROUP BY d.feedstock_code, ct.year, ct.month, ct.flow, ct.hs_code_6
)
SELECT feedstock_code, year, month, period, metric, value, value_unit, detail, source FROM nass_aggregated
UNION ALL
SELECT feedstock_code, year, month, period, metric, value, value_unit, detail, source FROM nass_seed_crush
UNION ALL
SELECT feedstock_code, year, month, period, metric, value, value_unit, detail, source FROM eia_biofuel_use
UNION ALL
SELECT feedstock_code, year, month, period, metric, value, value_unit, detail, source FROM census_trade_facts;

COMMENT ON VIEW silver.bbd_feedstock_monthly IS
'Normalized monthly BBD feedstock facts. Each row carries value_unit:
   mil lbs  for oils, fats, greases, trade flows, biofuel feedstock use
   mil bu   for oilseed crush volumes (per user convention 2026-04-25)
 Metrics: production, stocks, food_industrial_use, crush_seed,
   biofuel_use_(bd|rd|total), imports, exports.
 Sources: NASS_FATS_OILS, NASS_CRUSH, EIA_FEEDSTOCK, CENSUS_TRADE.';

-- ----------------------------------------------------------------------------
-- 4. BBD Supply-Demand Watch (gold) — with M/M and Y/Y deltas
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.bbd_sd_watch AS
WITH base AS (
    SELECT
        m.feedstock_code,
        d.feedstock_name,
        d.feedstock_category,
        d.sort_order,
        m.metric,
        m.value_unit,
        m.period,
        m.year,
        m.month,
        SUM(m.value) AS value   -- collapse multi-source duplicates per (code,metric,period,unit)
    FROM silver.bbd_feedstock_monthly m
    JOIN silver.bbd_feedstock_dim d USING (feedstock_code)
    GROUP BY m.feedstock_code, d.feedstock_name, d.feedstock_category, d.sort_order,
             m.metric, m.value_unit, m.period, m.year, m.month
)
SELECT
    feedstock_code,
    feedstock_name,
    feedstock_category,
    sort_order,
    metric,
    value_unit,
    period,
    year,
    month,
    value,
    -- Month-over-month
    LAG(value) OVER w_metric AS prev_month_value,
    value - LAG(value) OVER w_metric AS mom_change,
    CASE WHEN LAG(value) OVER w_metric > 0
         THEN (value - LAG(value) OVER w_metric)
              / LAG(value) OVER w_metric * 100
    END AS mom_pct,
    -- Year-over-year
    LAG(value, 12) OVER w_metric AS prior_year_value,
    value - LAG(value, 12) OVER w_metric AS yoy_change,
    CASE WHEN LAG(value, 12) OVER w_metric > 0
         THEN (value - LAG(value, 12) OVER w_metric)
              / LAG(value, 12) OVER w_metric * 100
    END AS yoy_pct,
    -- 12-month trailing
    SUM(value) OVER (PARTITION BY feedstock_code, metric ORDER BY period
                      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS trailing_12mo
FROM base
WINDOW w_metric AS (PARTITION BY feedstock_code, metric ORDER BY period);

COMMENT ON VIEW gold.bbd_sd_watch IS
'BBD Supply-Demand Watch — drives section 06 of the weekly report.
 Per (feedstock, metric, month): value (in value_unit), M/M and Y/Y deltas,
 12-mo trailing. value_unit is mil lbs for oils/fats, mil bu for crush_seed.';

-- ----------------------------------------------------------------------------
-- 5. Wide pivot for Excel-friendly consumption (one row per period × feedstock)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.bbd_sd_pivot AS
SELECT
    feedstock_code,
    feedstock_name,
    feedstock_category,
    period,
    year,
    month,
    -- Oil/fat metrics in mil lbs
    SUM(value) FILTER (WHERE metric = 'production')           AS production_mil_lbs,
    SUM(value) FILTER (WHERE metric = 'stocks')               AS stocks_mil_lbs,
    SUM(value) FILTER (WHERE metric = 'imports')              AS imports_mil_lbs,
    SUM(value) FILTER (WHERE metric = 'exports')              AS exports_mil_lbs,
    SUM(value) FILTER (WHERE metric = 'food_industrial_use')  AS food_industrial_use_mil_lbs,
    SUM(value) FILTER (WHERE metric = 'biofuel_use_bd')       AS biofuel_use_bd_mil_lbs,
    SUM(value) FILTER (WHERE metric = 'biofuel_use_rd')       AS biofuel_use_rd_mil_lbs,
    SUM(value) FILTER (WHERE metric = 'biofuel_use_total')    AS biofuel_use_total_mil_lbs,
    -- Seed crush in mil bu (per user 2026-04-25 convention)
    SUM(value) FILTER (WHERE metric = 'crush_seed')           AS crush_seed_mil_bu
FROM gold.bbd_sd_watch
GROUP BY feedstock_code, feedstock_name, feedstock_category, period, year, month;

COMMENT ON VIEW gold.bbd_sd_pivot IS
'Wide-format companion to gold.bbd_sd_watch — one row per (feedstock, period)
 with metric columns. Each metric column is suffixed with its unit (_mil_lbs
 for oils/fats, _mil_bu for seed crush).';

-- ----------------------------------------------------------------------------
-- 6. Permissions
-- ----------------------------------------------------------------------------
GRANT SELECT ON silver.bbd_feedstock_dim     TO PUBLIC;
GRANT SELECT ON silver.bbd_seed_unit_ref     TO PUBLIC;
GRANT SELECT ON silver.bbd_feedstock_monthly TO PUBLIC;
GRANT SELECT ON gold.bbd_sd_watch            TO PUBLIC;
GRANT SELECT ON gold.bbd_sd_pivot            TO PUBLIC;

COMMIT;
