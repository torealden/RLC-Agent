-- Migration 051: implied unit prices for everything in trade
--
-- Date: 2026-05-06
--
-- Why:
--   bronze.census_trade has value_usd and quantity (in source units like KG,
--   MT, LT) for every commodity, every month, every partner. The
--   implied unit price = value_usd / quantity is mechanically computable
--   but you need to know the right quote-unit per commodity to make it
--   useful: cents/lb for veg oils, $/bushel for grains and oilseeds,
--   $/gallon for fuels, $/short_ton for meals, etc.
--
--   This migration adds two columns to silver.trade_commodity_reference
--   that encode the preferred quote unit and the multiplier from
--   raw-source-units to that quote unit, then exposes a gold view that
--   joins everything together.
--
-- Conventions (industry standard quote units):
--   - Vegetable oils, animal fats, greases:  cents per pound
--   - Oilseeds, grains:                       dollars per bushel
--   - Meals (oilseed cakes):                  dollars per short ton
--   - Cotton:                                 dollars per pound
--   - Rice:                                   dollars per hundredweight (CWT)
--   - Liquid fuels (ethanol, biodiesel,
--     diesel, gasoline, jet, kerosene):       dollars per gallon
--   - Natural gas, LNG, fossil-only commodities  -> deferred (need
--     energy-density factors; not relevant for current ag/biofuel work)
--
-- Bushel weights (USDA standard):
--   Soybeans, wheat:  60 lb/bushel  -> 27.21555 kg/bu
--   Corn, sorghum, flaxseed:  56 lb/bu  -> 25.40117 kg/bu
--   Barley:           48 lb/bu  -> 21.77244 kg/bu
--   Oats:             32 lb/bu  -> 14.51496 kg/bu
--
-- The factor column converts (value_usd / quantity_in_source_unit) to the
-- preferred quote unit by direct multiplication. So:
--   price_in_quote_unit = value_usd / quantity * price_unit_factor
-- For a KG-source veg oil:
--   $/kg * (100 / 2.20462) = cents/lb        factor = 45.35924
-- For a MT-source corn:
--   $/MT * (25.40117 / 1000) = $/bu          factor = 0.025401
-- etc.
-- Rows with NULL price_unit_factor are excluded from the gold view;
-- we'll fill those in as additional commodities come online.

-- =============================================================================
-- 1. Add the columns
-- =============================================================================

ALTER TABLE silver.trade_commodity_reference
    ADD COLUMN IF NOT EXISTS price_unit_label  TEXT,
    ADD COLUMN IF NOT EXISTS price_unit_factor NUMERIC(20, 10);

COMMENT ON COLUMN silver.trade_commodity_reference.price_unit_label IS
'Preferred quote unit for this commodity (e.g. cents_per_lb, usd_per_bushel, usd_per_gallon).';

COMMENT ON COLUMN silver.trade_commodity_reference.price_unit_factor IS
'Multiplier from value_usd/quantity_in_source_unit to price_unit_label. NULL = not yet defined; gold view excludes those rows.';

-- =============================================================================
-- 2. Populate by commodity_group + source_unit
-- =============================================================================

-- Vegetable oils (KG source, cents/lb)  factor = 100/2.20462 = 45.3592370
UPDATE silver.trade_commodity_reference
SET price_unit_label = 'cents_per_lb',
    price_unit_factor = 45.3592370
WHERE source_unit = 'KG'
  AND commodity_group IN (
    'SOYBEAN_OIL','CORN_OIL','CWG','OTHER_PIG_FAT','OTHER_BOVINE_FAT',
    'EDIBLE_TALLOW','INEDIBLE_TALLOW','LARD','LARD_STEARIN','POULTRY_FAT',
    'YELLOW_GREASE','UCO','COCONUT_OIL','COTTONSEED_OIL','SUNFLOWER_OIL',
    'CANOLA_OIL','PALM_OIL','PALM_KERNEL_OIL','LINSEED_OIL','SESAME_OIL',
    'OTHER_VEG_OIL'
  );

-- Peanut oil — table has display_unit '1,000 MT' (volume not price); use cents/lb
UPDATE silver.trade_commodity_reference
SET price_unit_label = 'cents_per_lb',
    price_unit_factor = 45.3592370
WHERE source_unit = 'KG' AND commodity_group = 'PEANUT_OIL';

-- Oilseeds / grains, KG source
UPDATE silver.trade_commodity_reference
SET price_unit_label = 'usd_per_bushel',
    price_unit_factor =
        CASE commodity_group
            WHEN 'SOYBEANS' THEN 27.2155422  -- 60 lb/bu
            WHEN 'WHEAT'    THEN 27.2155422
            WHEN 'CORN'     THEN 25.4011773  -- 56 lb/bu
            WHEN 'SORGHUM'  THEN 25.4011773
            WHEN 'FLAXSEED' THEN 25.4011773
            WHEN 'BARLEY'   THEN 21.7724376  -- 48 lb/bu
        END
WHERE source_unit = 'KG'
  AND commodity_group IN ('SOYBEANS','WHEAT','CORN','SORGHUM','FLAXSEED','BARLEY');

-- Oilseeds / grains, MT source (CORN, SOYBEANS, WHEAT, BARLEY have MT rows)
UPDATE silver.trade_commodity_reference
SET price_unit_label = 'usd_per_bushel',
    price_unit_factor =
        CASE commodity_group
            WHEN 'SOYBEANS' THEN 0.0272155
            WHEN 'WHEAT'    THEN 0.0272155
            WHEN 'CORN'     THEN 0.0254012
            WHEN 'BARLEY'   THEN 0.0217724
        END
WHERE source_unit = 'MT'
  AND commodity_group IN ('SOYBEANS','WHEAT','CORN','BARLEY');

-- Other minor seeds (peanuts in shell, sunflower seed, mustard, copra, etc.)
-- - they don't trade by bushel; use cents/lb for consistency
UPDATE silver.trade_commodity_reference
SET price_unit_label = 'cents_per_lb',
    price_unit_factor = 45.3592370
WHERE source_unit = 'KG'
  AND commodity_group IN ('PEANUTS','SUNFLOWER','MUSTARD','SAFFLOWER','SESAME',
                          'COPRA','CANOLA','PALM_KERNEL','COTTONSEED');

-- Meals (oilseed cakes, KG source) -> $/short ton; 1 short ton = 907.18474 kg
UPDATE silver.trade_commodity_reference
SET price_unit_label = 'usd_per_short_ton',
    price_unit_factor = 907.18474
WHERE source_unit = 'KG'
  AND commodity_group IN ('SOYBEAN_MEAL','CANOLA_MEAL','COTTONSEED_MEAL',
                          'COPRA_MEAL','PALM_KERNEL_MEAL','PEANUT_MEAL',
                          'SUNFLOWER_MEAL','LINSEED_MEAL','OILCAKE_OTHER');

-- Meals MT source (CORN_GLUTEN, DDGS) -> $/short ton; 1 ton = 0.90718 short tons
UPDATE silver.trade_commodity_reference
SET price_unit_label = 'usd_per_short_ton',
    price_unit_factor = 0.9071847
WHERE source_unit = 'MT'
  AND commodity_group IN ('CORN_GLUTEN','DDGS');

-- Fuels (LT source -> $/gallon; 1 gallon = 3.78541 liters)
UPDATE silver.trade_commodity_reference
SET price_unit_label = 'usd_per_gallon',
    price_unit_factor = 3.7854118
WHERE source_unit = 'LT'
  AND commodity_group IN ('FUEL_ETHANOL','BIODIESEL','DIESEL','MOTOR_GASOLINE',
                          'JET_FUEL','RESIDUAL_FUEL_OIL');

-- Cotton (KG source -> $/lb; 1 lb = 0.453592 kg)
UPDATE silver.trade_commodity_reference
SET price_unit_label = 'usd_per_lb',
    price_unit_factor = 0.4535924
WHERE source_unit = 'KG' AND commodity_group = 'COTTON';

-- Rice (KG source -> $/cwt; 1 cwt = 45.35924 kg)
UPDATE silver.trade_commodity_reference
SET price_unit_label = 'usd_per_cwt',
    price_unit_factor = 45.3592370
WHERE source_unit = 'KG' AND commodity_group = 'RICE';

-- =============================================================================
-- 3. Gold view: implied unit prices, monthly, world total
-- =============================================================================

DROP VIEW IF EXISTS gold.trade_implied_unit_price_monthly;

CREATE VIEW gold.trade_implied_unit_price_monthly AS
WITH ref AS (
    -- Pick one row per (hs_code, flow) preferring the more specific name when
    -- multiple commodity_names exist for the same code
    SELECT DISTINCT ON (hs_code_10, flow_type)
        hs_code_10,
        flow_type,
        commodity_group,
        commodity_name,
        source_unit,
        price_unit_label,
        price_unit_factor
    FROM silver.trade_commodity_reference
    WHERE is_active = TRUE
      AND price_unit_factor IS NOT NULL
    ORDER BY hs_code_10, flow_type, hs_code_10
),
agg AS (
    SELECT ct.year,
           ct.month,
           ct.flow,
           ct.hs_code,
           ct.country_code,
           SUM(ct.value_usd) AS value_usd,
           SUM(ct.quantity)  AS quantity
    FROM bronze.census_trade ct
    WHERE ct.country_code = '-'         -- World Total
      AND ct.quantity > 0
    GROUP BY ct.year, ct.month, ct.flow, ct.hs_code, ct.country_code
)
SELECT a.year,
       a.month,
       (a.year || '-' || lpad(a.month::text, 2, '0'))::text AS year_month,
       a.flow,
       a.hs_code,
       r.commodity_group,
       r.commodity_name,
       r.source_unit,
       a.quantity              AS quantity_in_source_unit,
       a.value_usd,
       r.price_unit_label,
       ROUND(
           (a.value_usd / NULLIF(a.quantity, 0) * r.price_unit_factor)::numeric,
           4
       ) AS implied_price
FROM agg a
JOIN ref r ON r.hs_code_10 = a.hs_code AND r.flow_type = upper(a.flow)
ORDER BY a.year, a.month, r.commodity_group, a.flow, a.hs_code;

COMMENT ON VIEW gold.trade_implied_unit_price_monthly IS
'Monthly world-total implied unit prices for every trade-tracked commodity, in industry-standard quote units (cents/lb for oils and fats, $/bu for grains and oilseeds, $/gal for liquid fuels, $/short_ton for meals, $/lb for cotton, $/cwt for rice). Source: bronze.census_trade joined to silver.trade_commodity_reference. Caveat: implied unit values reflect export/import mix and small monthly volumes can produce noisy results — use volume + 3-month smoothing for charts.';
