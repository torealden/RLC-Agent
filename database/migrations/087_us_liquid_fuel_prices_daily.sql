-- =============================================================================
-- Migration 087: gold.us_liquid_fuel_prices_daily
-- =============================================================================
-- Fourth sibling view feeding the "Prices" sheet of the liquid fuel workbook.
-- Daily cadence (other three sheets are monthly).
--
-- Sources:
--   biodiesel_usd_gal       = bronze.fuel_prices.b100_national     (Fastmarkets, weekly Fri)
--   renewable_diesel_usd_gal = NULL  (column `rd_california` exists but its values
--                                     range 0.007–0.046 — clearly a spread or credit
--                                     value, not $/gal. Source needs investigation.)
--   diesel_usd_gal          = silver.futures_price HO FRONT settle (NYMEX, daily 2000+)
--   gasoline_usd_gal        = silver.futures_price RB FRONT settle (NYMEX, daily 2000+)
--   ethanol / saf / co_processing / jet_fuel / others = NULL (no clean daily source yet)
--
-- Output unit: USD per gallon for all liquid fuels.
-- Row exists for any date where at least one source has data.
-- =============================================================================

DROP VIEW IF EXISTS gold.us_liquid_fuel_prices_daily;

CREATE VIEW gold.us_liquid_fuel_prices_daily AS
WITH ho AS (
    SELECT trade_date AS price_date, settlement AS ulsd_gulf_usd_gal
    FROM silver.futures_price WHERE symbol = 'HO' AND contract_month = 'FRONT'
),
rb AS (
    SELECT trade_date AS price_date, settlement AS gasoline_usd_gal
    FROM silver.futures_price WHERE symbol = 'RB' AND contract_month = 'FRONT'
),
fm AS (
    -- bronze.fuel_prices is weekly (Fastmarkets Fri close). Use as-is.
    -- Only b100_national is in clean $/gal units; rd_california is excluded
    -- pending unit investigation.
    SELECT price_date,
           b100_national    AS biodiesel_usd_gal
    FROM bronze.fuel_prices
),
all_dates AS (
    SELECT price_date FROM ho
    UNION SELECT price_date FROM rb
    UNION SELECT price_date FROM fm
)
SELECT
    d.price_date,
    fm.biodiesel_usd_gal,
    NULL::numeric                 AS renewable_diesel_usd_gal,
    NULL::numeric                 AS co_processing_usd_gal,
    NULL::numeric                 AS saf_usd_gal,
    NULL::numeric                 AS ethanol_usd_gal,
    ho.ulsd_gulf_usd_gal          AS diesel_usd_gal,
    NULL::numeric                 AS jet_fuel_usd_gal,
    rb.gasoline_usd_gal,
    NULL::numeric                 AS glycerin_usd_gal,
    NULL::numeric                 AS fame_usd_gal,
    NULL::numeric                 AS renewable_naphtha_usd_gal,
    NULL::numeric                 AS renewable_propane_usd_gal,
    NULL::numeric                 AS soap_stock_usd_gal,
    NULL::numeric                 AS methyl_acetate_usd_gal
FROM all_dates d
LEFT JOIN ho ON ho.price_date = d.price_date
LEFT JOIN rb ON rb.price_date = d.price_date
LEFT JOIN fm ON fm.price_date = d.price_date
ORDER BY d.price_date;

COMMENT ON VIEW gold.us_liquid_fuel_prices_daily IS
'US liquid fuel prices in $/gal, daily where available. Diesel/Gasoline = NYMEX '
'HO/RB front-month settle. Biodiesel/RD = Fastmarkets weekly. Ethanol/SAF/jet/'
'co-proc currently NULL (no clean daily source ingested). DTN integration is '
'expected to expand coverage materially.';
