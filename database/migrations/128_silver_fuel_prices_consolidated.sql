-- Migration 128: silver.fuel_prices_consolidated — EIA + legacy fastmarkets
--
-- Parallel to silver.feedstock_prices_consolidated (mig 120). Combines:
--   - EIA v2 observations (bronze.eia_observations) — live, public-domain
--   - Legacy bronze.fuel_prices (fastmarkets, frozen ~2025-04-18)
--
-- Output shape mirrors bronze.fuel_prices column structure so existing
-- consumers (workbook builders, dashboards, the data_pack) can drop-in
-- the new view path.
--
-- Per memory:feedback_fastmarkets_keep_dont_show — fastmarkets rows
-- stay in the view for internal triangulation; client-facing consumers
-- filter source = 'EIA' (or NOT LIKE 'fastmarkets%') before rendering.

BEGIN;

CREATE OR REPLACE VIEW silver.fuel_prices_consolidated AS

-- Pivot EIA series into bronze.fuel_prices column shape (one row per date)
WITH eia_pivot AS (
    SELECT
        period AS price_date,
        frequency,
        MAX(CASE WHEN series_id = 'EER_EPD2DXL0_PF4_RGC_DPG' THEN value END)
            AS ulsd_gulf,
        MAX(CASE WHEN series_id = 'EER_EPD2DXL0_PF4_Y35NY_DPG' THEN value END)
            AS ulsd_nyharbor,
        MAX(CASE WHEN series_id = 'EER_EPJK_PF4_RGC_DPG' THEN value END)
            AS jet_a_spot,
        -- US retail diesel + gasoline (additional columns vs legacy schema)
        MAX(CASE WHEN series_id = 'EMD_EPD0_PTE_NUS_DPG' THEN value END)
            AS retail_diesel_us,
        MAX(CASE WHEN series_id = 'EMM_EPM0_PTE_NUS_DPG' THEN value END)
            AS retail_gasoline_us,
        -- Wholesale gasoline (RBOB)
        MAX(CASE WHEN series_id = 'EER_EPMRR_PF4_RGC_DPG' THEN value END)
            AS rbob_gulf,
        MAX(CASE WHEN series_id = 'EER_EPMR_PF4_Y35NY_DPG' THEN value END)
            AS rbob_nyharbor,
        -- Propane Mont Belvieu
        MAX(CASE WHEN series_id = 'EER_EPLLPA_PF4_RGC_DPG' THEN value END)
            AS propane_mb,
        -- WTI crude (daily, fills wti_crude proxy)
        MAX(CASE WHEN series_id = 'RWTC' THEN value END)
            AS wti_crude,
        -- Brent crude
        MAX(CASE WHEN series_id = 'RBRTE' THEN value END)
            AS brent_crude
    FROM bronze.eia_observations
    WHERE series_id IN (
        'EER_EPD2DXL0_PF4_RGC_DPG', 'EER_EPD2DXL0_PF4_Y35NY_DPG',
        'EER_EPJK_PF4_RGC_DPG', 'EMD_EPD0_PTE_NUS_DPG',
        'EMM_EPM0_PTE_NUS_DPG', 'EER_EPMRR_PF4_RGC_DPG',
        'EER_EPMR_PF4_Y35NY_DPG', 'EER_EPLLPA_PF4_RGC_DPG',
        'RWTC', 'RBRTE'
    )
    GROUP BY period, frequency
)

-- EIA path (live data going forward)
SELECT
    price_date,
    frequency,
    -- Petroleum prices
    ulsd_gulf,
    ulsd_nyharbor,
    jet_a_spot,
    rbob_gulf,
    rbob_nyharbor,
    propane_mb,
    wti_crude,
    brent_crude,
    retail_diesel_us,
    retail_gasoline_us,
    -- B100 / RD columns left NULL — EIA doesn't publish biodiesel cash;
    -- those still come from the fastmarkets arm below.
    NULL::numeric AS b100_national,
    NULL::numeric AS b100_northeast,
    NULL::numeric AS b100_southeast,
    NULL::numeric AS b100_upper_midwest,
    NULL::numeric AS b100_lower_midwest,
    NULL::numeric AS b100_south_central,
    NULL::numeric AS b100_rocky_mountain,
    NULL::numeric AS rd_california,
    NULL::numeric AS heating_oil_futures,
    'EIA' AS source,
    FALSE AS is_proprietary
FROM eia_pivot

UNION ALL

-- Legacy fastmarkets path (B100, RD, heating oil futures — until sourced)
SELECT
    price_date,
    frequency,
    ulsd_gulf,
    ulsd_nyharbor,
    NULL::numeric AS jet_a_spot,   -- jet_a_spot in legacy is NULL anyway
    NULL::numeric AS rbob_gulf,
    NULL::numeric AS rbob_nyharbor,
    NULL::numeric AS propane_mb,
    wti_crude,
    NULL::numeric AS brent_crude,
    NULL::numeric AS retail_diesel_us,
    NULL::numeric AS retail_gasoline_us,
    b100_national,
    b100_northeast,
    b100_southeast,
    b100_upper_midwest,
    b100_lower_midwest,
    b100_south_central,
    b100_rocky_mountain,
    rd_california,
    heating_oil_futures,
    source,
    is_proprietary
FROM bronze.fuel_prices;

COMMENT ON VIEW silver.fuel_prices_consolidated IS
'Unified fuel price feed. UNIONs bronze.fuel_prices (legacy fastmarkets, frozen ~2025-04) with bronze.eia_observations (EIA v2 live). Per row source column tells consumers which feed it came from. Client-facing renderers should filter source = ''EIA'' to avoid the keep-don''t-show fastmarkets data.';

COMMIT;

-- Verification:
-- SELECT source, COUNT(*) AS n, MIN(price_date), MAX(price_date)
-- FROM silver.fuel_prices_consolidated GROUP BY source;
