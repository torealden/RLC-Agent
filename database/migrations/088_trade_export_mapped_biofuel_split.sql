-- =============================================================================
-- Migration 088: Wire BD/RD split into gold.trade_export_mapped
-- =============================================================================
-- Problem: gold.trade_export_matrix (used by EnergyTradeUpdater VBA) maps all
-- HS 3826 census records to commodity_group='BIODIESEL' via the silver.trade_
-- commodity_reference table. Tore's new RD tabs in us_fuel_trade.xlsm query
-- commodity_group='RENEWABLE_DIESEL' and get no rows.
--
-- Solution: Replace HS 3826 rows in gold.trade_export_mapped with two output
-- rows per source — one BIODIESEL and one RENEWABLE_DIESEL — using the same
-- split logic that gold.biofuel_trade_split applies. The Census '-' record
-- (TOTAL FOR ALL COUNTRIES) is replaced with SUMS of the per-country splits
-- rather than the default 90/10 share applied to the aggregate, because the
-- country-weighted mix is more accurate than the default rule.
--
-- Side-effect: Tore's existing BD tabs will show SPLIT BD numbers (smaller
-- than the un-split totals before). That's the correct behavior — prior
-- un-split numbers double-counted RD imports as BD. Same logic as the
-- bal-sheet workbook population from yesterday.
-- =============================================================================

CREATE OR REPLACE VIEW gold.trade_export_mapped AS
WITH non_biofuel AS (
    SELECT
        ct.year, ct.month, ct.flow, ct.hs_code,
        cr.commodity_group, cr.commodity_name, cr.display_unit,
        ct.country_code,
        ct.country_name                                                AS census_country_name,
        COALESCE(tcr.country_name, ct.country_name)                    AS standard_country_name,
        tcr.region, tcr.region_sort_order, tcr.country_sort_order,
        tcr.spreadsheet_row, tcr.is_regional_total,
        ct.value_usd,
        ct.quantity                                                    AS quantity_raw,
        ct.quantity * cr.conversion_factor                             AS quantity_converted,
        cr.conversion_factor,
        CASE WHEN ct.month >= 9
             THEN (ct.year || '/') || RIGHT((ct.year + 1)::text, 2)
             ELSE ((ct.year - 1) || '/') || RIGHT(ct.year::text, 2)
        END                                                            AS marketing_year,
        CASE WHEN ct.month >= 9 THEN ct.year + 1 ELSE ct.year END      AS marketing_year_end
    FROM bronze.census_trade ct
    LEFT JOIN silver.trade_commodity_reference cr
           ON ct.hs_code::text = cr.hs_code_10::text
          AND UPPER(ct.flow::text) = cr.flow_type::text
    LEFT JOIN silver.trade_country_reference tcr
           ON UPPER(ct.country_name::text) = UPPER(tcr.country_name::text)
           OR UPPER(ct.country_name::text) = UPPER(tcr.country_name_alt::text)
    WHERE cr.is_active = true
      AND ct.hs_code NOT LIKE '3826%'
),
biofuel_country_base AS (
    -- HS 3826 records, COUNTRY-LEVEL only (excludes '-' aggregate and regional
    -- codes like 0001/1XXX). Those rows are re-derived as sums below.
    SELECT
        ct.id, ct.year, ct.month, ct.flow, ct.hs_code,
        ct.country_code,
        ct.country_name                                                AS census_country_name,
        COALESCE(tcr.country_name, ct.country_name)                    AS standard_country_name,
        tcr.region, tcr.region_sort_order, tcr.country_sort_order,
        tcr.spreadsheet_row, tcr.is_regional_total,
        ct.value_usd,
        ct.quantity                                                    AS quantity_raw,
        ct.quantity * COALESCE(bcf.blend_content_factor, 1.0)          AS quantity_net_kg,
        (
            SELECT bts.rule_id FROM reference.biofuel_trade_split bts
            WHERE bts.hs_code = ct.hs_code AND bts.flow = ct.flow
              AND ct.year BETWEEN bts.year_from AND bts.year_to
              AND (bts.origin = ct.country_code OR bts.origin IS NULL)
            ORDER BY (bts.origin IS NOT NULL) DESC, bts.year_from DESC
            LIMIT 1
        )                                                              AS rule_id
    FROM bronze.census_trade ct
    LEFT JOIN silver.trade_country_reference tcr
           ON UPPER(ct.country_name::text) = UPPER(tcr.country_name::text)
           OR UPPER(ct.country_name::text) = UPPER(tcr.country_name_alt::text)
    LEFT JOIN reference.biofuel_hs_blend_content bcf
           ON bcf.hs_code = ct.hs_code AND bcf.flow = ct.flow
    WHERE ct.hs_code LIKE '3826%'
      AND ct.country_code <> '-'
      AND COALESCE(tcr.is_regional_total, false) = false
),
biofuel_bd AS (
    SELECT
        b.year, b.month, b.flow, b.hs_code,
        'BIODIESEL'::varchar(50)                                       AS commodity_group,
        'Biodiesel (BD/RD split applied)'::varchar(100)                AS commodity_name,
        '000 gallons'::varchar(30)                                     AS display_unit,
        b.country_code, b.census_country_name, b.standard_country_name,
        b.region, b.region_sort_order, b.country_sort_order,
        b.spreadsheet_row, b.is_regional_total,
        (b.value_usd * COALESCE(r.bd_share, 1.0))::numeric(18,2)       AS value_usd,
        b.quantity_raw::numeric(18,4)                                  AS quantity_raw,
        (b.quantity_net_kg * COALESCE(r.bd_share, 1.0) * 0.301 / 1000.0)::numeric AS quantity_converted,
        0.000301000000::numeric(20,12)                                 AS conversion_factor,
        CASE WHEN b.month >= 9
             THEN (b.year || '/') || RIGHT((b.year + 1)::text, 2)
             ELSE ((b.year - 1) || '/') || RIGHT(b.year::text, 2)
        END                                                            AS marketing_year,
        CASE WHEN b.month >= 9 THEN b.year + 1 ELSE b.year END         AS marketing_year_end
    FROM biofuel_country_base b
    LEFT JOIN reference.biofuel_trade_split r ON r.rule_id = b.rule_id
),
biofuel_rd AS (
    SELECT
        b.year, b.month, b.flow, b.hs_code,
        'RENEWABLE_DIESEL'::varchar(50)                                AS commodity_group,
        'Renewable Diesel (BD/RD split applied)'::varchar(100)         AS commodity_name,
        '000 gallons'::varchar(30)                                     AS display_unit,
        b.country_code, b.census_country_name, b.standard_country_name,
        b.region, b.region_sort_order, b.country_sort_order,
        b.spreadsheet_row, b.is_regional_total,
        (b.value_usd * COALESCE(r.rd_share, 0.0))::numeric(18,2)       AS value_usd,
        b.quantity_raw::numeric(18,4)                                  AS quantity_raw,
        (b.quantity_net_kg * COALESCE(r.rd_share, 0.0) * 0.301 / 1000.0)::numeric AS quantity_converted,
        0.000301000000::numeric(20,12)                                 AS conversion_factor,
        CASE WHEN b.month >= 9
             THEN (b.year || '/') || RIGHT((b.year + 1)::text, 2)
             ELSE ((b.year - 1) || '/') || RIGHT(b.year::text, 2)
        END                                                            AS marketing_year,
        CASE WHEN b.month >= 9 THEN b.year + 1 ELSE b.year END         AS marketing_year_end
    FROM biofuel_country_base b
    LEFT JOIN reference.biofuel_trade_split r ON r.rule_id = b.rule_id
    WHERE COALESCE(r.rd_share, 0.0) > 0
),
-- Synthesize WORLD TOTAL + TOTAL FOR ALL COUNTRIES aggregate rows by summing
-- the per-country split values. tcr has two reference rows for the world total
-- (id=208 'WORLD TOTAL' row 217, and id=233 'TOTAL FOR ALL COUNTRIES' row 999)
-- — emit both so downstream views that key on either find a row.
biofuel_world AS (
    SELECT
        bf.year, bf.month, bf.flow, bf.hs_code,
        bf.commodity_group, bf.commodity_name, bf.display_unit,
        '-'::varchar(10)                                               AS country_code,
        'TOTAL FOR ALL COUNTRIES'::varchar(100)                        AS census_country_name,
        tcr.country_name                                               AS standard_country_name,
        tcr.region, tcr.region_sort_order, tcr.country_sort_order,
        tcr.spreadsheet_row, tcr.is_regional_total,
        SUM(bf.value_usd)::numeric(18,2)                               AS value_usd,
        SUM(bf.quantity_raw)::numeric(18,4)                            AS quantity_raw,
        SUM(bf.quantity_converted)::numeric                            AS quantity_converted,
        MAX(bf.conversion_factor)::numeric(20,12)                      AS conversion_factor,
        bf.marketing_year, bf.marketing_year_end
    FROM (SELECT * FROM biofuel_bd UNION ALL SELECT * FROM biofuel_rd) bf
    CROSS JOIN silver.trade_country_reference tcr
    -- The two world-total reference entries
    WHERE (tcr.country_name = 'WORLD TOTAL' OR (tcr.country_code = '-' AND tcr.country_name = 'TOTAL FOR ALL COUNTRIES'))
    GROUP BY bf.year, bf.month, bf.flow, bf.hs_code, bf.commodity_group,
             bf.commodity_name, bf.display_unit,
             tcr.country_name, tcr.region, tcr.region_sort_order, tcr.country_sort_order,
             tcr.spreadsheet_row, tcr.is_regional_total,
             bf.marketing_year, bf.marketing_year_end
)
SELECT * FROM non_biofuel
UNION ALL SELECT * FROM biofuel_bd
UNION ALL SELECT * FROM biofuel_rd
UNION ALL SELECT * FROM biofuel_world;

COMMENT ON VIEW gold.trade_export_mapped IS
'Core trade view feeding all downstream matrix views. For HS 3826 (biodiesel '
'mixtures), country rows are split into BIODIESEL and RENEWABLE_DIESEL using '
'reference.biofuel_trade_split + reference.biofuel_hs_blend_content. WORLD '
'TOTAL aggregates are computed by summing the per-country splits (more accurate '
'than applying the default rule to the Census aggregate). Non-3826 unchanged.';
