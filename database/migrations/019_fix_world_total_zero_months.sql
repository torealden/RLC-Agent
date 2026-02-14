-- =============================================================================
-- Migration: 019_fix_world_total_zero_months.sql
-- Date: 2025-02-08
-- Description: Fix gold.trade_export_matrix to generate WORLD TOTAL rows with
--              zeros for months with no trade data within the commodity's date range
--
-- Issue: When Census API returned no records for a month (no trade activity),
--        the gold view had no WORLD TOTAL row, leaving blanks in spreadsheets.
--        VBA script expected 0 for months with no trade.
--
-- Solution: Generate a date spine for each commodity/flow and LEFT JOIN to
--           actual data, using COALESCE to fill zeros for missing months.
-- =============================================================================

DROP VIEW IF EXISTS gold.trade_export_matrix CASCADE;

CREATE VIEW gold.trade_export_matrix AS
WITH
-- Date spine: generate all months from 2013-01 to current
date_spine AS (
    SELECT
        y.year,
        m.month
    FROM generate_series(2013, EXTRACT(YEAR FROM CURRENT_DATE)::int) AS y(year)
    CROSS JOIN generate_series(1, 12) AS m(month)
    WHERE y.year * 12 + m.month <= EXTRACT(YEAR FROM CURRENT_DATE)::int * 12 + EXTRACT(MONTH FROM CURRENT_DATE)::int
),
-- Get all commodity/flow combinations with their date ranges (including WORLD TOTAL data)
commodity_flow_ranges AS (
    SELECT
        commodity_group,
        flow,
        MIN(year * 12 + month) as min_ym,
        MAX(year * 12 + month) as max_ym
    FROM gold.trade_monthly_by_country
    GROUP BY commodity_group, flow

    UNION

    -- Also include ranges from WORLD TOTAL only commodities (from trade_export_mapped)
    SELECT
        commodity_group,
        flow,
        MIN(year * 12 + month) as min_ym,
        MAX(year * 12 + month) as max_ym
    FROM gold.trade_export_mapped
    WHERE standard_country_name = 'WORLD TOTAL'
    GROUP BY commodity_group, flow
),
-- Consolidated date ranges (take the widest range for each commodity/flow)
consolidated_ranges AS (
    SELECT
        commodity_group,
        flow,
        MIN(min_ym) as min_ym,
        MAX(max_ym) as max_ym
    FROM commodity_flow_ranges
    GROUP BY commodity_group, flow
),
-- Generate all months for each commodity/flow within its date range
all_commodity_months AS (
    SELECT
        cr.commodity_group,
        cr.flow,
        ds.year,
        ds.month
    FROM consolidated_ranges cr
    CROSS JOIN date_spine ds
    WHERE ds.year * 12 + ds.month >= cr.min_ym
      AND ds.year * 12 + ds.month <= cr.max_ym
),
-- Get display units for each commodity/flow
commodity_metadata AS (
    SELECT DISTINCT ON (commodity_group, flow)
        commodity_group,
        flow,
        display_unit
    FROM gold.trade_export_mapped
    WHERE display_unit IS NOT NULL
),
-- Individual country data (non-WORLD TOTAL, non-regional)
country_data AS (
    SELECT
        commodity_group,
        flow,
        country_name,
        region,
        region_sort_order,
        country_sort_order,
        spreadsheet_row,
        FALSE as is_regional_total,
        year,
        month,
        marketing_year,
        marketing_year_end,
        display_unit,
        quantity,
        value_usd
    FROM gold.trade_monthly_by_country
    WHERE is_regional_total = FALSE
      AND country_name != 'WORLD TOTAL'
),
-- Regional totals from existing view
regional_totals AS (
    SELECT
        rm.commodity_group,
        rm.flow,
        rr.region_name AS country_name,
        rm.region,
        rm.region_sort_order,
        0 AS country_sort_order,
        (SELECT spreadsheet_row FROM silver.trade_country_reference
         WHERE region = rm.region AND is_regional_total = TRUE LIMIT 1) AS spreadsheet_row,
        TRUE as is_regional_total,
        rm.year,
        rm.month,
        rm.marketing_year,
        rm.marketing_year_end,
        rm.display_unit,
        rm.quantity,
        rm.value_usd
    FROM gold.trade_regional_monthly rm
    JOIN silver.trade_region_reference rr ON rm.region = rr.region_code
),
-- Actual WORLD TOTAL data (from trade_monthly_by_country OR from trade_export_mapped for WORLD TOTAL-only commodities)
actual_world_totals AS (
    SELECT
        commodity_group,
        flow,
        year,
        month,
        quantity,
        value_usd,
        display_unit,
        marketing_year,
        marketing_year_end
    FROM gold.trade_monthly_by_country
    WHERE country_name = 'WORLD TOTAL'

    UNION ALL

    -- Include WORLD TOTAL from trade_export_mapped for commodities that only have WORLD TOTAL data
    SELECT
        commodity_group,
        flow,
        year,
        month,
        SUM(quantity_converted) as quantity,
        SUM(value_usd) as value_usd,
        display_unit,
        marketing_year,
        marketing_year_end
    FROM gold.trade_export_mapped
    WHERE standard_country_name = 'WORLD TOTAL'
      AND (commodity_group, flow) NOT IN (
          SELECT DISTINCT commodity_group, flow
          FROM gold.trade_monthly_by_country
          WHERE country_name != 'WORLD TOTAL'
      )
    GROUP BY commodity_group, flow, year, month, display_unit, marketing_year, marketing_year_end
),
-- Complete WORLD TOTAL with zeros for missing months
world_total_complete AS (
    SELECT
        acm.commodity_group,
        acm.flow,
        'WORLD TOTAL'::varchar(100) AS country_name,
        'WORLD'::varchar(50) AS region,
        99 AS region_sort_order,
        0 AS country_sort_order,
        217 AS spreadsheet_row,
        TRUE AS is_regional_total,
        acm.year,
        acm.month,
        CASE
            WHEN acm.month >= 9 THEN acm.year::text || '/' || RIGHT((acm.year + 1)::text, 2)
            ELSE (acm.year - 1)::text || '/' || RIGHT(acm.year::text, 2)
        END AS marketing_year,
        CASE
            WHEN acm.month >= 9 THEN acm.year + 1
            ELSE acm.year
        END AS marketing_year_end,
        COALESCE(cm.display_unit, 'UNKNOWN'::varchar(30)) AS display_unit,
        COALESCE(awt.quantity, 0::numeric) AS quantity,
        COALESCE(awt.value_usd, 0::numeric) AS value_usd
    FROM all_commodity_months acm
    LEFT JOIN actual_world_totals awt
        ON acm.commodity_group = awt.commodity_group
        AND acm.flow = awt.flow
        AND acm.year = awt.year
        AND acm.month = awt.month
    LEFT JOIN commodity_metadata cm
        ON acm.commodity_group = cm.commodity_group
        AND acm.flow = cm.flow
)
-- Union all together
SELECT * FROM country_data
UNION ALL
SELECT * FROM regional_totals
UNION ALL
SELECT * FROM world_total_complete
ORDER BY region_sort_order, country_sort_order, year, month;

-- Verify the fix
-- SELECT commodity_group, flow, COUNT(*) as months,
--        SUM(CASE WHEN quantity = 0 THEN 1 ELSE 0 END) as zero_months
-- FROM gold.trade_export_matrix
-- WHERE country_name = 'WORLD TOTAL'
-- GROUP BY commodity_group, flow
-- ORDER BY commodity_group, flow;
