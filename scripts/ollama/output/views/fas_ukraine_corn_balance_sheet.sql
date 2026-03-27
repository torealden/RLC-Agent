DROP VIEW IF EXISTS gold.fas_ukraine_corn_balance_sheet;
CREATE VIEW gold.fas_ukraine_corn_balance_sheet AS
SELECT
    marketing_year,
    report_date,
    'USDA PSD' AS source,
    area_harvested,
    yield,
    beginning_stocks,
    production,
    imports,
    total_supply,
    feed_dom_consumption,
    fsi_consumption,
    domestic_consumption,
    exports,
    total_distribution,
    ending_stocks,
    CASE WHEN total_distribution > 0
         THEN ROUND((ending_stocks / total_distribution * 100)::numeric, 1)
         ELSE NULL
    END AS stocks_use_pct,
    unit
FROM bronze.fas_psd
WHERE commodity = 'corn'
  AND country_code = 'UP'
ORDER BY marketing_year DESC, report_date DESC;