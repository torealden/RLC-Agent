DROP VIEW IF EXISTS gold.fas_malaysia_palm_oil_balance_sheet;
CREATE VIEW gold.fas_malaysia_palm_oil_balance_sheet AS
SELECT
    marketing_year,
    report_date,
    'USDA PSD' AS source,
    beginning_stocks,
    production,
    imports,
    total_supply,
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
WHERE commodity = 'palm_oil'
  AND country_code = 'MY'
ORDER BY marketing_year DESC, report_date DESC;