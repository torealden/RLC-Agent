DROP VIEW IF EXISTS gold.fas_world_soybeans_balance_sheet;
CREATE VIEW gold.fas_world_soybeans_balance_sheet AS
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
    crush,
    fsi_consumption,
    domestic_consumption,
    exports,
    total_distribution,
    ending_stocks,
    CASE WHEN total_distribution > 0
         THEN ROUND((ending_stocks / total_distribution * 100)::numeric, 1)
         ELSE NULL
    END AS stocks_use_pct,
    unit,
    country_code,
    country
FROM bronze.fas_psd
WHERE commodity = 'soybeans'
ORDER BY marketing_year DESC, country, report_date DESC;