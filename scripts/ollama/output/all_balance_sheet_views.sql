DROP VIEW IF EXISTS gold.fas_argentina_corn_balance_sheet;
CREATE VIEW gold.fas_argentina_corn_balance_sheet AS
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
  AND country_code = 'AR'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_argentina_soybeans_balance_sheet;
CREATE VIEW gold.fas_argentina_soybeans_balance_sheet AS
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
    unit
FROM bronze.fas_psd
WHERE commodity = 'soybeans'
  AND country_code = 'AR'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_argentina_wheat_balance_sheet;
CREATE VIEW gold.fas_argentina_wheat_balance_sheet AS
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
WHERE commodity = 'wheat'
  AND country_code = 'AR'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_australia_wheat_balance_sheet;
CREATE VIEW gold.fas_australia_wheat_balance_sheet AS
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
WHERE commodity = 'wheat'
  AND country_code = 'AS'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_brazil_corn_balance_sheet;
CREATE VIEW gold.fas_brazil_corn_balance_sheet AS
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
  AND country_code = 'BR'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_brazil_cotton_balance_sheet;
CREATE VIEW gold.fas_brazil_cotton_balance_sheet AS
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
    domestic_consumption,
    fsi_consumption,
    exports,
    total_distribution,
    ending_stocks,
    CASE WHEN total_distribution > 0
         THEN ROUND((ending_stocks / total_distribution * 100)::numeric, 1)
         ELSE NULL
    END AS stocks_use_pct,
    unit
FROM bronze.fas_psd
WHERE commodity = 'cotton'
  AND country_code = 'BR'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_brazil_soybeans_balance_sheet;
CREATE VIEW gold.fas_brazil_soybeans_balance_sheet AS
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
    unit
FROM bronze.fas_psd
WHERE commodity = 'soybeans'
  AND country_code = 'BR'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_canada_rapeseed_balance_sheet;
CREATE VIEW gold.fas_canada_rapeseed_balance_sheet AS
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
    unit
FROM bronze.fas_psd
WHERE commodity = 'rapeseed'
  AND country_code = 'CA'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_canada_wheat_balance_sheet;
CREATE VIEW gold.fas_canada_wheat_balance_sheet AS
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
WHERE commodity = 'wheat'
  AND country_code = 'CA'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_china_soybeans_balance_sheet;
CREATE VIEW gold.fas_china_soybeans_balance_sheet AS
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
    unit
FROM bronze.fas_psd
WHERE commodity = 'soybeans'
  AND country_code = 'CH'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_eu_corn_balance_sheet;
CREATE VIEW gold.fas_eu_corn_balance_sheet AS
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
  AND country_code = 'E4'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_eu_rapeseed_balance_sheet;
CREATE VIEW gold.fas_eu_rapeseed_balance_sheet AS
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
    unit
FROM bronze.fas_psd
WHERE commodity = 'rapeseed'
  AND country_code = 'E4'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_eu_wheat_balance_sheet;
CREATE VIEW gold.fas_eu_wheat_balance_sheet AS
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
WHERE commodity = 'wheat'
  AND country_code = 'E4'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_india_cotton_balance_sheet;
CREATE VIEW gold.fas_india_cotton_balance_sheet AS
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
    domestic_consumption,
    fsi_consumption,
    exports,
    total_distribution,
    ending_stocks,
    CASE WHEN total_distribution > 0
         THEN ROUND((ending_stocks / total_distribution * 100)::numeric, 1)
         ELSE NULL
    END AS stocks_use_pct,
    unit
FROM bronze.fas_psd
WHERE commodity = 'cotton'
  AND country_code = 'IN'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_indonesia_palm_oil_balance_sheet;
CREATE VIEW gold.fas_indonesia_palm_oil_balance_sheet AS
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
  AND country_code = 'ID'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_malaysia_palm_oil_balance_sheet;
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
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_paraguay_soybeans_balance_sheet;
CREATE VIEW gold.fas_paraguay_soybeans_balance_sheet AS
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
    unit
FROM bronze.fas_psd
WHERE commodity = 'soybeans'
  AND country_code = 'PA'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_russia_wheat_balance_sheet;
CREATE VIEW gold.fas_russia_wheat_balance_sheet AS
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
WHERE commodity = 'wheat'
  AND country_code = 'RS'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_ukraine_corn_balance_sheet;
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
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_ukraine_wheat_balance_sheet;
CREATE VIEW gold.fas_ukraine_wheat_balance_sheet AS
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
WHERE commodity = 'wheat'
  AND country_code = 'UP'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_us_barley_balance_sheet;
CREATE VIEW gold.fas_us_barley_balance_sheet AS
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
WHERE commodity = 'barley'
  AND country_code = 'US'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_us_canola_balance_sheet;
CREATE VIEW gold.fas_us_canola_balance_sheet AS
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
    unit
FROM bronze.fas_psd
WHERE commodity = 'rapeseed'
  AND country_code = 'US'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_us_cotton_balance_sheet;
CREATE VIEW gold.fas_us_cotton_balance_sheet AS
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
    domestic_consumption,
    fsi_consumption,
    exports,
    total_distribution,
    ending_stocks,
    CASE WHEN total_distribution > 0
         THEN ROUND((ending_stocks / total_distribution * 100)::numeric, 1)
         ELSE NULL
    END AS stocks_use_pct,
    unit
FROM bronze.fas_psd
WHERE commodity = 'cotton'
  AND country_code = 'US'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_us_palm_oil_balance_sheet;
CREATE VIEW gold.fas_us_palm_oil_balance_sheet AS
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
  AND country_code = 'US'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_us_peanut_balance_sheet;
CREATE VIEW gold.fas_us_peanut_balance_sheet AS
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
    unit
FROM bronze.fas_psd
WHERE commodity = 'peanuts'
  AND country_code = 'US'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_us_rice_balance_sheet;
CREATE VIEW gold.fas_us_rice_balance_sheet AS
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
WHERE commodity = 'rice_milled'
  AND country_code = 'US'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_us_sorghum_balance_sheet;
CREATE VIEW gold.fas_us_sorghum_balance_sheet AS
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
WHERE commodity = 'sorghum'
  AND country_code = 'US'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_us_sunflower_balance_sheet;
CREATE VIEW gold.fas_us_sunflower_balance_sheet AS
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
    unit
FROM bronze.fas_psd
WHERE commodity = 'sunflower_seed'
  AND country_code = 'US'
ORDER BY marketing_year DESC, report_date DESC;DROP VIEW IF EXISTS gold.fas_world_soybeans_balance_sheet;
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