-- 169: expose psd_cycle (and vintage_source) on gold.fas_us_wasde_comp.
--
-- WHY: mig 168 redefined gold.psd_wasde_vintages as the PSD + WASDE-archive
-- union. Within an active marketing year, cycles past the 19th all cap at
-- vintage_rank 79, so "ORDER BY vintage_rank DESC" alone no longer orders
-- vintages once a MY carries 20+ cycles (US corn MY2012 has SEVENTEEN cycles
-- tied at 79; active MYs start tying in early 2027). build_usda_comp_tabs.py
-- already breaks ties with psd_cycle DESC; WASDECompUpdater.bas reads THIS
-- view, which did not expose psd_cycle -- so the VBA had no column to break
-- ties on. Columns are APPENDED (CREATE OR REPLACE-safe).
--
-- vintage_source ('PSD' | 'WASDE_ARCHIVE') is appended too: API-precision vs
-- published-2dp rows differ in the last digit and should not be read as
-- revisions across the boundary (see mig 168 header).

BEGIN;

CREATE OR REPLACE VIEW gold.fas_us_wasde_comp AS
SELECT commodity,
       marketing_year,
       report_date,
       vintage,
       vintage_rank,
       area_planted,
       area_harvested,
       yield,
       beginning_stocks,
       production,
       imports,
       total_supply,
       feed_dom_consumption,
       fsi_consumption,
       crush,
       domestic_consumption,
       exports,
       total_distribution,
       ending_stocks,
       unit,
       psd_cycle,
       vintage_source
FROM gold.psd_wasde_vintages
WHERE country_code::text = 'US'::text
ORDER BY commodity, marketing_year DESC, vintage_rank DESC, psd_cycle DESC;

COMMIT;
