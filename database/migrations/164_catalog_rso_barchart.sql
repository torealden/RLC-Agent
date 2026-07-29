-- 164_catalog_rso_barchart.sql
--
-- Guidance Report price-feed layer: catalog RSO (European FOB Dutch Mill Rapeseed Oil, Argus) and the
-- Barchart-CSV route it arrives on. First contract of the CME cash-settled veg-oil family (brief v1.1
-- §B; inventory in clients/Contracts/Helios/CME_settled_veg_oil_futures_inventory.md), start-with set
-- ruled by Tore = RSO + UCO/UCOME.
--
-- ROUTE / PROVENANCE (verified 2026-07-29):
--   * CME's own settlement endpoint is ToS-BLOCKED (403, explicit anti-scraping notice) and Barchart's
--     data API is robots-DISALLOWED (/proxies/). The only compliant free path is the human-initiated
--     per-page CSV "Download" (personal-use ToS), which Tore/Desktop provide into the edition folder.
--     So this is NOT an automated collector -- it is a loader for those hand-exported CSVs.
--   * Two provenance facts, both recorded:
--       - RSO is a real CME/CBOT exchange cash-settlement -> quality SETTLE_OFFICIAL, source_tier
--         EXCHANGE_SETTLE. The value cash-settles on the ARGUS Rapeseed Oil FOB Dutch Mill assessment
--         (the bring-forward licensing target -- noted on the series).
--       - The ROUTE is Barchart under personal-use ToS -> license_status PERSONAL_USE, can_republish
--         FALSE. This route constraint is the binding one for redistribution today.
--   * can_republish stays FALSE until GP licenses the assessment AND a redistributable route exists.
--
-- NEW reference rows only.

BEGIN;

-- The route: Barchart CSV, CME-listed contracts. (Per-venue source name so a future Barchart-Bursa
-- route gets its own row with the correct exchange publisher -- the multi-publisher note from mig 163.)
INSERT INTO reference.price_source (source_name, source_tier, tier_ordinal, publisher, license_status, description) VALUES
    ('barchart_cme', 'EXCHANGE_SETTLE', 10,
     'CME Group (contracts cash-settled on Argus/Fastmarkets assessments), relayed via Barchart CSV export',
     'PERSONAL_USE',
     'Human-exported Barchart futures-prices CSV for CME-listed cash-settled contracts (RSO, UCO, UCOME, ...). '
     'Route is personal-use ToS -> can_republish FALSE. Quality is exchange-settle. The underlying assessor '
     '(Argus/Fastmarkets) and the pending-license status are on reference.price_series per contract.');

-- The series: RSO. Barchart root BDO = CME code RSO (one contract).
INSERT INTO reference.price_series
    (series_key, description, commodity, region, home_country, unit, currency, cadence, register_num, active, notes) VALUES
    ('RSO',
     'European FOB Dutch Mill Rapeseed Oil (Argus) futures -- CME code RSO / Barchart root BDO',
     'rapeseed_oil', 'NW Europe (FOB Dutch Mill)', NULL, 'EUR/t', 'EUR', 'daily', 24, true,
     'CME/CBOT cash-settled on the Argus Rapeseed Oil FOB Dutch Mill assessment (monthly avg, prompt 5-40 '
     'days fwd since ~Mar 2026). Route: Barchart CSV export (personal-use). Underlying assessor = Argus; '
     'republication PENDING GP license (bring-forward / ASSESSED_PENDING_LICENSE). Retires the register #24 '
     'ECO-derived interim; keep Euronext ECO as cross-check.');

COMMIT;
