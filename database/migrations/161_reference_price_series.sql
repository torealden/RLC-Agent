-- 161_reference_price_series.sql
--
-- Guidance Report price-feed layer: the series catalog (review item (d), Tore 2026-07-28).
--
-- WHAT / WHY: silver.price_mark.series_key ('DCO_ECB', 'FX_USDMYR', 'ZC') has, until now, no metadata
-- and nothing FKs its unit/currency -- so a collector typo ('cent/lb' vs 'cents/lb', or 'USD/bbl' vs
-- 'bbl/USD') would silently FORK a series into two and no query would notice. This table:
--   1. names every series (description, commodity, region, cadence, register #) so the dashboard's
--      29-row register and any consumer can resolve a bare series_key;
--   2. records `active` (a live collector feeds it) vs historical/backfill-only, so a consumer knows
--      which keys still update;
--   3. records `home_country` -- the series' home authority, the tag amendment 2 of migration 160
--      flagged as needed to ever resolve "gov of the series' own country vs another" (NULL where a
--      series has no single home: FX pairs, global crude benchmarks);
--   4. becomes the canonical unit/currency, ENFORCED on silver.price_mark by a guard trigger below.
--
-- Verified 2026-07-28: every series_key currently in price_mark has exactly ONE (unit, currency) -- no
-- existing rows contradict the catalog, so turning on the guard cannot retroactively break a collector.
--
-- Same table-not-ENUM discipline as reference.price_quality_rank / reference.price_source: adding or
-- re-describing a series is an INSERT/UPDATE.
--
-- NEW objects only; nothing to backfill.

BEGIN;

CREATE TABLE reference.price_series (
    series_key    text     PRIMARY KEY,               -- matches silver.price_mark.series_key
    description   text     NOT NULL,
    commodity     text     NOT NULL,
    region        text,                               -- NULL for FX / global benchmarks
    home_country  text,                               -- ISO-2 home authority for gov-of-series resolution; NULL if none
    unit          text     NOT NULL,                  -- canonical unit; enforced on price_mark by trigger below
    currency      text     NOT NULL,                  -- ISO 4217
    cadence       text     NOT NULL,
    register_num  integer,                            -- Price_Series_Register.md # (1-29); NULL for ride-alongs/extras
    active        boolean  NOT NULL DEFAULT true,     -- FALSE = historical/backfill-only, no live collector into price_mark
    notes         text,
    CONSTRAINT price_series_cadence_ck CHECK (cadence IN ('daily','weekly','monthly')),
    CONSTRAINT price_series_register_ck CHECK (register_num IS NULL OR register_num BETWEEN 1 AND 29)
);

INSERT INTO reference.price_series
    (series_key, description, commodity, region, home_country, unit, currency, cadence, register_num, active, notes) VALUES
    -- Register #1-4 + free ride-alongs: CBOT/KCBT/MGEX grain settles (ams_grain_settlement, daily).
    ('ZC','CBOT Corn futures settlement','corn','US','US','cents/bu','USD','daily',1,true,'AMS settle strip + yfinance NEARBY history.'),
    ('ZW','CBOT SRW Wheat futures settlement','wheat_srw','US','US','cents/bu','USD','daily',2,true,'AMS settle strip + yfinance NEARBY history.'),
    ('KE','KC HRW Wheat futures settlement','wheat_hrw','US','US','cents/bu','USD','daily',3,true,'AMS settle strip + yfinance NEARBY history.'),
    ('MWE','MGEX HRS Wheat futures settlement','wheat_hrs','US','US','cents/bu','USD','daily',4,true,'AMS settle strip.'),
    ('ZS','CBOT Soybeans futures settlement','soybeans','US','US','cents/bu','USD','daily',NULL,true,'Rides free on the AMS grain block (not a register series).'),
    ('ZO','CBOT Oats futures settlement','oats','US','US','cents/bu','USD','daily',NULL,true,'Rides free on the AMS grain block (not a register series).'),
    -- Register #5 ZL: only yfinance history so far; official CME strip (#5) not yet built -> inactive.
    ('ZL','CBOT Soybean Oil futures','soybean_oil','US','US','cents/lb','USD','daily',5,false,'yfinance history only; official CME strip (#5) pending -> flip active on build.'),
    -- Register #6 FCPO: ibkr backfill only; official Bursa EOD (#6) not yet built -> inactive.
    ('FCPO','Bursa Malaysia Crude Palm Oil futures','palm_oil','Malaysia','MY','MYR/t','MYR','daily',6,false,'ibkr backfill (2024->) only; official Bursa EOD (#6) pending.'),
    -- Register #11 FX: fred_h10 (primary) + ecb_ref* (holiday-gap fallback), daily.
    ('FX_EURUSD','USD per EUR spot (FRED H.10 / ECB)','fx',NULL,NULL,'USD/EUR','USD','daily',11,true,'fred_h10 primary; ecb fills US holidays. Two-country pair -> no home_country.'),
    ('FX_USDMYR','Malaysian ringgit per USD spot','fx',NULL,NULL,'MYR/USD','MYR','daily',11,true,'fred_h10 primary; ecb fallback.'),
    ('FX_USDCNY','Chinese yuan per USD spot','fx',NULL,NULL,'CNY/USD','CNY','daily',11,true,'fred_h10 primary; ecb fallback.'),
    ('FX_USDMXN','Mexican peso per USD spot','fx',NULL,NULL,'MXN/USD','MXN','daily',11,true,'fred_h10 primary; ecb fallback.'),
    ('FX_USDCAD','Canadian dollar per USD spot','fx',NULL,NULL,'CAD/USD','CAD','daily',11,true,'fred_h10 primary; ecb fallback.'),
    ('FX_USDBRL','Brazilian real per USD spot','fx',NULL,NULL,'BRL/USD','BRL','daily',11,true,'fred_h10 primary; ecb fallback. ARS still a gap (not in H.10).'),
    -- Register #12 crude: EIA official daily spot.
    ('WTI','WTI Cushing crude spot (EIA)','crude_oil','US Cushing','US','USD/bbl','USD','daily',12,true,'eia_crude_price_bridge.'),
    ('BRENT','Brent crude spot (EIA)','crude_oil','North Sea',NULL,'USD/bbl','USD','daily',12,true,'eia_crude_price_bridge. Global benchmark -> no single home_country.'),
    -- Register #13 DCO: USDA AMS weekly regional cash survey (ams_dco_prices).
    ('DCO_IA','Distillers Corn Oil FOB plant, Iowa','distillers_corn_oil','Iowa','US','cents/lb','USD','weekly',13,true,'ams_3618.'),
    ('DCO_KS','Distillers Corn Oil FOB plant, Kansas','distillers_corn_oil','Kansas','US','cents/lb','USD','weekly',13,true,'ams_3618.'),
    ('DCO_WI','Distillers Corn Oil FOB plant, Wisconsin','distillers_corn_oil','Wisconsin','US','cents/lb','USD','weekly',13,true,'ams_3618.'),
    ('DCO_MO','Distillers Corn Oil FOB plant, Missouri','distillers_corn_oil','Missouri','US','cents/lb','USD','weekly',13,true,'ams_3618.'),
    ('DCO_NE','Distillers Corn Oil FOB plant, Nebraska','distillers_corn_oil','Nebraska','US','cents/lb','USD','weekly',13,true,'ams_3618.'),
    ('DCO_SD','Distillers Corn Oil FOB plant, South Dakota','distillers_corn_oil','South Dakota','US','cents/lb','USD','weekly',13,true,'ams_3618.'),
    ('DCO_MN','Distillers Corn Oil FOB plant, Minnesota','distillers_corn_oil','Minnesota','US','cents/lb','USD','weekly',13,true,'ams_3618.'),
    ('DCO_ECB','Distillers Corn Oil FOB plant, Eastern Cornbelt','distillers_corn_oil','Eastern Cornbelt','US','cents/lb','USD','weekly',13,true,'ams_3618.'),
    -- Off-register energy/ag extras that rode in on the yfinance/ibkr backfill (mig 159). Historical-only.
    ('CL','NYMEX WTI crude futures (delayed)','crude_oil','US Cushing','US','USD/bbl','USD','daily',NULL,false,'yfinance backfill; not an official series. See WTI (#12) for the live spot.'),
    ('HO','NYMEX ULSD/heating oil futures (delayed)','heating_oil','US','US','USD/gal','USD','daily',NULL,false,'yfinance backfill only.'),
    ('RB','NYMEX RBOB gasoline futures (delayed)','gasoline','US','US','USD/gal','USD','daily',NULL,false,'yfinance backfill only.'),
    ('NG','NYMEX Henry Hub natural gas futures (delayed)','natural_gas','US','US','USD/MMBtu','USD','daily',NULL,false,'yfinance backfill only.'),
    ('ZM','CBOT Soybean Meal futures (delayed)','soybean_meal','US','US','USD/short ton','USD','daily',NULL,false,'yfinance backfill only.'),
    ('ZR','CBOT Rough Rice futures (delayed)','rice','US','US','USD/cwt','USD','daily',NULL,false,'yfinance backfill only.'),
    ('DC','CME Class III Milk futures (delayed)','milk_class_iii','US','US','USD/cwt','USD','daily',NULL,false,'yfinance backfill only.');

COMMENT ON TABLE reference.price_series IS
'Series catalog for the Helios price layer: one row per silver.price_mark.series_key with description, '
'commodity, region, home_country (gov-of-series resolution, mig 160 amendment 2), canonical unit/currency '
'(enforced on price_mark by trigger price_series_unit_guard), cadence, register #, and active flag '
'(FALSE = backfill/historical-only). Extend by INSERT. See migration 161.';
COMMENT ON COLUMN reference.price_series.active IS
'TRUE when a registered collector feeds this series into price_mark; FALSE for backfill/historical-only '
'keys (e.g. yfinance/ibkr extras frozen at their last pull). Flip to TRUE when an official collector lands.';
COMMENT ON COLUMN reference.price_series.home_country IS
'ISO-2 home authority of the series, for resolving gov-of-series-country precedence (mig 160 amendment 2). '
'NULL where there is no single home (FX pairs, global crude benchmarks).';

-- ---------------------------------------------------------------------------------------------------
-- Unit/currency guard: a cataloged series must be written to price_mark with its catalog unit+currency.
-- Uncataloged series_keys pass freely (enforcement is opt-in via cataloging). Fires per row, but the
-- lookup is a PK hit on a ~30-row, memory-resident table -> negligible even on a 60k-row FRED batch.
-- ---------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION reference.price_series_unit_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    c_unit text;
    c_curr text;
BEGIN
    SELECT unit, currency INTO c_unit, c_curr
    FROM reference.price_series WHERE series_key = NEW.series_key;

    IF NOT FOUND THEN
        RETURN NEW;  -- uncataloged series: no enforcement yet
    END IF;

    IF NEW.unit <> c_unit OR NEW.currency <> c_curr THEN
        RAISE EXCEPTION
            'price_series unit guard: % writes unit=%/curr=% but catalog says unit=%/curr=% (typo forks the series)',
            NEW.series_key, NEW.unit, NEW.currency, c_unit, c_curr
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER price_series_unit_guard
    BEFORE INSERT OR UPDATE ON silver.price_mark
    FOR EACH ROW EXECUTE FUNCTION reference.price_series_unit_guard();

COMMENT ON FUNCTION reference.price_series_unit_guard() IS
'Rejects a silver.price_mark write whose (unit, currency) disagrees with reference.price_series for a '
'cataloged series_key -- stops a unit typo silently forking a series. Uncataloged keys pass. See mig 161.';

-- ---------------------------------------------------------------------------------------------------
-- Status view: catalog joined to the latest best mark, for the dashboard register + freshness.
-- ---------------------------------------------------------------------------------------------------
CREATE VIEW gold.price_series_status AS
SELECT
    s.series_key, s.description, s.commodity, s.region, s.home_country,
    s.unit, s.currency, s.cadence, s.register_num, s.active,
    b.obs_date      AS last_obs_date,
    b.value         AS last_value,
    b.source        AS last_source,
    b.quality_rank  AS last_quality_rank,
    (CURRENT_DATE - b.obs_date) AS days_stale
FROM reference.price_series s
LEFT JOIN LATERAL (
    SELECT obs_date, value, source, quality_rank
    FROM gold.price_mark_best m
    WHERE m.series_key = s.series_key
    ORDER BY m.obs_date DESC
    LIMIT 1
) b ON true;

COMMENT ON VIEW gold.price_series_status IS
'reference.price_series joined to its latest gold.price_mark_best mark: one row per catalogued series '
'with last obs_date/value/source and days_stale. Drives the dashboard price register + freshness. Mig 161.';

COMMIT;
