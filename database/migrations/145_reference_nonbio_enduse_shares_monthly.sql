-- 145_reference_nonbio_enduse_shares_monthly.sql
-- Seasonal (calendar-month) non-biofuel end-use shares — the month-varying companion to the flat
-- annual reference.nonbio_enduse_shares (migration 144). Each (commodity, month) set sums to 1.0,
-- so splitting a monthly non_biofuel_use total by these still closes exactly, but the components
-- BREATHE by calendar month instead of sitting at a flat percentage (which reads as synthetic).
--
-- SEEDED FROM SOURCE DATA, not INSERT statements: run
--   python scripts/build_nonbio_seasonal_shares.py
-- which derives the month-of-year pattern from the 2006-2011 Census monthlies (soybean oil from
-- us_oilseed_crush.xlsm "Census Crush" tab; tallow from bronze.census_cir_fats) and applies it as
-- a seasonal index around the RULED annual level in nonbio_enduse_shares (so the annual level is
-- preserved; only the shape is added). Analogs: canola<-SBO edible index, PF/CWG/YG<-tallow index,
-- dco/uco_yg/cottonseed flat across months.
--
-- This migration is the schema of record; the build script's CREATE IF NOT EXISTS matches it.

CREATE TABLE IF NOT EXISTS reference.nonbio_enduse_shares_monthly (
    commodity  varchar(32) NOT NULL,
    month      int         NOT NULL CHECK (month BETWEEN 1 AND 12),
    end_use    varchar(48) NOT NULL,
    share_pct  numeric      NOT NULL,   -- fraction of the non-biofuel total; sums to 1.0 per (commodity, month)
    measured   boolean      NOT NULL DEFAULT false,
    basis      text,
    PRIMARY KEY (commodity, month, end_use)
);

COMMENT ON TABLE reference.nonbio_enduse_shares_monthly IS
'Seasonal calendar-month non-biofuel end-use shares. Seeded by '
'scripts/build_nonbio_seasonal_shares.py from 2006-2011 Census monthlies (soybean oil = '
'us_oilseed_crush.xlsm Census Crush tab; tallow = bronze.census_cir_fats), applied as a seasonal '
'index around the ruled annual level in reference.nonbio_enduse_shares. Consumed by the oils/fats '
'flat-file writers (nonbio_components); each (commodity, month) sums to 1.0 so sheets still close.';
