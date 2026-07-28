-- 163_price_source_provenance.sql
--
-- Guidance Report price-feed layer: make provenance a first-class, queryable fact (Tore ruling
-- 2026-07-28). Principle: "as we grow, someone will ask where we get our prices, and I never want to
-- hesitate on that answer. Free legal paths are publishable; mark everything else unpublishable, but
-- RECORD the background data and NOTE THE PUBLISHER, so we can bring it forward through a real
-- relationship later."
--
-- Two columns on reference.price_source turn that rule into a SELECT:
--   * publisher       -- the underlying data owner/authority we attribute or would license from. The
--                        relationship target, NOT the technical route. (For a Barchart-relayed board the
--                        publisher is the EXCHANGE, e.g. Bursa Malaysia, not Barchart -- see the note on
--                        the Barchart collector below; those get per-board source_names when built.)
--   * license_status  -- the redistribution basis for this route, i.e. WHY can_republish is set the way
--                        it is. Vocabulary (tie-out to can_republish is a per-collector decision, NOT a
--                        hard constraint, because our own DERIVED_* stacks republish TRUE even when their
--                        inputs are restricted -- our math, our terms):
--        PUBLIC_DOMAIN            US-government work; freely republishable            -> can_republish TRUE
--        FREE_ATTRIBUTION         free to use/republish with attribution/terms (ECB) -> TRUE
--        PERSONAL_USE             route ToS = personal/internal only (Barchart)      -> FALSE
--        ASSESSED_PENDING_LICENSE underlying is a paid assessment (Argus/Fastmarkets/LSEG) reached via a
--                                 free legal window (an exchange settle onto it); usable internally, NOT
--                                 republishable until licensed. THE "bring-forward" bucket.            FALSE
--        DELAYED_INDICATIVE       delayed/derived retail feed (yfinance/ibkr)        -> FALSE
--
-- Additive; backfills the 10 existing sources. New sources INSERT with both fields set from the start.

BEGIN;

ALTER TABLE reference.price_source ADD COLUMN publisher      text;
ALTER TABLE reference.price_source ADD COLUMN license_status text;

UPDATE reference.price_source SET publisher = v.pub, license_status = v.lic
FROM (VALUES
    ('usda_ams_settle_3192','USDA AMS (relaying CBOT/KCBT/MGEX official settlements)','PUBLIC_DOMAIN'),
    ('usda_ams_settle_2850','USDA AMS (relaying CBOT/KCBT/MGEX official settlements)','PUBLIC_DOMAIN'),
    ('usda_ams_settle_2771','USDA AMS (relaying CBOT/KCBT/MGEX official settlements)','PUBLIC_DOMAIN'),
    ('eia_spot',            'US Energy Information Administration','PUBLIC_DOMAIN'),
    ('usda_ams_3618',       'USDA Agricultural Marketing Service','PUBLIC_DOMAIN'),
    ('fred_h10',            'US Federal Reserve Board (H.10 release, via FRED / St. Louis Fed)','PUBLIC_DOMAIN'),
    ('ecb_ref',            'European Central Bank (euro reference rates)','FREE_ATTRIBUTION'),
    ('ecb_ref_xrate',      'European Central Bank (euro reference rates)','FREE_ATTRIBUTION'),
    ('yfinance',           'Yahoo Finance (delayed market data)','DELAYED_INDICATIVE'),
    ('ibkr_tws',           'Interactive Brokers (TWS delayed feed)','DELAYED_INDICATIVE')
) AS v(source_name, pub, lic)
WHERE reference.price_source.source_name = v.source_name;

-- Now enforce presence + vocabulary (after backfill, so the NOT NULL/CHECK can't fail on existing rows).
ALTER TABLE reference.price_source ALTER COLUMN publisher      SET NOT NULL;
ALTER TABLE reference.price_source ALTER COLUMN license_status SET NOT NULL;
ALTER TABLE reference.price_source ADD CONSTRAINT price_source_license_ck CHECK (license_status IN
    ('PUBLIC_DOMAIN','FREE_ATTRIBUTION','PERSONAL_USE','ASSESSED_PENDING_LICENSE','DELAYED_INDICATIVE'));

COMMENT ON COLUMN reference.price_source.publisher IS
'Underlying data owner/authority we attribute or would license from -- the relationship target, not the '
'technical route. For a Barchart-relayed board this is the exchange (Bursa, ZCE, ...), captured via '
'per-board source_names when the Barchart collector lands. See migration 163.';
COMMENT ON COLUMN reference.price_source.license_status IS
'Redistribution basis for this route (why can_republish is set as it is). ASSESSED_PENDING_LICENSE marks '
'the bring-forward bucket: a paid assessment reached via a free legal window, usable internally but not '
'republishable until licensed. Tie to can_republish is a per-collector decision, not a hard constraint '
'(our own DERIVED_* stacks republish TRUE regardless of input status). See migration 163.';

COMMIT;
