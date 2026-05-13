-- =============================================================================
-- Migration 086: Recalibrate Canada BD/RD trade split rules
-- =============================================================================
-- Initial Canada rules in mig 082 shifted Canada to 90% RD from 2018, on the
-- assumption that Tidewater/Braya RD plants dominated. Canada export volume
-- data shows the opposite: Canadian BD exports to US stayed steady at
-- 250-400 kMT through 2013-2024 — RD growth was layered on top, BD producers
-- (Cargill, Greenfield, Western, Eastern Canada operations) kept exporting.
--
-- Plant timeline:
--   Pre-2020: Canadian BD producers (Cargill Clavet, Greenfield Varennes, etc.)
--             with no significant RD output
--   2020-2021: Tidewater Prince George BC (~3 kbbl/day RD) ramps
--   2022:     Braya Come By Chance NL (~15 kbbl/day RD) starts up
--   2023+:    Both RD plants at full scale
--
-- Revised rules approximate the gradual transition.
-- =============================================================================

DELETE FROM reference.biofuel_trade_split
WHERE origin = '1220' AND flow = 'imports';

INSERT INTO reference.biofuel_trade_split (hs_code, flow, origin, year_from, year_to, bd_share, rd_share, confidence, source, notes) VALUES
  -- 2013-2017: pre-RD era, Canadian exports nearly all BD
  ('3826001000','imports','1220',2013,2017, 0.97, 0.03, 'high',   'mig 086 recalibration', 'Pre-Tidewater era; Canadian BD producers dominant'),
  ('3826003000','imports','1220',2013,2017, 0.97, 0.03, 'high',   'mig 086 recalibration', NULL),
  -- 2018-2020: BD still dominant; minor RD ramping
  ('3826001000','imports','1220',2018,2020, 0.85, 0.15, 'medium', 'mig 086 recalibration', 'Tidewater commissioning; BD still majority'),
  ('3826003000','imports','1220',2018,2020, 0.85, 0.15, 'medium', 'mig 086 recalibration', NULL),
  -- 2021-2022: Tidewater operating, Braya not yet — roughly half-half
  ('3826001000','imports','1220',2021,2022, 0.55, 0.45, 'medium', 'mig 086 recalibration', 'Tidewater scaled; Braya pre-startup'),
  ('3826003000','imports','1220',2021,2022, 0.55, 0.45, 'medium', 'mig 086 recalibration', NULL),
  -- 2023+: Braya online; RD dominant but BD continues at historical pace
  ('3826001000','imports','1220',2023,2099, 0.25, 0.75, 'medium', 'mig 086 recalibration', 'Braya at scale; RD dominant, BD residual'),
  ('3826003000','imports','1220',2023,2099, 0.25, 0.75, 'medium', 'mig 086 recalibration', NULL);
