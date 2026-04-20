-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 015
-- Source: 14 HB Weekly ChatGPT Summaries (Aug-Oct 2025), 1 Tore Daily Comment
--         (Jul 2025), 2 Long-Term Biofuel Outlook docs (Oct 2025),
--         3 WASDE summaries, 2 Bull/Bear reports
-- Location: C:/Users/torem/RLC Dropbox/RLC Team Folder/RLC Documents/Reports/
-- Extracted: 2026-04-16
-- Scope: Incremental reinforcement of batches 001-009 (HB Weekly analytical
--         framework). Focus on NEW insights not already in the KG.
-- ============================================================================

-- FINDINGS:
--   The HB Weekly ChatGPT Summaries are AI-generated structured recaps of
--   weekly grain market fundamentals (corn/soy/wheat) from Aug-Oct 2025.
--   They are comprehensive but derivative — describing market conditions rather
--   than encoding the analyst's original framework. The KG batches 001-009
--   (extracted from Tore's actual HB Weekly Text reports 2019-2023) already
--   capture the core analytical architecture:
--     - Synthesis framework, recommendation structure, outlook scenarios
--     - Calendar spread % of carry interpretation (benchmarks, VSR thresholds)
--     - Crop condition → yield model, growing season calendar
--     - WASDE interpretation, Chinese demand tracking, Brazil crop framework
--     - Split market dynamic (crush vs export), data vacuum adaptation
--     - Fund positioning extremes, short-covering mechanics
--
--   The ChatGPT summaries REINFORCE all of the above (confirming they are
--   stable analyst frameworks) but add few genuinely new nodes/edges.
--
--   NEW MATERIAL found in:
--   1. Tore's original daily comment (Jul 1 2025) — bimodal price thesis,
--      price elasticity asymmetry for veg oil/fats
--   2. World Biofuel Long-Term Report (Oct 2025) — BBD complexity escalation
--   3. Bull/Bear reports — Argentina tax holiday demand shock pattern
--   4. Multiple reports — Mississippi River low water as recurring logistics risk
--   5. Oct 1 report — Brazilian corn quality rejection (mycotoxin) as disruptor
-- ============================================================================


-- ============================================================================
-- SOURCE REGISTRATION
-- ============================================================================
-- [AUTO-FIXED] INSERT INTO core.kg_source (source_key, source_type, title, url, report_date, category, commodities, themes, status, first_seen, last_processed, node_count, edge_count, context_count)
-- [AUTO-FIXED] VALUES
-- [AUTO-FIXED] ('hb_weekly_chatgpt_aug_oct_2025', 'report_collection', 'HB Weekly ChatGPT Summaries (Aug-Oct 2025)',
-- [AUTO-FIXED]  'file://dropbox/RLC Documents/Reports/HB Weekly ChatGPT Summary/', '2025-10-01',
-- [AUTO-FIXED]  'weekly_market_analysis',
-- [AUTO-FIXED]  '["corn","soybeans","soybean_oil","soybean_meal","wheat_srw","wheat_hrw","wheat_hrs","ethanol"]',
-- [AUTO-FIXED]  '["harvest_fundamentals","carry_spreads","basis_logistics","export_demand","fund_positioning","south_american_weather"]',
-- [AUTO-FIXED]  'completed', '2026-04-16', '2026-04-16', 2, 4, 7),
-- [AUTO-FIXED] 
-- [AUTO-FIXED] ('tore_daily_20250701', 'daily_comment', 'Tore Comments - July 1, 2025',
-- [AUTO-FIXED]  'file://dropbox/RLC Documents/Reports/Daily Comments/2025/July/Tore Comments - 070125.docx', '2025-07-01',
-- [AUTO-FIXED]  'daily_analysis',
-- [AUTO-FIXED]  '["soybeans","soybean_oil","corn"]',
-- [AUTO-FIXED]  '["bimodal_price_distribution","feedstock_demand_elasticity","biofuel_policy"]',
-- [AUTO-FIXED]  'completed', '2026-04-16', '2026-04-16', 1, 2, 2),
-- [AUTO-FIXED] 
-- [AUTO-FIXED] ('world_biofuel_lt_oct_2025', 'long_term_outlook', 'World Biofuel Long-Term Report (Oct 2025)',
-- [AUTO-FIXED]  'file://dropbox/RLC Documents/Reports/Long-Term Outlooks/2025/October/', '2025-10-05',
-- [AUTO-FIXED]  'long_term_biofuel_outlook',
-- [AUTO-FIXED]  '["soybean_oil","renewable_diesel","sustainable_aviation_fuel"]',
-- [AUTO-FIXED]  '["bbd_complexity","saf_market_structure","ci_pathway_approval"]',
-- [AUTO-FIXED]  'completed', '2026-04-16', '2026-04-16', 0, 0, 1)
-- [AUTO-FIXED] ON CONFLICT (source_key) DO NOTHING;


-- ============================================================================
-- NEW NODES
-- ============================================================================

-- Mississippi River logistics as a recurring risk node
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('logistics_risk', 'mississippi_river_low_water', 'Mississippi River Low Water Event',
 '{"context": "Recurring late-summer/fall risk. Draft restrictions reduce barge loads 5-15% per foot lost, doubling freight costs. Directly widens interior-Gulf basis for corn/soy. Documented events: 2022, 2023, 2024, 2025. Peak impact Sep-Nov during harvest season. Key gauges: Memphis, St Louis. Can divert grain to PNW rail, compress Gulf export capacity.",
  "threshold_memphis": "Below -2 feet triggers draft restrictions",
  "frequency": "4 consecutive years as of 2025",
  "freight_impact": "Barge rates 400-800% of tariff during severe events",
  "basis_impact": "Interior basis can widen 30-50c/bu beyond normal seasonal decline"}'),

-- Argentina tax holiday as repeatable policy shock
('policy_event', 'argentina_soy_export_tax_holiday', 'Argentina Soybean Export Tax Holiday',
 '{"context": "Temporary elimination of soy/meal/oil export taxes (normally 33% on beans). Pattern: government offers preferential FX rate or tax reduction to stimulate farmer sales and dollar inflows. Chinese buyers immediately exploit the discount, booking 10-20 cargoes within days. Repeatable pattern observed Sep 2025. Creates sudden bearish shock for US soybean export competitiveness.",
  "precedents": ["soy_dollar_2023", "devaluation_aug_2025", "tax_holiday_sep_2025"],
  "china_response": "10-20 cargoes (1.3-1.6 MMT) booked within days",
  "us_impact": "Directly undercuts US Gulf offers, delays Chinese US purchases"}')
ON CONFLICT (node_key) DO NOTHING;


-- ============================================================================
-- NEW EDGES
-- ============================================================================
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, properties, confidence, source_count) VALUES

-- Mississippi River low water CAUSES basis widening
((SELECT id FROM core.kg_node WHERE node_key = 'mississippi_river_low_water'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'CAUSES',
 '{"mechanism": "Draft restrictions reduce barge loads, spike freight rates 2-4x normal. Interior basis widens 20-50c to offset. Gulf CIF weakens on logistics uncertainty. PNW export basis strengthens as alternative. Paradoxically can support futures short-term by trapping grain inland.",
  "timing": "Sep-Nov, during peak harvest/export season",
  "historical": "2022 event caused record barge freight, Gulf basis collapsed to -$1+ in some delta locations"}',
 0.90, 4),

-- Mississippi River low water CAUSES basis widening in soybeans
((SELECT id FROM core.kg_node WHERE node_key = 'mississippi_river_low_water'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'CAUSES',
 '{"mechanism": "Even more critical for soybeans than corn because peak soy export season (Oct-Nov) coincides with lowest river levels. Can physically constrain Gulf shipments, force re-routing to PNW rail, or delay Chinese purchases. 2022 precedent: some Chinese cargoes shifted to Brazil new crop because US logistics were snarled.",
  "basis_impact": "Northern Plains basis collapsed to -$1.50/bu in 2025 due to combined river/no-China effect",
  "export_risk": "If Gulf cannot ship ~2.5-3.0 MMT/month, export forecast must be cut"}',
 0.90, 4),

-- Argentina tax holiday COMPETES_WITH US soybean exports
((SELECT id FROM core.kg_node WHERE node_key = 'argentina_soy_export_tax_holiday'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'COMPETES_WITH',
 '{"mechanism": "When Argentina temporarily drops 33% soy export tax, Chinese buyers immediately redirect purchases from US to Argentina. Creates sudden 1-2 MMT demand shift away from US. Effect is amplified when combined with existing US-China tariff friction.",
  "sep_2025": "10+ cargoes booked by China within days, ~$2.00 premium over Nov C&F",
  "pattern": "Repeatable — Argentine government uses tax holidays as FX management tool, typically pre-election or during balance-of-payments stress"}',
 0.85, 2),

-- BBD complexity escalation (RD → SAF adds order of magnitude)
((SELECT id FROM core.kg_node WHERE node_key = 'sustainable_aviation_fuel'),
 (SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'EXTENDS_COMPLEXITY',
 '{"mechanism": "Three-stage complexity escalation: (1) Biodiesel: buy feedstock, transesterify, sell locally + BTC + RIN. (2) RD: add LCFS/CI pathway approval, rail fuel to California, CI score makes feedstock market 10x more complex. (3) SAF: add international mandates (ReFuelEU, CORSIA), book-and-claim mechanisms, multiple jurisdictional credit stacking (45Z + LCFS + RIN + offtake premium). Each stage is an order of magnitude more complex than the last.",
  "source": "World Biofuel Long-Term Report Oct 2025",
  "implication": "SAF profitability increasingly narrow path — requires simultaneous policy alignment across multiple jurisdictions"}',
 0.85, 1)
ON CONFLICT DO NOTHING;


-- ============================================================================
-- NEW CONTEXTS
-- ============================================================================
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES

-- Tore's original framework: bimodal price distribution thesis
((SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'expert_rule', 'bimodal_price_distribution',
 '{"rule": "BBD feedstock demand has created a bimodal price distribution for US oilseeds. In any given year, prices will either rally sharply (if weather reduces yields against mandated demand) or drop to multi-year lows (if yields are trend or above). The middle ground is shrinking because mandated biofuel demand creates a structural floor that is hit hard — any shortfall triggers panic buying, while any surplus means mandates are easily met and demand is capped. This setup persists until (a) feedstock demand growth drops below annual yield growth, (b) industry finds new feedstock sources, or (c) biofuel policy support wanes.",
  "author": "Tore Alden",
  "date": "2025-07-01",
  "implication": "Standard mean-reversion strategies are less effective. Position for tails. Growing-season volatility is structurally underpriced relative to the binary outcome."}',
 'growing_season', 'tore_daily_20250701'),

-- Tore's original framework: price elasticity asymmetry
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'expert_rule', 'price_elasticity_asymmetry',
 '{"rule": "Mandated biofuel buyers must compete for vegetable oils and animal fats against industrial/food buyers with extremely low price elasticity. For a paint manufacturer, soybean oil is <10% of total cost ($4/gal out of $50/gal retail) — they can afford to pay far more than a biofuel producer where feedstock is ~90% of cost structure. Additionally, many industrial applications have fixed recipes making substitution nearly impossible. This creates a structural price floor: industrial demand is inelastic and effectively unlimited at current prices, while mandated biofuel demand is volume-certain but margin-sensitive. The pinch point is that mandated buyers cannot reduce volume (law requires it) but cannot absorb high prices either.",
  "author": "Tore Alden",
  "date": "2025-07-01",
  "framework": "Two demand curves: (1) Industrial/food: low elasticity, small share of end-product cost, nearly impossible substitution. (2) Biofuel: mandated volume, ~90% feedstock cost, margin-sensitive. When supply tightens, biofuel producers get squeezed because industrial buyers absorb price increases without reducing demand.",
  "implication": "Veg oil price spikes are rationing biofuel production, not food/industrial use. Policy mandates force biofuel producers to bid against buyers who can outbid them on unit economics."}',
 'always', 'tore_daily_20250701'),

-- BBD market complexity escalation framework
((SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'expert_rule', 'bbd_complexity_escalation',
 '{"rule": "The BBD industry has undergone three stages of complexity escalation, each adding an order of magnitude: Stage 1 (Biodiesel 2000s): Simple transesterification. Revenue = fuel + glycerin + BTC + RIN. Local rack market. Stage 2 (RD 2010s-2020s): HEFA technology adds LCFS/CI pathway requirement. Must rail fuel to California. CI score (GREET model) makes feedstock sourcing 10x more complex because every feedstock has different CI value. Stage 3 (SAF 2020s-2030s): International mandates (ReFuelEU, CORSIA), book-and-claim mechanisms, multiple jurisdictional credit stacking (45Z + LCFS + RIN + state incentive + offtake premium). Increasingly narrow path to profitability for any given facility.",
  "source": "World Biofuel Long-Term Report Oct 2025",
  "key_stat": "$16 billion in approved BBD investments for 2025, $8B deployed in H1",
  "implication": "SAF capacity is growing but profitability path is so narrow that many facilities may never achieve sustainable margins. Complexity itself is a barrier to entry AND a risk multiplier."}',
 'always', 'world_biofuel_lt_oct_2025'),

-- Mississippi River low water as recurring seasonal risk
((SELECT id FROM core.kg_node WHERE node_key = 'mississippi_river_low_water'),
 'expert_rule', 'low_water_harvest_logistics_framework',
 '{"rule": "Mississippi River low water is now a recurring annual risk (4 consecutive years 2022-2025) coinciding with peak grain export season. Framework: (1) Monitor Memphis gauge from August — below +2 feet signals concern, below -2 triggers restrictions. (2) Each 1-foot draft cut = 5-8% less cargo per barge = proportional freight increase. (3) Barge rates express as % of tariff: normal ~300%, stress ~500-800%, crisis ~1000%+. (4) Basis impact cascades: river terminals widen first, interior follows, Gulf CIF weakens. (5) Counterfactual: PNW rail picks up slack but capacity ~1.5 MMT/month max. (6) If severe, Chinese buyers may shift purchases to Brazil new crop rather than wait for US logistics.",
  "monitoring": ["Memphis gauge (target above -2ft)", "USDA Grain Transportation Report weekly", "ACBL American Currents barge conditions", "St Louis barge tariff rates"],
  "2025_specifics": "Memphis approached -3.7 feet by Sep 29. Barge freight doubled vs normal. ND soy basis collapsed to -$1.50. Delta corn basis counterintuitively firmed as downstream terminals gained bidding power."}',
 'aug_through_nov', 'hb_weekly_chatgpt_aug_oct_2025'),

-- Brazilian corn quality rejection as market disruptor
((SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'expert_rule', 'brazil_quality_rejection_pattern',
 '{"rule": "Brazilian safrinha corn exports can suffer mycotoxin contamination, particularly in years when rain falls at maturity. European feed processors (especially Italy, Spain) may reject cargoes, creating sudden pockets of demand for US or Ukrainian corn at premium prices. In Oct 2025, Italian corn prices jumped above EUR 250/MT delivered (>$6.50/bu) when Brazilian cargoes were rejected. This is an intermittent but potentially significant demand shock for US corn exports to Europe.",
  "frequency": "Intermittent — depends on safrinha harvest weather",
  "magnitude": "Individual rejections redirect demand for 50-200k MT per incident",
  "source": "Oct 1 2025 weekly report"}',
 'post_safrinha_harvest', 'hb_weekly_chatgpt_aug_oct_2025'),

-- Reinforcement: Sep-Nov carry at record levels signals structural shift
((SELECT id FROM core.kg_node WHERE node_key = 'calendar_spread.pct_carry'),
 'historical_analog', 'record_harvest_carry_2025',
 '{"observation": "Sep-Nov 2025 soybean spread reached 21c carry — largest for that timeframe in at least two decades. Dec-Mar corn at 80%+ of full carry. Both near-full-carry spreads persisted even as flat prices bounced, confirming commercials were selling into rallies and storing. This extreme carry in multiple commodities simultaneously (corn, soy, wheat all at 60-80%+ of full carry) reflects: (1) record/near-record crops, (2) high interest rates raising theoretical full carry, (3) minimal nearby export demand pull, (4) abundant storage capacity.",
  "date": "Aug-Sep 2025",
  "threshold": "When ALL three grains show 60%+ carry simultaneously, market is unambiguously in surplus mode. Rallies will be sold. The first sign of regime change is carry compression in any one commodity.",
  "principle": "Simultaneous wide carries across the complex is the strongest bearish market-structure signal available."}',
 'harvest_season', 'hb_weekly_chatgpt_aug_oct_2025'),

-- Reinforcement: Fund short extremes create fragile market
((SELECT id FROM core.kg_node WHERE node_key = 'cftc.cot'),
 'expert_rule', 'triple_short_fragility',
 '{"rule": "When managed money is simultaneously heavily short corn (~150k contracts), short wheat (~110k SRW + ~54k HRW record), and only modestly long/flat soybeans, the entire grain complex is fragile to short-covering. Any single bullish catalyst (weather, Chinese purchases, policy shock) can trigger correlated covering across all three markets. The Aug-Sep 2025 period demonstrated this: corn and wheat each had near-record fund shorts while soy was flat. Brief rallies of 3-5% occurred on modest catalysts (Mexico corn purchases, Pro Farmer yield concerns) before fading. The key insight is that the SHORT ITSELF becomes the dominant price driver in a surplus environment — positioning dynamics can override fundamentals for 1-2 week periods.",
  "date_range": "Aug-Oct 2025",
  "corn_short_peak": "~150k contracts net short, largest of calendar year",
  "wheat_short_peak": "~110k SRW + ~54k HRW, HRW at record",
  "trigger_examples": ["Pro Farmer yield below USDA", "Mexico 313k MT corn sale", "Argentina tax holiday (bearish soy, but cross-market)", "Frost scare (did not materialize)"]}',
 'when_fund_positions_extreme', 'hb_weekly_chatgpt_aug_oct_2025')

ON CONFLICT DO NOTHING;


-- ============================================================================
-- REINFORCEMENT: Confidence bumps for existing contexts (source_count++)
-- These frameworks from batches 002-007 were explicitly confirmed by the
-- 2025 ChatGPT summaries. Rather than duplicate contexts, we note them here
-- for future confidence updates.
-- ============================================================================

-- The following existing KG entries are REINFORCED by the 2025 summaries:
--
-- 1. 'synthesis_framework' (batch 002) — Every weekly report ends with
--    "So What?" synthesis, base/bull/bear scenarios, and watchlist. Confirmed
--    in all 8 weekly summaries.
--
-- 2. 'carry_interpretation' (batch 002) — % of full carry benchmarks used
--    extensively. All reports calculate carry using 6-7% interest + $0.05/bu/mo
--    storage. VSR threshold at 80% referenced multiple times. CONFIRMED.
--
-- 3. 'outlook_structure' (batch 002) — Base/Bullish/Bearish scenarios with
--    specific price ranges in every weekly summary. CONFIRMED.
--
-- 4. 'split_market_dynamic' (batch 002) — "Tale of two demands: robust crush
--    vs lagging exports" appears verbatim in multiple reports. CONFIRMED.
--
-- 5. 'chinese_export_tracking' (batch 004) — Chinese absence from US soy
--    purchases tracked obsessively in every report. "Unknown" buyer analysis,
--    Brazil substitution, tariff impact all documented. CONFIRMED.
--
-- 6. 'brazil_crop_framework' (batch 004) — Safrinha timing, monsoon onset
--    monitoring, planting pace, ANEC export forecasts all tracked weekly.
--    CONFIRMED.
--
-- 7. 'growing_season_calendar' (batch 005) — Dent/maturity/frost risk
--    tracking through Aug-Sep. Pod fill/dropping leaves for soybeans.
--    CONFIRMED.
--
-- 8. 'yield_model_parameters' (batch 005) — G/E condition ratings tracked
--    weekly (71%→68%→65% corn decline), cross-referenced against yield
--    expectations. CONFIRMED.
--
-- 9. 'energy_inflation_framework' (batch 002) — Crude oil impact on ethanol
--    margins, biofuel demand, and freight costs tracked in every macro section.
--    CONFIRMED.
--
-- Recommended: UPDATE core.kg_edge SET source_count = source_count + 8
--   for edges involving these contexts, reflecting 8 additional weekly reports
--   that confirm the frameworks. (Not executed here per instructions.)
-- ============================================================================
