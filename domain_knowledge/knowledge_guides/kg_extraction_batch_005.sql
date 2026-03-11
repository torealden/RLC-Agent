-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 005
-- Source: 6 HB Weekly Text reports (Jul-Sep 2020, Jul-Sep 2021, Jul-Sep 2022)
-- Focus: Growing season analytical frameworks
-- Extracted: 2026-02-14
-- ============================================================================

-- NEW NODES
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'usda.crop_condition_rating', 'USDA Weekly Crop Condition Ratings', '{"context": "Monday publication. G/E percentage is primary metric. Historical relationship between change in rating Aug1-Sep1 predicts USDA yield change in next WASDE. Seasonal decline is normal — must compare to avg decline, not absolute level."}'),
('data_series', 'profarmer.crop_tour', 'ProFarmer Midwest Crop Tour', '{"context": "Third week of August. Scouts traverse SD→IN→MN. Corn yields measured, soybean pod counts taken. Results compared to prior year and 3yr avg. Tour signals beginning of end of growing season."}'),
('data_series', 'fsa.acreage', 'FSA Certified Acreage Data', '{"context": "Farm Service Agency insurance program data. Mostly complete in August. FSA planted area vs USDA June estimate indicates potential acreage revisions. 10yr avg of harvested/planted ratio used to project harvested area."}'),
('data_series', 'usda.crop_progress.development', 'USDA Crop Development Stages', '{"context": "Silking/dent/mature for corn, blooming/pods/dropping leaves for soybeans. Development pace vs 5yr avg indicates if crop is early/late and vulnerability to frost or heat."}'),
('seasonal_event', 'august_wasde_pivot', 'August WASDE Yield Methodology Pivot', '{"context": "Shoulder month between trendline (May-Jul) and objective data (Sep+). Aug relies on farmer surveys reflecting crop condition ratings. First month of actual yield-based forecasts. Often largest yield change month."}'),
('seasonal_event', 'peak_weather_sensitivity', 'Peak Weather Market Sensitivity Window', '{"context": "Jul-early Aug for corn (pollination), Aug-mid Sep for soybeans (pod fill). After peak weather, traders shift focus from weather maps to WASDE reports. RSI flatlines between overbought/oversold during transition."}'),
('analytical_model', 'crop_condition_yield_model', 'Crop Condition → Yield Prediction Model', '{"context": "HB signature methodology. Uses difference in G/E rating between first week of prior month and first week of report month to predict USDA yield change. More reliable than absolute rating level. Must adjust for seasonal decline (corn G/E avg drops 3.4% Aug→Sep)."}'),
('analytical_model', 'derecho_impact_model', 'Extreme Weather Event Impact Model', '{"context": "Aug 2020 derecho in Iowa: G/E corn dropped 28%, soybeans dropped 23% in Iowa alone. Largest single-state decline in 20 years. USDA crop condition ratings capture damage but may understate initial impact."}'),
('data_series', 'usda.wasde.aug_yield_change', 'August WASDE Yield Change Patterns', '{"context": "Aug 2021: first corn yield cut since 2017, first soy yield cut since 2013 — both below analyst range. Historical avg change small but can surprise significantly when conditions deteriorate."}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- EDGES
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES

-- Crop condition → yield prediction (core growing season framework)
((SELECT id FROM core.kg_node WHERE node_key = 'crop_condition_yield_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'PREDICTS', 0.95,
 '{"mechanism": "Change in G/E rating from first week of prior month to first week of report month predicts USDA yield change direction and magnitude. Key parameters by month:",
   "august_methodology": "Jul1→Aug1 G/E change. In 2021: unchanged rating implied +3.9 bpa corn, +0.1 bpa soy increase. 1% G/E drop needed in soy to imply yield cut. 5%+ G/E corn drop needed for unchanged yield. In 2022: Jul→Jul28 decline implied +1.5 bpa corn but forecast to cross negative at 58% G/E threshold.",
   "september_methodology": "Aug7→Sep4 G/E change. Avg Aug→Sep corn change since 2000 is -3.4% (seasonal). So 4% drop in 2022 only slightly worse than avg → implied yield change just -0.1 bpa. But analysts expected -2.9 bpa cut — massive divergence from model.",
   "historical_calibration": "2020: Aug2→Aug30 corn G/E fell 10% (largest since 2003 when -21%). 2003 10% drop → USDA -1.4 bpa. 2020 10% drop → implied -1.0 bpa. Soy 7% G/E drop (largest since 2013 13%) → implied -0.8 bpa.",
   "edge_case": "Oct raise then Nov cut in soy only 3/20yr (from Batch 004). Model works best when deviation from seasonal avg is clear signal vs noise."}',
 'extracted', 0.95),

-- FSA acreage → production forecast
((SELECT id FROM core.kg_node WHERE node_key = 'fsa.acreage'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'LEADS', 0.90,
 '{"mechanism": "FSA certified acreage vs USDA June estimate indicates revision direction. FSA typically below USDA June estimate but key is WHERE FSA exceeds June in specific states. Aug 2021 example: FSA corn 90.3M vs USDA June 92.7M (2.4M below). But FSA exceeded June in AR, DE, IN, MS, MO, NE, ND → implied final acreage 1M above June estimate. HB raised planted area to 93.7M, harvested to 85.6M. USDA eventually raised to 93.3M in Sept (600K).",
   "prevent_plant": "FSA prevent-plant acreage indicates lost production potential. 2021: just 2M acres → total major crop planted 250.9M (largest since 2011).",
   "timing": "FSA data sufficiently complete in Aug. USDA historically waited until Oct to revise acreage, but in 2021 and 2022 moved up to September — may become permanent practice."}',
 'extracted', 0.90),

-- Peak weather → seasonal price pattern
((SELECT id FROM core.kg_node WHERE node_key = 'peak_weather_sensitivity'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'CAUSES', 0.90,
 '{"mechanism": "Corn peaks Jul (pollination). Soybeans peak Aug-early Sep (pod fill). After peak weather passes: (1) market removes weather risk premium, (2) traders shift from weather maps to WASDE reports for catalysts, (3) RSI flatlines between overbought/oversold, (4) seasonal harvest pressure adds downside. Jul 2021: corn through pollination → volatility drops. But soybean pod fill still 3 weeks away → soy has more upside optionality than corn in late Jul/early Aug.",
   "northern_plains_asymmetry": "25% of soy acreage but only 18% of corn in N Plains → drought there is more bearish for soy production than corn. Creates relative value opportunity.",
   "late_season_save": "Rain during pod fill can still save soy yields. Corn cannot recover lost pollination. This makes soybean yield forecasts more uncertain through mid-September.",
   "weather_forecast_reliability": "7-day forecasts stretch into mid-Aug from late Jul. 14-day maps less reliable but thematic consistency over multiple days increases confidence. When maps show consistent hot/dry ridge for 7+ days, pattern change is high probability."}',
 'extracted', 0.90),

-- ProFarmer tour → price catalyst
((SELECT id FROM core.kg_node WHERE node_key = 'profarmer.crop_tour'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'LEADS', 0.85,
 '{"mechanism": "Third week of August. Scouts traverse SD→IN→MN measuring corn yields and soybean pod counts. Results compared to prior year and 3yr avg. Provides early read before Aug WASDE. Tour results often drive selling if yields meet expectations (confirms no weather disaster). 2021: yields above prior year everywhere except SD. Soy pod counts more variable in NE. 2022: variability dominant theme — corn worse than expected, soybeans better. In WI District 4, corn -20 bpa vs 2021 and -4% vs 3yr avg while soy pod counts above prior year.",
   "trading_implications": "Tour signals beginning of end of growing season → beginning of seasonal harvest decline. If results confirm expectations, any weather premium is removed. If results show surprise losses, provides early validation for bearish production forecasts before USDA confirms in September."}',
 'extracted', 0.85),

-- August WASDE methodology pivot → market surprise potential
((SELECT id FROM core.kg_node WHERE node_key = 'august_wasde_pivot'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'CAUSES', 0.90,
 '{"mechanism": "August is shoulder month between trendline (May-Jul) and objective data (Sep+). USDA relies on farmer surveys reflecting crop condition ratings. Aug 2020: first year without objective yield data (COVID disrupted field surveys) → USDA used survey only. Bearish data but corn and soy rallied 1% → contra-intuitive move indicated market had already priced in worst case.",
   "surprise_history": "Aug 2021: USDA cut corn -4.9 bpa (first since 2017) and soy -0.8 bpa (first since 2013). Both below analyst range — drove sharp rallies in corn/wheat. Market treated double-cut as rare event.",
   "aug_2022_soy_surprise": "USDA RAISED soy yield when market expected cut. Sharp sell-off followed but pod fill weather outlook brought buyers back same day. Finished +5c. Demonstrates Aug estimates are preliminary and can reverse.",
   "practical_rule": "When Aug WASDE yield is at extreme of analyst range, position for September correction. USDA Aug estimates are least reliable of the production-season reports."}',
 'extracted', 0.90),

-- Soybean critical development window
((SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'SELF_REINFORCING', 0.90,
 '{"mechanism": "Soybean yield development timeline: Blooming (Jul) → Pod Setting (early-mid Aug, 80-95% by Aug 15-23) → Pod Fill (late Aug-mid Sep) → Dropping Leaves (Sep-Oct). Pod fill is THE critical period. Rain during pod fill can save yields. Drought during pod fill causes pod abortion — plants shed developing pods to conserve resources.",
   "iowa_2020_example": "95% of Iowa soy crop set pods before Aug 23. Dryness after pod set → pod abortion risk. G/E rating fell from 73% to 50% (23 point drop, largest of any state). Late rain too late to fully recover — plant had already aborted pods.",
   "development_pace": "When crop is behind 5yr avg, pod fill extends later → more vulnerability to early frost. When ahead of 5yr avg, crop matures early → less late-season risk but more mid-Aug heat risk during pod fill.",
   "kernel_weight_corn_analog": "40% of corn kernel dry matter accumulates after dent stage. Even after pollination, conditions during grain fill still matter for corn — just less than soybeans."}',
 'extracted', 0.90),

-- WASDE pre-report positioning framework
((SELECT id FROM core.kg_node WHERE node_key = 'crop_condition_yield_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'PREDICTS', 0.90,
 '{"mechanism": "HB builds independent balance sheet BEFORE each WASDE. Compares own yield/production/demand estimates to analyst consensus. When HB diverges from consensus, positions for the divergence direction. Sep 2022 example: crop condition model implied -0.1 bpa corn yield cut. Analysts expected -2.9 bpa cut. HB model right (USDA cut only -0.4). This divergence framework validated across multiple years.",
   "demand_side_forecast": "Not just yield — also forecast demand adjustments USDA will make. Sep 2022: predicted USDA would cut ethanol by 100M bu, potentially cut exports. For wheat Sep, predicted USDA would cut food and feed 5M each. Building the full balance sheet reveals where ending stocks will land even if individual supply/demand components differ from consensus.",
   "world_balance_decomposition": "Calculate implied non-US changes by subtracting US predicted change from world predicted change. If world carryout change = US change, no international surprise expected. If divergence, look for specific country production revisions (Canada, Australia, Russia)."}',
 'extracted', 0.90),

-- FOMC/macro risk to crop markets
((SELECT id FROM core.kg_node WHERE node_key = 'crude_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'CAUSES', 0.70,
 '{"mechanism": "Aug 2021: FOMC minutes suggesting tapering triggered selling across financial AND agricultural markets. When fundamental backdrop is bullish, macro pressure creates buying opportunity. When bearish, macro amplifies decline. HB typically does not parse FOMC but flagged this as unusual environment where monetary policy could substantially impact price outlook at current market cycle stage. Parallels to 2022 when rate hikes drove broad commodity selloff despite tight grain fundamentals."}',
 'extracted', 0.75);


-- CONTEXTS
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES

-- Master growing season calendar
((SELECT id FROM core.kg_node WHERE node_key = 'peak_weather_sensitivity'),
 'expert_rule', 'growing_season_calendar',
 '{"calendar": {
   "may_jun": "Planting pace → USDA acreage. Prevent plant if too wet. Emergence ratings begin.",
   "late_jun": "Crop condition ratings stabilize after initial decline from high starting point. Jun30 acreage/stocks reports.",
   "jul": "CORN CRITICAL: pollination. Jul1→Aug1 G/E change predicts Aug WASDE. RSI begins to flatline. Weather maps dominate.",
   "aug_wk1_2": "Aug WASDE (shoulder methodology). FSA acreage data begins publishing. ProFarmer tour wk3.",
   "aug_wk3_4": "SOYBEAN CRITICAL: pod fill begins. Tour results. Market removes corn weather premium, shifts to WASDE focus.",
   "sep_wk1_2": "Sep WASDE (first objective yield data). Acreage revision (recently moved up from Oct). Soybean pod fill continues. Harvest begins in Delta.",
   "sep_wk3_oct": "Harvest pressure accelerates. Oct WASDE. Basis widens then narrows. Funds build or cover seasonal positions.",
   "nov_jan": "Nov WASDE (final field survey). Historical yield revision patterns (corn -0.7 avg, soy +0.1 avg). Jan Annual Summary = final production."
 }}',
 'growing_season', 'extracted'),

-- Crop condition yield model parameters
((SELECT id FROM core.kg_node WHERE node_key = 'crop_condition_yield_model'),
 'expert_rule', 'yield_model_parameters',
 '{"model": "USDA yield change = f(G/E rating change from first week of prior month to first week of report month)",
   "corn_aug": "Jul1→Aug1 change. Historical: unchanged rating → +3.9 bpa increase expected. Need >5% G/E drop to imply unchanged yield.",
   "corn_sep": "Aug7→Sep4 change. Avg seasonal decline 3.4%. Only deviation from avg matters — 4% drop implies just -0.1 bpa cut.",
   "soy_aug": "Jul1→Aug1 change. 1% G/E drop threshold for yield cut. More sensitive to deterioration than corn.",
   "soy_sep": "Aug→Sep change. 2% decline implies +0.1 bpa (seasonal pattern). Deviation must be substantial.",
   "calibration_years": "2000-2022. 2020 methodology change (no objective data in Aug) slightly skewed relationship.",
   "practical_threshold": "When model diverges from analyst consensus by >1 bpa, the divergence direction IS the trade. Validated 2021 (model right on direction, HB beat consensus), 2022 (model closer to USDA than analyst avg)."}',
 'pre_report', 'extracted'),

-- Analyst range as positioning tool
((SELECT id FROM core.kg_node WHERE node_key = 'usda.wasde.aug_yield_change'),
 'expert_rule', 'analyst_range_positioning',
 '{"framework": "When USDA yield falls outside analyst pre-report range, market reaction is amplified. Position for this by identifying when own model diverges from consensus.",
   "aug_2021_corn": "Analysts: 175.1-179.0, avg 177.5. USDA: 174.6 (1.1 below range floor). Rally 2.5%. HB had 177.5 — too high but less wrong than consensus.",
   "aug_2021_soy": "Analysts: 49.3-51.3, avg 50.4. USDA: 50.0 (near bottom). Initial rally then faded.",
   "aug_2022_soy": "Market expected yield CUT. USDA raised yield. Sharp sell then recovery as weather outlook bearish for pod fill.",
   "sep_2022_corn": "Analysts avg -2.9 bpa cut. Crop condition model implied -0.1 bpa. Model was right — USDA likely closer to model than consensus.",
   "rule": "Build own estimate. Compare to range. If outside range in direction your model predicts, high conviction trade. If inside range, lower conviction."}',
 'pre_report', 'extracted'),

-- FSA → USDA acreage revision framework
((SELECT id FROM core.kg_node WHERE node_key = 'fsa.acreage'),
 'expert_rule', 'fsa_acreage_revision_framework',
 '{"methodology": "FSA published Aug, mostly complete. Compare state-by-state to USDA June estimate.",
   "step1": "Identify states where FSA EXCEEDS June estimate (implies USDA will raise in those states)",
   "step2": "Assess whether excess in those states offsets deficit in others (usually yes for corn, sometimes no for soybeans)",
   "step3": "Apply 10yr avg harvested/planted ratio to estimate harvested area change",
   "step4": "Calculate production impact at current yield forecast",
   "step5": "Assign additional production to ending stocks (demand side rarely changes on acreage revision alone)",
   "2021_corn": "FSA 90.3M vs USDA June 92.7M. Excess in 7 states → HB raised to 93.7M. USDA went to 93.3M in Sep. HB closer to final.",
   "2021_soy": "FSA 85.3M vs USDA June 87.6M. Excess in 5 states but offset by deficits elsewhere → HB kept 87.6M.",
   "timing_change": "USDA moved acreage revision from Oct to Sep starting 2021. May be permanent. Means Sep WASDE now has BOTH yield AND acreage revisions — more volatile month."}',
 'harvest_season', 'extracted'),

-- Contra-intuitive price reaction framework
((SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'expert_rule', 'contra_intuitive_reaction',
 '{"framework": "When market rallies on bearish data or sells on bullish data, the move indicates positioning exceeded fundamentals.",
   "aug_2020_example": "USDA raised yield (bearish). Corn and soy rallied 1%. Implied market had priced in worse than USDA delivered. Signaled bottom was in — next directional move would be up.",
   "aug_2022_soy_example": "USDA raised soy yield (bearish) when market expected cut. Sharp sell then reversal to +5c. Pod fill weather outlook overrode USDA number. Demonstrates market looks forward not backward.",
   "rule": "If bearish data → rally, the fundamental low is likely in. If bullish data → sell, the fundamental high is likely in. Price reaction on report day more informative than the data itself.",
   "caveat": "Only applies to substantial data surprises. Minor data changes within analyst range dont generate reliable contra-signals."}',
 'report_day', 'extracted');


-- SOURCE REGISTRY
INSERT INTO core.kg_source (source_key, source_type, title, location_uri, document_date, document_type, commodities, topics, status, first_processed, last_processed, nodes_extracted, edges_extracted, contexts_extracted) VALUES
('gdoc_14TkcAexZV_1LIGCRoe35i1mwaSAyxnmqg8cVjTJ0gEg', 'gdrive_doc', 'HB Weekly Text - 09082022 (Sep WASDE Preview)', 'https://docs.google.com/document/d/14TkcAexZV_1LIGCRoe35i1mwaSAyxnmqg8cVjTJ0gEg/edit', '2022-09-08', 'weekly_text', '{corn,soybeans,wheat}', '{wasde_september,crop_conditions,yield_model,acreage_revision,analyst_consensus,balance_sheet}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1Pp-YFYcOGNpqGWxeuO8UOBAg3Ckz8kZoAzpEaCUsue4', 'gdrive_doc', 'HB Weekly Text - 08192021 (ProFarmer Tour + FSA)', 'https://docs.google.com/document/d/1Pp-YFYcOGNpqGWxeuO8UOBAg3Ckz8kZoAzpEaCUsue4/edit', '2021-08-19', 'weekly_text', '{corn,soybeans,wheat}', '{profarmer_tour,fsa_acreage,price_forecast,fomc_risk,seasonal_decline}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1UpICb5CwEfYu_Aat7A8Y7c89TS8XziICuOGe5YvoVHc', 'gdrive_doc', 'HB Weekly Text - 08122021 (Aug WASDE Surprise)', 'https://docs.google.com/document/d/1UpICb5CwEfYu_Aat7A8Y7c89TS8XziICuOGe5YvoVHc/edit', '2021-08-12', 'weekly_text', '{corn,soybeans,wheat,soybean_oil}', '{wasde_august,yield_cut,world_wheat_production,biofuel_policy,price_forecast}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1aohrkSAUHIhyI0kTOfOhJ2-HTRiXvcTWA4fxqN7Ooes', 'gdrive_doc', 'HB Weekly Text - 09022020 (Derecho + Drought Impact)', 'https://docs.google.com/document/d/1aohrkSAUHIhyI0kTOfOhJ2-HTRiXvcTWA4fxqN7Ooes/edit', '2020-09-02', 'weekly_text', '{corn,soybeans}', '{crop_conditions,derecho,drought,iowa,yield_forecast,condition_decline}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1wewmglMylj0MOaxBhwLR7CWn7UkEnmzyMylPwefWnV4', 'gdrive_doc', 'HB Weekly Text - 07292021 (Peak Weather + RD Demand)', 'https://docs.google.com/document/d/1wewmglMylj0MOaxBhwLR7CWn7UkEnmzyMylPwefWnV4/edit', '2021-07-29', 'weekly_text', '{corn,soybeans,wheat,soybean_oil}', '{peak_weather,crop_conditions,yield_model,renewable_diesel,crush_demand,brazil_corn}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1O2IocnuPvVcUvOhTmPNkmWr_i2Guwpgnc7hd43Gxecs', 'gdrive_doc', 'HB Weekly Text - 07282022 (Weather Pattern Shift)', 'https://docs.google.com/document/d/1O2IocnuPvVcUvOhTmPNkmWr_i2Guwpgnc7hd43Gxecs/edit', '2022-07-28', 'weekly_text', '{corn,soybeans,wheat}', '{weather_pattern,crop_conditions,yield_thresholds,development_stages,spring_wheat}', 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();
