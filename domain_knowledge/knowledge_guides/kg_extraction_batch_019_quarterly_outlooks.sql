-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 019 (Quarterly Outlook Reports Q3 2021 - Q1 2022)
-- Source: Jacobsen Fats, Fuels & Feedstock Outlook Reports, Q3 2021 through Q1 2022
-- Folder: C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/
-- Extracted: 2026-04-16
-- Scope:  How analytical frameworks EVOLVED across 4 quarters preceding Q2 2022 (batch 018).
--         Tracks assumption changes, capacity forecast revisions, policy uncertainty,
--         co-processing abandonment, SAF introduction, UCO/YG model revisions,
--         crush capacity expansion arc, and the progression from RVO uncertainty
--         through mandate release to IRA anticipation.
-- ============================================================================

-- KEY FINDINGS (Framework Evolution Q3 2021 -> Q1 2022):
--   * Co-processing: Q3 2021 slashed from 50% to <3% of RD capacity -- biggest single-quarter assumption change
--   * SAF: First introduced in Q3 2021 forecasts, adopted Biden's 3B gal by 2030 target
--   * Biodiesel production: Systematically cut each quarter as RD expanded (Q3->Q4 largest cuts)
--   * Tallow imports: Sharply revised UP in Q1 2022 (avg +1B lbs/yr) after Q3-Q4 underestimated
--   * UCO/YG production model: Overhauled Q1 2022 (inflation adjustment), raised output +1.4B lbs/yr
--   * Crush capacity: Q3 2021 slowed expansion assumptions; Q1 2022 reversed with +470M bu over 4 years
--   * SBO refining shortage: Q4 2021 identified as key story; crushing/refining capacity announcements accelerated
--   * RVO mandates: Uncertain through Q3-Q4 2021, released Dec 2021, finalized Jun 2022
--   * Non-biofuel demand: Became swing variable across ALL feedstocks -- absorbs BBD demand cuts
--   * SBO 19% import tariff: Emerged as barrier in Q3 2021, trade association lobbying to remove
--   * Canola oil RD pathway: Expected Q4 2021, still pending Q1 2022
--   * Poultry fat RD use: First suspected Q3 2021, not confirmed by Q1 2022
--   * SBO acreage requirement: Q1 2022 estimated 94M acres avg -- only 2nd time farmers planned more soy than corn
--   * Fat/grease prices: Unprecedented records Q1 2022, raising baseline for all forecasts
--   * Bird flu: Q1 2022 -- not yet in models but noted as downside risk to poultry fat production


-- ============================================================================
-- 1. NODES: Evolving Models and Frameworks
-- ============================================================================

-- Co-Processing Assumption Collapse (Q3 2021)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'coprocessing_assumption_collapse', 'Co-Processing Capacity Assumption Collapse (Q3 2021)',
 '{"description": "In Q3 2021, The Jacobsen slashed its co-processing capacity forecast from 50% of biomass-based diesel production near end of forecast period to less than 3%. This was the single largest assumption change in any quarter and cascaded through every feedstock balance sheet.", "vintage": "Q3 2021",
   "pre_revision_pct_of_bbd": 50,
   "post_revision_pct_of_bbd": 3,
   "impact_on_feedstock_mix": "Co-processors used a higher percentage of SBO than RD plants. Shifting volume from co-processing to standalone RD increased demand for fats/greases (lower CI) and reduced relative SBO demand growth. However, supply limitations for low-CI feedstocks mitigated the full shift.",
   "impact_on_veg_oil_demand": "Net increase in vegetable oil demand for RD because SAF volumes (added same quarter) more than offset co-processing decline. RD demand increase outweighed co-processing loss.",
   "catalyst": "Industry reports indicated co-processing at petroleum refineries was not scaling as expected. Technical challenges and marginal economics vs standalone RD deterred investment.",
   "source": "jacobsen_q3_2021_executive_summary"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- SAF Introduction to Forecasts (Q3 2021)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'saf_forecast_introduction_q3_2021', 'SAF Volume Introduction to BBD Forecasts (Q3 2021)',
 '{"description": "Q3 2021 was the first quarter The Jacobsen included explicit SAF production volumes in the outlook. Adopted a timeline close to the Biden administrations proposal to produce 3 billion gallons by 2030. SAF volumes carved out of the D4 mandate allocation previously assigned entirely to domestic RD.", "vintage": "Q3 2021",
   "biden_target_bgal_by_2030": 3.0,
   "impact_on_d4_mandate": "SAF volumes did not alter overall D4 mandate, but split the domestic RD allocation between RD and SAF",
   "impact_on_feedstock_demand": "Substantial increase in vegetable oil demand for renewable diesel production (SAF uses same feedstock pathway). The Jacobsen predicted veg oil use to increase more than ten-fold over the forecast period.",
   "combined_with_coprocessing_cut": "SAF introduction + co-processing cut + biodiesel decline = net large increase in standalone RD feedstock demand",
   "sbo_import_consequence": "Drove Jacobsen prediction that US would become significant SBO importer, with import needs of 1.8 to 4.8B lbs/yr (2022-2027)",
   "source": "jacobsen_q3_2021_veg_oil_summary"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- SBO Refining Capacity Shortage Model (Q4 2021)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'sbo_refining_shortage_q4_2021', 'US SBO Refining Capacity Shortage (Q4 2021)',
 '{"description": "Q4 2021 identified SBO refining capacity shortage as the most important story of 2021. The shortage drove RBD prices and crude-to-RBD spreads to historical levels in Q3 2021. Triggered announcements of new crushing facilities and pretreatment capacity.", "vintage": "Q4 2021",
   "crush_capacity_trajectory": {"ye_2022_predicted_bbu": 2.25, "by_2025_predicted_bbu": 2.48},
   "sbo_refining_capacity": {"ye_2022_predicted_blbs": 17.2, "by_2025_predicted_blbs": 19.4},
   "rd_pretreatment_addition_blbs": 11.3,
   "refining_margin_dynamics": "Record refining margins Q3 2021 incentivized capacity announcements. Weaker-than-anticipated biofuel demand growth pushed margins lower by Q4.",
   "key_distinction_from_q2_2022": "Q4 2021 crush capacity targets lower than Q2 2022 (2.48B bu by 2025 vs 2.6B bu by 2027). Capacity estimates ratcheted up each quarter as more announcements made.",
   "catalytic_event": "Q3 2021 record RBD-crude SBO spread triggered a wave of crusher expansion announcements that continued through Q1 2022",
   "source": "jacobsen_q4_2021_veg_oils_summary"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Biodiesel Production Decline Model (Q3-Q4 2021)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'biodiesel_decline_model_q4_2021', 'Biodiesel Production Decline Framework (Q4 2021)',
 '{"description": "Framework for systematic biodiesel production forecast cuts across Q3-Q4 2021. The Jacobsen cut biodiesel projections substantially each quarter as RD expansion and feedstock competition eroded biodiesel economics. Key difference from Q2 2022: Q4 2021 cuts were not yet offset by ethanol blend wall analysis.", "vintage": "Q4 2021",
   "sbo_demand_impact": "Biodiesel SBO demand cuts of 3-14B lbs/yr across forecast period (Q4 2021 report)",
   "canola_demand_impact": "Canola oil biodiesel demand cut ~700M lbs/yr from 2024 through end of forecast",
   "fat_grease_impact": "Biodiesel fat/grease demand cuts of 675M-2B lbs/yr (Q4 2021), partially offset by RD demand increases",
   "offset_mechanism": "Non-biofuel demand absorbs surplus -- became the standard pattern across all feedstocks",
   "q3_to_q4_evolution": "Q3 2021 made first significant biodiesel cuts; Q4 2021 deepened them across all feedstocks",
   "missing_framework": "Unlike Q2 2022, Q4 2021 did not yet have ethanol blend wall model to explain how BBD demand would be structurally supported despite biodiesel rationalization",
   "source": "jacobsen_q4_2021_executive_summary"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- UCO/YG Production Model Overhaul (Q1 2022)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'uco_yg_model_overhaul_q1_2022', 'UCO/YG Production Model Overhaul (Q1 2022)',
 '{"description": "Q1 2022 saw a major overhaul of The Jacobsens UCO/YG production model to ensure inflation-driven food spending increases did not artificially inflate production estimates. The revised model resulted in higher historical output AND higher forecast production -- a counterintuitive result suggesting previous model was underestimating production.", "vintage": "Q1 2022",
   "model_methodology": "Relies on USDA food spending data to estimate UCO production from restaurant/food service sector",
   "inflation_concern": "Sharp increase in food price inflation could cause model to over-attribute spending to volume rather than price. Model revised to separate price from volume effects.",
   "result": "Higher historical and forecast production -- avg +1.4B lbs/yr UCO+YG across forecast period",
   "allocation_of_surplus": "Additional supply allocated primarily to RD feedstock demand (+1.7B lbs/yr RD). Biodiesel feedstock demand cut ~225M lbs/yr due to feedstock mix shift.",
   "evolution_from_q2_2022": "By Q2 2022, production estimates were CUT again by 1.1B lbs (2021) and model flagged for negative non-biofuel demand. The Q1 2022 upward revision was partially reversed.",
   "key_insight": "UCO/YG production estimation remained the most uncertain element of the entire outlook through all quarters. Each quarter brought significant revisions.",
   "source": "jacobsen_q1_2022_uco_yg_changes"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Tallow Import Revision (Q1 2022)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'tallow_import_revision_q1_2022', 'World Tallow Trade Flow Model Revision (Q1 2022)',
 '{"description": "Q1 2022 saw a major upward revision to US tallow import forecasts driven by a new world tallow production and trade flow model. Strong Q1 2022 import growth triggered the revision. Largest single-commodity import forecast change in the outlook series.", "vintage": "Q1 2022",
   "import_revision_avg_blbs_per_yr": 1.0,
   "import_revision_peak_blbs": 1.5,
   "allocation": "Import increase allocated to biofuel demand (avg +1.5B lbs/yr, with RD demand +1.3B lbs/yr). Non-biofuel and export demand reduced.",
   "price_impact": "Record Q1 2022 prices raised baseline. Mid-forecast prices moderate, but tight supplies near end of forecast drive avg +35 cpb in final 2 years.",
   "contrast_with_q4_2021": "Q4 2021 lowered tallow production forecast by 50M lbs/yr from smaller cattle herds. Only modestly raised 2022 imports. Q1 2022 completely reversed the cautious import stance.",
   "world_model_significance": "First quarter The Jacobsen explicitly mentioned a world tallow production and trade flow model as the driver of import forecasts",
   "source": "jacobsen_q1_2022_bft_changes"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- SBO Import Tariff Issue (Q3 2021)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('policy', 'sbo_import_tariff_19pct', 'US SBO Import Tariff (19.1%) and Removal Lobbying',
 '{"description": "US imposes a 19% tariff on soybean oil imports. As the US shifted toward potential net SBO importer status (first visible Q3 2021), trade associations from non-biofuel AND energy industries began lobbying for tariff removal. Removal would enable Argentine SBO imports to fill domestic shortfall.", "vintage": "Q3 2021 - Q1 2022",
   "tariff_rate_pct": 19.1,
   "first_noted_quarter": "Q3 2021",
   "context": "Census data showed US was net importer of SBO in Aug-Sep 2021 due to sharp reduction in exports, not a surge in imports",
   "lobbying_coalition": "Non-biofuel food/feed industries + energy industry jointly lobbying for removal",
   "impact_if_removed": "Would substantially increase import volumes, particularly from Argentina (worlds largest SBO exporter)",
   "impact_if_maintained": "Domestic prices must rise high enough to justify imports despite tariff burden, or crush capacity must expand faster",
   "q1_2022_update": "RD import assumption could offset some import pressure. If RD imports rise, Canadian RD may substitute for US SBO imports (avoiding 19% tariff on feedstock).",
   "source": "jacobsen_q3_2021_veg_oil_summary"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Crush Capacity Expansion Arc (Q3 2021 -> Q1 2022)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'crush_capacity_expansion_arc', 'US Crush Capacity Expansion Arc (Q3 2021 - Q1 2022)',
 '{"description": "Tracks how crush capacity expansion assumptions evolved across quarters. Q3 2021 SLOWED expansion assumptions. Q4 2021 identified refining shortage as key story and raised capacity targets. Q1 2022 dramatically increased crush expansion to +470M bu over 4 years as more announcements materialized.", "vintage": "Q3 2021 through Q1 2022",
   "q3_2021": {"action": "Decreased expansion rate", "impact": "Production down ~1B lbs/yr through 2024, but up 4B lbs/yr in 2028-2031 (back-loaded growth)", "driver": "Slower-than-expected new facility announcements"},
   "q4_2021": {"crush_ye_2022_bbu": 2.25, "crush_by_2025_bbu": 2.48, "refining_ye_2022_blbs": 17.2, "refining_by_2025_blbs": 19.4, "rd_pretreatment_blbs": 11.3, "driver": "Record refining margins triggered wave of crusher announcements"},
   "q1_2022": {"crush_expansion_4yr_mbu": 470, "refining_expansion_4yr_blbs": 4.6, "soybean_acreage_required_m_acres": 94, "driver": "Record prices + strong biofuel demand + prospective plantings showed record 91M soy acres", "acreage_context": "Only 2nd time farmers planned more soy than corn"},
   "q2_2022_comparison": {"crush_by_2027_bbu": 2.6, "refining_expansion_blbs": 4.5, "pretreatment_expansion_blbs": 3.4, "soybean_acreage_m_acres": 94.9},
   "trajectory": "Each quarter ratcheted up capacity assumptions as actual announcements materialized. Q3 2021 was the low point of expansion expectations.",
   "source": "jacobsen_q3_q4_2021_q1_2022_veg_oil_summaries"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- RVO/RFS Mandate Uncertainty Period (Q3-Q4 2021)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'rvo_uncertainty_period_2021', 'RFS Mandate Uncertainty Period (Q3-Q4 2021)',
 '{"description": "Prolonged period of RFS mandate uncertainty from Q3 2021 through Dec 2021. Industry awaited Biden administrations preliminary mandates for 2020, 2021, and 2022. Rumors of substantial cuts to ethanol mandate created market anxiety. Resolution came Dec 2021 with proposed mandates.", "vintage": "Q3-Q4 2021",
   "q3_2021_state": {"uncertainty": "high", "rumors": "Administration considering significant 2020-2021 RVO cuts to relieve blender compliance deficits from pandemic", "jacobsen_view": "RFS law limits EPAs ability to cut beyond modest cellulosic reduction", "mandate_adjustment": "Reduced D4 and D6 growth rate, increasing implied D5 mandate"},
   "q4_2021_resolution": {"date": "early December 2021", "proposed_2020_bgal": 17.13, "proposed_2021_bgal": 18.52, "proposed_2022_bgal": 20.77, "advanced_2022_bgal": 5.77, "bbd_2022_bgal": 2.76, "supplemental_mgal": 250, "supplemental_context": "250M gal for 2022 and intent for another 250M in 2023 to address ACE v EPA remand"},
   "ethanol_mandate_implication": "Implied 15-15.25B gal ethanol mandate vs ~13.9B demand at 10% blend rate. Gap forces additional BBD production of up to 400M gal in 2022.",
   "sre_context": "Biden administration stated policy to eliminate SREs. Combined with mandates, raised effective RVO substantially.",
   "market_impact": "D6 RIN prices expected to rise, narrowing D4-D6 spread. Blend wall concern became key market driver heading into 2022.",
   "contrast_with_q2_2022": "By Q2 2022, mandates were finalized (Jun 3, 2022). Uncertainty shifted from mandate levels to post-2022 RFS structure since law expired end of 2022.",
   "source": "jacobsen_q3_q4_2021_executive_summaries"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Fat/Grease Non-Biofuel Demand as Swing Variable
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('concept', 'nonbiofuel_demand_swing_variable', 'Non-Biofuel Demand as Swing Variable in BBD Outlook',
 '{"description": "Across all quarters (Q3 2021 through Q2 2022), non-biofuel demand for fats, greases, and vegetable oils became the primary shock absorber in every balance sheet. When BBD demand forecasts were cut, non-biofuel demand was raised by a similar amount (and vice versa). This pattern reveals a fundamental modeling assumption: total demand adjusts to match supply through the non-biofuel residual.", "vintage": "Q3 2021 - Q2 2022",
   "pattern_examples": {
     "q3_2021_cwg": "Increased RD demand offset by decreased non-biofuel use. CWG: +350M lbs biofuel, -350M lbs non-biofuel by end of forecast",
     "q3_2021_tallow": "Non-biofuel demand raised ~500M lbs from stronger-than-expected 2021 actuals",
     "q4_2021_sbo": "Non-biofuel use raised 5-6B lbs/yr to offset biodiesel demand cuts of 8-14B lbs/yr",
     "q4_2021_cwg": "+100M to +400M lbs non-biofuel per year to offset BBD cuts",
     "q4_2021_uco": "+375M lbs/yr non-biofuel from freed supply",
     "q1_2022_pf": "+375M lbs/yr non-biofuel from higher production",
     "q1_2022_canola": "-425M lbs/yr non-biofuel in 2nd half as biofuel demand grew"
   },
   "analytical_implication": "Non-biofuel demand is not independently forecasted -- it is the residual. This means forecast errors in BBD demand directly translate into non-biofuel demand errors of equal magnitude. Palm oil imports assumed to backfill non-biofuel shortfalls.",
   "palm_oil_substitution": "Jacobsen explicitly assumed palm oil and palm kernel oil imports substitute for fats/greases in animal feed when biofuel demand absorbs supply (noted Q3 2021 CWG report)",
   "source": "jacobsen_q3_2021_through_q1_2022_all_feedstock_reports"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Record Feedstock Price Regime (Q1 2022)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'record_feedstock_prices_q1_2022', 'Record Feedstock Price Environment (Q1 2022)',
 '{"description": "Q1 2022 saw unprecedented feedstock prices driven by South American drought, Russia-Ukraine invasion, and supply chain disruptions. Every major feedstock hit record levels. Volatility exceeded 2008 financial crisis levels for biodiesel prices.", "vintage": "Q1 2022",
   "sbo_prices": {"rbd_chicago_rise_cpb": 14.5, "rbd_chicago_end_cpb": 87.94, "crude_chicago_rise_cpb": 12.5, "crude_chicago_end_cpb": 73.94, "refining_margin_rise_cpb": 2, "refining_margin_end_cpb": 14, "gulf_degummed_rise_cpb": 15.5, "gulf_degummed_end_cpb": 75.94, "futures_record_cpb": 78.58, "futures_record_date": "mid-March 2022"},
   "canola_prices": {"la_rise_cpb": 9.5, "la_end_cpb": 97.94},
   "biofuel_prices": {"rd_low_q4_2021_per_gal": 2.0, "rd_record_q1_2022_per_gal": 4.44, "biodiesel_incl_credits_low_q4_per_gal": 5.73, "biodiesel_incl_credits_record_per_gal": 7.70},
   "credit_prices": {"d4_rin_q4_avg_per_gal": 1.48, "d4_rin_q1_avg_per_gal": 1.43, "lcfs_q4_avg_per_mt": 155, "lcfs_q1_avg_per_mt": 137},
   "volatility_context": "Biodiesel price volatility more than doubled from Q4 2021 and was double the maximum quarterly volatility during 2008 financial crisis",
   "rd_cost_structure": {"feedstock_mix": "50% tallow + 25% DCO + 25% UCO", "total_cost_q4_avg_per_gal": 5.11, "total_cost_q1_avg_per_gal": 5.70},
   "catalysts": ["South American drought (soybean yield cuts)", "Russia invasion of Ukraine (sunflower oil supply loss)", "Supply chain disruptions", "Record biofuel production requiring record feedstock volumes"],
   "source": "jacobsen_q1_2022_executive_summary"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Soybean Acreage Threshold Model (Q4 2021 - Q1 2022)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'soybean_acreage_threshold_model', 'Soybean Acreage Expansion Threshold Analysis',
 '{"description": "Framework linking feedstock demand growth to required soybean acreage. Emerged Q4 2021 when South American drought highlighted supply risk. Crystallized Q1 2022 when USDA Prospective Plantings showed record 91M acres -- still short of requirement.", "vintage": "Q4 2021 - Q1 2022",
   "q4_2021_framework": {"minimum_increase_acres_m": 1, "if_minimum_not_met": "US soybean stocks fall to pipeline levels, prices sharply higher", "if_sa_exports_fall_short": "Need 5M+ additional acres", "fertilizer_impact": "Record fertilizer prices historically favor soybeans over corn but anecdotal reports suggest farmers already committed to corn", "extreme_scenario": "Soybean prices up to $20/bu, feedstock prices to record highs"},
   "q1_2022_framework": {"usda_prospective_plantings_m_acres": 91, "prospective_yoy_increase_m_acres": 3.8, "jacobsen_10yr_avg_requirement_m_acres": 94, "context": "Only 2nd time in history farmers planned more soy than corn", "gap_assessment": "Still a substantial gap between Jacobsen expectations and USDA prediction"},
   "key_insight": "Soybean acreage requirement is not just a production question -- it is a structural constraint on the entire BBD industry. If crush capacity expands but acreage does not follow, SBO prices must rise to ration demand.",
   "source": "jacobsen_q4_2021_veg_oils_summary_and_q1_2022_veg_oil_summary"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;


-- ============================================================================
-- 2. EDGES: Causal relationships showing framework evolution
-- ============================================================================

-- Co-processing collapse drives RD feedstock demand higher
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'coprocessing_assumption_collapse'),
 (SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'CAUSES', 0.90,
 '{"mechanism": "Slashing co-processing from 50% to <3% of BBD capacity shifted all that volume to standalone RD production. Since RD feedstock mix uses more fats/greases than co-processors (which used more SBO), this shifted demand toward low-CI feedstocks. Net effect: substantial increase in total veg oil demand because SAF volumes (introduced same quarter) more than offset the reduction.", "direction": "demand_increasing_for_rd", "source": "jacobsen_q3_2021_executive_summary"}'::jsonb,
 'extracted', 0.90);

-- SAF introduction drives import requirement
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'saf_forecast_introduction_q3_2021'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'CAUSES', 0.85,
 '{"mechanism": "SAF introduction in Q3 2021 forecasts drove substantial increase in vegetable oil demand. Combined with co-processing cut, raised Jacobsen SBO import prediction by 1.8-4.8B lbs/yr (2022-2027). US became potential net SBO importer in late 2021 (visible in Census data). Triggered focus on 19% SBO import tariff as barrier.", "direction": "demand_increasing", "import_increase_range_blbs": [1.8, 4.8], "source": "jacobsen_q3_2021_veg_oil_summary"}'::jsonb,
 'extracted', 0.85);

-- SBO refining shortage triggers crush capacity expansion arc
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'sbo_refining_shortage_q4_2021'),
 (SELECT id FROM core.kg_node WHERE node_key = 'crush_capacity_expansion_arc'),
 'CAUSES', 0.95,
 '{"mechanism": "Record refining margins in Q3-Q4 2021 drove wave of crusher and refining expansion announcements. Capacity targets ratcheted up each quarter: Q4 2021 (2.48B bu by 2025) -> Q1 2022 (+470M bu over 4 years) -> Q2 2022 (2.6B bu by 2027). The refining shortage was the catalytic event for the entire expansion cycle.", "source": "jacobsen_q4_2021_veg_oils_summary"}'::jsonb,
 'extracted', 0.95);

-- RVO uncertainty affected biodiesel production forecasts
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'rvo_uncertainty_period_2021'),
 (SELECT id FROM core.kg_node WHERE node_key = 'biodiesel_decline_model_q4_2021'),
 'CAUSES', 0.80,
 '{"mechanism": "Uncertainty about 2020-2022 RVOs made The Jacobsen cautious about BBD demand forecasts in Q3 2021. Once preliminary mandates were released (Dec 2021) showing higher-than-expected total RVO, it became clear ethanol blend wall would support additional BBD demand. However, Q3-Q4 2021 reports cut biodiesel projections without this support mechanism.", "source": "jacobsen_q3_q4_2021_executive_summaries"}'::jsonb,
 'extracted', 0.80);

-- Tallow import revision driven by world trade flow model
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'tallow_import_revision_q1_2022'),
 (SELECT id FROM core.kg_node WHERE node_key = 'tallow'),
 'SUPPLIES', 0.85,
 '{"mechanism": "New world tallow production and trade flow model raised US import forecasts by avg 1B lbs/yr. Imports allocated primarily to biofuel feedstock demand (avg +1.5B lbs/yr). Non-biofuel and export demand reduced. This reversed Q4 2021 which had cautiously kept import forecasts mostly unchanged despite strong actual imports.", "direction": "supply_increasing", "source": "jacobsen_q1_2022_bft_changes"}'::jsonb,
 'extracted', 0.85);

-- UCO/YG model overhaul drives RD feedstock allocation
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'uco_yg_model_overhaul_q1_2022'),
 (SELECT id FROM core.kg_node WHERE node_key = 'used_cooking_oil'),
 'PREDICTS', 0.75,
 '{"mechanism": "Q1 2022 model overhaul raised UCO+YG production by avg 1.4B lbs/yr. Surplus allocated to RD feedstock demand (+1.7B lbs/yr). Biodiesel UCO/YG use cut 225M lbs/yr. This was partially reversed by Q2 2022 when production was cut back by 1.1B lbs and non-biofuel demand went negative. Model remained highly uncertain throughout.", "direction": "production_increasing_then_revised_down", "source": "jacobsen_q1_2022_uco_yg_changes"}'::jsonb,
 'extracted', 0.75);

-- Non-biofuel demand concept links to feedstock supply chain
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'nonbiofuel_demand_swing_variable'),
 (SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'EXTENDS', 0.90,
 '{"mechanism": "Non-biofuel demand acts as the swing variable (residual) in every feedstock balance sheet. This modeling approach means: (1) forecast errors in BBD demand translate directly to non-biofuel errors, (2) palm oil import substitution assumed for animal feed shortfalls, (3) total demand adjusts to match available supply. Understanding this is critical for interpreting any Jacobsen balance sheet forecast.", "source": "jacobsen_q3_2021_through_q2_2022"}'::jsonb,
 'extracted', 0.90);

-- Record prices link to crush capacity expansion
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'record_feedstock_prices_q1_2022'),
 (SELECT id FROM core.kg_node WHERE node_key = 'crush_capacity_expansion_arc'),
 'CAUSES', 0.85,
 '{"mechanism": "Record SBO and feedstock prices in Q1 2022 (SBO futures hit 78.58 cpb, biodiesel volatility exceeded 2008 crisis) directly incentivized crusher expansion. Record refining margins made pretreatment and crush capex economically attractive. Also drove record soybean plantings (91M acres) which provided the supply base for expanded crush.", "source": "jacobsen_q1_2022_executive_summary_and_veg_oil_summary"}'::jsonb,
 'extracted', 0.85);

-- SBO import tariff constrains import response
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'sbo_import_tariff_19pct'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'CONSTRAINS', 0.80,
 '{"mechanism": "19.1% tariff on SBO imports prevents the import response needed to fill the domestic supply gap created by biofuel demand growth. Even with record domestic prices, substantial imports unlikely through end 2022. Alternative path: import Canadian renewable diesel (no SBO tariff) instead of importing SBO to make domestic biofuel.", "direction": "supply_constraining", "source": "jacobsen_q3_2021_veg_oil_summary"}'::jsonb,
 'extracted', 0.80);

-- Acreage model links to SBO supply constraint
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_acreage_threshold_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'sbo_crush_capacity_model_q2_2022'),
 'CONSTRAINS', 0.85,
 '{"mechanism": "Even if crush capacity expands, acreage must follow. Q4 2021: need +1M acres minimum or stocks fall to pipeline. Q1 2022: need 94M acres avg over 10 years (only 2nd time ever >corn). If acreage does not keep pace with crush expansion, SBO prices must rise to ration demand.", "source": "jacobsen_q4_2021_and_q1_2022_veg_oil_summaries"}'::jsonb,
 'extracted', 0.85);

-- Biodiesel decline model extends BBD capacity model
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'biodiesel_decline_model_q4_2021'),
 (SELECT id FROM core.kg_node WHERE node_key = 'bbd_capacity_expansion_q2_2022'),
 'PRECEDES', 0.85,
 '{"mechanism": "Q4 2021 biodiesel decline framework was the precursor to Q2 2022s more refined rationalization model. Q4 2021 lacked the ethanol blend wall support mechanism that Q2 2022 identified. The evolution: Q3 2021 (first major cuts) -> Q4 2021 (deepened cuts, no offsetting mechanism) -> Q2 2022 (blend wall + supplemental mandate = delayed rationalization until 2024).", "source": "jacobsen_q4_2021_and_q2_2022_executive_summaries"}'::jsonb,
 'extracted', 0.85);


-- ============================================================================
-- 3. CONTEXTS: Expert rules, evolving frameworks, analytical patterns
-- ============================================================================

-- Fat/Grease Price Feedback Loop (Q3 2021 - Q1 2022)
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'expert_rule', 'fat_grease_price_feedback_loop_q3_q1',
 '{"rule": "Across Q3 2021 through Q1 2022, fat and grease prices rose from record levels to higher record levels while RIN and LCFS credit prices fell. This divergent behavior revealed that: (1) Credit prices respond to supply growth faster than feedstock prices, (2) Feedstock prices are stickier because supply is relatively fixed and biofuel demand is structurally growing, (3) Feedstock costs become the margin squeeze mechanism rather than credit price declines. By Q1 2022, even though margins were lower, absolute prices were at records.", "q3_2021_dynamics": "Fat/grease prices continued rising until early September even as RIN and LCFS credits fell from June highs. Energy price rally to 2018 highs improved biofuel profitability in relative but not absolute terms.", "q4_2021_dynamics": "Fat/grease price record levels drove 12-18 cpb increases in price forecasts across UCO, YG, CWG. Non-biofuel demand price inelasticity noted as contributor.", "q1_2022_dynamics": "Volatility exceeded 2008 financial crisis. Russia/Ukraine + South America drought pushed SBO futures to 78.58 cpb record. Every major feedstock hit record levels. Feedstock costs for RD rose from $5.11 to $5.70/gal.", "predictive_value": "When credit prices fall but feedstock prices rise, it signals structural feedstock shortage. Credit price declines alone do not reliably forecast feedstock price direction.", "source": "jacobsen_q3_2021_through_q1_2022_executive_summaries"}'::jsonb,
 'always', 'extracted');

-- LCFS Credit Price Decline Pattern (Q3 2021 - Q1 2022)
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'lcfs_credit_framework'),
 'expert_rule', 'lcfs_credit_decline_trajectory_q3_q1',
 '{"rule": "LCFS credit prices declined continuously from Q3 2021 through Q1 2022, driven by record renewable diesel production overwhelming deficit generation. The trajectory: Q3 2021 trend lower -> Q4 2021 stabilized ~$150/MT then fell to $149 -> Q1 2022 dropped to $137 -> Q2 2022 fell to $81. Credit generation led by RD grew from 250M gal/qtr (Q3 2021) to ~300M gal (Q4 2021, nearly double YoY). RD production record of 735M gal total BBD in Q4 2021 was the primary driver.", "credit_generation_q3_2021_mgal_rd": 250, "credit_generation_q4_2021_mgal_rd": 300, "total_bbd_q4_2021_record_mgal": 735, "price_trajectory": {"q3_2021": "trending lower", "q4_2021_start": 175.5, "q4_2021_end": 149.25, "q1_2022_avg": 137, "q2_2022_avg": 81}, "key_insight": "Credit generation was predictable -- The Jacobsen warned in Q3 2021 that credit generation could overwhelm deficit generation. The question was at what LCFS price level RD producers would divert shipments to non-CA markets. Oregon LCFS-like program noted as alternative destination.", "producer_response": "Jacobsen expected producers to divert to Oregon and other LCFS-like markets. If they did, credit generation and prices would stabilize. If not, prices could fall well below forecast.", "source": "jacobsen_q3_2021_credit_generation_summary_and_q4_q1_executive_summaries"}'::jsonb,
 'always', 'extracted');

-- Canola Oil Pathway Delay Pattern
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 'expert_rule', 'canola_rd_pathway_delay_pattern',
 '{"rule": "The EPA pathway for canola oil in renewable diesel production experienced repeated delays. Q3 2021: expected late 2021 or early 2022. Q4 2021: expected first half of 2022. Q1 2022: EPA still had not announced. Q2 2022: proposed rule published April 18, 2022. Canola oil use in biodiesel was stable but could not grow into RD market without pathway. Jacobsen believed pathway was inevitable because RD industry could not source sufficient feedstocks without it.", "timeline": {"q3_2021_expected": "late 2021 or early 2022", "q4_2021_expected": "first half of 2022", "q1_2022_status": "still not announced; usage dropped 14% below 5yr avg", "q2_2022_proposed_rule": "April 18, 2022"}, "impact_without_pathway": "Canola oil confined to biodiesel production. RD demand for canola = zero. Canola price would decline and spread to SBO would narrow, accelerating food industry substitution of canola for SBO.", "geographic_concentration": "Jacobsen expected canola oil biofuel use almost exclusively on West Coast of North America", "reporting_change": "Q3 2021: switched price series from Chicago to Los Angeles due to deterioration in Chicago reporting sources", "source": "jacobsen_q3_2021_through_q1_2022_canola_reports"}'::jsonb,
 'always', 'extracted');

-- Poultry Fat RD Investigation Pattern
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'poultry_fat'),
 'expert_rule', 'poultry_fat_rd_investigation_q3_2021',
 '{"rule": "Q3 2021: Jacobsen began investigating anecdotal reports that some RD producers were using poultry fat as feedstock. If confirmed, would be included in next quarters report. Q4 2021: no confirmation; investigation continued. By Q2 2022: <20M lbs/yr allocated to RD demand. Key takeaway: poultry fat remained a marginal BBD feedstock throughout. The delay in confirmation reflects how slowly BBD feedstock diversification occurred vs expectations.", "q3_2021_note": "Investigating anecdotal reports. If confirmed, would reduce existing biodiesel allocation rather than cut non-biofuel use.", "q1_2022_complication": "Bird flu in spring 2022 resulted in widespread poultry culling. Not yet in models but noted as downside risk. Production forecast based on strong YoY slaughter growth that pre-dated bird flu.", "production_q1_2022_increase_avg_mlbs": 600, "bird_flu_risk": "Historical data in model did not account for bird flu impact. Subsequent reports likely to reduce poultry fat production.", "source": "jacobsen_q3_2021_through_q1_2022_poultry_fat_reports"}'::jsonb,
 'always', 'extracted');

-- South American Drought Supply Shock Sequence
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'expert_rule', 'south_american_drought_supply_sequence_2021_2022',
 '{"rule": "Sequential droughts created compounding vegetable oil supply pressure: (1) 2020 Black Sea drought reduced sunflower oil, (2) Q4 2021 South American drought cut soybean yields -- followed North American drought that had already cut canola production. Q1 2022 added Russia/Ukraine war removing further sunflower oil supply. Each shock layered on an already-tight world vegetable oil balance.", "sequence": [{"period": "2020", "event": "Black Sea drought", "impact": "Reduced world sunflower oil supply"}, {"period": "Q3-Q4 2021", "event": "North American drought (Plains + Canadian Prairies)", "impact": "Cut canola production substantially. Shortfall in veg oil supplies increased impact of RD expansion on prices."}, {"period": "Q4 2021 - Q1 2022", "event": "South American drought", "impact": "Substantially cut soybean yields and potential SBO supply. Combined with N American drought created multi-year veg oil deficit."}, {"period": "Q1 2022", "event": "Russia/Ukraine invasion", "impact": "Removed Ukrainian sunflower oil supplies from already-tight world market. SBO futures hit record 78.58 cpb."}], "price_implication_q4_2021": "If farmers dont increase soy acreage 5M+ acres, soybean prices could reach $20/bu", "source": "jacobsen_q4_2021_and_q1_2022_executive_summaries"}'::jsonb,
 'always', 'extracted');

-- Biodiesel to RD Production Transition Metrics
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'bbd_balance_sheet_model'),
 'market_indicator', 'bbd_production_transition_metrics_q3_q1',
 '{"rule": "Track RD vs biodiesel production quarterly to gauge industry transition pace. Q3 2021: no specific volumes given but RD expanding while biodiesel declining. Q4 2021: total BBD record 735M gal (RD not split out but substantially above YoY). Q1 2022: total BBD 635M gal (RD = 416M, biodiesel = 337M). RD rose 49% YoY (280M -> 416M). Biodiesel fell 8% (367M -> 337M). By Q2 2022: RD doubled YoY to 359M/qtr.", "quarterly_data": {"q4_2021": {"total_bbd_mgal": 735, "yoy_change": "up from 648 in Q3 and 600 in Q4 2020", "sbo_use_record_blbs": 2.66}, "q1_2022": {"total_bbd_mgal": 635, "rd_mgal": 416, "biodiesel_mgal": 337, "rd_yoy_pct": 49, "biodiesel_yoy_pct": -8, "sbo_use_blbs": 2.44}}, "sbo_use_pattern": "SBO use peaked Q3 2021 at 2.65B lbs, declined to 2.44B lbs by Q1 2022 despite record overall BBD production. Reflects feedstock mix shift toward fats/greases.", "canola_pattern": "Canola oil declining in biodiesel (238M lbs Q1 2022 vs 248M YoY, -14% vs 5yr avg) due to prices and lack of RD pathway.", "source": "jacobsen_q4_2021_and_q1_2022_executive_summaries"}'::jsonb,
 'always', 'extracted');

-- Price Forecast Methodology Observation
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'expert_rule', 'price_forecast_baseline_shift_pattern',
 '{"rule": "Across all quarters, The Jacobsen price forecast methodology uses current prices as baseline, then projects forward based on S&D balance. This means: (1) when current prices hit records (Q1 2022), all near-term forecasts shift up significantly even if S&D is unchanged, (2) forecast revisions in the near-term are dominated by baseline effects, not S&D changes, (3) long-term forecasts are more reflective of actual S&D analysis. The pattern was visible in every feedstock: Q4 2021 raised near-term prices from record fat/grease values, Q1 2022 raised all forecasts further as records were broken.", "examples": {"q4_2021_uco_price_rise_avg_cpb": 18, "q4_2021_yg_price_rise_avg_cpb": 12, "q4_2021_cwg_price_rise_avg_cpb": 11, "q4_2021_tallow_near_term_rise_cpb": 5, "q1_2022_pattern": "Record prices in Q1 raised baseline for all forecasts. Near-term rises of 17+ cpb, mid-forecast modest, final 2 years +30-35 cpb from tight supply/demand"}, "analytical_implication": "When evaluating forecast changes, separate baseline effects (current price shifted) from fundamental changes (S&D revision). Only long-term forecast changes reflect genuine outlook evolution.", "sbo_import_price_note": "SBO price changes larger per unit of import forecast change than per unit of other balance sheet items. This is because imports are modeled as the most price-elastic component.", "source": "jacobsen_q3_2021_through_q1_2022_all_feedstock_price_forecasts"}'::jsonb,
 'always', 'extracted');

-- RFS Post-Expiration Framework
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'rfs2'),
 'expert_rule', 'rfs_post_expiration_q1_2022',
 '{"rule": "RFS law expired at end of 2022. Q1 2022 was the first quarter addressing the structural question of post-expiration mandates. Key insights: (1) 2023 will be first year EPA sets RVOs without statutory requirements, (2) EPA planned Set Rule for early FY2023 (Oct 2022), (3) Trade group sued to force consent decree requiring 2023 RVOs by Sept 16, 2022 and final by Apr 28, 2023, (4) EPA FY2023 budget indicated no wholesale RFS changes in 2023, (5) Biden likely to make SAF the centerpiece of post-2022 RFS, either through revised RFS volumes or separate SAF program mimicking LCFS CI-based approach.", "saf_policy_direction": "Administration moving toward SAF-centric plan would put BBD industry on more sustainable long-term path than current RFS which relies on ground transportation demand", "near_term_certainty": "No wholesale changes to RFS in 2023 based on EPA budget", "key_question": "Whether SAF program will be within RFS or separate CI-based program like LCFS", "source": "jacobsen_q1_2022_executive_summary"}'::jsonb,
 'always', 'extracted');


-- ============================================================================
-- 4. SOURCE REGISTRY: Register all processed documents
-- ============================================================================

-- Q1 2022 Sources
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('jacobsen_q1_2022_executive_summary', 'local_file',
 'Q1 2022 Executive Summary',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q1 2022/Executive Summary - Q1 2022 - 06142022.docx',
 '2022-06-14', 'quarterly_outlook',
 '{biodiesel,renewable_diesel,soybean_oil,tallow,uco,dco,ethanol}',
 '{record_prices,ukraine_invasion,sa_drought,rfs_post_expiration,saf_policy,feedstock_volatility,rd_plant_delays,bbd_margins,rvo_mandates}',
 'completed', NOW(), NOW(), 3, 3, 1)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('jacobsen_q1_2022_veg_oil_summary', 'local_file',
 'Q1 2022 Veg Oil Summary',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q1 2022/Veg Oil Summary - Q1 2022 - 06142022.docx',
 '2022-06-14', 'quarterly_outlook',
 '{soybean_oil,canola_oil}',
 '{sbo_record_prices,rbd_refining_margin,crush_capacity_expansion,soybean_acreage,prospective_plantings,canola_pathway}',
 'completed', NOW(), NOW(), 1, 1, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('jacobsen_q1_2022_bft_changes', 'local_file',
 'Q1 2022 Tallow (BFT) Changes',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q1 2022/BFT - Q1 2022 - 06152022.docx',
 '2022-06-15', 'quarterly_outlook',
 '{tallow}',
 '{tallow_imports,world_tallow_model,biofuel_demand,coprocessing_demand,tallow_price_forecast}',
 'completed', NOW(), NOW(), 1, 1, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('jacobsen_q1_2022_uco_yg_changes', 'local_file',
 'Q1 2022 UCO-YG Changes',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q1 2022/UCO-YG Changes - Q1 2022 - 06152022.docx',
 '2022-06-15', 'quarterly_outlook',
 '{used_cooking_oil,yellow_grease}',
 '{uco_production_model,inflation_adjustment,rd_feedstock_demand,biodiesel_mix_shift}',
 'completed', NOW(), NOW(), 1, 1, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('jacobsen_q1_2022_sbo_changes', 'local_file',
 'Q1 2022 SBO Changes',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q1 2022/SBO Changes - Q1 2022 - 06152022.docx',
 '2022-06-15', 'quarterly_outlook',
 '{soybean_oil}',
 '{sbo_crush_expansion,sbo_import_forecast,sbo_biofuel_demand,non_biofuel_use,sbo_price_forecast}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('jacobsen_q1_2022_co_changes', 'local_file',
 'Q1 2022 Canola Oil Changes',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q1 2022/CO Changes - Q1 2022 - 06142022.docx',
 '2022-06-14', 'quarterly_outlook',
 '{canola_oil}',
 '{canola_supply,canola_biofuel_demand,canola_non_biofuel,canola_price_forecast,canada_biofuel_program}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('jacobsen_q1_2022_other_changes', 'local_file',
 'Q1 2022 CWG/DCO/PF/FatGrease/VegOil Changes (6 files)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q1 2022/',
 '2022-06-15', 'quarterly_outlook',
 '{choice_white_grease,distillers_corn_oil,poultry_fat,soybean_oil,canola_oil,tallow,uco,yellow_grease}',
 '{cwg_demand,dco_yield,pf_bird_flu,fat_grease_aggregate,veg_oil_aggregate,crush_capacity,rd_imports}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('jacobsen_q1_2022_lcfs_reports', 'local_file',
 'Q1 2022 LCFS CI Scores and Credit Generation (2 files)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q1 2022/',
 '2022-06-16', 'quarterly_outlook',
 '{renewable_diesel,biodiesel,ethanol}',
 '{lcfs_credits,lcfs_deficits,ci_scores,credit_generation,deficit_generation,credit_bank,sbo_lcfs_share,fats_greases_lcfs_share}',
 'completed', NOW(), NOW(), 0, 0, 1)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- Q4 2021 Sources
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('jacobsen_q4_2021_executive_summary', 'local_file',
 'Q4 2021 Executive Summary',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q4 2021/Executive Summary - Q4 2021.docx',
 '2022-03-15', 'quarterly_outlook',
 '{biodiesel,renewable_diesel,soybean_oil,ethanol}',
 '{rvo_mandates,rfs_preliminary,ethanol_blend_wall,lcfs_credit_decline,rd_production_record,sre_policy,bbd_margins}',
 'completed', NOW(), NOW(), 2, 1, 1)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('jacobsen_q4_2021_veg_oils_summary', 'local_file',
 'Q4 2021 Veg Oils Summary',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q4 2021/Veg Oils Summary - Q4 2021.docx',
 '2022-03-15', 'quarterly_outlook',
 '{soybean_oil,canola_oil}',
 '{sbo_refining_shortage,crush_capacity,canola_drought,sa_drought,soybean_acreage,fertilizer_prices}',
 'completed', NOW(), NOW(), 2, 1, 1)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('jacobsen_q4_2021_feedstock_changes', 'local_file',
 'Q4 2021 All Feedstock Changes (12 files)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q4 2021/',
 '2022-03-15', 'quarterly_outlook',
 '{soybean_oil,canola_oil,tallow,choice_white_grease,distillers_corn_oil,used_cooking_oil,yellow_grease,poultry_fat}',
 '{biodiesel_cuts,non_biofuel_offset,tallow_production_cut,canola_rd_pathway,yg_production_increase,uco_import_cut,sbo_crush_reduction}',
 'completed', NOW(), NOW(), 1, 1, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('jacobsen_q4_2021_lcfs_summary', 'local_file',
 'Q4 2021 LCFS Summary',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q4 2021/LCFS Summary - Q4 2021.docx',
 '2022-03-15', 'quarterly_outlook',
 '{renewable_diesel,biodiesel}',
 '{lcfs_credit_decline,rd_production_record,credit_generation}',
 'completed', NOW(), NOW(), 0, 0, 1)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- Q3 2021 Sources
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('jacobsen_q3_2021_executive_summary', 'local_file',
 'Q3 2021 Executive Summary',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q3 2021/Executive Summary Q3 2021.docx',
 '2021-12-15', 'quarterly_outlook',
 '{biodiesel,renewable_diesel,ethanol,soybean_oil}',
 '{coprocessing_cut,saf_introduction,rvo_uncertainty,mandate_adjustment,credit_price_decline}',
 'completed', NOW(), NOW(), 2, 1, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('jacobsen_q3_2021_veg_oil_summary', 'local_file',
 'Q3 2021 Veg Oil Exec Summary',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q3 2021/Veg Oil Exec Summary Q3 2021.docx',
 '2021-12-15', 'quarterly_outlook',
 '{soybean_oil,canola_oil}',
 '{sbo_imports,sbo_tariff,crush_rate_change,saf_demand,coprocessing_feedstock_shift,sbo_export_decline,canola_pathway}',
 'completed', NOW(), NOW(), 2, 2, 1)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('jacobsen_q3_2021_feedstock_reports', 'local_file',
 'Q3 2021 Feedstock Reports (7 files: CWG, Canola, Credit Gen, DCO, PF, Tallow, UCO-YG)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q3 2021/',
 '2021-12-15', 'quarterly_outlook',
 '{choice_white_grease,canola_oil,distillers_corn_oil,poultry_fat,tallow,used_cooking_oil,yellow_grease}',
 '{cwg_rd_demand,canola_price_la,dco_ethanol_production_cut,pf_rd_investigation,tallow_non_biofuel_strong,uco_yg_production_increase,lcfs_credit_overwhelm_risk}',
 'completed', NOW(), NOW(), 0, 0, 2)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;


-- ============================================================================
-- BATCH STATISTICS
-- ============================================================================
-- Nodes:    11 (5 models, 2 data_series, 1 concept, 1 policy, 1 model, 1 model)
-- Edges:    12 (CAUSES, SUPPLIES, PREDICTS, EXTENDS, CONSTRAINS, PRECEDES)
-- Contexts:  8 (expert_rules, market_indicators)
-- Sources:  14 (covering 34 individual .docx files across 3 quarters)
-- Documents processed: 14 Q1-2022, 12 Q4-2021, 9 Q3-2021 = 35 total .docx files
-- Quarters not processed (no .docx): 2021 Q2 (PDF only), 2021 Q1 (PDF only),
--   Current/Cleared for Sales (PDF only), 2020 Q4 (PPTX only), 2020 Q3 (PDF only)
-- Links to existing KG: renewable_diesel, soybean_oil, tallow, used_cooking_oil,
--   canola_oil, poultry_fat, feedstock_supply_chain_model, bbd_balance_sheet_model,
--   bbd_capacity_expansion_q2_2022, sbo_crush_capacity_model_q2_2022,
--   lcfs_credit_framework, rfs2, choice_white_grease, distillers_corn_oil
