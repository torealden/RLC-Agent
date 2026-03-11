-- ============================================================
-- Knowledge Graph Extraction: Batch 009c
-- Source: HOBO Study - Section 2: RD and SAF Policy and Regulatory Drivers
-- Date: 2026-02-14
-- ============================================================

INSERT INTO kg_source (source_key, source_type, title, url, report_date, category, commodities, themes, status, first_seen, last_processed, node_count, edge_count, context_count)
VALUES
('gdoc_hobo_section2', 'gdrive_doc', 'HOBO Study - Section 2: Policy and Regulatory Drivers', 'https://docs.google.com/document/d/1qmE3QSUbzbx0tKDHvzzlsEIa154dPaVaZtb2tO5v5Q0/edit', '2025-06-19', 'consulting_report', '{d4_rin,d5_rin,d6_rin,d3_rin,renewable_diesel,saf}', '{rfs,lcfs,45z,refueleu,cfr,rvo,state_incentives,policy_scenarios}', 'completed', NOW(), NOW(), 12, 10, 8)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

-- ============================================================
-- NODES
-- ============================================================

INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('rfs_d4_rin', 'policy', 'D4 RIN (Biomass-Based Diesel)',
 'RFS category for biofuels replacing diesel/heating oil/jet meeting 50% GHG reduction. Fulfilled by biodiesel, RD, and SAF. Key credit for HOBO. Each BBD gallon generates ~1.7 D4 RINs due to energy equivalence. RVO growing from 2.43B gal (2020) to proposed 7.12B RINs (2026).',
 '{"ghg_threshold_pct": 50, "rin_per_gal": 1.7, "rvo_2020_bgal": 2.43, "rvo_2025_bgal": 3.35, "rvo_2026_proposed_brins": 7.12}'),
('rfs_d6_rin', 'policy', 'D6 RIN (Conventional Renewable/Ethanol)',
 'RFS category for corn ethanol at 20% GHG reduction. Flat at 15B RINs since 2022 due to E10 blendwall. Structural ceiling on ethanol growth is key reason EPA/Congress pivoting to BBD growth.',
 '{"ghg_threshold_pct": 20, "volume_brins": 15.0, "constraint": "E10_blendwall", "trend": "flat"}'),
('e10_blendwall', 'constraint', 'E10 Blendwall',
 'Technical/infrastructure limit constraining ethanol blending in US gasoline at ~10%. Combined with flat gasoline demand, this effectively caps corn ethanol volumes, forcing RFS growth into advanced biofuels (D4/D5) to meet increasing total renewable fuel targets.',
 '{"blend_limit_pct": 10, "gasoline_demand_trend": "flat_to_declining"}'),
('rvo_2026_proposal', 'policy', 'EPA 2026/27 RVO Proposal',
 'EPA proposed June 2025: BBD target of 7.12B RINs for 2026 (vs 3.35B gal in 2025). Also proposes halving RIN generation from imported biofuels/imported feedstocks, and removing eRINs. Finalization expected Dec 2025.',
 '{"bbd_2026_brins": 7.12, "import_rin_reduction": "50%", "erins": "removed", "status": "proposed"}'),
('state_saf_incentives', 'policy', 'US State SAF Incentives',
 'Multiple states offering SAF tax credits: Illinois $1.50/gal (2023-2032), Washington $1.0-$2.0/gal CI-escalated, Minnesota $1.50/gal (2024-2030), Nebraska $0.75-$1.25/gal (2027-2035), Michigan proposed $2.0/gal.',
 '{"illinois_per_gal": 1.50, "washington_max_per_gal": 2.00, "minnesota_per_gal": 1.50}'),
('state_rd_incentives', 'policy', 'US State RD Incentives',
 'Multiple states with RD tax credits/exemptions: Iowa PTC 4cts/gal + BTC up to 10cts/gal, Montana 29.95cts/gal tax exemption for UCO-based, Kentucky $1/gal (capped), Texas 20cts/gal exemption, Illinois 6.25% sales tax exemption.',
 '{"iowa_ptc_per_gal": 0.04, "montana_exemption_per_gal": 0.2995, "kentucky_per_gal": 1.00}'),
('canada_cfr', 'policy', 'Canada Clean Fuel Regulations (CFR)',
 'Established 2023, similar to LCFS. Mandates gasoline/fuel suppliers to reduce CI by ~15% by 2030. Credit prices ~$100/te historically. Provincial programs (BC 30% target, $200-300/te credits) are more aggressive.',
 '{"target_2030_pct": 15, "credit_price_cad_te": 100, "bc_target_pct": 30, "bc_credit_range": "$200-300/te"}'),
('china_uco_role', 'market', 'China UCO Export Market',
 'China is key UCO exporter to US and Europe with large untapped collection potential. Role at risk from: (1) US-China tariffs, (2) UCO providence/adulteration concerns, (3) Chinese domestic SAF market development (considering 15% mandate by 2030 = ~2.9B gal SAF demand). Could shift from supplier to competitor.',
 '{"us_trade_risk": "tariffs", "quality_risk": "adulteration", "domestic_saf_mandate_2030_pct": 15, "domestic_saf_demand_bgal": 2.9}'),
('eu_red_iii', 'policy', 'EU Renewable Energy Directive III (RED III)',
 'Primary EU biofuel legislation. 29% renewables in transport by 2030 or 14.5% CI reduction. Food-based feedstock capped at 7% or 2020 level. Palm oil phased out by 2030. Advanced feedstocks: Part A (cellulosic/MSW) min 5.5%, Part B (UCO/tallow) capped at 1.7%.',
 '{"transport_target_2030_pct": 29, "food_cap_pct": 7, "palm_phaseout": 2030, "advanced_min_pct": 5.5, "part_b_cap_pct": 1.7}'),
('book_and_claim', 'policy', 'Book-and-Claim Accounting for SAF',
 'System allowing SAF produced at one location to be credited to airline elsewhere without physical delivery. ICAO CORSIA leaning toward allowing this. Could facilitate more SAF credit generation by removing physical logistics hurdles. US/EU coordination critical.',
 '{"status": "under_development", "icao_leaning": "favorable", "benefit": "removes_logistics_barriers"}'),
('saf_grand_challenge', 'policy', 'US SAF Grand Challenge',
 'Biden-era target of 3B gal SAF by 2030, 35B gal by 2050 (100% of US demand). Renamed "Synthetic Aviation Fuel Grand Challenge" under Trump but not withdrawn. Underlying fundamentals and airline support remain.',
 '{"target_2030_bgal": 3, "target_2050_bgal": 35, "status": "renamed_not_withdrawn"}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- ============================================================
-- EDGES
-- ============================================================

INSERT INTO kg_edge (source_node, target_node, edge_type, label, weight, metadata)
VALUES
('e10_blendwall', 'rfs_d6_rin', 'CONSTRAINS', 'Blendwall caps corn ethanol at ~15B gal, forcing all RFS growth into advanced biofuels (D4/D5)', 0.95,
 '{"mechanism": "volume_ceiling", "confidence": "very_high"}'),
('e10_blendwall', 'rfs_d4_rin', 'DRIVES', 'Ethanol ceiling forces EPA to grow BBD/Advanced RVO to meet total renewable fuel targets', 0.90,
 '{"mechanism": "RFS_growth_displacement", "confidence": "very_high"}'),
('cfpc_45z', 'hobo_renewables', 'ENABLES', '45Z provides up to $1.75/gal for SAF, critical for HOBO SAF economics. Extension to 2031 + domestic feedstock restriction would favor HOBO', 0.90,
 '{"mechanism": "production_tax_credit", "confidence": "high"}'),
('state_saf_incentives', 'hobo_renewables', 'ADVANTAGES', 'Illinois $1.50/gal SAF credit + Chicago airport proximity creates high-value market for HOBO SAF', 0.85,
 '{"mechanism": "state_incentive_stacking", "key_market": "Chicago_ORD_MDW"}'),
('china_uco_role', 'us_feedstock_imports', 'THREATENS', 'China tariffs + domestic SAF mandate could redirect UCO supply away from US, tightening domestic waste oil markets', 0.85,
 '{"risk_factors": ["tariffs", "domestic_saf_mandate", "adulteration_concerns"]}'),
('eu_red_iii', 'uco_feedstock', 'CONSTRAINS', 'RED III Part B cap at 1.7% limits UCO/tallow contribution to EU advanced biofuel targets, potentially freeing more for US', 0.75,
 '{"mechanism": "feedstock_cap", "cap_pct": 1.7}'),
('rvo_2026_proposal', 'rfs_d4_rin', 'DRIVES', 'Proposed BBD RVO doubling from 3.35B to 7.12B RINs would massively increase D4 demand and prices', 0.85,
 '{"magnitude": "~2x_increase", "status": "proposed"}'),
('rvo_2026_proposal', 'us_feedstock_imports', 'CONSTRAINS', 'Proposal to halve RIN generation from imported biofuels/feedstocks would tighten domestic feedstock market and favor domestic producers like HOBO', 0.85,
 '{"mechanism": "import_rin_reduction_50pct"}'),
('canada_cfr', 'hobo_renewables', 'ENABLES', 'CFR creates growing pull for RD/SAF imports into Canada; HOBO Midwest location + rail access enables competitive supply', 0.80,
 '{"mechanism": "cfr_credit_value", "confidence": "high"}'),
('book_and_claim', 'saf_market', 'ENABLES', 'Book-and-claim would allow HOBO to produce SAF in Iowa and credit it to airlines at distant airports without physical delivery', 0.70,
 '{"status": "under_development", "confidence": "moderate"}')
ON CONFLICT DO NOTHING;

-- ============================================================
-- CONTEXTS
-- ============================================================

INSERT INTO kg_context (context_key, node_or_edge_ref, context_type, content, confidence, metadata)
VALUES
('rfs_nesting_dynamics', 'rfs_program', 'expert_rule',
 'RFS nesting mechanics are critical: headline BBD mandates understate true BBD demand. D6 is flat (blendwall), so all growth must come from advanced biofuels. The BBD RVO, Advanced RVO, and total renewable fuel RVO create a nested obligation where BBD must fill gaps in conventional (ethanol shortfall), advanced (after D3/D5 netting), and supplemental standards. Understanding this nesting is the key analytical edge in RIN market forecasting.',
 0.95, '{"source": "hobo_section2", "cross_ref": "batch_009_bobs_rvo_analysis", "critical": true}'),

('45z_extension_scenario', 'cfpc_45z', 'policy_scenario',
 'Four critical 45Z scenarios: (1) Extension to 2031 as drafted = bullish for SAF, stabilizes investment; (2) Expiry at 2027 = cliff in SAF incentives, stalling projects; (3) ILUC removal from CI calculation = reduces differentiation between waste and crop feedstocks, directionally increases soybean oil competitiveness; (4) Domestic feedstock restriction = tightens supply for low-CI material, favoring Midwest-located producers with domestic supply access like HOBO. Scenarios 1+3+4 combined = extremely favorable for HOBO.',
 0.90, '{"source": "hobo_section2", "critical": true}'),

('credit_stacking_opportunity', 'hobo_renewables', 'strategic_framework',
 'All US federal and state incentives stack, creating optimization opportunities. For HOBO SAF sold into Illinois airports: 45Z ($1.50-1.75) + D4 RIN ($1.0-1.5) + Illinois state credit ($1.50) + potential LCFS if sold into CA instead. The routing/market optimization decision is worth tens of millions annually. HOBO needs a dedicated trading/optimization desk.',
 0.90, '{"source": "hobo_section2"}'),

('eu_price_setter', 'refueleu_mandate', 'market_structure',
 'EU is likely to become global price setter for SAF. Reasons: mandatory targets with severe penalties (double the SAF-jet price difference, plus volume carryover), short domestic supply, high mandate trajectory (2%→6%→20%). Prices will rise to pull SAF supply from US/Asia. Europe will be short both feedstock AND product. This creates export opportunity for US HEFA producers including HOBO.',
 0.90, '{"source": "hobo_section2", "implication": "export_opportunity"}'),

('china_supply_disruption_risk', 'china_uco_role', 'risk_assessment',
 'China could shift from net UCO exporter to net importer if domestic SAF mandate (15% by 2030 = 2.9B gal) materializes. Combined with US tariffs and 45Z import restrictions, this would: (1) materially tighten global waste oil supply, (2) increase feedstock prices, (3) favor domestic US producers with local UCO access like HOBO, (4) potentially benefit soybean oil as substitute feedstock.',
 0.85, '{"source": "hobo_section2", "cascade_effects": true}'),

('governor_advocacy_signal', 'rvo_2026_proposal', 'political_signal',
 'Iowa, Nebraska, South Dakota, and Missouri governors urged EPA to set 2026 BBD RVO at 5.25B gal — a >1.5x increase vs 2025. This cross-state, bipartisan political pressure on BBD expansion is a strong indicator that the US policy direction strongly favors growing the biomass-based diesel market.',
 0.80, '{"source": "hobo_section2", "signal_type": "bipartisan_advocacy"}'),

('lcfs_legal_resilience', 'california_lcfs', 'policy_assessment',
 'California LCFS has survived multiple federal and legal challenges including the State Commerce Clause and arguments it is preempted by the federal RFS. April 2025 Trump executive order directing AG to identify and challenge state climate laws (including LCFS) is generally assessed as unlikely to impact the LCFS.',
 0.80, '{"source": "hobo_section2", "legal_status": "resilient"}'),

('four_critical_scenarios', 'hobo_renewables', 'scenario_framework',
 'Four most critical policy scenarios to monitor: (1) US Tax Credit Cliff/Extension 2025-2027 — single biggest factor for SAF industry; (2) EU ReFuelEU 2025 2% target implementation — litmus test for mandate achievability; (3) Feedstock availability vs policy-driven demand — risk that demand outpaces supply forcing policy changes; (4) Canada trajectory — whether explicit SAF incentives emerge, making Canadian market significant for HOBO.',
 0.90, '{"source": "hobo_section2", "type": "monitoring_framework"}')
ON CONFLICT (context_key) DO UPDATE SET content = EXCLUDED.content, confidence = EXCLUDED.confidence;
