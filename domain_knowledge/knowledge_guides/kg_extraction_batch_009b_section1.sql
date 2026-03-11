-- ============================================================
-- Knowledge Graph Extraction: Batch 009b
-- Source: HOBO Study - Section 1: RD/SAF 101
-- Date: 2026-02-14
-- ============================================================

INSERT INTO kg_source (source_key, source_type, title, url, report_date, category, commodities, themes, status, first_seen, last_processed, node_count, edge_count, context_count)
VALUES
('gdoc_hobo_section1', 'gdrive_doc', 'HOBO Study - Section 1: RD/SAF 101', 'https://docs.google.com/document/d/1SN4F_sUWxB7VNudNiQHglUB5TQ74MwQ9YjG0L4ly3Zo/edit', '2025-06-19', 'consulting_report', '{renewable_diesel,saf,soybean_oil,uco,tallow,canola_oil,palm_oil}', '{production_pathways,feedstock_requirements,hefa,atj,fischer_tropsch,coprocessing}', 'completed', NOW(), NOW(), 8, 6, 6)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

-- ============================================================
-- NODES
-- ============================================================

INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('rd_fuel', 'commodity', 'Renewable Diesel (RD)',
 'Hydrocarbon fuel chemically identical to petroleum diesel. Drop-in fuel meeting ASTM D975. Superior cold weather performance vs biodiesel (FAME). 50-80% lower lifecycle GHG vs petroleum depending on feedstock. Produced via HEFA hydrotreatment of lipids.',
 '{"spec": "ASTM_D975", "ghg_reduction_pct": "50-80", "compatibility": "drop_in", "vs_biodiesel": "superior_cold_flow"}'),
('saf_fuel', 'commodity', 'Sustainable Aviation Fuel (SAF)',
 'Renewable alternative to Jet A/A-1. Certified under ASTM D7566. Can blend up to 50% with conventional jet. Up to 80% GHG reduction depending on pathway/feedstock. Only near-to-midterm option to decarbonize aviation.',
 '{"spec": "ASTM_D7566", "max_blend_pct": 50, "ghg_reduction_pct": "up_to_80", "sector": "aviation"}'),
('coprocessing', 'technology', 'Co-Processing',
 'Processing vegetable oils comingled with conventional diesel feedstocks through existing refinery hydrotreater. Typically limited to ~5% blend, recently revised to allow up to 30% for Jet A-1. Lower CapEx but limited feedstock flexibility and SAF capability.',
 '{"blend_limit_pct": "5_typical_30_revised", "advantage": "lower_capex", "limitation": "limited_feedstock_flexibility_and_saf"}'),
('atj_technology', 'technology', 'Alcohol-to-Jet (ATJ)',
 'ASTM-approved pathway converting ethanol/alcohols into jet hydrocarbons. Technology readiness significantly lower than HEFA. Higher capital, operating, and financing costs.',
 '{"feedstock": "ethanol_alcohols", "maturity": "developing", "vs_hefa": "higher_cost"}'),
('ft_technology', 'technology', 'Fischer-Tropsch (FT) Synthesis',
 'Gasifies solid biomass or municipal waste into syngas, then catalytically converts to synthetic fuels. FT-SAF certified for aviation. Technology readiness lower than HEFA.',
 '{"feedstock": "biomass_msw", "maturity": "developing", "product": "synthetic_fuels"}'),
('power_to_liquid', 'technology', 'Power-to-Liquid (PtL) e-Fuels',
 'Emerging pathway synthesizing jet fuel from green hydrogen and CO2. Near-zero CI potential but highest cost of all pathways. May contribute longer-term.',
 '{"feedstock": "green_h2_plus_co2", "maturity": "emerging", "ci": "near_zero", "cost": "highest"}'),
('feedstock_pretreatment', 'technology', 'Feedstock Pretreatment Unit',
 'Critical component of HEFA plants that cleans and conditions feedstock before reactor entry. Enables use of lower-quality (cheaper) waste oils that contain contaminants like water, sulfur, metals, chlorides. Key differentiator in feedstock flexibility.',
 '{"purpose": "remove_contaminants", "enables": "cheaper_waste_oil_processing", "contaminants": ["water", "sulfur", "metals", "chlorides"]}'),
('canola_oil_feedstock', 'commodity', 'Canola/Rapeseed Oil (for biofuels)',
 'Surged from virtually zero pre-2022 to significant share of BBD feedstock slate. Canadian import dependency growing. Higher CI than waste oils but lower than soy.',
 '{"pre_2022": "near_zero", "trend": "surging", "source": "Canada_imports"}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- ============================================================
-- EDGES
-- ============================================================

INSERT INTO kg_edge (source_node, target_node, edge_type, label, weight, metadata)
VALUES
('hefa_technology', 'rd_fuel', 'PRODUCES', 'HEFA is dominant commercial technology producing RD via hydrotreatment of lipids at high pressure', 0.95,
 '{"confidence": "very_high"}'),
('hefa_technology', 'saf_fuel', 'PRODUCES', 'HEFA produces SAF with additional cracking/isomerization; plants can swing RD/SAF ratio', 0.90,
 '{"mechanism": "adjustable_cracking", "confidence": "very_high"}'),
('feedstock_pretreatment', 'hefa_technology', 'ENABLES', 'Pretreatment enables HEFA to process cheaper low-quality waste oils by removing contaminants', 0.90,
 '{"mechanism": "contaminant_removal", "confidence": "high"}'),
('atj_technology', 'saf_fuel', 'PRODUCES', 'ATJ converts ethanol/alcohols to jet hydrocarbons but at higher cost than HEFA', 0.75,
 '{"vs_hefa": "higher_cost", "confidence": "high"}'),
('ft_technology', 'saf_fuel', 'PRODUCES', 'FT synthesis produces SAF from biomass/MSW gasification but lower technology readiness than HEFA', 0.70,
 '{"vs_hefa": "lower_TRL", "confidence": "high"}'),
('rd_fuel', 'saf_fuel', 'RELATED_TO', 'RD and SAF produced by same HEFA process with different cut points; co-products of same facility', 0.90,
 '{"mechanism": "shared_production", "confidence": "very_high"}')
ON CONFLICT DO NOTHING;

-- ============================================================
-- CONTEXTS
-- ============================================================

INSERT INTO kg_context (context_key, node_or_edge_ref, context_type, content, confidence, metadata)
VALUES
('rd_vs_biodiesel', 'rd_fuel', 'expert_rule',
 'Critical distinction: RD is a hydrocarbon (drop-in, pipeline-compatible, ASTM D975) while biodiesel is an ester (FAME, blending limits, cold flow issues). RD has superior cold weather performance and can use existing infrastructure without modification. This is why RD has displaced biodiesel in new capacity buildout.',
 0.95, '{"source": "hobo_section1", "critical_distinction": true}'),

('hefa_dominance', 'hefa_technology', 'market_structure',
 'HEFA is by far the dominant technology for both RD and SAF because it is commercially mature with 100+ plants worldwide. All alternative pathways (ATJ, FT, PtL) have significantly higher capital, operating, and financing costs. HEFA will supply ~60-70% of US SAF by 2030 per DOE estimates.',
 0.95, '{"source": "hobo_section1", "doe_saf_share_2030_pct": "60-70"}'),

('feedstock_slate_evolution', 'us_rd_feedstock_consumption', 'historical_pattern',
 'Dramatic feedstock shift 2016→2024: Tallow fell from 80%→28%. UCO+DCO rose to 32% combined. SBO went from near-zero to 40% (now declining from 53% to 49%). Canola oil surged from virtually zero pre-2022. Shift driven by LCFS/RFS CI-based regulations rewarding waste feedstocks with more credits, and HEFA plants better able to process waste oils than FAME biodiesel units.',
 0.90, '{"source": "hobo_section1", "time_range": "2016-2024"}'),

('coprocessing_limitations', 'coprocessing', 'technical_constraint',
 'Co-processing is limited to ~5% vegetable oil blend (recently revised to 30% for Jet A-1). Advantages: lower CapEx using existing refinery. Disadvantages: limited feedstock flexibility, no back-end upgrading limits SAF capability, primarily an RD production route. Not suitable for HOBO ambition of flexible RD/SAF producer.',
 0.80, '{"source": "hobo_section1"}'),

('saf_scale_challenge', 'saf_fuel', 'market_structure',
 'SAF is currently <0.5% of global jet fuel. Must scale to tens of billions of gallons per year. This is the only near-to-midterm option for aviation decarbonization — electric/hydrogen aircraft for long-haul are decades away. IATA targets SAF to account for ~65% of its 2050 50% GHG reduction goal.',
 0.90, '{"source": "hobo_section1", "current_penetration_pct": "<0.5", "iata_target_contribution_pct": 65}'),

('new_crush_wave', 'soybean_oil_feedstock', 'market_signal',
 'Over 20 new soy crushing projects announced since RD boom vs ~70 existing. The wave of crush capacity is the clearest signal that biofuel demand is structurally reshaping the US oilseed processing industry. Despite SBO having higher CI than waste oils, its abundance makes it the fallback feedstock as waste supplies tighten.',
 0.85, '{"source": "hobo_section1", "new_projects": "20+", "existing_plants": "~70"}')
ON CONFLICT (context_key) DO UPDATE SET content = EXCLUDED.content, confidence = EXCLUDED.confidence;
