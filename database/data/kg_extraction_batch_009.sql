-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 009
-- Source: HOBO Renewable Fuels Landscape and Feedstock Availability Study
--         (Fastmarkets consulting engagement for HOBO Renewable Diesel LLC)
-- Focus: RD/SAF feasibility, HEFA technology, feedstock availability,
--        policy drivers (RFS/LCFS/45Z/ReFuelEU), competitive positioning,
--        economic viability, margin construction, CI value framework,
--        product slate optimization, regional netback analysis
-- Sections: Executive Summary, RD/SAF 101, Policy & Regulatory Drivers,
--           Project Overview (Clinton County), Strategic Assessment (SWOT),
--           Economic Viability, Price Projections & Margin Outlook
-- Extracted: 2026-02-14
-- ============================================================================

-- BATCH REGISTRATION
INSERT INTO core.kg_source (source_key, source_type, title, location_uri, document_date, document_type, commodities, topics, status, first_processed, last_processed, nodes_extracted, edges_extracted, contexts_extracted) VALUES
('gdoc_hobo_exec_summary', 'gdrive_doc', 'HOBO Study - Executive Summary', 'https://docs.google.com/document/d/1_if_QrZ1Kn34JLAH9xdiY3go5T-bpD2fFdIH2B55LuQ/edit', '2025-06-19', 'consulting_report', '{renewable_diesel,saf,soybean_oil,uco,tallow,dcor,choice_white_grease}', '{feasibility_study,feedstock_availability,policy_drivers,logistics,economic_competitiveness,permitting}', 'completed', NOW(), NOW(), 18, 14, 10),
('gdoc_hobo_section1', 'gdrive_doc', 'HOBO Study - Section 1: RD/SAF 101', 'https://docs.google.com/document/d/1SN4F_sUWxB7VNudNiQHglUB5TQ74MwQ9YjG0L4ly3Zo/edit', '2025-06-19', 'consulting_report', '{renewable_diesel,saf,soybean_oil,uco,tallow,canola_oil,palm_oil}', '{production_pathways,feedstock_requirements,hefa,atj,fischer_tropsch,coprocessing}', 'completed', NOW(), NOW(), 8, 6, 6),
('gdoc_hobo_section2', 'gdrive_doc', 'HOBO Study - Section 2: Policy and Regulatory Drivers', 'https://docs.google.com/document/d/1qmE3QSUbzbx0tKDHvzzlsEIa154dPaVaZtb2tO5v5Q0/edit', '2025-06-19', 'consulting_report', '{d4_rin,d5_rin,d6_rin,d3_rin,renewable_diesel,saf}', '{rfs,lcfs,45z,refueleu,cfr,rvo,state_incentives,policy_scenarios}', 'completed', NOW(), NOW(), 12, 10, 8),
('gdoc_hobo_section4', 'gdrive_doc', 'HOBO Study - Section 4: Project Overview - Clinton County Facility', 'https://docs.google.com/document/d/1qc-BOvut86TdqCXi7BL_gHUEBBE_gClCXiszCvONwYE/edit', '2025-06-19', 'consulting_report', '{renewable_diesel,saf}', '{facility_design,clinton_county,logistics,capacity,hobo_spread}', 'completed', NOW(), NOW(), 4, 4, 3),
('gdoc_hobo_section6', 'gdrive_doc', 'HOBO Study - Section 6: Strategic Assessment (SWOT)', 'https://docs.google.com/document/d/1TWHAEpSlElgX58QvPimSgVG3X3IgenEXUPknuKsBLVQ/edit', '2025-06-19', 'consulting_report', '{renewable_diesel,saf,uco,tallow,dcor,soybean_oil,yellow_grease}', '{swot,competitive_strategy,feedstock_security,credit_arbitrage,policy_risk}', 'completed', NOW(), NOW(), 4, 6, 6),
('gdoc_hobo_section7', 'gdrive_doc', 'HOBO Study - Section 7: Economic Viability', 'https://docs.google.com/document/d/1YMSUN3k4Q385uQAXruP2I56jhmQxjUATAlRTACvCd1g/edit', '2025-06-19', 'consulting_report', '{renewable_diesel,saf,uco,tallow,dcor,soybean_oil,choice_white_grease}', '{capex,opex,margins,sensitivity,feedstock_cost,competitor_benchmarking,diamond_green,execution_timeline}', 'completed', NOW(), NOW(), 8, 6, 8),
('gdoc_hobo_section8', 'gdrive_doc', 'HOBO Study - Section 8: Price Projections and Margin Outlook', 'https://docs.google.com/document/d/1tzMuI3_2Us4jKbtycC9a4KLs-TXvceQuJCGrFQKGqiA/edit', '2025-06-19', 'consulting_report', '{renewable_diesel,saf,d4_rin,ulsd,jet_fuel,uco,soybean_oil,tallow}', '{pricing_dynamics,credit_stacking,ci_value,margin_scenarios,competitor_benchmarking,product_flexibility,market_optimization}', 'completed', NOW(), NOW(), 10, 8, 10)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

-- ============================================================================
-- NEW NODES (all 7 sub-files combined)
-- ============================================================================
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES

-- ===================== BATCH 009a: EXECUTIVE SUMMARY =====================

-- HOBO Renewables (the project company)
('company', 'hobo_renewables', 'HOBO Renewable Diesel LLC', '{"context": "Development-stage renewable fuels company planning a 125+ MMgy HEFA RD/SAF facility in Clinton County, Iowa. Name reflects the Heating Oil-to-Bean Oil (HOBO) spread that underpins RD economics.", "location": "Clinton County, Iowa", "capacity_mgy": 125, "capacity_bpd": 9300, "technology": "HEFA", "products": ["renewable_diesel", "saf"], "status": "development_stage"}'),

-- HEFA Technology
('technology', 'hefa_technology', 'HEFA (Hydroprocessed Esters and Fatty Acids)', '{"context": "Commercially dominant technology for RD/SAF production. Hydrotreats lipid feedstocks at high pressure with hydrogen to produce drop-in hydrocarbon fuels. 100+ plants worldwide operating, under construction, or announced. Can be tuned to produce variable RD/SAF ratios.", "maturity": "commercial", "plants_worldwide": "100+", "typical_rd_saf_ratio": "80/20 adjustable", "feedstock_type": "lipids"}'),

-- US RD Capacity
('metric', 'us_rd_capacity', 'US Renewable Diesel Capacity', '{"context": "US renewable diesel production capacity expanded from ~1 billion gallons in 2020 to ~5 billion gallons expected by 2025. Growth is entirely policy-driven: RD costs more than fossil diesel without mandates and credits.", "capacity_2020_bgal": 1.0, "capacity_2025_bgal": 5.0, "growth_driver": "policy"}'),

-- US RD Feedstock Consumption
('metric', 'us_rd_feedstock_consumption', 'US RD Feedstock Consumption', '{"context": "US renewable diesel production consumed over 30 billion lbs of fats and oils in 2024, up from ~4 billion lbs in 2017. Demand is outpacing domestic supply (~32.5 billion lbs), forcing record imports.", "consumption_2017_blbs": 4, "consumption_2024_blbs": 30, "domestic_supply_blbs": 32.5, "import_growth_2020_2024": "10x"}'),

-- HOBO Feedstock Catchment
('metric', 'hobo_feedstock_catchment', 'HOBO 250-Mile Feedstock Catchment', '{"context": "HOBO has potential access to ~6 billion lbs of feedstock within 250 miles (tallow, CWG, UCO, DCO, soybean oil) — roughly 6x its annual requirement of 1-1.1 billion lbs. Extends to 21 billion lbs at 500 miles.", "radius_250mi_blbs": 6, "radius_500mi_blbs": 21, "annual_requirement_blbs": "1.0-1.1", "coverage_ratio_250mi": "6x", "uco_access_blbs": 2.6}'),

-- IEA Feedstock Crunch Warning
('event', 'iea_feedstock_crunch', 'IEA Feedstock Crunch Warning (2027)', '{"context": "IEA warns of potential feedstock crunch by 2027 as worldwide demand for vegetable oils and waste fats is expected to jump ~56% to 174 billion lbs under current trends.", "projected_demand_blbs": 174, "demand_increase_pct": 56, "year": 2027}'),

-- US Feedstock Imports
('metric', 'us_feedstock_imports', 'US Biofuel Feedstock Imports', '{"context": "US imports of UCO, tallow, and other feedstocks surged 10-fold from 2020 to 2024 (738 million lbs to ~7.5 billion lbs) to meet RD demand. Import dependency creates tariff and policy risk.", "imports_2020_mlbs": 738, "imports_2024_blbs": 7.5, "growth_factor": "10x", "risk_factors": ["tariffs", "45Z_restriction", "china_trade_war"]}'),

-- ReFuelEU Aviation Mandate
('policy', 'refueleu_mandate', 'ReFuelEU Aviation Mandate', '{"context": "EU regulation requiring minimum SAF in jet fuel: 2% by 2025, 6% by 2030, 20% by 2035. Equates to ~0.3B gal (2025), ~1.1B gal (2030), ~3.9B gal (2035). Virtually guarantees a robust SAF market.", "pct_2025": 2, "pct_2030": 6, "pct_2035": 20, "vol_2025_bgal": 0.3, "vol_2030_bgal": 1.1, "vol_2035_bgal": 3.9}'),

-- Global SAF Offtake Agreements
('metric', 'global_saf_offtake', 'Global SAF Offtake Agreements', '{"context": "By late 2023, major airlines worldwide signed offtake agreements for over 14 billion gallons of SAF supply. These are multi-year agreements reflecting confidence production will scale.", "total_bgal": 14, "as_of": "late_2023", "type": "multi_year_offtake"}'),

-- Clean Fuel Production Credit (45Z)
('policy', 'cfpc_45z', 'Clean Fuel Production Credit (45Z)', '{"context": "IRA tax credit replacing Blenders Tax Credit from 2025. Base $0.20/gal RD ($1.00 with prevailing wage/apprenticeship), $0.35/gal SAF ($1.75 enhanced). Scales linearly with CI below 50kgCO2/MMBTU. Set to expire 2027 but House Ways & Means drafting extension to 2031 with domestic feedstock requirement and ILUC exclusion.", "rd_base_per_gal": 0.20, "rd_enhanced_per_gal": 1.00, "saf_base_per_gal": 0.35, "saf_enhanced_per_gal": 1.75, "ci_threshold_kgco2_mmbtu": 50, "expiry": 2027, "proposed_extension": 2031}'),

-- HOBO CI Advantage
('metric', 'hobo_ci_advantage', 'HOBO Carbon Intensity Advantage', '{"context": "HOBO targets CI in the low 20s gCO2/MJ vs industry average in the upper 30s. 75% of hydrogen feedstock produced from recycling process off-gases, reducing CI significantly. Lower CI = more credits per gallon.", "hobo_ci_gco2mj": "low_20s", "industry_avg_ci": "upper_30s", "h2_recycling_pct": 75}'),

-- RD Market
('market', 'rd_market', 'Renewable Diesel Market', '{"context": "US RD market driven by RFS D4 RIN mandate, LCFS credits, and 45Z tax credit. California is dominant consumption market. Capacity ~5B gal by 2025.", "primary_driver": "RFS", "key_market": "California", "capacity_2025_bgal": 5}'),

-- SAF Market
('market', 'saf_market', 'Sustainable Aviation Fuel Market', '{"context": "Nascent but rapidly growing market driven by EU mandates (ReFuelEU), IRA 45Z credits, state incentives, and corporate sustainability commitments. Airlines signed 14B+ gal in offtake agreements.", "stage": "nascent_rapid_growth", "key_drivers": ["ReFuelEU", "45Z", "corporate_commitments"]}'),

-- California LCFS
('policy', 'california_lcfs', 'California LCFS', '{"context": "State LCFS awarding credits for fuels with lower CI than fossil baseline. Currently targeting 20% reduction by 2030, amendment processing for 30% by 2030 and 90% by 2045. Single biggest driver of RD growth historically.", "target_2030_current_pct": 20, "target_2030_amended_pct": 30, "target_2045_pct": 90}'),

-- RFS Program
('policy', 'rfs_program', 'Renewable Fuel Standard (RFS)', '{"context": "Federal mandate administered by EPA establishing annual biofuel volume obligations. Uses tradeable RIN credits. D4 (biomass-based diesel) RVO has grown strongly while corn ethanol D6 is flat due to E10 blendwall.", "administrator": "EPA", "key_rin": "D4", "trend": "BBD_RVO_increasing"}'),

-- UCO Feedstock
('commodity', 'uco_feedstock', 'Used Cooking Oil (UCO)', '{"context": "Preferred HEFA SAF feedstock due to favorable carbon chain length vs other waste oils. US reliant on imports (esp. from China). HOBO location provides advantaged access to US domestic UCO supplies.", "ci_advantage": "very_low", "import_risk": "high", "china_dependency": "significant"}'),

-- Soybean Oil Feedstock
('commodity', 'soybean_oil_feedstock', 'Soybean Oil (for biofuels)', '{"context": "Largest single feedstock for US biomass-based diesel at ~49% share but declining from 53%. Higher CI than waste oils but abundant domestic supply. 20+ new crush plants announced since RD boom.", "bbd_share_pct": 49, "trend": "declining_share", "new_crush_plants": "20+", "ci": "higher_than_waste"}'),

-- Tallow Feedstock
('commodity', 'tallow_feedstock', 'Tallow (for biofuels)', '{"context": "Animal fat feedstock. Was 80% of US RD feedstock in 2016, declined to 28% by 2024 as feedstock slate diversified. Low CI waste feedstock generating more credits.", "bbd_share_2016_pct": 80, "bbd_share_2024_pct": 28, "ci": "low"}'),

-- DCO Feedstock
('commodity', 'dcor_feedstock', 'Distillers Corn Oil (DCO)', '{"context": "Byproduct of ethanol production, extracted from distillers grains. Combined with UCO represents ~32% of BBD feedstock by 2024. Low CI waste feedstock.", "bbd_share_combined_with_uco_pct": 32, "source": "ethanol_byproduct", "ci": "low"}'),

-- ===================== BATCH 009b: SECTION 1 - RD/SAF 101 =====================

-- Renewable Diesel fuel
('commodity', 'rd_fuel', 'Renewable Diesel (RD)', '{"context": "Hydrocarbon fuel chemically identical to petroleum diesel. Drop-in fuel meeting ASTM D975. Superior cold weather performance vs biodiesel (FAME). 50-80% lower lifecycle GHG vs petroleum depending on feedstock. Produced via HEFA hydrotreatment of lipids.", "spec": "ASTM_D975", "ghg_reduction_pct": "50-80", "compatibility": "drop_in", "vs_biodiesel": "superior_cold_flow"}'),

-- SAF fuel
('commodity', 'saf_fuel', 'Sustainable Aviation Fuel (SAF)', '{"context": "Renewable alternative to Jet A/A-1. Certified under ASTM D7566. Can blend up to 50% with conventional jet. Up to 80% GHG reduction depending on pathway/feedstock. Only near-to-midterm option to decarbonize aviation.", "spec": "ASTM_D7566", "max_blend_pct": 50, "ghg_reduction_pct": "up_to_80", "sector": "aviation"}'),

-- Co-Processing
('technology', 'coprocessing', 'Co-Processing', '{"context": "Processing vegetable oils comingled with conventional diesel feedstocks through existing refinery hydrotreater. Typically limited to ~5% blend, recently revised to allow up to 30% for Jet A-1. Lower CapEx but limited feedstock flexibility and SAF capability.", "blend_limit_pct": "5_typical_30_revised", "advantage": "lower_capex", "limitation": "limited_feedstock_flexibility_and_saf"}'),

-- ATJ Technology
('technology', 'atj_technology', 'Alcohol-to-Jet (ATJ)', '{"context": "ASTM-approved pathway converting ethanol/alcohols into jet hydrocarbons. Technology readiness significantly lower than HEFA. Higher capital, operating, and financing costs.", "feedstock": "ethanol_alcohols", "maturity": "developing", "vs_hefa": "higher_cost"}'),

-- Fischer-Tropsch Technology
('technology', 'ft_technology', 'Fischer-Tropsch (FT) Synthesis', '{"context": "Gasifies solid biomass or municipal waste into syngas, then catalytically converts to synthetic fuels. FT-SAF certified for aviation. Technology readiness lower than HEFA.", "feedstock": "biomass_msw", "maturity": "developing", "product": "synthetic_fuels"}'),

-- Power-to-Liquid
('technology', 'power_to_liquid', 'Power-to-Liquid (PtL) e-Fuels', '{"context": "Emerging pathway synthesizing jet fuel from green hydrogen and CO2. Near-zero CI potential but highest cost of all pathways. May contribute longer-term.", "feedstock": "green_h2_plus_co2", "maturity": "emerging", "ci": "near_zero", "cost": "highest"}'),

-- Feedstock Pretreatment
('technology', 'feedstock_pretreatment', 'Feedstock Pretreatment Unit', '{"context": "Critical component of HEFA plants that cleans and conditions feedstock before reactor entry. Enables use of lower-quality (cheaper) waste oils that contain contaminants like water, sulfur, metals, chlorides. Key differentiator in feedstock flexibility.", "purpose": "remove_contaminants", "enables": "cheaper_waste_oil_processing", "contaminants": ["water", "sulfur", "metals", "chlorides"]}'),

-- Canola Oil Feedstock
('commodity', 'canola_oil_feedstock', 'Canola/Rapeseed Oil (for biofuels)', '{"context": "Surged from virtually zero pre-2022 to significant share of BBD feedstock slate. Canadian import dependency growing. Higher CI than waste oils but lower than soy.", "pre_2022": "near_zero", "trend": "surging", "source": "Canada_imports"}'),

-- ===================== BATCH 009c: SECTION 2 - POLICY DRIVERS =====================

-- D4 RIN
('policy', 'rfs_d4_rin', 'D4 RIN (Biomass-Based Diesel)', '{"context": "RFS category for biofuels replacing diesel/heating oil/jet meeting 50% GHG reduction. Fulfilled by biodiesel, RD, and SAF. Key credit for HOBO. Each BBD gallon generates ~1.7 D4 RINs due to energy equivalence. RVO growing from 2.43B gal (2020) to proposed 7.12B RINs (2026).", "ghg_threshold_pct": 50, "rin_per_gal": 1.7, "rvo_2020_bgal": 2.43, "rvo_2025_bgal": 3.35, "rvo_2026_proposed_brins": 7.12}'),

-- D6 RIN
('policy', 'rfs_d6_rin', 'D6 RIN (Conventional Renewable/Ethanol)', '{"context": "RFS category for corn ethanol at 20% GHG reduction. Flat at 15B RINs since 2022 due to E10 blendwall. Structural ceiling on ethanol growth is key reason EPA/Congress pivoting to BBD growth.", "ghg_threshold_pct": 20, "volume_brins": 15.0, "constraint": "E10_blendwall", "trend": "flat"}'),

-- E10 Blendwall
('constraint', 'e10_blendwall', 'E10 Blendwall', '{"context": "Technical/infrastructure limit constraining ethanol blending in US gasoline at ~10%. Combined with flat gasoline demand, this effectively caps corn ethanol volumes, forcing RFS growth into advanced biofuels (D4/D5) to meet increasing total renewable fuel targets.", "blend_limit_pct": 10, "gasoline_demand_trend": "flat_to_declining"}'),

-- EPA 2026/27 RVO Proposal
('policy', 'rvo_2026_proposal', 'EPA 2026/27 RVO Proposal', '{"context": "EPA proposed June 2025: BBD target of 7.12B RINs for 2026 (vs 3.35B gal in 2025). Also proposes halving RIN generation from imported biofuels/imported feedstocks, and removing eRINs. Finalization expected Dec 2025.", "bbd_2026_brins": 7.12, "import_rin_reduction": "50%", "erins": "removed", "status": "proposed"}'),

-- State SAF Incentives
('policy', 'state_saf_incentives', 'US State SAF Incentives', '{"context": "Multiple states offering SAF tax credits: Illinois $1.50/gal (2023-2032), Washington $1.0-$2.0/gal CI-escalated, Minnesota $1.50/gal (2024-2030), Nebraska $0.75-$1.25/gal (2027-2035), Michigan proposed $2.0/gal.", "illinois_per_gal": 1.50, "washington_max_per_gal": 2.00, "minnesota_per_gal": 1.50}'),

-- State RD Incentives
('policy', 'state_rd_incentives', 'US State RD Incentives', '{"context": "Multiple states with RD tax credits/exemptions: Iowa PTC 4cts/gal + BTC up to 10cts/gal, Montana 29.95cts/gal tax exemption for UCO-based, Kentucky $1/gal (capped), Texas 20cts/gal exemption, Illinois 6.25% sales tax exemption.", "iowa_ptc_per_gal": 0.04, "montana_exemption_per_gal": 0.2995, "kentucky_per_gal": 1.00}'),

-- Canada CFR
('policy', 'canada_cfr', 'Canada Clean Fuel Regulations (CFR)', '{"context": "Established 2023, similar to LCFS. Mandates gasoline/fuel suppliers to reduce CI by ~15% by 2030. Credit prices ~$100/te historically. Provincial programs (BC 30% target, $200-300/te credits) are more aggressive.", "target_2030_pct": 15, "credit_price_cad_te": 100, "bc_target_pct": 30, "bc_credit_range": "$200-300/te"}'),

-- China UCO Role
('market', 'china_uco_role', 'China UCO Export Market', '{"context": "China is key UCO exporter to US and Europe with large untapped collection potential. Role at risk from: (1) US-China tariffs, (2) UCO providence/adulteration concerns, (3) Chinese domestic SAF market development (considering 15% mandate by 2030 = ~2.9B gal SAF demand). Could shift from supplier to competitor.", "us_trade_risk": "tariffs", "quality_risk": "adulteration", "domestic_saf_mandate_2030_pct": 15, "domestic_saf_demand_bgal": 2.9}'),

-- EU RED III
('policy', 'eu_red_iii', 'EU Renewable Energy Directive III (RED III)', '{"context": "Primary EU biofuel legislation. 29% renewables in transport by 2030 or 14.5% CI reduction. Food-based feedstock capped at 7% or 2020 level. Palm oil phased out by 2030. Advanced feedstocks: Part A (cellulosic/MSW) min 5.5%, Part B (UCO/tallow) capped at 1.7%.", "transport_target_2030_pct": 29, "food_cap_pct": 7, "palm_phaseout": 2030, "advanced_min_pct": 5.5, "part_b_cap_pct": 1.7}'),

-- Book and Claim
('policy', 'book_and_claim', 'Book-and-Claim Accounting for SAF', '{"context": "System allowing SAF produced at one location to be credited to airline elsewhere without physical delivery. ICAO CORSIA leaning toward allowing this. Could facilitate more SAF credit generation by removing physical logistics hurdles. US/EU coordination critical.", "status": "under_development", "icao_leaning": "favorable", "benefit": "removes_logistics_barriers"}'),

-- SAF Grand Challenge
('policy', 'saf_grand_challenge', 'US SAF Grand Challenge', '{"context": "Biden-era target of 3B gal SAF by 2030, 35B gal by 2050 (100% of US demand). Renamed Synthetic Aviation Fuel Grand Challenge under Trump but not withdrawn. Underlying fundamentals and airline support remain.", "target_2030_bgal": 3, "target_2050_bgal": 35, "status": "renamed_not_withdrawn"}'),

-- ===================== BATCH 009d: SECTION 4 - PROJECT OVERVIEW =====================

-- Clinton County Site
('location', 'clinton_county_site', 'Clinton County, Iowa Facility Site', '{"context": "Proposed HOBO facility location. In heart of agricultural center with proximity to abundant feedstock (SBO, corn oil, animal fats). Multi-modal transport access: Mississippi River barges, rail lines with spur, highways. Strategic for both inbound feedstock and outbound distribution.", "state": "Iowa", "county": "Clinton", "transport": ["barge", "rail", "highway"], "river": "Mississippi"}'),

-- HOBO Facility Specs
('asset', 'hobo_facility_specs', 'HOBO HEFA Facility Specifications', '{"context": "Output exceeding 125 MMgy of RD and SAF. ~9,300 bpd capacity — among the larger standalone RD plants in the US. Flexible product slate (can swing between SAF and RD). Built-for-purpose greenfield design vs repurposed refineries.", "capacity_mgy": 125, "capacity_bpd": 9300, "design": "greenfield_built_for_purpose", "flexibility": "RD_SAF_swing"}'),

-- HOBO Spread Concept
('metric', 'hobo_spread_concept', 'HOBO Spread (Heating Oil minus Bean Oil)', '{"context": "The company name HOBO reflects the Heating Oil-to-Bean Oil price spread — the core economics of converting soybean/bio oils into diesel-range fuels. This spread is the fundamental profitability indicator for RD production.", "formula": "heating_oil_price - soybean_oil_price", "significance": "core_RD_economics"}'),

-- Mississippi River Access
('infrastructure', 'mississippi_river_access', 'Mississippi River Barge Access', '{"context": "HOBO site offers Mississippi River barge access for optional bulk transport. Combined with rail spur and highway access creates multi-modal logistics flexibility for feedstock inbound and product outbound.", "mode": "barge", "benefit": "bulk_transport_flexibility"}'),

-- ===================== BATCH 009e: SECTION 6 - SWOT =====================

-- HOBO H2 Recycling
('technology', 'hobo_h2_recycling', 'HOBO H2 Off-Gas Recycling', '{"context": "75% of HOBO hydrogen feedstock produced from recycling process off-gases from the renewable feedstock itself. This CI advantage yields CI in low 20s gCO2/MJ vs industry average upper 30s. More valuable product via additional credits.", "recycling_pct": 75, "ci_result": "low_20s", "industry_avg": "upper_30s"}'),

-- Credit Arbitrage Strategy
('strategy', 'credit_arbitrage_strategy', 'RD/SAF Credit Arbitrage Strategy', '{"context": "HOBO strategy of toggling between RD and SAF output to maximize credit arbitrage. Increase SAF for SAF-specific tax credits, shift to RD if diesel RIN/LCFS credits make that more profitable. Dynamic optimization could enhance revenues vs single-product peers.", "mechanism": "product_slate_flexibility", "optimization": "continuous"}'),

-- Yellow Grease Price Volatility
('metric', 'yellow_grease_price', 'Yellow Grease (UCO) Price Volatility', '{"context": "US yellow grease prices spiked to ~60cts/lb in 2022 from under 25cts two years prior, fell back to mid-30cts by 2024. Illustrates how quickly feedstock economics can change and importance of price risk management.", "price_2020_per_lb": "<0.25", "price_2022_peak_per_lb": 0.60, "price_2024_per_lb": "mid_0.30s"}'),

-- ATJ Competitive Threat
('technology', 'atj_competitive_threat', 'ATJ as Competitive Threat to HEFA SAF', '{"context": "Alcohol-to-Jet technology may commercialize in 2030s. Whether threat or opportunity for HEFA depends on ATJ conversion efficiency and ethanol feedstock cost. If higher cost than HEFA, ATJ becomes the price setter and HEFA retains margin advantage.", "timeline": "2030s", "key_variable": "ethanol_cost", "dual_outcome": "threat_or_price_setter"}'),

-- ===================== BATCH 009f: SECTION 7 - ECONOMIC VIABILITY =====================

-- HEFA CapEx Benchmarks
('metric', 'hefa_capex_benchmark', 'HEFA CapEx Benchmarks', '{"context": "A plant of HOBO scale (~10,000 bpd / ~150 MMgy) typically requires $1,200-1,500M in CapEx. Translates to ~$8-10 per gallon of annual capacity, mid-range by refining standards. Technology proven so can use established designs.", "capex_range_M": "1200-1500", "per_gal_capacity": "$8-10", "scale_bpd": 10000}'),

-- HEFA OpEx Structure
('metric', 'hefa_opex_structure', 'HEFA Operating Cost Structure', '{"context": "Variable OPEX relatively low at $0.30-0.40/gal excluding feedstock. Key costs: hydrogen (from natural gas), utilities, catalysts, labor. Feedstock = 70-80% of cash cost per gallon. Iowa offers lower natural gas costs. Managing feedstock procurement is central to economic viability.", "variable_opex_per_gal": "$0.30-0.40", "feedstock_pct_of_cost": "70-80%", "h2_source": "natural_gas"}'),

-- Feedstock Price Sensitivity Rule
('metric', 'feedstock_sensitivity_rule', 'Feedstock Price Sensitivity ($0.05/lb = ~$0.35-0.40/gal)', '{"context": "A $0.05/lb change in feedstock price translates to ~$0.35-0.40 per gallon change in production cost (since ~7-8 lbs of feed yield a gallon). This means a jump from $0.40 to $0.50/lb in feedstock could erase ~$0.75/gal in margin.", "sensitivity": "$0.05_per_lb = $0.35-0.40_per_gal", "lbs_per_gallon": "7-8", "critical": true}'),

-- Diamond Green Diesel
('company', 'diamond_green_diesel', 'Diamond Green Diesel (DGD)', '{"context": "Joint venture of Valero and Darling Ingredients. Over 1B gal/year capacity — largest US RD producer. Integrated feedstock supply via Darling global fats/oils collection. DGD margins ranged $0.50-$1.30/gal in recent years, falling to $0.60/gal by Q3 2024.", "partners": ["Valero", "Darling"], "capacity_bgal": 1.0, "margin_range_per_gal": "$0.50-1.30", "margin_q3_2024": "$0.60"}'),

-- Neste Corporation
('company', 'neste_corporation', 'Neste Corporation', '{"context": "World-leading renewable diesel producer. Sales margin fell from ~$813/ton Q4 2023 to $242/ton Q4 2024 — nearly 70% drop due to low diesel prices, oversupply, and weaker credits. $242/ton = ~$0.18/gal, very slim.", "margin_q4_2023_per_ton": 813, "margin_q4_2024_per_ton": 242, "margin_decline_pct": 70}'),

-- HOBO Execution Timeline
('metric', 'hobo_execution_timeline', 'HOBO Project Execution Timeline', '{"context": "HOBO has secured permits and LSTK EPC contract — shovel ready. FID expected Q3 2025. Construction ~40 months. Target commissioning end 2028. 6+ month ramp-up to full capacity. Feedstock MOUs claimed for 200% of plant requirements.", "fid": "Q3_2025", "construction_months": 40, "commissioning": "end_2028", "feedstock_mou_coverage_pct": 200, "permits": "secured"}'),

-- Midwest Feedstock Cost Curve
('metric', 'midwest_feedstock_cost_curve', 'Midwest Feedstock Cost Curve', '{"context": "Midwest waste fats trade at discount to coastal: CWG mid-$0.30s/lb (vs $0.40+ coastal). UCO/DCO low $0.40s (at times near $0.50). Local ethanol DCO and renderer animal fats form cheapest tier. Cost curve rises as HOBO taps more distant or premium sources. SBO at ~$0.50/lb is fallback but higher CI.", "cwg_midwest_per_lb": "mid_0.30s", "cwg_coastal_per_lb": "$0.40+", "uco_dco_per_lb": "low_0.40s", "sbo_per_lb": "~0.50"}'),

-- CI Reduction ROI
('metric', 'ci_reduction_roi', 'CI Reduction ROI', '{"context": "Every 5 CI points improvement yields ~$0.15/gal extra margin via LCFS + 45Z. For 10,000 bpd plant (~155 MMgy), that is ~$20-30M/year per 5 CI points. Moving from CI 35 to CI 20 = ~$0.45/gal more. To CI 0 = ~$0.80-1.00/gal more. ROI on CI-reducing investments is immediate via credits.", "per_5ci_per_gal": 0.15, "per_5ci_annual_M": "20-30", "ci35_to_ci20_per_gal": 0.45, "ci35_to_ci0_per_gal": "0.80-1.00"}'),

-- ===================== BATCH 009g: SECTION 8 - PRICE PROJECTIONS =====================

-- RD Price Stack
('metric', 'rd_price_stack', 'RD Price Stack Construction', '{"context": "RD revenue per gallon is a stack: Base ULSD price + D4 RINs (1.7 x RIN price) + LCFS credit + 45Z/BTC tax credit. In California 2025: ~$2.50 + $2.55 (RINs) + $0.50-0.75 (LCFS) + $0.65-1.00 (45Z) = $5.50-6.00+/gal effective price. Credits fill ~50% of total revenue.", "components": ["ulsd_base", "d4_rin", "lcfs", "45z"], "ca_2025_total": "$5.50-6.00+", "credit_share_pct": "~50"}'),

-- SAF Price Stack
('metric', 'saf_price_stack', 'SAF Price Stack Construction', '{"context": "SAF cost ~$8.00/gal without credits. Airlines pay ~$4.00/gal (jet parity). Gap bridged by: RINs ~$1.80/gal + LCFS ~$0.70/gal + SAF credit $1.50/gal = ~$4.00 in credits. Net: buyer pays $4, credits pay $4. Without credits, SAF is unmarketable at scale.", "total_cost": "$8.00", "buyer_pays": "$4.00", "credits": "$4.00", "credit_essential": true}'),

-- Regional Netback Hierarchy
('metric', 'regional_netback_hierarchy', 'Regional RD/SAF Netback Hierarchy', '{"context": "HOBO pricing hierarchy: California > Illinois > Alberta in base case. California advantage driven by LCFS + cap-and-trade. 2022 example: CA RD total value ~$1,028/ton vs IL ~$877/ton — CA 30% premium. Canada competitive but requires credit arbitrage optimization.", "hierarchy": "CA > IL > AB", "ca_premium_pct": 30, "ca_2022_per_ton": 1028, "il_2022_per_ton": 877}'),

-- SAF vs RD Economics
('metric', 'saf_vs_rd_economics', 'SAF vs RD Relative Economics Over Time', '{"context": "Pre-2023: SAF yielded $0.60-1.00/gal LESS than RD (no dedicated credit, jet<diesel). 2023 with SAF BTC: SAF became MORE valuable — +$288/ton vs RD in Midwest, +$55/ton in CA. 2025+ with 45Z: SAF and RD achieve comparable netbacks when credits strong. Without credits: RD outperforms by $0.20-0.30/gal.", "pre_2023_saf_penalty_per_gal": "$0.60-1.00", "2023_saf_premium_midwest_per_ton": 288, "no_credit_rd_advantage_per_gal": "$0.20-0.30"}'),

-- CI Value Framework
('metric', 'ci_value_framework', 'CI Value Per Point Framework', '{"context": "Baseline CI 35: LCFS credit ~$0.50/gal, 45Z ~$0.65/gal RD ($1.40 SAF). Each 5 CI points reduction = ~$0.15/gal more. CI 35->20 = +$0.45/gal. CI 35->0 = +$0.80-1.00/gal. For HOBO 155 MMgy, each 5 CI points = $20-30M/year. CI improvement ROI is immediate.", "baseline_ci": 35, "per_5ci_per_gal": 0.15, "ci35_to_ci0_per_gal": "0.80-1.00", "per_5ci_annual_M": "20-30"}'),

-- CI Reference Table
('reference', 'ci_table', 'SAF CI by Pathway Reference Table', '{"context": "CI benchmarks (gCO2e/MJ): UCO/Animal Fats HEFA ~18.2 (80% reduction); SBO HEFA ~64.9 (27%); Corn ethanol ATJ ~90.8 (~0%); Cellulosic ATJ ~24.6-39.7 (60-72%); MSW/Wood FT ~7.7-32.5 (63-91%); PtL ~7 (92%). Fossil jet baseline: 89.", "uco_hefa": 18.2, "sbo_hefa": 64.9, "corn_atj": 90.8, "cellulosic_atj": "24.6-39.7", "ft_msw": "7.7-32.5", "ptl": 7, "fossil_baseline": 89}'),

-- Base Case Margins
('metric', 'base_case_margins', 'HOBO Base Case Margins by Market', '{"context": "Base case margins (first operating year): Illinois RD ~$130-150/ton ($0.40-0.50/gal), IL SAF ~$180/ton ($0.55/gal). California RD ~$220-230/ton ($0.65-0.70/gal), CA SAF ~$270-290/ton ($0.80-0.88/gal). Alberta RD ~$240-260/ton ($1.05-1.10/gal, includes credit arbitrage assumptions).", "il_rd_per_gal": "$0.40-0.50", "il_saf_per_gal": 0.55, "ca_rd_per_gal": "$0.65-0.70", "ca_saf_per_gal": "$0.80-0.88", "ab_rd_per_gal": "$1.05-1.10"}'),

-- Best/Worst Case Margins
('metric', 'best_worst_margins', 'HOBO Best/Worst Case Margin Scenarios', '{"context": "Best case: margins could exceed base by 50-100%. Chicago RD approaching $0.80-0.90/gal, CA surpassing $1.00/gal (precedent in late 2021/early 2022). EBITDA margins mid-30%+. Worst case: Midwest RD near $0/gal, CA under $0.20/gal. Similar to industry 2024 downturn.", "best_case_il_rd_per_gal": "$0.80-0.90", "best_case_ca_per_gal": ">$1.00", "worst_case_il_per_gal": "~$0", "worst_case_ca_per_gal": "<$0.20"}'),

-- Product Slate Optimization
('strategy', 'product_slate_optimization', 'Product Slate Optimization Strategy', '{"context": "HOBO should continuously optimize: (1) RD vs SAF production ratio based on relative credit economics; (2) Geographic routing (CA vs IL vs Canada) based on netback after freight/credits; (3) Contract vs spot mix (lock base margin, float remainder for upside). Value at stake from optimization is tens of millions annually.", "optimization_dimensions": ["product_ratio", "geographic_routing", "contract_vs_spot"], "value": "tens_of_millions_annually"}'),

-- Canada Credit Arbitrage
('strategy', 'canada_credit_arbitrage', 'Canada Credit Arbitrage Opportunity', '{"context": "Surprising finding: Alberta margins appear highest at $1.05-1.10/gal due to potential double-dip of US credits (RIN/45Z) + Canadian CFR credit. Practically cannot double-count on same gallon — must optimize per-gallon credit routing. But Canada optionality is valuable, especially post-2027 if US credits sunset.", "margin_per_gal": "$1.05-1.10", "caveat": "cannot_double_count", "post_2027_value": "high"}')

ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- ============================================================================
-- EDGES (all 7 sub-files combined)
-- ============================================================================
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES

-- ===================== BATCH 009a: EXEC SUMMARY EDGES =====================

-- Policy drives demand
((SELECT id FROM core.kg_node WHERE node_key = 'rfs_program'),
 (SELECT id FROM core.kg_node WHERE node_key = 'rd_market'),
 'DRIVES', 0.95,
 '{"label": "RFS D4 RVO is main demand driver for US renewable diesel", "mechanism": "D4_RIN_obligation", "source_count": 1}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'california_lcfs'),
 (SELECT id FROM core.kg_node WHERE node_key = 'rd_market'),
 'DRIVES', 0.95,
 '{"label": "LCFS has been single biggest driver of RD growth, pulling most US RD into California", "mechanism": "CI_credit_value"}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'cfpc_45z'),
 (SELECT id FROM core.kg_node WHERE node_key = 'saf_market'),
 'ENABLES', 0.90,
 '{"label": "45Z SAF credit of up to $1.75/gal bridges cost gap to fossil jet, enabling SAF market development", "mechanism": "production_tax_credit", "saf_credit_max": 1.75}',
 'extracted', 0.90),

((SELECT id FROM core.kg_node WHERE node_key = 'refueleu_mandate'),
 (SELECT id FROM core.kg_node WHERE node_key = 'saf_market'),
 'DRIVES', 0.95,
 '{"label": "EU SAF mandate virtually guarantees robust SAF demand: 2%->6%->20% by 2025/2030/2035", "mechanism": "blending_mandate"}',
 'extracted', 0.95),

-- Feedstock dynamics
((SELECT id FROM core.kg_node WHERE node_key = 'us_rd_feedstock_consumption'),
 (SELECT id FROM core.kg_node WHERE node_key = 'iea_feedstock_crunch'),
 'CONTRIBUTES_TO', 0.85,
 '{"label": "US RD feedstock demand (30B+ lbs) contributing to projected global feedstock crunch by 2027", "mechanism": "demand_outpacing_supply"}',
 'extracted', 0.85),

((SELECT id FROM core.kg_node WHERE node_key = 'us_rd_feedstock_consumption'),
 (SELECT id FROM core.kg_node WHERE node_key = 'us_feedstock_imports'),
 'DRIVES', 0.90,
 '{"label": "Domestic demand outpacing domestic supply (~32.5B lbs) forces record imports (10x growth 2020-2024)", "mechanism": "supply_deficit"}',
 'extracted', 0.90),

-- HOBO advantages
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_feedstock_catchment'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'ADVANTAGES', 0.90,
 '{"label": "HOBO has 6x coverage ratio on feedstock within 250mi, providing significant supply security vs coastal competitors", "coverage_ratio": "6x", "radius_miles": 250}',
 'extracted', 0.90),

((SELECT id FROM core.kg_node WHERE node_key = 'hobo_ci_advantage'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'ADVANTAGES', 0.85,
 '{"label": "HOBO CI in low 20s vs industry upper 30s means more valuable product via higher credit generation per gallon", "ci_delta_gco2mj": "~15", "mechanism": "higher_credits"}',
 'extracted', 0.85),

-- Feedstock competition
((SELECT id FROM core.kg_node WHERE node_key = 'uco_feedstock'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil_feedstock'),
 'COMPETES_WITH', 0.85,
 '{"label": "UCO and SBO compete as HEFA feedstocks; UCO preferred for low CI but supply-constrained, SBO abundant but higher CI", "differentiation": "CI_vs_availability"}',
 'extracted', 0.85),

-- Technology
((SELECT id FROM core.kg_node WHERE node_key = 'hefa_technology'),
 (SELECT id FROM core.kg_node WHERE node_key = 'rd_market'),
 'ENABLES', 0.95,
 '{"label": "HEFA is commercially dominant technology for RD production with 100+ plants worldwide"}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'hefa_technology'),
 (SELECT id FROM core.kg_node WHERE node_key = 'saf_market'),
 'ENABLES', 0.90,
 '{"label": "HEFA also produces SAF via adjusted hydrocracking/fractionation; same plants can swing between RD and SAF", "mechanism": "product_slate_flexibility"}',
 'extracted', 0.90),

-- Market structure
((SELECT id FROM core.kg_node WHERE node_key = 'rd_market'),
 (SELECT id FROM core.kg_node WHERE node_key = 'saf_market'),
 'COMPETES_WITH', 0.85,
 '{"label": "RD and SAF compete for same HEFA feedstock and production capacity; plants can swing output based on relative economics", "mechanism": "shared_feedstock_shared_capacity"}',
 'extracted', 0.85),

-- Strategic pillar
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 (SELECT id FROM core.kg_node WHERE node_key = 'saf_market'),
 'TARGETS', 0.80,
 '{"label": "HOBO targets SAF market via flexible product slate, Chicago airport proximity ($1.50/gal IL incentive), and Canadian aviation demand", "key_markets": ["Illinois_airports", "Canada", "Europe_export"]}',
 'extracted', 0.80),

-- ===================== BATCH 009b: SECTION 1 EDGES =====================

((SELECT id FROM core.kg_node WHERE node_key = 'hefa_technology'),
 (SELECT id FROM core.kg_node WHERE node_key = 'rd_fuel'),
 'PRODUCES', 0.95,
 '{"label": "HEFA is dominant commercial technology producing RD via hydrotreatment of lipids at high pressure"}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'hefa_technology'),
 (SELECT id FROM core.kg_node WHERE node_key = 'saf_fuel'),
 'PRODUCES', 0.90,
 '{"label": "HEFA produces SAF with additional cracking/isomerization; plants can swing RD/SAF ratio", "mechanism": "adjustable_cracking"}',
 'extracted', 0.90),

((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_pretreatment'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hefa_technology'),
 'ENABLES', 0.90,
 '{"label": "Pretreatment enables HEFA to process cheaper low-quality waste oils by removing contaminants", "mechanism": "contaminant_removal"}',
 'extracted', 0.90),

((SELECT id FROM core.kg_node WHERE node_key = 'atj_technology'),
 (SELECT id FROM core.kg_node WHERE node_key = 'saf_fuel'),
 'PRODUCES', 0.75,
 '{"label": "ATJ converts ethanol/alcohols to jet hydrocarbons but at higher cost than HEFA", "vs_hefa": "higher_cost"}',
 'extracted', 0.75),

((SELECT id FROM core.kg_node WHERE node_key = 'ft_technology'),
 (SELECT id FROM core.kg_node WHERE node_key = 'saf_fuel'),
 'PRODUCES', 0.70,
 '{"label": "FT synthesis produces SAF from biomass/MSW gasification but lower technology readiness than HEFA", "vs_hefa": "lower_TRL"}',
 'extracted', 0.70),

((SELECT id FROM core.kg_node WHERE node_key = 'rd_fuel'),
 (SELECT id FROM core.kg_node WHERE node_key = 'saf_fuel'),
 'RELATED_TO', 0.90,
 '{"label": "RD and SAF produced by same HEFA process with different cut points; co-products of same facility", "mechanism": "shared_production"}',
 'extracted', 0.90),

-- ===================== BATCH 009c: SECTION 2 EDGES =====================

((SELECT id FROM core.kg_node WHERE node_key = 'e10_blendwall'),
 (SELECT id FROM core.kg_node WHERE node_key = 'rfs_d6_rin'),
 'CONSTRAINS', 0.95,
 '{"label": "Blendwall caps corn ethanol at ~15B gal, forcing all RFS growth into advanced biofuels (D4/D5)", "mechanism": "volume_ceiling"}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'e10_blendwall'),
 (SELECT id FROM core.kg_node WHERE node_key = 'rfs_d4_rin'),
 'DRIVES', 0.90,
 '{"label": "Ethanol ceiling forces EPA to grow BBD/Advanced RVO to meet total renewable fuel targets", "mechanism": "RFS_growth_displacement"}',
 'extracted', 0.90),

((SELECT id FROM core.kg_node WHERE node_key = 'cfpc_45z'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'ENABLES', 0.90,
 '{"label": "45Z provides up to $1.75/gal for SAF, critical for HOBO SAF economics. Extension to 2031 + domestic feedstock restriction would favor HOBO", "mechanism": "production_tax_credit"}',
 'extracted', 0.90),

((SELECT id FROM core.kg_node WHERE node_key = 'state_saf_incentives'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'ADVANTAGES', 0.85,
 '{"label": "Illinois $1.50/gal SAF credit + Chicago airport proximity creates high-value market for HOBO SAF", "mechanism": "state_incentive_stacking", "key_market": "Chicago_ORD_MDW"}',
 'extracted', 0.85),

((SELECT id FROM core.kg_node WHERE node_key = 'china_uco_role'),
 (SELECT id FROM core.kg_node WHERE node_key = 'us_feedstock_imports'),
 'THREATENS', 0.85,
 '{"label": "China tariffs + domestic SAF mandate could redirect UCO supply away from US, tightening domestic waste oil markets", "risk_factors": ["tariffs", "domestic_saf_mandate", "adulteration_concerns"]}',
 'extracted', 0.85),

((SELECT id FROM core.kg_node WHERE node_key = 'eu_red_iii'),
 (SELECT id FROM core.kg_node WHERE node_key = 'uco_feedstock'),
 'CONSTRAINS', 0.75,
 '{"label": "RED III Part B cap at 1.7% limits UCO/tallow contribution to EU advanced biofuel targets, potentially freeing more for US", "mechanism": "feedstock_cap", "cap_pct": 1.7}',
 'extracted', 0.75),

((SELECT id FROM core.kg_node WHERE node_key = 'rvo_2026_proposal'),
 (SELECT id FROM core.kg_node WHERE node_key = 'rfs_d4_rin'),
 'DRIVES', 0.85,
 '{"label": "Proposed BBD RVO doubling from 3.35B to 7.12B RINs would massively increase D4 demand and prices", "magnitude": "~2x_increase", "status": "proposed"}',
 'extracted', 0.85),

((SELECT id FROM core.kg_node WHERE node_key = 'rvo_2026_proposal'),
 (SELECT id FROM core.kg_node WHERE node_key = 'us_feedstock_imports'),
 'CONSTRAINS', 0.85,
 '{"label": "Proposal to halve RIN generation from imported biofuels/feedstocks would tighten domestic feedstock market and favor domestic producers like HOBO", "mechanism": "import_rin_reduction_50pct"}',
 'extracted', 0.85),

((SELECT id FROM core.kg_node WHERE node_key = 'canada_cfr'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'ENABLES', 0.80,
 '{"label": "CFR creates growing pull for RD/SAF imports into Canada; HOBO Midwest location + rail access enables competitive supply", "mechanism": "cfr_credit_value"}',
 'extracted', 0.80),

((SELECT id FROM core.kg_node WHERE node_key = 'book_and_claim'),
 (SELECT id FROM core.kg_node WHERE node_key = 'saf_market'),
 'ENABLES', 0.70,
 '{"label": "Book-and-claim would allow HOBO to produce SAF in Iowa and credit it to airlines at distant airports without physical delivery", "status": "under_development"}',
 'extracted', 0.70),

-- ===================== BATCH 009d: SECTION 4 EDGES =====================

((SELECT id FROM core.kg_node WHERE node_key = 'clinton_county_site'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'HOSTS', 0.95,
 '{"label": "Clinton County site provides multi-modal logistics and feedstock proximity"}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'hobo_spread_concept'),
 (SELECT id FROM core.kg_node WHERE node_key = 'rd_market'),
 'MEASURES', 0.95,
 '{"label": "HOBO spread (HO-BO) is the fundamental profitability measure for converting bio-oils to diesel", "cross_ref": "batch_001_hobo_spread"}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'mississippi_river_access'),
 (SELECT id FROM core.kg_node WHERE node_key = 'clinton_county_site'),
 'CONNECTS', 0.85,
 '{"label": "Mississippi River provides barge access for bulk feedstock/product transport"}',
 'extracted', 0.85),

((SELECT id FROM core.kg_node WHERE node_key = 'hobo_facility_specs'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'DEFINES', 0.95,
 '{"label": "HOBO facility designed at 125+ MMgy with RD/SAF swing capability — among largest standalone US RD plants"}',
 'extracted', 0.95),

-- ===================== BATCH 009e: SECTION 6 EDGES =====================

((SELECT id FROM core.kg_node WHERE node_key = 'hobo_h2_recycling'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_ci_advantage'),
 'DRIVES', 0.90,
 '{"label": "75% H2 recycling from off-gases is primary driver of HOBO CI advantage (low 20s vs upper 30s)", "mechanism": "reduced_fossil_h2_input"}',
 'extracted', 0.90),

((SELECT id FROM core.kg_node WHERE node_key = 'credit_arbitrage_strategy'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'ADVANTAGES', 0.85,
 '{"label": "Product slate flexibility enables credit arbitrage — maximize whichever product (RD/SAF) yields highest value at any given time", "mechanism": "dynamic_optimization"}',
 'extracted', 0.85),

((SELECT id FROM core.kg_node WHERE node_key = 'cfpc_45z'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'THREATENS', 0.85,
 '{"label": "45Z expiry/change is primary policy risk. ILUC removal from CI calculation would eliminate differentiation between waste and crop feedstocks, changing HOBO economics", "risk": "iluc_removal_reduces_waste_oil_advantage"}',
 'extracted', 0.85),

((SELECT id FROM core.kg_node WHERE node_key = 'yellow_grease_price'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'THREATENS', 0.85,
 '{"label": "Feedstock price volatility (UCO 25cts->60cts->35cts in 4 years) directly impacts margins. Competitors bidding up regional feedstock a major risk", "mechanism": "feedstock_cost_volatility"}',
 'extracted', 0.85),

((SELECT id FROM core.kg_node WHERE node_key = 'atj_competitive_threat'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hefa_technology'),
 'COMPETES_WITH', 0.70,
 '{"label": "ATJ may commercialize in 2030s as alternative SAF pathway. Could cap HEFA SAF demand OR become higher-cost price setter that protects HEFA margins", "timeline": "2030s"}',
 'extracted', 0.70),

((SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canada_cfr'),
 'TARGETS', 0.80,
 '{"label": "HOBO targets Canadian markets (Calgary, Winnipeg, Alberta aviation) as relatively undersupplied for RD/SAF. Canada prices at CA-plus-transport to attract barrels, where HOBO has lowest delivered cost", "mechanism": "proximity_cost_advantage"}',
 'extracted', 0.80),

-- ===================== BATCH 009f: SECTION 7 EDGES =====================

((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_sensitivity_rule'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'CONSTRAINS', 0.95,
 '{"label": "$0.05/lb feedstock swing = $0.35-0.40/gal margin impact — feedstock is THE dominant economic variable for HEFA", "critical": true}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'diamond_green_diesel'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'COMPETES_WITH', 0.85,
 '{"label": "DGD is formidable competitor with 1B+ gal scale and integrated Darling feedstock supply. HOBO counters with Midwest location cost edge and SAF flexibility", "competitive_dynamic": "scale_vs_location"}',
 'extracted', 0.85),

((SELECT id FROM core.kg_node WHERE node_key = 'neste_corporation'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'COMPETES_WITH', 0.80,
 '{"label": "Neste is global leader but margins collapsed 70% in 2024. HOBO projects similar margin range ($0.50-0.70/gal) but with lower logistics costs serving domestic markets", "competitive_dynamic": "global_vs_domestic"}',
 'extracted', 0.80),

((SELECT id FROM core.kg_node WHERE node_key = 'ci_reduction_roi'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_ci_advantage'),
 'REINFORCES', 0.90,
 '{"label": "Each CI point reduction adds direct dollar value via credits. HOBO CI advantage (low 20s vs industry 30s) translates to $20-30M+ annually in credit revenue advantage", "mechanism": "credit_generation"}',
 'extracted', 0.90),

((SELECT id FROM core.kg_node WHERE node_key = 'midwest_feedstock_cost_curve'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'ADVANTAGES', 0.85,
 '{"label": "Midwest waste fats trade at discount to coastal markets. HOBO local sourcing within 250mi by truck minimizes transport costs and middleman markup", "mechanism": "proximity_discount"}',
 'extracted', 0.85),

((SELECT id FROM core.kg_node WHERE node_key = 'hefa_opex_structure'),
 (SELECT id FROM core.kg_node WHERE node_key = 'feedstock_sensitivity_rule'),
 'EXPLAINS', 0.95,
 '{"label": "Feedstock at 70-80% of HEFA cash cost means feedstock price changes dominate all other cost variables by a wide margin"}',
 'extracted', 0.95),

-- ===================== BATCH 009g: SECTION 8 EDGES =====================

((SELECT id FROM core.kg_node WHERE node_key = 'rd_price_stack'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'DETERMINES', 0.90,
 '{"label": "RD revenue stack (ULSD + RINs + LCFS + 45Z) determines ~$5.50-6.00/gal effective price in CA. Credits = ~50% of revenue"}',
 'extracted', 0.90),

((SELECT id FROM core.kg_node WHERE node_key = 'saf_price_stack'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'DETERMINES', 0.95,
 '{"label": "SAF requires ~$4.00/gal in credits to bridge $8.00 production cost to $4.00 buyer price. Credits are not optional — SAF is unmarketable without them", "critical": true}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'ci_value_framework'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_ci_advantage'),
 'QUANTIFIES', 0.90,
 '{"label": "Each 5 CI points = $0.15/gal = $20-30M/year. HOBO CI 20s vs industry 30s = ~$40-60M/year advantage in credit revenue", "mechanism": "credit_generation_differential"}',
 'extracted', 0.90),

((SELECT id FROM core.kg_node WHERE node_key = 'saf_vs_rd_economics'),
 (SELECT id FROM core.kg_node WHERE node_key = 'credit_arbitrage_strategy'),
 'INFORMS', 0.85,
 '{"label": "SAF more valuable than RD when credits strong (+$288/ton Midwest 2023), RD more valuable when credits weak ($0.20-0.30/gal advantage). Flexibility to switch is the hedge."}',
 'extracted', 0.85),

((SELECT id FROM core.kg_node WHERE node_key = 'regional_netback_hierarchy'),
 (SELECT id FROM core.kg_node WHERE node_key = 'product_slate_optimization'),
 'INFORMS', 0.85,
 '{"label": "CA>IL>AB netback hierarchy guides geographic routing. But rankings shift with credit price changes — continuous monitoring required"}',
 'extracted', 0.85),

((SELECT id FROM core.kg_node WHERE node_key = 'base_case_margins'),
 (SELECT id FROM core.kg_node WHERE node_key = 'diamond_green_diesel'),
 'BENCHMARKS_AGAINST', 0.85,
 '{"label": "HOBO base case ($0.50-0.70/gal) sits middle of DGD range ($0.50-1.30) and matches Neste global average. Competitive positioning realistic"}',
 'extracted', 0.85),

((SELECT id FROM core.kg_node WHERE node_key = 'product_slate_optimization'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'ADVANTAGES', 0.85,
 '{"label": "Dynamic optimization of product/market/contract mix worth tens of millions annually. Requires dedicated trading/optimization capability"}',
 'extracted', 0.85),

((SELECT id FROM core.kg_node WHERE node_key = 'canada_credit_arbitrage'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'ADVANTAGES', 0.80,
 '{"label": "Canada provides both geographic diversification and potential credit arbitrage. Especially valuable post-2027 if US federal credits expire"}',
 'extracted', 0.80);

-- ============================================================================
-- CONTEXTS (all 7 sub-files combined)
-- ============================================================================
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source, last_updated) VALUES

-- ===================== BATCH 009a: EXEC SUMMARY CONTEXTS =====================

-- Strategic Framework: Four Pillars
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'strategic_framework', 'hobo_four_pillars',
 '{"content": "HOBO study recommends four strategic pillars: (1) Feedstock Security via long-term contracts, partnerships, and multi-feedstock flexibility; (2) Production Flexibility between RD and SAF to respond to market signals; (3) Market and Policy Alignment by registering for all credit programs and securing airline/distributor offtake pre-startup; (4) Phased Growth starting with proven HEFA/established feedstocks then exploring novel feedstocks and CI improvement pathways.", "confidence": 0.90, "source_doc": "hobo_exec_summary", "type": "recommendation"}',
 'always', 'extracted', NOW()),

-- Critical Insight: Policy Dependence
((SELECT id FROM core.kg_node WHERE node_key = 'rd_market'),
 'expert_rule', 'rd_saf_policy_dependence',
 '{"content": "RD/SAF are margin-sensitive commodity businesses despite green premium. Production costs exceed fossil equivalents. Profitability HINGES on policy incentives and feedstock pricing. Feedstock = 70-80% of HEFA operating costs. Without credits, RD carries hefty premium over diesel and struggles to compete.", "confidence": 0.95, "source_doc": "hobo_exec_summary", "critical": true}',
 'always', 'extracted', NOW()),

-- Feedstock Shift Pattern
((SELECT id FROM core.kg_node WHERE node_key = 'us_rd_feedstock_consumption'),
 'historical_pattern', 'feedstock_slate_shift',
 '{"content": "US RD feedstock slate shifted dramatically: tallow fell from 80% (2016) to 28% (2024). UCO+DCO grew to 32% combined. SBO declined from 53% to 49% but remains #1. Canola oil surged from near-zero to significant share. Trend driven by CI regulations rewarding waste feedstocks with more credits.", "confidence": 0.90, "source_doc": "hobo_exec_summary", "time_range": "2016-2024"}',
 'always', 'extracted', NOW()),

-- Import Vulnerability
((SELECT id FROM core.kg_node WHERE node_key = 'us_feedstock_imports'),
 'risk_assessment', 'import_vulnerability',
 '{"content": "US feedstock import dependency creates three risks: (1) US/Chinese tariff disputes threatening UCO supply (China is key importer); (2) Likely removal of imported feedstocks from 45Z eligibility; (3) Global competition for limited waste oil supply. Coastal/import-dependent producers face greater exposure than Midwest-located HOBO.", "confidence": 0.90, "source_doc": "hobo_exec_summary", "risk_level": "high"}',
 'always', 'extracted', NOW()),

-- Logistics Competitive Advantage
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'competitive_advantage', 'hobo_logistics_advantage',
 '{"content": "Most RD/SAF projects repurposed existing oil refineries located 500-1500 miles from primary feedstock basin. While quick to execute, they are not built-for-purpose (limits feedstock flexibility and CI) and have long supply chains. HOBO as greenfield in feedstock heartland has short supply chains (majority within 250mi by truck), built-for-purpose design, and multi-directional outbound access (rail, barge, highway) to 45% of US population centers plus Canadian demand.", "confidence": 0.90, "source_doc": "hobo_exec_summary", "key_differentiator": true}',
 'always', 'extracted', NOW()),

-- Airline SAF Commitment Scale
((SELECT id FROM core.kg_node WHERE node_key = 'saf_market'),
 'market_data', 'airline_saf_commitments',
 '{"content": "By late 2023, major airlines had signed offtake agreements for 14+ billion gallons of SAF. This is multi-year supply reflecting confidence in production scale-up. Combined with EU ReFuelEU mandates (2%->6%->20% by 2025/2030/2035), demand signals are very strong.", "confidence": 0.85, "source_doc": "hobo_exec_summary", "data_point": "14B_gal_offtake"}',
 'always', 'extracted', NOW()),

-- Permitting Precedent Warning
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'risk_assessment', 'permitting_risk',
 '{"content": "Permitting can kill projects: a 250 MMgy RD plant in Washington State was cancelled after years due to permitting delays and uncertainties. HOBO reports all major permits secured, but Fastmarkets has not independently validated this claim.", "confidence": 0.75, "source_doc": "hobo_exec_summary", "caveat": "unverified_by_fastmarkets"}',
 'always', 'extracted', NOW()),

-- HOBO UCO Access
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_feedstock_catchment'),
 'competitive_advantage', 'hobo_uco_access',
 '{"content": "HOBO has potential access to 2.6 billion lbs of UCO. UCO is preferred HEFA SAF feedstock due to carbon chain length. US UCO import dependency (especially from China) is at risk from tariffs and 45Z restrictions. HOBO domestic access is a significant differentiator vs import-dependent coastal producers.", "confidence": 0.85, "source_doc": "hobo_exec_summary", "feedstock": "UCO", "quantity_blbs": 2.6}',
 'always', 'extracted', NOW()),

-- New Crush Capacity Signal
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil_feedstock'),
 'market_signal', 'crush_capacity_buildout',
 '{"content": "Over 20 new soy crushing plant projects announced since the RD boom began, versus ~70 currently operating. This massive capacity buildout signals structural shift: biofuel demand for soybean oil is reshaping the entire US oilseed processing industry.", "confidence": 0.85, "source_doc": "hobo_exec_summary", "new_plants": "20+", "existing_plants": "~70"}',
 'always', 'extracted', NOW()),

-- Capacity Growth Rate
((SELECT id FROM core.kg_node WHERE node_key = 'us_rd_capacity'),
 'historical_pattern', 'rd_capacity_growth',
 '{"content": "US RD capacity grew from ~1B gal (2020) to ~5B gal (2025) - a 5x expansion in 5 years. This is the fastest infrastructure buildout in US biofuels history, driven entirely by policy incentives (RFS, LCFS, BTC/45Z).", "confidence": 0.90, "source_doc": "hobo_exec_summary", "growth_factor": "5x", "period": "2020-2025"}',
 'always', 'extracted', NOW()),

-- ===================== BATCH 009b: SECTION 1 CONTEXTS =====================

-- RD vs Biodiesel
((SELECT id FROM core.kg_node WHERE node_key = 'rd_fuel'),
 'expert_rule', 'rd_vs_biodiesel',
 '{"content": "Critical distinction: RD is a hydrocarbon (drop-in, pipeline-compatible, ASTM D975) while biodiesel is an ester (FAME, blending limits, cold flow issues). RD has superior cold weather performance and can use existing infrastructure without modification. This is why RD has displaced biodiesel in new capacity buildout.", "confidence": 0.95, "source_doc": "hobo_section1", "critical_distinction": true}',
 'always', 'extracted', NOW()),

-- HEFA Dominance
((SELECT id FROM core.kg_node WHERE node_key = 'hefa_technology'),
 'market_structure', 'hefa_dominance',
 '{"content": "HEFA is by far the dominant technology for both RD and SAF because it is commercially mature with 100+ plants worldwide. All alternative pathways (ATJ, FT, PtL) have significantly higher capital, operating, and financing costs. HEFA will supply ~60-70% of US SAF by 2030 per DOE estimates.", "confidence": 0.95, "source_doc": "hobo_section1", "doe_saf_share_2030_pct": "60-70"}',
 'always', 'extracted', NOW()),

-- Feedstock Slate Evolution
((SELECT id FROM core.kg_node WHERE node_key = 'us_rd_feedstock_consumption'),
 'historical_pattern', 'feedstock_slate_evolution',
 '{"content": "Dramatic feedstock shift 2016->2024: Tallow fell from 80%->28%. UCO+DCO rose to 32% combined. SBO went from near-zero to 40% (now declining from 53% to 49%). Canola oil surged from virtually zero pre-2022. Shift driven by LCFS/RFS CI-based regulations rewarding waste feedstocks with more credits, and HEFA plants better able to process waste oils than FAME biodiesel units.", "confidence": 0.90, "source_doc": "hobo_section1", "time_range": "2016-2024"}',
 'always', 'extracted', NOW()),

-- Co-Processing Limitations
((SELECT id FROM core.kg_node WHERE node_key = 'coprocessing'),
 'technical_constraint', 'coprocessing_limitations',
 '{"content": "Co-processing is limited to ~5% vegetable oil blend (recently revised to 30% for Jet A-1). Advantages: lower CapEx using existing refinery. Disadvantages: limited feedstock flexibility, no back-end upgrading limits SAF capability, primarily an RD production route. Not suitable for HOBO ambition of flexible RD/SAF producer.", "confidence": 0.80, "source_doc": "hobo_section1"}',
 'always', 'extracted', NOW()),

-- SAF Scale Challenge
((SELECT id FROM core.kg_node WHERE node_key = 'saf_fuel'),
 'market_structure', 'saf_scale_challenge',
 '{"content": "SAF is currently <0.5% of global jet fuel. Must scale to tens of billions of gallons per year. This is the only near-to-midterm option for aviation decarbonization — electric/hydrogen aircraft for long-haul are decades away. IATA targets SAF to account for ~65% of its 2050 50% GHG reduction goal.", "confidence": 0.90, "source_doc": "hobo_section1", "current_penetration_pct": "<0.5", "iata_target_contribution_pct": 65}',
 'always', 'extracted', NOW()),

-- New Crush Wave
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil_feedstock'),
 'market_signal', 'new_crush_wave',
 '{"content": "Over 20 new soy crushing projects announced since RD boom vs ~70 existing. The wave of crush capacity is the clearest signal that biofuel demand is structurally reshaping the US oilseed processing industry. Despite SBO having higher CI than waste oils, its abundance makes it the fallback feedstock as waste supplies tighten.", "confidence": 0.85, "source_doc": "hobo_section1", "new_projects": "20+", "existing_plants": "~70"}',
 'always', 'extracted', NOW()),

-- ===================== BATCH 009c: SECTION 2 CONTEXTS =====================

-- RFS Nesting Dynamics
((SELECT id FROM core.kg_node WHERE node_key = 'rfs_program'),
 'expert_rule', 'rfs_nesting_dynamics',
 '{"content": "RFS nesting mechanics are critical: headline BBD mandates understate true BBD demand. D6 is flat (blendwall), so all growth must come from advanced biofuels. The BBD RVO, Advanced RVO, and total renewable fuel RVO create a nested obligation where BBD must fill gaps in conventional (ethanol shortfall), advanced (after D3/D5 netting), and supplemental standards. Understanding this nesting is the key analytical edge in RIN market forecasting.", "confidence": 0.95, "source_doc": "hobo_section2", "cross_ref": "batch_009_bobs_rvo_analysis", "critical": true}',
 'always', 'extracted', NOW()),

-- 45Z Extension Scenario
((SELECT id FROM core.kg_node WHERE node_key = 'cfpc_45z'),
 'policy_scenario', '45z_extension_scenario',
 '{"content": "Four critical 45Z scenarios: (1) Extension to 2031 as drafted = bullish for SAF, stabilizes investment; (2) Expiry at 2027 = cliff in SAF incentives, stalling projects; (3) ILUC removal from CI calculation = reduces differentiation between waste and crop feedstocks, directionally increases soybean oil competitiveness; (4) Domestic feedstock restriction = tightens supply for low-CI material, favoring Midwest-located producers with domestic supply access like HOBO. Scenarios 1+3+4 combined = extremely favorable for HOBO.", "confidence": 0.90, "source_doc": "hobo_section2", "critical": true}',
 'always', 'extracted', NOW()),

-- Credit Stacking Opportunity
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'strategic_framework', 'credit_stacking_opportunity',
 '{"content": "All US federal and state incentives stack, creating optimization opportunities. For HOBO SAF sold into Illinois airports: 45Z ($1.50-1.75) + D4 RIN ($1.0-1.5) + Illinois state credit ($1.50) + potential LCFS if sold into CA instead. The routing/market optimization decision is worth tens of millions annually. HOBO needs a dedicated trading/optimization desk.", "confidence": 0.90, "source_doc": "hobo_section2"}',
 'always', 'extracted', NOW()),

-- EU Price Setter
((SELECT id FROM core.kg_node WHERE node_key = 'refueleu_mandate'),
 'market_structure', 'eu_price_setter',
 '{"content": "EU is likely to become global price setter for SAF. Reasons: mandatory targets with severe penalties (double the SAF-jet price difference, plus volume carryover), short domestic supply, high mandate trajectory (2%->6%->20%). Prices will rise to pull SAF supply from US/Asia. Europe will be short both feedstock AND product. This creates export opportunity for US HEFA producers including HOBO.", "confidence": 0.90, "source_doc": "hobo_section2", "implication": "export_opportunity"}',
 'always', 'extracted', NOW()),

-- China Supply Disruption Risk
((SELECT id FROM core.kg_node WHERE node_key = 'china_uco_role'),
 'risk_assessment', 'china_supply_disruption_risk',
 '{"content": "China could shift from net UCO exporter to net importer if domestic SAF mandate (15% by 2030 = 2.9B gal) materializes. Combined with US tariffs and 45Z import restrictions, this would: (1) materially tighten global waste oil supply, (2) increase feedstock prices, (3) favor domestic US producers with local UCO access like HOBO, (4) potentially benefit soybean oil as substitute feedstock.", "confidence": 0.85, "source_doc": "hobo_section2", "cascade_effects": true}',
 'always', 'extracted', NOW()),

-- Governor Advocacy Signal
((SELECT id FROM core.kg_node WHERE node_key = 'rvo_2026_proposal'),
 'political_signal', 'governor_advocacy_signal',
 '{"content": "Iowa, Nebraska, South Dakota, and Missouri governors urged EPA to set 2026 BBD RVO at 5.25B gal — a >1.5x increase vs 2025. This cross-state, bipartisan political pressure on BBD expansion is a strong indicator that the US policy direction strongly favors growing the biomass-based diesel market.", "confidence": 0.80, "source_doc": "hobo_section2", "signal_type": "bipartisan_advocacy"}',
 'always', 'extracted', NOW()),

-- LCFS Legal Resilience
((SELECT id FROM core.kg_node WHERE node_key = 'california_lcfs'),
 'policy_assessment', 'lcfs_legal_resilience',
 '{"content": "California LCFS has survived multiple federal and legal challenges including the State Commerce Clause and arguments it is preempted by the federal RFS. April 2025 Trump executive order directing AG to identify and challenge state climate laws (including LCFS) is generally assessed as unlikely to impact the LCFS.", "confidence": 0.80, "source_doc": "hobo_section2", "legal_status": "resilient"}',
 'always', 'extracted', NOW()),

-- Four Critical Scenarios
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'scenario_framework', 'four_critical_scenarios',
 '{"content": "Four most critical policy scenarios to monitor: (1) US Tax Credit Cliff/Extension 2025-2027 — single biggest factor for SAF industry; (2) EU ReFuelEU 2025 2% target implementation — litmus test for mandate achievability; (3) Feedstock availability vs policy-driven demand — risk that demand outpaces supply forcing policy changes; (4) Canada trajectory — whether explicit SAF incentives emerge, making Canadian market significant for HOBO.", "confidence": 0.90, "source_doc": "hobo_section2", "type": "monitoring_framework"}',
 'always', 'extracted', NOW()),

-- ===================== BATCH 009d: SECTION 4 CONTEXTS =====================

-- HOBO Name Etymology
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_spread_concept'),
 'expert_rule', 'hobo_name_etymology',
 '{"content": "HOBO stands for Heating Oil minus Bean Oil — the fundamental spread that determines RD production economics. This is the same HOBO spread identified in Batch 001 as the master lead indicator for D4 RIN direction and biodiesel production economics from the RIN forecast reports. The company literally named itself after the spread that defines its profitability.", "confidence": 0.95, "source_doc": "hobo_section4", "cross_ref": "batch_001_hobo_as_lead_indicator"}',
 'always', 'extracted', NOW()),

-- Greenfield vs Repurposed
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_facility_specs'),
 'competitive_advantage', 'greenfield_vs_repurposed',
 '{"content": "Most recent RD/SAF projects repurposed existing oil refineries for quick execution and existing logistics. But this creates disadvantages: located 500-1500mi from primary feedstock basin, not built-for-purpose (limits feedstock flexibility and CI), pre-existing infrastructure constrains feedstock types processable. HOBO as greenfield has short supply chains, full design optimization, and broader feedstock flexibility — turning the apparent disadvantage of no existing infrastructure into a key strength.", "confidence": 0.90, "source_doc": "hobo_section4", "strategy": "greenfield_advantage"}',
 'always', 'extracted', NOW()),

-- Outbound Market Reach
((SELECT id FROM core.kg_node WHERE node_key = 'clinton_county_site'),
 'strategic_assessment', 'outbound_market_reach',
 '{"content": "Clinton County outbound logistics advantage: proximal to Midwest, Mountain, and Northeast US markets (45% of US population centers). Rail access to West Coast/California. Mississippi barge to Gulf. Also advantaged for Central, Rockies, Central and Maritime Canada. Can optimize placement of RD vs SAF into respective highest-priced markets (e.g., RD into Canada, SAF into Illinois).", "confidence": 0.90, "source_doc": "hobo_section4", "population_reach_pct": 45}',
 'always', 'extracted', NOW()),

-- ===================== BATCH 009e: SECTION 6 CONTEXTS =====================

-- HOBO SWOT Strengths
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'strategic_assessment', 'hobo_swot_strengths',
 '{"content": "STRENGTHS: (1) Iowa location in feedstock-rich region with low-cost waste oils; (2) H2 off-gas recycling delivers CI in low 20s vs industry upper 30s = more valuable product; (3) Multi-modal logistics (rail, barge, highway) reaching coastal markets and Canada; (4) Flexible RD/SAF product slate enabling credit arbitrage; (5) Access to Midwest, Mountain, Northeast US + Central/Maritime Canada markets.", "confidence": 0.90, "source_doc": "hobo_section6", "type": "swot_s"}',
 'always', 'extracted', NOW()),

-- HOBO SWOT Weaknesses
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'strategic_assessment', 'hobo_swot_weaknesses',
 '{"content": "WEAKNESSES: (1) Greenfield startup requiring several hundred million $ without oil major backing; (2) No existing feedstock collection network or downstream distribution assets; (3) Market timing risk — coming online late 2020s potentially as incentives expire or market oversupplies; (4) Lack of vertical integration exposes to higher input costs and third-party dependency.", "confidence": 0.85, "source_doc": "hobo_section6", "type": "swot_w"}',
 'always', 'extracted', NOW()),

-- HOBO SWOT Opportunities
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'strategic_assessment', 'hobo_swot_opportunities',
 '{"content": "OPPORTUNITIES: (1) RD/SAF credit arbitrage via flexible production; (2) Canadian low-carbon fuel markets relatively undersupplied; (3) IRA 45Z incentives could significantly boost early-year profits if operational before phase-out; (4) Strategic partnerships (airline SAF offtake, logistics companies, feedstock providers); (5) Niche positioning as one of few Midwest SAF producers.", "confidence": 0.85, "source_doc": "hobo_section6", "type": "swot_o"}',
 'always', 'extracted', NOW()),

-- HOBO SWOT Threats
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'strategic_assessment', 'hobo_swot_threats',
 '{"content": "THREATS: (1) Policy uncertainty — 45Z expiry/change, ILUC removal would reshape economics; (2) Feedstock price volatility (yellow grease 25cts->60cts->35cts in 4 years); (3) Competitor feedstock lock-up in region; (4) ATJ/e-fuels commercializing in 2030s; (5) Potential oversupply as multiple HEFA projects come online simultaneously.", "confidence": 0.85, "source_doc": "hobo_section6", "type": "swot_t"}',
 'always', 'extracted', NOW()),

-- ILUC Removal Impact
((SELECT id FROM core.kg_node WHERE node_key = 'cfpc_45z'),
 'policy_scenario', 'iluc_removal_impact',
 '{"content": "CRITICAL SCENARIO: If ILUC is removed from 45Z CI calculation, the CI differentiation between soybean oil and waste feedstocks (UCO, DCO, tallow) would narrow substantially. This would: (1) Directionally increase soybean oil competitiveness for 45Z; (2) Reduce the premium for waste oil access that HOBO relies on; (3) Directionally incentivize RD into California (where LCFS still includes ILUC) relative to SAF (more US-homogeneous pricing). HOBO must monitor and adapt feedstock/market strategy accordingly.", "confidence": 0.90, "source_doc": "hobo_section6", "critical": true}',
 'always', 'extracted', NOW()),

-- Canadian Market Underserved
((SELECT id FROM core.kg_node WHERE node_key = 'canada_cfr'),
 'market_assessment', 'canadian_market_underserved',
 '{"content": "Canada CFR + provincial mandates creating growing pull for RD/SAF imports, especially Western Canada. Market prices at California-plus-transport to attract barrels. HOBO has lowest delivered cost due to Midwest proximity. Key outlets: Calgary, Winnipeg, BC, Ontario aviation. By exporting to Canada, HOBO may tap additional credit premiums under CFR/provincial CI regimes while avoiding California market saturation.", "confidence": 0.85, "source_doc": "hobo_section6", "strategy": "avoid_ca_saturation"}',
 'always', 'extracted', NOW()),

-- ===================== BATCH 009f: SECTION 7 CONTEXTS =====================

-- Margin Construction RD California
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'quantitative_framework', 'margin_construction_rd_california',
 '{"content": "Illustrative RD margin build (California, 2025): ULSD price ~$2.50/gal + D4 RINs ~$2.80/gal (1.7 RINs x ~$1.69) + LCFS credits ~$0.50-0.75/gal + BTC/45Z ~$1.00/gal = effective net selling price $5.50-6.00+/gal. Production cost ~$4.00-4.50/gal (feedstock $3.00+ plus ~$0.50 other). Margin potential >$1.00/gal in optimized California scenario.", "confidence": 0.85, "source_doc": "hobo_section7", "type": "margin_build"}',
 'always', 'extracted', NOW()),

-- No Credit Breakeven
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'risk_assessment', 'no_credit_breakeven',
 '{"content": "CRITICAL: Without policy incentives, HOBO needs to sell at ~$4.00+/gal to break even on variable costs — substantially above typical diesel/jet market prices of $2.50-3.00. This means credits are not nice to have — they are ESSENTIAL for viability. This risk is not unique to HOBO but applies to the entire RD/SAF industry.", "confidence": 0.95, "source_doc": "hobo_section7", "critical": true}',
 'always', 'extracted', NOW()),

-- Competitor Margin Collapse 2024
((SELECT id FROM core.kg_node WHERE node_key = 'rd_market'),
 'market_data', 'competitor_margin_collapse_2024',
 '{"content": "Industry margin compression 2024: Neste margins fell from $813/ton to $242/ton (70% decline). DGD margins fell from ~$0.95/gal to $0.60/gal. Causes: low diesel prices, new competition/oversupply, weaker credits. This is the downside scenario HOBO must be prepared for. Even industry leaders were severely impacted.", "confidence": 0.90, "source_doc": "hobo_section7", "neste_decline_pct": 70, "dgd_margin_q3_2024": 0.60}',
 'always', 'extracted', NOW()),

-- Feedstock Cost Is Everything
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_sensitivity_rule'),
 'expert_rule', 'feedstock_cost_is_everything',
 '{"content": "Feedstock cost dominates all other economic variables in HEFA economics. At 70-80% of cash cost, a $0.05/lb feedstock change = $0.35-0.40/gal margin change. For comparison, variable OPEX is only $0.30-0.40/gal total. Every cent/lb saved on feedstock = ~$0.08/gal margin. HOBO Midwest location advantage of even $0.05/lb vs coastal = ~$0.35-0.40/gal structural margin edge. This is why feedstock strategy (Section 5) is THE most critical section of the business plan.", "confidence": 0.95, "source_doc": "hobo_section7", "critical": true, "conversion": "1_cent_per_lb = 0.08_per_gal"}',
 'always', 'extracted', NOW()),

-- HOBO vs DGD Strategy
((SELECT id FROM core.kg_node WHERE node_key = 'diamond_green_diesel'),
 'competitive_analysis', 'hobo_vs_dgd_strategy',
 '{"content": "DGD advantages: massive scale (1B+ gal), integrated Darling feedstock supply (global collection), Valero refining/trading expertise. HOBO counters: Midwest location feedstock cost edge (sitting at the source vs coastal competitors), SAF flexibility (DGD primarily RD), lower transport costs for domestic feedstock, potentially lower CI via built-for-purpose design. HOBO cannot replicate DGD scale but can differentiate on cost position and flexibility.", "confidence": 0.85, "source_doc": "hobo_section7", "strategy": "niche_vs_scale"}',
 'always', 'extracted', NOW()),

-- Execution Readiness
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_execution_timeline'),
 'project_assessment', 'execution_readiness',
 '{"content": "HOBO claims strong execution readiness: all permits secured (unverified by Fastmarkets), LSTK EPC contract in place (shovel ready), feedstock MOUs for 200% of requirements (also unverified). FID Q3 2025, construction 40 months, commissioning end 2028. Timeline aggressive but feasible if financing closes. Key risk: financing without oil major backing requires strong feedstock/offtake contracts to satisfy lenders.", "confidence": 0.80, "source_doc": "hobo_section7", "caveats": "permits_and_mous_unverified"}',
 'always', 'extracted', NOW()),

-- CI as Investment Thesis
((SELECT id FROM core.kg_node WHERE node_key = 'ci_reduction_roi'),
 'investment_framework', 'ci_as_investment_thesis',
 '{"content": "CI reduction is the most powerful margin lever after feedstock cost. Each 5 CI points = ~$0.15/gal = ~$20-30M/year for a plant HOBO size. Moving from CI 35 to CI 20 = ~$0.45/gal = ~$60-70M/year. This means investments in CI reduction (renewable process energy, green H2, carbon capture, feedstock optimization) have immediate payback via credits. This makes the CI strategy effectively self-funding.", "confidence": 0.90, "source_doc": "hobo_section7", "type": "investment_case"}',
 'always', 'extracted', NOW()),

-- Feedstock Supply Curve
((SELECT id FROM core.kg_node WHERE node_key = 'midwest_feedstock_cost_curve'),
 'market_structure', 'feedstock_supply_curve',
 '{"content": "HOBO feedstock supply curve has distinct tiers: Tier 1 (cheapest) = local ethanol plant DCO and renderer animal fats, minimal transport/middleman. Tier 2 = regional waste oils (UCO, CWG) at moderate cost. Tier 3 = more distant sources or premium feedstocks. Tier 4 = fallback soybean oil at ~$0.50/lb (higher cost, higher CI = fewer credits). Strategy: maximize Tier 1-2 to minimize cost and maximize CI performance, only use Tier 3-4 when needed. Risk: 1B+ lbs annual requirement may exhaust local cheap supply.", "confidence": 0.85, "source_doc": "hobo_section7", "annual_requirement_blbs": "1.0-1.1"}',
 'always', 'extracted', NOW()),

-- ===================== BATCH 009g: SECTION 8 CONTEXTS =====================

-- Price Stack Decomposition
((SELECT id FROM core.kg_node WHERE node_key = 'rd_price_stack'),
 'quantitative_framework', 'price_stack_decomposition',
 '{"content": "Complete RD price decomposition (California 2025 illustrative): Base ULSD $2.50 + D4 RINs $2.55 (1.7 x $1.50) + LCFS $0.50-0.75 (at $60-70/MT, 55g/MJ avoided) + 45Z $0.65-1.00 (CI-dependent) = effective selling price $5.50-6.50/gal. Production cost: feedstock $3.00-4.50 (varies by oil type) + OPEX $0.30-0.50 + overhead = $3.50-5.00/gal. Net margin $0.50-1.50/gal depending on all variables. Key insight: credits make up roughly HALF of total revenue. Without credits, RD would need to sell at $4.00+/gal — far above $2.50 ULSD.", "confidence": 0.90, "source_doc": "hobo_section8", "type": "margin_build"}',
 'always', 'extracted', NOW()),

-- SAF Economic Impossibility Without Credits
((SELECT id FROM core.kg_node WHERE node_key = 'saf_price_stack'),
 'expert_rule', 'saf_economic_impossibility_without_credits',
 '{"content": "SAF economics are absolutely credit-dependent. Production cost ~$8.00/gal. Airlines willing to pay ~$4.00 (jet parity, maybe slight premium). Gap = $4.00 MUST come from credits (RINs + LCFS + 45Z). The IRA SAF credit + RIN + LCFS was estimated to provide up to ~$3.78/gal in combined incentive. Even this barely closes the gap. Without any credits, SAF would carry $3-4/gal premium over jet — unmarketable at scale. This is THE central economic fact for any SAF project.", "confidence": 0.95, "source_doc": "hobo_section8", "critical": true}',
 'always', 'extracted', NOW()),

-- SAF Flipped Economics 2023
((SELECT id FROM core.kg_node WHERE node_key = 'saf_vs_rd_economics'),
 'market_data', 'saf_flipped_economics_2023',
 '{"content": "SAF economics flipped in 2023 when dedicated SAF BTC ($1.25-1.75/gal) was introduced. Before 2023: SAF yielded $60-140/ton LESS than RD (jet fuel cheaper than diesel, no SAF-specific credit). After SAF credit: SAF yielded $288/ton MORE than RD in Midwest, $55/ton more in CA. This validates HOBO product flexibility strategy — without ability to swing, producers locked into wrong product at wrong time.", "confidence": 0.90, "source_doc": "hobo_section8", "turning_point": 2023}',
 'always', 'extracted', NOW()),

-- Margin Scenarios Spread
((SELECT id FROM core.kg_node WHERE node_key = 'base_case_margins'),
 'scenario_analysis', 'margin_scenarios_spread',
 '{"content": "Margin range is enormous: Best case (high diesel, strong credits, cheap feedstock) = IL RD $0.80-0.90/gal, CA >$1.00/gal, EBITDA margins 35%+. Base case = IL $0.40-0.50/gal, CA $0.65-0.70/gal, EBITDA 25-30%. Worst case (low diesel, credit collapse, feedstock spike) = IL near $0/gal, CA <$0.20/gal. Worst case is NOT hypothetical — Neste experienced $242/ton ($0.18/gal) in Q4 2024. HOBO must be prepared for full range.", "confidence": 0.90, "source_doc": "hobo_section8", "range_il_per_gal": "$0 to $0.90"}',
 'always', 'extracted', NOW()),

-- LCFS Credit Price Impact
((SELECT id FROM core.kg_node WHERE node_key = 'california_lcfs'),
 'market_data', 'lcfs_credit_price_impact',
 '{"content": "LCFS credit price is highly volatile and directly impacts margin: $20/ton change in LCFS = ~$0.17/gal for typical RD. Credits ranged from $200 to $70/ton in recent years. At CI 35, HOBO earns ~$0.50/gal LCFS credit in CA. If LCFS credits fell from $100 to $50/ton, ~$0.50/gal of value lost. LCFS amendment to tighten from 20% to 30% by 2030 would likely support credit prices.", "confidence": 0.85, "source_doc": "hobo_section8", "sensitivity": "$20/ton = $0.17/gal"}',
 'always', 'extracted', NOW()),

-- CI Pathway Economics
((SELECT id FROM core.kg_node WHERE node_key = 'ci_table'),
 'reference_data', 'ci_pathway_economics',
 '{"content": "CI determines credit value AND market access. UCO HEFA at CI 18.2 generates maximum credits in ALL programs. SBO HEFA at CI 64.9 barely qualifies for some programs (50% threshold = 44.5 gCO2/MJ). Corn ATJ at CI 90.8 fails 50% threshold entirely without CCS. PtL at CI 7 maximizes 45Z. HOBO target of CI low 20s positions it in the premium credit zone — matching UCO-level performance even with blended feedstock.", "confidence": 0.90, "source_doc": "hobo_section8", "type": "pathway_comparison"}',
 'always', 'extracted', NOW()),

-- Trading Desk Recommendation
((SELECT id FROM core.kg_node WHERE node_key = 'product_slate_optimization'),
 'recommendation', 'trading_desk_recommendation',
 '{"content": "HOBO should establish a dedicated trading/optimization desk that daily evaluates: (1) Where should next railcar go (CA vs IL vs Canada)? (2) What product slate maximizes value (more RD or SAF this week)? (3) What credit prices are doing (RIN, LCFS, 45Z) and how to capture them? This kind of arbitrage is common in commodities and can add several million dollars annually at essentially no capital cost — just smart decision-making. HOBO team has decades of commodity risk experience to execute this.", "confidence": 0.85, "source_doc": "hobo_section8", "value": "millions_annually_at_zero_capex"}',
 'always', 'extracted', NOW()),

-- 80/20 Split Optimization
((SELECT id FROM core.kg_node WHERE node_key = 'product_slate_optimization'),
 'quantitative_framework', '80_20_split_optimization',
 '{"content": "Intermediate production splits analyzed: 80/20 RD/SAF in base case yields blended margin very close to 100% SAF case when SAF incentives are rich (because 20% SAF gets big credit uplift). Even small SAF proportion raises overall margin: 80% RD at $0.50 + 20% SAF at $1.00 = weighted average $0.60 vs pure RD $0.50. In downside scenario (weak SAF credits), 80/20 protects bulk of margin in RD. Flexibility is an embedded option with significant value.", "confidence": 0.85, "source_doc": "hobo_section8", "type": "optimization_analysis"}',
 'always', 'extracted', NOW()),

-- All SAF Routes Above Fossil
((SELECT id FROM core.kg_node WHERE node_key = 'saf_market'),
 'expert_rule', 'all_saf_routes_above_fossil',
 '{"content": "Most analyses (IEA, McKinsey) find ALL SAF routes remain more expensive than fossil jet through 2050 absent continuous subsidies. Learning curves and hydrogen cost declines may narrow gaps, but policy support (mandates/credits) will remain essential for SAF viability for DECADES. Lenders should expect permanent policy dependence for any SAF investment.", "confidence": 0.90, "source_doc": "hobo_section8", "time_horizon": "through_2050", "critical": true}',
 'always', 'extracted', NOW()),

-- Strategic Recommendations Section 8
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_renewables'),
 'recommendation_set', 'strategic_recommendations_section8',
 '{"content": "Section 8 strategic recommendations: (1) Prioritize low-CI pathways — each CI point = direct dollars; (2) Maintain product flexibility RD/SAF for first years with mixed contract/spot; (3) Diversify markets across CA, OR, WA, IL, Canada, possibly EU export; (4) Feedstock cost management via long-term contracts, portfolio diversification, potential vertical integration; (5) Hedging program for commodity/credit exposure; (6) Policy engagement and advocacy for credit extensions; (7) Continuous improvement culture — treat margin optimization as ongoing exercise.", "confidence": 0.90, "source_doc": "hobo_section8", "type": "recommendation_set"}',
 'always', 'extracted', NOW())

ON CONFLICT (node_id, context_type, context_key) DO UPDATE SET context_value = EXCLUDED.context_value, last_updated = NOW();

-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT 'Batch 009 Load Summary' AS report;
SELECT 'Sources' AS entity_type, COUNT(*) AS count FROM core.kg_source WHERE source_key LIKE 'gdoc_hobo%';
SELECT 'Nodes (batch 009 keys)' AS entity_type, COUNT(*) AS count FROM core.kg_node WHERE node_key IN (
    'hobo_renewables','hefa_technology','us_rd_capacity','us_rd_feedstock_consumption',
    'hobo_feedstock_catchment','iea_feedstock_crunch','us_feedstock_imports','refueleu_mandate',
    'global_saf_offtake','cfpc_45z','hobo_ci_advantage','rd_market','saf_market',
    'california_lcfs','rfs_program','uco_feedstock','soybean_oil_feedstock','tallow_feedstock',
    'dcor_feedstock','rd_fuel','saf_fuel','coprocessing','atj_technology','ft_technology',
    'power_to_liquid','feedstock_pretreatment','canola_oil_feedstock','rfs_d4_rin','rfs_d6_rin',
    'e10_blendwall','rvo_2026_proposal','state_saf_incentives','state_rd_incentives','canada_cfr',
    'china_uco_role','eu_red_iii','book_and_claim','saf_grand_challenge','clinton_county_site',
    'hobo_facility_specs','hobo_spread_concept','mississippi_river_access','hobo_h2_recycling',
    'credit_arbitrage_strategy','yellow_grease_price','atj_competitive_threat',
    'hefa_capex_benchmark','hefa_opex_structure','feedstock_sensitivity_rule','diamond_green_diesel',
    'neste_corporation','hobo_execution_timeline','midwest_feedstock_cost_curve','ci_reduction_roi',
    'rd_price_stack','saf_price_stack','regional_netback_hierarchy','saf_vs_rd_economics',
    'ci_value_framework','ci_table','base_case_margins','best_worst_margins',
    'product_slate_optimization','canada_credit_arbitrage'
);
SELECT 'Total KG Nodes' AS entity_type, COUNT(*) AS count FROM core.kg_node;
SELECT 'Total KG Edges' AS entity_type, COUNT(*) AS count FROM core.kg_edge;
SELECT 'Total KG Contexts' AS entity_type, COUNT(*) AS count FROM core.kg_context;
SELECT 'Total KG Sources' AS entity_type, COUNT(*) AS count FROM core.kg_source;
