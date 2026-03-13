-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 002
-- Source: 9 MPOB Annual Overview of the Industry Reports (2016–2024)
-- Extracted: 2026-03-13
-- Scope: Malaysian palm oil industry — supply, trade, price, and policy frameworks
-- ============================================================================


-- ============================================================================
-- NODES
-- ============================================================================

-- Commodities: Malaysian Palm Oil Complex
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('commodity', 'cpo', 'Crude Palm Oil (CPO)', '{"markets": ["Bursa Malaysia Derivatives (BMD)", "MPOB local delivered"], "units": "MYR/tonne", "ticker": "FCPO", "pricing_basis": "local_delivered_Malaysia", "key_price_series": "MPOB monthly average", "note": "CPO trades at discount to soybean oil globally. Discount narrows on supply tightness."}'),
('commodity', 'ffb', 'Fresh Fruit Bunches (FFB)', '{"units": "MYR/tonne or MYR per 1% OER", "pricing_basis": "mill_gate_Malaysia", "note": "FFB price derived from CPO and PK prices weighted by regional OER and KER. Benchmark at 1% OER for smallholder pricing formula."}'),
('commodity', 'rbd_palm_olein', 'RBD Palm Olein', '{"units": "MYR/tonne", "pricing_basis": "FOB_Malaysia", "context": "Primary liquid fraction of refined palm oil. Main Malaysian export product for Asian cooking oil use. Key product for India MICECA duty advantage."}'),
('commodity', 'rbd_palm_stearin', 'RBD Palm Stearin', '{"units": "MYR/tonne", "pricing_basis": "FOB_Malaysia", "context": "Solid fraction of refined palm oil. Used in food fats, spreads, and industrial applications."}'),
('commodity', 'pfad', 'Palm Fatty Acid Distillate (PFAD)', '{"units": "MYR/tonne", "pricing_basis": "FOB_Malaysia", "context": "By-product of palm oil refining. Increasingly used as biodiesel feedstock, particularly valued for low CI score in EU context. Discount to CPO widens in bearish markets, narrows in supply-constrained environments."}'),
('commodity', 'palm_kernel', 'Palm Kernel (PK)', '{"units": "MYR/tonne", "pricing_basis": "ex_mill_Malaysia", "context": "Seed extracted from oil palm fruit after CPO separation. Crushed to produce CPKO and PKC. PK price tracks CPO broadly but diverges based on lauric oil complex dynamics."}'),
('commodity', 'cpko', 'Crude Palm Kernel Oil (CPKO)', '{"units": "MYR/tonne and USD/tonne", "context": "Lauric oil from palm kernel crushing. Competes directly with coconut oil globally. CPKO price volatility relative to CPO can be extreme — fell 29.7% in 2019 while CPO fell only 6.9%; surged 74.8% in 2021 vs CPO +64.1%."}'),
('commodity', 'pkc', 'Palm Kernel Cake (PKC)', '{"units": "USD/tonne", "context": "Protein-rich by-product of palm kernel crushing. Used as livestock and dairy feed. Major export to New Zealand (~30%), South Korea (~20%), EU. Relatively stable demand regardless of palm oil price environment."}'),
('commodity', 'palm_oleochemicals', 'Palm-Based Oleochemicals', '{"subcategories": ["fatty_acids", "fatty_alcohols", "methyl_ester", "glycerine", "soap_noodles"], "units": "million_tonnes", "key_markets": ["China", "EU", "USA", "Japan"], "context": "Value-added derivative products from CPO and CPKO. Higher-margin segment of Malaysian palm industry. Exports ~2.7–3.3M tonnes/year over 2016-2024."}'),
('commodity', 'palm_biodiesel_my', 'Malaysian Palm-Based Biodiesel', '{"units": "million_tonnes", "context": "Biodiesel produced from palm oil under Malaysia B-mandate program. Export volumes depend on domestic mandate level, global biodiesel price competitiveness, and FAME CI scores in key markets (EU RED)."}'),
('commodity', 'sunflower_oil', 'Sunflower Oil', '{"major_producers": ["Ukraine", "Russia", "Argentina"], "context": "Competes with palm oil in EU, Turkey, India, MENA. Supply disruptions (Ukraine war 2022) are the primary short-term bullish catalyst for palm oil in these markets."}'),
('commodity', 'coconut_oil', 'Coconut Oil', '{"major_producers": ["Philippines", "Indonesia"], "context": "Lauric oil competing directly with CPKO/PKO in food manufacturing, soaps, cosmetics, and surfactants. Coconut oil and PKO move closely together. Philippines domestic coconut oil availability is the key inverse driver for Philippine palm oil imports."}'),
('commodity', 'rapeseed_oil_eu', 'European Rapeseed Oil', '{"context": "Primary domestic vegetable oil in EU. Competes with Malaysian palm oil imports. Higher EU rapeseed crush or harvest reduces EU palm oil demand. Also used as biodiesel feedstock competing with palm methyl ester under EU RED."}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Production Regions
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('region', 'malaysia', 'Malaysia', '{"context": "Second largest palm oil producer globally after Indonesia. Total planted area 5.6–5.9M ha, CPO production 17–20M tonnes/year. Three main production regions: Peninsular Malaysia, Sabah, Sarawak.", "global_share_pct": "~25–27% of world production"}'),
('region', 'malaysia.peninsular', 'Peninsular Malaysia', '{"context": "Largest planted area share (~45–47%). Generally higher FFB yields due to more mature, well-managed estates. OER typically lowest of three regions. Most exposed to El Nino-driven yield shocks.", "planted_area_share_pct": 46, "oer_range_pct": "19.46–19.93"}'),
('region', 'malaysia.sabah', 'Sabah', '{"context": "Second largest planted area (~26%). Consistently highest national OER of three regions (~20.25–21.11%). FFB yields comparable to Peninsular. Labour-intensive operations more vulnerable to COVID-19 worker restrictions.", "planted_area_share_pct": 26, "oer_range_pct": "20.25–21.11", "note": "Sarawak surpassed Sabah as largest planted area state from 2017 onward"}'),
('region', 'malaysia.sarawak', 'Sarawak', '{"context": "Largest planted area state since 2017 (~27–29%). Generally lowest FFB yields of three regions (~13.9–15.6 t/ha). Newer, still-expanding estate base. OER competitive with Peninsular.", "planted_area_share_pct": 27, "ffb_yield_range_tha": "13.9–15.6", "trend": "steady planted area expansion despite national plateau"}'),
('region', 'indonesia', 'Indonesia', '{"context": "Worlds largest palm oil producer (~47–53M tonnes CPO/year vs Malaysia ~18–20M tonnes). Indonesian export taxes, levies, DMO requirements, and biodiesel mandate (B35/B40) directly determine global CPO supply availability and impact Malaysian market share. Competes with Malaysia in India, China, EU, and all major markets.", "role": "primary_competitor_and_price_setter"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Export Market Countries
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('country', 'india', 'India', '{"context": "Largest Malaysian palm oil export market every year since 2014. Share ranges 12–24% of total. Highly price-sensitive and policy-sensitive buyer. Primarily imports RBD palm olein for consumer cooking oil blending. Import duty differentials between Malaysia (MICECA) and Indonesia (ASEAN-India FTA) drive large year-to-year volume swings.", "import_profile": "RBD_palm_olein_primary", "market_share_range_pct": "12–24"}'),
('country', 'eu', 'European Union', '{"context": "Major market for Malaysian palm oil, PKO, PFAD, and oleochemicals. Share ~7–13% of palm oil exports. EU domestic rapeseed and soybean oil supply inversely affects demand. EUDR sustainability requirements add regulatory complexity and long-term headwinds.", "import_profile": "palm_oil_oleochemicals_pko_pfad"}'),
('country', 'pakistan', 'Pakistan', '{"context": "Consistently the fourth largest market since 2016. Imports primarily for consumer edible oil use. Volumes sensitive to domestic soybean crush from Brazil — higher Brazil soy imports for crushing reduces palm oil intake.", "import_profile": "palm_oil_edible_oil"}'),
('country', 'turkey', 'Turkey', '{"context": "Significant but volatile market (~3.8–5.2% share). Imports for food manufacturing. Volume swings inversely with Black Sea sunflower oil supply from Ukraine/Russia.", "import_profile": "palm_oil_food_manufacturing", "key_competitor": "sunflower_oil"}'),
('country', 'philippines', 'Philippines', '{"context": "Domestic coconut oil production is the primary inverse driver — higher coconut oil availability reduces Philippine palm oil imports. ~3.4–4.1% of Malaysian exports.", "import_profile": "palm_oil_competing_with_coconut_oil"}'),
('country', 'vietnam', 'Vietnam', '{"context": "Growing palm oil import market (~3.0–3.8% share historically). Sensitive to domestic oilseed crushing activity.", "import_profile": "palm_oil_food_biodiesel"}'),
('country', 'new_zealand', 'New Zealand', '{"context": "Largest importer of Malaysian Palm Kernel Cake (PKC), typically ~30–33% of total Malaysian PKC exports. Imports for dairy and livestock feed. Not a significant palm oil buyer.", "import_profile": "pkc_only"}'),
('country', 'south_korea', 'South Korea', '{"context": "Major PKC importer (~15–23% share) and secondary palm oil buyer. Imports oleochemicals.", "import_profile": "pkc_primary_palm_oil_oleochemicals"}'),
('country', 'kenya', 'Kenya', '{"context": "Emerged as a top-7 Malaysian palm oil market from 2021. Imports primarily CPO for local refining and re-export to landlocked African countries (Uganda, Rwanda, Congo, Burundi). Demand driven by East Africa regional growth, not just Kenyan domestic consumption.", "import_profile": "cpo_for_refining_and_reexport", "note": "Rapid growth market — from minor buyer to 4.3–7.5% of Malaysian palm exports in 2021–2024"}'),
('country', 'japan', 'Japan', '{"context": "Stable, mature market for Malaysian palm oil (~3.3–3.6% share) and oleochemicals. Consistent buyer with limited growth. Higher-specification product requirements.", "import_profile": "palm_oil_oleochemicals"}'),
('country', 'uae', 'United Arab Emirates', '{"context": "Growing oleochemical import destination, with some palm oil re-export activity to wider MENA region.", "import_profile": "oleochemicals_palm_oil_reexport"}'),
('country', 'saudi_arabia', 'Saudi Arabia', '{"context": "PKC importer for livestock sector and palm oil buyer for food use.", "import_profile": "palm_oil_pkc"}'),
('country', 'bangladesh', 'Bangladesh', '{"context": "Emerging palm oil importer. Included as volumes occasionally enter top-15 markets.", "import_profile": "palm_oil"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Data Series
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'mpob.cpo_production', 'Malaysia CPO Production', '{"source": "MPOB", "frequency": "monthly_annual", "units": "million_tonnes", "breakdown": ["Peninsular Malaysia", "Sabah", "Sarawak"], "historical_range_mmt": {"low": 17.32, "low_year": 2016, "high": 19.86, "high_year": 2019}, "release": "MPOB monthly supply and demand report (typically mid-following-month)"}'),
('data_series', 'mpob.planted_area', 'Malaysia Oil Palm Planted Area', '{"source": "MPOB", "frequency": "annual", "units": "million_hectares", "breakdown": ["Peninsular Malaysia", "Sabah", "Sarawak"], "historical_range_mha": {"low": 5.61, "low_year": 2024, "high": 5.90, "high_year": 2019}, "trend": "Peaked 2019, declining gradually due to replanting cycle gaps and land conversion. Sarawak stable while Peninsular and Sabah declining."}'),
('data_series', 'mpob.ffb_yield', 'Malaysia FFB Yield', '{"source": "MPOB", "frequency": "annual", "units": "tonnes_per_hectare", "breakdown": ["Peninsular Malaysia", "Sabah", "Sarawak"], "historical_range": {"low_tha": 15.47, "low_year": 2021, "high_tha": 17.89, "high_year": 2017}, "note": "High inter-year variability (up to 13.9%) driven by El Nino, labour availability, biological rest year cycles"}'),
('data_series', 'mpob.oer', 'Malaysia National Oil Extraction Rate (OER)', '{"source": "MPOB", "frequency": "monthly_annual", "units": "percent", "breakdown": ["Peninsular Malaysia", "Sabah", "Sarawak"], "national_range_pct": {"low": 19.70, "low_year": 2022, "high": 20.21, "high_year": 2019}, "note": "Sabah consistently highest (20.25–21.11%). Peninsular and Sarawak typically 19.5–20.0%. OER below 19.70% is a warning signal of harvesting quality problems."}'),
('data_series', 'mpob.palm_oil_stocks', 'Malaysia Palm Oil Closing Stocks', '{"source": "MPOB", "frequency": "monthly_annual", "units": "million_tonnes", "breakdown": ["crude_palm_oil", "processed_palm_oil"], "historical_range_mmt": {"low": 1.27, "low_year": 2020, "high": 3.22, "high_year": 2018}, "note": "December year-end stocks are the primary annual benchmark. Strong inverse correlation with CPO prices — most reliable fundamental signal in Malaysian palm oil."}'),
('data_series', 'mpob.palm_oil_exports', 'Malaysia Palm Oil Exports', '{"source": "MPOB/DOS", "frequency": "monthly_annual", "units": "million_tonnes", "historical_range_mmt": {"low": 15.13, "low_year": 2023, "high": 18.47, "high_year": 2019}, "breakdowns": ["destination_country", "product_type (crude vs processed)"], "note": "Export volume and revenue can diverge sharply — volume record 2019 at low prices; volume trough 2021 at record revenue"}'),
('data_series', 'mpob.palm_oil_imports', 'Malaysia Palm Oil Imports', '{"source": "MPOB/DOS", "frequency": "monthly_annual", "units": "million_tonnes", "primary_source": "Indonesia (nearly 100% share)", "range_mmt": {"low": 0.25, "low_year": 2024, "high": 1.18, "high_year": 2021}, "note": "Imports surge when domestic production is low and processing sector demand cannot be met internally. Imports decline when production is adequate or domestic demand falls."}'),
('data_series', 'mpob.cpo_price', 'Malaysia CPO Annual Average Price', '{"source": "MPOB", "frequency": "daily_monthly_annual", "units": "MYR/tonne", "pricing_basis": "local_delivered_Malaysia", "historical_annual_averages": {"2016": 2653, "2017": 2783, "2018": 2232.50, "2019": 2079, "2020": 2685.50, "2021": 4407, "2022": 5087.50, "2023": 3809.50, "2024": 4179.50}, "key_drivers": ["palm_oil_stocks", "soybean_oil_price", "brent_crude_oil", "MYR_USD_exchange_rate", "Indonesia_export_policy"]}'),
('data_series', 'mpob.pk_price', 'Malaysia Palm Kernel Annual Average Price', '{"source": "MPOB", "frequency": "monthly_annual", "units": "MYR/tonne", "pricing_basis": "ex_mill_Malaysia", "historical_annual_averages": {"2016": 2611, "2017": 2536, "2018": 1827.50, "2019": 1214, "2020": 1532, "2021": 2773, "2022": 3118, "2023": 2016, "2024": 2645.50}, "linkage": "Tracks CPKO and coconut oil (lauric oil complex), diverges from CPO"}'),
('data_series', 'mpob.cpko_price', 'Malaysia CPKO Annual Average Price', '{"source": "MPOB", "frequency": "monthly_annual", "units": "MYR/tonne", "pricing_basis": "local_delivered_Malaysia", "historical_annual_averages": {"2016": 5492.50, "2017": 5325, "2018": 3734.50, "2019": 2626.50, "2020": 3247, "2021": 5674.50, "2022": 6327, "2023": 3896, "2024": 5475.50}, "global_reference": "World PKO price (USD/tonne) and coconut oil price (USD/tonne) — lauric complex"}'),
('data_series', 'mpob.pfad_price', 'Malaysia PFAD Annual Average Price', '{"source": "MPOB", "frequency": "monthly_annual", "units": "MYR/tonne", "pricing_basis": "FOB_Malaysia", "historical_annual_averages": {"2016": 2462.50, "2017": 2733, "2018": 1922, "2019": 1807, "2020": 2546, "2021": 4233, "2022": 4313.50, "2023": 3424, "2024": 3802}, "note": "PFAD-CPO discount varies — widens in bearish environments (2019: RM272 discount), narrows in tight markets (2022: RM774 discount narrowed vs prior bearish spread)"}'),
('data_series', 'mpob.rbd_olein_price', 'Malaysia RBD Palm Olein Annual Average Price', '{"source": "MPOB", "frequency": "daily_monthly_annual", "units": "MYR/tonne", "pricing_basis": "FOB_Malaysia", "historical_annual_averages": {"2016": 2769.50, "2017": 2953.50, "2018": 2328.50, "2019": 2236.50, "2020": 2844, "2021": 4764.50, "2022": 5366.50, "2023": 4018, "2024": 4417}, "note": "Primary liquid export product. Closely tracks CPO with refining margin premium (~RM150–250/tonne in normal markets)"}'),
('data_series', 'mpob.export_revenue', 'Malaysia Total Palm Oil Products Export Revenue', '{"source": "MPOB/DOS", "frequency": "annual", "units": "RM_billion", "historical_annual_RM_bn": {"2016": 64.59, "2017": 77.85, "2018": 67.49, "2019": 64.84, "2020": 73.25, "2021": 108.52, "2022": 137.89, "2023": 94.95, "2024": 109.39}, "note": "Revenue and volume can diverge dramatically. 2021: volume -9.2% but revenue +48% (record prices). 2019: volume +12.1% but revenue -4% (low prices)."}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Reports
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('report', 'mpob.annual_overview', 'MPOB Annual Overview of the Malaysian Oil Palm Industry', '{"frequency": "annual", "publisher": "Malaysian Palm Oil Board (MPOB)", "coverage": "Full calendar year supply, trade, and price statistics", "key_tables": ["planted_area_by_region", "CPO_production_by_region", "palm_oil_closing_stocks", "exports_by_product_and_destination", "export_revenue_by_product", "imports", "prices_full_product_range", "OER_by_region", "FFB_yield_by_region"], "context": "Primary official reference for Malaysian palm oil industry annual performance. Gold standard for supply/demand balance sheet calibration."}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Market Participants
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('market_participant', 'mpob', 'Malaysian Palm Oil Board (MPOB)', '{"context": "Government agency that regulates and promotes the Malaysian palm oil industry. Publishes official monthly production, trade, stock, and price statistics. Sets FFB pricing formula for smallholders. Administers MSPO sustainability certification."}'),
('market_participant', 'indonesia.palm_oil_producers', 'Indonesian Palm Oil Industry', '{"context": "Collectively the largest palm oil producing industry globally (~47–53M tonnes CPO/year). Government policy decisions on export taxes, levies, domestic market obligations (DMO), and biodiesel mandates (B30–B40) are the primary external fundamental driver for global CPO prices and Malaysian market share."}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Policy Mechanisms
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('policy', 'miceca', 'Malaysia-India Comprehensive Economic Cooperation Agreement (MICECA)', '{"context": "Trade agreement giving Malaysian palm oil preferential import duty rates into India vs Indonesian palm oil. In 2019: Malaysian RBD palm olein faced 45% duty vs 50% for Indonesia via ASEAN-India FTA — the 5% differential triggered a 75.4% surge in Indian imports of Malaysian palm oil that year.", "effective_date": "2011", "primary_product": "RBD_palm_olein", "impact": "Activates periodic large-scale shifts of Indian buying toward Malaysia when duty differential is operative"}'),
('policy', 'asean_india_fta', 'ASEAN-India Free Trade Agreement', '{"context": "FTA governing most ASEAN palm oil exports to India, including Indonesia. MICECA gives Malaysia additional preferential access beyond ASEAN-India FTA rates. The rate differential between MICECA and ASEAN-India FTA is the key variable determining Malaysian vs Indonesian market share in India."}'),
('policy', 'indonesia.export_policy', 'Indonesia CPO Export Tax / Levy / Export Ban Policy', '{"context": "Indonesia regularly adjusts CPO export taxes, reference price-based levies, and domestic market obligations (DMO). Export bans (e.g. April–May 2022 CPO export ban) create immediate global supply disruptions and price spikes. High levies divert supply to domestic B35/B40 mandate, reducing export availability and benefiting Malaysia.", "modes": {"restriction": "Bullish for global CPO price and Malaysian market share", "liberalization": "Bearish — Indonesian supply floods markets, competing with Malaysia on price"}, "monitoring": "Track Indonesia CPO reference price schedule, DMO compliance, and biodiesel mandate absorption monthly"}'),
('policy', 'malaysia.biodiesel_mandate', 'Malaysia Biodiesel Mandate (B-Program)', '{"context": "Malaysia runs a mandatory biodiesel blending program that has progressed: B5 → B7 (2014) → B10 transport sector (2019) → aspirational B20. Domestic blending absorbs CPO supply, supporting prices and reducing export availability.", "estimated_absorption_mmt_per_step": 0.3, "note": "Each mandate tier increase consumes additional ~0.3–0.5M tonnes CPO domestically"}'),
('policy', 'india.palm_oil_import_policy', 'India Palm Oil Import Policy', '{"context": "India periodically adjusts import duties on crude vs refined palm oil and country-specific rates. Two defining events in 2016-2024: (1) Jan 2019: MICECA advantage activated → Indian imports surged 75.4% to 4.41M tonnes; (2) Jan 2020: India restricted processed palm oil imports from Malaysia (diplomatically motivated) → Malaysian RBD olein exports to India collapsed 99.7% from 2.34M to 7,323 tonnes.", "risk_level": "Very high — single policy change can swing 1–2M tonnes of Malaysian exports"}'),
('policy', 'indonesia.biodiesel_mandate', 'Indonesia National Biodiesel Mandate (B35 / B40)', '{"context": "The worlds largest national biodiesel program. At B35 (2023+) absorbs approximately 8–12M tonnes CPO domestically. Announced B40 implementation in 2025. This structural domestic demand floor removes supply from export markets, providing ongoing support to global CPO prices. Cited by MPOB as a 2024 CPO price support factor.", "impact": "Each 5% mandate increase adds ~1–1.5M tonnes of domestic Indonesian CPO demand"}'),
('policy', 'eu.deforestation_regulation', 'EU Deforestation Regulation (EUDR)', '{"context": "EU regulation requiring supply chain due diligence ensuring commodities (including palm oil) are not linked to deforestation. Adds compliance burden for Malaysian exporters. Risk of accelerated EU palm oil demand reduction if compliance barriers become prohibitive. Implementation delayed beyond original 2023 dates; regulatory uncertainty continues.", "status_2024": "Delayed implementation; outcome uncertain but structural headwind for EU-Malaysia palm oil flows"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Macro Events / Drivers
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('macro_driver', 'el_nino', 'El Niño Weather Phenomenon', '{"context": "ENSO warm phase causing prolonged dry weather and below-average rainfall across SE Asia, directly reducing FFB yield 6–12 months after onset. Most severe documented impact: El Niño 2015-16 caused 13.9% FFB yield decline in 2016, driving CPO production down 13.2% to a 9-year low of 17.32M tonnes.", "lag_months": "6–12 from onset to FFB yield impact", "production_impact_pct": "–10 to –15% in primary impact year", "recovery_pattern": "Strong bounce-back in following year: 2017 FFB yield +12.4%, CPO production +15.0%"}'),
('macro_driver', 'la_nina', 'La Niña Weather Phenomenon', '{"context": "ENSO cool phase, typically associated with above-average rainfall in SE Asia. Generally supports palm oil production. May cause flooding-related harvesting delays in low-lying areas but overall neutral-to-positive for FFB production."}'),
('macro_driver', 'covid19', 'COVID-19 Pandemic', '{"context": "Dual impact on Malaysian palm oil: (1) Demand disruption H1 2020 from mobility restrictions in importing countries; (2) More critically, temporary suspension of foreign worker intake 2020–2022 created plantation labour shortage, reducing harvest frequency, increasing overripe fruit, lowering both FFB yield and OER. 2021 CPO production fell 5.4% to 18.12M tonnes primarily due to labour shortage.", "primary_impact": "Production constraint via labour shortage (2020–2022)", "secondary_impact": "Demand disruption H1 2020"}'),
('macro_driver', 'ukraine_russia_war', 'Ukraine-Russia War (2022+)', '{"context": "Disrupted global sunflower oil supply chain from February 2022. Ukraine and Russia together supply ~60% of global sunflower oil. Supply disruption triggered substitution demand for palm oil, especially from EU and Turkey. Contributed to CPO hitting a record annual average of RM5,087.50/tonne in 2022 and all-time monthly high of RM6,873 (May 2022). Effect faded in 2023 as alternative supply sources developed and Indonesian export ban was lifted.", "impact_on_palm_oil": "Very bullish 2022 via substitution demand. Faded 2023 as Ukraine resumed some exports."}'),
('macro_driver', 'asf', 'African Swine Fever (ASF)', '{"context": "Devastated Chinese pig herd from 2018 onward. Reduced Chinese domestic demand for soybean meal (pig feed), causing lower Chinese soybean crush in 2019. Lower soybean crush → lower soybean oil co-production → put some support under soybean oil and palm oil demand in China. Also contributed to broader soybean price weakness that dragged SBO and CPO lower in 2018–2019.", "net_palm_oil_impact": "Minor but measurable — reduced global soybean crush marginally supported Chinese palm oil demand as substitute"}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Price Level Reference Nodes
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('price_level', 'cpo.historical_range_2016_2024', 'CPO Annual Average Price Range 2016–2024', '{"annual_averages_myr_tonne": {"2016": 2653, "2017": 2783, "2018": 2232.50, "2019": 2079, "2020": 2685.50, "2021": 4407, "2022": 5087.50, "2023": 3809.50, "2024": 4179.50}, "nine_year_range": {"low": 2079, "low_year": 2019, "high": 5087.50, "high_year": 2022}, "cycle_narrative": "El Nino supply shock peak (2016) → supply recovery + high stocks decline (2018–19) → COVID demand disruption then recovery (2020) → labour shortage + SBO surge record (2021) → Ukraine war + Indonesia ban new record (2022) → normalization correction (2023) → partial recovery on B40 anticipation (2024)"}'),
('price_level', 'malaysia.palm_oil_stocks.history_2016_2024', 'Malaysian Palm Oil Year-End Closing Stocks 2016–2024', '{"year_end_stocks_mmt": {"2016": 1.67, "2017": 2.73, "2018": 3.22, "2019": 2.01, "2020": 1.27, "2021": 1.61, "2022": 2.20, "2023": 2.29, "2024": 1.71}, "inverse_correlation_with_price": "Strong and consistent. 2018 stocks peak (3.22M) coincided with CPO trough (RM2,232). 2020 stocks trough (1.27M) preceded 2021 price record (RM4,407).", "bullish_threshold_mmt": 1.5, "bearish_threshold_mmt": 3.0}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;


-- ============================================================================
-- EDGES
-- ============================================================================

-- SUPPLY CHAIN: Weather & Events → Yield & Production
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'el_nino'),
 (SELECT id FROM core.kg_node WHERE node_key = 'mpob.ffb_yield'),
 'CAUSES', 0.95,
 '{"mechanism": "El Nino causes prolonged dry weather across SE Asia, reducing soil moisture and stressing oil palm, causing FFB yield to fall 6–12 months after El Nino onset.", "direction": "inverse", "lag_months": "6–12", "magnitude": "–10 to –15% in primary impact year", "historical_evidence": "2015-16 El Nino → 2016 FFB yield –13.9% to 15.91 t/ha (from 18.48 in 2015). Recovery: 2017 yield +12.4% to 17.89 t/ha."}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'mpob.ffb_yield'),
 (SELECT id FROM core.kg_node WHERE node_key = 'mpob.cpo_production'),
 'CAUSES', 0.98,
 '{"mechanism": "FFB yield is the primary driver of CPO production. With planted area relatively stable year-to-year, yield swings dominate production changes. Higher yield → more FFB processed → more CPO.", "direction": "positive", "lag": "same_period", "note": "When FFB yield and OER both decline simultaneously, production impact is compounded (e.g. 2016: both lower)"}',
 'extracted', 0.98),

((SELECT id FROM core.kg_node WHERE node_key = 'mpob.oer'),
 (SELECT id FROM core.kg_node WHERE node_key = 'mpob.cpo_production'),
 'CAUSES', 0.90,
 '{"mechanism": "OER directly scales FFB processed into CPO tonnage. Higher OER from better quality ripe FFB increases production independent of yield. National OER ranged 19.70–20.21% over 2016–2024 — a 0.5% shift on ~19M tonnes FFB processed ≈ ±95,000 tonnes CPO.", "direction": "positive"}',
 'extracted', 0.90),

((SELECT id FROM core.kg_node WHERE node_key = 'covid19'),
 (SELECT id FROM core.kg_node WHERE node_key = 'mpob.cpo_production'),
 'CAUSES', 0.85,
 '{"mechanism": "COVID-19 pandemic caused temporary suspension of foreign plantation worker intake, reducing harvester availability and lengthening harvesting intervals. More overripe/unharvested FFB reduced both volume and OER.", "direction": "inverse", "primary_channel": "labour_shortage_in_plantation_sector", "evidence": "2021: CPO production –5.4% to 18.12M tonnes despite stable planted area — explicitly attributed to COVID labour shortage"}',
 'extracted', 0.85),

-- STOCK DYNAMICS
((SELECT id FROM core.kg_node WHERE node_key = 'mpob.cpo_production'),
 (SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_stocks'),
 'CAUSES', 0.90,
 '{"mechanism": "Higher CPO production adds to stock accumulation when export demand does not absorb the increase. Stock builds when production growth outpaces exports.", "direction": "positive_if_exports_unchanged", "formula": "Closing_stocks = Opening_stocks + Production + Imports – Exports – Domestic_processing"}',
 'extracted', 0.90),

((SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_exports'),
 (SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_stocks'),
 'CAUSES', 0.95,
 '{"mechanism": "Exports are the primary lever for drawing down or building stocks. Higher exports reduce stocks; lower exports allow stocks to accumulate.", "direction": "inverse_strong", "historical_evidence": "2019: exports +12% (+1.98M tonnes) drove stocks down 37.5% to 2.01M tonnes. 2018: export volume –0.4% while production –2% still allowed stocks to build to 3.22M tonnes because opening stocks were high."}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_imports'),
 (SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_stocks'),
 'CAUSES', 0.75,
 '{"mechanism": "Palm oil imports (almost entirely from Indonesia) add to domestic supply and stocks. Imports surge when production is low and processing sector cannot be met internally.", "direction": "positive", "typical_range_mmt": "0.25–1.18"}',
 'extracted', 0.75),

-- PRICE FORMATION
((SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_stocks'),
 (SELECT id FROM core.kg_node WHERE node_key = 'mpob.cpo_price'),
 'CAUSES', 0.95,
 '{"mechanism": "Malaysian palm oil closing stocks are the single strongest fundamental driver of CPO price. High stocks compress prices; tight stocks support prices. Inverse correlation is consistent across all nine years of MPOB data.", "direction": "inverse_strong", "threshold_signal": {"very_bullish_below_mmt": 1.5, "very_bearish_above_mmt": 3.0}, "historical_evidence": "2018 stocks 3.22M → 2019 CPO RM2,079 (trough). 2020 stocks 1.27M → 2021 CPO RM4,407 (record)."}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'cpo'),
 'CAUSES', 0.90,
 '{"mechanism": "SBO and CPO compete in the global vegetable oil market. Higher SBO prices lift CPO by narrowing the CPO discount. MPOB explicitly cites SBO prices as a primary CPO driver in every annual overview.", "direction": "positive_correlation", "historical_evidence": "2020–21: SBO +9.7% then +68% to US$1,393/tonne contributed to CPO price surge. 2018–19: SBO weakness contributed to CPO decline."}',
 'extracted', 0.90),

((SELECT id FROM core.kg_node WHERE node_key = 'crude_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'cpo'),
 'CAUSES', 0.80,
 '{"mechanism": "Brent crude oil prices influence CPO through the biofuel demand channel — higher crude makes biodiesel more economically viable, increasing palm oil feedstock demand. Also affects MYR/USD which prices palm oil competitiveness. MPOB cites Brent as a recurring CPO price driver.", "direction": "positive_correlation", "channel": "higher_crude → better_biodiesel_economics → more_palm_oil_demand → price_support"}',
 'extracted', 0.80),

((SELECT id FROM core.kg_node WHERE node_key = 'cpo'),
 (SELECT id FROM core.kg_node WHERE node_key = 'ffb'),
 'CAUSES', 0.95,
 '{"mechanism": "FFB mill gate price is directly derived from CPO (dominant weight ~70–75%) and PK (25–30%) prices, adjusted for regional OER and KER. MPOB publishes FFB pricing formula. National average FFB price at national OER tracks CPO closely.", "direction": "positive_derived", "historical_evidence": "2021: CPO +64.1% → FFB +70.2% to RM955/tonne. 2019: CPO –6.9% → FFB –9.8% to RM422/tonne."}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'coconut_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'cpko'),
 'COMPETES_WITH', 0.90,
 '{"mechanism": "Coconut oil and CPKO are the two primary lauric oils, substitutable in food manufacturing, soaps, cosmetics, and surfactants. Prices move closely together. Divergence of >10–15% creates arbitrage substitution flows.", "direction": "bidirectional", "historical_evidence": "2019: world PKO US$668/t, coconut oil US$738/t; 2022: PKO US$1,598/t, coconut oil US$1,621/t — tight spread throughout cycle"}',
 'extracted', 0.90),

-- TRADE AND POLICY
((SELECT id FROM core.kg_node WHERE node_key = 'indonesia.export_policy'),
 (SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_exports'),
 'CAUSES', 0.90,
 '{"mechanism": "Indonesia export restrictions cause importing countries to shift purchases to Malaysian palm oil, boosting Malaysian volumes and market share. Conversely, Indonesian liberalization increases competition and reduces Malaysian market share.", "direction": "positive_when_indonesia_restricts", "historical_evidence": "2021: Indonesia high export tax → India shifted to Malaysia, Indian imports +31.3%. April–May 2022: Indonesia export ban → global CPO price spike and Malaysian share gains. 2023: Indonesia ban lifted → Malaysian market share declined and CPO price fell –25.1%."}',
 'extracted', 0.90),

((SELECT id FROM core.kg_node WHERE node_key = 'miceca'),
 (SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_exports'),
 'CAUSES', 0.90,
 '{"mechanism": "MICECA gives Malaysian palm oil (especially RBD palm olein) a preferential import duty advantage into India versus Indonesian palm oil. When the differential is activated, triggers large-scale Indian purchases of Malaysian product.", "direction": "positive", "primary_market": "india", "primary_product": "RBD_palm_olein", "evidence": "2019: 5% MICECA duty advantage → Indian imports of Malaysian palm oil surged 75.4% to 4.41M tonnes"}',
 'extracted', 0.90),

((SELECT id FROM core.kg_node WHERE node_key = 'india.palm_oil_import_policy'),
 (SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_exports'),
 'CAUSES', 0.95,
 '{"mechanism": "India import policy changes are the single largest cause of year-to-year swings in Malaysian palm oil export volumes. India accounts for 12–24% of Malaysian exports; policy-driven swings of 30–75% create systemically important volume changes.", "direction": "bidirectional_high_sensitivity", "critical_events": {"2019": "+75.4% on MICECA duty advantage", "2020": "–37.7% on Indian restriction of RBD olein from Malaysia — RBD olein exports collapsed 99.7% from 2.34M to 7,323 tonnes"}}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'ukraine_russia_war'),
 (SELECT id FROM core.kg_node WHERE node_key = 'sunflower_oil'),
 'CAUSES', 0.95,
 '{"mechanism": "Ukraine is the worlds largest sunflower oil exporter. The 2022 war disrupted export logistics and supply chains, creating an acute global sunflower oil supply gap.", "direction": "inverse_supply_shock", "magnitude": "Ukraine + Russia together supply ~60% of global sunflower oil"}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'sunflower_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'cpo'),
 'COMPETES_WITH', 0.80,
 '{"mechanism": "Sunflower oil and palm oil compete for global vegetable oil demand, particularly in EU, Turkey, India, and MENA. Supply disruptions in sunflower oil (war, drought) trigger substitution purchases of palm oil. Cheap abundant sunflower oil displaces palm.", "direction": "bidirectional", "evidence": "2022: Ukraine war → sunflower supply shock → substitution demand surge contributed to CPO record RM5,087.50/tonne"}',
 'extracted', 0.80),

((SELECT id FROM core.kg_node WHERE node_key = 'indonesia.biodiesel_mandate'),
 (SELECT id FROM core.kg_node WHERE node_key = 'cpo'),
 'CAUSES', 0.85,
 '{"mechanism": "Indonesian B35/B40 mandate absorbs 8–12M tonnes of CPO domestically, removing that supply from export markets. This structural domestic demand floor provides ongoing support to global CPO prices. Indonesia domestic mandate growth is the key long-run bullish structural driver for CPO.", "direction": "positive_price_support", "magnitude": "Each 5% mandate increase ≈ 1–1.5M tonnes additional Indonesian CPO demand"}',
 'extracted', 0.85),

((SELECT id FROM core.kg_node WHERE node_key = 'malaysia.biodiesel_mandate'),
 (SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_stocks'),
 'CAUSES', 0.75,
 '{"mechanism": "Malaysian domestic biodiesel blending mandate absorbs palm oil that would otherwise be exported or add to stocks. Mandate progression from B5 to B10 reduces export supply availability and draws down stocks at the margin.", "direction": "inverse_on_stocks", "magnitude_mmt": "Estimated 0.3–0.6M tonnes absorbed domestically under B7–B10"}',
 'extracted', 0.75),

-- COMPETITION
((SELECT id FROM core.kg_node WHERE node_key = 'indonesia.palm_oil_producers'),
 (SELECT id FROM core.kg_node WHERE node_key = 'cpo'),
 'COMPETES_WITH', 0.95,
 '{"mechanism": "Indonesian CPO competes directly with Malaysian CPO in all major export markets. Indonesia produces ~2.5–3x Malaysian volumes. Indonesian export tax adjustments, port logistics, and price competitiveness determine relative market share in India, China, EU, Pakistan, and other markets.", "market_dynamics": {"india": "Duty differential (MICECA vs ASEAN-India FTA) determines switching. India is the primary battleground.", "china": "Price and logistics-determined competition", "eu": "Both compete but sustainability certification (RSPO, ISCC) adds dimension"}}',
 'extracted', 0.95),

((SELECT id FROM core.kg_node WHERE node_key = 'rapeseed_oil_eu'),
 (SELECT id FROM core.kg_node WHERE node_key = 'cpo'),
 'COMPETES_WITH', 0.75,
 '{"mechanism": "EU rapeseed oil is the primary domestic vegetable oil in EU and competes with palm oil imports. Higher EU rapeseed harvests and soybean crushing reduce EU palm oil import demand. EU rapeseed also competes as biodiesel feedstock under EU RED.", "direction": "inverse_availability", "evidence": "2016: higher EU rapeseed/soy supply → EU palm oil imports –15.3%; multiple years show EU imports as partially inverse of domestic oilseed availability"}',
 'extracted', 0.75),

((SELECT id FROM core.kg_node WHERE node_key = 'mpob.planted_area'),
 (SELECT id FROM core.kg_node WHERE node_key = 'mpob.cpo_production'),
 'CAUSES', 0.70,
 '{"mechanism": "Planted area sets the physical ceiling for production. With Malaysian area declining since 2019 peak (5.90M to 5.61M ha by 2024), production growth must come from yield and OER improvement — not area expansion. Replanting programs temporarily remove productive area before replacements mature.", "direction": "positive_long_run", "lag_years": "3–4 years for new palms to reach peak production", "note": "Short-run production more sensitive to yield/OER than area changes"}',
 'extracted', 0.70);


-- ============================================================================
-- CONTEXTS
-- ============================================================================

INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES

-- CPO Price Driver Hierarchy
((SELECT id FROM core.kg_node WHERE node_key = 'mpob.cpo_price'),
 'expert_rule', 'cpo_price_driver_hierarchy',
 '{"rule": "CPO price is driven by a hierarchy of factors. In descending order of typical influence: (1) Malaysian/Indonesian palm oil stock levels — single strongest signal; (2) Soybean oil price — CPO trades at discount to SBO, discount narrows on tightness; (3) Indonesia export policy — restrictions are immediate bullish catalysts; (4) Brent crude oil — through biofuel demand channel; (5) MYR/USD exchange rate — weaker MYR boosts competitiveness; (6) Weather events (El Nino/La Nina) — with 6–12 month lag.",
   "all_bullish_year_reference": "2021: labour shortage (low production) + low stocks + SBO +68% record + crude oil recovery — all aligned → CPO record RM4,407",
   "all_bearish_year_reference": "2018–19: high stocks (3.22M tonnes) + weak SBO + no Indonesia restrictions + adequate supply → CPO trough RM2,079 (2019)",
   "extreme_events": "2022: Ukraine war sunflower disruption + Indonesia export ban — extraordinary bullish combination → RM5,087 record; 2023: Indonesia ban lifted + stocks normalized → –25.1% correction"}',
 'always', 'extracted'),

-- Closing Stocks Thresholds
((SELECT id FROM core.kg_node WHERE node_key = 'mpob.palm_oil_stocks'),
 'risk_threshold', 'stocks_to_price_signal_levels',
 '{"very_tight": {"max_mmt": 1.5, "price_signal": "Very bullish. 2020 year-end at 1.27M tonnes preceded 2021 CPO record RM4,407/tonne."},
   "tight": {"min_mmt": 1.5, "max_mmt": 2.0, "price_signal": "Bullish — supportive price environment"},
   "neutral": {"min_mmt": 2.0, "max_mmt": 2.5, "price_signal": "Neutral — price determined more by external factors (SBO, crude, Indonesia policy)"},
   "comfortable": {"min_mmt": 2.5, "max_mmt": 3.0, "price_signal": "Bearish lean — prices tend to ease"},
   "very_bearish": {"min_mmt": 3.0, "price_signal": "Very bearish. 2018 at 3.22M tonnes — CPO weakest at RM1,794.50 in December 2018."},
   "nine_year_history_mmt": {"2016": 1.67, "2017": 2.73, "2018": 3.22, "2019": 2.01, "2020": 1.27, "2021": 1.61, "2022": 2.20, "2023": 2.29, "2024": 1.71}}',
 'always', 'extracted'),

-- FFB Yield Shock Patterns
((SELECT id FROM core.kg_node WHERE node_key = 'mpob.ffb_yield'),
 'expert_rule', 'yield_shock_pattern_playbook',
 '{"el_nino_pattern": "El Nino impact year: FFB yield declines 10–15%+. 2016: –13.9% to 15.91 t/ha. Recovery year is sharp: 2017: +12.4% to 17.89 t/ha.",
   "biological_rest_year": "After an exceptionally high yield year, a biological rest effect occurs in the following year even without weather disruption. 2018 followed record 2017 with –4.1% decline, compounded by unfavorable weather.",
   "covid_labour_impact": "Labour shortage reduces harvest frequency — increases overripe FFB, lowering both effective yield AND OER simultaneously. Different from weather-driven yield decline which mainly affects yield not OER.",
   "national_trough_threshold": "FFB yield below 16 t/ha implies national CPO production <18.5M tonnes. Below 15.5 t/ha implies <18M tonnes.",
   "regional_vulnerability": "Sabah yield more volatile (coast to interior estate mix); Sarawak newer estates with lower absolute yield but higher growth trajectory. Peninsular most stable but highest-yielding.",
   "nine_year_range_tha": {"trough": 15.47, "trough_year": 2021, "peak": 17.89, "peak_year": 2017}}',
 'always', 'extracted'),

-- India Demand Framework
((SELECT id FROM core.kg_node WHERE node_key = 'india'),
 'expert_rule', 'india_palm_oil_demand_framework',
 '{"rule": "India is the most volatile major market for Malaysian palm oil. Year-to-year swings of 30–75% are common based purely on policy — not underlying consumption growth. India accounts for 12–24% of Malaysian exports depending on duty environment.",
   "key_variables_to_monitor": ["MICECA vs ASEAN-India FTA duty differential (Malaysian RBD olein vs Indonesian)", "India restrictions on refined palm oil imports (Jan 2020 precedent)", "India domestic oilseed production (mustard/groundnut crop)", "India SBO and sunflower oil import volumes as substitutes"],
   "historical_swings": {"2016_to_2017": "–28.2% (Indonesia more competitive in India)", "2017_to_2019": "+75.4% (MICECA duty advantage activated Jan 2019)", "2019_to_2020": "–37.7% (India restricted Malaysian processed palm oil)", "2020_to_2021": "+31.3% (Indonesia export tax made Malaysia more competitive)"},
   "practical_rule": "India market share above 20% indicates MICECA advantage active. Below 15% often indicates Indian policy headwind or strong Indonesian competition."}',
 'always', 'extracted'),

-- Indonesia Export Policy Framework
((SELECT id FROM core.kg_node WHERE node_key = 'indonesia.export_policy'),
 'expert_rule', 'indonesia_policy_market_impact_framework',
 '{"rule": "Indonesia export policy is the most immediate external shock variable for global CPO prices and Malaysian market share. Three operating modes:",
   "mode_1_high_levy": "Indonesia high export taxes/levies redirect Indonesian supply to domestic B-mandate, reducing global export availability. Bullish for Malaysian prices and market share. Example: 2021 high Indonesian levy → India switched heavily to Malaysia.",
   "mode_2_export_ban": "Acute supply shock — immediate CPO price spike globally. April–May 2022 crude palm oil export ban contributed to all-time monthly high RM6,873/tonne.",
   "mode_3_liberalization": "Indonesia reduces export barriers, flooding markets. Bearish for Malaysian prices and market share. 2023 Indonesian ban removal: CPO fell –25.1%.",
   "monitoring_approach": "Track Indonesia CPO reference price monthly, export levy schedule, and B-mandate absorption volumes (available via APROBI reports). Watch DMO (Domestic Market Obligation) compliance announcements."}',
 'always', 'extracted'),

-- OER Interpretation
((SELECT id FROM core.kg_node WHERE node_key = 'mpob.oer'),
 'expert_rule', 'oer_quality_and_warning_signals',
 '{"rule": "National OER is a quality signal for FFB being processed. Higher OER indicates better proportion of ripe, well-harvested FFB. Lower OER indicates harvesting problems or weather effects on fruit quality.",
   "regional_benchmarks": {"sabah": "Consistently highest: 20.25–21.11%. Best harvesting practices, mature high-yielding estates.", "peninsular": "19.46–19.93%. Large estate/mill base creates averaging effect.", "sarawak": "19.47–20.03%. More variable. Newer estate base.", "national": "19.67–20.21% over 2016–2024"},
   "warning_signal": "National OER below 19.70% — 2022 at 19.70% was the lowest since 2001 (19.22%). Signals industry-wide labour or weather quality problems.",
   "production_sensitivity": "0.5% OER change on ~19M tonnes FFB processed ≈ ±95,000 tonnes CPO difference — meaningful but secondary to FFB yield in driving production."}',
 'always', 'extracted'),

-- El Nino Playbook
((SELECT id FROM core.kg_node WHERE node_key = 'el_nino'),
 'expert_rule', 'el_nino_palm_oil_playbook',
 '{"sequence": "(1) El Nino onset (typically Q3–Q4 of a year): begin pricing in production risk for following 12 months; (2) Peak impact year (12 months after onset): FFB yield falls 10–15%, CPO production declines; (3) Recovery year: strong FFB yield and production rebound of +10–15%.",
   "price_implications": "El Nino year: stocks draw down, prices supported. Recovery year: production surge can weigh on prices if demand does not keep pace (especially if stocks were rebuilt).",
   "compound_effects": "El Nino impact is amplified when it coincides with low opening stocks or strong global veg oil demand. 2016: moderate demand + El Nino supply shock → CPO +23.2%.",
   "differentiation": "El Nino affects FFB yield (fruit per hectare). COVID labour shortage affects harvest efficiency (fruit collected per available FFB), reducing yield AND OER. Net CPO impact similar but OER response differs — a diagnostic signal for the root cause."}',
 'always', 'extracted'),

-- Lauric Oil (CPKO/PK) Framework
((SELECT id FROM core.kg_node WHERE node_key = 'cpko'),
 'expert_rule', 'lauric_oil_pricing_framework',
 '{"rule": "CPKO and PK prices are governed by the global lauric oil complex (PKO + coconut oil), not just CPO. The lauric complex can diverge significantly from CPO.",
   "cpko_cpo_ratio_history": {"2016": "5492/2653 = 2.07x (premium peak)", "2019": "2626/2079 = 1.26x (lauric weakness)", "2021": "5674/4407 = 1.29x (both tight)", "2022": "6327/5087 = 1.24x (convergence — all oils tight)", "2024": "5475/4179 = 1.31x"},
   "divergence_signal": "When CPKO-to-CPO ratio is above 1.8x, lauric oil supply is tight relative to CPO. Below 1.3x, lauric is relatively weak — watch for coconut oil supply recovery or demand slowdown.",
   "key_driver_for_divergence": "Philippine/Indonesian coconut oil production cycle is the primary CPKO-specific driver. Strong coconut harvest → cheap coconut oil → CPKO competition → CPKO/PK price weakness independent of CPO direction."}',
 'always', 'extracted'),

-- Export Revenue vs Volume Divergence
((SELECT id FROM core.kg_node WHERE node_key = 'mpob.export_revenue'),
 'expert_rule', 'volume_revenue_divergence_rule',
 '{"rule": "Malaysian palm oil export value and volume frequently move in opposite directions. Always track both metrics independently.",
   "documented_divergences": {
     "2019": "Volume +12.0% YoY but revenue –1.6% (low prices offset volume gains)",
     "2021": "Volume –10.5% YoY but revenue +41.6% (record prices more than offset volume shortfall)",
     "2022": "Volume +0.9% but revenue +27.7% (price effect dominated completely)",
     "2023": "Volume –3.7% but revenue –27.1% (price correction amplified volume decline)",
     "2024": "Volume +8.9% and revenue +15.2% (both positive — unusual but occurred on production recovery + higher prices)"
   },
   "analytical_rule": "In high-price environments, volume declines may signal demand destruction or market share loss worth monitoring. In low-price environments, volume gains alone do not compensate — watch for signs that price weakness is structural vs cyclical."}',
 'always', 'extracted'),

-- Planted Area Declining Trend
((SELECT id FROM core.kg_node WHERE node_key = 'mpob.planted_area'),
 'expert_rule', 'planted_area_trend_and_implication',
 '{"rule": "Malaysian oil palm planted area peaked in 2019 at 5.90M ha and has been declining gradually (5.61M ha in 2024 = –4.9% over 5 years). Key drivers: (1) Replanting programs — old palms removed before replacements mature, creating temporary production gap; (2) Land conversion to coconut, residential, commercial use; (3) Slower approvals in Sabah and Peninsular due to land scarcity and sustainability pressures.",
   "production_ceiling_implication": "With area declining, Malaysian CPO production growth must come entirely from FFB yield and OER improvement. Structural production ceiling approximately 20–21M tonnes under ideal conditions. Declining area biases medium-term production to flat or lower absent yield improvement.",
   "sarawak_exception": "Sarawak planted area has been stable to marginally growing despite national decline — remaining frontier for expansion, constrained by sustainability certification requirements (RSPO, MSPO)."}',
 'always', 'extracted'),

-- Ukraine War Substitution Template
((SELECT id FROM core.kg_node WHERE node_key = 'ukraine_russia_war'),
 'expert_rule', 'sunflower_oil_supply_shock_playbook',
 '{"rule": "The 2022 Ukraine war established a template for how Black Sea sunflower oil supply shocks affect palm oil markets.",
   "sequence": "(1) Supply disruption creates immediate sunflower oil price spike and supply gap; (2) EU, Turkey, India, MENA buyers substitute palm oil; (3) Palm oil price rallies sharply; (4) Alternative supply sources develop over 6–18 months; (5) Sunflower supply normalizes, palm oil demand retreats.",
   "palm_oil_price_impact": "2022 CPO record RM5,087.50 partly attributable to Ukraine war substitution demand. 2023 correction –25.1% partly as Ukraine exports partially resumed.",
   "early_warning_signal": "Monitor: (1) Ukraine crop and logistics conditions during planting (Mar–May) and harvest (Sep–Nov); (2) Russia export quota announcements; (3) Argentina sunflower production as alternative source.",
   "magnitude_guide": "Ukraine alone supplies ~45–50% of global sunflower oil exports. Disruption of even 30% of Ukrainian supply creates meaningful palm oil demand pull."}',
 'always', 'extracted');


-- ============================================================================
-- SOURCE REGISTRY
-- ============================================================================

INSERT INTO core.kg_source (source_key, source_type, title, location_uri, document_date, document_type, commodities, topics, status, first_processed, last_processed, nodes_extracted, edges_extracted, contexts_extracted) VALUES
('gdoc_1XYyVbe5GAQOcE9jvKJSRTYlgwN2JFaGDV3LKLq7jj_U', 'gdrive_doc', 'MPOB_Overview_of_Industry_2016', 'https://docs.google.com/document/d/1XYyVbe5GAQOcE9jvKJSRTYlgwN2JFaGDV3LKLq7jj_U/edit', '2017-02-15', 'annual_report', '{cpo,ffb,palm_kernel,cpko,pkc,palm_oleochemicals,palm_biodiesel_my}', '{el_nino_impact,production_decline,stocks_drawdown,higher_prices,india_exports,pakistan_exports,turkey_exports,planted_area_growth}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1tyg_7r9L49cMUCUxo3HX0bNF29xFIKvZrRcW-_iqynY', 'gdrive_doc', 'MPOB_Overview_of_Industry_2017', 'https://docs.google.com/document/d/1tyg_7r9L49cMUCUxo3HX0bNF29xFIKvZrRcW-_iqynY/edit', '2018-01-01', 'annual_report', '{cpo,ffb,palm_kernel,cpko,pkc,palm_oleochemicals}', '{el_nino_recovery,production_surge,stocks_build,sarawak_surpasses_sabah,india_exports,biological_rest_year_warning}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1zCpbxsID6ZYFFeWJrP20encAl9rs3BG2ZxeWF3xkSX0', 'gdrive_doc', 'MPOB_Overview_of_Industry_2018', 'https://docs.google.com/document/d/1zCpbxsID6ZYFFeWJrP20encAl9rs3BG2ZxeWF3xkSX0/edit', '2019-01-01', 'annual_report', '{cpo,ffb,palm_kernel,cpko,pkc,palm_oleochemicals}', '{high_stocks_3.22M,price_decline,biological_rest_year,india_uptake_positive,asf_context,cpo_export_duty_suspension}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1DSy-8fbRe_hDrTBYYGtTBu6bRqDf5WD-_lM12Yid5XQ', 'gdrive_doc', 'MPOB_Overview_of_Industry_2019', 'https://docs.google.com/document/d/1DSy-8fbRe_hDrTBYYGtTBu6bRqDf5WD-_lM12Yid5XQ/edit', '2020-01-01', 'annual_report', '{cpo,ffb,rbd_palm_olein,palm_kernel,cpko,pkc,palm_oleochemicals}', '{miceca_india_surge_75pct,export_volume_record_18.47M,stocks_drawdown_2.01M,cpo_price_trough_2079,asf_china_soy_effect}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_140hnoTbQJ-QvSaUdn2azf1sZYgRlUk_2aPl5x6ij0ts', 'gdrive_doc', 'MPOB_Overview_of_Industry_2020', 'https://docs.google.com/document/d/140hnoTbQJ-QvSaUdn2azf1sZYgRlUk_2aPl5x6ij0ts/edit', '2021-01-01', 'annual_report', '{cpo,ffb,rbd_palm_olein,palm_kernel,cpko,pkc,palm_oleochemicals}', '{covid19_demand_disruption,india_rbd_olein_restriction_99pct_collapse,stocks_drawdown_1.27M,cpo_recovery_2685,china_second_largest}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1DVy3MVdb4nEG-TWdrxj0ZVpAhsmQtHPiHqnHjbndHOc', 'gdrive_doc', 'MPOB_Overview_of_Industry_2021', 'https://docs.google.com/document/d/1DVy3MVdb4nEG-TWdrxj0ZVpAhsmQtHPiHqnHjbndHOc/edit', '2022-01-01', 'annual_report', '{cpo,ffb,palm_kernel,cpko,pkc,palm_oleochemicals}', '{covid_labour_shortage,production_decline_5.4pct,record_cpo_4407,indonesia_export_tax,india_surge_31pct,export_revenue_record_108bn,kenya_emerging}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1hSHi_Rjhxgw2ib6MrAkr8YckLrkYiiUMlrsNYGcW2_8', 'gdrive_doc', 'MPOB_Overview_of_Industry_2022', 'https://docs.google.com/document/d/1hSHi_Rjhxgw2ib6MrAkr8YckLrkYiiUMlrsNYGcW2_8/edit', '2023-01-01', 'annual_report', '{cpo,ffb,palm_kernel,cpko,pkc,palm_oleochemicals,palm_biodiesel_my}', '{ukraine_war_sunflower_disruption,indonesia_export_ban_apr_may,record_cpo_5087,lowest_oer_since_2001,stocks_build_2.20M,export_revenue_record_137bn}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1Jd8rK2FPd0W2XAeFMGdR8NGZ_M9rfXrqxAdh-STGR3Y', 'gdrive_doc', 'MPOB_Overview_of_Industry_2023', 'https://docs.google.com/document/d/1Jd8rK2FPd0W2XAeFMGdR8NGZ_M9rfXrqxAdh-STGR3Y/edit', '2024-01-01', 'annual_report', '{cpo,ffb,palm_kernel,cpko,pkc,palm_oleochemicals,palm_biodiesel_my}', '{indonesia_ban_lifted,price_correction_25pct,kenya_growth_4th_market,labour_supply_recovery,stocks_build_2.29M,oer_improvement}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1pDeHRVDQitW0mjywZ2GJvqnvGwY8wYW7wkEjEkF-J9U', 'gdrive_doc', 'MPOB_Overview_of_Industry_2024', 'https://docs.google.com/document/d/1pDeHRVDQitW0mjywZ2GJvqnvGwY8wYW7wkEjEkF-J9U/edit', '2025-01-01', 'annual_report', '{cpo,ffb,palm_kernel,cpko,pkc,palm_oleochemicals,palm_biodiesel_my}', '{production_recovery_4.2pct,labour_recovery,export_surge_16.9M,indonesia_b40_anticipation,stocks_drawdown_1.71M,cpo_price_recovery_4179}', 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();
