# CARB LCFS ↔ DB Cross-Reference Report

- **CARB liquid-biofuel pathway facilities:** 94
- **DB biofuel-ish facility rows:** 504
  - reference.biodiesel_facilities: 192
  - reference.biofuel_facilities:   241
  - reference.renewable_diesel_facilities: 66
  - reference.facility_master (biofuel codes): 5

- **A. Matched (CARB ⇔ DB):** 70 CARB facilities
- **B. Missing (in CARB, not in DB):** 24 CARB facilities
- **C. DB-only (in DB, not in CARB):** 388 DB facility rows

## B. Missing — CARB facilities we don't have in DB

These are CARB-certified biofuel/SAF producers that have NO match in our DB.
Each is potentially a facility we should add. Sorted by pathway_count (more
pathways = more commercial activity).

| Fuel Producer | Facility Name | Location | Fuel Type | Feedstocks | Pathways | Weak match? |
|---|---|---|---|---|---|---|
| Jaxon Energy, LLC | Jaxon Energy, LLC | Mississippi | Renewable Diesel (RND) | Distillers' Corn Oil  (003); Distillers' Corn Oil  | 8 | `biofuel_facilities` 81 (score 1, overlap: jaxon) |
| Dansuk Industrial Co., Ltd | Dansuk Industrial Co., Ltd | South Korea | Biodiesel; Biodiesel (BIO) | Tallow (animal and poultry fat) (002); Used Cookin | 7 |  |
| Wyoming Renewable Diesel Company LLC | Wyoming Renewable Diesel Company LL | Wyoming | Renewable Diesel (RND) | Canola Oil (006); Soybean Oil (005) | 7 | `biofuel_facilities` 315 (score 1, overlap: wyoming) |
| Dansuk Industrial Co., Ltd | Pyeongtaek 2 | South Korea | Biodiesel (BIO) | Used Cooking Oil/Waste Oil (UCO) (001) | 5 | `biodiesel_facilities` us.unk.bd.bp_cherry_point_wa_stage_2 (score 1, overlap: 2) |
| NEWPORT BIODIESEL INC | NEWPORT BIODIESEL LLC | Rhode Island | Biodiesel (BIO) | Used Cooking Oil/Waste Oil (UCO) (001) | 4 | `biodiesel_facilities` us.unk.bd.newport_biodiesel_inc (score 1, overlap: newport) |
| IOWA RENEWABLE ENERGY LLC | IOWA RENEWABLE ENERGY LLC | Iowa | Biodiesel (BIO) | Canola Oil (006); Soybean Oil (005); Tallow (anima | 4 | `biodiesel_facilities` us.unk.bd.agron_bioenergy_bought_in_18_by_western_iowa_energy_llc (score 1, overlap: iowa) |
| TIDEWATER RENEWABLES LTD. | PRINCE GEORGE RENEWABLE DIESEL | Canada | Renewable Diesel (RND) | Canola Oil (006); Distillers' Corn Oil (003); Tall | 4 | `biodiesel_facilities` us.unk.bd.delek_renewables_cleburne (score 1, overlap: renewables) |
| Biocom Energia | Biocom Energia | Spain | Biodiesel | Used Cooking Oil (Europe); Used Cooking Oil (Globa | 3 |  |
| SJV BIODIESEL LLC | SJV BIODIESEL | California | Biodiesel; Biodiesel (BIO) | Distillers' Corn Oil  (003); Used Cooking Oil/Wast | 3 | `biodiesel_facilities` us.unk.bd.sjv_biodiesel_llc (score 1, overlap: sjv) |
| Wyoming Renewable Diesel Company LLC |  | Wyoming | Renewable Diesel (RND) | Tallow (animal and poultry fat) (002) | 3 | `biofuel_facilities` 315 (score 1, overlap: wyoming) |
| Cargill Biodiesel | Cargill Incorporated | Iowa | Biodiesel (BIO) | Soybean Oil (005) | 3 | `biodiesel_facilities` us.unk.bd.cargill_inc_iowa_falls (score 1, overlap: cargill) |
| Universal Biofuels Private, Ltd | Universal Biofuels Private, Ltd | Biodiesel | Biodiesel | Tallow; UCO | 2 |  |
| Ensyn Technologies Inc. | Ensyn Ontario Facility | Canada | Biodiesel; Renewable Diesel | Pyrolysis Oil from Forest Residue | 2 |  |
| BUSTER BIOFUELS LLC | BUSTER BIOFUELS LLC | California | Biodiesel | Used Cooking Oil | 2 | `biodiesel_facilities` us.unk.bd.buster_biofuels (score 1, overlap: buster) |
| JC Chemical Co., Ltd. | JC Chemical Co., Ltd. | Korea, South | Biodiesel; Biodiesel (BIO) | Used Cooking Oil; Used Cooking Oil/Waste Oil (UCO) | 2 | `biodiesel_facilities` us.unk.bd.futurefuel_chemical_company (score 1, overlap: chemical) |
| Consolidated Biofuels Ltd. | Consolidated Biofuels Ltd. | Canada | Biodiesel | Used Cooking Oil | 1 |  |
| Clinton Biodiesel, LLC | Clinton Biodiesel LLC | Iowa | Biodiesel | Soybean | 1 | `biofuel_facilities` 49 (score 1, overlap: clinton) |
| GeoGreen Biofuels | GeoGreen Biofuels | California | Biodiesel | Used Cooking Oil | 1 | `biodiesel_facilities` us.unk.bd.geogreen_biofuels_inc (score 1, overlap: geogreen) |
| General Biodiesel Seattle, LLC | General Biodiesel Seattle, LLC | Washington | Biodiesel | Used Cooking Oil | 1 | `biodiesel_facilities` us.unk.bd.general_biodiesel_northwest (score 1, overlap: general) |
| ASB Biodiesel Hong Kong | ASB Biodiesel Hong Kong | Hong Kong | Biodiesel | Used Cooking Oil (UCO) | 1 |  |
| Eco Solutions Co., Ltd | Eco Solutions Co., Ltd | South Korea | Biodiesel | Used Cooking Oil (UCO) | 1 | `biodiesel_facilities` us.unk.bd.alaska_green_waste_solutions_inc (score 1, overlap: solutions) |
| SIMPLE FUELS BIODIESEL INC | SIMPLE FUELS BIODIESEL | California | Biodiesel (BIO) | Used Cooking Oil/Waste Oil (UCO) (001) | 1 | `biodiesel_facilities` us.unk.bd.simple_fuels_biodiesel_inc (score 1, overlap: simple) |
| SJV BIODIESEL LLC |  | California | Biodiesel (BIO) | Tallow (animal and poultry fat) (002) | 1 | `biodiesel_facilities` us.unk.bd.sjv_biodiesel_llc (score 1, overlap: sjv) |
| BE8 S.A. | BE8 S.A. | Brazil | Biodiesel (BIO) | Tallow (animal and poultry fat) (002) | 1 | `biodiesel_facilities` us.unk.bd.world_energy_rome_at_u_s_biofuels_inc (score 1, overlap: s) |

## C. DB-only — Our facilities NOT in CARB pathways

Reasons something here is NOT in CARB:
- Plant doesn't ship into CA market → no LCFS pathway needed
- Plant is **closed/idled** but our DB still marks active
- Plant exists under a different operator name CARB recognizes
- Plant is new and pathway is pending

Sorted by state, then operator. **Suspicious cases highlighted with ⚠️**.

### ? (291)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| us.unk.bd.adm_velva | ADM - Velva | ADM - Velva |  | Operating | 75.0 | biodiesel_facilities |
| us.unk.bd.agp_algona | AGP Algona | AGP Algona |  | Operating | 60.0 | biodiesel_facilities |
| us.unk.bd.agp_sergeant_bluff | AGP Sergeant Bluff | AGP Sergeant Bluff |  | Operating | 60.0 | biodiesel_facilities |
| us.unk.bd.agp_st_joseph | AGP St. Joseph | AGP St. Joseph |  | Operating | 30.0 | biodiesel_facilities |
| 293 | Abundia Biomass-to-liquids - Teessi | Abundia Biomass-to-liquids | Teesside | announced | 0.4 | biofuel_facilities |
| 257 | Acelen Renewables - Mataripe, Bahia | Acelen Renewables | Mataripe, Bahia | under_construction | 82.0 | biofuel_facilities |
| us.unk.bd.adkins_energy_llc | Adkins Energy LLC | Adkins Energy LLC |  | Operating | 2.0 | biodiesel_facilities |
| us.unk.bd.agribiofuels_llc | Agribiofuels LLC | Agribiofuels LLC |  | Operating | 12.0 | biodiesel_facilities |
| us.unk.bd.alaska_green_waste_solutions_inc | Alaska Green Waste Solutions Inc. | Alaska Green Waste Solutions I |  | Operating | 0.3 | biodiesel_facilities |
| us.unk.bd.allied_renewable_energy_llc | Allied Renewable Energy LLC | Allied Renewable Energy LLC |  | Operating | 6.0 | biodiesel_facilities |
| us.unk.bd.alt_air_expansion | Alt Air Expansion | Alt Air Expansion |  | Operating | 220.0 | biodiesel_facilities |
| us.unk.bd.altair_fuels | AltAir Fuels | AltAir Fuels |  | Operating |  | biodiesel_facilities |
| us.unk.bd.american_biodiesel_energy_inc | American Biodiesel Energy Inc. | American Biodiesel Energy Inc. |  | Operating | 2.0 | biodiesel_facilities |
| us.unk.rd.andeavor_marathon_dickinson_nd | Andeavor (Marathon) Dickinson ND | Andeavor (Marathon) Dickinson  |  | Operating |  | renewable_diesel_facilities |
| us.unk.bd.atlantic_biodiesel_corp | Atlantic Biodiesel Corp. | Atlantic Biodiesel Corp. |  | Operating |  | biodiesel_facilities |
| 118 | BD Percent | BD Percent |  | operating |  | biofuel_facilities |
| us.unk.rd.bp_cherry_point | BP Cherry Point | BP Cherry Point |  | Operating |  | renewable_diesel_facilities |
| us.unk.bd.bp_cherry_point_wa_stage_1_maybe_rd_at_some_point | BP Cherry Point WA - Stage 1 (maybe | BP Cherry Point WA - Stage 1 ( |  | Operating | 42.0 | biodiesel_facilities |
| us.unk.bd.bp_cherry_point_wa_stage_2 | BP Cherry Point WA - Stage 2 | BP Cherry Point WA - Stage 2 |  | Operating | 70.0 | biodiesel_facilities |
| us.unk.bd.baker_commodities_billerica | Baker Commodities Billerica | Baker Commodities Billerica |  | Operating |  | biodiesel_facilities |
| us.unk.bd.baker_commodities_los_angeles | Baker Commodities Los Angeles | Baker Commodities Los Angeles |  | Operating |  | biodiesel_facilities |
| us.unk.bd.bakersfield | Bakersfield | Bakersfield |  | Operating | 100.0 | biodiesel_facilities |
| us.unk.bd.bay_biodiesel_llc | Bay Biodiesel, LLC | Bay Biodiesel, LLC |  | Operating |  | biodiesel_facilities |
| us.unk.bd.beaver_biodiesel_llc | Beaver Biodiesel LLC | Beaver Biodiesel LLC |  | Operating | 0.9 | biodiesel_facilities |
| us.unk.bd.bio_alternative_llc_covington | Bio-Alternative LLC - Covington | Bio-Alternative LLC - Covingto |  | Operating | 15.5 | biodiesel_facilities |
| us.unk.bd.biovantage_fuels_llc | BioVantage Fuels LLC | BioVantage Fuels LLC |  | Operating | 3.2 | biodiesel_facilities |
| 114 | Biodiesel | Biodiesel |  | operating |  | biofuel_facilities |
| 122 | Biodiesel Annual Capacity (mmgal/yr | Biodiesel Annual Capacity (mmg |  | operating |  | biofuel_facilities |
| us.unk.bd.biodiesel_of_las_vegas | Biodiesel of Las Vegas | Biodiesel of Las Vegas |  | Operating | 8.0 | biodiesel_facilities |
| us.unk.bd.biodiesel_of_texas_inc | Biodiesel of Texas, Inc. | Biodiesel of Texas, Inc. |  | Operating |  | biodiesel_facilities |
| 284 | Biorefinery Ostrand SCA/ST1 - Ostra | Biorefinery Ostrand SCA/ST1 | Ostrand, Timra | announced | 38.8 | biofuel_facilities |
| us.unk.bd.blue_ridge_biofuels | Blue Ridge Biofuels | Blue Ridge Biofuels |  | Operating | 1.0 | biodiesel_facilities |
| 258 | Brasil BioFuels (BBF) - Manaus | Brasil BioFuels (BBF) | Manaus | under_construction | 66.0 | biofuel_facilities |
| us.unk.bd.bridgeport_biodiesel_llc | Bridgeport Biodiesel LLC | Bridgeport Biodiesel LLC |  | Operating | 14.0 | biodiesel_facilities |
| us.unk.bd.business_name_location | Business Name/Location | Business Name/Location |  | Operating |  | biodiesel_facilities |
| us.unk.bd.buster_biofuels | Buster Biofuels | Buster Biofuels |  | Operating | 5.0 | biodiesel_facilities |
| us.unk.bd.canada_biodiesel | CANADA - BIODIESEL | CANADA - BIODIESEL |  | Operating |  | biodiesel_facilities |
| us.unk.bd.canada_co_processing | CANADA - CO-PROCESSING | CANADA - CO-PROCESSING |  | Operating |  | biodiesel_facilities |
| us.unk.bd.canada_renewable_diesel | CANADA - RENEWABLE DIESEL | CANADA - RENEWABLE DIESEL |  | Operating |  | biodiesel_facilities |
| us.unk.bd.canada_questionable | CANADA QUESTIONABLE | CANADA QUESTIONABLE |  | Operating |  | biodiesel_facilities |
| 77 | CVR Energy - Coffeyville KS | CVR Energy |  | operating | 150.0 | biofuel_facilities |
| 76 | CVR Energy - Wynnewood OK | CVR Energy |  | operating | 92.0 | biofuel_facilities |
| us.unk.rd.cvr_wynnewood | CVR Wynnewood | CVR Wynnewood |  | Operating |  | renewable_diesel_facilities |
| 90 | Calumet Montana Refining - Great Fa | Calumet Montana Refining |  | operating |  | biofuel_facilities |
| 109 | Capacity Utilization | Capacity Utilization |  | operating |  | biofuel_facilities |
| us.unk.bd.cape_cod_biofuels_inc | Cape Cod BioFuels, Inc. | Cape Cod BioFuels, Inc. |  | Operating | 0.4 | biodiesel_facilities |
| 320 | Cargill Inc. - Iowa Falls | Cargill Inc. | Iowa Falls | operating | 70.0 | biofuel_facilities |
| 322 | Cargill Inc. - Owensboro | Cargill Inc. | Owensboro | operating | 60.0 | biofuel_facilities |
| us.unk.bd.cargill_inc_iowa_falls | Cargill Inc.-Iowa Falls | Cargill Inc.-Iowa Falls |  | Operating | 56.0 | biodiesel_facilities |
| us.unk.bd.cargill_inc_wichita | Cargill Inc.-Wichita | Cargill Inc.-Wichita |  | Operating | 60.0 | biodiesel_facilities |
| us.unk.bd.center_alternative_energy_company | Center Alternative Energy Company | Center Alternative Energy Comp |  | Operating |  | biodiesel_facilities |
| 80 | Center Point/Fulcrum Bioenergy - Ga | Center Point/Fulcrum Bioenergy |  | operating | 33.0 | biofuel_facilities |
| 78 | Chevron - Readifuels - IA | Chevron |  | operating | 36.0 | biofuel_facilities |
| us.unk.rd.china_sinopec | China Sinopec | China Sinopec |  | Operating |  | renewable_diesel_facilities |
| 263 | Chuangui New Energy - Qinzhou | Chuangui New Energy | Qinzhou | planned | 75.0 | biofuel_facilities |
| us.unk.rd.cielo_canada | Cielo - Canada | Cielo - Canada |  | Operating |  | renewable_diesel_facilities |
| us.unk.bd.cielo_waste_solutions_high_river | Cielo Waste Solutions - High River | Cielo Waste Solutions - High R |  | Operating | 2.0 | biodiesel_facilities |
| us.unk.bd.cincinnati_renewable_fuels_llc | Cincinnati Renewable Fuels LLC | Cincinnati Renewable Fuels LLC |  | Operating | 70.0 | biodiesel_facilities |
| us.unk.bd.closed | Closed | Closed |  | Operating |  | biodiesel_facilities |
| us.unk.rd.co_processing_domestic | Co-Processing - Domestic | Co-Processing - Domestic |  | Operating |  | renewable_diesel_facilities |
| us.unk.rd.co_processing_foreign | Co-Processing - Foreign | Co-Processing - Foreign |  | Operating |  | renewable_diesel_facilities |
| us.unk.bd.community_fuels | Community Fuels | Community Fuels |  | Operating | 22.5 | biodiesel_facilities |
| us.unk.bd.cowichan_bio_diesel_co_op | Cowichan Bio-Diesel Co-Op | Cowichan Bio-Diesel Co-Op |  | Operating | 0.4 | biodiesel_facilities |
| 336 | Crimson Renewable Energy LP - Baker | Crimson Renewable Energy LP | Bakersfield | operating | 36.0 | biofuel_facilities |
| us.unk.bd.crimson_renewable_energy_lp | Crimson Renewable Energy, LP | Crimson Renewable Energy, LP |  | Operating | 36.0 | biodiesel_facilities |
| 108 | D4 - Domestic Renewable Jet Fuel | D4 |  | operating |  | biofuel_facilities |
| 107 | D4 Domestic RD | D4 Domestic RD |  | operating |  | biofuel_facilities |
| us.unk.bd.dallas_county_schools | Dallas County Schools | Dallas County Schools |  | Operating | 0.0 | biodiesel_facilities |
| us.unk.bd.darling_ingredients_inc_ky | Darling Ingredients Inc KY | Darling Ingredients Inc KY |  | Operating |  | biodiesel_facilities |
| us.unk.bd.deerfield_energy | Deerfield Energy | Deerfield Energy |  | Operating | 50.0 | biodiesel_facilities |
| 323 | Deerfield Energy LLC - Deerfield | Deerfield Energy LLC | Deerfield | operating | 60.0 | biofuel_facilities |
| us.unk.bd.dickinson_college_biodiesel | Dickinson College Biodiesel | Dickinson College Biodiesel |  | Operating | 0.0 | biodiesel_facilities |
| us.unk.bd.double_diamond_energy_inc | Double Diamond Energy Inc. | Double Diamond Energy Inc. |  | Operating | 40.0 | biodiesel_facilities |
| us.unk.bd.down_to_earth_energy_inc | Down to Earth Energy, Inc. | Down to Earth Energy, Inc. |  | Operating | 2.0 | biodiesel_facilities |
| us.unk.bd.duonix_llc | Duonix LLC | Duonix LLC |  | Operating | 50.0 | biodiesel_facilities |
| 275 | ECB Group -Omega Green project - Vi | ECB Group -Omega Green project | Villeta | under_construction | 147.0 | biofuel_facilities |
| 106 | EIA Reported Capacity | EIA Reported Capacity |  | operating |  | biofuel_facilities |
| 272 | ENI - Gela | ENI | Gela | announced | 24.8 | biofuel_facilities |
| us.unk.rd.eni_sicily_gela_expansion | ENI (Sicily Gela Expansion) | ENI (Sicily Gela Expansion) |  | Operating |  | renewable_diesel_facilities |
| us.unk.rd.eni_venice_expansion | ENI (Venice Expansion) | ENI (Venice Expansion) |  | Operating |  | renewable_diesel_facilities |
| us.unk.rd.eni_venice_porto_marghera | ENI Venice (Porto Marghera) | ENI Venice (Porto Marghera) |  | Operating |  | renewable_diesel_facilities |
| us.unk.bd.envia_energy_oklahoma_city_llc | ENVIA Energy Oklahoma City LLC | ENVIA Energy Oklahoma City LLC |  | Operating | 3.0 | biodiesel_facilities |
| us.unk.bd.eberle_biodiesel | Eberle Biodiesel | Eberle Biodiesel |  | Operating | 0.3 | biodiesel_facilities |
| 255 | Eco-Refineries of the South (ERS) p | Eco-Refineries of the South (E | Chubut | announced | 17.4 | biofuel_facilities |
| 264 | EcoCeres - Zhangjiagaing, Jiangsu | EcoCeres | Zhangjiagaing, Jiangsu | planned | 55.5 | biofuel_facilities |
| us.unk.bd.ecogy_bio_fuels_llc | Ecogy Bio-Fuels LLC | Ecogy Bio-Fuels LLC |  | Operating | 30.0 | biodiesel_facilities |
| 103 | Edgewood | Edgewood |  | operating | 120.0 | biofuel_facilities |
| 91 | Emerald One - Port Arthur TX | Emerald One |  | operating | 120.0 | biofuel_facilities |
| us.unk.bd.emergent_green_energy | Emergent Green Energy | Emergent Green Energy |  | Operating | 5.0 | biodiesel_facilities |
| 262 | Energy China - Heilongjiang | Energy China | Heilongjiang | planned | 99.0 | biofuel_facilities |
| 269 | Engie/Infinium (Project Reuze) - Du | Engie/Infinium (Project Reuze) | Dunkirk | under_construction | 9.1 | biofuel_facilities |
| 273 | EniLive - Livorno | EniLive | Livorno | operating | 1.7 | biofuel_facilities |
| us.unk.bd.enviro_brite_solutions | Enviro-Brite Solutions | Enviro-Brite Solutions |  | Operating | 0.1 | biodiesel_facilities |
| us.unk.bd.ethos_alternative_energy | Ethos Alternative Energy | Ethos Alternative Energy |  | Operating | 5.0 | biodiesel_facilities |
| us.unk.bd.ever_cat_fuels_llc | Ever Cat Fuels, LLC | Ever Cat Fuels, LLC |  | Operating | 3.0 | biodiesel_facilities |
| us.unk.bd.foothills_bio_energies_llc | Foothills Bio-Energies LLC | Foothills Bio-Energies LLC |  | Operating | 5.0 | biodiesel_facilities |
| us.unk.rd.foreign_rd | Foreign RD | Foreign RD |  | Operating |  | renewable_diesel_facilities |
| us.unk.bd.fuel_bio_one_llc | Fuel: Bio One LLC | Fuel: Bio One LLC |  | Operating | 25.0 | biodiesel_facilities |
| 79 | Fulcrum Bioenergy - Sierra - Reno N | Fulcrum Bioenergy |  | operating | 11.0 | biofuel_facilities |
| us.unk.rd.fulcrum_gary_indiana | Fulcrum Gary Indiana | Fulcrum Gary Indiana |  | Operating |  | renewable_diesel_facilities |
| us.unk.rd.fulcrum_seirra_nevada | Fulcrum Seirra Nevada | Fulcrum Seirra Nevada |  | Operating |  | renewable_diesel_facilities |
| us.unk.bd.geb3 | GEB3 | GEB3 |  | Operating | 40.0 | biodiesel_facilities |
| us.unk.bd.gtbe_production | GTBE Production | GTBE Production |  | Operating | 1.2 | biodiesel_facilities |
| 277 | Galp/Mitsui Sines biofuels project  | Galp/Mitsui Sines biofuels pro | Sines | under_construction | 44.5 | biofuel_facilities |
| us.unk.bd.general_biodiesel_northwest | General Biodiesel Northwest | General Biodiesel Northwest |  | Operating | 10.0 | biodiesel_facilities |
| us.unk.bd.genuine_bio_fuel_of_new_jersey | Genuine Bio-Fuel of New Jersey | Genuine Bio-Fuel of New Jersey |  | Operating | 5.5 | biodiesel_facilities |
| us.unk.bd.genuine_bio_fuel_inc | Genuine Bio-Fuel, Inc. | Genuine Bio-Fuel, Inc. |  | Operating | 9.2 | biodiesel_facilities |
| us.unk.bd.geogreen_biofuels_inc | GeoGreen Biofuels, Inc. | GeoGreen Biofuels, Inc. |  | Operating | 3.0 | biodiesel_facilities |
| us.unk.bd.global_fuels_llc | Global Fuels LLC | Global Fuels LLC |  | Operating | 6.0 | biodiesel_facilities |
| us.unk.bd.gold_coast_refining_llc | Gold Coast Refining LLC | Gold Coast Refining LLC |  | Operating | 40.0 | biodiesel_facilities |
| us.unk.bd.golden_leaf_energy_llc | Golden Leaf Energy LLC | Golden Leaf Energy LLC |  | Operating | 2.2 | biodiesel_facilities |
| us.unk.bd.grecycle_arizona_llc | Grecycle Arizona, LLC | Grecycle Arizona, LLC |  | Operating |  | biodiesel_facilities |
| us.unk.bd.green_biofuels_miami_llc | Green Biofuels Miami LLC | Green Biofuels Miami LLC |  | Operating | 7.0 | biodiesel_facilities |
| us.unk.bd.green_energy_biofuels | Green Energy Biofuels | Green Energy Biofuels |  | Operating | 0.3 | biodiesel_facilities |
| us.unk.bd.griffin_industries_inc | Griffin Industries Inc. | Griffin Industries Inc. |  | Operating | 2.0 | biodiesel_facilities |
| 84 | Gron - Baton Rouge | Gron |  | operating | 920.0 | biofuel_facilities |
| us.unk.bd.hero_bx_alabama_llc | HERO BX Alabama, LLC | HERO BX Alabama, LLC |  | Operating | 20.0 | biodiesel_facilities |
| us.unk.bd.hero_bx_illinois | HERO BX Illinois | HERO BX Illinois |  | Operating | 15.0 | biodiesel_facilities |
| us.unk.bd.hero_bx_iowa | HERO BX Iowa | HERO BX Iowa |  | Operating |  | biodiesel_facilities |
| us.unk.bd.hero_bx_lake_erie_biofuels | HERO BX Lake Erie Biofuels | HERO BX Lake Erie Biofuels |  | Operating | 50.0 | biodiesel_facilities |
| 102 | HOBO Renewables #1 | HOBO Renewables #1 |  | operating | 120.0 | biofuel_facilities |
| 270 | HOLBORN Europa Raffinerie - Hamburg | HOLBORN Europa Raffinerie | Hamburg | operating | 36.3 | biofuel_facilities |
| 86 | Heartwell Renewables (Cargill - Lov | Heartwell Renewables (Cargill |  | operating | 80.0 | biofuel_facilities |
| us.unk.bd.hofti | Hofti | Hofti |  | Operating |  | biodiesel_facilities |
| us.unk.rd.hollyfrontier_cheyenne_refinery | HollyFrontier - Cheyenne Refinery | HollyFrontier - Cheyenne Refin |  | Operating |  | renewable_diesel_facilities |
| us.unk.rd.hollyfrontier_new_mexico_artesia | HollyFrontier - New Mexico - Artesi | HollyFrontier - New Mexico - A |  | Operating |  | renewable_diesel_facilities |
| us.unk.bd.idle | Idle | Idle |  | Operating |  | biodiesel_facilities |
| 342 | Imperial Western Products Inc. - Co | Imperial Western Products Inc. | Coachella | operating | 12.0 | biofuel_facilities |
| 116 | Implied Monthly BD Yield | Implied Monthly BD Yield |  | operating |  | biofuel_facilities |
| 112 | Implied Monthly RD Yield | Implied Monthly RD Yield |  | operating |  | biofuel_facilities |
| 113 | Implied Monthly RD Yield ex. SAF | Implied Monthly RD Yield ex. S |  | operating |  | biofuel_facilities |
| 121 | Implied Monthly SAF Yield | Implied Monthly SAF Yield |  | operating |  | biofuel_facilities |
| 319 | Incobrasa Industries LTD. - Gilman | Incobrasa Industries LTD. | Gilman | operating | 75.0 | biofuel_facilities |
| us.unk.bd.incobrasa_industries_ltd | Incobrasa Industries, Ltd. | Incobrasa Industries, Ltd. |  | Operating | 32.0 | biodiesel_facilities |
| 99 | Indaba Renewable Fuels -  Imperial  | Indaba Renewable Fuels |  | operating | 95.0 | biofuel_facilities |
| 290 | Ineratec/ Zenith Energy Terminals - | Ineratec/ Zenith Energy Termin | Amsterdam | announced | 5.8 | biofuel_facilities |
| us.unk.bd.innoltek_inc | Innoltek Inc | Innoltek Inc |  | Operating | 6.0 | biodiesel_facilities |
| us.unk.bd.integrity_biofuels | Integrity Biofuels | Integrity Biofuels |  | Operating | 5.2 | biodiesel_facilities |
| us.unk.bd.invigor_bioenergy_corp | Invigor Bioenergy Corp. | Invigor Bioenergy Corp. |  | Operating | 71.0 | biodiesel_facilities |
| 335 | Iowa Renewable Energy LLC - Washing | Iowa Renewable Energy LLC | Washington | operating | 36.0 | biofuel_facilities |
| us.unk.bd.iowa_renewable_energy_llc | Iowa Renewable Energy, LLC | Iowa Renewable Energy, LLC |  | Operating | 30.0 | biodiesel_facilities |
| us.unk.bd.jns_biofuels | JNS Biofuels | JNS Biofuels |  | Operating | 7.5 | biodiesel_facilities |
| us.unk.rd.jp_has_it_jacobsen_doesn_t | JP has it, Jacobsen Doesn’t | JP has it, Jacobsen Doesn’t |  | Operating |  | renewable_diesel_facilities |
| us.unk.rd.jacobsen_has_it_jp_doesn_t | Jacobsen has it, JP Doesn't | Jacobsen has it, JP Doesn't |  | Operating |  | renewable_diesel_facilities |
| 261 | Jiaao Enprotech/ BP(15%) - Lianyung | Jiaao Enprotech/ BP(15%) | Lianyungang | planned | 125.0 | biofuel_facilities |
| 274 | KazMunayGaz (KMG) / Axens | KazMunayGaz (KMG) / Axens |  | announced | 49.5 | biofuel_facilities |
| us.unk.bd.kelley_green_biofuel | Kelley Green Biofuel | Kelley Green Biofuel |  | Operating | 0.1 | biodiesel_facilities |
| us.unk.rd.kern_oil | Kern Oil | Kern Oil |  | Operating |  | renewable_diesel_facilities |
| 344 | Kern Oil & Refining 1 - Bakersfield | Kern Oil & Refining 1 | Bakersfield | operating | 6.0 | biofuel_facilities |
| us.unk.bd.lakeview_biodiesel_llc | Lakeview Biodiesel LLC | Lakeview Biodiesel LLC |  | Operating | 10.0 | biodiesel_facilities |
| us.unk.bd.louis_dreyfus_agricultural_industries_llc | Louis Dreyfus Agricultural Industri | Louis Dreyfus Agricultural Ind |  | Operating | 90.0 | biodiesel_facilities |
| us.unk.bd.louisiana_eco_green_llc | Louisiana ECO Green, LLC | Louisiana ECO Green, LLC |  | Operating |  | biodiesel_facilities |
| us.unk.bd.loyola_university_chicago | Loyola University Chicago | Loyola University Chicago |  | Operating | 0.1 | biodiesel_facilities |
| us.unk.bd.me_bio_energy_llc | ME Bio Energy LLC | ME Bio Energy LLC |  | Operating | 5.0 | biodiesel_facilities |
| us.unk.bd.maine_bio_fuel_inc | Maine Bio-Fuel Inc. | Maine Bio-Fuel Inc. |  | Operating | 1.5 | biodiesel_facilities |
| us.unk.bd.maine_standard_biofuel | Maine Standard Biofuel | Maine Standard Biofuel |  | Operating |  | biodiesel_facilities |
| us.unk.bd.marathon | Marathon | Marathon |  | Operating | 100.0 | biodiesel_facilities |
| us.unk.rd.marathon_martinez | Marathon - Martinez | Marathon - Martinez |  | Operating |  | renewable_diesel_facilities |
| us.unk.bd.marathon_nd_stage_1 | Marathon ND - Stage 1 | Marathon ND - Stage 1 |  | Operating | 100.0 | biodiesel_facilities |
| us.unk.bd.marathon_nd_stage_2 | Marathon ND - Stage 2 | Marathon ND - Stage 2 |  | Operating | 240.0 | biodiesel_facilities |
| 312 | Martinez Renewables LLC - Martinez | Martinez Renewables LLC | Martinez | operating | 731.0 | biofuel_facilities |
| us.unk.bd.mason_biodiesel_llc | Mason Biodiesel LLC | Mason Biodiesel LLC |  | Operating | 1.2 | biodiesel_facilities |
| us.unk.bd.mid_america_agri_products_wheatland_llc | Mid America Agri Products/Wheatland | Mid America Agri Products/Whea |  | Operating | 1.0 | biodiesel_facilities |
| 327 | Mid-America Biofuels LLC - Mexico | Mid-America Biofuels LLC | Mexico | operating | 50.0 | biofuel_facilities |
| us.unk.bd.mid_america_biofuels_llc | Mid-America Biofuels, LLC | Mid-America Biofuels, LLC |  | Operating | 50.0 | biodiesel_facilities |
| us.unk.bd.midlands_biofuels_llc | Midlands Biofuels LLC | Midlands Biofuels LLC |  | Operating | 0.2 | biodiesel_facilities |
| us.unk.bd.midwest_biodiesel_products_llc | Midwest Biodiesel Products LLC | Midwest Biodiesel Products LLC |  | Operating | 12.0 | biodiesel_facilities |
| us.unk.bd.milligan_bio_tech_inc | Milligan Bio-Tech Inc. | Milligan Bio-Tech Inc. |  | Operating | 14.0 | biodiesel_facilities |
| us.unk.bd.minnesota_soybean_processors | Minnesota Soybean Processors | Minnesota Soybean Processors |  | Operating | 30.0 | biodiesel_facilities |
| 280 | Moeve (formerly Cepsa)/Apical - Bio | Moeve (formerly Cepsa)/Apical  | Andalusia | under_construction | 82.6 | biofuel_facilities |
| us.unk.bd.natural_biodiesel_plant_llc | Natural Biodiesel Plant LLC | Natural Biodiesel Plant LLC |  | Operating | 5.0 | biodiesel_facilities |
| 287 | Neste - Rotterdam | Neste | Rotterdam | operating | 264.0 | biofuel_facilities |
| us.unk.rd.neste_denmark_rotterdam | Neste Denmark/Rotterdam | Neste Denmark/Rotterdam |  | Operating |  | renewable_diesel_facilities |
| us.unk.rd.neste_finland_1 | Neste Finland 1 | Neste Finland 1 |  | Operating |  | renewable_diesel_facilities |
| us.unk.rd.neste_finland_2 | Neste Finland 2 | Neste Finland 2 |  | Operating |  | renewable_diesel_facilities |
| 93 | New Rise Renewables - Reno NV | New Rise Renewables |  | operating | 47.0 | biofuel_facilities |
| us.unk.bd.newport_biodiesel_inc | Newport Biodiesel, Inc. | Newport Biodiesel, Inc. |  | Operating | 2.8 | biodiesel_facilities |
| us.unk.rd.next_fuels_portland_or_bp_shell | Next Fuels Portland OR (BP/Shell) | Next Fuels Portland OR (BP/She |  | Operating |  | renewable_diesel_facilities |
| us.unk.bd.northeast_biodiesel_llc | Northeast Biodiesel LLC | Northeast Biodiesel LLC |  | Operating | 1.8 | biodiesel_facilities |
| 92 | Northwest Advanced Biofuels - Grays | Northwest Advanced Biofuels |  | operating | 60.0 | biofuel_facilities |
| 278 | OMV Petrobrazi Refinery - Ploiesti | OMV Petrobrazi Refinery | Ploiesti | under_construction | 41.3 | biofuel_facilities |
| us.unk.bd.omaha_biofuels_coop | Omaha Biofuels Coop | Omaha Biofuels Coop |  | Operating | 0.0 | biodiesel_facilities |
| us.unk.bd.owensboro_grain_biodiesel_llc | Owensboro Grain Biodiesel LLC | Owensboro Grain Biodiesel LLC |  | Operating | 45.0 | biodiesel_facilities |
| 73 | P66 - Rodeo CA | P66 |  | operating |  | biofuel_facilities |
| 74 | P66 - Ryze - Las Vegas NV | P66 |  | operating | 100.0 | biofuel_facilities |
| us.unk.bd.p66_ferndale_wa | P66 Ferndale WA | P66 Ferndale WA |  | Operating | 120.0 | biodiesel_facilities |
| 85 | PBF (ENI Sustained Mobility) - Chal | PBF (ENI Sustained Mobility) |  | operating | 306.0 | biofuel_facilities |
| 276 | PKN - Plock | PKN | Plock | under_construction | 49.6 | biofuel_facilities |
| us.unk.bd.pacific_biodiesel | Pacific Biodiesel | Pacific Biodiesel |  | Operating | 5.5 | biodiesel_facilities |
| us.unk.bd.parkland | Parkland | Parkland |  | Operating |  | biodiesel_facilities |
| us.unk.bd.paseo_cargill_energy_llc | Paseo - Cargill Energy, LLC | Paseo - Cargill Energy, LLC |  | Operating | 56.0 | biodiesel_facilities |
| 321 | Paseo Cargill Energy LLC - Kansas   | Paseo Cargill Energy LLC | Kansas  City | operating | 70.0 | biofuel_facilities |
| us.unk.bd.patriot_biodiesel_llc | Patriot Biodiesel LLC | Patriot Biodiesel LLC |  | Operating | 5.2 | biodiesel_facilities |
| us.unk.bd.patriot_fuels_biodiesel_llc_cargill | Patriot Fuels Biodiesel, LLC - Carg | Patriot Fuels Biodiesel, LLC - |  | Operating | 5.0 | biodiesel_facilities |
| 259 | Petrobras - Cubatao | Petrobras | Cubatao | announced | 45.5 | biofuel_facilities |
| 291 | Petroineos (Petrochina/ Ineos) - Gr | Petroineos (Petrochina/ Ineos) | Grangemouth, Scotland | operating | 33.0 | biofuel_facilities |
| us.unk.bd.pleasant_valley_biofuels_llc | Pleasant Valley Biofuels LLC | Pleasant Valley Biofuels LLC |  | Operating | 5.2 | biodiesel_facilities |
| 123 | Pre-Aug Revisions | Pre-Aug Revisions |  | operating |  | biofuel_facilities |
| 110 | Pre-Treatment Capacity | Pre-Treatment Capacity |  | operating |  | biofuel_facilities |
| 283 | Preem ProjectViking - Lysekil | Preem ProjectViking | Lysekil | announced | 75.5 | biofuel_facilities |
| 286 | Preem Synsat - Lysekil | Preem Synsat | Lysekil | operating | 7.7 | biofuel_facilities |
| 314 | RBF Port Neches LLC - Houston | RBF Port Neches LLC | Houston | operating | 144.0 | biofuel_facilities |
| us.unk.bd.rbf_port_neches_llc | RBF Port Neches, LLC | RBF Port Neches, LLC |  | Operating | 180.0 | biodiesel_facilities |
| 117 | RD Percent | RD Percent |  | operating |  | biofuel_facilities |
| us.unk.bd.reg_emporia_llc | REG Emporia, LLC | REG Emporia, LLC |  | Operating |  | biodiesel_facilities |
| 316 | REG Geismar LLC - Geismar | REG Geismar LLC | Geismar | operating | 108.0 | biofuel_facilities |
| us.unk.bd.reg_geismar_llc | REG Geismar, LLC | REG Geismar, LLC |  | Operating | 75.0 | biodiesel_facilities |
| us.unk.bd.reg_houston_llc | REG Houston, LLC | REG Houston, LLC |  | Operating | 35.0 | biodiesel_facilities |
| us.unk.bd.reg_madison_llc | REG Madison, LLC | REG Madison, LLC |  | Operating | 20.0 | biodiesel_facilities |
| us.unk.bd.reg_ralston_llc | REG Ralston, LLC | REG Ralston, LLC |  | Operating | 30.0 | biodiesel_facilities |
| us.unk.rd.readifuels_ia_chevron | ReadiFuels IA (Chevron) | ReadiFuels IA (Chevron) |  | Operating |  | renewable_diesel_facilities |
| us.unk.bd.reco_biodiesel_llc | Reco Biodiesel LLC | Reco Biodiesel LLC |  | Operating | 3.6 | biodiesel_facilities |
| us.unk.bd.red_birch_energy_inc | Red Birch Energy Inc. | Red Birch Energy Inc. |  | Operating | 3.0 | biodiesel_facilities |
| 82 | Red Rock Biofuels - Lakeview OR | Red Rock Biofuels |  | operating | 16.1 | biofuel_facilities |
| 256 | Refinaria Riograndense - Rio Grande | Refinaria Riograndense | Rio Grande | announced | 122.6 | biofuel_facilities |
| 326 | Reg Danville - Danville | Reg Danville | Danville | operating | 50.0 | biofuel_facilities |
| 334 | Reg Mason City LLC - Mason City | Reg Mason City LLC | Mason City | operating | 39.0 | biofuel_facilities |
| 333 | Reg Newton LLC - Newton | Reg Newton LLC | Newton | operating | 41.0 | biofuel_facilities |
| 317 | Reg Seneca - Seneca | Reg Seneca | Seneca | operating | 80.0 | biofuel_facilities |
| 281 | Repsol - Cartegena | Repsol | Cartegena | operating | 41.3 | biofuel_facilities |
| 282 | Repsol - Puertollano | Repsol | Puertollano | announced | 39.6 | biofuel_facilities |
| us.unk.bd.rio_valley_biofuels_llc | Rio Valley Biofuels LLC | Rio Valley Biofuels LLC |  | Operating | 17.0 | biodiesel_facilities |
| us.unk.bd.rothsay_biodiesel_llc | Rothsay Biodiesel LLC | Rothsay Biodiesel LLC |  | Operating | 45.0 | biodiesel_facilities |
| us.unk.rd.rumored_production_foreign | Rumored  Production - Foreign | Rumored  Production - Foreign |  | Operating |  | renewable_diesel_facilities |
| us.unk.rd.rumored_domestic_production | Rumored Domestic Production | Rumored Domestic Production |  | Operating |  | renewable_diesel_facilities |
| us.unk.rd.ryze_p66_las_vegas_nv | Ryze (P66) Las Vegas NV | Ryze (P66) Las Vegas NV |  | Operating |  | renewable_diesel_facilities |
| us.unk.rd.ryze_p66_reno_nv | Ryze (P66) Reno, NV | Ryze (P66) Reno, NV |  | Operating |  | renewable_diesel_facilities |
| 119 | SAF | SAF |  | operating |  | biofuel_facilities |
| 89 | SAFuels X  - Trenton ND | SAFuels X |  | operating |  | biofuel_facilities |
| us.unk.bd.sjv_biodiesel_llc | SJV Biodiesel LLC | SJV Biodiesel LLC |  | Operating | 5.0 | biodiesel_facilities |
| us.unk.bd.sme_dublin_llc | SME Dublin LLC | SME Dublin LLC |  | Operating | 5.0 | biodiesel_facilities |
| 285 | ST1/SCA - Gothenburg | ST1/SCA | Gothenburg | operating | 33.0 | biofuel_facilities |
| us.unk.bd.sequential_crimson_owned_now | SeQuential (Crimson owned now) | SeQuential (Crimson owned now) |  | Operating | 17.0 | biodiesel_facilities |
| 63 | Seaboard - Houghton KS | Seaboard |  | operating | 85.0 | biofuel_facilities |
| us.unk.bd.seaboard_energy_missouri_llc | Seaboard Energy Missouri LLC | Seaboard Energy Missouri LLC |  | Operating |  | biodiesel_facilities |
| 337 | Seaboard Energy Missouri LLC - St.  | Seaboard Energy Missouri LLC | St. Joseph | operating | 35.0 | biofuel_facilities |
| us.unk.bd.seaboard_energy_oklahoma_llc | Seaboard Energy Oklahoma LLC | Seaboard Energy Oklahoma LLC |  | Operating |  | biodiesel_facilities |
| 288 | Shell - Rotterdam | Shell | Rotterdam | idle | 135.4 | biofuel_facilities |
| 325 | Shell Oil Products U.S. 1 - Norco | Shell Oil Products U.S. 1 | Norco | operating | 54.0 | biofuel_facilities |
| us.unk.bd.shenandoah_agricultural_products | Shenandoah Agricultural Products | Shenandoah Agricultural Produc |  | Operating | 0.3 | biodiesel_facilities |
| us.unk.bd.simple_fuels_biodiesel_inc | Simple Fuels Biodiesel, Inc. | Simple Fuels Biodiesel, Inc. |  | Operating | 2.0 | biodiesel_facilities |
| us.unk.bd.sinclair | Sinclair | Sinclair |  | Operating | 100.0 | biodiesel_facilities |
| 266 | Sinopec - Zhenhai, Zhejiang | Sinopec | Zhenhai, Zhejiang | planned | 16.5 | biofuel_facilities |
| 265 | Sinopec/TotalEnergies | Sinopec/TotalEnergies |  | operating | 38.0 | biofuel_facilities |
| 289 | SkyNRG - Delfzijl | SkyNRG | Delfzijl | announced | 57.8 | biofuel_facilities |
| us.unk.bd.southeast_biodiesel_llc | Southeast Biodiesel LLC | Southeast Biodiesel LLC |  | Operating | 5.0 | biodiesel_facilities |
| us.unk.bd.stepan_co_joliet | Stepan Co.-Joliet | Stepan Co.-Joliet |  | Operating | 21.0 | biodiesel_facilities |
| us.unk.bd.stepan_company | Stepan Company | Stepan Company |  | Operating |  | biodiesel_facilities |
| 87 | Strategic Biofuels - Port of Colomb | Strategic Biofuels |  | operating | 32.0 | biofuel_facilities |
| us.unk.bd.sullens_biodiesel_llc | Sullens Biodiesel LLC | Sullens Biodiesel LLC |  | Operating | 2.0 | biodiesel_facilities |
| 260 | Sulzer (tech provider - who is oper | Sulzer (tech provider - who is | Shandong | planned | 165.0 | biofuel_facilities |
| us.unk.bd.sun_products_corporation_pasadena | Sun Products Corporation-Pasadena | Sun Products Corporation-Pasad |  | Operating | 4.0 | biodiesel_facilities |
| us.unk.bd.suncor | Suncor | Suncor |  | Operating |  | biodiesel_facilities |
| 83 | Suncor - Matsui - Lanza Jet - Soper | Suncor |  | operating | 10.0 | biofuel_facilities |
| us.unk.bd.suspected_closed | Suspected closed | Suspected closed |  | Operating |  | biodiesel_facilities |
| us.unk.bd.synergy_biofuels_llc | Synergy Biofuels LLC | Synergy Biofuels LLC |  | Operating | 3.0 | biodiesel_facilities |
| us.unk.bd.tenaska_biodiesel | Tenaska Biodiesel | Tenaska Biodiesel |  | Operating |  | biodiesel_facilities |
| us.unk.bd.texas_biotech_inc | Texas BioTech Inc. | Texas BioTech Inc. |  | Operating | 3.0 | biodiesel_facilities |
| 105 | Texmark Chemicals | Texmark Chemicals |  | operating | 350.0 | biofuel_facilities |
| us.unk.rd.total | Total | Total |  | Operating |  | renewable_diesel_facilities |
| 111 | Total Feedstock Use | Total Feedstock Use |  | operating |  | biofuel_facilities |
| us.unk.rd.total_la_mede_france | Total La Mede - France | Total La Mede - France |  | Operating |  | renewable_diesel_facilities |
| us.unk.rd.total_of_totals | Total of Totals | Total of Totals |  | Operating |  | renewable_diesel_facilities |
| 268 | TotalEnergies - Grandpuits | TotalEnergies | Grandpuits | under_construction | 17.3 | biofuel_facilities |
| us.unk.bd.triangle_biofuels_industries_inc | Triangle Biofuels Industries Inc. | Triangle Biofuels Industries I |  | Operating | 5.0 | biodiesel_facilities |
| us.unk.bd.united_states_co_processing | UNITED STATES - CO-PROCESSING | UNITED STATES - CO-PROCESSING |  | Operating |  | biodiesel_facilities |
| us.unk.bd.united_states_renewable_diesel | UNITED STATES - RENEWABLE DIESEL | UNITED STATES - RENEWABLE DIES |  | Operating |  | biodiesel_facilities |
| us.unk.bd.united_states_renwable_diesel | UNITED STATES - RENWABLE DIESEL | UNITED STATES - RENWABLE DIESE |  | Operating |  | biodiesel_facilities |
| us.unk.rd.upm_finland_kotka | UPM Finland (Kotka) | UPM Finland (Kotka) |  | Operating |  | renewable_diesel_facilities |
| us.unk.rd.upm_finland_lappeenranta | UPM Finland (Lappeenranta) | UPM Finland (Lappeenranta) |  | Operating |  | renewable_diesel_facilities |
| 345 | US Oil & Refining Co. 1 - Tacoma | US Oil & Refining Co. 1 | Tacoma | operating | 5.0 | biofuel_facilities |
| us.unk.bd.us_questionable | US QUESTIONABLE | US QUESTIONABLE |  | Operating |  | biodiesel_facilities |
| us.unk.bd.under_construction | Under Construction | Under Construction |  | Operating |  | biodiesel_facilities |
| us.unk.bd.viesel_fort_myers | Viesel Fort Myers | Viesel Fort Myers |  | Operating | 6.0 | biodiesel_facilities |
| us.unk.bd.virginia_biodiesel_refinery_llc | Virginia Biodiesel Refinery LLC | Virginia Biodiesel Refinery LL |  | Operating | 5.0 | biodiesel_facilities |
| us.unk.bd.w2fuel_adrian | W2Fuel - Adrian | W2Fuel - Adrian |  | Operating | 10.0 | biodiesel_facilities |
| us.unk.bd.w2fuel_crawfordsville | W2Fuel - Crawfordsville | W2Fuel - Crawfordsville |  | Operating |  | biodiesel_facilities |
| us.unk.bd.wie_agron_bioenergy_llc | WIE - Agron Bioenergy, LLC | WIE - Agron Bioenergy, LLC |  | Operating |  | biodiesel_facilities |
| us.unk.bd.walsh_bio_fuels_llc | Walsh Bio Fuels, LLC | Walsh Bio Fuels, LLC |  | Operating | 5.0 | biodiesel_facilities |
| us.unk.bd.washakie_renewable_energy_llc | Washakie Renewable Energy LLC | Washakie Renewable Energy LLC |  | Operating | 22.0 | biodiesel_facilities |
| us.unk.bd.western_dubuque_biodiesel | Western Dubuque Biodiesel | Western Dubuque Biodiesel |  | Operating | 33.0 | biodiesel_facilities |
| 330 | Western Dubuque Biodiesel  LLC - Fa | Western Dubuque Biodiesel  LLC | Farley | operating | 45.0 | biofuel_facilities |
| 331 | Western Iowa Energy LLC - Wall Lake | Western Iowa Energy LLC | Wall Lake | operating | 45.0 | biofuel_facilities |
| us.unk.bd.white_mountain_biodiesel_llc | White Mountain Biodiesel LLC | White Mountain Biodiesel LLC |  | Operating | 6.5 | biodiesel_facilities |
| us.unk.bd.world_energy_hamilton | World Energy Hamilton | World Energy Hamilton |  | Operating | 67.0 | biodiesel_facilities |
| us.unk.bd.world_energy_houston | World Energy Houston | World Energy Houston |  | Operating | 90.0 | biodiesel_facilities |
| us.unk.bd.world_energy_natchez | World Energy Natchez | World Energy Natchez |  | Operating | 72.0 | biodiesel_facilities |
| us.unk.bd.world_energy_paramount | World Energy Paramount | World Energy Paramount |  | Operating | 40.0 | biodiesel_facilities |
| 67 | World Energy Paramount- Houston, TX | World Energy Paramount- Housto |  | operating | 250.0 | biofuel_facilities |
| us.unk.bd.world_energy_sombra | World Energy Sombra | World Energy Sombra |  | Operating |  | biodiesel_facilities |
| 279 | Zhouyue New Energy | Zhouyue New Energy |  | operating | 26.0 | biofuel_facilities |

### AL (2)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 294 | Alleo Energy - Bay Minette, AL | Alleo Energy | Bay Minette, AL | operating | 1.3 | biofuel_facilities |
| 38 | Hero BX - Moundville | Hero BX | Moundville | operating | 20.0 | biofuel_facilities |

### CA (5)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 96 | Aemetis Carbon Zero - Riverbank, CA | Aemetis Carbon Zero | Riverbank, CA | operating |  | biofuel_facilities |
| us.ca.rd.global_clean_energy_holdings | Global Clean Energy Holdings | Global Clean Energy Holdings | Bakersfield | Operating | 210.0 | renewable_diesel_facilities |
| us.ca.rd.marathon | Marathon | Marathon | Pacheco | Operating | 700.0 | renewable_diesel_facilities |
| 98 | UrbanX - Bakersfield, CA | UrbanX | Bakersfield | operating | 75.0 | biofuel_facilities |
| 43 | Western Iowa Energy - Agron Bioener | Western Iowa Energy | Watsonville | operating | 15.0 | biofuel_facilities |

### CALIFORNIA (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 395 | Bakersfield Renewable Fuels LLC - B | Bakersfield Renewable Fuels LL | Bakersfield | operating | 138.0 | biofuel_facilities |

### FL (2)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 297 | Blue Biofuels - Frostproof, FL | Blue Biofuels | Frostproof, FL | operating | 0.3 | biofuel_facilities |
| 52 | Genuine Bio-Fuel Inc. | Genuine Bio-Fuel Inc. | Indiantown | operating | 9.0 | biofuel_facilities |

### FLORIDA (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 399 | Biodiesel Las Americas LLC - Doral | Biodiesel Las Americas LLC | Doral | operating | 8.0 | biofuel_facilities |

### GA (2)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 57 | SMES Dublin LLC | SMES Dublin LLC | East Dublin | operating | 5.0 | biofuel_facilities |
| us.ga.rd.suncor_matsui_lanza_jet | Suncor - Matsui - Lanza Jet | Suncor - Matsui - Lanza Jet | Soperton | Operating | 10.0 | renewable_diesel_facilities |

### HAWAII (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 401 | Big Island Biodiesel LLC - Keaau | Big Island Biodiesel LLC | Keaau | operating | 6.0 | biofuel_facilities |

### HI (2)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 55 | Pacific Biodiesel | Pacific Biodiesel | Kea`au | operating | 6.0 | biofuel_facilities |
| 298 | Par Pacific - Kapolei, HI | Par Pacific | Kapolei, HI | announced | 24.4 | biofuel_facilities |

### IA (10)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 12 | Cargill - Iowa Falls | Cargill | Iowa Falls | operating | 56.0 | biofuel_facilities |
| ia.cargill_iowa_falls_biodiesel | Cargill Iowa Falls Biodiesel | Cargill | Iowa Falls | active |  | facility_master |
| ia.chevron_reg_mason_city | Chevron REG Mason City | Chevron Renewable Energy Group | Mason City | active |  | facility_master |
| ia.chevron_reg_newton | Chevron REG Newton | Chevron Renewable Energy Group | Newton | active |  | facility_master |
| ia.chevron_reg_ralston | Chevron REG Ralston | Chevron Renewable Energy Group | Ralston | active |  | facility_master |
| 49 | Hero BX - Clinton | Hero BX | Clinton | operating | 10.0 | biofuel_facilities |
| 31 | Iowa Renewable Energy LLC | Iowa Renewable Energy LLC | Washington | operating | 36.0 | biofuel_facilities |
| 50 | W2Fuel - Crawfordsville | W2Fuel | Crawfordsville | operating | 10.0 | biofuel_facilities |
| 33 | Western Dubuque Biodiesel LLC | Western Dubuque Biodiesel LLC | Farley | operating | 36.0 | biofuel_facilities |
| ia.western_iowa_wall_lake_biodiesel | Western Iowa Energy Biodiesel | Western Iowa Energy LLC | Wall Lake | active |  | facility_master |

### IL (3)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 299 | Aether Fuels Aurora/ GTI Energy - C | Aether Fuels Aurora/ GTI Energ | Chicago, IL | operating |  | biofuel_facilities |
| 10 | Incobrasa Industries Ltd. | Incobrasa Industries Ltd. | Gilman | operating | 62.0 | biofuel_facilities |
| 94 | LanzaJet Marquis - Hennepin, IL | LanzaJet Marquis | Hennepin | operating | 120.0 | biofuel_facilities |

### IN (2)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| us.in.rd.fulcrum_bioenergy | Fulcrum Bioenergy | Fulcrum Bioenergy | Gary | Operating | 33.0 | renewable_diesel_facilities |
| 3 | Louis Dreyfus Agricultural Industri | Louis Dreyfus Agricultural Ind | Claypool | operating | 99.0 | biofuel_facilities |

### KANSAS (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 407 | Cargill-Wichita - Wichita | Cargill-Wichita | Wichita | operating | 100.0 | biofuel_facilities |

### KENTUCKY (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 409 | Continental Refining Company - Some | Continental Refining Company | Somerset | operating | 5.0 | biofuel_facilities |

### KS (5)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| us.ks.rd.cvr_energy | CVR Energy | CVR Energy | Coffeyville | Operating | 150.0 | renewable_diesel_facilities |
| 4 | Cargill - Wichita | Cargill | Wichita | operating | 90.0 | biofuel_facilities |
| us.ks.rd.east_kansas_agri_energy_llc | East Kansas Agri-Energy LLC | East Kansas Agri-Energy LLC | Garnett | Operating | 3.0 | renewable_diesel_facilities |
| 301 | SAFFiRE - Liberal, KS | SAFFiRE | Liberal, KS | announced | 0.1 | biofuel_facilities |
| us.ks.rd.seaboard | Seaboard | Seaboard | Houghton | Operating | 85.0 | renewable_diesel_facilities |

### KY (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 14 | Owensboro Grain Biodiesel LLC | Owensboro Grain Biodiesel LLC | Owensboro | operating | 54.0 | biofuel_facilities |

### LA (7)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| us.la.rd.diamond_green_diesel | Diamond Green Diesel | Diamond Green Diesel | Norco | Operating | 290.0 | renewable_diesel_facilities |
| 60 | Diamond Green Diesel - Norco, LA | Diamond Green Diesel | Norco | operating |  | biofuel_facilities |
| us.la.rd.gron | Gron | Gron | Baton Rouge | Operating | 920.0 | renewable_diesel_facilities |
| us.la.rd.pbf | PBF | PBF | Chalmette | Operating | 306.0 | renewable_diesel_facilities |
| us.la.rd.reg_geismar | REG Geismar | REG Geismar | Geismar | Operating |  | renewable_diesel_facilities |
| 64 | REG Geismar - Geismar, LA | REG Geismar | Geismar | operating |  | biofuel_facilities |
| us.la.rd.strategic_biofuels | Strategic Biofuels | Strategic Biofuels | Port of Colombia | Operating | 32.0 | renewable_diesel_facilities |

### MAINE (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 302 | Maine Bio-Fuel Inc. - Portland | Maine Bio-Fuel Inc. | Portland | operating | 1.0 | biofuel_facilities |

### MASSACHUSETTS (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 412 | Cape Cod Biofuels Inc. - Sandwich | Cape Cod Biofuels Inc. | Sandwich | operating | 1.0 | biofuel_facilities |

### MI (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 42 | W2Fuel - Adrian | W2Fuel | Adrian | operating | 15.0 | biofuel_facilities |

### MICHIGAN (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 413 | Adrian LVA Biofuel LLC - Adrian | Adrian LVA Biofuel LLC | Adrian | operating | 15.0 | biofuel_facilities |

### MINNESOTA (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 414 | Minnesota Soybean Processors - Brew | Minnesota Soybean Processors | Brewster | operating | 36.0 | biofuel_facilities |

### MN (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 30 | Minnesota Soybean Processors | Minnesota Soybean Processors | Brewster | operating | 36.0 | biofuel_facilities |

### MO (4)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 22 | Ag Processing Inc. - St. Joseph | Ag Processing Inc. | St. Joseph | operating | 42.0 | biofuel_facilities |
| 13 | Cargill - Kansas City | Cargill | Kansas City | operating | 56.0 | biofuel_facilities |
| 16 | Mid-America Biofuels | Mid-America Biofuels | Mexico | operating | 50.0 | biofuel_facilities |
| 34 | Seaboard Energy Missouri LLC | Seaboard Energy Missouri LLC | St. Joseph | operating | 35.0 | biofuel_facilities |

### MS (4)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 81 | Shell - Jaxon Energy - Jackson, MS | Shell | Jackson | operating | 20.0 | biofuel_facilities |
| us.ms.rd.shell_jaxon_energy | Shell - Jaxon Energy | Shell - Jaxon Energy | Jackson | Operating | 20.0 | renewable_diesel_facilities |
| 95 | Velocys (Bayou Fuels) - Natchez, MS | Velocys (Bayou Fuels) | Natchez | operating | 35.0 | biofuel_facilities |
| 9 | World Energy - Natchez | World Energy | Natchez | operating | 72.0 | biofuel_facilities |

### MT (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| us.mt.rd.calumet_montana_refining | Calumet Montana Refining | Calumet Montana Refining | Great Falls | Operating | 150.0 | renewable_diesel_facilities |

### ND (4)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 97 | Gevo - Lake Preston, ND | Gevo | Lake Preston | operating | 46.0 | biofuel_facilities |
| us.nd.rd.marathon | Marathon | Marathon | Dickinson | Operating | 184.0 | renewable_diesel_facilities |
| 72 | Marathon - Dickinson, ND | Marathon | Dickinson | operating | 184.0 | biofuel_facilities |
| us.nd.rd.safuels_x | SAFuels X | SAFuels X | Trenton | Operating |  | renewable_diesel_facilities |

### NE (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| us.ne.rd.cargill_loves | Cargill - Loves | Cargill - Loves | Hastings | Operating | 80.0 | renewable_diesel_facilities |

### NEW HAMPSHIRE (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 419 | Renewable Fuels by Peterson - North | Renewable Fuels by Peterson | North Haverhill | operating | 8.0 | biofuel_facilities |

### NH (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 54 | White Mountain Biodiesel LLC | White Mountain Biodiesel LLC | North Haverhill | operating | 8.0 | biofuel_facilities |

### NM (2)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| us.nm.rd.holly_frontier | Holly Frontier | Holly Frontier | Artesia | Operating | 125.0 | renewable_diesel_facilities |
| 69 | Holly Frontier - Artesia, NM | Holly Frontier | Artesia | operating |  | biofuel_facilities |

### NORTH CAROLINA (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 421 | Blue Ridge Biofuels LLC - Newton | Blue Ridge Biofuels LLC | Newton | operating | 2.0 | biofuel_facilities |

### NV (3)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 303 | Edgewood Renewables - Clark County, | Edgewood Renewables | Clark County, NV | under_construction | 60.0 | biofuel_facilities |
| us.nv.rd.fulcrum_bioenergy | Fulcrum Bioenergy | Fulcrum Bioenergy | Reno | Operating | 11.0 | renewable_diesel_facilities |
| us.nv.rd.p66_ryze | P66 - Ryze | P66 - Ryze | Las Vegas | Operating | 100.0 | renewable_diesel_facilities |

### OK (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 26 | Seaboard Energy Oklahoma LLC | Seaboard Energy Oklahoma LLC | Guymon | operating | 38.0 | biofuel_facilities |

### OKLAHOMA (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 425 | Seaboard Energy Oklahoma LLC - Guym | Seaboard Energy Oklahoma LLC | Guymon | operating | 42.0 | biofuel_facilities |

### OR (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| us.or.rd.redrock | Redrock | Redrock | Lakeview | Operating | 16.1 | renewable_diesel_facilities |

### PA (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 18 | Hero BX - Erie | Hero BX | Erie | operating | 50.0 | biofuel_facilities |

### PENNSYLVANIA (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 426 | Lake Erie Biofuels LLC - Erie | Lake Erie Biofuels LLC | Erie | operating | 45.0 | biofuel_facilities |

### RI (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 53 | Newport Biodiesel Inc. | Newport Biodiesel Inc. | Newport | operating | 8.0 | biofuel_facilities |

### SC (2)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 25 | Green Energy Biofuel (GEB3) | Green Energy Biofuel (GEB3) | Warrenville | operating | 40.0 | biofuel_facilities |
| 59 | Southeast Biodiesel | Southeast Biodiesel | Charleston | operating | 5.0 | biofuel_facilities |

### TEXAS (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 306 | Eberle Biodiesel LLC - Liverpool | Eberle Biodiesel LLC | Liverpool | operating |  | biofuel_facilities |

### TX (4)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| us.tx.rd.emerald_one | Emerald One | Emerald One | Port Arthur | Operating | 110.0 | renewable_diesel_facilities |
| 304 | Infinium (Project Pathfinder) - Cor | Infinium (Project Pathfinder) | Corpus Christi, TX | announced | 0.4 | biofuel_facilities |
| 1 | RBF Port Neches LLC | RBF Port Neches LLC | Port Neches | operating | 144.0 | biofuel_facilities |
| 40 | Rio Valley Biofuels LLC | Rio Valley Biofuels LLC | El Paso | operating | 15.0 | biofuel_facilities |

### US (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 307 | CountryMark - Indiana | CountryMark | Indiana | operating | 22.0 | biofuel_facilities |

### VA (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 56 | Virginia Biodiesel Refinery LLC | Virginia Biodiesel Refinery LL | West Point | operating | 5.0 | biofuel_facilities |

### VIRGINIA (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 432 | Virginia Biodiesel Refinery LLC - K | Virginia Biodiesel Refinery LL | Kilmarnock | operating | 5.0 | biofuel_facilities |

### WA (3)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 309 | BP - Cherry Point, WA | BP | Cherry Point, WA | operating |  | biofuel_facilities |
| 308 | CastleRock Green Energy - Shelton,  | CastleRock Green Energy | Shelton, WA | operating | 10.0 | biofuel_facilities |
| 310 | Twelve / Air Plant One - Moses Lake | Twelve / Air Plant One | Moses Lake, WA | operating |  | biofuel_facilities |

### WASHINGTON (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 437 | Reg Grays Harbor - Hoquiam | Reg Grays Harbor | Hoquiam | operating | 109.0 | biofuel_facilities |

### WI (2)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 36 | REG Madison LLC | REG Madison LLC | DeForest | operating | 28.0 | biofuel_facilities |
| 58 | Walsh BioFuels LLC | Walsh BioFuels LLC | Mauston | operating | 5.0 | biofuel_facilities |

### WY (1)

| facility_id | name | operator | city | status | cap (mmgy) | source |
|---|---|---|---|---|---|---|
| 101 | Slate / Starwood - Douglas, WY | Slate / Starwood | Douglas | operating | 100.0 | biofuel_facilities |

## A. Matched — CARB facilities present in DB

(70 facilities matched, full list in JSON sibling)

| CARB Fuel Producer | CARB Facility | Location | DB matches |
|---|---|---|---|
|  | Solfuels USA LLC | Arkansas | `23`; `us.unk.bd.solfuels_usa_inc` |
|  | Canary Renewables Corp. Port o | California | `396`; `51` |
|  | SEABOARD ENERGY KANSAS, LLC | Kansas | `408` |
| ADM Agri-Industries Company | ADM Agri Industries | Canada | `404` |
| ARTESIA RENEWABLE DIESEL COMPANY LL |  | New Mexico | `420` |
| Adkins Energy LLC | Adkins Energy, LLC | Illinois | `403` |
| Ag Processing Inc | Ag Processing Inc - Sgt. Bluff | Iowa | `8`; `318` |
| Ag Processing Inc |  | Missouri | `416`; `8` |
| AltAir Paramount, LLC | AltAir Paramount, LLC | California | `66`; `us.ca.rd.world_energy_paramount` |
| American Biodiesel, Inc., dba Commu | Community Fuels Port of Stockt | Stockton, California | `51`; `65` |
| American Greenfuels, LLC | AMERICAN GREENFUELS LLC | Connecticut | `35`; `397` |
| Archer Daniels Midland Co | ADM Agri Industries | Canada | `us.unk.bd.archer_daniels_midland_co_lloydminster`; `5` |
| Archer Daniels Midland Co | ADM Velva | North Dakota | `423`; `5` |
| Archer Daniels Midland Co | ADM Mexico | Mexico, Missouri | `us.unk.bd.archer_daniels_midland_co_lloydminster`; `5` |
| Archer Daniels Midland Co | ADM Deerfield | Missouri | `17`; `us.unk.bd.archer_daniels_midland_co_lloydminster` |
| Archer Daniels Midland Co |  | Missouri | `us.unk.bd.archer_daniels_midland_co_lloydminster`; `5` |
| BIOX Canada Limited | BIOX Canada Limited | Ontario, Canada | `us.unk.rd.biox_forge_shell_ventures_valent_sombra_canada` |
| BP Products North America, Inc | Cherry Point Refinery | Washington | `436`; `124` |
| Biodico Westside | Biodico Plant | California | `us.unk.bd.biodico_westside` |
| Bioenergy Development Group LLC | Bioenergy Development Group, L | Tennessee | `428`; `us.unk.bd.bioenergy_development_group` |
| CVR RENEWABLES WYN, LLC | CVR RENEWABLES WYN, LLC | Oklahoma | `424`; `us.ok.rd.cvr_energy` |
| Canary Biofuels Inc. |  | Canada | `51` |
| Central Valley Renewable Fuels, LLC | Central Valley Renewable Fuels | California | `104`; `295` |
| Chevron Products Company | Chevron El Segundo | California | `126`; `46` |
| Cheyenne Renewable Diesel Company L |  | Wyoming | `70`; `438` |
| Crimson Renewable Enegy LP | Crimson Renewable Enegy Bakers | California | `29`; `48` |
| Crimson Renewable Energy LLC | Crimson Renewable Enegy Bakers | California | `29`; `48` |
| Crimson Renewable Energy LP | Crimson Renewable Energy Baker | Bakersfield, California | `29`; `48` |
| DAKOTA PRAIRIE REFINING |  | North Dakota | `422` |
| Delek Renewables, LLC | Delek Renewables Crossett Biod | Arkansas | `41`; `us.unk.bd.delek_renewables_crossett` |
| Delek Renewables, LLC | Delek Renewables Cleburne Biod | Texas | `us.unk.bd.delek_renewables_cleburne`; `47` |
| Delek Renewables, LLC | DELEK RENEWABLES NEW ALBANY BI | Mississippi | `45`; `us.unk.bd.delek_renewables_cleburne` |
| Delek Renewables, LLC | Delek Renewables Crossett Biod | Arkansas | `41`; `us.unk.bd.delek_renewables_crossett` |
| Diamond Green Diesel Holdings LLC | Diamond Green Diesel LLC | Louisiana | `410`; `us.unk.bd.diamond_green_diesel_llc` |
| Diamond Green Diesel Holdings LLC | DIAMOND GREEN DIESEL - PORT AR | Texas | `305`; `61` |
| East Kansas Agri-Energy, LLC | East Kansas Agri-Energy, LLC | Kansas | `us.unk.bd.east_kansas_agri_energy_llc`; `62` |
| Fuel Producer: REG Seneca, LLC | Fuel Producer | Illinois | `6`; `15` |
| FutureFuel Chemical Company | FutureFuel Chemical Company | Arkansas | `11`; `392` |
| Global Alternative Fuels, LLC | Global Alternative Fuels, LLC | Texas | `us.unk.bd.global_alternative_fuels_llc`; `341` |
| High Plains Bioenergy | HPB - St. Joe Biodiesel LLC | Missouri | `us.unk.bd.high_plains_bioenergy_llc`; `us.unk.bd.hpb_st_joe_biodiesel_llc` |
| High Plains Bioenergy | High Plains Bioenergy | Oklahoma | `us.unk.bd.high_plains_bioenergy_llc` |
| Imperial Western Products | Imperial Western Products | California | `46`; `us.unk.bd.imperial_western_products` |
| Kern Oil & Refining Co. | Kern Oil & Refining Co. | California | `104`; `us.unk.bd.kern_oil` |
| MARTINEZ RENEWABLES LLC | MARTINEZ REFINERY | California | `65`; `71` |
| MONTANA RENEWABLES, LLC |  | Montana | `417` |
| Neste Oyj | Neste Renewable Fuels - Porvoo | Finland | `267` |
| Neste Renewable Fuels Oy | Neste Renewable Fuels - Porvoo | Finland | `267` |
| Neste Singapore Pte Ltd | Neste Singapore | Singapore | `65`; `us.or.rd.bp_neste_next_renewables` |
| New Leaf Biofuel | New Leaf Biofuel | San Diego, California | `44`; `us.unk.bd.new_leaf_biofuel_llc` |
| PHILLIPS 66 COMPANY | Phillips 66 Rodeo | California | `311`; `us.ca.rd.p66` |
| REG Albert Lea, LLC | REG Albert Lea, LLC | Minnesota | `us.unk.bd.reg_albert_lea_llc`; `20` |
| REG Danville, LLC | REG Danville, LLC | Illinois | `15`; `6` |
| REG Geismar, LLC | REG Geismar, LLC | Louisiana | `19`; `27` |
| REG Grays Harbor, LLC | REG Grays Harbor, LLC | Hoquiam, Washinton | `2`; `us.wa.rd.northwest_advanced_biofuels` |
| REG Mason City, LLC | REG Mason City, LLC | Iowa | `us.unk.bd.reg_mason_city_llc`; `2` |
| REG New Boston, LLC | REG New Boston, LLC | Texas | `us.unk.bd.reg_new_boston`; `us.unk.bd.reg_new_orleans_llc` |
| REG Newton, LLC | REG Newton, LLC | Iowa | `2`; `us.unk.bd.reg_newton_llc` |
| REG Seneca, LLC | REG Seneca, LLC | Illinois | `6`; `15` |
| Rothsay, A Division of Darling Inte | Rothsay Biodiesel | Canada | `us.unk.bd.darling_ingredients_inc_canada` |
| ST BERNARD RENEWABLES LLC | St Bernard Renewables LLC | Louisiana | `313` |
| _...10 more matches..._ | | | |
