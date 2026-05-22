# data/raw — ingest plan (v2)

*Last updated 2026-05-22 09:50 UTC. Companion to `data_raw_ingest_inventory.md`.*

**Scope:** 685+ files (Tore is still adding). 34 FAO domain ZIPs now on disk.

This document priorities the entities for ingest. Tore's stated preference: ingest everything, use everything if it helps. This plan defers nothing permanently; "skip" tiers are "later, not never."

---

## A. FAO domains on disk — 34 zips

### A.1 P0 Core — directly feeds the analytical sheets

| Domain | File | Size | Use |
|---|---|---|---|
| Population (OA) | `Population_E_All_Data.zip` | 1.4 MB | Per-capita normalizers |
| Production Crops + Livestock (QCL) | `Production_Crops_Livestock_E_All_Data.zip` | 24 MB | **gCAU/pCAU livestock counts**; non-US production cross-check |
| Food Balance Sheets (FBS) | `FoodBalanceSheets_E_All_Data.zip` | ? | Country S&D — production / imports / exports / feed / food / waste |
| Commodity Balances non-food (CB 2010+) | `CommodityBalances_(non-food)_(2010-)_E_All_Data.zip` | 0.3 MB | Industrial use breakouts |
| Commodity Balances non-food OLD method (pre-2013) | `CommodityBalances_(non-food)_(-2013_old_methodology)_E_All_Data_(Normalized).zip` | ? | History extension for CB |

### A.2 P1 Strategic — high analytical value, ingest after P0

These are not "ag econ" per traditional definition but are load-bearing for any serious global model.

| Domain | File | Why it matters |
|---|---|---|
| Exchange Rate | `Exchange_rate_E_All_Data.zip` | FX-adjusted trade competitiveness; price normalization to USD |
| Producer Prices | `Prices_E_All_Data.zip` + `PricesArchive_*.zip` | Origin-country producer prices — input to bilateral arbitrage |
| Consumer Price Indices | `ConsumerPriceIndices_E_All_Data.zip` | Real vs nominal price comparisons, inflation context |
| Deflators | `Deflators_E_All_Data.zip` | Same — inflation deflators by country |
| Macro Statistics Key Indicators | `Macro-Statistics_Key_Indicators_E_All_Data.zip` | GDP, GDP per capita — demand pressure |
| Value of Production | `Value_of_Production_E_All_Data.zip` | Country agricultural value of production by commodity |

### A.3 P2 Useful — analytical layer two

| Domain | File | Use |
|---|---|---|
| Fertilizers Nutrient | `Inputs_FertilizersNutrient_E_All_Data.zip` | Fertilizer use → yield correlation |
| Fertilizers Product | `Inputs_FertilizersProduct_E_All_Data.zip` | Same, by product |
| Fertilizers Archive | `Inputs_FertilizersArchive_E_All_Data.zip` | History extension |
| Land Use | `Inputs_LandUse_E_All_Data.zip` | Arable land changes, agricultural land area |
| Fertilizers Trade Matrix | `Fertilizers_DetailedTradeMatrix_E_All_Data.zip` | Fertilizer bilateral trade — input cost driver |
| Trade Crops + Livestock (TCL) | `Trade_CropsLivestock_E_All_Data.zip` | Backup/cross-check vs country-Census trade |
| Trade Detailed Matrix (TM) | `Trade_DetailedTradeMatrix_E_All_Data.zip` | Same — bilateral cross-check |

### A.4 P3 Skip unless asked

Per Tore's honest assessment: not directly needed for ag market modeling. Stored on disk; ingest deferred until a specific use case surfaces.

| Domain | File | Why skip for now |
|---|---|---|
| Capital Stock | `Investment_CapitalStock_E_All_Data.zip` | Tore: "probably don't need" — confirmed |
| Pesticides Trade | `Inputs_Pesticides_Trade_E_All_Data.zip` | Tore: "probably don't need" — confirmed |
| Pesticides Use | `Inputs_Pesticides_Use_E_All_Data.zip` | Same |
| Livestock Manure | `Environment_LivestockManure_E_All_Data.zip` | Tore: "probably don't need" — environmental angle only |
| Machinery (current + archive) | `Investment_Machinery*.zip` | Capital-flow signal, lower analytical value |
| Credit Agriculture | `Investment_CreditAgriculture_E_All_Data.zip` | Planting-decision input, indirect |
| Foreign Direct Investment | `Investment_ForeignDirectInvestment_E_All_Data.zip` | Capital-flow only |
| Government Expenditure | `Investment_GovernmentExpenditure_E_All_Data.zip` | Indirect demand signal |
| Land Cover (Environment) | `Environment_LandCover_E_All_Data.zip` | Satellite-derived, lower-frequency |
| Development Assistance | `Development_Assistance_to_Agriculture_E_All_Data.zip` | Aid flows, not market signals |
| Value shares industry primary factors | `Value_shares_industry_primary_factors_E_All_Data.zip` | Labor/capital shares (input-output) |
| SDG Bulk Downloads | `SDG_BulkDownloads_E_All_Data.zip` | Sustainability indicators, mostly political |
| World Census of Agriculture | `World_Census_Agriculture_E_All_Data.zip` | Decennial structural — not a market series |

---

## B. The "what is this for" framework

When the deep-dive conversation happens (deferred per Tore's morning preference), this is the lens I'd use to assess any non-traditional series:

**Layer 0 — Traditional ag econ** (already covered):
Production / acreage / yield / S&D / crush margins / futures + cash prices / export pace.

**Layer 1 — Adjacent and obvious value**:
- **Exchange rates** — every bilateral trade decision goes through FX. Brazil exports more soy when BRL weakens vs USD.
- **Producer prices in origin countries** — input to "what's competitive vs USA" math.
- **Macro indicators (GDP)** — demand pressure proxy for soybean meal in China, rice in Indonesia, etc.
- **Consumer price indices** — for real-price work and income elasticity.

**Layer 2 — Worth tracking if you have the bandwidth**:
- **Fertilizer use → yield correlation** — yield models need this input.
- **Land use change** — acreage frontier expansion (Brazil cerrado, Argentina Chaco).
- **Credit availability** — planting decision input where credit is tight.
- **Investment flows** — capacity build forward signal for crush, ethanol, BBD.

**Layer 3 — Probably skip unless a specific question surfaces**:
- Pesticides, capital stock, manure, FDI, government expenditure, SDG indicators, World Census of Ag.

The pattern: each layer further from the price signal is more interesting *analytically* but less reliable as a *forecast input*. Layer 1 and 2 should be in our system. Layer 3 is "have it on disk in case."

---

## C. Other on-disk entities (unchanged from v1)

[See v1 sections — Tier 1 ERS Feed Grains Yearbook, Tier 1 ERS Oil Crops 24yr, Tier 2 NASS Cattle on Feed, Tier 2 FGIS pre-2014, Tier 3 reference snapshots, etc. — all unchanged.]

---

## D. Newly-confirmed CONAB downloads (2026-05-22)

Tore was copy-pasting from CONAB's portal. Direct downloads exist at `https://portaldeinformacoes.conab.gov.br/downloads/arquivos/`:

| File | Size | Schema |
|---|---:|---|
| `Frete.txt` | 2.4 MB | Inland freight rates: origin/destination/year/month/distance_km/R$ per ton + R$ per ton-km |
| `PrecoMinimo.txt` | 51 KB | Minimum prices by product/state/region/start-date |
| `CustoProducao.txt` | 44 KB | Production cost by enterprise/product/state/municipality/year-month |
| `OfertaDemanda.txt` | 3.5 KB | Supply/demand by product/safra (S&D table) |
| `Estoques.txt` | ~1 KB gz | Stocks |

**Action:** rebuild the existing CONAB collector to hit these endpoints directly. Eliminates Tore's manual copy-paste workflow for all 5 series.

---

## E. MPOB ingest path (2026-05-22 confirmed)

Tore worried MPOB pastes data as images. **Confirmed not the case for the April 2026 docx files** — both `mpob_april.docx` (45×5 detail table) and `mpob_summary_april.docx` (24×14 annual trend) contain proper Word tables. Production, stocks, exports, by region (P. Malaysia / Sabah / Sarawak), are extractable directly via `python-docx`.

**Action:** write a simple MPOB docx ingest that reads `mpob_*.docx` files in `data/raw/oilseeds_fats_greases/`, parses both tables, writes to `bronze.mpob_monthly`. Estimated 30 minutes.

---

## F. Recommended next ingest sequence (post-Helios)

1. **FAO Population (OA)** — 1 hr — per-capita normalizers
2. **FAO QCL livestock filter** — 2 hr — gCAU/pCAU inputs
3. **USDA ERS Feed Grains Yearbook (Table 30)** — 2 hr — US-side gCAU/pCAU authoritative
4. **CONAB direct-download rebuild** — 2 hr — kill the copy-paste workflow
5. **MPOB docx ingest** — 30 min — kill another copy-paste workflow
6. **FAO Exchange Rate + Macro Stats** — 2 hr — unlocks FX-adjusted trade analytics
7. **FAO Food Balance Sheets** — 2 hr — country S&D framework
8. **USDA ERS Oil Crops Yearbook 24yr backfill** — 4 hr

The first 5 (= ~7.5 hours of focused work) eliminate two manual workflows AND deliver the gCAU/pCAU substrate. That's the highest leverage hour-for-hour.

---

## Open architectural question (deferred)

Tore noted: outside the traditional balance-sheet model, he doesn't yet have a sense of how to structure new schema. Worth a dedicated design session once the P0 + P1 FAO ingests are done, because the database shape will inform what's possible analytically. **Not this morning.**
