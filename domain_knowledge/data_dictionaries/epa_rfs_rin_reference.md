# EPA Renewable Fuel Standard (RFS) and RIN Data Reference

## Overview

The Renewable Fuel Standard (RFS) is a U.S. federal program requiring specific volumes of renewable fuel to be blended into transportation fuel annually. Created by the Energy Policy Act of 2005 and expanded by the Energy Independence and Security Act of 2007 (EISA), the program is administered by the EPA Office of Transportation and Air Quality (OTAQ).

### Program Objectives
- Reduce greenhouse gas emissions from transportation fuels
- Expand the nation's renewable fuels sector
- Reduce reliance on imported petroleum
- Support rural economies through domestic feedstock production

### Key Dates
- **2005**: RFS1 established by Energy Policy Act (P.L. 109-58)
- **2007**: RFS2 expansion via EISA (P.L. 110-140)
- **July 1, 2010**: EMTS launched; RFS2 data collection begins
- **2022**: End of statutory volume tables
- **2023+**: EPA "Set Authority" for determining annual volumes

---

## D-Code Categories

D-codes identify the renewable fuel category based on feedstock, production process, and lifecycle greenhouse gas (GHG) reduction. Each fuel type is assigned a D-code per 40 CFR 80.1426.

### D3 - Cellulosic Biofuel
| Attribute | Value |
|-----------|-------|
| GHG Reduction Requirement | ≥60% vs. 2005 petroleum baseline |
| Feedstock Source | Cellulose, hemicellulose, or lignin |
| Common Fuel Types | Cellulosic ethanol, renewable CNG/LNG from cellulosic sources, biogas from landfills, agricultural digesters |
| Can Satisfy | Cellulosic Biofuel RVO, Advanced Biofuel RVO, Total Renewable Fuel RVO |

### D4 - Biomass-Based Diesel (BBD)
| Attribute | Value |
|-----------|-------|
| GHG Reduction Requirement | ≥50% vs. 2005 diesel baseline |
| Feedstock Source | Non-cellulosic renewable biomass |
| Common Fuel Types | Biodiesel (FAME), renewable diesel (non-cellulosic), renewable jet fuel |
| Common Feedstocks | Soybean oil, canola oil, corn oil, used cooking oil (UCO), tallow, yellow grease, white grease, poultry fat |
| Can Satisfy | Biomass-Based Diesel RVO, Advanced Biofuel RVO, Total Renewable Fuel RVO |

### D5 - Advanced Biofuel
| Attribute | Value |
|-----------|-------|
| GHG Reduction Requirement | ≥50% vs. 2005 petroleum baseline |
| Feedstock Source | Any renewable biomass EXCEPT corn starch |
| Common Fuel Types | Sugarcane ethanol, non-corn starch ethanol (wheat, barley, sorghum), renewable naphtha, renewable LPG, RNG not qualifying for D3, renewable diesel via co-processing |
| Can Satisfy | Advanced Biofuel RVO, Total Renewable Fuel RVO |

### D6 - Renewable Fuel (Conventional)
| Attribute | Value |
|-----------|-------|
| GHG Reduction Requirement | ≥20% for facilities constructed after Dec 19, 2007; grandfathered facilities exempt |
| Feedstock Source | Any qualifying renewable biomass |
| Common Fuel Types | Corn starch ethanol (primary), grain sorghum ethanol |
| Can Satisfy | Total Renewable Fuel RVO only |

### D7 - Cellulosic Diesel
| Attribute | Value |
|-----------|-------|
| GHG Reduction Requirement | ≥60% vs. 2005 petroleum baseline |
| Feedstock Source | Cellulose, hemicellulose, or lignin |
| Common Fuel Types | Renewable diesel from cellulosic biomass, cellulosic jet fuel |
| Special Status | Qualifies as BOTH cellulosic biofuel AND biomass-based diesel |
| Can Satisfy | Cellulosic Biofuel RVO OR Biomass-Based Diesel RVO (obligated party chooses), Advanced Biofuel RVO, Total Renewable Fuel RVO |

### Nesting Structure

The RFS uses a nested compliance structure where "inner" fuel categories can satisfy "outer" requirements:

```
┌─────────────────────────────────────────────────────────────┐
│                  Total Renewable Fuel (D6)                   │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              Advanced Biofuel (D5)                     │  │
│  │  ┌─────────────────────┐  ┌─────────────────────────┐ │  │
│  │  │ Cellulosic Biofuel  │  │  Biomass-Based Diesel   │ │  │
│  │  │      (D3)           │  │        (D4)             │ │  │
│  │  │                     │  │                         │ │  │
│  │  │    ┌───────────┐    │  │                         │ │  │
│  │  │    │    D7     │────┼──┼─────────────────────────┤ │  │
│  │  │    │(Cellulosic│    │  │  (D7 can satisfy either │ │  │
│  │  │    │  Diesel)  │    │  │   D3 or D4, not both)   │ │  │
│  │  │    └───────────┘    │  │                         │ │  │
│  │  └─────────────────────┘  └─────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**Nesting Rules:**
- D3 RINs → can satisfy: Cellulosic, Advanced, Total Renewable Fuel RVOs
- D4 RINs → can satisfy: BBD, Advanced, Total Renewable Fuel RVOs
- D5 RINs → can satisfy: Advanced, Total Renewable Fuel RVOs
- D6 RINs → can satisfy: Total Renewable Fuel RVO only
- D7 RINs → can satisfy: Cellulosic OR BBD (not both), Advanced, Total Renewable Fuel RVOs

---

## Equivalence Values (EV)

Equivalence values determine how many RINs are generated per physical gallon of renewable fuel. EVs are based on energy content relative to ethanol (baseline = 1.0). Defined in 40 CFR 80.1415.

| Fuel Type | Equivalence Value | Notes |
|-----------|-------------------|-------|
| Denatured Ethanol | 1.0 | Baseline reference |
| Biodiesel (FAME) | 1.5 | Fatty acid methyl esters |
| Butanol | 1.3 | Four-carbon alcohol |
| Renewable Diesel (non-ester) | 1.7 | Requires LHV ≥123,500 Btu/gal |
| Renewable CNG/LNG/RNG | 1.0 | Per 77,000 Btu LHV |
| Renewable Electricity | 1.0 | Per 22.6 kWh (pathway being removed in 2026) |
| Sustainable Aviation Fuel | 1.6 | Varies by pathway |
| Other fuels | Varies | Must apply to EPA for EV determination |

**Example Calculation:**
- 1,000 physical gallons of renewable diesel (EV 1.7) = 1,700 RINs
- 1,000 physical gallons of biodiesel (EV 1.5) = 1,500 RINs
- 1,000 physical gallons of ethanol (EV 1.0) = 1,000 RINs

---

## RIN Structure and Lifecycle

### RIN Format (38-character code)

```
K YYYY CCCC FFFFF BBBBB RR D SSSSSSSS EEEEEEEE
│ │    │    │     │     │  │ │        └── End RIN number (8 digits)
│ │    │    │     │     │  │ └── Start RIN number (8 digits)
│ │    │    │     │     │  └── D-code (1 digit)
│ │    │    │     │     └── Equivalence Value (2 digits, e.g., 10, 15, 17)
│ │    │    │     └── Batch number (5 digits)
│ │    │    └── Facility ID (5 digits)
│ │    └── Company ID (4 digits)
│ └── Year of production (4 digits)
└── Assignment code: 1=Assigned, 2=Separated
```

### RIN Lifecycle

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ GENERATION  │───►│ ASSIGNMENT  │───►│ SEPARATION  │───►│   TRADING   │
│             │    │             │    │             │    │             │
│ Producer/   │    │ RIN attached│    │ RIN detached│    │ Separated   │
│ importer    │    │ to fuel     │    │ from fuel   │    │ RINs traded │
│ creates RIN │    │ batch       │    │ batch       │    │ between     │
│ in EMTS     │    │ (K=1)       │    │ (K=2)       │    │ parties     │
└─────────────┘    └─────────────┘    └─────────────┘    └──────┬──────┘
                                                                │
                   ┌─────────────┐    ┌─────────────┐           │
                   │ EXPIRATION  │◄───│ RETIREMENT  │◄──────────┘
                   │             │    │             │
                   │ Unused RINs │    │ Obligated   │
                   │ expire after│    │ parties     │
                   │ compliance  │    │ retire RINs │
                   │ year + 1    │    │ for RVO     │
                   └─────────────┘    └─────────────┘
```

### Transaction Types in EMTS

| Transaction Type | Description | Reporting Deadline |
|-----------------|-------------|-------------------|
| Generation | RIN created when renewable fuel produced/imported | 5 business days |
| Buy/Sell | RIN traded between registered parties | 5 business days |
| Separation | RIN detached from physical fuel batch | 5 business days |
| Retirement - Compliance | RINs submitted for annual RVO compliance | March 31 following compliance year |
| Retirement - Export | RINs retired for exported renewable fuel | 30 days after export |
| Retirement - Invalid | RINs retired due to errors/fraud | As discovered |

### RIN Separation Conditions (40 CFR 80.1429)

RINs may only be separated when:
- Renewable fuel is blended into gasoline or diesel for use as transportation fuel
- Renewable fuel is used in its neat (unblended) form as transportation fuel
- Renewable fuel is exported
- Renewable fuel is designated for heating oil use (if applicable)
- Other specific conditions per regulation

### RIN Validity Period

- RINs are valid for compliance in:
  - The year generated, AND
  - The following compliance year only
- Example: 2024 RINs valid for 2024 and 2025 compliance
- Prior year RINs may satisfy up to 20% of current year RVO
- Unused RINs expire and cannot be used for compliance

---

## Renewable Volume Obligations (RVO)

### RVO Calculation

Obligated parties calculate their RVO for each fuel category:

```
RVO = (Gasoline Volume + Diesel Volume) × EPA Percentage Standard + Deficit Carryover
```

Where:
- Gasoline Volume = non-renewable gasoline produced or imported
- Diesel Volume = non-renewable diesel produced or imported
- EPA Percentage Standard = annual standard published by EPA for each category
- Deficit Carryover = any deficit from prior year (must be satisfied)

### Compliance Deadlines

| Action | Deadline |
|--------|----------|
| Compliance period | Calendar year (Jan 1 - Dec 31) |
| Annual compliance demonstration | March 31 following compliance year |
| Deficit carryover satisfaction | Must satisfy in following year along with new RVO |
| Q4 attest engagement submission | June 1 following compliance year |

### 2023-2025 Volume Requirements (Set 1 Rule - Final)

| Category | 2023 | 2024 | 2025 |
|----------|------|------|------|
| Cellulosic Biofuel (billion RINs) | 0.72 | 1.09 | 1.19* |
| Biomass-Based Diesel (billion gal) | 2.82 | 2.89 | 2.95 |
| Advanced Biofuel (billion RINs) | 5.94 | 6.54 | 7.33 |
| Total Renewable Fuel (billion RINs) | 20.94 | 21.54 | 22.33 |

*2025 cellulosic partially waived to 1.19 billion due to production shortfall

### 2026-2027 Volume Requirements (Proposed Set 2 Rule)

| Category | 2026 (Proposed) | 2027 (Proposed) |
|----------|-----------------|-----------------|
| Cellulosic Biofuel (billion RINs) | 1.30 | 1.36 |
| Biomass-Based Diesel (billion RINs) | 7.12 | 7.50 |
| Advanced Biofuel (billion RINs) | 9.02 | 9.46 |
| Total Renewable Fuel (billion RINs) | 24.52 | 25.46 |

Note: Proposed rule changes BBD from physical gallons to RINs for consistency.

---

## EMTS Party Categories

### Obligated Parties (Must retire RINs for compliance)

| Category | Description |
|----------|-------------|
| Refiner | Produces gasoline or diesel fuel domestically |
| Importer | Imports gasoline or diesel fuel into the U.S. |

### Non-Obligated Parties

| Category | Description |
|----------|-------------|
| RIN Originator | Domestic renewable fuel producer or renewable fuel importer that generates RINs |
| RIN Owner | Owns or transacts RINs but does not generate or retire for compliance |
| Exporter | Exports renewable fuel; must retire RINs within 30 days of export |
| Foreign Producer | Foreign renewable fuel producer registered per 40 CFR 80.1466 |

### Market Statistics (2022)
- ~2,400 registered parties
- ~800 active participants
- ~400 obligated parties
- ~300 renewable fuel producers
- ~100 RIN owners
- 194,870 transactions
- 25+ billion RINs traded
- ~$38 billion transaction value

---

## Data Sources and Access

### EPA EMTS Public Data Portal

**Primary Access Point:**
https://www.epa.gov/fuels-registration-reporting-and-compliance-help/public-data-renewable-fuel-standard

**Platform:** Qlik Sense® interactive dashboards

**Data Coverage:** July 1, 2010 to present (RFS2 only)

**Update Frequency:** Monthly (typically 30-60 days lag)

### Available Data Reports

| Report Category | Data Included | Update Frequency |
|-----------------|---------------|------------------|
| RIN Generation | Monthly RINs generated by D-code, producer type | Monthly |
| Renewable Fuel Volume | Physical gallons produced by fuel category | Monthly |
| RIN Generation by Fuel Type | Detailed breakdown by fuel pathway | Monthly |
| Feedstock Summary | Feedstock volumes and RINs by feedstock type | Monthly |
| RIN Trades & Prices | Weekly volume-weighted average prices by D-code | Weekly (reported monthly) |
| Transaction Volumes | Weekly aggregated RIN transactions | Weekly (reported monthly) |
| Available RINs | Holdings by vintage, D-code, party type | Quarterly |
| RIN Separation | Monthly separation transactions by D-code | Monthly |
| RIN Retirement | Compliance retirements by party type | Annual (after March 31) |
| Annual Compliance | RVO compliance, deficits, fuel volumes | Annual |
| Cellulosic Waiver Credits | CWC purchases by year | Annual |

### CSV Download Links

**RIN Generation Data:**
```
https://www.epa.gov/fuels-registration-reporting-and-compliance-help/spreadsheet-rin-generation-data-renewable-fuel
```
File pattern: `generationbreakout_[mon][year].csv`

**RIN Generation + Volume by Month:**
```
https://www.epa.gov/fuels-registration-reporting-and-compliance-help/spreadsheet-rin-generation-and-renewable-fuel
```

**RIN Generation + Volume by Fuel Type:**
```
https://www.epa.gov/fuels-registration-reporting-and-compliance-help/spreadsheet-rin-generation-and-renewable-fuel-0
```

**Available RINs to Date:**
```
https://www.epa.gov/fuels-registration-reporting-and-compliance-help/spreadsheet-available-rins-date-renewable-fuel
```

**RIN Retirement Data:**
```
https://www.epa.gov/fuels-registration-reporting-and-compliance-help/spreadsheet-rin-retirement-data-renewable-fuel
```

**RIN Separation Data:**
```
https://www.epa.gov/fuels-registration-reporting-and-compliance-help/spreadsheet-rin-separation-data-renewable-fuel
```

### Sample CSV URLs (Recent Months)

```
# November 2025
https://www.epa.gov/system/files/other-files/2025-12/generationbreakout_nov2025.csv

# October 2025
https://www.epa.gov/system/files/other-files/2025-11/generationbreakout_oct2025.csv

# September 2025
https://www.epa.gov/system/files/other-files/2025-10/generationbreakout_sep2025.csv
```

### Data Access Limitations

| Limitation | Details |
|------------|---------|
| No REST API | EPA does not provide programmatic API access to EMTS data |
| Manual Download | CSV files must be downloaded manually or via web scraping |
| Qlik Platform | Interactive reports require browser access |
| Data Lag | Monthly data typically 30-60 days after month end |
| Revision Risk | Data subject to change due to remedial actions, error corrections |

---

## RIN Price Data

### Weekly Price Reports

EPA publishes weekly volume-weighted average prices for separated RINs by D-code.

**Data Includes:**
- Separated RINs only (K=2)
- By D-code (D3, D4, D5, D6, D7)
- By QAP status (Q-RINs vs non-Q-RINs)
- Volume-weighted average $/RIN

### Price Filters Applied by EPA

To remove outlier transactions, EPA applies price filters:

| D-Code | Pre-2020 Range | Post-2019 Range |
|--------|----------------|-----------------|
| D3 | $0.05 - $3.50 | $0.05 - $3.50 |
| D4 | $0.05 - $2.00 | $0.05 - $3.00 |
| D5 | $0.05 - $2.00 | $0.05 - $3.00 |
| D6 | $0.01 - $2.00 | $0.01 - $2.00 |
| D7 | Same as D3 | Same as D3 |

### Price Relationships

```
D3 Price Ceiling = CWC Price + D4 or D5 Price
D3 Price Floor ≈ D4 Price (theoretical)

CWC Price = MAX($0.25, $3.00 - 12-month avg wholesale gasoline price)
```

### Quality Assurance Plan (QAP)

- Voluntary third-party audit/verification program
- Q-RINs: RINs verified by approved QAP provider
- Non-Q-RINs: Standard RINs without QAP verification
- Separate price tracking for Q-RINs vs non-Q-RINs
- QAP provides additional assurance against invalid RINs

---

## Cellulosic Waiver Credits (CWC)

### Purpose
Alternative compliance mechanism when cellulosic biofuel is unavailable in sufficient quantities.

### How CWCs Work
- CWC + D4 or D5 RIN = satisfies cellulosic RVO
- EPA makes CWCs available when cellulosic volumes are waived
- Obligated parties purchase CWCs from EPA

### CWC Pricing Formula
```
CWC Price = MAX($0.25, $3.00 - P)
```
Where P = 12-month average wholesale gasoline price

### Market Impact
- CWC price sets ceiling for D3 RIN prices
- When cellulosic supply is short, obligated parties buy CWCs + D4/D5
- As cellulosic supply increased (especially RNG), CWC usage has declined

---

## Feedstock Tracking

### Major Feedstock Categories

**Ethanol Feedstocks:**
- Corn starch (D6)
- Sugarcane (D5)
- Grain sorghum/milo (D5)
- Wheat (D5)
- Cellulosic biomass (D3)

**Biomass-Based Diesel Feedstocks:**
- Soybean oil
- Canola/rapeseed oil
- Corn oil (distillers)
- Palm oil (limited, requires specific certification)
- Used cooking oil (UCO)
- Yellow grease
- White grease
- Tallow (beef)
- Poultry fat
- Fish oil

**Cellulosic Feedstocks (D3/D7):**
- Crop residues (corn stover, wheat straw)
- Wood/forestry residues
- Municipal solid waste (cellulosic fraction)
- Landfill gas
- Agricultural digesters
- Wastewater treatment biogas

### Feedstock Reporting in EMTS

Renewable fuel producers report:
- Feedstock type codes used
- Feedstock volumes (by type or combination)
- RINs generated per feedstock
- Enables feedstock-to-RIN generation analysis

### Mixed Feedstock Rules

For fuels from mixed feedstocks:
- EPA evaluates lifecycle GHG separately for each component
- Feedstocks with ≥75% adjusted cellulosic content can claim 100% cellulosic RINs
- Feedstocks with <75% adjusted cellulosic content receive proportional RINs

---

## Approved Fuel Pathways

### Pathway Requirements

Each pathway specifies:
- Feedstock type
- Production process
- D-code assigned
- GHG reduction threshold
- Equivalence value
- Any special conditions

### Pathway Reference

**Full List:**
https://www.epa.gov/renewable-fuel-standard/approved-pathways-renewable-fuel

**EMTS Reporting Codes:**
https://www.epa.gov/fuels-registration-reporting-and-compliance-help/reporting-codes-and-fuel-pathways-epa-moderated

### Common Pathway Examples

| Fuel Type | Feedstock | Process | D-Code | EV |
|-----------|-----------|---------|--------|-----|
| Ethanol | Corn starch | Dry mill | D6 | 1.0 |
| Ethanol | Sugarcane | Fermentation | D5 | 1.0 |
| Ethanol | Corn stover | Cellulosic conversion | D3 | 1.0 |
| Biodiesel | Soybean oil | Transesterification | D4 | 1.5 |
| Renewable diesel | Soybean oil | Hydrotreating | D4 | 1.7 |
| Renewable diesel | Used cooking oil | Hydrotreating | D4 | 1.7 |
| RNG | Landfill gas | Upgrading | D3 | 1.0 |
| RNG | Agricultural digester | Anaerobic digestion | D3 | 1.0 |
| SAF | Soybean oil | HEFA | D4 | 1.6 |

---

## Compliance Framework

### Small Refinery Exemptions (SRE)

- Small refineries (≤75,000 bbl/day capacity) may petition for RVO exemption
- Must demonstrate disproportionate economic hardship
- EPA reviews and grants/denies petitions annually
- SRE volumes impact total RVO pool and can affect RIN markets
- August 2025: EPA issued decisions on 175 SRE petitions for 2016-2024

### Deficit Carryover Rules

- Obligated parties may carry a deficit into the following year
- Must satisfy prior year deficit + current year RVO in that following year
- Cannot carry deficits for more than one year
- Must maintain same aggregation approach if carrying deficit

### Compliance Aggregation Options

**Refiners:**
- Aggregate compliance (all refineries combined)
- Refinery-by-refinery compliance

**Importers:**
- Must comply in aggregate

**Exporters:**
- Must comply in aggregate

### Attest Engagements

- Annual third-party audit of RFS compliance
- Required for obligated parties, exporters, renewable fuel producers
- Due June 1 following compliance year
- Verifies accuracy of reported data and RIN transactions

---

## Regulatory References

### Primary Regulations

| Citation | Description |
|----------|-------------|
| 40 CFR Part 80 Subpart M | Renewable Fuel Standard regulations |
| 40 CFR 80.1401 | Definitions |
| 40 CFR 80.1405 | EPA determination of standards |
| 40 CFR 80.1406 | RVO calculations |
| 40 CFR 80.1415 | Equivalence values |
| 40 CFR 80.1426 | D-code definitions and pathway requirements |
| 40 CFR 80.1427 | Compliance demonstration |
| 40 CFR 80.1429 | RIN separation conditions |
| 40 CFR 80.1431 | Invalid RIN provisions |
| 40 CFR 80.1451 | Reporting requirements |
| 40 CFR 80.1466 | Foreign renewable fuel producer requirements |

### Statutory Authority

| Law | Citation | Description |
|-----|----------|-------------|
| Energy Policy Act of 2005 | P.L. 109-58 | Established RFS1 |
| Energy Independence and Security Act of 2007 | P.L. 110-140 | Expanded to RFS2, set volume tables through 2022 |
| Clean Air Act Section 211(o) | 42 U.S.C. 7545(o) | Codified RFS requirements |

### Key Federal Register Rules

| Rule | Citation | Description |
|------|----------|-------------|
| RFS2 Final Rule | 75 FR 14670 (Mar. 26, 2010) | Implemented EISA 2007 requirements |
| Set 1 Rule | 88 FR 44468 (Jul. 12, 2023) | 2023-2025 volume requirements |
| Set 2 Proposed | 90 FR 25786 (Jun. 17, 2025) | 2026-2027 proposed requirements |

---

## RIN Balance Sheet Tracking

### Balance Calculation

```
Available RINs = RINs Generated 
                - RINs Retired (Compliance)
                - RINs Retired (Export)
                - RINs Retired (Invalid)
                - RINs Expired
```

### Key Metrics to Track

| Metric | Description | Data Source |
|--------|-------------|-------------|
| Net Generation | Gross generation minus invalid RINs | RIN Generation reports |
| Available RINs by Vintage | RINs available by year generated | Available RINs report |
| Retirement Rate | RINs retired vs. generated | Retirement reports |
| Carry-in RINs | Prior year RINs available for current year | Available RINs report |
| Bank | Excess RINs beyond RVO requirements | Calculated |

### Vintage Year Tracking

- RINs tracked by "vintage" year (year generated)
- Current year RINs: unlimited use for current year RVO
- Prior year RINs: limited to 20% of current year RVO
- Two-year validity window

---

## Integration with EIA Data

### Complementary Data Points

| EPA RFS Data | EIA Data | Use Case |
|--------------|----------|----------|
| RIN generation by D-code | Production volumes | Validate physical vs. credit volumes |
| BBD RINs | Biodiesel/RD production (EIA-22M) | Cross-reference production |
| D6 RINs | Fuel ethanol production | Cross-reference ethanol output |
| Feedstock data | Feedstock prices | Cost analysis |

### Cross-Reference Series

```
EIA Series for Biofuel Production:
- STEO.ETHPROD.M (Fuel Ethanol Production)
- PET.M_EPOORDB_YPR_NUS_MBBLD.M (Biodiesel Production)
- PET.M_EPOORDS_YPR_NUS_MBBLD.M (Renewable Diesel Production)

Compare with:
- EPA D6 RIN generation (ethanol)
- EPA D4 RIN generation (biodiesel/renewable diesel)
```

---

## Data Collection Recommendations

### Automated Collection Strategy

Since EPA does not provide a REST API, consider:

1. **Web Scraping** (with caution):
   - Monitor CSV download pages for new files
   - Follow file naming patterns
   - Respect rate limits and terms of service

2. **Manual Download Schedule**:
   - Monthly: RIN generation, separation, volume data
   - Weekly: Check for price updates
   - Quarterly: Available RINs snapshots
   - Annually: Compliance and retirement data

3. **File Storage Pattern**:
   ```
   /data/epa_rfs/
   ├── generation/
   │   └── generationbreakout_[MONYEAR].csv
   ├── separation/
   │   └── separation_[MONYEAR].csv
   ├── retirement/
   │   └── retirement_[YEAR].csv
   ├── available/
   │   └── available_rins_[DATE].csv
   └── prices/
       └── rin_prices_[DATE].csv
   ```

### Key Data Fields for Analysis

**RIN Generation CSV Structure (typical):**
- Year
- Month
- D-Code
- Producer Type (Domestic/Foreign/Importer)
- Fuel Type
- RINs Generated
- Gallons Produced
- Equivalence Value

**Available RINs Structure:**
- RIN Year (Vintage)
- D-Code
- Party Category
- Balance Quarter
- Total RINs Held

---

## Document Metadata

| Field | Value |
|-------|-------|
| Version | 1.0 |
| Created | January 28, 2026 |
| Author | RLC Consulting |
| Data Sources | EPA, EIA, CFR, CRS Reports |
| Update Frequency | As EPA rules change |

---

## Appendix: Quick Reference Tables

### D-Code Summary

| D-Code | Category | GHG Threshold | Primary Fuel | EV Range |
|--------|----------|---------------|--------------|----------|
| D3 | Cellulosic Biofuel | ≥60% | RNG, Cellulosic Ethanol | 1.0 |
| D4 | Biomass-Based Diesel | ≥50% | Biodiesel, Renewable Diesel | 1.5-1.7 |
| D5 | Advanced Biofuel | ≥50% | Sugarcane Ethanol, Other | 1.0+ |
| D6 | Renewable Fuel | ≥20%* | Corn Ethanol | 1.0 |
| D7 | Cellulosic Diesel | ≥60% | Cellulosic RD/SAF | 1.6-1.7 |

*Post-2007 facilities; grandfathered facilities exempt

### Key URLs

| Resource | URL |
|----------|-----|
| RFS Program Home | https://www.epa.gov/renewable-fuel-standard |
| Public Data Portal | https://www.epa.gov/fuels-registration-reporting-and-compliance-help/public-data-renewable-fuel-standard |
| Approved Pathways | https://www.epa.gov/renewable-fuel-standard/approved-pathways-renewable-fuel |
| EMTS Documentation | https://www.epa.gov/fuels-registration-reporting-and-compliance-help/emts-system-documentation |
| eCFR Part 80 | https://www.ecfr.gov/current/title-40/chapter-I/subchapter-C/part-80/subpart-M |
