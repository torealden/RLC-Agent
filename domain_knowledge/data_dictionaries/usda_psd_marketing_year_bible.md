# USDA PSD Marketing Year Reference Guide

## Agricultural data alignment requires precise understanding of marketing year conventions

The USDA Foreign Agricultural Service Production, Supply, and Distribution (PSD) database uses **marketing years (MY)** aligned to harvest and marketing cycles rather than calendar years. This reference document provides comprehensive mapping of country/commodity combinations to their official marketing years, enabling accurate time series alignment across global agricultural data.

**Critical distinction**: Marketing Year (MY) reflects local harvest timing, while Trade Year (TY) provides uniform 12-month periods for global trade comparisons. The labeling convention varies by hemisphere—for most Northern Hemisphere countries, MY 2024/25 begins in 2024, but for Southern Hemisphere summer crops, MY 2024/25 begins in **2025**.

---

## PSD system architecture and data conventions

The PSD database is maintained by USDA's Foreign Agricultural Service with monthly updates by an interagency committee chaired by the World Agricultural Outlook Board (WAOB). Data for WASDE commodities is reviewed by FAS, ERS, FSA, and AMS, with international input from agricultural attachés worldwide. The database updates at **12:00 PM on WASDE release days**.

All PSD supply/use tables balance on **local marketing years**, while trade tables contain both MY and TY data for analytical comparisons. This means world imports and exports may not balance due to marketing year differences, shipments in transit, and reporting discrepancies.

### Marketing year label formatting

Standard labels use split-year notation (e.g., "2024/25") where the first year indicates when the MY begins for Northern Hemisphere countries. Southern Hemisphere oilseeds and summer grains use the **second year** to indicate MY start—MY 2024/25 for Brazil soybeans begins in February 2025, not 2024. Calendar year commodities (livestock, dairy) use single-year format ("2024" for January-December).

### Trade year definitions by commodity group

| Commodity Group | Trade Year | Period |
|----------------|------------|--------|
| Wheat | July-June | TY 2024/25 = Jul 2024 - Jun 2025 |
| Coarse Grains | October-September | TY 2024/25 = Oct 2024 - Sep 2025 |
| Rice | Calendar Year | TY 2024/25 = Jan-Dec 2025 |
| Oilseeds | October-September | Adjusted from local MYs |
| Cotton | August-July | Same as local MY |
| Livestock/Dairy | Calendar Year | Jan-Dec |

---

## United States marketing years serve as baseline reference

US marketing years are well-documented and serve as anchor points for understanding the system. The following table provides complete US MY definitions.

### US food and feed grains

| Commodity | MY Begin | Period | Notes |
|-----------|----------|--------|-------|
| Wheat | June (6) | Jun-May | |
| Rice | August (8) | Aug-Jul | Long-grain harvest Aug-Oct |
| Corn | September (9) | Sep-Aug | Changed from Oct/Sep in 1986 |
| Sorghum | September (9) | Sep-Aug | |
| Barley | June (6) | Jun-May | |
| Oats | June (6) | Jun-May | |

### US oilseeds and products

| Commodity | MY Begin | Period | Notes |
|-----------|----------|--------|-------|
| Soybeans | September (9) | Sep-Aug | |
| Soybean Meal | October (10) | Oct-Sep | |
| Soybean Oil | October (10) | Oct-Sep | |
| Peanuts | August (8) | Aug-Jul | |
| Cottonseed | August (8) | Aug-Jul | |
| Canola/Rapeseed | June (6) | Jun-May | |
| Sunflowerseed | September (9) | Sep-Aug | |

### US other commodities

| Commodity | MY Begin | Period |
|-----------|----------|--------|
| Cotton | August (8) | Aug-Jul |
| Sugar | October (10) | Oct-Sep |
| Beef/Cattle | January (1) | Calendar |
| Pork/Swine | January (1) | Calendar |
| Poultry | January (1) | Calendar |
| Dairy Products | January (1) | Calendar |
| Lard | January (1) | Calendar |
| Tallow | January (1) | Calendar |

---

## Southern Hemisphere conventions demand careful interpretation

The most critical implementation detail for global data alignment involves Southern Hemisphere summer crops. For Brazil, Argentina, and other Southern Hemisphere producers of soybeans and summer corn, the **second year of the MY label indicates when the marketing year begins**.

### Brazil marketing years

| Commodity | MY Begin | MY 2024/25 Starts | Labeling |
|-----------|----------|-------------------|----------|
| Soybeans | February (2) | Feb 2025 | Second year |
| Corn | March (3) | Mar 2025 | Second year |
| Cotton | August (8) | Aug 2024 | First year |
| Sugar | April (4) | Apr 2024 | First year |
| Beef/Poultry | January (1) | Jan 2024 | Calendar |

### Argentina marketing years

| Commodity | MY Begin | MY 2024/25 Starts | Labeling |
|-----------|----------|-------------------|----------|
| Soybeans | October (10) | Oct 2024 | Adjusted to TY |
| Corn | March (3) | Mar 2025 | Second year |
| Wheat | December (12) | Dec 2024 | First year |
| Sunflower | March (3) | Mar 2025 | Second year |
| Beef | January (1) | Jan 2024 | Calendar |

### Australia marketing years

| Commodity | MY Begin | MY 2024/25 Starts | Labeling |
|-----------|----------|-------------------|----------|
| Wheat | October (10) | Oct 2024 | First year |
| Barley | November (11) | Nov 2024 | First year |
| Cotton | August (8) | Aug 2024 | First year |
| Sorghum | March (3) | Mar 2025 | Second year |
| Beef/Dairy | January (1) | Jan 2024 | Calendar |

**Implementation note**: For Brazil soybeans MY 2024/25, the crop is planted September-November 2024, harvested January-April 2025, with the marketing year running February 2025 through January 2026. The "2024/25" label refers to the harvest year (2025) being the start.

---

## Wheat marketing years across major producers and traders

Wheat uses a **July-June Trade Year** globally for analytical comparisons, while local marketing years vary by harvest timing.

| Country | MY Begin | TY Begin | Notes |
|---------|----------|----------|-------|
| United States | June (6) | July (7) | Jun/May local |
| Canada | August (8) | July (7) | Aug/Jul local |
| Australia | October (10) | July (7) | Southern Hemisphere |
| Argentina | December (12) | July (7) | Dec/Nov local |
| EU | July (7) | July (7) | Same as TY |
| Russia | July (7) | July (7) | Same as TY |
| Ukraine | July (7) | July (7) | Same as TY |
| Kazakhstan | September (9) | July (7) | Sep/Aug local |
| India | April (4) | July (7) | Rabi harvest Mar-May |
| China | July (7) | July (7) | Same as TY |
| Turkey | June (6) | July (7) | Jun/May local |
| Pakistan | May (5) | July (7) | May/Apr local |
| Iran | April (4) | July (7) | Apr/Mar local |
| Egypt | July (7) | July (7) | Same as TY |
| Japan | April (4) | July (7) | Apr/Mar fiscal |
| Mexico | May (5) | July (7) | May/Apr local |

---

## Rice marketing years reflect diverse harvest patterns

Rice uses a **calendar year Trade Year** (TY 2024/25 = January-December 2025). Local marketing years vary significantly.

| Country | MY Begin | Notes |
|---------|----------|-------|
| United States | August (8) | Aug/Jul |
| India | October (10) | Kharif harvest Oct-Dec |
| Thailand | January (1) | Calendar year |
| Vietnam | January (1) | Calendar year |
| Pakistan | November (11) | Nov/Oct |
| China | January (1) | Calendar year |
| Philippines | January (1) | Calendar year |
| Indonesia | January (1) | Calendar year |
| Bangladesh | July (7) | Jul/Jun |
| Japan | April (4) | Fiscal year |
| Brazil | March (3) | Mar/Feb, Southern Hemisphere |
| Egypt | October (10) | Oct/Sep |
| EU | September (9) | Sep/Aug |

---

## Feed grains marketing years by major country

### Corn

| Country | MY Begin | Notes |
|---------|----------|-------|
| United States | September (9) | Sep/Aug |
| China | October (10) | Oct/Sep |
| Brazil | March (3) | Second year labeling |
| Argentina | March (3) | Second year labeling |
| Ukraine | October (10) | Oct/Sep |
| EU | October (10) | Oct/Sep |
| Mexico | October (10) | Oct/Sep |
| South Africa | May (5) | Second year labeling |
| Canada | September (9) | Sep/Aug |
| Russia | October (10) | Oct/Sep |

### Barley

| Country | MY Begin | Notes |
|---------|----------|-------|
| United States | June (6) | Jun/May |
| Canada | August (8) | Aug/Jul |
| Australia | November (11) | Nov/Oct |
| EU | July (7) | Jul/Jun |
| Russia | July (7) | Jul/Jun |
| Ukraine | July (7) | Jul/Jun |
| Argentina | December (12) | Dec/Nov |
| Kazakhstan | July (7) | Jul/Jun |
| Turkey | June (6) | Jun/May |

### Sorghum

| Country | MY Begin | Notes |
|---------|----------|-------|
| United States | September (9) | Sep/Aug |
| Mexico | October (10) | Oct/Sep |
| Argentina | March (3) | Second year labeling |
| Australia | March (3) | Second year labeling |
| Nigeria | October (10) | Oct/Sep |
| Sudan | November (11) | Nov/Oct |
| Ethiopia | December (12) | Dec/Nov |
| India | October (10) | Oct/Sep |

---

## Oilseeds marketing years for major producers

### Soybeans

| Country | MY Begin | Notes |
|---------|----------|-------|
| United States | September (9) | Sep/Aug |
| Brazil | October (10) | Second year; adjusted to TY |
| Argentina | October (10) | Second year; adjusted to TY |
| China | October (10) | Oct/Sep |
| India | October (10) | Oct/Sep |
| Paraguay | January (1) | Jan/Dec |
| Canada | August (8) | Aug/Jul |
| Ukraine | October (10) | Oct/Sep |
| EU | October (10) | Oct/Sep |

### Rapeseed/Canola

| Country | MY Begin | Notes |
|---------|----------|-------|
| Canada | August (8) | Aug/Jul, major exporter |
| EU | July (7) | Jul/Jun |
| China | October (10) | Oct/Sep |
| India | October (10) | Oct/Sep |
| Australia | October (10) | Oct/Sep |
| Ukraine | October (10) | Oct/Sep |
| United States | October (10) | Oct/Sep |

### Sunflowerseed

| Country | MY Begin | Notes |
|---------|----------|-------|
| Ukraine | September (9) | Sep/Aug |
| Russia | September (9) | Sep/Aug |
| Argentina | March (3) | Mar/Feb, second year |
| EU | October (10) | Oct/Sep |
| Turkey | September (9) | Sep/Aug |
| United States | September (9) | Sep/Aug |

---

## Vegetable oils and protein meals generally follow October-September

Most vegetable oils and protein meals use **October-September** marketing years globally, with exceptions noted below.

### Major oils by country

| Commodity | Country | MY Begin | Notes |
|-----------|---------|----------|-------|
| Palm Oil | Indonesia | October (10) | Oct/Sep |
| Palm Oil | Malaysia | October (10) | Oct/Sep |
| Soybean Oil | US | October (10) | Oct/Sep |
| Rapeseed Oil | EU | July (7) | Follows rapeseed |
| Rapeseed Oil | Canada | August (8) | Follows canola |
| Sunflower Oil | Ukraine | September (9) | Follows seed |
| Sunflower Oil | Russia | September (9) | Follows seed |
| Sunflower Oil | Argentina | March (3) | Mar/Feb |

### Protein meals

Protein meals generally follow October-September with the same country-specific exceptions as their source oilseeds:
- **Mexico**: September-August for soybean meal
- **Canada**: August-July for rapeseed/soybean meals
- **Paraguay, Vietnam, Philippines**: January-December
- **Bolivia**: March-February
- **Ukraine/Russia**: September-August for sunflower meal
- **Argentina**: March-February for sunflower meal

### Animal fats

All animal fats use **calendar year** (January-December) across all countries, consistent with livestock commodity conventions.

---

## Secondary commodities: cotton, sugar, livestock, dairy

### Cotton uses universal August-July

All countries use **August 1 - July 31** for cotton, regardless of hemisphere. Beginning stocks represent cotton lint physically located within a country on August 1.

### Sugar marketing years vary by country

| Country | MY Begin | Period |
|---------|----------|--------|
| Brazil | April (4) | Apr-Mar |
| India | October (10) | Oct-Sep |
| EU | October (10) | Oct-Sep |
| Thailand | December (12) | Dec-Nov |
| China | October (10) | Oct-Sep |
| United States | October (10) | Oct-Sep |
| Australia | July (7) | Jul-Jun |
| Philippines | September (9) | Sep-Aug |
| Most others | October (10) | Oct-Sep |

### Livestock and dairy use calendar year

All livestock products (beef, pork, poultry) and dairy products (butter, cheese, milk powder) use **January-December** calendar year for all countries globally.

---

## PSD country and commodity codes

### Major country codes

| Country | Code |
|---------|------|
| United States | US |
| Brazil | BR |
| Argentina | AR |
| Australia | AS |
| Canada | CA |
| China | CH |
| European Union | EU |
| India | IN |
| Indonesia | ID |
| Japan | JA |
| Malaysia | MY |
| Mexico | MX |
| Russia | RS |
| Thailand | TH |
| Ukraine | UA |

### API endpoints

Full lists available via PSD API:
- Countries: `https://apps.fas.usda.gov/OpenData/api/psd/countries`
- Commodities: `https://apps.fas.usda.gov/OpenData/api/psd/commodities`
- Regions: `https://apps.fas.usda.gov/OpenData/api/psd/regions`

---

## Implementation guidance for data alignment

When aligning time series data across countries:

1. **Identify the commodity group** to determine base TY convention
2. **Check if the country is Southern Hemisphere** for summer crops
3. **Apply the correct year interpretation** based on labeling rules
4. **Adjust to Trade Year** when comparing trade flows across countries
5. **Use calendar year** for all livestock and dairy products

For Brazil soybeans specifically: MY 2024/25 data represents February 2025 through January 2026, NOT October 2024 through September 2025. When comparing with US soybeans (September 2024 through August 2025), adjust for the approximately 5-month offset.

---

## Machine-readable JSON reference

The following JSON array provides the complete mapping for LLM agent consumption:

```json
[
  {"country": "United States", "country_code": "US", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 6, "my_end_month": 5, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "Jun-May local; Jul-Jun trade year"},
  {"country": "United States", "country_code": "US", "commodity": "Rice", "commodity_group": "food_grains", "my_begin_month": 8, "my_end_month": 7, "my_label_format": "2024/25", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Aug-Jul; TY is calendar year"},
  {"country": "United States", "country_code": "US", "commodity": "Corn", "commodity_group": "feed_grains", "my_begin_month": 9, "my_end_month": 8, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Sep-Aug since 1986"},
  {"country": "United States", "country_code": "US", "commodity": "Barley", "commodity_group": "feed_grains", "my_begin_month": 6, "my_end_month": 5, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Jun-May"},
  {"country": "United States", "country_code": "US", "commodity": "Sorghum", "commodity_group": "feed_grains", "my_begin_month": 9, "my_end_month": 8, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Sep-Aug"},
  {"country": "United States", "country_code": "US", "commodity": "Oats", "commodity_group": "feed_grains", "my_begin_month": 6, "my_end_month": 5, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Jun-May"},
  {"country": "United States", "country_code": "US", "commodity": "Soybeans", "commodity_group": "oilseeds", "my_begin_month": 9, "my_end_month": 8, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Sep-Aug"},
  {"country": "United States", "country_code": "US", "commodity": "Rapeseed", "commodity_group": "oilseeds", "my_begin_month": 6, "my_end_month": 5, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Jun-May"},
  {"country": "United States", "country_code": "US", "commodity": "Sunflowerseed", "commodity_group": "oilseeds", "my_begin_month": 9, "my_end_month": 8, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Sep-Aug"},
  {"country": "United States", "country_code": "US", "commodity": "Peanuts", "commodity_group": "oilseeds", "my_begin_month": 8, "my_end_month": 7, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Aug-Jul"},
  {"country": "United States", "country_code": "US", "commodity": "Cottonseed", "commodity_group": "oilseeds", "my_begin_month": 8, "my_end_month": 7, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Aug-Jul"},
  {"country": "United States", "country_code": "US", "commodity": "Soybean Meal", "commodity_group": "protein_meals", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep"},
  {"country": "United States", "country_code": "US", "commodity": "Soybean Oil", "commodity_group": "vegetable_oils", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep"},
  {"country": "United States", "country_code": "US", "commodity": "Cotton", "commodity_group": "cotton", "my_begin_month": 8, "my_end_month": 7, "my_label_format": "2024/25", "ty_begin_month": 8, "ty_end_month": 7, "notes": "Aug-Jul universal"},
  {"country": "United States", "country_code": "US", "commodity": "Sugar", "commodity_group": "sugar", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep fiscal year"},
  {"country": "United States", "country_code": "US", "commodity": "Beef", "commodity_group": "livestock", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "United States", "country_code": "US", "commodity": "Pork", "commodity_group": "livestock", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "United States", "country_code": "US", "commodity": "Poultry", "commodity_group": "livestock", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "United States", "country_code": "US", "commodity": "Butter", "commodity_group": "dairy", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "United States", "country_code": "US", "commodity": "Cheese", "commodity_group": "dairy", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "United States", "country_code": "US", "commodity": "Milk Powder", "commodity_group": "dairy", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "United States", "country_code": "US", "commodity": "Lard", "commodity_group": "fats", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "United States", "country_code": "US", "commodity": "Tallow", "commodity_group": "fats", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "Brazil", "country_code": "BR", "commodity": "Soybeans", "commodity_group": "oilseeds", "my_begin_month": 2, "my_end_month": 1, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Feb-Jan local; second year labeling (MY 2024/25 starts Feb 2025)"},
  {"country": "Brazil", "country_code": "BR", "commodity": "Corn", "commodity_group": "feed_grains", "my_begin_month": 3, "my_end_month": 2, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Mar-Feb; second year labeling"},
  {"country": "Brazil", "country_code": "BR", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 7, "my_end_month": 6, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "Jul-Jun"},
  {"country": "Brazil", "country_code": "BR", "commodity": "Cotton", "commodity_group": "cotton", "my_begin_month": 8, "my_end_month": 7, "my_label_format": "2024/25", "ty_begin_month": 8, "ty_end_month": 7, "notes": "Aug-Jul universal"},
  {"country": "Brazil", "country_code": "BR", "commodity": "Sugar", "commodity_group": "sugar", "my_begin_month": 4, "my_end_month": 3, "my_label_format": "2024/25", "ty_begin_month": 4, "ty_end_month": 3, "notes": "Apr-Mar; unique to Brazil"},
  {"country": "Brazil", "country_code": "BR", "commodity": "Beef", "commodity_group": "livestock", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "Brazil", "country_code": "BR", "commodity": "Poultry", "commodity_group": "livestock", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "Argentina", "country_code": "AR", "commodity": "Soybeans", "commodity_group": "oilseeds", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep; adjusted to TY"},
  {"country": "Argentina", "country_code": "AR", "commodity": "Corn", "commodity_group": "feed_grains", "my_begin_month": 3, "my_end_month": 2, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Mar-Feb; second year labeling"},
  {"country": "Argentina", "country_code": "AR", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 12, "my_end_month": 11, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "Dec-Nov local"},
  {"country": "Argentina", "country_code": "AR", "commodity": "Barley", "commodity_group": "feed_grains", "my_begin_month": 12, "my_end_month": 11, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Dec-Nov"},
  {"country": "Argentina", "country_code": "AR", "commodity": "Sunflowerseed", "commodity_group": "oilseeds", "my_begin_month": 3, "my_end_month": 2, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Mar-Feb; second year labeling"},
  {"country": "Argentina", "country_code": "AR", "commodity": "Beef", "commodity_group": "livestock", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "Australia", "country_code": "AS", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "Oct-Sep; harvest Dec-Feb"},
  {"country": "Australia", "country_code": "AS", "commodity": "Barley", "commodity_group": "feed_grains", "my_begin_month": 11, "my_end_month": 10, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Nov-Oct"},
  {"country": "Australia", "country_code": "AS", "commodity": "Sorghum", "commodity_group": "feed_grains", "my_begin_month": 3, "my_end_month": 2, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Mar-Feb; second year labeling"},
  {"country": "Australia", "country_code": "AS", "commodity": "Cotton", "commodity_group": "cotton", "my_begin_month": 8, "my_end_month": 7, "my_label_format": "2024/25", "ty_begin_month": 8, "ty_end_month": 7, "notes": "Aug-Jul universal"},
  {"country": "Australia", "country_code": "AS", "commodity": "Beef", "commodity_group": "livestock", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "Australia", "country_code": "AS", "commodity": "Sugar", "commodity_group": "sugar", "my_begin_month": 7, "my_end_month": 6, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "Jul-Jun"},
  {"country": "Canada", "country_code": "CA", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 8, "my_end_month": 7, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "Aug-Jul"},
  {"country": "Canada", "country_code": "CA", "commodity": "Corn", "commodity_group": "feed_grains", "my_begin_month": 9, "my_end_month": 8, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Sep-Aug"},
  {"country": "Canada", "country_code": "CA", "commodity": "Barley", "commodity_group": "feed_grains", "my_begin_month": 8, "my_end_month": 7, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Aug-Jul"},
  {"country": "Canada", "country_code": "CA", "commodity": "Oats", "commodity_group": "feed_grains", "my_begin_month": 8, "my_end_month": 7, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Aug-Jul"},
  {"country": "Canada", "country_code": "CA", "commodity": "Soybeans", "commodity_group": "oilseeds", "my_begin_month": 8, "my_end_month": 7, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Aug-Jul"},
  {"country": "Canada", "country_code": "CA", "commodity": "Rapeseed", "commodity_group": "oilseeds", "my_begin_month": 8, "my_end_month": 7, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Aug-Jul; major canola exporter"},
  {"country": "European Union", "country_code": "EU", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 7, "my_end_month": 6, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "Jul-Jun; same as TY"},
  {"country": "European Union", "country_code": "EU", "commodity": "Corn", "commodity_group": "feed_grains", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep"},
  {"country": "European Union", "country_code": "EU", "commodity": "Barley", "commodity_group": "feed_grains", "my_begin_month": 7, "my_end_month": 6, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Jul-Jun"},
  {"country": "European Union", "country_code": "EU", "commodity": "Rapeseed", "commodity_group": "oilseeds", "my_begin_month": 7, "my_end_month": 6, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Jul-Jun"},
  {"country": "European Union", "country_code": "EU", "commodity": "Sunflowerseed", "commodity_group": "oilseeds", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep"},
  {"country": "European Union", "country_code": "EU", "commodity": "Sugar", "commodity_group": "sugar", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep"},
  {"country": "China", "country_code": "CH", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 7, "my_end_month": 6, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "Jul-Jun"},
  {"country": "China", "country_code": "CH", "commodity": "Rice", "commodity_group": "food_grains", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024/25", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "China", "country_code": "CH", "commodity": "Corn", "commodity_group": "feed_grains", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep"},
  {"country": "China", "country_code": "CH", "commodity": "Soybeans", "commodity_group": "oilseeds", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep"},
  {"country": "China", "country_code": "CH", "commodity": "Cotton", "commodity_group": "cotton", "my_begin_month": 8, "my_end_month": 7, "my_label_format": "2024/25", "ty_begin_month": 8, "ty_end_month": 7, "notes": "Aug-Jul; largest producer"},
  {"country": "China", "country_code": "CH", "commodity": "Sugar", "commodity_group": "sugar", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep"},
  {"country": "China", "country_code": "CH", "commodity": "Pork", "commodity_group": "livestock", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year; largest producer"},
  {"country": "Russia", "country_code": "RS", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 7, "my_end_month": 6, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "Jul-Jun; same as TY"},
  {"country": "Russia", "country_code": "RS", "commodity": "Corn", "commodity_group": "feed_grains", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep"},
  {"country": "Russia", "country_code": "RS", "commodity": "Barley", "commodity_group": "feed_grains", "my_begin_month": 7, "my_end_month": 6, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Jul-Jun"},
  {"country": "Russia", "country_code": "RS", "commodity": "Sunflowerseed", "commodity_group": "oilseeds", "my_begin_month": 9, "my_end_month": 8, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Sep-Aug"},
  {"country": "Ukraine", "country_code": "UA", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 7, "my_end_month": 6, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "Jul-Jun; same as TY"},
  {"country": "Ukraine", "country_code": "UA", "commodity": "Corn", "commodity_group": "feed_grains", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep"},
  {"country": "Ukraine", "country_code": "UA", "commodity": "Barley", "commodity_group": "feed_grains", "my_begin_month": 7, "my_end_month": 6, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Jul-Jun"},
  {"country": "Ukraine", "country_code": "UA", "commodity": "Sunflowerseed", "commodity_group": "oilseeds", "my_begin_month": 9, "my_end_month": 8, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Sep-Aug; world's largest producer"},
  {"country": "India", "country_code": "IN", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 4, "my_end_month": 3, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "Apr-Mar; Rabi harvest"},
  {"country": "India", "country_code": "IN", "commodity": "Rice", "commodity_group": "food_grains", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Oct-Sep; Kharif harvest"},
  {"country": "India", "country_code": "IN", "commodity": "Corn", "commodity_group": "feed_grains", "my_begin_month": 11, "my_end_month": 10, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Nov-Oct; Kharif crop"},
  {"country": "India", "country_code": "IN", "commodity": "Soybeans", "commodity_group": "oilseeds", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep"},
  {"country": "India", "country_code": "IN", "commodity": "Cotton", "commodity_group": "cotton", "my_begin_month": 8, "my_end_month": 7, "my_label_format": "2024/25", "ty_begin_month": 8, "ty_end_month": 7, "notes": "Aug-Jul"},
  {"country": "India", "country_code": "IN", "commodity": "Sugar", "commodity_group": "sugar", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep"},
  {"country": "Indonesia", "country_code": "ID", "commodity": "Rice", "commodity_group": "food_grains", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024/25", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "Indonesia", "country_code": "ID", "commodity": "Palm Oil", "commodity_group": "vegetable_oils", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep; world's largest producer"},
  {"country": "Malaysia", "country_code": "MY", "commodity": "Palm Oil", "commodity_group": "vegetable_oils", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep"},
  {"country": "Thailand", "country_code": "TH", "commodity": "Rice", "commodity_group": "food_grains", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024/25", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "Thailand", "country_code": "TH", "commodity": "Sugar", "commodity_group": "sugar", "my_begin_month": 12, "my_end_month": 11, "my_label_format": "2024/25", "ty_begin_month": 12, "ty_end_month": 11, "notes": "Dec-Nov"},
  {"country": "Vietnam", "country_code": "VN", "commodity": "Rice", "commodity_group": "food_grains", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024/25", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "Japan", "country_code": "JA", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 4, "my_end_month": 3, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "Apr-Mar fiscal year"},
  {"country": "Japan", "country_code": "JA", "commodity": "Rice", "commodity_group": "food_grains", "my_begin_month": 4, "my_end_month": 3, "my_label_format": "2024/25", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Apr-Mar fiscal year"},
  {"country": "Mexico", "country_code": "MX", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 5, "my_end_month": 4, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "May-Apr"},
  {"country": "Mexico", "country_code": "MX", "commodity": "Corn", "commodity_group": "feed_grains", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep"},
  {"country": "Mexico", "country_code": "MX", "commodity": "Sugar", "commodity_group": "sugar", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep"},
  {"country": "Egypt", "country_code": "EG", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 7, "my_end_month": 6, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "Jul-Jun"},
  {"country": "Egypt", "country_code": "EG", "commodity": "Rice", "commodity_group": "food_grains", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Oct-Sep; Nile Delta harvest"},
  {"country": "Pakistan", "country_code": "PK", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 5, "my_end_month": 4, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "May-Apr; Rabi harvest"},
  {"country": "Pakistan", "country_code": "PK", "commodity": "Rice", "commodity_group": "food_grains", "my_begin_month": 11, "my_end_month": 10, "my_label_format": "2024/25", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Nov-Oct; Kharif harvest"},
  {"country": "Pakistan", "country_code": "PK", "commodity": "Cotton", "commodity_group": "cotton", "my_begin_month": 8, "my_end_month": 7, "my_label_format": "2024/25", "ty_begin_month": 8, "ty_end_month": 7, "notes": "Aug-Jul"},
  {"country": "Kazakhstan", "country_code": "KZ", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 9, "my_end_month": 8, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "Sep-Aug"},
  {"country": "Turkey", "country_code": "TU", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 6, "my_end_month": 5, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "Jun-May"},
  {"country": "Turkey", "country_code": "TU", "commodity": "Barley", "commodity_group": "feed_grains", "my_begin_month": 6, "my_end_month": 5, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Jun-May"},
  {"country": "Turkey", "country_code": "TU", "commodity": "Sunflowerseed", "commodity_group": "oilseeds", "my_begin_month": 9, "my_end_month": 8, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Sep-Aug"},
  {"country": "South Africa", "country_code": "SF", "commodity": "Corn", "commodity_group": "feed_grains", "my_begin_month": 5, "my_end_month": 4, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "May-Apr; second year labeling"},
  {"country": "South Africa", "country_code": "SF", "commodity": "Wheat", "commodity_group": "food_grains", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 7, "ty_end_month": 6, "notes": "Oct-Sep"},
  {"country": "New Zealand", "country_code": "NZ", "commodity": "Butter", "commodity_group": "dairy", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year"},
  {"country": "New Zealand", "country_code": "NZ", "commodity": "Cheese", "commodity_group": "dairy", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year; major exporter"},
  {"country": "New Zealand", "country_code": "NZ", "commodity": "Milk Powder", "commodity_group": "dairy", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024", "ty_begin_month": 1, "ty_end_month": 12, "notes": "Calendar year; largest exporter"},
  {"country": "Paraguay", "country_code": "PA", "commodity": "Soybeans", "commodity_group": "oilseeds", "my_begin_month": 1, "my_end_month": 12, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Jan-Dec calendar year"},
  {"country": "Peru", "country_code": "PE", "commodity": "Fish Meal", "commodity_group": "protein_meals", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep; major anchovy producer"},
  {"country": "Chile", "country_code": "CI", "commodity": "Fish Meal", "commodity_group": "protein_meals", "my_begin_month": 10, "my_end_month": 9, "my_label_format": "2024/25", "ty_begin_month": 10, "ty_end_month": 9, "notes": "Oct-Sep"}
]
```

---

## Conclusion: Key implementation priorities

This reference enables accurate agricultural data alignment by addressing three critical challenges:

1. **Southern Hemisphere year labeling** is the most common source of alignment errors. For Brazil/Argentina summer crops (soybeans, corn), always interpret MY 2024/25 as starting in calendar year **2025**, not 2024. This 12-month offset relative to Northern Hemisphere data must be explicitly handled in time series joins.

2. **Trade Year standardization** allows apples-to-apples comparison of trade flows. When analyzing global wheat trade, convert all countries to July-June TY; for corn and oilseeds, use October-September TY. The PSD API provides both MY and TY-adjusted trade data.

3. **Commodity group conventions** provide predictable defaults: grains and oilseeds use crop-aligned marketing years, while all livestock, dairy, and animal fats use calendar year universally. Cotton uses August-July globally regardless of hemisphere.

For programmatic implementation, query the PSD API endpoints for authoritative country and commodity codes, then apply the marketing year mappings from this reference. The JSON array above provides the flat structure optimized for LLM agent consumption, enabling automated data alignment workflows.