# Data Sources Master Checklist

Use this file to track what we have and what we need for each data source.
The CSV file `data_sources_master.csv` is the machine-readable version.

## Legend
- ✅ Complete
- ⚠️ Partial
- ❌ Needed
- ➖ Not applicable

---

## USDA Reports

### 1. Cattle on Feed (NASS)
| Item | Status | Value |
|------|--------|-------|
| Schema | ✅ | `schemas/cattle_on_feed_bronze.sql` |
| Agent | ❌ | `usda_cattle_agent` |
| Sample File | ✅ | `C:\Users\torem\report_samples\cofd1225.pdf` |
| Publication URL | ✅ | https://esmis.nal.usda.gov/publication/cattle-feed |
| Download Pattern | ✅ | `cofd{MMYY}.pdf` and `.zip` |
| Has API | ❌ | |
| 2026 Dates | ✅ | Jan 23, Feb 20, Mar 20, Apr 17, May 22, Jun 18, Jul 24, Aug 21, Sep 18, Oct 23, Nov 20, Dec 18 |
| Release Time | ✅ | 3:00 PM ET |
| Holiday Aware | ➖ | No (specific dates) |

---

### 2. WASDE (OCE)
| Item | Status | Value |
|------|--------|-------|
| Schema | ❌ | |
| Agent | ❌ | `usda_wasde_agent` |
| Sample File | ⚠️ | `Jacobsen\Data Files\WASDE - 0122.pdf` (older) |
| Publication URL | ❌ | |
| Download Pattern | ❌ | |
| Has API | ❌ | |
| 2026 Dates | ✅ | Jan 12, Feb 10, Mar 10, Apr 9, May 12, Jun 11, Jul 10, Aug 12, Sep 11, Oct 9, Nov 10, Dec 10 |
| Release Time | ✅ | 12:00 PM ET |
| Holiday Aware | ➖ | No (specific dates) |

---

### 3. Export Sales (FAS)
| Item | Status | Value |
|------|--------|-------|
| Schema | ❌ | |
| Agent | ❌ | `usda_export_sales_agent` |
| Sample File | ⚠️ | `Jacobsen\Data Files\Export Sales - *.pdf` |
| Publication URL | ❌ | |
| Download Pattern | ❌ | |
| Has API | ❌ | |
| 2026 Dates | ➖ | Weekly - Thursday |
| Release Time | ✅ | 8:30 AM ET |
| Holiday Aware | ✅ | Yes - shifts to Friday if holiday earlier in week |

---

### 4. Export Inspections (AMS)
| Item | Status | Value |
|------|--------|-------|
| Schema | ❌ | |
| Agent | ❌ | `usda_export_inspections_agent` |
| Sample File | ⚠️ | `Jacobsen\Data Files\Export Inspections - *.xlsx` (115 files) |
| Publication URL | ❌ | |
| Download Pattern | ❌ | |
| Has API | ❌ | |
| 2026 Dates | ➖ | Weekly - Monday |
| Release Time | ✅ | 10:00 AM ET |
| Holiday Aware | ✅ | Yes - shifts to Tuesday if Monday holiday |

---

### 5. Grain Stocks (NASS)
| Item | Status | Value |
|------|--------|-------|
| Schema | ❌ | |
| Agent | ❌ | `usda_grain_stocks_agent` |
| Sample File | ⚠️ | `Jacobsen\Data Files\Grain Stocks - 0122.pdf` |
| Publication URL | ❌ | |
| Download Pattern | ❌ | |
| Has API | ❌ | |
| 2026 Dates | ✅ | Jan 12 (w/WASDE), Mar 31 (w/Plantings), Jun 30 (w/Acreage), Sep 30 |
| Release Time | ✅ | 12:00 PM ET |
| Holiday Aware | ➖ | No (specific dates) |

---

### 6. Prospective Plantings (NASS)
| Item | Status | Value |
|------|--------|-------|
| Schema | ❌ | |
| Agent | ❌ | `usda_plantings_agent` |
| Sample File | ⚠️ | `Jacobsen\Data Files\Prospective Plantings - 2022 - 03312022.pdf` |
| Publication URL | ❌ | |
| Download Pattern | ❌ | |
| Has API | ❌ | |
| 2026 Dates | ✅ | Mar 31 |
| Release Time | ✅ | 12:00 PM ET |
| Holiday Aware | ➖ | No (last biz day of March) |

---

### 7. Acreage (NASS)
| Item | Status | Value |
|------|--------|-------|
| Schema | ❌ | |
| Agent | ❌ | `usda_acreage_agent` |
| Sample File | ⚠️ | `Jacobsen\Data Files\Acreage - 0622.pdf` |
| Publication URL | ❌ | |
| Download Pattern | ❌ | |
| Has API | ❌ | |
| 2026 Dates | ✅ | Jun 30 |
| Release Time | ✅ | 12:00 PM ET |
| Holiday Aware | ➖ | No (last biz day of June) |

---

### 8. Crop Progress (NASS)
| Item | Status | Value |
|------|--------|-------|
| Schema | ❌ | |
| Agent | ❌ | `usda_crop_progress_agent` |
| Sample File | ⚠️ | `Jacobsen\Data Files\Crop Progress - 07072020.pdf` |
| Publication URL | ❌ | |
| Download Pattern | ❌ | |
| Has API | ❌ | |
| 2026 Dates | ➖ | Weekly - Monday (April-November) |
| Release Time | ✅ | 4:00 PM ET |
| Holiday Aware | ✅ | Yes - shifts to Tuesday if Monday holiday |
| Seasonal | ✅ | April - November only |

---

### 9. Feed Grains Outlook (ERS)
| Item | Status | Value |
|------|--------|-------|
| Schema | ❌ | |
| Agent | ❌ | `usda_feed_grains_agent` |
| Sample File | ⚠️ | `Jacobsen\Data Files\Feed Grains Outlook - 1022.pdf` |
| Publication URL | ❌ | |
| Download Pattern | ❌ | |
| Has API | ❌ | |
| 2026 Dates | ❌ | Monthly - need specific dates |
| Release Time | ⚠️ | ~12:00 PM ET? |
| Holiday Aware | ❌ | Unknown |

---

### 10. Oil Crops Outlook (ERS)
| Item | Status | Value |
|------|--------|-------|
| Schema | ❌ | |
| Agent | ❌ | `usda_oil_crops_agent` |
| Sample File | ⚠️ | `Jacobsen\Data Files\Oil Crops Outlook - April 2022.xlsx` |
| Publication URL | ❌ | |
| Download Pattern | ❌ | |
| Has API | ❌ | |
| 2026 Dates | ❌ | Monthly - need specific dates |
| Release Time | ⚠️ | ~12:00 PM ET? |
| Holiday Aware | ❌ | Unknown |

---

## Other Government Reports

### 11. EIA Petroleum Status
| Item | Status | Value |
|------|--------|-------|
| Schema | ❌ | |
| Agent | ❌ | `eia_petroleum_agent` |
| Sample File | ⚠️ | Multiple in `Jacobsen\Data Files\` |
| Publication URL | ❌ | |
| Download Pattern | ❌ | |
| Has API | ⚠️ | EIA has an API |
| 2026 Dates | ➖ | Weekly - Wednesday |
| Release Time | ✅ | 10:30 AM ET |
| Holiday Aware | ✅ | Yes - shifts to Thursday if holiday earlier in week |

---

### 12. Census Trade Data
| Item | Status | Value |
|------|--------|-------|
| Schema | ❌ | |
| Agent | ❌ | `census_trade_agent` |
| Sample File | ⚠️ | `Jacobsen\Data Files\Census US Biodiesel Imports - *.xlsx` |
| Publication URL | ❌ | |
| Download Pattern | ❌ | |
| Has API | ⚠️ | Census has an API |
| 2026 Dates | ⚠️ | Jan 29 only - need rest |
| Release Time | ✅ | 8:30 AM ET |
| Holiday Aware | ➖ | No (specific dates) |

---

## International Reports

### 13. CONAB Brazil
| Item | Status | Value |
|------|--------|-------|
| Schema | ❌ | |
| Agent | ❌ | `conab_agent` |
| Sample File | ⚠️ | Multiple Brazil files in `Jacobsen\Data Files\` |
| Publication URL | ❌ | |
| Download Pattern | ❌ | |
| Has API | ❌ | |
| 2026 Dates | ❌ | Monthly ~10th - need specific dates |
| Release Time | ⚠️ | ~9:00 AM? |
| Holiday Aware | ❌ | Unknown |

---

## Industry Reports

### 14. NOPA Crush
| Item | Status | Value |
|------|--------|-------|
| Schema | ❌ | |
| Agent | ❌ | `nopa_crush_agent` |
| Sample File | ❌ | |
| Publication URL | ❌ | |
| Download Pattern | ❌ | |
| Has API | ❌ | |
| 2026 Dates | ⚠️ | 15th of each month (or next biz day) |
| Release Time | ⚠️ | ~12:00 PM ET? |
| Holiday Aware | ✅ | Simple - next biz day if weekend/holiday |

---

### 15. CME Settlements
| Item | Status | Value |
|------|--------|-------|
| Schema | ❌ | |
| Agent | ❌ | `cme_settlements_agent` |
| Sample File | ❌ | |
| Publication URL | ❌ | |
| Download Pattern | ❌ | |
| Has API | ⚠️ | CME has data feeds |
| 2026 Dates | ➖ | Daily - weekdays |
| Release Time | ✅ | 5:00 PM CT |
| Holiday Aware | ✅ | Skips market holidays |

---

## Summary Statistics

| Category | Total | Schema Done | Agent Done | Sample Available |
|----------|-------|-------------|------------|------------------|
| USDA | 10 | 1 | 0 | 10 |
| Other Gov | 2 | 0 | 0 | 2 |
| International | 1 | 0 | 0 | 1 |
| Industry | 2 | 0 | 0 | 0 |
| **TOTAL** | **15** | **1** | **0** | **13** |

---

## To Fill In

For each report, please provide (where available):
1. **Publication URL** - Landing page for the report
2. **Download URL** - Direct link to a recent file (I'll extract the pattern)
3. **2026 Dates** - If not weekly and we don't have them
4. **Sample File** - If better/newer sample available
5. **API Info** - If you know of an API endpoint

