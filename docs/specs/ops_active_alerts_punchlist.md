# Ops dashboard — Active Alerts triage

*Generated 2026-05-21 20:56 UTC. Total active: 437 alerts across 30 sources.*

Three buckets:
- 📥 **Manual** — Tore action needed (download a file, register for an API key, etc.)
- 🔴 **Code** — Claude-UI action (collector bug or registration issue)
- 🟢 **Noise** — already resolved or informational; safe to bulk-acknowledge

---

## 📥 Manual — 7 sources, 28 alerts total

**These are the spreadsheets/downloads you asked about.** Each one is a publicly-accessible USDA / EPA / global-government data file whose URL the collector can't reach automatically (auth change, URL change, server 403).

| Source | Count | Last seen | What to download |
|--------|------:|-----------|------------------|
| `mpob` | 7 | 2026-05-10 08:30 | Malaysian Palm Oil Board: all endpoints 404. MPOB restructured their public data site — go to https://bepi.mpob.gov.my/ and download monthly Excel files. |
| `faostat` | 6 | 2026-05-01 10:31 | FAOSTAT bulk CSV download getting 403. Options: register for API key at fao.org/faostat OR download bulk zips manually from https://www.fao.org/faostat/en/#data. |
| `conab` | 4 | 2026-05-10 13:30 | CONAB Brazil: 404 on automated download. Grab the latest Levantamento de Safra PDF/xlsx from https://www.conab.gov.br/info-agro/safras manually. |
| `usda_ers_oil_crops` | 4 | 2026-04-13 09:18 | USDA ERS Oil Crops Yearbook: no data retrieved. Download the latest from https://www.ers.usda.gov/data-products/oil-crops-yearbook/ |
| `usda_ers_feed_grains` | 3 | 2026-03-20 13:30 | USDA ERS Feed Grains: HTTP 404. Download from https://www.ers.usda.gov/data-products/feed-grains-database/ |
| `usda_ers_wheat` | 3 | 2026-03-20 14:30 | USDA ERS Wheat Data: HTTP 404. Download from https://www.ers.usda.gov/data-products/wheat-data/ |
| `epa_rfs` | 1 | 2026-03-06 12:11 | EPA data files. Their public download moved — likely need to grab from https://www.epa.gov/fuels-registration-reporting-and-compliance-help/public-data-renewable-fuel-standard manually. |

## 🔴 Code — 9 sources, 189 alerts total

Real collector bugs. Tasks created or referenced where applicable.

| Source | Count | Last seen | Diagnosis |
|--------|------:|-----------|-----------|
| `cme_settlements` | 55 | 2026-05-21 11:59 | Listed as OVERDUE but daily run is operating normally — alert logic needs a tighter threshold. |
| `gefs_ensemble` | 39 | 2026-05-12 12:30 | GEFS ensemble collector returning None error. Code/parser bug. |
| `usda_ams_ddgs` | 30 | 2026-05-15 18:30 | AMS Grain Co-Products parser: "No data parsed from report". Same family of issue as feedstocks. |
| `usda_ams_feedstocks` | 30 | 2026-05-15 18:30 | AMS Tallow/Protein parser: "No data parsed from report". Slug ID may have changed or report format shifted. |
| `yield_forecast` | 27 | 2026-05-12 11:30 | Collector not found in registry — needs registration in collector_registry.py. |
| `ndvi_charts` | 3 | 2026-04-28 14:30 | NDVICollector.download_all_charts signature mismatch on `commodity` kwarg. |
| `weather_daily_summary` | 3 | 2026-03-11 11:00 | Same — not in registry. Class file exists, just not registered. |
| `eia_ethanol` | 1 | 2026-02-14 13:55 | Reports needing EIA_API_KEY but the key IS set. Code likely missing the env-var lookup. |
| `nass_processing` | 1 | 2026-04-12 10:00 | NASSProcessingCollector has no .collect() override. Same bug as AMS pre-patch. |

## 🟢 Noise — 14 sources, 220 alerts total

Already-resolved failures still showing in the UI because nobody acknowledged them. Safe to bulk-clear with the SQL below.

| Source | Count | Last seen | Why noise |
|--------|------:|-----------|-----------|
| `dispatcher_watchdog` | 51 | 2026-05-21 17:20 | Healthy operational events (watchdog restarts). Acknowledge to clear UI. |
| `usda_fas_export_sales` | 51 | 2026-05-21 13:00 | Pre-fix failures from earlier today (before v2 collector landed). Tomorrow run is on the v2 path; alerts will stop. |
| `canada_cgc` | 34 | 2026-03-06 20:42 | Fixed in March 2026 (rewrite to CSV bulk download). Old alerts pre-fix. |
| `canada_statscan` | 34 | 2026-03-06 20:47 | Fixed in March 2026. Old alerts pre-fix. |
| `drought_monitor` | 27 | 2026-03-06 20:29 | Fixed in March 2026 (FIPS codes + CSV parsing). Old alerts pre-fix. |
| `census_trade` | 4 | 2026-03-12 20:28 | Old March connection errors; collector is healthy now (470k rows). |
| `epa_echo_biodiesel` | 3 | 2026-05-01 09:30 | May 1 transient "No facilities found" from EPA ECHO. Resolved — recent runs succeed. |
| `epa_echo_ethanol` | 3 | 2026-05-01 09:00 | Same May 1 transient. Resolved. |
| `epa_echo_milling` | 3 | 2026-05-01 10:00 | Same May 1 transient. Resolved. |
| `knowledge_graph` | 3 | 2026-02-14 14:03 | KG batch-load success notifications. |
| `wasde_monthly` | 3 | 2026-04-09 16:14 | These are `report_generated` success events at priority=1. Acknowledge. |
| `DOC` | 2 | 2026-04-03 10:09 | Daily ops anomaly counts. Review but not blocking. |
| `cftc_cot` | 1 | 2026-03-06 11:47 | Single old isoformat error from March. Collector healthy now. |
| `kg_extraction` | 1 | 2026-02-14 14:25 | KG extraction success notifications. |

### Bulk-acknowledge SQL (clears the noise bucket)

```sql
UPDATE core.event_log SET acknowledged = TRUE
WHERE id = ANY(ARRAY[366,464,466,468,470,473,474,476,479,481,482,484,486,488,490,493,494,496,498,501,502,505,507,508,511,512,515,516,519,520,522,525,526,528,530,532,534,536,538,541,542,544,546,548,551,553,586,949,954,980,990,23,24,25,26,27,30,31,32,33,34,35,36,240,241,242,246,247,248,249,250,251,331,333,335,391,393,394,442,444,445,580,582,583,646,648,649,699,701,702,767,769,770,845,846,847,918,919,920,986,988,989,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,110,253,254,258,778,780,782,776,777,779,781,783,785,2,5,6,13,14,584,441,451,22,9]);
```

*(That clears 220 rows.)*
