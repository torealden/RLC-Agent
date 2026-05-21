# Silent-Failure Audit — 28 suspect collectors

*Generated 2026-05-21 17:40 UTC.*

Audit pattern: collectors with no obvious save_to_bronze in their class file were flagged as suspects. For each one, we look at: (a) whether the dispatcher has been running it in the last 30 days, and (b) whether its bronze target table has fresh data.

**Diagnostic states:**
- ✅ **HEALTHY** — runs + bronze data current within 14 days
- 🟡 **STALE** — bronze data older than 14 days (could be normal for slow-cadence sources)
- ⚠️  **SILENT FAILURE** — runs report success but bronze data is stale — the AMS / CFTC pattern
- ❌ **NEVER RUNS / EMPTY BRONZE / MISSING TABLE** — straight broken
- ❓ **UNKNOWN** — couldn't determine freshness from schema

## ⚠️ SILENT FAILURE — 1 collectors

| Collector | Bronze table | Last run | Bronze latest | Rows | Notes |
|-----------|--------------|----------|---------------|------|-------|
| `usda_nass_crop_progress` | `bronze.nass_crop_progress` | 2026-05-18 | 2025-11-25 | 9,077 | Runs report success but bronze data is 177d old. |

## ❌ NEVER RUNS — 2 collectors

| Collector | Bronze table | Last run | Bronze latest | Rows | Notes |
|-----------|--------------|----------|---------------|------|-------|
| `usda_nass_acreage` | `bronze.nass_acreage` | — | 2026-04-02 | 30,442 |  |
| `usda_nass_stocks` | `bronze.nass_stocks` | — | 2026-01-28 | 48 |  |

## ❌ MISSING TABLE — 16 collectors

| Collector | Bronze table | Last run | Bronze latest | Rows | Notes |
|-----------|--------------|----------|---------------|------|-------|
| `anec` | `bronze.anec` | — | — | — | Bronze table bronze.anec does not exist. |
| `cme_settlements` | `bronze.cme_settlements` | 2026-05-20 | — | — | Bronze table bronze.cme_settlements does not exist. |
| `eia_ethanol` | `bronze.eia_ethanol` | 2026-05-20 | — | — | Bronze table bronze.eia_ethanol does not exist. |
| `eia_petroleum` | `bronze.eia_petroleum` | 2026-05-20 | — | — | Bronze table bronze.eia_petroleum does not exist. |
| `epa_rfs` | `bronze.epa_rfs_rin_generation` | — | — | — | Bronze table bronze.epa_rfs_rin_generation does not exist. |
| `futures_overnight` | `bronze.futures_prices` | — | — | — | Bronze table bronze.futures_prices does not exist. |
| `futures_settlement` | `bronze.cme_settlements` | — | — | — | Bronze table bronze.cme_settlements does not exist. |
| `futures_us_session` | `bronze.futures_prices` | — | — | — | Bronze table bronze.futures_prices does not exist. |
| `gefs_ensemble` | `bronze.gefs_ensemble` | 2026-05-21 | — | — | Bronze table bronze.gefs_ensemble does not exist. |
| `gfs_forecast` | `bronze.gfs_forecast` | 2026-05-21 | — | — | Bronze table bronze.gfs_forecast does not exist. |
| `mpob` | `bronze.mpob` | 2026-05-10 | — | — | Bronze table bronze.mpob does not exist. |
| `ndvi_charts` | `bronze.ndvi_data` | 2026-05-19 | — | — | Bronze table bronze.ndvi_data does not exist. |
| `usda_ers_feed_grains` | `bronze.feed_grains` | 2026-05-20 | — | — | Bronze table bronze.feed_grains does not exist. |
| `usda_ers_oil_crops` | `bronze.oil_crops` | 2026-05-20 | — | — | Bronze table bronze.oil_crops does not exist. |
| `usda_ers_wheat` | `bronze.usda_ers_wheat` | 2026-05-20 | — | — | Bronze table bronze.usda_ers_wheat does not exist. |
| `yield_forecast` | `bronze.yield_forecast` | 2026-05-19 | — | — | Bronze table bronze.yield_forecast does not exist. |

## 🟡 STALE — 1 collectors

| Collector | Bronze table | Last run | Bronze latest | Rows | Notes |
|-----------|--------------|----------|---------------|------|-------|
| `usda_nass_production` | `bronze.nass_production` | 2026-05-12 | 2026-01-28 | 52 | Bronze data 113d old. |

## ❓ UNKNOWN — 1 collectors

| Collector | Bronze table | Last run | Bronze latest | Rows | Notes |
|-----------|--------------|----------|---------------|------|-------|
| `conab` | `bronze.conab_production` | 2026-05-10 | — | 7,255 | No identifiable date column in bronze.conab_production. |

## ✅ HEALTHY — 6 collectors

| Collector | Bronze table | Last run | Bronze latest | Rows | Notes |
|-----------|--------------|----------|---------------|------|-------|
| `epa_echo_biodiesel` | `bronze.epa_echo_facility` | 2026-05-21 | 2026-05-21 | 2,864 |  |
| `epa_echo_ethanol` | `bronze.epa_echo_facility` | 2026-05-21 | 2026-05-21 | 2,864 |  |
| `epa_echo_milling` | `bronze.epa_echo_facility` | 2026-05-21 | 2026-05-21 | 2,864 |  |
| `epa_echo_oilseed` | `bronze.epa_echo_facility` | 2026-05-21 | 2026-05-21 | 2,864 |  |
| `usda_ams_ddgs` | `bronze.ams_price_record` | 2026-05-15 | 2026-05-20 | 68,341 |  |
| `usda_ams_feedstocks` | `bronze.ams_price_record` | 2026-05-15 | 2026-05-20 | 68,341 |  |

---

## Recommended actions (priority order)

1. **SILENT FAILURE collectors** — these are the AMS/CFTC bug. Patch each with a `collect()` override that calls `save_to_bronze`. Highest-priority targets:
   - `usda_nass_crop_progress` (Runs report success but bronze data is 177d old.)

2. **NEVER RUNS** — investigate why the scheduler isn't firing. Could be: not in master_scheduler RELEASE_SCHEDULES, schedule conditions never satisfied, or disabled flag set.

3. **EMPTY BRONZE** — collector runs but bronze table is empty. Either the API is empty or the save layer is broken.

4. **STALE** — accept-or-investigate. Some sources (CONAB monthly, MPOB monthly) are naturally slow-cadence and ~30-day staleness is normal. Cross-check the source's official release frequency before treating as a bug.
