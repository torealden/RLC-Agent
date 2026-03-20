# Part 3: Daily Operations

[← Back to Table of Contents](00_COVER_AND_TOC.md) | [← Previous: Understanding the Data](02_UNDERSTANDING_DATA.md)

---

## 3.1 The Operations Dashboard

The Operations Dashboard provides real-time visibility into system health and data collection status.

### Launching the Dashboard

**Option 1: Double-click the launcher**
```
scripts\launch_data_dashboard.bat
```

**Option 2: Command line**
```bash
cd C:\dev\RLC-Agent
streamlit run dashboards/ops/app.py
```

The dashboard opens in your default browser at `http://localhost:8501`.

### Dashboard Components

**[GRAPHIC: Annotated Dashboard Screenshot]**
*See [Appendix E](APPENDIX_E_GRAPHICS.md#dashboard-annotated) for graphic specifications*

#### 1. Health Score Banner

The health score (0-100) provides an at-a-glance system status:

| Score Range | Status | Meaning |
|-------------|--------|---------|
| 80-100 | 🟢 Healthy | All systems normal |
| 50-79 | 🟡 Warning | Some issues need attention |
| 0-49 | 🔴 Critical | Immediate attention required |

**Health Score Calculation:**
- Starts at 100
- -5 points per overdue data source
- -10 points per failed collection (last 24h)
- -2 points per stale source (>24h old but not overdue)

#### 2. Summary Metrics

| Metric | Description |
|--------|-------------|
| **Sources OK** | Data sources collected within expected timeframe |
| **Overdue** | Sources past their expected refresh time |
| **Failed (24h)** | Collection attempts that failed in last 24 hours |
| **Stale** | Sources not refreshed in >24 hours (but not formally overdue) |

#### 3. Data Freshness Table

Shows the status of each data source:

| Column | Description |
|--------|-------------|
| Collector Name | Internal identifier |
| Display Name | Human-readable name |
| Last Collected | Timestamp of last successful collection |
| Hours Since | Time elapsed since last collection |
| Expected Frequency | How often this source should update |
| Last Status | Success/Failed |
| Status | 🟢 OK / 🟡 Stale / 🔴 Overdue / ⚪ Never |

#### 4. Active Alerts

Unacknowledged warnings and errors requiring attention:

| Priority | Icon | Response |
|----------|------|----------|
| 1 (Critical) | 🔴 | Immediate action required |
| 2 (Warning) | 🟠 | Review within 24 hours |
| 3 (Info) | 🟡 | Review when convenient |

Click an alert to expand details and see the full error message.

#### 5. Recent Failures

Lists collection failures from the last 7 days. Each entry shows:
- Collector name
- When it failed
- Duration before failure
- Error message

#### 6. Collection Trends (30 days)

A bar chart showing daily success/failure counts. Useful for identifying patterns:
- Consistent failures on specific days
- Gradual degradation
- Recovery after fixes

#### 7. Schedule Overview

Reference table of all configured collectors:
- Expected release times
- Collection frequencies
- Priority levels

### Auto-Refresh

The dashboard auto-refreshes every 5 minutes by default. Adjust in the sidebar:
- Minimum: 30 seconds
- Recommended: 300 seconds (5 minutes)

Click **Refresh Now** for immediate update.

---

## 3.2 Running Data Collections

### Automated Collection

The platform includes a scheduler that runs collections automatically based on configured schedules. When the scheduler is running, you don't need to manually trigger collections.

**Check if scheduler is running:**
Look for "Dispatcher: Running" or "Dispatcher: Stopped" in the dashboard sidebar.

### Manual Collection

For ad-hoc collection or testing, use the collection script:

```bash
cd C:\dev\RLC-Agent

# Run a specific collector
python scripts/collect.py --collector cftc_cot

# Run multiple collectors
python scripts/collect.py --collectors eia_ethanol eia_petroleum

# Run all collectors scheduled for today
python scripts/collect.py --today

# Run with verbose output
python scripts/collect.py --collector usda_nass --verbose
```

### Available Collectors

| Collector ID | Data Source | Typical Schedule |
|--------------|-------------|------------------|
| `cftc_cot` | CFTC Commitment of Traders | Friday 3:30 PM |
| `usda_nass` | USDA NASS Crop Progress | Monday 4:00 PM |
| `usda_fas` | USDA FAS Export Sales | Thursday 8:30 AM |
| `eia_ethanol` | EIA Ethanol | Wednesday 10:30 AM |
| `eia_petroleum` | EIA Petroleum | Wednesday 10:30 AM |
| `drought` | US Drought Monitor | Thursday 8:30 AM |
| `census_trade` | Census Bureau Trade | ~6th of month |
| `conab` | Brazil CONAB | ~14th of month |
| `cme_settlements` | CME Daily Settlements | Weekdays 5:00 PM |

### Collection Logs

Each collection run produces a log file in `logs/`:

```
logs/
├── cftc_cot_2025-03-14_153012.log
├── usda_nass_2025-03-11_160045.log
└── ...
```

Log files contain:
- API requests made
- Responses received (hashed for verification)
- Rows inserted/updated
- Any errors or warnings

---

## 3.3 Monitoring Data Quality

### Daily Monitoring Routine

**Morning check (5 minutes):**
1. Open Operations Dashboard
2. Review Health Score - should be 80+
3. Check for any 🔴 Overdue sources
4. Review Active Alerts
5. Note any failures from overnight

**Weekly review (15 minutes):**
1. Review Collection Trends chart for patterns
2. Check that weekly sources (CFTC, NASS) collected successfully
3. Review any recurring failures
4. Verify Gold views have recent data

### Data Validation Queries

**Check latest data timestamps:**
```sql
SELECT
    ds.name AS source,
    MAX(o.observation_time) AS latest_observation,
    MAX(ir.started_at) AS latest_collection
FROM silver.observation o
JOIN core.series s ON o.series_id = s.id
JOIN core.data_source ds ON s.data_source_code = ds.code
LEFT JOIN audit.ingest_run ir ON ir.data_source_code = ds.code
GROUP BY ds.name
ORDER BY latest_collection DESC;
```

**Check for data gaps:**
```sql
-- Find weeks with missing NASS data
WITH weeks AS (
    SELECT generate_series(
        '2024-01-01'::date,
        CURRENT_DATE,
        '1 week'::interval
    )::date AS week_start
)
SELECT w.week_start
FROM weeks w
LEFT JOIN silver.crop_progress cp
    ON cp.observation_date >= w.week_start
    AND cp.observation_date < w.week_start + 7
WHERE cp.id IS NULL
    AND w.week_start < CURRENT_DATE - 7;  -- Exclude current week
```

**Compare Bronze vs Silver row counts:**
```sql
SELECT
    'bronze.wasde_cell' AS table_name,
    COUNT(*) AS row_count
FROM bronze.wasde_cell
UNION ALL
SELECT
    'silver.observation (wasde)',
    COUNT(*)
FROM silver.observation o
JOIN core.series s ON o.series_id = s.id
WHERE s.data_source_code = 'wasde';
```

### Data Quality Indicators

| Indicator | Good | Warning | Problem |
|-----------|------|---------|---------|
| Collection success rate (7d) | >95% | 80-95% | <80% |
| Hours since collection | Within schedule | 1.5x expected | 2x+ expected |
| Row count trend | Stable/growing | Small dips | Large drops |
| Error rate in logs | <1% | 1-5% | >5% |

---

## 3.4 Troubleshooting Common Issues

### Issue: Collector Fails with "Connection Refused"

**Symptoms:**
- Error: `ConnectionRefusedError` or `timeout`
- Dashboard shows failed status

**Causes:**
- API endpoint is down
- Network connectivity issue
- Rate limiting

**Solutions:**
1. Check if the API is available (try URL in browser)
2. Wait 15-30 minutes and retry
3. Check for API status page announcements
4. If persistent, check firewall/proxy settings

### Issue: Collector Fails with "Authentication Error"

**Symptoms:**
- Error: `401 Unauthorized` or `403 Forbidden`
- Happens suddenly after working previously

**Causes:**
- API key expired or revoked
- API key quota exceeded
- Account issue

**Solutions:**
1. Verify API key in `.env` file
2. Check API provider's dashboard for quota/status
3. Regenerate API key if needed
4. Update `.env` and restart collector

### Issue: Data Not Appearing in Gold Views

**Symptoms:**
- Bronze has data
- Silver has data
- Gold view shows old or no data

**Causes:**
- View needs refresh (for materialized views)
- Filter excludes new data
- Join key mismatch

**Solutions:**
1. Check if view is materialized:
   ```sql
   SELECT matviewname FROM pg_matviews WHERE schemaname = 'gold';
   ```
2. Refresh materialized views:
   ```sql
   REFRESH MATERIALIZED VIEW gold.your_view_name;
   ```
3. Check view definition for date filters

### Issue: Dashboard Shows "Error loading data"

**Symptoms:**
- Dashboard loads but sections show errors
- Error mentions "relation does not exist"

**Causes:**
- Database schema not fully initialized
- Connection to wrong database
- Missing tables/views

**Solutions:**
1. Verify `.env` points to correct database
2. Check that required views exist:
   ```sql
   SELECT table_name FROM information_schema.views WHERE table_schema = 'core';
   ```
3. Run missing migration scripts if needed

### Issue: Duplicate Data Appearing

**Symptoms:**
- Same values appearing multiple times
- Aggregates higher than expected

**Causes:**
- Collector ran multiple times
- Idempotent write not working
- Missing unique constraint

**Solutions:**
1. Check Bronze table for duplicates:
   ```sql
   SELECT release_id, table_id, row_id, column_id, COUNT(*)
   FROM bronze.wasde_cell
   GROUP BY 1,2,3,4
   HAVING COUNT(*) > 1;
   ```
2. If found, deduplicate:
   ```sql
   DELETE FROM bronze.wasde_cell a
   USING bronze.wasde_cell b
   WHERE a.id > b.id
     AND a.release_id = b.release_id
     AND a.table_id = b.table_id;
   ```
3. Report to dev team to fix root cause

### Quick Diagnostic Commands

```bash
# Check database connection
python -c "from dashboards.ops.db import get_connection; print('OK')"

# Test specific collector
python scripts/collect.py --collector cftc_cot --dry-run

# View recent logs
Get-Content -Tail 50 logs/cftc_cot_*.log | Select-Object -Last 50

# Check Python environment
pip list | findstr "pandas sqlalchemy streamlit"
```

---

## Emergency Procedures

### Complete Collection Failure

If all collectors are failing:

1. **Check network connectivity**
   ```bash
   ping google.com
   curl -I https://api.census.gov
   ```

2. **Check database connectivity**
   ```bash
   python -c "from dashboards.ops.db import get_connection; print('OK')"
   ```

3. **Check environment variables**
   ```bash
   python -c "import os; print(os.getenv('RLC_PG_HOST', 'NOT SET'))"
   ```

4. **Restart services**
   - Close all Python processes
   - Restart command prompt
   - Re-run collectors

### Data Corruption Suspected

If you suspect data has been corrupted:

1. **Stop all collectors immediately**
2. **Document the issue** (timestamps, symptoms, affected tables)
3. **Contact database admin**
4. **Do NOT attempt manual fixes** without coordination

---

[← Previous: Understanding the Data](02_UNDERSTANDING_DATA.md) | [Next: Working with Power BI →](04_POWER_BI.md)
