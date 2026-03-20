# Appendix D: Troubleshooting Guide

[← Back to Table of Contents](00_COVER_AND_TOC.md)

---

## Quick Diagnostic

Run this first to identify common issues:

```bash
python scripts/verify_setup.py
```

---

## Connection Issues

### "Connection refused" or "Connection timed out"

**Symptoms:**
- Cannot connect to database
- Dashboard fails to load
- Collectors fail immediately

**Causes & Solutions:**

| Check | Command | Solution |
|-------|---------|----------|
| Host is correct | `echo %RLC_PG_HOST%` | Verify in `.env` |
| Host is reachable | `ping your-host.com` | Check network/VPN |
| Port is open | `telnet host 5432` | Check firewall |
| Credentials correct | Test with psql | Update `.env` |

### "Authentication failed"

**Symptoms:**
- Error: `password authentication failed for user`
- Error: `FATAL: no pg_hba.conf entry`

**Solutions:**
1. Verify username and password in `.env`
2. Check if password contains special characters (may need escaping)
3. Confirm your IP is allowed in database firewall rules
4. Contact database admin to verify account

### "SSL required" or "SSL connection error"

**Symptoms:**
- Works locally but fails remotely
- Error mentions SSL/TLS

**Solution:**
Add SSL mode to connection string:
```ini
DATABASE_URL=postgresql://user:pass@host:5432/db?sslmode=require
```

---

## Collection Issues

### Collector runs but no data appears

**Check sequence:**

1. **Verify collector ran successfully:**
   ```bash
   # Check latest log
   Get-ChildItem logs/ | Sort-Object LastWriteTime -Descending | Select-Object -First 5
   type logs/your_collector_latest.log | Select-Object -Last 50
   ```

2. **Check Bronze table:**
   ```sql
   SELECT COUNT(*), MAX(collected_at)
   FROM bronze.your_table
   WHERE collected_at > NOW() - INTERVAL '1 day';
   ```

3. **Check Silver transformation:**
   - Did ETL run?
   - Are there series entries for this source?

### API Rate Limit Errors (429)

**Symptoms:**
- Error: `429 Too Many Requests`
- Collector fails after some records

**Solutions:**
1. BaseCollector handles this automatically (exponential backoff)
2. If persistent, increase `rate_limit_delay` in collector
3. Check if API has daily limits

### API Authentication Errors (401/403)

**Symptoms:**
- Error: `401 Unauthorized`
- Error: `403 Forbidden`

**Solutions:**
1. Verify API key is correct in `.env`
2. Check if key has expired
3. Verify key has required permissions
4. Generate new key if needed

### Collector Timeout

**Symptoms:**
- Collector hangs
- Error: `TimeoutError` or `ReadTimeout`

**Solutions:**
1. Check if API endpoint is responsive (try in browser)
2. Increase timeout in collector:
   ```python
   response = self.make_request(url, timeout=120)
   ```
3. Check network connectivity

---

## Dashboard Issues

### "Relation does not exist"

**Symptoms:**
- Dashboard loads but shows errors
- Error mentions missing table/view

**Causes:**
- Database schema not fully initialized
- Connected to wrong database
- View was dropped

**Solutions:**
1. Verify database name in `.env`
2. Check that view exists:
   ```sql
   SELECT table_name FROM information_schema.views
   WHERE table_schema = 'core';
   ```
3. Run missing migrations

### Dashboard loads but shows no data

**Check:**
1. Is there data in the database?
   ```sql
   SELECT COUNT(*) FROM silver.observation;
   ```
2. Have collections run?
   Check Operations Dashboard "Data Freshness" section
3. Are date filters excluding data?

### "Cannot connect to database" in Dashboard

**Solutions:**
1. Verify `dashboards/ops/.env` exists
2. Check credentials match main `.env`
3. Test connection:
   ```python
   python -c "from dashboards.ops.db import get_connection; print('OK')"
   ```

---

## Data Quality Issues

### Duplicate records

**Detection:**
```sql
SELECT release_id, table_id, row_id, column_id, COUNT(*)
FROM bronze.wasde_cell
GROUP BY 1,2,3,4
HAVING COUNT(*) > 1;
```

**Solution:**
```sql
-- Remove duplicates, keeping oldest
DELETE FROM bronze.wasde_cell a
USING bronze.wasde_cell b
WHERE a.id > b.id
  AND a.release_id = b.release_id
  AND a.table_id = b.table_id
  AND a.row_id = b.row_id
  AND a.column_id = b.column_id;
```

### Missing data for expected date

**Check collection logs:**
```bash
# Find logs for the date
Get-ChildItem logs/ -Filter "*2025-03-14*"
```

**Check if source released data:**
- Visit source website
- Check release calendar

### Data values seem wrong

**Verification steps:**
1. Compare to source website
2. Check units (MT vs bushels?)
3. Check if value is revision vs. original
4. Query Bronze to see raw value:
   ```sql
   SELECT raw_json FROM bronze.your_table WHERE id = 12345;
   ```

---

## Power BI Issues

### "Unable to connect to database"

**Solutions:**
1. Install PostgreSQL ODBC driver if missing
2. Use correct format: `host:port` (not `host, port`)
3. Try Import mode instead of DirectQuery
4. Check firewall allows Power BI

### Data not refreshing

**Check:**
1. Is scheduled refresh configured?
2. Is gateway online (if using on-prem)?
3. Have credentials expired?
4. Check refresh history for errors

### Slow queries

**Solutions:**
1. Switch to Import mode
2. Use Gold views (pre-aggregated)
3. Add date filters to reduce data volume
4. Avoid complex DAX on large tables

---

## Python Environment Issues

### ModuleNotFoundError

**Solution:**
```bash
pip install -r requirements.txt
```

### Version conflicts

**Solution:**
```bash
# Create fresh virtual environment
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Import errors after Git pull

**Solution:**
```bash
# Reinstall packages
pip install -r requirements.txt --upgrade
```

---

## Emergency Contacts

| Issue Type | Contact | Method |
|------------|---------|--------|
| Database access | Database Admin | Slack #data-platform |
| API key issues | Data Team Lead | Email |
| Critical data issues | On-call engineer | PagerDuty |

---

## Diagnostic Commands Summary

```bash
# Check environment
python scripts/verify_setup.py

# Test database
python -c "from dashboards.ops.db import get_connection; print('OK')"

# Check .env values
python -c "from dotenv import load_dotenv; load_dotenv(); import os; print(os.getenv('RLC_PG_HOST'))"

# View recent logs
Get-ChildItem logs/ | Sort-Object LastWriteTime -Descending | Select-Object -First 10

# Check Python packages
pip list | findstr "pandas sqlalchemy streamlit"

# Test collector (dry run)
python scripts/collect.py --collector cftc_cot --dry-run
```

---

[← Back to Table of Contents](00_COVER_AND_TOC.md) | [Next: Graphic Specifications →](APPENDIX_E_GRAPHICS.md)
