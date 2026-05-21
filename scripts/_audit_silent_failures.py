"""
Per-collector freshness audit — checks the 28 "no persist code" suspects
flagged in earlier audit.

For each collector, looks up the recent collection_status runs AND the
actual latest data in its bronze target table. Three diagnostic states:

  ✅ HEALTHY     — recent run + bronze updated within expected window
  🟡 STALE       — collector running but bronze data is older than the
                   collector frequency suggests
  ⚠️  SILENT     — runs report success but bronze hasn't changed = the
                   AMS / CFTC pattern. THIS is the silent-failure bug.
  ❌ NEVER       — no successful run in recent history

Reports as a markdown punch list at docs/specs/silent_failure_audit.md.
"""
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv
load_dotenv()
import psycopg2
from psycopg2.extras import RealDictCursor

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUT = PROJECT_ROOT / "docs" / "specs" / "silent_failure_audit.md"

# Suspects from earlier audit (28 collectors with no persist code in class file)
SUSPECTS = {
    'anec': 'bronze.anec',
    'cme_settlements': 'bronze.cme_settlements',
    'conab': 'bronze.conab_production',
    'eia_ethanol': 'bronze.eia_ethanol',
    'eia_petroleum': 'bronze.eia_petroleum',
    'epa_echo_biodiesel': 'bronze.epa_echo_facility',
    'epa_echo_ethanol': 'bronze.epa_echo_facility',
    'epa_echo_milling': 'bronze.epa_echo_facility',
    'epa_echo_oilseed': 'bronze.epa_echo_facility',
    'epa_rfs': 'bronze.epa_rfs_rin_generation',
    'futures_overnight': 'bronze.futures_prices',
    'futures_settlement': 'bronze.cme_settlements',
    'futures_us_session': 'bronze.futures_prices',
    'gefs_ensemble': 'bronze.gefs_ensemble',
    'gfs_forecast': 'bronze.gfs_forecast',
    'mpob': 'bronze.mpob',
    'ndvi_charts': 'bronze.ndvi_data',
    'usda_ams_ddgs': 'bronze.ams_price_record',
    'usda_ams_feedstocks': 'bronze.ams_price_record',
    'usda_ers_feed_grains': 'bronze.feed_grains',
    'usda_ers_oil_crops': 'bronze.oil_crops',
    'usda_ers_wheat': 'bronze.usda_ers_wheat',
    'usda_nass_acreage': 'bronze.nass_acreage',
    'usda_nass_crop_progress': 'bronze.nass_crop_progress',
    'usda_nass_production': 'bronze.nass_production',
    'usda_nass_stocks': 'bronze.nass_stocks',
    'yield_forecast': 'bronze.yield_forecast',
}

conn = psycopg2.connect(
    host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT", "5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"),
    user=os.getenv("RLC_PG_USER"), password=os.getenv("RLC_PG_PASSWORD"),
    sslmode="require",
)
cur = conn.cursor(cursor_factory=RealDictCursor)


def date_col_for(table_fqn: str) -> str | None:
    """Find a reasonable 'data freshness' column for a bronze table."""
    sch, t = table_fqn.split('.')
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
    """, (sch, t))
    cols = {r['column_name'] for r in cur.fetchall()}
    # Prefer in priority order
    for c in ['report_date', 'week_ending', 'observation_date',
              'collected_at', 'period_start', 'period', 'as_of_date',
              'date', 'forecast_date', 'data_date']:
        if c in cols:
            return c
    return None


def table_exists(table_fqn: str) -> bool:
    sch, t = table_fqn.split('.')
    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema=%s AND table_name=%s
    """, (sch, t))
    return cur.fetchone() is not None


def audit_collector(name: str, bronze_table: str) -> dict:
    """Run all probes for one collector."""
    out = {
        'collector': name,
        'bronze_table': bronze_table,
        'recent_runs': 0,
        'last_run_status': None,
        'last_run_at': None,
        'rows_collected_last': None,
        'bronze_latest_data': None,
        'bronze_total_rows': None,
        'verdict': None,
        'notes': '',
    }

    # 1. Recent collection_status runs (last 30 days)
    cur.execute("""
        SELECT status, run_started_at, rows_collected, error_message
        FROM core.collection_status
        WHERE collector_name = %s
          AND run_started_at > NOW() - INTERVAL '30 days'
        ORDER BY run_started_at DESC LIMIT 1
    """, (name,))
    last = cur.fetchone()
    if last:
        out['last_run_status'] = last['status']
        out['last_run_at'] = last['run_started_at']
        out['rows_collected_last'] = last['rows_collected']

    cur.execute("""
        SELECT COUNT(*) AS n FROM core.collection_status
        WHERE collector_name=%s AND run_started_at > NOW() - INTERVAL '30 days'
    """, (name,))
    out['recent_runs'] = cur.fetchone()['n']

    # 2. Bronze freshness
    if not table_exists(bronze_table):
        out['notes'] = f"Bronze table {bronze_table} does not exist."
        out['verdict'] = "❌ MISSING TABLE"
        return out

    date_col = date_col_for(bronze_table)
    if not date_col:
        out['notes'] = f"No identifiable date column in {bronze_table}."
        # Just get row count
        try:
            cur.execute(f"SELECT COUNT(*) AS n FROM {bronze_table}")
            out['bronze_total_rows'] = cur.fetchone()['n']
        except Exception:
            conn.rollback()
        out['verdict'] = "❓ UNKNOWN"
        return out

    try:
        cur.execute(f"SELECT MAX({date_col}) AS latest, COUNT(*) AS n FROM {bronze_table}")
        r = cur.fetchone()
        out['bronze_latest_data'] = r['latest']
        out['bronze_total_rows'] = r['n']
    except Exception as e:
        conn.rollback()
        out['notes'] = f"Query error on {date_col}: {str(e)[:100]}"
        out['verdict'] = "❓ UNKNOWN"
        return out

    # 3. Diagnose verdict
    if out['recent_runs'] == 0:
        out['verdict'] = "❌ NEVER RUNS"
        return out

    # Days since last bronze data point
    if out['bronze_latest_data'] is None:
        out['verdict'] = "❌ EMPTY BRONZE"
        return out

    latest = out['bronze_latest_data']
    if isinstance(latest, datetime):
        latest_d = latest.date() if hasattr(latest, 'date') else latest
    else:
        latest_d = latest
    age_days = (datetime.now().date() - (latest_d if hasattr(latest_d, 'year') else latest_d)).days

    # If runs are happening but bronze data is significantly older than the
    # interval between runs, it's the silent-failure pattern.
    if out['last_run_status'] == 'success' and age_days > 14 and out['recent_runs'] > 2:
        out['verdict'] = "⚠️ SILENT FAILURE"
        out['notes'] = f"Runs report success but bronze data is {age_days}d old."
    elif age_days > 14:
        out['verdict'] = "🟡 STALE"
        out['notes'] = f"Bronze data {age_days}d old."
    else:
        out['verdict'] = "✅ HEALTHY"

    return out


# Run audit
results = []
for collector, bronze_table in SUSPECTS.items():
    print(f"  auditing {collector}...")
    try:
        results.append(audit_collector(collector, bronze_table))
    except Exception as e:
        conn.rollback()
        print(f"    ERROR: {e}")
        results.append({
            'collector': collector, 'bronze_table': bronze_table,
            'verdict': "❓ AUDIT ERROR", 'notes': str(e)[:120],
        })

# Group by verdict
by_verdict: dict[str, list] = {}
for r in results:
    by_verdict.setdefault(r['verdict'], []).append(r)

# Write report
lines = []
P = lines.append
P("# Silent-Failure Audit — 28 suspect collectors")
P("")
P(f"*Generated {datetime.utcnow():%Y-%m-%d %H:%M UTC}.*")
P("")
P("Audit pattern: collectors with no obvious save_to_bronze in their class file were "
  "flagged as suspects. For each one, we look at: (a) whether the dispatcher has been "
  "running it in the last 30 days, and (b) whether its bronze target table has fresh data.")
P("")
P("**Diagnostic states:**")
P("- ✅ **HEALTHY** — runs + bronze data current within 14 days")
P("- 🟡 **STALE** — bronze data older than 14 days (could be normal for slow-cadence sources)")
P("- ⚠️  **SILENT FAILURE** — runs report success but bronze data is stale — the AMS / CFTC pattern")
P("- ❌ **NEVER RUNS / EMPTY BRONZE / MISSING TABLE** — straight broken")
P("- ❓ **UNKNOWN** — couldn't determine freshness from schema")
P("")

verdict_order = [
    "⚠️ SILENT FAILURE", "❌ NEVER RUNS", "❌ EMPTY BRONZE", "❌ MISSING TABLE",
    "🟡 STALE", "❓ UNKNOWN", "❓ AUDIT ERROR", "✅ HEALTHY",
]
for verdict in verdict_order:
    items = by_verdict.get(verdict, [])
    if not items:
        continue
    P(f"## {verdict} — {len(items)} collectors")
    P("")
    P("| Collector | Bronze table | Last run | Bronze latest | Rows | Notes |")
    P("|-----------|--------------|----------|---------------|------|-------|")
    for r in sorted(items, key=lambda x: x['collector']):
        last_run = (r['last_run_at'].strftime('%Y-%m-%d') if r.get('last_run_at')
                    else '—')
        bronze_latest = (
            r['bronze_latest_data'].strftime('%Y-%m-%d')
            if r.get('bronze_latest_data') and hasattr(r['bronze_latest_data'], 'strftime')
            else (str(r['bronze_latest_data']) if r.get('bronze_latest_data') else '—')
        )
        rows = f"{r['bronze_total_rows']:,}" if r.get('bronze_total_rows') is not None else '—'
        P(f"| `{r['collector']}` | `{r['bronze_table']}` | {last_run} | {bronze_latest} | {rows} | {r.get('notes','')[:80]} |")
    P("")

P("---")
P("")
P("## Recommended actions (priority order)")
P("")
P("1. **SILENT FAILURE collectors** — these are the AMS/CFTC bug. Patch each with a `collect()` override that calls `save_to_bronze`. Highest-priority targets:")
for r in by_verdict.get("⚠️ SILENT FAILURE", []):
    P(f"   - `{r['collector']}` ({r['notes']})")
P("")
P("2. **NEVER RUNS** — investigate why the scheduler isn't firing. Could be: not in master_scheduler RELEASE_SCHEDULES, schedule conditions never satisfied, or disabled flag set.")
P("")
P("3. **EMPTY BRONZE** — collector runs but bronze table is empty. Either the API is empty or the save layer is broken.")
P("")
P("4. **STALE** — accept-or-investigate. Some sources (CONAB monthly, MPOB monthly) are naturally slow-cadence and ~30-day staleness is normal. Cross-check the source's official release frequency before treating as a bug.")
P("")

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text("\n".join(lines), encoding="utf-8")
print()
print(f"Wrote: {OUT}")
print(f"Verdict summary:")
for v in verdict_order:
    if by_verdict.get(v):
        print(f"  {v:<30s}  {len(by_verdict[v])}")
conn.close()
