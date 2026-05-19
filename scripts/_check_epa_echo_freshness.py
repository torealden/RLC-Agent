"""EPA ECHO freshness — bronze counts + recent run history."""
import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor(cursor_factory=RealDictCursor)

print("=== bronze.epa_echo_facility count by search_profile ===")
cur.execute("""SELECT search_profile, COUNT(*) AS n,
    MIN(collected_at) AS first_seen, MAX(collected_at) AS last_seen
FROM bronze.epa_echo_facility
GROUP BY search_profile ORDER BY search_profile""")
for r in cur.fetchall():
    print(f"  {r['search_profile']!s:<28s}  n={r['n']:>6d}  first={r['first_seen']}  last={r['last_seen']}")

print("\n=== recent core.collection_status runs for epa_echo_* ===")
cur.execute("""SELECT collector_name, run_started_at, status, rows_collected, error_message
FROM core.collection_status
WHERE collector_name LIKE 'epa_echo_%'
ORDER BY run_started_at DESC LIMIT 20""")
for r in cur.fetchall():
    print(f"  {r['run_started_at']}  {r['collector_name']:<22s}  {r['status']:<8s}  rows={r['rows_collected']}")
    if r['error_message']:
        print(f"    ERROR: {r['error_message'][:200]}")

conn.close()
