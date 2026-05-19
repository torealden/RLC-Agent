"""What is the dispatcher doing right now? Any runs in 'running' status?"""
import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2

conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor()

print("=== runs currently in 'running' status ===")
cur.execute("""SELECT collector_name, run_started_at, triggered_by
FROM core.collection_status
WHERE status = 'running'
ORDER BY run_started_at DESC LIMIT 20""")
rows = cur.fetchall()
if rows:
    for r in rows: print(f"  {r[0]}  started={r[1]}  by={r[2]}")
else:
    print("  (none)")

print("\n=== most recent 10 runs across all collectors ===")
cur.execute("""SELECT collector_name, run_started_at, status, rows_collected
FROM core.collection_status
ORDER BY run_started_at DESC LIMIT 10""")
for r in cur.fetchall(): print(f"  {r[1]}  {r[0]:<35s}  {r[2]:<8s}  rows={r[3]}")

conn.close()
