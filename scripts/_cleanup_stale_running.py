"""Clean up orphan 'running' entries in core.collection_status from prior dispatcher crashes."""
import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2

conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor()

# Mark any 'running' row > 1h old as failed (it never finished)
cur.execute("""UPDATE core.collection_status
SET status = 'failed', run_finished_at = NOW(),
    error_message = 'Marked failed by cleanup: stranded "running" >1h'
WHERE status = 'running'
  AND run_started_at < NOW() - INTERVAL '1 hour'
RETURNING id, collector_name, run_started_at""")
rows = cur.fetchall()
print(f"Cleaned {len(rows)} stale 'running' entries:")
for r in rows: print(f"  id={r[0]}  {r[1]}  started={r[2]}")
conn.commit()
conn.close()
