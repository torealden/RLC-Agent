"""Check what we have for CARB pathway CI + LCFS baseline data."""
import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor(cursor_factory=RealDictCursor)

print("=== tables LIKE %lcfs%, %carb%, %feedstock% ===")
cur.execute("""SELECT table_schema, table_name FROM information_schema.tables
WHERE table_name ILIKE '%lcfs%' OR table_name ILIKE '%carb%' OR table_name ILIKE '%feedstock%'
ORDER BY 1,2""")
for r in cur.fetchall(): print(f"  {r['table_schema']}.{r['table_name']}")

print("\n=== silver.lcfs_pathway_ci_summary (if exists) ===")
try:
    cur.execute("""SELECT feedstock_category, fuel_type,
        COUNT(*) AS n_pathways,
        ROUND(AVG(ci_score)::numeric, 2) AS avg_ci,
        ROUND(MIN(ci_score)::numeric, 2) AS min_ci,
        ROUND(MAX(ci_score)::numeric, 2) AS max_ci
    FROM silver.lcfs_pathway_ci_summary
    GROUP BY 1,2 ORDER BY 1,2""")
    for r in cur.fetchall():
        print(f"  {r['feedstock_category']!s:<25s} {r['fuel_type']!s:<8s} n={r['n_pathways']!s:<4s}  CI avg={r['avg_ci']}  range=[{r['min_ci']}, {r['max_ci']}]")
except Exception as e:
    print(f"  error: {e}")
    conn.rollback()

print("\n=== bronze.carb_lcfs_pathways  fuel_type counts ===")
try:
    cur.execute("""SELECT fuel_type, status, COUNT(*) FROM bronze.carb_lcfs_pathways
    GROUP BY 1,2 ORDER BY 1,2""")
    for r in cur.fetchall(): print(f"  {r['fuel_type']!s:<10s} {r['status']!s:<10s} n={r['count']}")
except Exception as e:
    print(f"  error: {e}")
    conn.rollback()

conn.close()
