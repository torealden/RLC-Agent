"""All Minnesota facilities in our curated facility list, by industry_code.
For the bundled MPCA Information Request.
"""
import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor(cursor_factory=RealDictCursor)

# What columns does silver.facility_map have?
cur.execute("""SELECT column_name FROM information_schema.columns
WHERE table_schema='silver' AND table_name='facility_map' ORDER BY ordinal_position""")
cols = [r['column_name'] for r in cur.fetchall()]
print("=== silver.facility_map columns ===")
print(" ", ", ".join(cols))

# Quick MN facility list — pivot by industry_code
print("\n=== MN facility count by industry_code ===")
cur.execute("""SELECT industry_code, COUNT(*) FROM silver.facility_map
WHERE state = 'MN' GROUP BY industry_code ORDER BY 2 DESC""")
for r in cur.fetchall():
    print(f"  {r['industry_code']!s:<30s}  n={r['count']}")

# Full MN list
print("\n=== All MN facilities (sorted by industry_code, name) ===")
# Compose a SELECT picking sensible identifier columns
ident_cols = []
for c in ['name', 'operator', 'parent_company', 'industry_code',
         'city', 'status', 'status_normalized',
         'nameplate_mmgy', 'nameplate_mmbu_yr',
         'naics_codes', 'data_tier', 'source_table']:
    if c in cols:
        ident_cols.append(c)
sel = ', '.join(ident_cols)
cur.execute(f"""SELECT {sel} FROM silver.facility_map
WHERE state = 'MN' ORDER BY industry_code, {ident_cols[0]}""")
rows = cur.fetchall()
print(f"  total: {len(rows)} MN facilities")
for r in rows:
    parts = [f"{c}={r[c]!s}" for c in ident_cols if r.get(c) is not None]
    print(f"  - {' | '.join(parts)}")

conn.close()
