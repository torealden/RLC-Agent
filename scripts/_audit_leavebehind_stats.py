"""Verify every concrete stat in the Helios leave-behind doc is current."""
import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2

conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor()

probes = [
    ("bronze tables",       "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='bronze'"),
    ("silver tables",       "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='silver'"),
    ("gold views",          "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='gold'"),
    ("KG nodes",            "SELECT COUNT(*) FROM core.kg_node"),
    ("KG edges",            "SELECT COUNT(*) FROM core.kg_edge"),
    ("KG contexts",         "SELECT COUNT(*) FROM core.kg_context"),
    ("KG sources",          "SELECT COUNT(*) FROM core.kg_source"),
    ("bronze.cftc_cot rows",        "SELECT COUNT(*) FROM bronze.cftc_cot"),
    ("CARB LCFS pathways",          "SELECT COUNT(*) FROM bronze.carb_lcfs_pathways"),
    ("silver.facility_map facilities", "SELECT COUNT(*) FROM silver.facility_map"),
    ("silver.weather_observation rows", "SELECT COUNT(*) FROM silver.weather_observation"),
]
print("=== Live counts ===")
for label, sql in probes:
    try:
        cur.execute(sql)
        n = cur.fetchone()[0]
        print(f"  {label:<40s} {n:>10,}")
    except Exception as e:
        print(f"  {label:<40s} ERROR: {type(e).__name__}: {str(e)[:80]}")
        conn.rollback()

# Rail segments — table name unknown, try a couple
print("\n=== Rail probes ===")
for cand in ['silver.rail_segments', 'bronze.rail_segments', 'reference.rail_segments',
             'silver.ntad_rail_network', 'bronze.ntad_rail_network']:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {cand}")
        print(f"  {cand:<35s} {cur.fetchone()[0]:>10,}")
    except Exception:
        conn.rollback()

# Collectors registered (from collector_registry or master_scheduler)
print("\n=== Distinct collector_name values in core.collection_status ===")
cur.execute("SELECT COUNT(DISTINCT collector_name) FROM core.collection_status")
print(f"  ever-seen collector names: {cur.fetchone()[0]}")

conn.close()
