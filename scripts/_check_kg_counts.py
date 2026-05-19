"""Current KG counts — nodes, edges, contexts, sources, callables."""
import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2

conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor()

for label, sql in [
    ("kg_node",      "SELECT COUNT(*) FROM core.kg_node"),
    ("kg_edge",      "SELECT COUNT(*) FROM core.kg_edge"),
    ("kg_context",   "SELECT COUNT(*) FROM core.kg_context"),
    ("kg_source",    "SELECT COUNT(*) FROM core.kg_source"),
    ("kg_callable",  "SELECT COUNT(*) FROM core.kg_callable"),
]:
    try:
        cur.execute(sql)
        print(f"  {label:<14s}  n={cur.fetchone()[0]}")
    except Exception as e:
        print(f"  {label:<14s}  ERROR {type(e).__name__}: {e}")
        conn.rollback()

# Node-type breakdown
print("\n=== node_type breakdown ===")
try:
    cur.execute("""SELECT node_type, COUNT(*) FROM core.kg_node
    GROUP BY node_type ORDER BY 2 DESC""")
    for r in cur.fetchall():
        print(f"  {r[0]!s:<28s} n={r[1]}")
except Exception as e:
    print(f"  ERROR: {e}")

conn.close()
