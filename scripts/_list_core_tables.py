import os, sys
print("START", flush=True)
from dotenv import load_dotenv; load_dotenv()
print("env", flush=True)
import psycopg2
print("psycopg2 imported", flush=True)
conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
print("connected", flush=True)
cur = conn.cursor()
cur.execute("SELECT current_database(), current_schema(), version()")
print("ctx:", cur.fetchone(), flush=True)
cur.execute("SELECT schema_name FROM information_schema.schemata ORDER BY 1")
print("all schemas:", [r[0] for r in cur.fetchall()], flush=True)
# Which schema has kg_callable?
cur.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_name='kg_callable'")
print("kg_callable lives in:", cur.fetchall(), flush=True)
conn.close()
print("DONE", flush=True)
