"""Stand up Felipe's DB sandbox (user-guide §8 / curriculum §2a). Idempotent.

Creates sandbox_{bronze,silver,gold,reference} schemas + a restricted `felipe` LOGIN role that can
WRITE only to sandbox_* and READ-ONLY production — so a learner + an LLM physically cannot corrupt
production. Copies the reference layer into sandbox_reference so Felipe can modify config for his
country. Password is taken from env FELIPE_PW (never hardcoded/committed); rotate via ALTER ROLE.

Run: FELIPE_PW=... python scripts/setup_felipe_sandbox.py
"""
import os, sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

SANDBOX = ['sandbox_bronze', 'sandbox_silver', 'sandbox_gold', 'sandbox_reference']
PROD_READ = ['bronze', 'silver', 'gold', 'reference', 'core']

def main():
    pw = os.environ["FELIPE_PW"]
    with get_connection() as c:
        cur = c.cursor()
        for s in SANDBOX:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname='felipe'")
        cur.execute(("ALTER" if cur.fetchone() else "CREATE") + " ROLE felipe LOGIN PASSWORD %s", (pw,))
        for s in SANDBOX:
            cur.execute(f"GRANT USAGE, CREATE ON SCHEMA {s} TO felipe")
            cur.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA {s} TO felipe")
            cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {s} GRANT ALL ON TABLES TO felipe")
        for s in PROD_READ:  # read-only production: templates + reference, no write
            cur.execute(f"GRANT USAGE ON SCHEMA {s} TO felipe")
            cur.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {s} TO felipe")
            cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {s} GRANT SELECT ON TABLES TO felipe")
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='reference'")
        for t in [r['tablename'] for r in cur.fetchall()]:
            cur.execute(f'DROP TABLE IF EXISTS sandbox_reference."{t}"')
            cur.execute(f'CREATE TABLE sandbox_reference."{t}" AS TABLE reference."{t}"')
        cur.execute("GRANT ALL ON ALL TABLES IN SCHEMA sandbox_reference TO felipe")
        c.commit()
        print(f"OK: {SANDBOX} + felipe role (write sandbox, read-only prod) + reference copied")

if __name__ == "__main__":
    main()
