"""Tidy bronze.ers_food_sales_monthly -> silver.food_expenditure (UCO collection proxy driver).

Bronze encodes outlet AND unit in the outlet_type string. Parse into (category FAH/FAFH, outlet,
unit nominal/real). FAFH (restaurant fryer-oil activity) is the collection driver; real (constant
1988$) strips inflation so it tracks physical activity. This is vocabulary-independent plumbing —
Desktop's methodology consumes it but the tidy series is stable.
"""
import re, sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

DDL = """
CREATE TABLE IF NOT EXISTS silver.food_expenditure (
    period date NOT NULL, year int NOT NULL, month int NOT NULL,
    category text NOT NULL,   -- FAH | FAFH
    outlet text NOT NULL,     -- total | full_service | limited_service | other | grocery | warehouse
    unit text NOT NULL,       -- nominal | real
    value_mil_usd numeric,
    loaded_at timestamptz DEFAULT now(),
    PRIMARY KEY (period, category, outlet, unit)
);
"""

def parse(ot):
    """outlet_type string -> (category, outlet, unit) or None to skip."""
    s = (ot or '').lower()
    unit = 'real' if 'constant' in s else 'nominal'
    if 'away-from-home' in s or 'restaurant' in s:
        cat = 'FAFH'
    elif 'at-home' in s or 'grocery' in s or 'warehouse' in s:
        cat = 'FAH'
    else:
        return None  # 'Total food sales' aggregate — skip (FAH+FAFH)
    if 'full-service' in s:      outlet = 'full_service'
    elif 'limited-service' in s: outlet = 'limited_service'
    elif s.startswith('total') or 'total food-a' in s: outlet = 'total'
    elif 'grocery' in s:         outlet = 'grocery'
    elif 'warehouse' in s:       outlet = 'warehouse'
    elif 'other' in s:           outlet = 'other'
    else:                        outlet = 'total'
    return cat, outlet, unit

with get_connection() as c:
    cur = c.cursor()
    cur.execute(DDL)
    cur.execute("TRUNCATE silver.food_expenditure")
    cur.execute("""SELECT year, month, outlet_type, sales_value FROM bronze.ers_food_sales_monthly
                   WHERE sales_value IS NOT NULL AND month BETWEEN 1 AND 12""")
    n = 0; skipped = 0
    for r in cur.fetchall():
        p = parse(r['outlet_type'])
        if not p:
            skipped += 1; continue
        cat, outlet, unit = p
        try:
            y, m = int(r['year']), int(r['month']); val = float(r['sales_value'])
        except (TypeError, ValueError):
            continue
        cur.execute("""INSERT INTO silver.food_expenditure (period, year, month, category, outlet, unit, value_mil_usd)
            VALUES (make_date(%s,%s,1),%s,%s,%s,%s,%s,%s)
            ON CONFLICT (period, category, outlet, unit) DO UPDATE SET value_mil_usd=EXCLUDED.value_mil_usd""",
            (y, m, y, m, cat, outlet, unit, val))
        n += 1
    c.commit()
    print(f"silver.food_expenditure: {n} rows ({skipped} total-food aggregates skipped)")
    # sanity: FAFH total real, recent + coverage
    cur.execute("""SELECT category, outlet, unit, count(*) nrows, min(year) mn, max(year) mx
                   FROM silver.food_expenditure WHERE outlet IN ('total','full_service','limited_service') GROUP BY 1,2,3 ORDER BY 1,2,3""")
    for r in cur.fetchall():
        print(f"  {r['category']:5} {r['outlet']:15} {r['unit']:8} {r['nrows']:4} rows {r['mn']}-{r['mx']}")
    cur.execute("""SELECT year, round(avg(value_mil_usd),0) v FROM silver.food_expenditure
                   WHERE category='FAFH' AND outlet='total' AND unit='real' AND year IN (2019,2020,2024) GROUP BY 1 ORDER BY 1""")
    print("FAFH total real avg/mo ($M 1988):", [(r['year'], float(r['v'])) for r in cur.fetchall()])
