"""Map Abiove bronze → silver.monthly_realized (the six Brazil soy-complex series).

Lands Abiove monthly data as country='BR', source='ABIOVE', unit='1000 MT',
commodity='soybeans' (meal/oil carried as attributes, matching the US convention
in monthly_realized). Brazil is calendar-native → marketing_year = calendar_year
(analyst picks MY framing downstream; see reference_brazil_my_alignment).

Canonical bronze tab per series (avoids double-counting overlapping tabs):
  crush                → Tabela          (2012+, dedicated monthly crush history)
  meal_production      → Balanco_Brasil  (meal, production)   [2025+ only in this file]
  oil_production_crude → Balanco_Brasil  (oil, production)    [2025+ only in this file]
  seed_stocks (NEW)    → Estoques_Finais (soybeans final_stock, 2021+)
  meal_stocks          → Estoques_Finais (meal final_stock, 2021+)
  oil_stocks           → Estoques_Finais (oil final_stock, 2021+)

is_preliminary <- bronze.is_projection (Abiove "(amostra)" sample months).

Usage:  python scripts/build_silver_abiove_monthly.py
"""
import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

# (bronze.commodity, bronze.attribute, bronze.source_tab) -> monthly_realized.attribute
MAPPING = [
    ("soybeans", "crush",       "Tabela",          "crush"),
    ("meal",     "production",   "Balanco_Brasil",  "meal_production"),
    ("oil",      "production",   "Balanco_Brasil",  "oil_production_crude"),
    ("soybeans", "final_stock",  "Estoques_Finais", "seed_stocks"),
    ("meal",     "final_stock",  "Estoques_Finais", "meal_stocks"),
    ("oil",      "final_stock",  "Estoques_Finais", "oil_stocks"),
]

UPSERT = """
INSERT INTO silver.monthly_realized
  (commodity, country, marketing_year, month, calendar_year, attribute,
   realized_value, unit, source, is_preliminary, collected_at)
SELECT 'soybeans', 'BR', b.year, b.month, b.year, %(tgt)s,
       b.value_1000t, '1000 MT', 'ABIOVE', b.is_projection, NOW()
FROM bronze.abiove_soy_complex b
WHERE b.frequency='monthly' AND b.month IS NOT NULL
  AND b.commodity=%(comm)s AND b.attribute=%(attr)s AND b.source_tab=%(tab)s
  AND b.value_1000t IS NOT NULL
ON CONFLICT (commodity, country, marketing_year, month, attribute, source)
DO UPDATE SET realized_value=EXCLUDED.realized_value, unit=EXCLUDED.unit,
   is_preliminary=EXCLUDED.is_preliminary, calendar_year=EXCLUDED.calendar_year,
   collected_at=NOW()
"""

def main():
    with get_connection() as conn:
        cur = conn.cursor()
        total = 0
        for comm, attr, tab, tgt in MAPPING:
            cur.execute(UPSERT, {"comm": comm, "attr": attr, "tab": tab, "tgt": tgt})
            total += cur.rowcount
            print(f"  {tgt:22} <- {tab:16} ({comm}/{attr})  rows={cur.rowcount}")
        conn.commit()
        print(f"\nUpserted {total} rows into silver.monthly_realized (country=BR, source=ABIOVE)")
        cur.execute("""SELECT attribute, count(*) n, min(calendar_year*100+month) mn,
                              max(calendar_year*100+month) mx,
                              count(*) FILTER (WHERE is_preliminary) prelim
                       FROM silver.monthly_realized WHERE source='ABIOVE'
                       GROUP BY attribute ORDER BY attribute""")
        print("\nsilver ABIOVE coverage:")
        for r in cur.fetchall():
            print(f"  {r['attribute']:22} n={r['n']:3}  {r['mn']} -> {r['mx']}  (prelim {r['prelim']})")

if __name__ == "__main__":
    main()
