"""Check indexes + constraints on bronze.ams_price_record."""
import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor(cursor_factory=RealDictCursor)

print("=== All indexes on bronze.ams_price_record ===")
cur.execute("""SELECT indexname, indexdef FROM pg_indexes
WHERE schemaname='bronze' AND tablename='ams_price_record'
ORDER BY indexname""")
for r in cur.fetchall():
    print(f"\n  {r['indexname']}:")
    print(f"    {r['indexdef']}")

print("\n=== Constraints on bronze.ams_price_record ===")
cur.execute("""SELECT con.conname, pg_get_constraintdef(con.oid) AS def
FROM pg_constraint con
JOIN pg_class rel ON con.conrelid = rel.oid
JOIN pg_namespace ns ON rel.relnamespace = ns.oid
WHERE ns.nspname='bronze' AND rel.relname='ams_price_record'""")
for r in cur.fetchall():
    print(f"  {r['conname']}: {r['def']}")

# Now try to run an INSERT manually and see what happens
print("\n=== Try a test INSERT to see what actually happens ===")
import json
test_rec = {
    'slug_id': '__TEST__',
    'report_date': '2026-05-19',
    'report_section': 'TEST',
    'commodity': 'TEST',
    'location': 'TEST',
    'grade': None,
    'delivery_period': None,
    'delivery_point': None,
    'transaction_type': None,
    'product_type': None,
    'price': 1.0, 'price_low': None, 'price_high': None,
    'price_avg': None, 'price_mostly': None,
    'basis': None, 'basis_low': None, 'basis_high': None, 'basis_change': None,
    'volume': None, 'weight_avg': None, 'weight_low': None, 'weight_high': None,
    'unit': None, 'raw_record': json.dumps({"test": True}),
}
try:
    cur.execute("""
        INSERT INTO bronze.ams_price_record (
            slug_id, report_date, report_section, commodity, location, grade,
            delivery_period, delivery_point, transaction_type, product_type,
            price, price_low, price_high, price_avg, price_mostly,
            basis, basis_low, basis_high, basis_change,
            volume, weight_avg, weight_low, weight_high, unit, raw_record, collected_at
        ) VALUES (
            %(slug_id)s, %(report_date)s, %(report_section)s, %(commodity)s, %(location)s, %(grade)s,
            %(delivery_period)s, %(delivery_point)s, %(transaction_type)s, %(product_type)s,
            %(price)s, %(price_low)s, %(price_high)s, %(price_avg)s, %(price_mostly)s,
            %(basis)s, %(basis_low)s, %(basis_high)s, %(basis_change)s,
            %(volume)s, %(weight_avg)s, %(weight_low)s, %(weight_high)s,
            %(unit)s, %(raw_record)s::jsonb, NOW()
        )
        ON CONFLICT (
            slug_id, report_date,
            COALESCE(report_section, ''),
            COALESCE(commodity, ''),
            COALESCE(location, ''),
            COALESCE(grade, ''),
            COALESCE(delivery_period, '')
        )
        DO NOTHING
    """, test_rec)
    print("  TEST INSERT SUCCEEDED")
    conn.rollback()  # don't commit test data
except Exception as e:
    print(f"  TEST INSERT FAILED: {type(e).__name__}: {e}")
    conn.rollback()

conn.close()
