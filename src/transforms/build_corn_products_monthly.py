"""
Hybrid monthlyization for the corn_products tab -> silver.corn_products.

Method (spec sec.5): allocate each annual/MY control total across months
proportional to the wet-mill driver gold.corn_grind_monthly col H (corn wet-mill
products other than fuel, mil bu). Proportional allocation makes the annual
true-up automatic (monthly values sum to the control total).

  Corn input (B-E)  : ERS Feed Grains Yearbook Table 31, MARKETING-YEAR totals
                      -> allocated across the 12 MY months (Sep-Aug) by H.
                      E (cereals/other, dry-mill) has no good driver -> flat,
                      confidence='low'.
  Sweetener prod    : ERS Sugar & Sweeteners (bronze.corn_products_raw),
  (F,G,I,J)           CALENDAR-YEAR totals -> allocated across Jan-Dec by H.
  Corn Starch (K)   : derived = corn_for_starch[mil bu] x 15.75 (yield 31.5/2).
  L/M               : not produced (no yields supplied).

Months without an H driver (pre-2016) fall back to flat allocation, conf='low'.
Idempotent upsert keyed on (obs_date, product).
"""

import sys
from datetime import date
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

STARCH_YIELD = 31.5            # lbs starch / bu corn
STARCH_FACTOR = STARCH_YIELD / 2.0   # mil bu -> 000 st  (x1e6 bu * lb/bu / 2000 / 1000)

YEARS = range(2015, 2027)      # tab rows Jan-2015 .. Dec-2026

# ERS Table 31 attribute -> silver product (corn input, mil bu)
INPUT_MAP = {
    'High-fructose corn syrup (HFCS) use': 'corn_for_hfcs',
    'Glucose and dextrose use':            'corn_for_glucose_dextrose',
    'Starch use':                          'corn_for_starch',
    'Cereals and other products use':      'corn_for_cereals_other',
}
WETMILL_INPUT = {'corn_for_hfcs', 'corn_for_glucose_dextrose', 'corn_for_starch'}
PROD_PRODUCTS = ['hfcs_42', 'hfcs_55', 'glucose', 'dextrose']


def load(cur):
    cur.execute("SET statement_timeout=0")
    # H driver (mil bu), monthly
    cur.execute("""SELECT year, month, display_value FROM gold.corn_grind_monthly
                   WHERE target_col='H' AND display_value IS NOT NULL""")
    H = {(r['year'], r['month']): float(r['display_value']) for r in cur.fetchall()}
    # annual corn input by MY-start-year (Table 31, annual MY)
    cur.execute("""SELECT attribute, year, amount FROM bronze.ers_feed_grains_yearbook
                   WHERE table_name LIKE 'Table 31%%' AND frequency='Annual'
                     AND timeperiod ILIKE '%%Sep-Aug%%'""")
    inp = defaultdict(dict)
    for r in cur.fetchall():
        p = INPUT_MAP.get(r['attribute'])
        if p and r['amount'] is not None:
            inp[p][int(r['year'])] = float(r['amount'])
    # annual production by calendar year (corn_products_raw)
    cur.execute("""SELECT product, year, raw_value, vintage FROM bronze.corn_products_raw
                   WHERE measure='production' AND period_type='annual_calendar'
                     AND product = ANY(%s)""", (PROD_PRODUCTS,))
    prod, vintage = defaultdict(dict), {}
    for r in cur.fetchall():
        if r['raw_value'] is not None:
            prod[r['product']][int(r['year'])] = float(r['raw_value'])
            if r['vintage']:
                vintage[r['product']] = r['vintage']
    return H, inp, prod, vintage


def my_of(y, m):
    return y if m >= 9 else y - 1


def build():
    with get_connection() as conn:
        cur = conn.cursor()
        H, inp, prod, vintage = load(cur)

        # H sums per calendar year and per MY (Sep y .. Aug y+1)
        H_cal = defaultdict(float)
        H_my = defaultdict(float)
        for (y, m), v in H.items():
            H_cal[y] += v
            H_my[my_of(y, m)] += v

        rows = []   # (obs_date, product, measure, value, unit, is_derived, confidence, vintage)

        def alloc(annual, hval, hsum):
            """proportional if driver present, else flat (caller picks conf)."""
            if hval is not None and hsum:
                return annual * hval / hsum, False
            return annual / 12.0, True   # flat fallback -> low confidence

        for y in YEARS:
            for m in range(1, 13):
                od = date(y, m, 1)
                hval = H.get((y, m))
                my = my_of(y, m)

                # --- corn input wet-mill streams (B,C,D): MY control, H-allocated
                starch_input = None
                for p in WETMILL_INPUT:
                    A = inp.get(p, {}).get(my)
                    if A is None:
                        continue
                    v, flat = alloc(A, hval, H_my.get(my))
                    rows.append((od, p, 'corn_input', v, 'million bushels',
                                 False, 'low' if flat else 'medium', None))
                    if p == 'corn_for_starch':
                        starch_input = v

                # --- E cereals/other (dry-mill): flat, low confidence
                A = inp.get('corn_for_cereals_other', {}).get(my)
                if A is not None:
                    rows.append((od, 'corn_for_cereals_other', 'corn_input', A / 12.0,
                                 'million bushels', False, 'low', None))

                # --- sweetener production (F,G,I,J): calendar control, H-allocated
                for p in PROD_PRODUCTS:
                    A = prod.get(p, {}).get(y)
                    if A is None:
                        continue
                    v, flat = alloc(A, hval, H_cal.get(y))
                    rows.append((od, p, 'production', v, '1000 short tons, dry',
                                 False, 'low' if flat else 'medium', vintage.get(p)))

                # --- K corn starch (derived from corn_for_starch input)
                if starch_input is not None:
                    rows.append((od, 'corn_starch', 'production',
                                 starch_input * STARCH_FACTOR, '1000 short tons',
                                 True, 'medium', None))

        # upsert
        n = 0
        for r in rows:
            cur.execute("""
                INSERT INTO silver.corn_products
                    (obs_date, product, measure, value, unit, is_derived, confidence, vintage, source)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'ERS+monthlyized')
                ON CONFLICT (obs_date, product) DO UPDATE SET
                    value=EXCLUDED.value, measure=EXCLUDED.measure, unit=EXCLUDED.unit,
                    is_derived=EXCLUDED.is_derived, confidence=EXCLUDED.confidence,
                    vintage=EXCLUDED.vintage, updated_at=NOW()
            """, r)
            n += 1
        conn.commit()
        return n


if __name__ == "__main__":
    print("silver.corn_products rows upserted:", build())
