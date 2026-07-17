"""Build reference.nonbio_enduse_shares_monthly — seasonal (calendar-month) non-bio end-use shares.

Motivation: flat held-forward shares put each component at the SAME percentage every month, which
reads as synthetic to a sharp analyst. This derives the real month-of-year pattern from the
2006-2011 Census end-use monthlies and applies it as a SEASONAL INDEX around the ruled annual
shares in reference.nonbio_enduse_shares (migration 144) — so the annual LEVEL is preserved
(nothing on the sheets shifts in annual terms) and only the month-to-month SHAPE is added.

  seasonal_share[b,m] = flat_annual[b] * (raw_monthly[b,m] / raw_annual[b]),  renormalized so
  the components sum to 1.0 within each (commodity, month) -> the sheet still closes every month.

Sources of the raw monthly pattern (2006-2011, survey discontinued 2011):
  - Soybean oil (7 buckets): models/Oilseeds/United States/us_oilseed_crush.xlsm "Census Crush"
    tab, cols HH baking / HI margarine / HJ salad / HK other-edible / HQ paint / HR resins /
    HT other-inedible. (Methyl esters excluded = biofuel.)  MEASURED.
  - Tallow (3 buckets): bronze.census_cir_fats  c234 Feed / c233 Fatty acids / c245 Other
    Inedible Products (window 2007-09..2011-07 where all three co-exist).  MEASURED.
Analogs (no own source): canola = SBO edible 3 with SBO's seasonal index; poultry_fat /
white_grease / yellow_grease = tallow monthly; dco (feed) / uco_yg (oleochemical_feed) are
single-bucket -> 1.0 every month (no seasonality). cottonseed_oil: flat copied to all months.

Run:  python scripts/build_nonbio_seasonal_shares.py
"""
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
import openpyxl
from openpyxl.utils import column_index_from_string
from src.services.database.db_config import get_connection

XLSM = ROOT / "models" / "Oilseeds" / "United States" / "us_oilseed_crush.xlsm"

# SBO end_use -> Census Crush column (must match reference.nonbio_enduse_shares soybean_oil rows)
SBO_COLS = {'salad_cooking_oil': 'HJ', 'baking_frying_fats': 'HH', 'margarine': 'HI',
            'other_edible': 'HK', 'other_inedible': 'HT', 'resins_plastics': 'HR',
            'paint_varnish': 'HQ'}
# tallow end_use -> CIR series
TALLOW_SERIES = {'feed': 'c234:Feed', 'fatty_acids': 'c233:Fatty acids',
                 'other_inedible': 'c245:Other Inedible Products'}


def _volume_weighted_monthly(sums_by_month):
    """sums_by_month[m][b] = summed volume. Return share[b][m] volume-weighted within each month."""
    share = defaultdict(dict)
    for m, bucket_sum in sums_by_month.items():
        tot = sum(bucket_sum.values())
        if tot <= 0:
            continue
        for b, v in bucket_sum.items():
            share[b][m] = v / tot
    return share


def sbo_monthly_raw():
    wb = openpyxl.load_workbook(str(XLSM), data_only=True, read_only=True)
    ws = wb['Census Crush']
    idx = {b: column_index_from_string(c) - 1 for b, c in SBO_COLS.items()}
    sums = defaultdict(lambda: defaultdict(float))
    for r in ws.iter_rows(min_row=2, values_only=True):
        d = r[0]
        if d is None or not hasattr(d, 'year'):
            continue
        if d.year < 2006 or d.year > 2011 or (d.year == 2011 and d.month > 7):
            continue
        vals = {b: (r[i] if i < len(r) and isinstance(r[i], (int, float)) else None)
                for b, i in idx.items()}
        if any(v is None for v in vals.values()) or sum(vals.values()) <= 0:
            continue
        for b, v in vals.items():
            sums[d.month][b] += float(v)
    return _volume_weighted_monthly(sums)


def tallow_monthly_raw(cur):
    cur.execute("""SELECT extract(month from period)::int m, series, value_mil_lbs v
                   FROM bronze.census_cir_fats
                   WHERE period>=date '2007-09-01' AND period<date '2011-08-01'
                     AND series = ANY(%s) AND value_mil_lbs IS NOT NULL""",
                (list(TALLOW_SERIES.values()),))
    inv = {v: k for k, v in TALLOW_SERIES.items()}
    per_period = defaultdict(dict)  # (m) accumulation needs all 3 present per period; sum by month
    sums = defaultdict(lambda: defaultdict(float))
    for row in cur.fetchall():
        sums[row['m']][inv[row['series']]] += float(row['v'])
    return _volume_weighted_monthly(sums)


def seasonalize(flat, raw_share):
    """flat[b]=annual share; raw_share[b][m]=raw monthly share. Return seasonal[m][b] preserving
    the flat annual level (index = raw_monthly/raw_annual), renormalized per month to sum 1.0."""
    buckets = list(flat)
    raw_annual = {b: (sum(raw_share.get(b, {}).values()) / max(1, len(raw_share.get(b, {}))))
                  for b in buckets}
    out = defaultdict(dict)
    for m in range(1, 13):
        weighted = {}
        for b in buckets:
            rm = raw_share.get(b, {}).get(m)
            idx = (rm / raw_annual[b]) if (rm is not None and raw_annual[b] > 0) else 1.0
            weighted[b] = flat[b] * idx
        tot = sum(weighted.values())
        for b in buckets:
            out[m][b] = weighted[b] / tot if tot > 0 else flat[b]
    return out


def main():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS reference.nonbio_enduse_shares_monthly (
            commodity varchar(32) NOT NULL, month int NOT NULL, end_use varchar(48) NOT NULL,
            share_pct numeric NOT NULL, measured boolean NOT NULL DEFAULT false, basis text,
            PRIMARY KEY (commodity, month, end_use))""")
        cur.execute("DELETE FROM reference.nonbio_enduse_shares_monthly")

        # flat annual shares (the ruled level) from migration 144
        cur.execute("SELECT commodity, end_use, share_pct, measured FROM reference.nonbio_enduse_shares")
        flat = defaultdict(dict); meas = {}
        for r in cur.fetchall():
            flat[r['commodity']][r['end_use']] = float(r['share_pct'])
            meas[(r['commodity'], r['end_use'])] = r['measured']

        sbo_raw = sbo_monthly_raw()
        tallow_raw = tallow_monthly_raw(cur)

        # seasonal index source per commodity: which raw pattern drives it
        seasonal = {}
        seasonal['soybean_oil'] = seasonalize(flat['soybean_oil'], sbo_raw)
        seasonal['tallow'] = seasonalize(flat['tallow'], tallow_raw)
        # canola: SBO edible buckets get SBO's seasonal index
        seasonal['canola_oil'] = seasonalize(flat['canola_oil'],
                                             {b: sbo_raw.get(b, {}) for b in flat['canola_oil']})
        # PF/CWG/YG: tallow pattern
        for c in ('poultry_fat', 'white_grease', 'yellow_grease'):
            seasonal[c] = seasonalize(flat[c], tallow_raw)
        # single-bucket + cottonseed: flat across all months (no seasonal source)
        for c in ('dco', 'uco_yg', 'cottonseed_oil'):
            seasonal[c] = {m: dict(flat[c]) for m in range(1, 13)}

        rows = []
        for commodity, by_month in seasonal.items():
            for m, shares in by_month.items():
                for b, s in shares.items():
                    measured = meas.get((commodity, b), False)
                    basis = ('seasonal index from 2006-2011 Census monthly, level preserved from '
                             'ruled annual' if commodity in ('soybean_oil', 'tallow')
                             else 'analog seasonal (index borrowed)' if commodity in
                             ('canola_oil', 'poultry_fat', 'white_grease', 'yellow_grease')
                             else 'flat (no seasonal source)')
                    rows.append((commodity, m, b, round(s, 6), measured, basis))
        from psycopg2.extras import execute_values
        execute_values(cur, """INSERT INTO reference.nonbio_enduse_shares_monthly
            (commodity, month, end_use, share_pct, measured, basis) VALUES %s""", rows)
        conn.commit()

        # verify: each (commodity, month) sums to 1.0
        cur.execute("""SELECT commodity, month, round(sum(share_pct),4) s FROM
                       reference.nonbio_enduse_shares_monthly GROUP BY 1,2 HAVING round(sum(share_pct),4)<>1.0""")
        bad = cur.fetchall()
        print(f"Seeded {len(rows)} monthly share rows across {len(seasonal)} commodities.")
        print(f"Rows where month-shares != 1.0: {len(bad)}" + (f"  {[dict(b) for b in bad][:5]}" if bad else "  (all close)"))
        # show SBO seasonality range
        cur.execute("""SELECT end_use, round(min(share_pct)*100,1) lo, round(max(share_pct)*100,1) hi
                       FROM reference.nonbio_enduse_shares_monthly WHERE commodity='soybean_oil'
                       GROUP BY 1 ORDER BY 2 DESC""")
        print("\nSBO monthly share ranges (annual level preserved):")
        for r in cur.fetchall():
            print(f"   {r['end_use']:22s} {r['lo']}%..{r['hi']}%")


if __name__ == "__main__":
    main()
