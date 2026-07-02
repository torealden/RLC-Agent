"""Build silver.wheat_series (canonical long) from bronze NASS — wheat supply slice.

First vertical slice of the flat-file pipeline (see models/Food Grains/flat_file_contract.md):
bronze.nass_acreage -> silver.wheat_series for area_planted / area_harvested, mapping NASS
reference_period -> (vintage, vintage_rank) so the estimate march (winter seedings -> prospective
-> acreage -> final) is preserved as row keys. Idempotent upsert.
"""
import re, sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

DDL = """
CREATE SCHEMA IF NOT EXISTS silver;
CREATE TABLE IF NOT EXISTS silver.wheat_series (
    commodity      text    NOT NULL DEFAULT 'wheat',
    class          text    NOT NULL DEFAULT 'ALL',
    series         text    NOT NULL,
    marketing_year int     NOT NULL,
    period_type    text    NOT NULL,
    period         text    NOT NULL,
    vintage        text    NOT NULL,
    vintage_rank   int     NOT NULL,
    value          numeric,
    unit           text,
    source         text,
    release_date   date,
    revision       int,
    loaded_at      timestamptz DEFAULT now(),
    PRIMARY KEY (commodity, class, series, marketing_year, period_type, period, vintage)
);
"""
MONTHS = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}

def vintage_of(refp):
    """NASS reference_period -> (vintage, vintage_rank). None to skip."""
    if not refp: return None
    r = refp.strip().upper()
    if r == 'YEAR': return ('FINAL', 90)
    if r == 'YEAR - DEC ACREAGE': return ('WINTER_SEEDINGS', 10)
    if r == 'YEAR - MAR ACREAGE': return ('PROSPECTIVE', 20)
    if r == 'YEAR - JUN ACREAGE': return ('ACREAGE', 30)
    m = re.match(r'YEAR - (\w{3}) FORECAST', r)
    if m and m.group(1) in MONTHS:
        mo = MONTHS[m.group(1)]
        return (f'CROP_PROD_{m.group(1)}', 40 + mo)
    m = re.match(r'YEAR - (\w{3}) ACREAGE', r)   # any other <MON> ACREAGE -> rank by month
    if m and m.group(1) in MONTHS:
        return (f'ACREAGE_{m.group(1)}', 20 + MONTHS[m.group(1)])
    return None

SERIES = {'AREA PLANTED': ('area_planted', 'ACRES'), 'AREA HARVESTED': ('area_harvested', 'ACRES')}

def class_of(short_desc):
    """NASS short_desc -> agronomic class key. Aggregate 'WHEAT - ...' = ALL.
    NOTE: NASS reports AREA by winter/spring/durum (agronomic), NOT the HRW/HRS/SRW/WHITE market
    classes. Market-class breakdown is a production-by-class series (Small Grains Summary) — a
    separate source not yet in bronze."""
    s = (short_desc or '').upper()
    if 'WINTER' in s: return 'WINTER'
    if 'EXCL DURUM' in s: return 'SPRING'   # 'SPRING, (EXCL DURUM)'
    if 'DURUM' in s: return 'DURUM'
    if 'SPRING' in s: return 'SPRING'
    return 'ALL'                             # 'WHEAT - ACRES ...' (aggregate)

with get_connection() as c:
    cur = c.cursor()
    cur.execute(DDL)
    cur.execute("TRUNCATE silver.wheat_series")   # clean rebuild (prior run conflated aggregate + class rows)
    cur.execute("""SELECT year, reference_period, statisticcat, short_desc, value
                   FROM bronze.nass_acreage
                   WHERE commodity ILIKE 'wheat' AND agg_level='NATIONAL'
                     AND statisticcat = ANY(%s) AND value IS NOT NULL""",
                (list(SERIES.keys()),))
    rows = cur.fetchall()
    ins = skipped = 0
    for r in rows:
        v = vintage_of(r['reference_period'])
        sc = SERIES.get(str(r['statisticcat']).upper())
        if not v or not sc:
            skipped += 1; continue
        vintage, rank = v; series, unit = sc
        cls = class_of(r['short_desc'])
        try:
            my = int(r['year']); val = float(r['value'])
        except (TypeError, ValueError):
            skipped += 1; continue
        cur.execute("""INSERT INTO silver.wheat_series
            (commodity,class,series,marketing_year,period_type,period,vintage,vintage_rank,value,unit,source,loaded_at)
            VALUES ('wheat',%s,%s,%s,'annual','ANNUAL',%s,%s,%s,%s,'NASS_QUICKSTATS',now())
            ON CONFLICT (commodity,class,series,marketing_year,period_type,period,vintage)
            DO UPDATE SET value=EXCLUDED.value, vintage_rank=EXCLUDED.vintage_rank, loaded_at=now()""",
            (cls, series, my, vintage, rank, val, unit))
        ins += 1
    c.commit()

    print(f"ingested {ins} rows, skipped {skipped} (unmapped refperiod)")
    cur.execute("SELECT series, class, count(*) n, min(marketing_year) mn, max(marketing_year) mx FROM silver.wheat_series GROUP BY 1,2 ORDER BY 1,2")
    for r in cur.fetchall():
        print(f"  {r['series']:16} {r['class']:7} rows={r['n']:4} MY {r['mn']}-{r['mx']}")
    print("\n=== area_planted MY2024 by class @ FINAL (ALL should ~= WINTER+SPRING+DURUM) ===")
    cur.execute("""SELECT class, value FROM silver.wheat_series
                   WHERE series='area_planted' AND marketing_year=2024 AND vintage='FINAL' ORDER BY class""")
    tot=0; agg=0
    for r in cur.fetchall():
        v=float(r['value']); print(f"  {r['class']:7} {v/1e6:7.3f} M acres")
        if r['class']=='ALL': agg=v
        else: tot+=v
    print(f"  -> classes sum {tot/1e6:.3f}M vs ALL {agg/1e6:.3f}M (delta {(tot-agg)/1e6:+.3f}M)")
