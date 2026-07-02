"""Build silver.wheat_series (canonical long) from bronze NASS — wheat supply side.

See models/Food Grains flat_file_contract.md. Sources:
  area (planted/harvested), by agronomic class  <- bronze.nass_acreage  (vintage march via reference_period)
  production (aggregate + 5 market classes)     <- bronze.nass_production
  stocks (quarterly)                            <- bronze.nass_stocks
  yield                                         <- derived = production / area_harvested per matched vintage
Idempotent (TRUNCATE + rebuild). Market classes: HRW/SRW/HRS/DURUM/WHITE sum to ALL (validated).
"""
import re, sys
from collections import defaultdict
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

DDL = """
CREATE SCHEMA IF NOT EXISTS silver;
CREATE TABLE IF NOT EXISTS silver.wheat_series (
    commodity text NOT NULL DEFAULT 'wheat', class text NOT NULL DEFAULT 'ALL', series text NOT NULL,
    marketing_year int NOT NULL, period_type text NOT NULL, period text NOT NULL,
    vintage text NOT NULL, vintage_rank int NOT NULL, value numeric, unit text, source text,
    release_date date, revision int, loaded_at timestamptz DEFAULT now(),
    PRIMARY KEY (commodity, class, series, marketing_year, period_type, period, vintage)
);"""
MONTHS = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}

def vintage_of(refp):
    if not refp: return None
    r = refp.strip().upper()
    if r == 'YEAR': return ('FINAL', 90)
    if r == 'YEAR - DEC ACREAGE': return ('WINTER_SEEDINGS', 10)
    if r == 'YEAR - MAR ACREAGE': return ('PROSPECTIVE', 20)
    if r == 'YEAR - JUN ACREAGE': return ('ACREAGE', 30)
    m = re.match(r'YEAR - (\w{3}) FORECAST', r)
    if m and m.group(1) in MONTHS: return (f'CROP_PROD_{m.group(1)}', 40 + MONTHS[m.group(1)])
    m = re.match(r'YEAR - (\w{3}) ACREAGE', r)
    if m and m.group(1) in MONTHS: return (f'ACREAGE_{m.group(1)}', 20 + MONTHS[m.group(1)])
    return None

def area_class_of(sd):
    s = (sd or '').upper()
    if 'WINTER' in s: return 'WINTER'
    if 'EXCL DURUM' in s: return 'SPRING'
    if 'DURUM' in s: return 'DURUM'
    if 'SPRING' in s: return 'SPRING'
    return 'ALL'

def prod_class_of(sd):
    """Market class from production short_desc. None = skip ($, organic, intermediate aggregates)."""
    s = (sd or '').upper()
    if 'MEASURED IN BU' not in s or 'ORGANIC' in s: return None
    if s == 'WHEAT - PRODUCTION, MEASURED IN BU': return 'ALL'
    if 'WINTER, RED, HARD' in s: return 'HRW'
    if 'WINTER, RED, SOFT' in s: return 'SRW'
    if 'SPRING, RED, HARD' in s: return 'HRS'
    if 'DURUM' in s and 'EXCL' not in s: return 'DURUM'
    return None   # WHITE derived as residual (NASS white coverage is inconsistent); skip aggregates too

STOCK_Q = {'FIRST OF JUN': ('Q1', 0), 'FIRST OF SEP': ('Q2', 0), 'FIRST OF DEC': ('Q3', 0), 'FIRST OF MAR': ('Q4', -1)}

def upsert(cur, cls, series, my, ptype, period, vintage, rank, val, unit, src):
    cur.execute("""INSERT INTO silver.wheat_series
        (commodity,class,series,marketing_year,period_type,period,vintage,vintage_rank,value,unit,source,loaded_at)
        VALUES ('wheat',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
        ON CONFLICT (commodity,class,series,marketing_year,period_type,period,vintage)
        DO UPDATE SET value=EXCLUDED.value, vintage_rank=EXCLUDED.vintage_rank, unit=EXCLUDED.unit, loaded_at=now()""",
        (cls, series, my, ptype, period, vintage, rank, val, unit, src))

with get_connection() as c:
    cur = c.cursor()
    cur.execute(DDL)
    cur.execute("TRUNCATE silver.wheat_series")
    n = defaultdict(int)

    # 1. AREA (planted/harvested) by agronomic class, with vintage march
    cur.execute("""SELECT year, reference_period, statisticcat, short_desc, value FROM bronze.nass_acreage
                   WHERE commodity ILIKE 'wheat' AND agg_level='NATIONAL'
                     AND statisticcat = ANY(ARRAY['AREA PLANTED','AREA HARVESTED']) AND value IS NOT NULL""")
    for r in cur.fetchall():
        v = vintage_of(r['reference_period'])
        if not v: continue
        series = 'area_planted' if r['statisticcat'] == 'AREA PLANTED' else 'area_harvested'
        try: my = int(r['year']); val = float(r['value'])
        except (TypeError, ValueError): continue
        upsert(cur, area_class_of(r['short_desc']), series, my, 'annual', 'ANNUAL', v[0], v[1], val, 'ACRES', 'NASS_QUICKSTATS'); n['area'] += 1

    # 2. PRODUCTION (aggregate + market classes) — SUM by (class, MY, vintage) so winter+spring WHITE combine
    cur.execute("""SELECT year, reference_period, short_desc, value FROM bronze.nass_production
                   WHERE commodity='wheat' AND agg_level='NATIONAL' AND statisticcat='PRODUCTION' AND value IS NOT NULL""")
    prod = defaultdict(float)
    for r in cur.fetchall():
        cls = prod_class_of(r['short_desc']); v = vintage_of(r['reference_period'])
        if not cls or not v: continue
        try: my = int(r['year']); val = float(r['value'])
        except (TypeError, ValueError): continue
        prod[(cls, my, v[0], v[1])] += val
    for (cls, my, vin, rank), val in prod.items():
        upsert(cur, cls, 'production', my, 'annual', 'ANNUAL', vin, rank, val, 'BU', 'NASS_QUICKSTATS'); n['production'] += 1

    # 2b. WHITE = ALL - (HRW+SRW+HRS+DURUM) residual, per (MY, vintage) where all present
    cur.execute("""SELECT marketing_year my, vintage, max(vintage_rank) rank,
                     max(value) FILTER (WHERE class='ALL') a,
                     sum(value) FILTER (WHERE class IN ('HRW','SRW','HRS','DURUM')) leaves,
                     count(*) FILTER (WHERE class IN ('ALL','HRW','SRW','HRS','DURUM')) k
                   FROM silver.wheat_series WHERE series='production' GROUP BY 1,2""")
    for r in cur.fetchall():
        if r['k'] == 5 and r['a'] and r['leaves']:
            w = float(r['a']) - float(r['leaves'])
            if w > 0:
                upsert(cur, 'WHITE', 'production', r['my'], 'annual', 'ANNUAL', r['vintage'], r['rank'], w, 'BU', 'DERIVED_RESIDUAL'); n['white'] += 1

    # 3. STOCKS (aggregate, quarterly) — Mar stocks belong to prior MY
    cur.execute("""SELECT year, reference_period, short_desc, value FROM bronze.nass_stocks
                   WHERE commodity ILIKE 'wheat' AND agg_level='NATIONAL' AND statisticcat='STOCKS'
                     AND short_desc ILIKE 'WHEAT - STOCKS%' AND value IS NOT NULL""")
    for r in cur.fetchall():
        q = STOCK_Q.get(str(r['reference_period']).upper())
        if not q: continue
        try: my = int(r['year']) + q[1]; val = float(r['value'])
        except (TypeError, ValueError): continue
        upsert(cur, 'ALL', 'stocks', my, 'quarter', q[0], 'ACTUAL', 99, val, 'BU', 'NASS_QUICKSTATS'); n['stocks'] += 1

    # 4. YIELD (ALL) derived = production / area_harvested at matched (MY, vintage)
    cur.execute("""SELECT p.marketing_year my, p.vintage, p.vintage_rank, p.value/h.value AS y
                   FROM silver.wheat_series p JOIN silver.wheat_series h
                     ON p.marketing_year=h.marketing_year AND p.vintage=h.vintage
                    AND p.class='ALL' AND h.class='ALL' AND p.series='production' AND h.series='area_harvested'
                   WHERE h.value > 0""")
    for r in cur.fetchall():
        upsert(cur, 'ALL', 'yield', r['my'], 'annual', 'ANNUAL', r['vintage'], r['vintage_rank'], round(float(r['y']),2), 'BU/ACRE', 'DERIVED'); n['yield'] += 1

    c.commit()

    print(f"ingested: {dict(n)}")
    cur.execute("SELECT series, count(*) c, count(distinct class) cl, min(marketing_year) mn, max(marketing_year) mx FROM silver.wheat_series GROUP BY 1 ORDER BY 1")
    for r in cur.fetchall(): print(f"  {r['series']:16} rows={r['c']:4} classes={r['cl']} MY {r['mn']}-{r['mx']}")
    print("\n=== VALIDATION: production by market class MY2024 FINAL (5 classes should sum to ALL) ===")
    cur.execute("""SELECT class, value FROM silver.wheat_series WHERE series='production' AND marketing_year=2024
                   AND vintage='FINAL' ORDER BY value DESC""")
    allv=0.0; leaf=0.0
    for r in cur.fetchall():
        v=float(r['value']); print(f"  {r['class']:5} {v/1e6:8.1f} M bu")
        if r['class']=='ALL': allv=v
        else: leaf+=v
    if allv: print(f"  -> 5 classes sum {leaf/1e6:.1f}M vs ALL {allv/1e6:.1f}M (delta {100*(leaf-allv)/allv:+.2f}%)")
    cur.execute("SELECT marketing_year, value FROM silver.wheat_series WHERE series='yield' AND class='ALL' AND vintage='FINAL' ORDER BY marketing_year DESC LIMIT 3")
    print("  recent FINAL yield (bu/acre):", [(r['marketing_year'], float(r['value'])) for r in cur.fetchall()])
