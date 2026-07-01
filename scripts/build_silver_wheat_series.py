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

with get_connection() as c:
    cur = c.cursor()
    cur.execute(DDL)
    cur.execute("""SELECT year, reference_period, statisticcat, value
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
        try:
            my = int(r['year']); val = float(r['value'])
        except (TypeError, ValueError):
            skipped += 1; continue
        cur.execute("""INSERT INTO silver.wheat_series
            (commodity,class,series,marketing_year,period_type,period,vintage,vintage_rank,value,unit,source,loaded_at)
            VALUES ('wheat','ALL',%s,%s,'annual','ANNUAL',%s,%s,%s,%s,'NASS_QUICKSTATS',now())
            ON CONFLICT (commodity,class,series,marketing_year,period_type,period,vintage)
            DO UPDATE SET value=EXCLUDED.value, vintage_rank=EXCLUDED.vintage_rank, loaded_at=now()""",
            (series, my, vintage, rank, val, unit))
        ins += 1
    c.commit()

    print(f"ingested {ins} rows, skipped {skipped} (unmapped refperiod)")
    cur.execute("SELECT series, count(*) n, min(marketing_year) mn, max(marketing_year) mx, count(distinct vintage) nv FROM silver.wheat_series GROUP BY 1")
    for r in cur.fetchall():
        print(f"  {r['series']:16} rows={r['n']:4} MY {r['mn']}-{r['mx']} vintages={r['nv']}")
    print("\n=== vintage march for area_planted MY2024 (should climb rank) ===")
    cur.execute("""SELECT vintage, vintage_rank, value FROM silver.wheat_series
                   WHERE series='area_planted' AND marketing_year=2024 ORDER BY vintage_rank""")
    for r in cur.fetchall():
        print(f"  {r['vintage']:16} rank={r['vintage_rank']:2}  {float(r['value'])/1e6:7.3f} M acres")
