"""Census HS 1518.00.40 -> silver.uco_imports (import-only + exports for net, monthly, mil lbs).

Handles the census_trade AGGREGATION TRAP: the table carries bloc groupings (TOTAL FOR ALL COUNTRIES,
APEC, ASIA, OECD, NATO, EU, ...) as pseudo-"countries" that overlap and triple-count if summed. We
take 'TOTAL FOR ALL COUNTRIES' as the clean total and real countries individually (bloc blacklist).

SCOPE CAVEAT (for Desktop's ruling): HS 1518.00.40 total imports (~5.4B lb 2024) run close to the
whole EIA Yellow Grease bucket (5.58B) -> the code is BROADER than pure UCO. Desktop rules whether to
haircut it, add a companion code, or calibrate intensity against it. China is the dominant partner
(2.8B lb 2024), confirming the UCO signature. Vocabulary-independent plumbing; scope pending.
"""
import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

KG_TO_LB = 2.20462
HS = '1518004000'
# Census bloc/aggregate pseudo-countries to exclude from by-country (would overlap TOTAL)
BLOCS = ('APEC','PACIFIC RIM COUNTRIES','ASIA','OECD','NATO','EUROPE','NORTH AMERICA','USMCA (NAFTA)',
         'ASEAN','EUROPEAN UNION','EURO AREA','AUSTRALIA AND OCEANIA','TWENTY LATIN AMERICAN REPUBLICS',
         'LAFTA','SOUTH AMERICA','CACM','CAFTA-DR','AFRICA','MIDDLE EAST','CIS')

DDL = """
CREATE TABLE IF NOT EXISTS silver.uco_imports (
    period date NOT NULL, year int NOT NULL, month int NOT NULL,
    country text NOT NULL,   -- 'TOTAL' | real country name
    flow text NOT NULL,      -- import | export
    mil_lbs numeric, hs_code text,
    loaded_at timestamptz DEFAULT now(),
    PRIMARY KEY (period, country, flow)
);
"""
with get_connection() as c:
    cur = c.cursor()
    cur.execute(DDL)
    cur.execute("TRUNCATE silver.uco_imports")
    # aggregate to (year, month, country-or-TOTAL, flow); real countries only + the TOTAL row
    cur.execute(f"""
        SELECT year, month,
               CASE WHEN country_name='TOTAL FOR ALL COUNTRIES' THEN 'TOTAL' ELSE country_name END country,
               CASE WHEN flow='imports' THEN 'import' ELSE 'export' END flow,
               sum(quantity)*{KG_TO_LB}/1e6 mil_lbs
        FROM bronze.census_trade
        WHERE hs_code=%s AND flow IN ('imports','exports') AND quantity IS NOT NULL
          AND (country_name='TOTAL FOR ALL COUNTRIES' OR country_name NOT IN %s)
        GROUP BY 1,2,3,4
    """, (HS, BLOCS))
    n = 0
    for r in cur.fetchall():
        try: y, m = int(r['year']), int(r['month'])
        except (TypeError, ValueError): continue
        if not (1 <= m <= 12): continue
        cur.execute("""INSERT INTO silver.uco_imports (period,year,month,country,flow,mil_lbs,hs_code)
            VALUES (make_date(%s,%s,1),%s,%s,%s,%s,%s,%s)
            ON CONFLICT (period,country,flow) DO UPDATE SET mil_lbs=EXCLUDED.mil_lbs""",
            (y, m, y, m, r['country'], r['flow'], float(r['mil_lbs']), HS))
        n += 1
    c.commit()
    print(f"silver.uco_imports: {n} rows")
    # sanity: net imports (TOTAL) + China share, recent years
    cur.execute("""SELECT year,
                     round(sum(mil_lbs) filter (where country='TOTAL' and flow='import')/1000.0,2) imp_bn,
                     round(sum(mil_lbs) filter (where country='TOTAL' and flow='export')/1000.0,2) exp_bn,
                     round(sum(mil_lbs) filter (where country='CHINA' and flow='import')/1000.0,2) china_bn
                   FROM silver.uco_imports WHERE year IN (2021,2023,2024) GROUP BY 1 ORDER BY 1""")
    print("year | total_imp | total_exp | china_imp  (B lb)")
    for r in cur.fetchall():
        print(f"  {r['year']}  {r['imp_bn']}  {r['exp_bn']}  {r['china_bn']}")
