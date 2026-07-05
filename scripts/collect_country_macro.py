"""Pull country GDP / GDP-per-capita / population from the World Bank API -> bronze.country_macro.

The multi-country UCO-collection proxy (design brief §4b): consumer strength = per-capita GDP x
population where food-spending isn't available. China is the critical term (dominant UCO exporter to
the US). World Bank is free, no key, historical; OECD/IMF projections (for the 2050 forecast) layer
on later per Desktop's forecast method. Vocabulary-independent plumbing.
"""
import sys, json, time, urllib.request
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

# feedstock/biofuel-trade-relevant countries (UCO exporters, RD producers, food markets)
COUNTRIES = "USA;CHN;CAN;MEX;MYS;IDN;BRA;ARG;GBR;DEU;FRA;ESP;ITA;NLD;JPN;KOR;IND;AUS;SGP;ARE"
INDICATORS = {  # WB code -> our metric
    'NY.GDP.MKTP.CD':  'gdp_usd',
    'NY.GDP.PCAP.CD':  'gdp_per_capita_usd',
    'SP.POP.TOTL':     'population',
}
API = "https://api.worldbank.org/v2/country/{c}/indicator/{ind}?format=json&per_page=20000&date=1997:2024"

DDL = """
CREATE TABLE IF NOT EXISTS bronze.country_macro (
    country_code text NOT NULL, country_name text, metric text NOT NULL,
    year int NOT NULL, value numeric, source text, collected_at timestamptz DEFAULT now(),
    PRIMARY KEY (country_code, metric, year)
);
"""

def fetch(ind, tries=5):
    url = API.format(c=COUNTRIES, ind=ind)
    for t in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=45) as resp:
                data = json.loads(resp.read())
            return data[1] if len(data) > 1 and data[1] else []
        except Exception as e:
            if t == tries - 1:
                print(f"  {ind}: FAILED after {tries} tries ({e})"); return []
            time.sleep(3 + 3 * t)   # WB gateway 502s are transient
    return []

with get_connection() as c:
    cur = c.cursor()
    cur.execute(DDL)
    cur.execute("DELETE FROM bronze.country_macro WHERE source='world_bank'")
    total = 0
    for wb_code, metric in INDICATORS.items():
        recs = fetch(wb_code)
        n = 0
        for r in recs:
            if r.get('value') is None:
                continue
            cur.execute("""INSERT INTO bronze.country_macro (country_code, country_name, metric, year, value, source)
                VALUES (%s,%s,%s,%s,%s,'world_bank')
                ON CONFLICT (country_code, metric, year) DO UPDATE SET value=EXCLUDED.value, country_name=EXCLUDED.country_name""",
                (r['countryiso3code'], r['country']['value'], metric, int(r['date']), float(r['value'])))
            n += 1
        total += n
        print(f"  {metric}: {n} rows")
    c.commit()
    cur.execute("SELECT count(distinct country_code) nc, min(year) mn, max(year) mx FROM bronze.country_macro")
    r = cur.fetchone(); print(f"bronze.country_macro: {total} rows, {r['nc']} countries, {r['mn']}-{r['mx']}")
    # sanity: China consumer strength (the key exporter)
    cur.execute("""SELECT year, round(value/1e9,1) pop_bn FROM bronze.country_macro WHERE country_code='CHN' AND metric='population' AND year IN (2015,2024) ORDER BY year""")
    print("China population (bn):", [(r['year'], float(r['pop_bn'])) for r in cur.fetchall()])
