"""Pull the Helios AI climate risk index into bronze.helios_climate_risk.

First live connection to the Helios Enterprise API (2026-07-21), the two-way data leg of the
Helios/PepsiCo engagement: their climate signal feeds RLC's models, our demand/IFV feeds theirs.

API
  base   https://api.helios.sc          (spec at /openapi.json, docs at /docs)
  auth   Authorization: Bearer hsk_<env>_<...>        -> env var HELIOS_API_KEY
  NOTE   country must be the two-letter CODE from /v1/climate/commodities/{c}/countries.
         Passing the display name ("United States") returns 404 resource-not-found.

The risk index is what Eden described on the 2026-07-13 call: four dimensions -- too hot, too cold,
too wet, too dry -- plus a weighted composite, at country level, daily, with the historical average
alongside so current season vs normal is directly readable.

  wapr            weighted average percent risk (the composite, 0-100)
  wapr_hist_avg   same day, historical average -> current-vs-normal in one subtraction
  too_*_wapr      the four dimensions
  phase           growing phase (Vegetative Growth, etc.) -- ties to our crop-progress work
  is_forecasted   forward days flagged; do NOT let these read as actuals
  harvest_year    marketing-year anchor

Calibration caveat worth carrying: RLC's scenario layer is calibrated to classical inputs
(rainfall, temperature, soil moisture). This composite is NOT a drop-in replacement -- it has to be
validated against the historical commentary archive (2012 drought, 2019 wet) before it drives
anything. Landing it in bronze is step one, not integration.

Run:  python scripts/collect_helios_climate.py [--commodities soya_beans,canola] [--dry-run]
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
import os
from src.services.database.db_config import get_connection

BASE = "https://api.helios.sc"
# Pepsi/Helios scope first: the five veg-oil complexes we owe guidance on, plus the grains in the
# meeting record. Helios exposes durum separately from wheat but has NOT split spring/winter --
# that remains the delivery gate flagged on 2026-07-13.
DEFAULT_COMMODITIES = ["soya_beans", "canola", "oil_palm_fruit", "wheat", "durum_wheat",
                       "corn_commodity_tracked", "seed_cotton_unginned"]

DDL = """
CREATE TABLE IF NOT EXISTS bronze.helios_climate_risk (
    commodity_slug  text NOT NULL,
    country_code    text NOT NULL,
    country_name    text,
    date_on         date NOT NULL,
    wapr            numeric,
    wapr_hist_avg   numeric,
    wapr_shade      numeric,
    too_hot_wapr    numeric,
    too_cold_wapr   numeric,
    too_wet_wapr    numeric,
    too_dry_wapr    numeric,
    season_status   text,
    severity        text,
    hist_severity   text,
    phase           text,
    is_in_season    boolean,
    is_forecasted   boolean,
    harvest_year    int,
    source          text DEFAULT 'HELIOS_API_V1',
    collected_at    timestamptz DEFAULT now(),
    PRIMARY KEY (commodity_slug, country_code, date_on)
);
"""

FIELDS = ['wapr', 'wapr_hist_avg', 'wapr_shade', 'too_hot_wapr', 'too_cold_wapr', 'too_wet_wapr',
          'too_dry_wapr', 'season_status', 'severity', 'hist_severity', 'phase', 'is_in_season',
          'is_forecasted', 'harvest_year']


def get(path, key, tries=3):
    # An explicit User-Agent is REQUIRED: the default "Python-urllib/3.x" gets a 403 at the edge
    # while the identical curl request succeeds. Cost 10 minutes the first time -- leave it set.
    req = urllib.request.Request(BASE + path, headers={
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
        "User-Agent": "RLC-Agent/1.0 (Round Lakes Commodities; tore.alden@roundlakescommodities.com)",
    })
    for i in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None                      # no coverage for this pair; not an error
            if e.code in (429, 500, 502, 503) and i < tries - 1:
                time.sleep(2 ** i)
                continue
            raise
        except (urllib.error.URLError, TimeoutError):
            if i < tries - 1:
                time.sleep(2 ** i)
                continue
            raise
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commodities", default=",".join(DEFAULT_COMMODITIES))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    key = os.getenv("HELIOS_API_KEY")
    if not key:
        sys.exit("HELIOS_API_KEY not set (add it to .env)")

    commodities = [c.strip() for c in args.commodities.split(",") if c.strip()]
    covered = {c["slug"] for c in (get("/v1/climate/commodities", key) or {}).get("commodities", [])}
    missing = [c for c in commodities if c not in covered]
    if missing:
        print(f"NOT in Helios climate coverage, skipping: {missing}")
    commodities = [c for c in commodities if c in covered]

    rows, pairs = [], 0
    for slug in commodities:
        cl = get(f"/v1/climate/commodities/{slug}/countries", key) or {}
        countries = cl.get("countries", [])
        print(f"{slug:26s} {len(countries):3d} countries")
        for c in countries:
            code, name = c["code"], c.get("name")
            d = get(f"/v1/climate/country-risk-index/{slug}/{code}", key)
            pts = (d or {}).get("points") or []
            if not pts:
                print(f"    {code} {str(name)[:22]:22s} -- no data")
                continue
            pairs += 1
            for p in pts:
                rows.append([slug, code, name, p.get("date_on")] + [p.get(f) for f in FIELDS])
            fc = sum(1 for p in pts if p.get("is_forecasted"))
            print(f"    {code} {str(name)[:22]:22s} {len(pts):5d} days "
                  f"({pts[0].get('date_on')} -> {pts[-1].get('date_on')}, {fc} forecast)")

    print(f"\n{len(rows)} rows across {pairs} commodity x country pairs")
    if args.dry_run:
        print("--dry-run: nothing written")
        return

    from psycopg2.extras import execute_values
    cols = ['commodity_slug', 'country_code', 'country_name', 'date_on'] + FIELDS
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(DDL)
        execute_values(cur, f"""INSERT INTO bronze.helios_climate_risk ({','.join(cols)})
            VALUES %s ON CONFLICT (commodity_slug, country_code, date_on) DO UPDATE SET
            {','.join(f'{c}=EXCLUDED.{c}' for c in cols[2:])}, collected_at=now()""",
                       rows, page_size=1000)
        conn.commit()
        cur.execute("""SELECT count(*) n, count(DISTINCT commodity_slug||'|'||country_code) pairs,
                         min(date_on) mn, max(date_on) mx,
                         count(*) FILTER (WHERE is_forecasted) fc
                       FROM bronze.helios_climate_risk""")
        s = cur.fetchone()
        print(f"\nbronze.helios_climate_risk: {s['n']} rows | {s['pairs']} pairs | "
              f"{s['mn']} -> {s['mx']} | {s['fc']} forecast days")


if __name__ == "__main__":
    main()
