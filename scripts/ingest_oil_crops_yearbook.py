"""
Ingest the USDA ERS Oil Crops Yearbook (OilCropsAllTables.csv) into bronze,
then map the monthly soybean tables into silver.monthly_realized.

Why: NASS QuickStats fats & oils history starts 2014/15. The Yearbook carries
monthly soybean series back further:
  Table 6 - soybean crush by month,    2000/01+  (thousand bushels)
  Table 7 - soybean meal S&D by month, 2007/08+  (thousand short tons)
  Table 8 - soybean oil S&D by month,  2007/08+  (thousand pounds)
These backfill the gap months in us_oilseed_crush.xlsm that currently hold
Tore's estimates (gray cells). Everything else (annual tables, all 43) lands
in bronze for reference but is not mapped to silver here.

Silver rows get source='ERS_OCY'. The monthly_realized unique key includes
source, so NASS rows are never touched. marketing_year is normalized to the
NASS soybean convention (Sep-Aug start year) so ERS and NASS rows align on
(commodity, marketing_year, month); calendar_year is derived from each
table's own published MY definition (Sep-Aug for seed, Oct-Sep for products).

Usage:
  python scripts/ingest_oil_crops_yearbook.py             # bronze + silver
  python scripts/ingest_oil_crops_yearbook.py --bronze-only
  python scripts/ingest_oil_crops_yearbook.py --verify    # ERS vs NASS overlap check
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import pandas as pd  # noqa: E402
from psycopg2.extras import execute_values  # noqa: E402
from src.services.database.db_config import get_connection  # noqa: E402

CSV_PATH = ROOT / "data/raw/oilseeds_fats_greases/OilCropsAllTables.csv"

MONTHS = {
    "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
    "July": 7, "August": 8, "September": 9, "October": 10, "November": 11,
    "December": 12,
}

# (table_number, attribute_desc) -> (silver attribute, unit, multiplier)
# Multipliers convert the Yearbook publication unit to the silver source unit
# used by NASS rows (crush/meal in short TONS, oil in LB):
#   thousand bushels soybeans -> tons: x1000 bu * 60 lb / 2000 = x30
#   thousand short tons       -> tons: x1000
#   thousand pounds           -> lb:   x1000
#
# Attribute naming: 'crush' and 'oil_production_crude' verify 0.00% against
# NASS for every overlapping month, so they share the NASS attribute name.
# ERS meal production runs ~7% above NASS CAKE & MEAL (ERS includes hulls/
# millfeed) and ERS stocks are total S&D-basis ending stocks (all positions,
# crude+refined for oil) vs NASS's narrower survey series -- those get an
# '_sd' suffix so nobody joins them to NASS thinking they're the same series.
SILVER_MAP = {
    (6, "Crush"):          ("crush",                "TONS", 30.0),
    (7, "Production"):     ("meal_production_sd",   "TONS", 1000.0),
    (7, "Ending stocks"):  ("meal_stocks_sd",       "TONS", 1000.0),
    (8, "Production"):     ("oil_production_crude", "LB",   1000.0),
    (8, "Ending stocks"):  ("oil_stocks_sd",        "LB",   1000.0),
}

# First month of the published marketing year, per table
TABLE_MY_START = {6: 9, 7: 10, 8: 10}


def parse_my_start(my: str):
    """'2007/08' -> 2007; '1980' -> 1980."""
    head = str(my).split("/")[0].strip()
    return int(head) if head.isdigit() else None


def load_csv() -> pd.DataFrame:
    df = pd.read_csv(CSV_PATH, encoding="utf-8", encoding_errors="replace")
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    df["my_start_year"] = df["Marketing_Year"].map(parse_my_start)
    for col in ("Commodity_Desc2", "Attribute_Desc2",
                "Geography_Desc", "Geography_Desc2"):
        df[col] = df[col].fillna("")
    return df


def upsert_bronze(df: pd.DataFrame) -> int:
    rows = [
        (
            int(r.Table_number), r.Table_name, r.Timeperiod_Desc,
            str(r.Marketing_Year), r.my_start_year, r.MY_Definition,
            r.Commodity_Group, r.Commodity, r.Commodity_Desc2,
            r.Attribute_Desc, r.Attribute_Desc2,
            r.Geography_Desc, r.Geography_Desc2,
            None if pd.isna(r.Amount) else float(r.Amount),
            r.Unit_Desc, CSV_PATH.name,
        )
        for r in df.itertuples(index=False)
    ]
    sql = """
        INSERT INTO bronze.ers_oil_crops_yearbook
            (table_number, table_name, timeperiod_desc, marketing_year,
             my_start_year, my_definition, commodity_group, commodity,
             commodity_desc2, attribute_desc, attribute_desc2,
             geography_desc, geography_desc2, amount, unit_desc, source_file)
        VALUES %s
        ON CONFLICT (table_number, marketing_year, timeperiod_desc, commodity,
                     commodity_desc2, attribute_desc, attribute_desc2,
                     geography_desc, geography_desc2)
        DO UPDATE SET amount = EXCLUDED.amount,
                      unit_desc = EXCLUDED.unit_desc,
                      table_name = EXCLUDED.table_name,
                      ingested_at = now()
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=2000)
        conn.commit()
    return len(rows)


def map_to_silver(df: pd.DataFrame) -> int:
    out = []
    for r in df.itertuples(index=False):
        key = (int(r.Table_number), r.Attribute_Desc)
        if key not in SILVER_MAP or r.Timeperiod_Desc not in MONTHS:
            continue
        if pd.isna(r.Amount) or r.my_start_year is None:
            continue
        attribute, unit, mult = SILVER_MAP[key]
        month = MONTHS[r.Timeperiod_Desc]
        my_start_month = TABLE_MY_START[int(r.Table_number)]
        cal_year = r.my_start_year if month >= my_start_month else r.my_start_year + 1
        # normalize MY to the NASS soybean Sep-Aug convention
        nass_my = cal_year if month >= 9 else cal_year - 1
        out.append((
            "soybeans", "US", nass_my, month, cal_year,
            attribute, float(r.Amount) * mult, unit, "ERS_OCY", False,
        ))

    sql = """
        INSERT INTO silver.monthly_realized
            (commodity, country, marketing_year, month, calendar_year,
             attribute, realized_value, unit, source, is_preliminary)
        VALUES %s
        ON CONFLICT (commodity, country, marketing_year, month, attribute, source)
        DO UPDATE SET realized_value = EXCLUDED.realized_value,
                      calendar_year = EXCLUDED.calendar_year,
                      unit = EXCLUDED.unit
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, out, page_size=1000)
        conn.commit()
    return len(out)


def verify():
    """Compare ERS vs NASS for overlapping months.

    crush / oil_production_crude are the same concept and should be ~0%.
    The *_sd attributes are definitionally different series (see SILVER_MAP
    comment); their diffs are shown for reference, not as a pass/fail.
    """
    q = """
        SELECT e.attribute, COUNT(*) n_overlap,
               ROUND(AVG(ABS(e.realized_value - n.realized_value)
                         / NULLIF(n.realized_value, 0)) * 100, 2) avg_abs_pct_diff,
               ROUND(MAX(ABS(e.realized_value - n.realized_value)
                         / NULLIF(n.realized_value, 0)) * 100, 2) max_abs_pct_diff
        FROM silver.monthly_realized e
        JOIN silver.monthly_realized n
          ON n.commodity = e.commodity AND n.country = e.country
         AND n.marketing_year = e.marketing_year AND n.month = e.month
         AND n.attribute = regexp_replace(e.attribute, '_sd$', '')
         AND n.source = 'NASS_SOY_CRUSH'
        WHERE e.source = 'ERS_OCY'
        GROUP BY e.attribute ORDER BY e.attribute
    """
    cov = """
        SELECT attribute, MIN(calendar_year) y0, MAX(calendar_year) y1, COUNT(*) n
        FROM silver.monthly_realized
        WHERE source = 'ERS_OCY'
        GROUP BY attribute ORDER BY attribute
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(cov)
            print("\nERS_OCY coverage in silver.monthly_realized:")
            for row in cur.fetchall():
                r = list(dict(row).values()) if not isinstance(row, tuple) else row
                print(f"  {r[0]:<24} {r[1]}-{r[2]}  ({r[3]} months)")
            cur.execute(q)
            print("\nERS vs NASS overlap check (same MY/month/attribute):")
            for row in cur.fetchall():
                r = list(dict(row).values()) if not isinstance(row, tuple) else row
                print(f"  {r[0]:<24} n={r[1]:<4} avg diff {r[2]}%  max {r[3]}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-only", action="store_true")
    ap.add_argument("--verify", action="store_true",
                    help="Run the ERS-vs-NASS overlap check only")
    args = ap.parse_args()

    if args.verify:
        verify()
        return

    df = load_csv()
    n = upsert_bronze(df)
    print(f"bronze.ers_oil_crops_yearbook: {n} rows upserted")
    if not args.bronze_only:
        m = map_to_silver(df)
        print(f"silver.monthly_realized (ERS_OCY): {m} rows upserted")
    verify()


if __name__ == "__main__":
    main()
