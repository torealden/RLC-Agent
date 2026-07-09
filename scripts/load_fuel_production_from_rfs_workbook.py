"""
Load fuel production actuals into silver.fuel_production_forecast from the
RFS data workbook (models/Biofuels/rfs_data.xlsm, sheet 'fuel_prod_by_type').

This REPLACES the older 'balance_sheet' driver for EMTS-covered fuel types with
the workbook's RIN-reported DOMESTIC production (million gallons), which is
current through 2026-05 and already carries the biodiesel / renewable_diesel /
SAF split done. Co-Processing is empty in the workbook for recent periods and is
skipped (never written as zero).

Sheet layout (fuel_prod_by_type):
  - Dates in column A starting row 4 (datetime, first of month).
  - Row 2 = sub-column (Domestic / Imported / Total Production).
  - Row 3 = units ('million gallons').
  - Fuel blocks (Domestic column used for the load):
        Biodiesel                 Domestic=B  Total=D  -> 'biodiesel'
        Renewable Diesel          Domestic=F  Total=H  -> 'renewable_diesel'
        Co-Processing             Domestic=J  Total=L  -> 'coprocessing'
        Sustainable Aviation Fuel Domestic=N  Total=P  -> 'saf'

We UPSERT the DOMESTIC value with is_forecast=FALSE, source='rfs_data_workbook'
for every month with actual (non-None) data. ON CONFLICT target is the table's
real unique constraint: (period, fuel_type).

Usage:
    python scripts/load_fuel_production_from_rfs_workbook.py
"""

import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv('.env')

import openpyxl

from src.services.database.db_config import get_connection

WORKBOOK = os.path.join('models', 'Biofuels', 'rfs_data.xlsm')
SHEET = 'fuel_prod_by_type'
DATA_START_ROW = 4

# fuel_type -> (domestic_col, total_col) 1-based column indices
# B=2 D=4 | F=6 H=8 | J=10 L=12 | N=14 P=16
FUEL_COLS = {
    'biodiesel':        (2, 4),
    'renewable_diesel': (6, 8),
    'coprocessing':     (10, 12),
    'saf':              (14, 16),
}

UPSERT_SQL = """
    INSERT INTO silver.fuel_production_forecast
        (period, fuel_type, production_mmgal, is_forecast, source, source_file, updated_at)
    VALUES (%s, %s, %s, FALSE, 'rfs_data_workbook', %s, NOW())
    ON CONFLICT (period, fuel_type) DO UPDATE SET
        production_mmgal = EXCLUDED.production_mmgal,
        is_forecast      = FALSE,
        source           = 'rfs_data_workbook',
        source_file      = EXCLUDED.source_file,
        updated_at       = NOW()
    RETURNING (xmax = 0) AS is_insert
"""


def _num(v):
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f


def read_workbook():
    """Return list of (period_date, fuel_type, domestic, total) for non-None domestic rows."""
    wb = openpyxl.load_workbook(WORKBOOK, data_only=True, read_only=True)
    ws = wb[SHEET]
    rows = []
    for row in ws.iter_rows(min_row=DATA_START_ROW):
        a = row[0].value  # column A date
        if a is None:
            continue
        if hasattr(a, 'date'):
            period = a.date() if not isinstance(a, date) else a
        else:
            # not a date -> skip
            continue
        period = date(period.year, period.month, 1)
        for fuel_type, (dcol, tcol) in FUEL_COLS.items():
            dom = _num(row[dcol - 1].value)
            tot = _num(row[tcol - 1].value)
            if dom is None or dom == 0:
                # skip None/blank AND explicit zeros -> never write zeros.
                # (Co-Processing is a real 0.0 for Feb-May 2026; those months
                #  should not become spurious 0-production allocation rows.)
                continue
            rows.append((period, fuel_type, dom, tot))
    wb.close()
    return rows


def main():
    rows = read_workbook()
    if not rows:
        print("No workbook rows found — aborting.")
        return

    source_file = os.path.basename(WORKBOOK)
    inserted = updated = 0
    with get_connection() as conn:
        cur = conn.cursor()
        for period, fuel_type, dom, tot in rows:
            cur.execute(UPSERT_SQL, (period, fuel_type, dom, source_file))
            res = cur.fetchone()
            is_insert = res[0] if not isinstance(res, dict) else res['is_insert']
            if is_insert:
                inserted += 1
            else:
                updated += 1
        conn.commit()

    periods = sorted(set(r[0] for r in rows))
    print(f"Loaded {len(rows)} rows into silver.fuel_production_forecast "
          f"({inserted} inserted, {updated} updated).")
    print(f"Period coverage: {periods[0]} .. {periods[-1]}")

    # Sanity print: 2026-01..05 Domestic (loaded) vs Total (comparison)
    print("\n2026 Jan-May: Domestic (loaded) vs Total (workbook) [million gallons]")
    print(f"{'period':<12}{'fuel_type':<20}{'Domestic':>12}{'Total':>12}")
    for period, fuel_type, dom, tot in sorted(rows):
        if period.year == 2026 and period.month <= 5:
            tot_s = f"{tot:.2f}" if tot is not None else "None"
            print(f"{str(period):<12}{fuel_type:<20}{dom:>12.2f}{tot_s:>12}")


if __name__ == '__main__':
    main()
