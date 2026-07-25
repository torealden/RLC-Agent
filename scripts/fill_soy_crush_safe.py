"""Fill us_soy_crush.xlsm 'NASS Crush' data columns from gold.nass_soy_crush_matrix.

SAFE variant (2026-07-25): matches rows by (year, month) READ-ONLY on col A and NEVER writes the date
column. The prior fill script rewrote col A with a naive Python datetime, which win32com/COM converted
through a tz offset and stamped a stray time-component (e.g. 2026-03-01 05:00) that broke the macro's
date matching and corrupted the sheet. This script avoids that entirely: it only writes the numeric
data columns the matrix supplies (all is_formula=false attributes), leaving dates and formulas alone.

Idempotent. Writes display_value where non-null; skips NULLs (leaves the cell as-is).
Requires the workbook CLOSED. Usage:
    python scripts/fill_soy_crush_safe.py                 # fill all matrix months
    python scripts/fill_soy_crush_safe.py --since 2024    # only months in year >= 2024
"""
import argparse
import sys
from pathlib import Path

ROOT = Path(r"C:/dev/RLC-Agent")
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

WB = ROOT / "models" / "Oilseeds" / "United States" / "us_soy_crush.xlsm"
SHEET = "NASS Crush"
DATE_COL = 1
DATA_START_ROW = 5


def load_matrix(since_year: int):
    """{(year, month): {spreadsheet_column: value}} from gold.nass_soy_crush_matrix (non-null only)."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT year, month, spreadsheet_column AS col, display_value AS v
            FROM gold.nass_soy_crush_matrix
            WHERE display_value IS NOT NULL AND year >= %s
        """, (since_year,))
        out = {}
        for r in cur.fetchall():
            out.setdefault((int(r['year']), int(r['month'])), {})[int(r['col'])] = float(r['v'])
        return out


def main(since_year: int):
    data = load_matrix(since_year)
    print(f"matrix: {len(data)} months (year>={since_year}), "
          f"cols/month ~{max(len(v) for v in data.values())}")

    import win32com.client as win32
    import pythoncom
    pythoncom.CoInitialize()
    xl = win32.DispatchEx("Excel.Application")
    xl.Visible = False; xl.DisplayAlerts = False; xl.AskToUpdateLinks = False; xl.EnableEvents = False
    try:
        wb = xl.Workbooks.Open(str(WB), UpdateLinks=0)
        ws = wb.Worksheets(SHEET)
        last = ws.Cells(ws.Rows.Count, DATE_COL).End(-4162).Row  # xlUp

        # map (year, month) -> row by READING col A only (never write it)
        row_of = {}
        for r in range(DATA_START_ROW, last + 1):
            v = ws.Cells(r, DATE_COL).Value
            try:
                row_of[(v.year, v.month)] = r
            except AttributeError:
                continue

        written, missing_row = 0, 0
        for (y, m), cols in sorted(data.items()):
            r = row_of.get((y, m))
            if not r:
                missing_row += 1
                print(f"  NO ROW for {y}-{m:02d} (skipped)")
                continue
            for col, val in cols.items():
                ws.Cells(r, col).Value = val
                written += 1
        print(f"wrote {written} cells; {missing_row} matrix months had no sheet row")

        # verify readback for the gap months
        for (y, m) in [(2026, 2), (2026, 3), (2026, 4), (2026, 5)]:
            r = row_of.get((y, m))
            if r:
                print(f"  verify {y}-{m:02d} row {r}: "
                      f"crush(C)={ws.Cells(r,3).Value} meal(K)={ws.Cells(r,11).Value} "
                      f"oil(V)={ws.Cells(r,22).Value}")
        wb.Save()
        print(f"saved {WB.name}")
        wb.Close(SaveChanges=False)
    finally:
        xl.Quit(); pythoncom.CoUninitialize()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--since', type=int, default=1900, help='only fill months with year >= this')
    args = ap.parse_args()
    main(args.since)
