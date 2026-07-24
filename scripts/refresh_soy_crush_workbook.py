"""Refresh us_soy_crush.xlsm 'NASS Crush' column C from the DB (NASS_SOY_CRUSH) -- repoint the
soybean-seed balance sheet's crush link to our pipeline instead of a stale manual paste (6d, decision D).

The problem (6d findings): us_soybean_complex_bal_sheets.xlsm::soy_balance_sheet pulls monthly crush from
`'[3]NASS Crush'!$D{row}` in this workbook. Column C = crush (000 short tons), column D = `=C*33.33/1000`
(mil bu). The manual paste stopped at Dec 2025, so Jan-May 2026 read 0 -> the oil-yield block divided by
0 -> #DIV/0! poisoned the forward seasonalization. The DB has NASS crush through May 2026 and ties to the
existing column C EXACTLY (verified: Sep-Dec 2025 match to 3 decimals), so this fills the gap and keeps it
current on every run.

Uses Excel COM (win32com) rather than openpyxl so the .xlsm macros, charts and formatting survive the
save untouched. Idempotent: rewrites column C for every DB month (existing values already match), appends
rows for months past the last populated row. Column A gets the month date, column D the standard formula.

Usage:  python scripts/refresh_soy_crush_workbook.py         # writes + saves
        python scripts/refresh_soy_crush_workbook.py --dry   # report only, no save
"""
import argparse
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(r"C:/dev/RLC-Agent")
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from src.services.database.db_config import get_connection

WB = ROOT / "models" / "Oilseeds" / "United States" / "us_soy_crush.xlsm"
SHEET = "NASS Crush"
COL_DATE, COL_CRUSH, COL_D = 1, 3, 4      # A=date, C=crush(000 ST), D=mil bu formula
FIRST_DATA_ROW = 5                         # rows 1-4 are titles/units
ST_PER_1000 = 1000.0


def db_crush_000st():
    """{(year, month): crush in 000 short tons} from NASS_SOY_CRUSH (DB stores raw short tons)."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT calendar_year AS y, month AS m, realized_value AS v
            FROM silver.monthly_realized
            WHERE commodity='soybeans' AND source='NASS_SOY_CRUSH' AND attribute='crush'
              AND realized_value IS NOT NULL
            ORDER BY 1, 2
        """)
        return {(int(r['y']), int(r['m'])): float(r['v']) / ST_PER_1000 for r in cur.fetchall()}


def main(dry: bool):
    crush = db_crush_000st()
    if not crush:
        print("no DB crush rows -- aborting"); return
    print(f"DB NASS crush: {len(crush)} months, latest {max(crush)}")

    import win32com.client as win32
    import pythoncom
    pythoncom.CoInitialize()
    xl = win32.DispatchEx("Excel.Application")
    xl.Visible = False; xl.DisplayAlerts = False; xl.AskToUpdateLinks = False; xl.EnableEvents = False
    try:
        wb = xl.Workbooks.Open(str(WB), UpdateLinks=0)
        ws = wb.Worksheets(SHEET)
        last = ws.Cells(ws.Rows.Count, COL_DATE).End(-4162).Row   # xlUp
        # map existing date rows -> row index
        row_of = {}
        for r in range(FIRST_DATA_ROW, last + 1):
            v = ws.Cells(r, COL_DATE).Value
            if v is not None:
                try:
                    row_of[(v.year, v.month)] = r
                except AttributeError:
                    pass
        print(f"workbook '{SHEET}': data rows {FIRST_DATA_ROW}..{last}, "
              f"{len(row_of)} dated rows, last date {max(row_of) if row_of else None}")

        updated, appended = 0, 0
        next_row = last + 1
        for (y, m) in sorted(crush):
            c_val = crush[(y, m)]
            if (y, m) in row_of:
                r = row_of[(y, m)]
                cur_c = ws.Cells(r, COL_CRUSH).Value
                if cur_c is None or abs(float(cur_c) - c_val) > 1e-6:
                    if not dry:
                        ws.Cells(r, COL_CRUSH).Value = c_val
                        if not ws.Cells(r, COL_D).Formula.startswith("="):
                            ws.Cells(r, COL_D).Formula = f"=C{r}*33.33/1000"
                    updated += 1
                    print(f"  update {y}-{m:02d} row {r}: C {cur_c} -> {c_val:.3f}")
            else:
                r = next_row
                if not dry:
                    ws.Cells(r, COL_DATE).Value = datetime(y, m, 1)
                    ws.Cells(r, COL_DATE).NumberFormat = ws.Cells(r - 1, COL_DATE).NumberFormat
                    ws.Cells(r, COL_CRUSH).Value = c_val
                    ws.Cells(r, COL_CRUSH).NumberFormat = ws.Cells(r - 1, COL_CRUSH).NumberFormat
                    ws.Cells(r, COL_D).Formula = f"=C{r}*33.33/1000"
                    ws.Cells(r, COL_D).NumberFormat = ws.Cells(r - 1, COL_D).NumberFormat
                appended += 1
                next_row += 1
                print(f"  append {y}-{m:02d} row {r}: C={c_val:.3f}  D==C{r}*33.33/1000")

        print(f"\n{'DRY RUN -- no save. ' if dry else ''}would update {updated}, append {appended} rows")
        if not dry and (updated or appended):
            wb.Save()
            print(f"saved {WB.name}")
        wb.Close(SaveChanges=False)
    finally:
        xl.Quit(); pythoncom.CoUninitialize()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry', action='store_true', help='report only, do not write/save')
    args = ap.parse_args()
    main(dry=args.dry)
