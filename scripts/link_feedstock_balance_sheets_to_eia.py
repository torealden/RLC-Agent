"""
Link feedstock balance sheet workbooks to eia_data.xlsm
=======================================================

For each feedstock balance sheet file (canola, corn oil, cottonseed,
palm, sunflower), populate the monthly BD/RD/SAF/coproc blocks in the
oil-specific tab with cell-to-cell formulas pointing at eia_data.xlsm.

Pattern (replicated from us_soybean_complex_bal_sheets soyoil tab):

  Each section is 12 rows (Oct..Sep marketing year) x N columns
  (marketing years across, col 2 = MY 1990/91 → col 57 = MY 2045/46).

  Section row anchors in the destination tab:
    BIODIESEL       header row 108, data rows 110-121
    RD              header row 124, data rows 126-137
    SAF             header row 140, data rows 142-153
    CO-PROCESSING   header row 156, data rows 158-169

  eia_data.xlsm row layout (same for all 4 fuel tabs):
    row 5 = Oct 1998, row 17 = Oct 1999, row 29 = Oct 2000, ...
    Generally:  row = 5 + 12 * (MY_start_year - 1998)

  Balance-sheet column → MY mapping:
    col 2 = MY 1990/91 → MY_start_year = 1990
    col 10 = MY 1998/99 (first MY where eia_data has data)
    col 57 = MY 2045/46

  Feedstock → eia_data column letter (varies by fuel tab because
  RD/SAF/coproc tabs have a "Vegetable Oils Total" subtotal col
  that BD doesn't):

                     BD    RD    SAF   CoProc
    Soybean Oil      B     B     B     B
    Canola Oil       C     C     C     C
    Corn Oil (DCO)   D     D     D     D
    Cottonseed Oil   E     E     E     E
    Palm Oil         F     F     F     F
    Sunflower/Other  G     G     G     G
    Poultry Fat      H     I     I     I
    Tallow           I     J     J     J
    White Grease     J     K     K     K
    Yellow Grease    L     M     M     M
    Other (UCO)      M     N     N     N

Usage:
  python scripts/link_feedstock_balance_sheets_to_eia.py
  python scripts/link_feedstock_balance_sheets_to_eia.py --dry-run
  python scripts/link_feedstock_balance_sheets_to_eia.py --file canola
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl.utils import get_column_letter

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('link_bs')


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Section anchor rows (Oct row of the monthly block)
SECTION_OCT_ROW = {
    'biodiesel_monthly':            110,
    'renewable_diesel_monthly':     126,
    'sustainable_aviation_monthly': 142,
    'co_processing_monthly':        158,
}

# Feedstock → eia_data column letter per fuel tab
FEEDSTOCK_COLS = {
    'soybean_oil':     {'bd': 'B', 'rd': 'B', 'saf': 'B', 'cop': 'B'},
    'canola_oil':      {'bd': 'C', 'rd': 'C', 'saf': 'C', 'cop': 'C'},
    'corn_oil':        {'bd': 'D', 'rd': 'D', 'saf': 'D', 'cop': 'D'},
    'cottonseed_oil':  {'bd': 'E', 'rd': 'E', 'saf': 'E', 'cop': 'E'},
    'palm_oil':        {'bd': 'F', 'rd': 'F', 'saf': 'F', 'cop': 'F'},
    'sunflower_oil':   {'bd': 'G', 'rd': 'G', 'saf': 'G', 'cop': 'G'},
    'poultry_fat':     {'bd': 'H', 'rd': 'I', 'saf': 'I', 'cop': 'I'},
    'tallow':          {'bd': 'I', 'rd': 'J', 'saf': 'J', 'cop': 'J'},
    'white_grease':    {'bd': 'J', 'rd': 'K', 'saf': 'K', 'cop': 'K'},
    'yellow_grease':   {'bd': 'L', 'rd': 'M', 'saf': 'M', 'cop': 'M'},
    'uco_other_grease':{'bd': 'M', 'rd': 'N', 'saf': 'N', 'cop': 'N'},
}

# Tab order in formulas matches dict order:
FUEL_TABS_ORDER = [
    ('bd',  'biodiesel_monthly'),
    ('rd',  'renewable_diesel_monthly'),
    ('saf', 'sustainable_aviation_monthly'),
    ('cop', 'co_processing_monthly'),
]

# (file, tab_in_file, feedstock_key)
FILES_TO_LINK = [
    (r'C:\dev\RLC-Agent\models\Oilseeds\us_canola_balance_sheets.xlsx',
     'canola_oil_balance_sheet', 'canola_oil'),
    (r'C:\dev\RLC-Agent\models\Oilseeds\us_corn_oil_balance_sheets.xlsx',
     'corn_oil_balance_sheet', 'corn_oil'),
    (r'C:\dev\RLC-Agent\models\Oilseeds\us_cottonseed_balance_sheets.xlsx',
     'cotton_oil_balance_sheet', 'cottonseed_oil'),
    (r'C:\dev\RLC-Agent\models\Oilseeds\us_palm_complex_balance_sheets.xlsm',
     'palm_oil_balance_sheet', 'palm_oil'),
    (r'C:\dev\RLC-Agent\models\Oilseeds\us_sunflower_balance_sheets.xlsx',
     'sunflower_oil_balance_sheet', 'sunflower_oil'),
]

# Balance sheet columns: col 2 = MY 1990/91, col 10 = MY 1998/99 (first
# eia_data MY), col 57 = MY 2045/46. We fill from col 10 onward.
FIRST_BS_COL = 10        # MY 1998/99
LAST_BS_COL = 57         # MY 2045/46
EIA_DATA_OCT_1998_ROW = 5  # row 5 in eia_data!*_monthly is Oct 1998

MONTHS_IN_MY = 12  # Oct..Sep


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def find_eia_link_index(wb: openpyxl.Workbook) -> int | None:
    """Find which external link index points at eia_data.xlsm by sheet
    name signature. Returns 1-based index, or None if not found."""
    eia_signature = {'feedstock_monthly', 'capacity_monthly',
                     'biodiesel_monthly', 'renewable_diesel_monthly'}
    for i, link in enumerate(wb._external_links, start=1):
        sheet_names = link.externalBook.sheetNames.sheetName if link.externalBook.sheetNames else []
        if eia_signature.issubset(set(sheet_names)):
            return i
    return None


def eia_data_row_for_my_col(bs_col: int, month_offset: int) -> int:
    """Given a balance-sheet column (col 10 = MY 1998/99) and a month
    offset within the MY (0 = Oct, 11 = Sep), return the eia_data row
    number."""
    my_start_year = 1990 + (bs_col - 2)  # col 2 = 1990
    # MY 1998/99 starts Oct 1998. eia_data row 5 = Oct 1998.
    # row offset from Oct 1998 = (my_start_year - 1998) * 12 + month_offset
    row_offset = (my_start_year - 1998) * 12 + month_offset
    return EIA_DATA_OCT_1998_ROW + row_offset


def link_file(file_path: str, tab_name: str, feedstock_key: str,
              dry_run: bool = False) -> dict:
    """Write formulas to one balance sheet file's monthly fuel blocks."""
    name = os.path.basename(file_path)
    if not os.path.exists(file_path):
        logger.error(f"  {name}: file not found")
        return {'cells': 0, 'errors': 1}

    if feedstock_key not in FEEDSTOCK_COLS:
        logger.error(f"  {name}: unknown feedstock {feedstock_key}")
        return {'cells': 0, 'errors': 1}

    logger.info(f"\n=== {name} :: {tab_name} ({feedstock_key}) ===")
    wb = openpyxl.load_workbook(file_path, data_only=False, keep_vba=file_path.endswith('.xlsm'))

    if tab_name not in wb.sheetnames:
        logger.error(f"  tab {tab_name} not found")
        wb.close()
        return {'cells': 0, 'errors': 1}

    eia_idx = find_eia_link_index(wb)
    if eia_idx is None:
        logger.error(f"  no eia_data external link found")
        wb.close()
        return {'cells': 0, 'errors': 1}
    logger.info(f"  eia_data link index = [{eia_idx}]")

    ws = wb[tab_name]
    cells_written = 0
    cols_used = FEEDSTOCK_COLS[feedstock_key]

    for fuel_key, eia_tab_name in FUEL_TABS_ORDER:
        eia_col_letter = cols_used[fuel_key]
        section_oct_row = SECTION_OCT_ROW[eia_tab_name]
        section_cells = 0
        for month_offset in range(MONTHS_IN_MY):
            target_row = section_oct_row + month_offset
            for bs_col in range(FIRST_BS_COL, LAST_BS_COL + 1):
                eia_row = eia_data_row_for_my_col(bs_col, month_offset)
                # eia_data tabs have max_row ~355 (BD/coproc/saf) or 703 (RD).
                # Don't write formulas referencing rows beyond max — but
                # we don't know each tab's max here. Capping at row 703.
                if eia_row > 703:
                    continue
                formula = f"=[{eia_idx}]{eia_tab_name}!${eia_col_letter}${eia_row}"
                if not dry_run:
                    ws.cell(target_row, bs_col, formula)
                section_cells += 1
                cells_written += 1
        logger.info(f"  {eia_tab_name:30}  col={eia_col_letter}  cells={section_cells}")

    if not dry_run:
        # Backup original first (in case we need to roll back)
        backup_path = file_path + f'.bak_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        shutil.copy2(file_path, backup_path)
        logger.info(f"  Backup: {os.path.basename(backup_path)}")
        wb.save(file_path)
        logger.info(f"  SAVED: {name}")
    wb.close()
    return {'cells': cells_written, 'errors': 0}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--file', help='Limit to one file by partial name match')
    args = parser.parse_args()

    total_cells = total_errors = 0
    for fp, tab, fs_key in FILES_TO_LINK:
        if args.file and args.file.lower() not in os.path.basename(fp).lower():
            continue
        result = link_file(fp, tab, fs_key, dry_run=args.dry_run)
        total_cells += result['cells']
        total_errors += result['errors']

    logger.info(f"\nDone: {total_cells} cells written, {total_errors} errors")
    if args.dry_run:
        logger.info("--dry-run: no files modified")


if __name__ == '__main__':
    main()
