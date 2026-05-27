"""
Add a peanut_food_use_subbalance tab to us_peanut_bal_sheets.xlsm.

Writes values (not formulas) from silver.peanut_food_use_annual and
silver.peanut_food_use_monthly. Two blocks:
  1. Annual ERS Yearbook Table 12 canon — MY columns, 5 sub-flows + total
  2. Monthly NASS detail — calendar year × month rows, 5 sub-flows + total

The 5 sub-flows: Peanut Butter, Peanut Candy, Snack Peanuts, Other Edible,
Clean In-shell.

Idempotent: removes the existing tab (if present) before re-adding.
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / '.env')
from src.services.database.db_config import get_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('peanut_food_use_tab')

XLSM_PATH = PROJECT_ROOT / 'models' / 'Oilseeds' / 'us_peanut_bal_sheets.xlsm'
TAB_NAME = 'peanut_food_use_subbalance'

# Internal-xlsx header convention: forest green fill, bold white Calibri
HEADER_FILL = PatternFill('solid', fgColor='3C7D22')
HEADER_FONT = Font(bold=True, color='FFFFFF', name='Calibri')
SECTION_FONT = Font(bold=True, name='Calibri')


def write_annual_block(ws, start_row: int, annual_rows: list) -> int:
    """Write annual ERS canon block. Returns next available row."""
    # Title
    c = ws.cell(start_row, 1, 'ANNUAL — ERS Oil Crops Yearbook Table 12 (food use, shelled basis, million pounds)')
    c.font = SECTION_FONT
    start_row += 1

    if not annual_rows:
        ws.cell(start_row, 1, '(no data)')
        return start_row + 2

    # Header row: MY columns
    ws.cell(start_row, 1, 'Sub-flow').font = HEADER_FONT
    ws.cell(start_row, 1).fill = HEADER_FILL
    for col_idx, row in enumerate(annual_rows, start=2):
        c = ws.cell(start_row, col_idx, row['marketing_year'])
        c.font = HEADER_FONT
        c.fill = HEADER_FILL
    start_row += 1

    # Sub-flow rows
    fields = [
        ('Peanut Butter',     'peanut_butter_mil_lbs'),
        ('Peanut Candy',      'peanut_candy_mil_lbs'),
        ('Snack Peanuts',     'snack_peanuts_mil_lbs'),
        ('Other Edible',      'other_food_mil_lbs'),
        ('Clean In-shell',    'clean_in_shell_mil_lbs'),
        ('Total Food Use',    'total_food_use_mil_lbs'),
    ]
    for label, field in fields:
        c = ws.cell(start_row, 1, label)
        if label == 'Total Food Use':
            c.font = SECTION_FONT
        for col_idx, row in enumerate(annual_rows, start=2):
            v = row[field]
            if v is not None:
                ws.cell(start_row, col_idx, float(v))
                if label == 'Total Food Use':
                    ws.cell(start_row, col_idx).font = SECTION_FONT
        start_row += 1
    return start_row + 1  # blank line


def write_monthly_block(ws, start_row: int, monthly_rows: list) -> int:
    """Write monthly NASS detail block."""
    c = ws.cell(start_row, 1, 'MONTHLY — NASS Peanut Stocks & Processing (food use, shelled basis, million pounds)')
    c.font = SECTION_FONT
    start_row += 1

    if not monthly_rows:
        ws.cell(start_row, 1, '(no data)')
        return start_row + 2

    headers = ['Period', 'Year', 'Month', 'MY Start',
               'Peanut Butter', 'Peanut Candy', 'Snack Peanuts',
               'Other Edible', 'Clean In-shell', 'Total Food Use']
    for col_idx, h in enumerate(headers, start=1):
        c = ws.cell(start_row, col_idx, h)
        c.font = HEADER_FONT
        c.fill = HEADER_FILL
    start_row += 1

    for r in monthly_rows:
        vals = [
            str(r['period']),
            r['year'],
            r['month'],
            r['marketing_year_start'],
            float(r['peanut_butter_mil_lbs']) if r['peanut_butter_mil_lbs'] is not None else None,
            float(r['peanut_candy_mil_lbs']) if r['peanut_candy_mil_lbs'] is not None else None,
            float(r['snack_peanuts_mil_lbs']) if r['snack_peanuts_mil_lbs'] is not None else None,
            float(r['other_food_mil_lbs']) if r['other_food_mil_lbs'] is not None else None,
            float(r['clean_in_shell_mil_lbs']) if r['clean_in_shell_mil_lbs'] is not None else None,
            float(r['total_food_use_mil_lbs']) if r['total_food_use_mil_lbs'] is not None else None,
        ]
        for col_idx, v in enumerate(vals, start=1):
            if v is not None:
                ws.cell(start_row, col_idx, v)
        start_row += 1
    return start_row


def main():
    if not XLSM_PATH.exists():
        logger.error(f'File not found: {XLSM_PATH}')
        sys.exit(1)

    # Pull silver data
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT marketing_year,
                       peanut_butter_mil_lbs, peanut_candy_mil_lbs,
                       snack_peanuts_mil_lbs, other_food_mil_lbs,
                       clean_in_shell_mil_lbs, total_food_use_mil_lbs
                FROM silver.peanut_food_use_annual
                ORDER BY marketing_year
            """)
            annual_rows = list(cur.fetchall())
            cur.execute("""
                SELECT period, year, month, marketing_year_start,
                       peanut_butter_mil_lbs, peanut_candy_mil_lbs,
                       snack_peanuts_mil_lbs, other_food_mil_lbs,
                       clean_in_shell_mil_lbs, total_food_use_mil_lbs
                FROM silver.peanut_food_use_monthly
                ORDER BY period
            """)
            monthly_rows = list(cur.fetchall())

    logger.info(f'Annual rows: {len(annual_rows)} MYs')
    logger.info(f'Monthly rows: {len(monthly_rows)} months')

    # Backup
    backup = XLSM_PATH.parent / (XLSM_PATH.name + f'.bak_{datetime.now():%Y%m%d_%H%M%S}')
    shutil.copy2(XLSM_PATH, backup)
    logger.info(f'Backup: {backup.name}')

    # Open keeping VBA
    wb = openpyxl.load_workbook(XLSM_PATH, keep_vba=True)
    if TAB_NAME in wb.sheetnames:
        del wb[TAB_NAME]
        logger.info(f'Removed existing tab {TAB_NAME}')

    ws = wb.create_sheet(TAB_NAME)
    # Title
    c = ws.cell(1, 1, 'US PEANUT FOOD USE SUB-BALANCE')
    c.font = Font(bold=True, size=14, name='Calibri')
    c = ws.cell(2, 1, 'Tier 2B of the peanut complex balance sheet model (docs/specs/peanut_balance_sheet_model.md)')
    c.font = Font(italic=True, name='Calibri')
    next_row = 4
    next_row = write_annual_block(ws, next_row, annual_rows)
    next_row = write_monthly_block(ws, next_row, monthly_rows)

    # Reasonable col widths
    ws.column_dimensions['A'].width = 26
    for c_idx in range(2, 60):
        ws.column_dimensions[get_column_letter(c_idx)].width = 11

    wb.save(XLSM_PATH)
    logger.info(f'Saved: {XLSM_PATH}')


if __name__ == '__main__':
    main()
