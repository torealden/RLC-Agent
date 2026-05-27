"""
Add coconut_oil_food_use_subbalance + palm_kernel_oil_food_use_subbalance
tabs to us_coconut_balance_sheets.xlsm and us_palm_complex_balance_sheets.xlsm
respectively.

Mirrors scripts/build_peanut_food_use_subbalance_tab.py but for the
lauric oils. Two blocks per tab:
  1. Tier 1 balance sheet (annual ERS T32: beg, imports, dom dis,
     exports, ending) — for context
  2. Tier 2B MODELED food use sub-balance (silver.lauric_food_use_modeled)
     — confectionery / baking_food_service / food_industrial /
     non_food_industrial

Per Tore (2026-05-27): food sub-flows are modeled with explicit
assumptions documented in reference.lauric_food_use_assumptions.
Revisit when industry data improves or facility-level data lands.

Idempotent — removes existing tab before re-adding.
"""

from __future__ import annotations

import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / '.env')
from src.services.database.db_config import get_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('lauric_tab')

HEADER_FILL = PatternFill('solid', fgColor='3C7D22')
HEADER_FONT = Font(bold=True, color='FFFFFF', name='Calibri')
SECTION_FONT = Font(bold=True, name='Calibri')

TARGETS = [
    {
        'xlsm':      PROJECT_ROOT / 'models' / 'Oilseeds' / 'us_coconut_balance_sheets.xlsm',
        'tab':       'coconut_oil_food_use_subbalance',
        'commodity': 'coconut_oil',
        'bs_view':   'silver.coconut_oil_balance_sheet',
        'title':     'US COCONUT OIL FOOD USE SUB-BALANCE',
    },
    {
        'xlsm':      PROJECT_ROOT / 'models' / 'Oilseeds' / 'us_palm_complex_balance_sheets.xlsm',
        'tab':       'pk_oil_food_use_subbalance',  # shortened, 31-char Excel limit
        'commodity': 'palm_kernel_oil',
        'bs_view':   'silver.palm_kernel_oil_balance_sheet',
        'title':     'US PALM KERNEL OIL FOOD USE SUB-BALANCE',
    },
]

SUB_FLOW_ORDER = ['confectionery', 'baking_food_service', 'food_industrial', 'non_food_industrial']
SUB_FLOW_LABELS = {
    'confectionery':       'Confectionery',
    'baking_food_service': 'Baking / Food Service',
    'food_industrial':     'Food Industrial (margarine, shortening)',
    'non_food_industrial': 'Non-food Industrial (soap, cosmetics, oleochem)',
}


def build_tab(cfg):
    fp = cfg['xlsm']
    if not fp.exists():
        logger.error(f"{fp.name}: not found")
        return False

    logger.info(f"\n=== {fp.name} ===")

    # Pull data
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT marketing_year, beginning_stocks_mil_lbs, imports_mil_lbs,
                       domestic_disappearance_mil_lbs, exports_mil_lbs,
                       ending_stocks_calc_mil_lbs, imports_dependency_pct
                FROM {cfg['bs_view']}
                ORDER BY marketing_year
            """)
            bs_rows = list(cur.fetchall())

            cur.execute("""
                SELECT marketing_year, sub_flow, allocated_mil_lbs, share_pct
                FROM silver.lauric_food_use_modeled
                WHERE commodity = %s
                ORDER BY marketing_year, sub_flow
            """, (cfg['commodity'],))
            food_rows = list(cur.fetchall())

    # Aggregate food rows into {(my, sub_flow): allocated}
    food_by_my_flow = {}
    shares = {}
    for r in food_rows:
        food_by_my_flow[(r['marketing_year'], r['sub_flow'])] = float(r['allocated_mil_lbs']) if r['allocated_mil_lbs'] is not None else None
        shares[r['sub_flow']] = float(r['share_pct'])

    logger.info(f"  Tier 1: {len(bs_rows)} MYs;  Food use: {len(food_rows)} rows")

    # Backup
    backup = fp.parent / (fp.name + f'.bak_{datetime.now():%Y%m%d_%H%M%S}')
    shutil.copy2(fp, backup)
    logger.info(f"  Backup: {backup.name}")

    wb = openpyxl.load_workbook(fp, keep_vba=fp.name.endswith('.xlsm'))
    if cfg['tab'] in wb.sheetnames:
        del wb[cfg['tab']]
        logger.info(f"  Removed existing tab {cfg['tab']}")
    ws = wb.create_sheet(cfg['tab'])

    # Title
    ws.cell(1, 1, cfg['title']).font = Font(bold=True, size=14, name='Calibri')
    ws.cell(2, 1, 'Tier 1 (ERS T32 canon) + Tier 2B (modeled allocation). Million pounds, MY Oct-Sep.').font = Font(italic=True, name='Calibri')

    # ── Block 1: Tier 1 balance sheet (annual ERS T32 canon) ──
    row = 4
    ws.cell(row, 1, 'TIER 1 — Annual ERS Yearbook Table 32 canon (million pounds, Oct-Sep MY)').font = SECTION_FONT
    row += 1
    headers = ['Line item'] + [r['marketing_year'] for r in bs_rows]
    for c_idx, h in enumerate(headers, start=1):
        c = ws.cell(row, c_idx, h)
        c.font = HEADER_FONT; c.fill = HEADER_FILL
    row += 1
    fields = [
        ('Beginning Stocks',         'beginning_stocks_mil_lbs'),
        ('Imports',                  'imports_mil_lbs'),
        ('Domestic Disappearance',   'domestic_disappearance_mil_lbs'),
        ('Exports',                  'exports_mil_lbs'),
        ('Ending Stocks (calc)',     'ending_stocks_calc_mil_lbs'),
        ('Imports Dependency %',     'imports_dependency_pct'),
    ]
    for label, field in fields:
        c = ws.cell(row, 1, label)
        if label == 'Imports Dependency %':
            c.font = SECTION_FONT
        for c_idx, r in enumerate(bs_rows, start=2):
            v = r[field]
            if v is not None:
                ws.cell(row, c_idx, float(v))
        row += 1
    row += 2

    # ── Block 2: Tier 2B modeled food use ──
    ws.cell(row, 1, 'TIER 2B — MODELED food use sub-flow allocation (million pounds)').font = SECTION_FONT
    row += 1
    ws.cell(row, 1, 'Assumptions: ' + ' / '.join(f'{SUB_FLOW_LABELS[k]}={shares.get(k, 0)*100:.0f}%' for k in SUB_FLOW_ORDER)).font = Font(italic=True, name='Calibri')
    row += 1

    headers = ['Sub-flow'] + [r['marketing_year'] for r in bs_rows]
    for c_idx, h in enumerate(headers, start=1):
        c = ws.cell(row, c_idx, h)
        c.font = HEADER_FONT; c.fill = HEADER_FILL
    row += 1

    for sub_flow in SUB_FLOW_ORDER:
        ws.cell(row, 1, SUB_FLOW_LABELS[sub_flow])
        for c_idx, bs in enumerate(bs_rows, start=2):
            my = bs['marketing_year']
            v = food_by_my_flow.get((my, sub_flow))
            if v is not None:
                ws.cell(row, c_idx, v)
        row += 1

    # Total row (= ERS domestic disappearance)
    c = ws.cell(row, 1, 'Total (= Tier 1 Domestic Disappearance)')
    c.font = SECTION_FONT
    for c_idx, bs in enumerate(bs_rows, start=2):
        v = bs['domestic_disappearance_mil_lbs']
        if v is not None:
            cell = ws.cell(row, c_idx, float(v))
            cell.font = SECTION_FONT
    row += 2

    # Note
    ws.cell(row, 1, 'Note: sub-flow allocation is MODELED, not USDA-published. See reference.lauric_food_use_assumptions for shares + basis. Update assumptions there and re-run this script to refresh.').font = Font(italic=True, color='666666', name='Calibri')

    # Column widths
    ws.column_dimensions['A'].width = 50
    for c_idx in range(2, len(bs_rows) + 3):
        ws.column_dimensions[get_column_letter(c_idx)].width = 11

    wb.save(fp)
    logger.info(f"  Saved: {fp.name}")
    return True


def main():
    successes = 0
    for cfg in TARGETS:
        if build_tab(cfg):
            successes += 1
    logger.info(f"\nDone: {successes}/{len(TARGETS)} files updated")


if __name__ == '__main__':
    main()
