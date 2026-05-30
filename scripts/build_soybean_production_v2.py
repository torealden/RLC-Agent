"""V2 production workbook: one file per commodity, one tab per statistic,
vintage columns. Soybeans as the prototype.

Per Tore 2026-05-30 (after the wide-format v1 was flagged for layout +
NASS short_desc contamination):
  - File: models/Oilseeds/us_soybean_production.xlsx (NEW)
  - Tabs: area_planted, area_harvested, production, yield, _meta
  - Rows: years ascending (latest at bottom — VLOOKUP convention)
  - Columns: vintage trail
      area_planted    : Year | PP (Mar) | Acreage (Jun) | Aug | Sep | Oct | Nov | Final (Jan)
      area_harvested  : Year | Aug | Sep | Oct | Nov | Final (Jan)
      production      : same as area_harvested
      yield           : same as area_harvested
  - National only (US Total). State-level revival is a follow-up.
  - Data filter: pin short_desc to the canonical ACRES PLANTED / ACRES
    HARVESTED / YIELD MEASURED IN BU/ACRE / PRODUCTION MEASURED IN BU rows
    so percentages and farm-counts don't sneak in.

Why this redesign:
  v1 picked "latest release_date" across ALL short_descs sharing a
  statisticcat_desc. NASS publishes per-method percentages and
  irrigated-farm counts under the same statisticcat — so the AH 2022 row
  got "2,979 operations" instead of 86.17M acres, etc. V2 pins to exact
  short_descs.
"""
from __future__ import annotations

import sys, logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / '.env')

from src.services.database.db_config import get_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger('build_soybean_production_v2')

# Per-commodity short_desc map. For soybeans these are the canonical rows
# that DEFINE area planted / harvested / yield / production. Anything else
# under the same statisticcat_desc (irrigated, biotech, double-cropped, etc.)
# is excluded.
SHORT_DESC = {
    'area_planted':   'SOYBEANS - ACRES PLANTED',
    'area_harvested': 'SOYBEANS - ACRES HARVESTED',
    'yield':          'SOYBEANS - YIELD, MEASURED IN BU / ACRE',
    'production':     'SOYBEANS - PRODUCTION, MEASURED IN BU',
}

# Vintage column definitions per statistic.
# Each entry: (column label, reference_period match, source_report match)
# reference_period match: exact string OR list of strings OR None.
# source_report match: exact string OR None.
# When both ref_period and source_report are set, both must match.
# When only one is set, it's the discriminator.
AP_COLS = [
    # Match on reference_period only — NASS uses 'YEAR - MAR ACREAGE' for
    # Prospective Plantings (Mar 31) and 'YEAR - JUN ACREAGE' for the
    # June Acreage report. Source_report is unreliable for these because
    # the collector's derivation only labels rp='YEAR' rows.
    ('PP (Mar)',     ['YEAR - MAR ACREAGE'],          None),
    ('Acreage (Jun)',['YEAR - JUN ACREAGE'],          None),
    ('Aug WASDE',    ['YEAR - AUG FORECAST'],         None),
    ('Sep',          ['YEAR - SEP FORECAST'],         None),
    ('Oct',          ['YEAR - OCT FORECAST'],         None),
    ('Nov',          ['YEAR - NOV FORECAST'],         None),
    # Final = the rp='YEAR' row, where source_report=Annual CPS (Jan release)
    # filters to the canonical post-harvest final. We take the latest 'YEAR'
    # release; revisions overwrite earlier as expected.
    ('Final (Jan)',  ['YEAR'],                        None),
]

# AH/Y/P start at Aug (Tore's choice)
SEASON_COLS = [
    ('Aug WASDE',    ['YEAR - AUG FORECAST'],         None),
    ('Sep',          ['YEAR - SEP FORECAST'],         None),
    ('Oct',          ['YEAR - OCT FORECAST'],         None),
    ('Nov',          ['YEAR - NOV FORECAST'],         None),
    ('Final (Jan)',  ['YEAR'],                         'Annual Crop Production Summary'),
]

# Brand styling
HEADER_FILL = PatternFill(start_color='3C7D22', end_color='3C7D22', fill_type='solid')
HEADER_FONT = Font(name='Calibri', size=11, bold=True, color='FFFFFF')
YEAR_FONT   = Font(name='Calibri', size=10, bold=True)
DATA_FONT   = Font(name='Calibri', size=10)
ALT_FILL    = PatternFill(start_color='F4F8F1', end_color='F4F8F1', fill_type='solid')


def fetch_vintage(commodity_db: str, class_db: str, statistic: str,
                  short_desc: str) -> dict:
    """Pull all (crop_year, reference_period, source_report) -> latest value.
    Returns dict[(crop_year, ref_period, source_report)] -> (value, unit, release_date)."""
    sql = """
        WITH ranked AS (
            SELECT
                crop_year, reference_period, source_report,
                value, unit, release_date,
                ROW_NUMBER() OVER (
                    PARTITION BY crop_year, reference_period, source_report
                    ORDER BY release_date DESC
                ) AS rn
            FROM silver.crop_production
            WHERE commodity = %s AND class = %s
              AND statistic = %s AND short_desc = %s
              AND agg_level = 'NATIONAL'
              AND value IS NOT NULL
        )
        SELECT crop_year, reference_period, source_report, value, unit, release_date
        FROM ranked WHERE rn = 1
        ORDER BY crop_year
    """
    out = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (commodity_db, class_db, statistic, short_desc))
            for r in cur.fetchall():
                d = dict(r)
                key = (int(d['crop_year']), d['reference_period'], d['source_report'])
                out[key] = (float(d['value']), d['unit'], d['release_date'])
    return out


def match_value(rows: dict, year: int, ref_periods: list,
                source_report: str | None) -> tuple | None:
    """Find a row matching (year, one of ref_periods, source_report or any).
    Returns the (value, unit, release_date) tuple or None."""
    for rp in ref_periods:
        if source_report is not None:
            key = (year, rp, source_report)
            if key in rows:
                return rows[key]
        else:
            # Match any source_report for this (year, ref_period)
            for k, v in rows.items():
                if k[0] == year and k[1] == rp:
                    return v
    return None


def build_tab(ws, statistic: str, short_desc: str, col_specs: list,
              year_ge: int, year_le: int):
    """Populate one tab. Returns row count."""
    rows = fetch_vintage('soybeans', 'all_classes', statistic, short_desc)

    # Determine units (should be consistent within one short_desc)
    units = sorted({r[1] for r in rows.values()})
    unit_label = units[0] if len(units) == 1 else f'mixed: {units}'

    # Headers
    ws.cell(1, 1, value='Year').font = HEADER_FONT
    ws.cell(1, 1).fill = HEADER_FILL
    ws.cell(1, 1).alignment = Alignment(horizontal='center')
    for i, (label, _, _) in enumerate(col_specs, start=2):
        c = ws.cell(1, i, value=label)
        c.font = HEADER_FONT
        c.fill = HEADER_FILL
        c.alignment = Alignment(horizontal='center')

    # Data rows, ascending years
    row = 2
    for year in range(year_ge, year_le + 1):
        c = ws.cell(row, 1, value=year)
        c.font = YEAR_FONT
        if year % 2 == 1:
            c.fill = ALT_FILL
        for i, (_, ref_periods, src_rep) in enumerate(col_specs, start=2):
            match = match_value(rows, year, ref_periods, src_rep)
            cell = ws.cell(row, i)
            if year % 2 == 1:
                cell.fill = ALT_FILL
            if match is None:
                continue
            value, unit, rel_date = match
            cell.value = value
            cell.font = DATA_FONT
            if statistic == 'yield':
                cell.number_format = '#,##0.0'
            else:
                cell.number_format = '#,##0'
        row += 1

    # Column widths + freeze
    ws.column_dimensions['A'].width = 8
    for c in range(2, 2 + len(col_specs)):
        ws.column_dimensions[openpyxl.utils.get_column_letter(c)].width = 16
    ws.freeze_panes = 'B2'

    # Footer note with unit
    foot_row = row + 1
    ws.cell(foot_row, 1, value=f'Units: {unit_label}').font = Font(name='Calibri', size=9, italic=True)

    return row - 2  # data rows written (excluding header)


def build_meta(ws, file_meta: dict):
    ws['A1'] = 'Meta'
    ws['A1'].font = HEADER_FONT
    ws['A1'].fill = HEADER_FILL
    ws['B1'] = 'Value'
    ws['B1'].font = HEADER_FONT
    ws['B1'].fill = HEADER_FILL

    ws['A2'] = 'Generated'
    ws['B2'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ws['A3'] = 'Source'
    ws['B3'] = 'silver.crop_production (NASS Crop Production + Annual CPS)'
    ws['A4'] = 'Layout'
    ws['B4'] = 'One row per crop year, columns = vintage release. Pinned to canonical short_desc to exclude percentages/farm-counts.'
    ws['A5'] = 'Vintage definitions'
    ws['B5'] = 'PP=Prospective Plantings (Mar 31); Acreage=Jun 30 USDA Acreage report; Aug/Sep/Oct/Nov=Crop Production monthly forecast; Final=Annual Crop Production Summary (Jan)'

    row = 7
    ws[f'A{row}'] = 'Tab'
    ws[f'B{row}'] = 'Rows'
    ws[f'C{row}'] = 'Short desc'
    for c in (f'A{row}', f'B{row}', f'C{row}'):
        ws[c].font = HEADER_FONT
        ws[c].fill = HEADER_FILL
    row += 1
    for tab, info in file_meta.items():
        ws.cell(row, 1, value=tab)
        ws.cell(row, 2, value=info['rows'])
        ws.cell(row, 3, value=info['short_desc'])
        row += 1

    ws.column_dimensions['A'].width = 22
    ws.column_dimensions['B'].width = 8
    ws.column_dimensions['C'].width = 60


def main():
    out_path = PROJECT_ROOT / 'models/Oilseeds/us_soybean_production.xlsx'
    out_path.parent.mkdir(parents=True, exist_ok=True)

    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    file_meta = {}

    tabs = [
        ('area_planted',   AP_COLS),
        ('area_harvested', SEASON_COLS),
        ('production',     SEASON_COLS),
        ('yield',          SEASON_COLS),
    ]
    for stat, col_specs in tabs:
        sd = SHORT_DESC[stat]
        log.info(f'Building tab: {stat}  short_desc={sd}')
        ws = wb.create_sheet(title=stat)
        n = build_tab(ws, stat, sd, col_specs, year_ge=2000, year_le=2026)
        file_meta[stat] = {'rows': n, 'short_desc': sd}

    ws_meta = wb.create_sheet(title='_meta')
    build_meta(ws_meta, file_meta)

    wb.save(out_path)
    log.info(f'Wrote {out_path}')


if __name__ == '__main__':
    main()
