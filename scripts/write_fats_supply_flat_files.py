"""Emit contract-compliant (flat_file_contract.md v1.1 LONG) fats/UCO supply+demand flat files.

Two workbooks, each with a supply tab, a demand tab, and a _meta tab:
  1. models/Oilseeds/us_tallow_supply_demand.xlsx  -> tallow_supply, tallow_demand, _meta
  2. models/Oilseeds/us_uco_supply_demand.xlsx     -> uco_supply,   uco_demand,   _meta

Supply tabs are near-passthroughs / assembly from silver.* balance tables (the vintage ladder is
preserved so Desktop's MAXIFS picks the top-ranked vintage per key). Demand tabs are the raked
allocator output (gold.bbd_feedstock_raked, latest run_day). 13-col LONG schema, value = RAW pounds.

Idempotent: rewrites both files each run. Does NOT commit/push.
"""
import sys
from pathlib import Path
import openpyxl
from openpyxl.styles import Font

ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

OUTDIR = ROOT / "models" / "Oilseeds"
COLS = ['commodity', 'class', 'series', 'marketing_year', 'period_type', 'period',
        'vintage', 'vintage_rank', 'value', 'unit', 'source', 'release_date', 'revision']

# vintage -> rank map for the UCO assembled series (uco_yg_balance has no vintage_rank column)
UCO_RANK = {'PROXY_FOOD': 40, 'PROXY_FOOD_CARRYFWD': 35, 'CENSUS': 90,
            'DERIVED': 90, 'EIA': 95, 'RULED_AMENDMENT1': 30}

TALLOW_SUPPLY_SERIES = (
    'tallow_production', 'tallow_imports', 'tallow_exports', 'tallow_biofuel_use',
    'non_bio_use', 'feed_use', 'non_bio_trend', 'eia_tallow_comparison',
    'cir_ibft_biodiesel_comparison',
)

NOTES = {
    # tallow supply
    'tallow_production': 'NASS/CENSUS_CIR/SLAUGHTER vintage ladder (rank 90/80/60)',
    'tallow_imports': 'Census gross imports by grade (EBFT/IBFT)',
    'tallow_exports': 'Census gross exports by grade (EBFT/IBFT)',
    'tallow_biofuel_use': 'modeled biofuel consumption, all grades',
    'non_bio_use': 'IBFT non-biofuel demand (model)',
    'feed_use': 'IBFT feed demand (model)',
    'non_bio_trend': 'IBFT non-biofuel trend line (model)',
    'eia_tallow_comparison': 'EIA comparison only -- never load-bearing',
    'cir_ibft_biodiesel_comparison': 'CIR-era IBFT biodiesel comparison only',
    # uco supply
    'uco_collection': 'UCO-grade collection, food-oil proxy',
    'yg_collection': 'YG-grade collection, food-oil proxy',
    'uco_imports': 'Census gross UCO imports (TOTAL)',
    'uco_exports': 'Census gross UCO exports (TOTAL)',
    'uco_biofuel_use': 'derived UCO biofuel consumption',
    'yg_biofuel_use': 'ruled 0 per UCO Amendment 1',
    # demand (both)
    'biofuel_use_biodiesel': 'raked allocator: biodiesel feedstock use',
    'biofuel_use_renewable_diesel': 'raked allocator: renewable diesel feedstock use',
    'biofuel_use_saf': 'raked allocator: SAF feedstock use',
    'biofuel_use_coprocessing': 'raked allocator: co-processing feedstock use',
    'biofuel_use_total': 'raked allocator: sum across all fuel types',
}
# non_bio_use override note for the UCO workbook (different meaning than tallow)
UCO_NONBIO_NOTE = 'YG-grade collection to non-bio, YG biofuel=0'


def pm(month):
    """month int -> 'M01'..'M12'"""
    return f"M{int(month):02d}"


def sort_key(r):
    return (r['series'], r['class'], r['marketing_year'], r['period'], r['vintage_rank'])


def dedupe(rows):
    """§7 uniqueness: one row per (commodity,class,series,marketing_year,period,vintage_rank)."""
    seen = {}
    for r in rows:
        k = (r['commodity'], r['class'], r['series'], r['marketing_year'], r['period'], r['vintage_rank'])
        if k not in seen:
            seen[k] = r
    return list(seen.values())


def write_tab(wb, title, rows):
    ws = wb.create_sheet(title)
    ws.append(COLS)
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for r in sorted(dedupe(rows), key=sort_key):
        ws.append([r[c] for c in COLS])
    return ws


def write_meta(wb, series_rows, note_override=None):
    """series_rows: list of dicts w/ series, source, unit, vintage_set."""
    wm = wb.create_sheet("_meta")
    wm.append(['series', 'source', 'unit', 'vintage_set', 'last_updated', 'notes'])
    for cell in wm[1]:
        cell.font = Font(bold=True)
    for m in series_rows:
        note = (note_override or {}).get(m['series'], NOTES.get(m['series'], ''))
        wm.append([m['series'], m['source'], m['unit'], m['vintage_set'], '', note])


def meta_from_rows(rows):
    """Build one _meta row per series from emitted data rows (preserving first-seen order)."""
    agg = {}
    order = []
    for r in rows:
        s = r['series']
        if s not in agg:
            agg[s] = {'series': s, 'source': set(), 'unit': set(), 'vintage_set': set()}
            order.append(s)
        agg[s]['source'].add(r['source'])
        agg[s]['unit'].add(r['unit'])
        agg[s]['vintage_set'].add(r['vintage'])
    out = []
    for s in order:
        a = agg[s]
        out.append({'series': s,
                    'source': ', '.join(sorted(a['source'])),
                    'unit': ', '.join(sorted(a['unit'])),
                    'vintage_set': ', '.join(sorted(a['vintage_set']))})
    return out


# ---------------------------------------------------------------- build data
with get_connection() as conn:
    cur = conn.cursor()
    max_run = None
    cur.execute("SELECT max(run_day) m FROM gold.bbd_feedstock_raked")
    max_run = cur.fetchone()['m']

    # ---- TALLOW SUPPLY (near-passthrough of silver.tallow_balance) ----
    cur.execute(
        "SELECT class, series, year, month, value_lbs, vintage, vintage_rank "
        "FROM silver.tallow_balance WHERE series = ANY(%s)",
        (list(TALLOW_SUPPLY_SERIES),))
    tallow_supply = [{
        'commodity': 'tallow', 'class': r['class'], 'series': r['series'],
        'marketing_year': r['year'], 'period_type': 'cal_month', 'period': pm(r['month']),
        'vintage': r['vintage'], 'vintage_rank': r['vintage_rank'], 'value': r['value_lbs'],
        'unit': 'LB', 'source': r['vintage'], 'release_date': '', 'revision': '',
    } for r in cur.fetchall()]

    # ---- UCO SUPPLY (assembled) ----
    uco_supply = []

    def uco_row(series, year, month, value, vintage):
        return {'commodity': 'uco_yg', 'class': 'ALL', 'series': series,
                'marketing_year': year, 'period_type': 'cal_month', 'period': pm(month),
                'vintage': vintage, 'vintage_rank': UCO_RANK[vintage], 'value': value,
                'unit': 'LB', 'source': vintage, 'release_date': '', 'revision': ''}

    # uco_collection + yg_collection from silver.uco_yg_balance
    cur.execute("SELECT series, year, month, value_lbs, vintage FROM silver.uco_yg_balance "
                "WHERE series IN ('uco_collection','yg_collection','uco_biofuel_use')")
    yg_collection_rows = []   # keep for non_bio_use + yg_biofuel_use derivation
    uco_collection_keys = []  # (year,month) where uco_collection exists
    for r in cur.fetchall():
        uco_supply.append(uco_row(r['series'], r['year'], r['month'], r['value_lbs'], r['vintage']))
        if r['series'] == 'yg_collection':
            yg_collection_rows.append((r['year'], r['month'], r['value_lbs']))
        if r['series'] == 'uco_collection':
            uco_collection_keys.append((r['year'], r['month']))

    # uco_imports / uco_exports GROSS from silver.uco_imports (country='TOTAL'), mil_lbs*1e6
    cur.execute("SELECT year, month, flow, mil_lbs FROM silver.uco_imports WHERE country='TOTAL'")
    imp_rows = cur.fetchall()
    for r in imp_rows:
        series = 'uco_imports' if r['flow'] == 'import' else 'uco_exports'
        uco_supply.append(uco_row(series, r['year'], r['month'], float(r['mil_lbs']) * 1e6, 'CENSUS'))

    # yg_biofuel_use = 0 for every (year,month) that uco_collection exists (Amendment 1 R1)
    for (y, m) in uco_collection_keys:
        uco_supply.append(uco_row('yg_biofuel_use', y, m, 0, 'RULED_AMENDMENT1'))

    # non_bio_use = yg_collection values (YG-grade collection flows to non-bio)
    for (y, m, v) in yg_collection_rows:
        uco_supply.append(uco_row('non_bio_use', y, m, v, 'DERIVED'))

    # ---- DEMAND (raked allocator) ----
    def demand_rows(commodity, codes):
        cur.execute(
            "SELECT extract(year from period)::int yr, extract(month from period)::int mo, "
            "fuel_type, sum(raked_mil_lbs) mil FROM gold.bbd_feedstock_raked "
            "WHERE run_day = %s AND feedstock_code = ANY(%s) "
            "GROUP BY 1,2,3", (max_run, list(codes)))
        raw = cur.fetchall()
        rows = []
        totals = {}  # (yr,mo) -> sum
        for r in raw:
            series = 'biofuel_use_' + str(r['fuel_type']).lower().replace(' ', '_')
            val = float(r['mil']) * 1e6
            rows.append({'commodity': commodity, 'class': 'ALL', 'series': series,
                         'marketing_year': r['yr'], 'period_type': 'cal_month', 'period': pm(r['mo']),
                         'vintage': 'RAKED', 'vintage_rank': 95, 'value': val, 'unit': 'LB',
                         'source': 'ALLOCATOR_RAKED', 'release_date': '', 'revision': ''})
            totals[(r['yr'], r['mo'])] = totals.get((r['yr'], r['mo']), 0.0) + val
        for (yr, mo), tot in totals.items():
            rows.append({'commodity': commodity, 'class': 'ALL', 'series': 'biofuel_use_total',
                         'marketing_year': yr, 'period_type': 'cal_month', 'period': pm(mo),
                         'vintage': 'RAKED', 'vintage_rank': 95, 'value': tot, 'unit': 'LB',
                         'source': 'ALLOCATOR_RAKED', 'release_date': '', 'revision': ''})
        return rows

    tallow_demand = demand_rows('tallow', ('EBFT', 'IBFT', 'BFT'))
    uco_demand = demand_rows('uco_yg', ('UCO', 'YG'))

# ---------------------------------------------------------------- write files
OUTDIR.mkdir(parents=True, exist_ok=True)

# File 1: tallow
wb1 = openpyxl.Workbook(); wb1.remove(wb1.active)
write_tab(wb1, 'tallow_supply', tallow_supply)
write_tab(wb1, 'tallow_demand', tallow_demand)
write_meta(wb1, meta_from_rows(sorted(dedupe(tallow_supply), key=sort_key) +
                               sorted(dedupe(tallow_demand), key=sort_key)))
f1 = OUTDIR / "us_tallow_supply_demand.xlsx"
wb1.save(f1)

# File 2: uco
wb2 = openpyxl.Workbook(); wb2.remove(wb2.active)
write_tab(wb2, 'uco_supply', uco_supply)
write_tab(wb2, 'uco_demand', uco_demand)
write_meta(wb2, meta_from_rows(sorted(dedupe(uco_supply), key=sort_key) +
                               sorted(dedupe(uco_demand), key=sort_key)),
           note_override={'non_bio_use': UCO_NONBIO_NOTE})
f2 = OUTDIR / "us_uco_supply_demand.xlsx"
wb2.save(f2)

print(f"wrote {f1}")
print(f"wrote {f2}")

# ---------------------------------------------------------------- verify
def verify(path):
    wb = openpyxl.load_workbook(path)
    print(f"\n===== {path.name} =====")
    print("sheets:", wb.sheetnames)
    for name in wb.sheetnames:
        if name == '_meta':
            continue
        ws = wb[name]
        header = [c.value for c in ws[1]]
        assert header == COLS, f"HEADER MISMATCH on {name}: {header}"
        data = list(ws.iter_rows(min_row=2, values_only=True))
        series = sorted({row[2] for row in data})
        print(f"  [{name}] header OK | {len(data)} data rows | series: {series}")
    return wb


wb1v = verify(f1)
wb2v = verify(f2)

# Spot-check: tallow_production IBFT M06 MY2024
print("\n-- spot: tallow_production IBFT M06 MY2024 --")
for row in wb1v['tallow_supply'].iter_rows(min_row=2, values_only=True):
    d = dict(zip(COLS, row))
    if d['series'] == 'tallow_production' and d['class'] == 'IBFT' and d['period'] == 'M06' and d['marketing_year'] == 2024:
        print("  ", d['vintage'], "rank", d['vintage_rank'], "value", d['value'])

def cy_sum(wb, tab, series, cls=None, year=2024):
    tot = 0.0
    for row in wb[tab].iter_rows(min_row=2, values_only=True):
        d = dict(zip(COLS, row))
        if d['series'] == series and d['marketing_year'] == year and (cls is None or d['class'] == cls):
            tot += float(d['value'] or 0)
    return tot

print("\n-- CY2024 control totals --")
print(f"  tallow tallow_biofuel_use (ALL): {cy_sum(wb1v,'tallow_supply','tallow_biofuel_use','ALL')/1e9:.3f} B lb  (expect ~4.6-4.7)")
print(f"  tallow biofuel_use_total (demand): {cy_sum(wb1v,'tallow_demand','biofuel_use_total')/1e9:.3f} B lb  (expect ~4.4-4.7)")
print(f"  uco    uco_biofuel_use: {cy_sum(wb2v,'uco_supply','uco_biofuel_use')/1e9:.3f} B lb  (expect ~8.7)")
print(f"  uco    biofuel_use_total (demand): {cy_sum(wb2v,'uco_demand','biofuel_use_total')/1e9:.3f} B lb  (expect ~8+)")

# yg_biofuel_use all zeros?
yg_vals = [dict(zip(COLS, row))['value'] for row in wb2v['uco_supply'].iter_rows(min_row=2, values_only=True)
           if dict(zip(COLS, row))['series'] == 'yg_biofuel_use']
print(f"\n  yg_biofuel_use rows: {len(yg_vals)}, all zero: {all((v or 0) == 0 for v in yg_vals)}")

# distinct fuel-type demand series
for label, wb, tab in [('tallow', wb1v, 'tallow_demand'), ('uco', wb2v, 'uco_demand')]:
    s = sorted({dict(zip(COLS, row))['series'] for row in wb[tab].iter_rows(min_row=2, values_only=True)})
    print(f"  {label} demand series: {s}")
