"""Emit contract-compliant (flat_file_contract.md v1.1 LONG) SBO + canola oil supply/demand flat files.

Companion to write_fats_supply_flat_files.py (which does the 6 fats/greases). The vegetable oils have
a different balance shape than the fats — crush-derived crude production, an edible/industrial
disappearance split (SBO), and canola's single removal-for-processing line — so they get their own
writer, same 13-col LONG contract and same non-bio ruling (disappearance-based, 2026-07-13).

  models/Oilseeds/us_soybean_oil_supply_demand.xlsx -> soybean_oil_supply, soybean_oil_demand, _meta
  models/Oilseeds/us_canola_oil_supply_demand.xlsx  -> canola_oil_supply,  canola_oil_demand,  _meta

Supply: production (NASS crude oil), imports/exports (Census HS 1507=soy oil / 1514=canola oil, all
subcodes summed per flow -> kg x 2.20462 -> LB), ending stocks (NASS). Demand: biofuel (raked
allocator SBO / CO, one line per fuel type incl. coprocessing + SAF), food_use (SBO NASS refined
edible), aggregate non_biofuel_use, and its nonbiofuel_use_<end_use> component split via
reference.nonbio_enduse_shares (components sum to the total). Value = RAW pounds. Idempotent full
rewrite. See docs/specs/nonbio_demand_breakout_categories.md.
"""
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
import openpyxl
from openpyxl.styles import Font
from src.services.database.db_config import get_connection

# Convention (Tore, 2026-07-16): flat files live in commodity\country folders. The United States
# oils file is the ACTIVE one Desktop links; the bare models/Oilseeds/*.xlsx copies are stale.
OUTDIR = ROOT / "models" / "Oilseeds" / "United States"
KG_TO_LB = 2.20462
COLS = ['commodity', 'class', 'series', 'marketing_year', 'period_type', 'period',
        'vintage', 'vintage_rank', 'value', 'unit', 'source', 'release_date', 'revision']


def pm(month):
    return f"M{int(month):02d}"


def row(commodity, series, year, month, value, vintage, rank, source):
    return {'commodity': commodity, 'class': 'ALL', 'series': series, 'marketing_year': year,
            'period_type': 'cal_month', 'period': pm(month), 'vintage': vintage, 'vintage_rank': rank,
            'value': float(value), 'unit': 'LB', 'source': source, 'release_date': '', 'revision': ''}


def sort_key(r):
    return (r['series'], r['class'], r['marketing_year'], r['period'], r['vintage_rank'])


def dedupe(rows):
    seen = {}
    for r in rows:
        k = (r['commodity'], r['class'], r['series'], r['marketing_year'], r['period'], r['vintage_rank'])
        seen.setdefault(k, r)
    return list(seen.values())


def write_tab(wb, title, rows):
    ws = wb.create_sheet(title)
    ws.append(COLS)
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for r in sorted(dedupe(rows), key=sort_key):
        ws.append([r[c] for c in COLS])


def write_meta(wb, rows, notes):
    wm = wb.create_sheet("_meta")
    wm.append(['series', 'source', 'unit', 'last_updated', 'notes'])
    for cell in wm[1]:
        cell.font = Font(bold=True)
    seen = {}
    for r in rows:
        seen.setdefault(r['series'], r['source'])
    for series, source in seen.items():
        wm.append([series, source, 'LB', '', notes.get(series, '')])


NOTES = {
    'production': 'NASS crude oil production (oil_production_crude), raw lb',
    'stocks': 'NASS ending stocks (oil_stocks), raw lb',
    'imports': 'Census HS 1507 (soy) / 1514 (canola) imports, all subcodes summed, kg -> lb',
    'exports': 'Census HS 1507 (soy) / 1514 (canola) exports, all subcodes summed, kg -> lb',
    'biofuel_use_biodiesel': 'raked allocator: biodiesel feedstock use',
    'biofuel_use_renewable_diesel': 'raked allocator: renewable diesel feedstock use',
    'biofuel_use_total': 'raked allocator: sum across fuel types',
    'food_use': 'NASS refined edible use (oil_refined_edible_use)',
    'industrial_use': 'NASS refined inedible use (oil_refined_inedible_use)',
    'non_biofuel_use': 'aggregate non-biofuel use (closes the sheet). SBO = food+industrial '
                       '(disappearance); canola = removal_for_processing - biofuel. Ruled 2026-07-13.',
}


def nass_series(cur, commodity, attribute, out_series, out_commodity):
    # source IN NASS_* only — silver.monthly_realized also holds ABIOVE (Brazil) rows under
    # commodity='soybeans' with 0/NA US values that would otherwise clobber the real NASS figure.
    cur.execute("""SELECT calendar_year yr, month, realized_value v FROM silver.monthly_realized
                   WHERE commodity=%s AND attribute=%s AND realized_value IS NOT NULL
                     AND source LIKE 'NASS%%'
                   ORDER BY 1,2""", (commodity, attribute))
    return [row(out_commodity, out_series, r['yr'], r['month'], r['v'], 'NASS_FATS_OILS', 90, 'NASS')
            for r in cur.fetchall()]


def census_trade(cur, hs_prefix, out_commodity):
    # country_code='-' is the "TOTAL FOR ALL COUNTRIES" row. The table also carries OVERLAPPING
    # country-group aggregates (LAFTA, OECD, SOUTH AMERICA, USMCA, ...) — summing those double-counts
    # massively (soy-oil exports came out ~6x high). Use only the TOTAL row per subcode.
    cur.execute("""SELECT year yr, month, flow, sum(quantity) kg FROM bronze.census_trade
                   WHERE hs_code LIKE %s AND quantity IS NOT NULL AND country_code='-'
                   GROUP BY 1,2,3 ORDER BY 1,2""",
                (hs_prefix + '%',))
    out = []
    for r in cur.fetchall():
        series = 'imports' if str(r['flow']).lower().startswith('import') else 'exports'
        out.append(row(out_commodity, series, r['yr'], r['month'], float(r['kg']) * KG_TO_LB,
                       'CENSUS', 90, 'CENSUS_trade'))
    return out


def raked_demand(cur, feedstock_code, out_commodity):
    cur.execute("""SELECT extract(year from period)::int yr, extract(month from period)::int mo,
                     fuel_type, sum(raked_mil_lbs) mil FROM gold.bbd_feedstock_raked
                   WHERE run_day=(SELECT max(run_day) FROM gold.bbd_feedstock_raked)
                     AND feedstock_code=%s GROUP BY 1,2,3""", (feedstock_code,))
    rows, totals = [], {}
    for r in cur.fetchall():
        s = 'biofuel_use_' + str(r['fuel_type']).lower().replace(' ', '_')
        v = float(r['mil']) * 1e6
        rows.append(row(out_commodity, s, r['yr'], r['mo'], v, 'RAKED', 95, 'ALLOCATOR_RAKED'))
        totals[(r['yr'], r['mo'])] = totals.get((r['yr'], r['mo']), 0.0) + v
    for (yr, mo), tot in totals.items():
        rows.append(row(out_commodity, 'biofuel_use_total', yr, mo, tot, 'RAKED', 95, 'ALLOCATOR_RAKED'))
    return rows, totals


def by_period(rows, series):
    return {(r['marketing_year'], r['period']): r['value'] for r in rows if r['series'] == series}


def nonbio_components(cur, commodity, demand_rows):
    """Split each non_biofuel_use TOTAL into end-use component lines via the SEASONAL
    (calendar-month) shares in reference.nonbio_enduse_shares_monthly — each month uses its own
    share so the components breathe seasonally instead of sitting at a flat percentage. Falls back
    to the flat annual reference.nonbio_enduse_shares if a commodity has no monthly rows.
    Components sum to the total within each month (shares sum to 1.0), so the sheet still closes.
    Vintage flags a measured Census share vs a modeled/analog assumption."""
    cur.execute("""SELECT month, end_use, share_pct, measured
                   FROM reference.nonbio_enduse_shares_monthly WHERE commodity=%s""", (commodity,))
    monthly = defaultdict(list)
    for s in cur.fetchall():
        monthly[s['month']].append(s)
    if not monthly:  # fallback: flat annual shares across every month
        cur.execute("""SELECT end_use, share_pct, measured FROM reference.nonbio_enduse_shares
                       WHERE commodity=%s""", (commodity,))
        flat = cur.fetchall()
        monthly = {m: flat for m in range(1, 13)}
    out = []
    for r in demand_rows:
        if r['series'] != 'non_biofuel_use':
            continue
        tot = float(r['value'] or 0)
        mo = int(r['period'][1:])
        ms = monthly.get(mo, [])
        ssum = sum(float(s['share_pct']) for s in ms) or 1.0  # normalize -> exact closure
        for s in ms:
            vint = 'NONBIO_MEASURED' if s['measured'] else 'NONBIO_MODELED'
            src = ('Census 2006-2011 seasonal end-use share x non_biofuel_use' if s['measured']
                   else 'analog/modeled seasonal end-use share x non_biofuel_use')
            out.append(row(commodity, 'nonbiofuel_use_' + s['end_use'], r['marketing_year'],
                           mo, tot * float(s['share_pct']) / ssum, vint, r['vintage_rank'], src))
    return out


with get_connection() as conn:
    cur = conn.cursor()

    # ---------------- SOYBEAN OIL ----------------
    sbo_supply = (nass_series(cur, 'soybeans', 'oil_production_crude', 'production', 'soybean_oil')
                  + nass_series(cur, 'soybeans', 'oil_stocks', 'stocks', 'soybean_oil')
                  + census_trade(cur, '1507', 'soybean_oil'))
    sbo_demand, _ = raked_demand(cur, 'SBO', 'soybean_oil')
    sbo_food = nass_series(cur, 'soybeans', 'oil_refined_edible_use', 'food_use', 'soybean_oil')
    sbo_demand += sbo_food
    # non-bio = food (edible) use only. Refined INEDIBLE use is predominantly the biofuel feedstock
    # stream (already in raked biofuel); adding it double-counts and blows the balance open. With
    # food-only, production+imports ~= biofuel+food+exports +/- stocks (closes to ~2%).
    for k, fv in by_period(sbo_food, 'food_use').items():
        sbo_demand.append(row('soybean_oil', 'non_biofuel_use', k[0], int(k[1][1:]), fv,
                              'DISAPPEARANCE', 90, 'NASS refined edible (food) use'))

    # ---------------- CANOLA OIL ----------------
    can_supply = (nass_series(cur, 'canola', 'oil_production_crude', 'production', 'canola_oil')
                  + nass_series(cur, 'canola', 'oil_stocks', 'stocks', 'canola_oil')
                  + census_trade(cur, '1514', 'canola_oil'))
    can_demand, can_bio_tot = raked_demand(cur, 'CO', 'canola_oil')
    # non-bio = residual (production + imports - biofuel - exports). Canola is IMPORT-dominant
    # (imports ~3x US crush production), and NASS removal_for_processing only covers US-crushed oil
    # refining -> it misses the food disappearance of imported canola oil. So the disappearance
    # measure is incomplete here and residual is the honest fallback (Tore's ruling: residual where
    # disappearance doesn't exist / is incomplete). Clamp >=0, only within the biofuel frontier.
    can_prod = by_period(can_supply, 'production'); can_imp = by_period(can_supply, 'imports')
    can_exp = by_period(can_supply, 'exports')
    bio_by_key = {(yr, pm(mo)): t for (yr, mo), t in can_bio_tot.items()}
    for k in sorted(set(can_prod) & set(bio_by_key)):
        nb = max(0.0, can_prod.get(k, 0) + can_imp.get(k, 0) - bio_by_key[k] - can_exp.get(k, 0))
        can_demand.append(row('canola_oil', 'non_biofuel_use', k[0], int(k[1][1:]), nb,
                              'RESIDUAL', 50, 'residual: production+imports-biofuel-exports'))

    # split each commodity's non_biofuel_use total into end-use component lines (shared shares)
    sbo_demand += nonbio_components(cur, 'soybean_oil', sbo_demand)
    can_demand += nonbio_components(cur, 'canola_oil', can_demand)

OUTDIR.mkdir(parents=True, exist_ok=True)
for fname, stab, dtab, supply, demand in [
    ('us_soybean_oil_supply_demand.xlsx', 'soybean_oil_supply', 'soybean_oil_demand', sbo_supply, sbo_demand),
    ('us_canola_oil_supply_demand.xlsx', 'canola_oil_supply', 'canola_oil_demand', can_supply, can_demand),
]:
    wb = openpyxl.Workbook(); wb.remove(wb.active)
    write_tab(wb, stab, supply)
    write_tab(wb, dtab, demand)
    write_meta(wb, supply + demand, NOTES)
    wb.save(OUTDIR / fname)
    print(f"wrote {OUTDIR / fname}")
    for tab, rows in [(stab, supply), (dtab, demand)]:
        ser = sorted(set(r['series'] for r in rows))
        print(f"  [{tab}] {len(dedupe(rows))} rows | series: {ser}")
