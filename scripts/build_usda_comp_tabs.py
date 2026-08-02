"""
Build / refresh `usda_comp` tabs in the oilseed balance-sheet workbooks.

Extends the wasde_comp pattern from us_soybean_complex_bal_sheets.xlsm to
every balance-sheet workbook, reading gold.psd_wasde_vintages (all countries,
all commodities, shared vintage ladder -- higher vintage_rank = more recent).

Layout per member block (mirrors the hand-built wasde_comp exactly):

    col A  row labels
    col B  USDA, current vintage, first active MY
    col C  delta vs prior vintage (formula =B-I)
    col D  RLC (formula link into the member balance-sheet tab)
    col E  USDA, current vintage, second active MY
    col F  delta (formula =E-J)
    col G  RLC
    col I  USDA, prior vintage, first active MY
    col J  USDA, prior vintage, second active MY

Write engines (ruling for the "Python vs VBA/ODBC per file type" decision in
docs/handoffs/2026-08-01_market_dashboard.md):
  * Generated country books (.xlsx outside `United States/`): openpyxl --
    same engine copy_legacy_monthly_blocks.py already uses on these files.
  * Hand-maintained US books (everything in `United States/`, .xlsx or
    .xlsm): Excel COM -- preserves VBA projects, charts and formatting that
    openpyxl would drop. Macros are force-disabled while open so
    Workbook_Open handlers (shortcut banners) don't fire.
  The us_soybean_complex book keeps its existing VBA wasde_comp and is
  skipped here.

Unit handling: PSD values are 1000 MT (area 1000 HA). The factor into book
units is NOT assumed -- it is derived per member sheet by cross-checking the
latest FINAL (closed-MY, vintage_rank 90) PSD value against the number the
sheet already carries for that MY, then snapped to a known conversion
factor. No clean snap -> the member is skipped loudly, never written wrong.

Usage:
    python scripts/build_usda_comp_tabs.py                 # all eligible books
    python scripts/build_usda_comp_tabs.py --only brazil   # filename filter
    python scripts/build_usda_comp_tabs.py --dry-run       # report, no writes
    python scripts/build_usda_comp_tabs.py --no-us         # skip COM/US books
"""

from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.services.database.db_config import get_connection  # noqa: E402

MODELS_DIR = PROJECT_ROOT / "models" / "Oilseeds"
ARCHIVE_DIR = MODELS_DIR / "Archive"

# ---------------------------------------------------------------------------
# Static maps
# ---------------------------------------------------------------------------

# Folder name -> PSD country code. PSD's own (FIPS-style) codes, verified
# against /api/psd/countries 2026-08-01. bronze.fas_psd also carries a few
# stray ISO-coded rows (CN, AU, ZA...) from an old mis-coded pull -- these
# PSD codes are the ones with real history; do not "correct" them to ISO.
COUNTRY_CODES = {
    "United States": "US",
    "Argentina": "AR",
    "Australia": "AS",
    "Brazil": "BR",
    "Canada": "CA",
    "China": "CH",
    "EU": "E4",
    "India": "IN",
    "Indonesia": "ID",
    "Japan": "JA",
    "Malaysia": "MY",
    "Mexico": "MX",
    "Paraguay": "PA",
    "Philippines": "RP",
    "Russia": "RS",
    "Ukraine": "UP",
    "Uruguay": "UY",
}

# In-sheet title fragment -> PSD commodity slug. Ordered most-specific-first;
# matching uses the SHEET TITLE (A2), never the tab name -- seven rapeseed
# country books still carry soy_* tab names from the template clone.
TITLE_TO_COMMODITY = [
    ("PALM KERNEL CAKE", "palm_kernel_meal"),
    ("PALM KERNEL MEAL", "palm_kernel_meal"),
    ("PALM KERNEL OIL", "palm_kernel_oil"),
    ("PALM KERNEL", "palm_kernel"),
    ("PALM OIL", "palm_oil"),
    ("COPRA MEAL", "copra_meal"),
    ("COPRA", "copra"),
    ("COCONUT OIL", "coconut_oil"),
    # US coconut book titles its copra (seed) sheet "US COCONUT S&D"
    ("COCONUT", "copra"),
    ("RAPESEED MEAL", "rapeseed_meal"),
    ("CANOLA MEAL", "rapeseed_meal"),
    ("RAPESEED OIL", "rapeseed_oil"),
    ("CANOLA/RAPESEED OIL", "rapeseed_oil"),
    ("CANOLA OIL", "rapeseed_oil"),
    ("RAPESEED", "rapeseed"),
    ("CANOLA", "rapeseed"),
    ("SUNFLOWERSEED MEAL", "sunflowerseed_meal"),
    ("SUNFLOWER MEAL", "sunflowerseed_meal"),
    ("SUNFLOWERSEED OIL", "sunflowerseed_oil"),
    ("SUNFLOWER OIL", "sunflowerseed_oil"),
    ("SUNFLOWERSEED", "sunflowerseed"),
    ("SUNFLOWER", "sunflowerseed"),
    ("COTTONSEED MEAL", "cottonseed_meal"),
    ("COTTONSEED OIL", "cottonseed_oil"),
    ("COTTONSEED", "cottonseed"),
    ("PEANUT MEAL", "peanut_meal"),
    ("PEANUT OIL", "peanut_oil"),
    ("PEANUT", "peanuts"),
    ("SOYBEAN MEAL", "soybean_meal"),
    ("SOYBEAN OIL", "soybean_oil"),
    ("SOYBEAN", "soybeans"),
]

SEED_COMMODITIES = {
    "soybeans", "rapeseed", "sunflowerseed", "cottonseed",
    "peanuts", "copra", "palm_kernel",
}

# Books that must not get a usda_comp tab at all.
SKIP_BOOKS = {
    # has the hand-built wasde_comp + WASDECompUpdater VBA already
    "us_soybean_complex_bal_sheets.xlsm": "existing wasde_comp (VBA)",
    # stale soybean clone, Tore is rebuilding it (handoff 2026-08-01 item 3b)
    "us_lauric_oils_bal_sheets.xlsm": "stale clone, being rebuilt",
}

# PSD does not publish these commodities at all (verified vs
# /api/psd/commodities 2026-08-01). Ruled by Tore 2026-08-01: these books
# get a NOTE-ONLY usda_comp tab saying no comp is available, rather than
# nothing (no fabricated #N/A rows in bronze -- the note lives in the book).
NO_PSD_BOOKS = {
    "us_corn_oil_balance_sheets.xlsx": "corn oil",
    "us_flaxseed_balance_sheets.xlsx": "flaxseed",
    "us_safflower_balance_sheets.xlsx": "safflower",
}

# Known 1000-MT -> book-unit factors the magnitude cross-check may snap to.
KNOWN_FACTORS = [
    (1.0, "thousand tonnes"),
    (2.204623, "million pounds"),
    (1.102311, "thousand short tons"),
    (0.0367437, "million bushels (60 lb)"),
    (0.0393683, "million bushels (56 lb)"),
    (0.001, "million tonnes"),
    # 480-lb bale as a MASS unit (1 bale = 480 lb exactly): 1000 MT =
    # 2.204623 mil lb / 480 = 4.59296 thousand bales. Deterministic only
    # under the mass definition — a lint-equivalent "bale of seed" would
    # drift with annual turnout and must NOT be snapped.
    (4.59296, "thousand 480-lb bales"),
    (0.00459296, "million 480-lb bales"),
]

# Row-label parenthetical -> factor. Used when the annual block is still
# empty (most generated country books carry the template structure but no
# data yet), where the magnitude cross-check has nothing to bite on.
LABEL_UNIT_FACTORS = {
    "thousand tonnes": (1.0, "thousand tonnes"),
    "thousand metric tons": (1.0, "thousand tonnes"),
    "million pounds": (2.204623, "million pounds"),
    "thousand short tons": (1.102311, "thousand short tons"),
    # among the oilseed books only soybeans are kept in bushels -> 60 lb
    "million bushels": (0.0367437, "million bushels (60 lb)"),
    "million 480-lb bales": (0.00459296, "million 480-lb bales"),
    "million 480 lb bales": (0.00459296, "million 480-lb bales"),
    "thousand 480-lb bales": (4.59296, "thousand 480-lb bales"),
    "thousand 480 lb bales": (4.59296, "thousand 480-lb bales"),
}
AREA_FACTORS = {
    # book area unit -> factor from PSD 1000 HA
    "million hectares": 0.001,
    "million acres": 0.00247105,
    "thousand hectares": 1.0,
    "thousand acres": 2.47105,
}

# Comp rows: (label, psd_field, kind) where kind drives which member types
# carry the row. psd_field None => formula row.
SEED_ROWS = [
    ("Harvested Area", "area_harvested"),
    ("Beginning Stocks", "beginning_stocks"),
    ("Production", "production"),
    ("Imports", "imports"),
    ("Total Supply", "SUM_SUPPLY"),
    ("Crush", "crush"),
    ("Exports", "exports"),
    # everything that isn't crush or exports: food, seed, feed/waste,
    # residual -- composition varies by seed (soybeans: seed+residual;
    # peanuts: mostly food; cottonseed: feed+residual)
    ("Other Domestic Use", "RESIDUAL"),
    ("Total Demand", "DEMAND"),
    ("Ending Stocks", "ending_stocks"),
    ("Stocks-to-Use", "STU"),
]
PRODUCT_ROWS = [
    ("Beginning Stocks", "beginning_stocks"),
    ("Production", "production"),
    ("Imports", "imports"),
    ("Total Supply", "SUM_SUPPLY"),
    ("Domestic Use", "domestic_consumption"),
    ("Exports", "exports"),
    ("Total Demand", "DEMAND"),
    ("Ending Stocks", "ending_stocks"),
    ("Stocks-to-Use", "STU"),
]

# Comp label -> member-sheet row label patterns (lowercased, ordered by
# priority) for the RLC link columns.
RLC_LINK_PATTERNS = {
    "Harvested Area": ["harvested area"],
    "Beginning Stocks": ["beginning stocks"],
    "Production": ["production"],
    "Imports": ["imports"],
    "Total Supply": ["total supply"],
    "Crush": ["crush"],
    "Domestic Use": ["total domestic use", "domestic use", "domestic demand",
                     "domestic disappearance"],
    "Exports": ["exports"],
    "Total Demand": ["total demand", "total use"],
    "Ending Stocks": ["ending stocks"],
    "Stocks-to-Use": ["stocks-to-use", "stocks to use"],
}

MONTH_NAMES = ["", "January", "February", "March", "April", "May", "June",
               "July", "August", "September", "October", "November",
               "December"]


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def load_vintages(cur, commodity: str, country_code: str):
    """Return {my: [newest_row, prior_row_or_None]} for active MYs, plus the
    newest FINAL (closed-MY) row for the unit cross-check."""
    cur.execute(
        """
        SELECT marketing_year, report_date, vintage, vintage_rank,
               area_harvested, beginning_stocks, production, imports,
               total_supply, crush, domestic_consumption, exports,
               ending_stocks
        FROM gold.psd_wasde_vintages
        WHERE commodity = %s AND country_code = %s AND is_active_my
        ORDER BY marketing_year, vintage_rank DESC
        """,
        (commodity, country_code),
    )
    by_my: dict[int, list] = {}
    for row in cur.fetchall():
        by_my.setdefault(row["marketing_year"], []).append(row)
    active = {my: (rows[0], rows[1] if len(rows) > 1 else None)
              for my, rows in by_my.items()}

    cur.execute(
        """
        SELECT marketing_year, area_harvested, beginning_stocks, production,
               imports, crush, domestic_consumption, exports, ending_stocks
        FROM gold.psd_wasde_vintages
        WHERE commodity = %s AND country_code = %s
          AND NOT is_active_my AND vintage = 'FINAL'
        ORDER BY marketing_year DESC
        LIMIT 3
        """,
        (commodity, country_code),
    )
    finals = cur.fetchall()
    return active, finals


# ---------------------------------------------------------------------------
# Workbook inspection (openpyxl, read-only pass, cached values)
# ---------------------------------------------------------------------------

def inspect_book(path: Path):
    """Read member-sheet structure: title, MY->column map, label->row map,
    cached values for the unit cross-check."""
    wb_v = openpyxl.load_workbook(path, data_only=True)   # cached values
    members = []
    for name in wb_v.sheetnames:
        if not name.endswith("_balance_sheet"):
            continue
        ws = wb_v[name]
        title = str(ws.cell(row=2, column=1).value or "").upper()
        commodity = None
        for frag, slug in TITLE_TO_COMMODITY:
            if frag in title:
                commodity = slug
                break

        # MY -> column. Row 3 carries '1990/91'-style labels in every book;
        # row 2 start-year ints exist only in the populated ones (Brazil, US).
        my_col = {}
        for col in range(2, 90):
            v3 = ws.cell(row=3, column=col).value
            if isinstance(v3, str) and "/" in v3:
                head = v3.split("/")[0].strip()
                if head.isdigit() and 1980 <= int(head) <= 2060:
                    my_col[int(head)] = col
                    continue
            v2 = ws.cell(row=2, column=col).value
            if isinstance(v2, (int, float)) and 1980 <= v2 <= 2060:
                my_col[int(v2)] = col

        # annual block: rows 4.. until the first ALL-CAPS section title
        label_row: dict[str, int] = {}
        values: dict[tuple[str, int], float] = {}
        for r in range(4, 40):
            raw = ws.cell(row=r, column=1).value
            if raw is None:
                continue
            label = str(raw).strip()
            if len(label) > 10 and label == label.upper():
                break  # monthly-section banner => annual block ended
            key = label.lower()
            if key not in label_row:
                label_row[key] = r
            for my, col in my_col.items():
                v = ws.cell(row=r, column=col).value
                if isinstance(v, (int, float)):
                    values[(key, my)] = float(v)

        members.append({
            "tab": name,
            "title": title,
            "commodity": commodity,
            "my_col": my_col,
            "label_row": label_row,
            "values": values,
        })
    wb_v.close()
    return members


def find_label_row(label_row: dict, patterns: list[str]):
    for pat in patterns:
        for key, r in label_row.items():
            base = key.split("(")[0].strip()
            if base == pat or base.startswith(pat):
                return r
    return None


def derive_unit_factor(member, finals):
    """Snap sheet_value / psd_value to a known conversion factor. Returns the
    snap backed by the LARGEST psd value (small denominators can't tell a
    ~10%% source difference from the short-tons factor)."""
    fields = [(["production"], "production"), (["imports"], "imports"),
              (["exports"], "exports"),
              (["total domestic use", "domestic use"], "domestic_consumption"),
              (["ending stocks"], "ending_stocks")]
    best = None  # (psd_v, factor, unit_name, evidence)
    for final in finals:
        my = final["marketing_year"]
        if my not in member["my_col"]:
            continue
        for sheet_patterns, psd_field in fields:
            psd_v = final.get(psd_field)
            if psd_v is None or abs(float(psd_v)) < 50:
                continue  # too small to discriminate factors
            r = find_label_row(member["label_row"], sheet_patterns)
            if r is None:
                continue
            key = [k for k in member["label_row"] if member["label_row"][k] == r][0]
            sheet_v = member["values"].get((key, my))
            if sheet_v is None or sheet_v == 0:
                continue
            ratio = sheet_v / float(psd_v)
            for factor, unit_name in KNOWN_FACTORS:
                if abs(ratio / factor - 1.0) < 0.05:
                    if best is None or float(psd_v) > best[0]:
                        best = (float(psd_v), factor, unit_name,
                                (sheet_patterns[0], my, sheet_v, float(psd_v)))
    if best is None:
        return None, None, None, 0.0
    return best[1], best[2], best[3], best[0]


def unit_factor_from_labels(member):
    """Read the unit out of the row-label parentheticals, preferring the
    stock/production rows ('Beginning Stocks (thousand tonnes)')."""
    preferred = ["beginning stocks", "production", "ending stocks"]
    keys = sorted(member["label_row"],
                  key=lambda k: next((i for i, p in enumerate(preferred)
                                      if k.startswith(p)), 99))
    for key in keys:
        if "(" not in key:
            continue
        unit_txt = key.split("(", 1)[1].rstrip(")").strip()
        if unit_txt in LABEL_UNIT_FACTORS:
            return LABEL_UNIT_FACTORS[unit_txt]
    return None, None


def derive_area_factor(member):
    """Area factor from the label text of the Harvested/Planted Area row."""
    for key in member["label_row"]:
        if "area" in key and "(" in key:
            unit_txt = key.split("(", 1)[1].rstrip(")").strip()
            if unit_txt in AREA_FACTORS:
                return AREA_FACTORS[unit_txt]
    # generated country books label area "(million hectares)" on the
    # Planted Area row only; Harvested Area inherits it
    return None


# ---------------------------------------------------------------------------
# Comp-tab content assembly (engine-neutral cell list)
# ---------------------------------------------------------------------------

def build_block(member, active, finals, country_name, start_row):
    """Return (cells, n_rows, notes). cells = list of
    (row, col, value, is_formula, num_fmt, bold)."""
    commodity = member["commodity"]
    is_seed = commodity in SEED_COMMODITIES
    rows_spec = SEED_ROWS if is_seed else PRODUCT_ROWS

    mys = sorted(active.keys())[:2]
    if len(mys) < 2:
        return None, 0, [f"{commodity}: fewer than 2 active MYs"]
    my1, my2 = mys

    # Unit factor: the row-label unit is authoritative when present (it is
    # part of the template). The magnitude cross-check can only overrule it
    # on STRONG evidence (a snapped field with PSD value >= 500) -- small
    # denominators can't tell a ~10% source difference from the short-tons
    # factor. No label and no snap -> loud skip, never a guessed unit.
    label_factor, label_unit = unit_factor_from_labels(member)
    snap_factor, snap_unit, evidence, snap_strength = derive_unit_factor(
        member, finals)
    notes = []
    if label_factor is not None:
        if (snap_factor is not None
                and abs(label_factor / snap_factor - 1.0) > 0.05):
            if snap_strength >= 500:
                return None, 0, [f"{commodity}: label says {label_unit} but "
                                 f"values strongly snap to {snap_unit} "
                                 f"(psd={snap_strength:,.0f}) -- skipped"]
            notes.append(f"{commodity}: weak snap to {snap_unit} "
                         f"(psd={snap_strength:,.0f}) ignored; label "
                         f"{label_unit} used")
            evidence = None
        factor, unit_name = label_factor, label_unit
        if snap_factor is not None and abs(label_factor / snap_factor - 1.0) <= 0.05:
            factor, unit_name = snap_factor, snap_unit  # confirmed by values
        else:
            evidence = None
    elif snap_factor is not None:
        factor, unit_name = snap_factor, snap_unit
    else:
        return None, 0, [f"{commodity}: no unit source (no labelled unit, "
                         f"no values to snap) -- skipped"]
    area_factor = derive_area_factor(member) or 0.001

    cur1, prior1 = active[my1]
    cur2, prior2 = active[my2]

    def conv(rowdict, field):
        if rowdict is None:
            return None
        v = rowdict.get(field)
        if v is None:
            return None
        f = area_factor if field == "area_harvested" else factor
        return round(float(v) * f, 2)

    member_title = member["title"].replace(" SUPPLY AND DEMAND", "")
    tab = member["tab"]

    cells = []
    r0 = start_row
    my_label = lambda my: f"{my}/{str(my + 1)[-2:]}"
    short = lambda my: f"{str(my)[-2:]}/{str(my + 1)[-2:]}"

    cur_month = MONTH_NAMES[cur1["report_date"].month]
    cur_year = cur1["report_date"].year
    prior_month = (MONTH_NAMES[prior1["report_date"].month]
                   if prior1 is not None else "")

    cells.append((r0, 1, f"{member_title} - USDA v RLC ({unit_name})",
                  False, None, True))
    cells.append((r0 + 1, 2, my_label(my1), False, None, True))
    cells.append((r0 + 1, 5, my_label(my2), False, None, True))
    if prior_month:
        cells.append((r0 + 1, 9, prior_month, False, None, True))
    hdr = r0 + 2
    delta_txt = f"Δ from {prior_month}" if prior_month else "Δ (no prior)"
    for col, txt in [(2, "USDA"), (3, delta_txt), (4, "RLC"),
                     (5, "USDA"), (6, delta_txt), (7, "RLC"),
                     (9, short(my1)), (10, short(my2))]:
        cells.append((hdr, col, txt, False, None, True))

    data0 = hdr + 1
    row_of = {}
    for i, (label, _f) in enumerate(rows_spec):
        row_of[label] = data0 + i

    n_fmt = "#,##0.0"
    for label, field in rows_spec:
        r = row_of[label]
        fmt = "0.0%" if field == "STU" else ("0.000" if field == "area_harvested" else n_fmt)
        cells.append((r, 1, label if field != "area_harvested"
                      else f"Harvested Area ({[k for k,v in AREA_FACTORS.items() if v == area_factor][0]})",
                      False, None, False))

        # RLC link columns D/G
        link_row = find_label_row(member["label_row"],
                                  RLC_LINK_PATTERNS.get(label, []))
        if link_row is not None:
            if my1 in member["my_col"]:
                c1 = get_column_letter(member["my_col"][my1])
                cells.append((r, 4, f"='{tab}'!{c1}{link_row}", True, fmt, False))
            if my2 in member["my_col"]:
                c2 = get_column_letter(member["my_col"][my2])
                cells.append((r, 7, f"='{tab}'!{c2}{link_row}", True, fmt, False))

        # formula rows land in B/E (current vintage) and, when a prior
        # vintage exists, in I/J too -- the hand-built wasde_comp carries
        # =I17/I16 etc. in the prior columns the same way
        formula_cols = [2, 5]
        if prior1 is not None:
            formula_cols.append(9)
        if prior2 is not None:
            formula_cols.append(10)

        if field == "SUM_SUPPLY":
            top, bot = row_of["Beginning Stocks"], r - 1
            for col in formula_cols:
                L = get_column_letter(col)
                cells.append((r, col, f"=SUM({L}{top}:{L}{bot})", True, fmt, False))
        elif field == "RESIDUAL":
            for col in formula_cols:
                L = get_column_letter(col)
                cells.append((r, col,
                              f"={L}{row_of['Total Demand']}-{L}{row_of['Crush']}-{L}{row_of['Exports']}",
                              True, fmt, False))
        elif field == "DEMAND":
            for col in formula_cols:
                L = get_column_letter(col)
                cells.append((r, col,
                              f"={L}{row_of['Total Supply']}-{L}{row_of['Ending Stocks']}",
                              True, fmt, False))
        elif field == "STU":
            for col in formula_cols:
                L = get_column_letter(col)
                cells.append((r, col,
                              f"={L}{row_of['Ending Stocks']}/{L}{row_of['Total Demand']}",
                              True, "0.0%", False))
        else:
            v1, v2 = conv(cur1, field), conv(cur2, field)
            p1, p2 = conv(prior1, field), conv(prior2, field)
            if v1 is not None:
                cells.append((r, 2, v1, False, fmt, False))
            if v2 is not None:
                cells.append((r, 5, v2, False, fmt, False))
            if p1 is not None:
                cells.append((r, 9, p1, False, fmt, False))
            if p2 is not None:
                cells.append((r, 10, p2, False, fmt, False))

        # deltas only when a prior vintage exists
        if prior1 is not None and field != "STU":
            cells.append((r, 3, f"=B{r}-I{r}", True, fmt, False))
        if prior2 is not None and field != "STU":
            cells.append((r, 6, f"=E{r}-J{r}", True, fmt, False))
        if field == "STU":
            if prior1 is not None:
                cells.append((r, 3, f"=B{r}-I{r}", True, "0.0%", False))
            if prior2 is not None:
                cells.append((r, 6, f"=E{r}-J{r}", True, "0.0%", False))

    note_r = data0 + len(rows_spec) + 1
    vtag = cur1["vintage"]
    note = (f"USDA columns: {vtag} ({cur_month} {cur_year} pull) from "
            f"gold.psd_wasde_vintages"
            + (f"; Δ vs {prior1['vintage']}" if prior1 is not None
               else "; no prior vintage yet -- Δ columns blank")
            + f". RLC columns link to {tab}. "
            + (f"Unit check: {evidence[0]} MY{evidence[1]} sheet "
               f"{evidence[2]:,.0f} vs PSD {evidence[3]:,.0f} x{factor:g}."
               if evidence is not None else
               f"Unit from row labels ({unit_name}); sheet has no values "
               f"yet to cross-check."))
    cells.append((note_r, 1, note, False, None, False))

    n_rows = (note_r - r0) + 3
    return cells, n_rows, notes


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def write_openpyxl(path: Path, all_cells, dry_run: bool):
    wb = openpyxl.load_workbook(path)
    if "usda_comp" in wb.sheetnames:
        del wb["usda_comp"]
    ws = wb.create_sheet("usda_comp", 0)
    ws.column_dimensions["A"].width = 40
    for col in "BCDEFGHIJ":
        ws.column_dimensions[col].width = 12
    for (r, c, v, is_formula, fmt, bold) in all_cells:
        cell = ws.cell(row=r, column=c, value=v)
        if fmt:
            cell.number_format = fmt
        if bold:
            cell.font = Font(bold=True)
    if not dry_run:
        wb.save(path)
    wb.close()


def write_com(path: Path, all_cells, dry_run: bool):
    if dry_run:
        return
    import pythoncom
    import win32com.client as win32
    pythoncom.CoInitialize()
    excel = win32.DispatchEx("Excel.Application")
    excel.Visible = False
    excel.DisplayAlerts = False
    # 3 = msoAutomationSecurityForceDisable: don't run Workbook_Open macros
    excel.AutomationSecurity = 3
    try:
        wb = excel.Workbooks.Open(str(path.resolve()), UpdateLinks=0)
        for sh in list(wb.Sheets):
            if sh.Name == "usda_comp":
                sh.Delete()
        ws = wb.Sheets.Add(Before=wb.Sheets(1))
        ws.Name = "usda_comp"
        ws.Columns("A").ColumnWidth = 40
        ws.Columns("B:J").ColumnWidth = 12
        for (r, c, v, is_formula, fmt, bold) in all_cells:
            cell = ws.Cells(r, c)
            if is_formula:
                cell.Formula = v
            else:
                cell.Value = v
            if fmt:
                cell.NumberFormat = fmt
            if bold:
                cell.Font.Bold = True
        wb.Save()
        wb.Close(SaveChanges=False)
    finally:
        excel.Quit()
        pythoncom.CoUninitialize()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_book(path: Path, cur, dry_run: bool, engine: str = "com"):
    country_folder = path.parent.name
    code = COUNTRY_CODES.get(country_folder)
    if code is None:
        return f"SKIP {path.name}: unknown country folder {country_folder!r}"

    members = inspect_book(path)
    all_cells, notes, built = [], [], 0
    next_row = 3
    for m in members:
        if m["commodity"] is None:
            notes.append(f"  {m['tab']}: unrecognized title {m['title'][:50]!r}")
            continue
        active, finals = load_vintages(cur, m["commodity"], code)
        if not active:
            notes.append(f"  {m['tab']}: no PSD data for "
                         f"({m['commodity']}, {code})")
            continue
        cells, n_rows, errs = build_block(m, active, finals,
                                          country_folder, next_row)
        if cells is None:
            notes.append("  " + "; ".join(errs))
            continue
        for w in errs:
            notes.append("  " + w)
        all_cells.extend(cells)
        next_row += n_rows
        built += 1

    if built == 0:
        return (f"SKIP {path.name}: no member with usable PSD data\n"
                + "\n".join(notes))

    # banner
    all_cells.insert(0, (1, 1,
                         f"{country_folder.upper()} — USDA (PSD/WASDE) vs RLC "
                         f"COMPARISON — refreshed {datetime.now():%Y-%m-%d}",
                         False, None, True))

    if not dry_run:
        ARCHIVE_DIR.mkdir(exist_ok=True)
        bak = ARCHIVE_DIR / f"{path.name}.bak_{datetime.now():%Y%m%d_%H%M%S}"
        shutil.copy2(path, bak)

    # Ruled by Tore 2026-08-01: ALL books are (or will be) hand-maintained,
    # so COM is the default everywhere -- openpyxl re-saves would drop
    # charts/objects added by hand. openpyxl remains as an explicit
    # escape hatch (--engine openpyxl) for headless/no-Excel environments,
    # safe only on books that are still generated shells.
    if engine == "openpyxl" and path.suffix.lower() != ".xlsm":
        write_openpyxl(path, all_cells, dry_run)
    else:
        write_com(path, all_cells, dry_run)

    tag = "DRY-RUN " if dry_run else ""
    out = f"{tag}OK   {path.name}: {built} member blocks"
    if notes:
        out += "\n" + "\n".join(notes)
    return out


def write_note_tab(path: Path, commodity_name: str, dry_run: bool):
    """usda_comp tab containing only an explanatory note, for books whose
    commodity PSD does not publish."""
    cells = [
        (1, 1, f"{path.parent.name.upper()} — USDA (PSD/WASDE) vs RLC "
               f"COMPARISON", False, None, True),
        (3, 1, f"No USDA comparison available: PSD does not publish "
               f"{commodity_name} (verified against /api/psd/commodities "
               f"2026-08-01). This tab is a placeholder so the absence is "
               f"deliberate, not an oversight.", False, None, False),
    ]
    if not dry_run:
        ARCHIVE_DIR.mkdir(exist_ok=True)
        bak = ARCHIVE_DIR / f"{path.name}.bak_{datetime.now():%Y%m%d_%H%M%S}"
        shutil.copy2(path, bak)
    write_com(path, cells, dry_run)
    return f"{'DRY-RUN ' if dry_run else ''}OK   {path.name}: note-only tab (no PSD coverage)"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="substring filter on filename")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-us", action="store_true",
                    help="skip hand-maintained US books (COM writer)")
    ap.add_argument("--engine", choices=["com", "openpyxl"], default="com",
                    help="com (default, safe on hand-maintained books) or "
                         "openpyxl (headless; generated .xlsx shells only)")
    args = ap.parse_args()

    books = sorted(
        p for p in MODELS_DIR.glob("*/*_bal*_sheets.xls[xm]")
        if p.parent.name != "Archive" and not p.name.startswith("~$")
    )

    with get_connection() as conn:
        cur = conn.cursor()
        for path in books:
            if args.only and args.only.lower() not in path.name.lower():
                continue
            if path.name in SKIP_BOOKS:
                print(f"SKIP {path.name}: {SKIP_BOOKS[path.name]}")
                continue
            if args.no_us and path.parent.name == "United States":
                print(f"SKIP {path.name}: --no-us")
                continue
            try:
                if path.name in NO_PSD_BOOKS:
                    print(write_note_tab(path, NO_PSD_BOOKS[path.name],
                                         args.dry_run))
                else:
                    print(process_book(path, cur, args.dry_run, args.engine))
            except Exception as e:  # noqa: BLE001
                print(f"FAIL {path.name}: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
