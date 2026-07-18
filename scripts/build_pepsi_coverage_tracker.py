"""Build the Pepsi (Helios pilot) coverage tracker + country folders.

Pilot deliverable = rolling price forecasts + reasons for SOYBEAN OIL, CANOLA OIL, SUNFLOWER OIL.
To forecast those prices we need the same S&D sheet set we have for the US, built out for every
major producing / exporting / crushing / importing country in each complex.

This writes a tick-box tracker (status dropdown per country x commodity x sheet-type) and creates
the country folders under models/Oilseeds/. Edit MATRIX/SHEET_TYPES and re-run to regenerate the
tracker (it does not clobber folders that already have content).

Run:  python scripts/build_pepsi_coverage_tracker.py
"""
from pathlib import Path
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.worksheet.datavalidation import DataValidation

ROOT = Path(r"C:/dev/RLC-Agent")
OILSEEDS = ROOT / "models" / "Oilseeds"
TRACKER = OILSEEDS / "_Pepsi_Coverage_Tracker.xlsx"

# Major countries per complex for a global price forecast (producers/exporters/crushers/importers)
MATRIX = {
    "Soybean oil": ["United States", "Brazil", "Argentina", "China", "Europe", "India", "Paraguay", "World"],
    "Canola oil":  ["Canada", "Europe", "Ukraine", "Australia", "China", "India", "United States", "World"],
    "Sunflower oil": ["Ukraine", "Russia", "Europe", "Argentina", "Turkey", "United States", "World"],
}
SHEET_TYPES = ["Production", "Balance Sheet (S&D)", "Oil S&D", "Crush", "Trade"]

# US is the built template — pre-mark what already exists (models/Oilseeds/United States/)
US_DONE = {
    "Soybean oil":   {"Production", "Balance Sheet (S&D)", "Oil S&D", "Crush", "Trade"},
    "Canola oil":    {"Production", "Balance Sheet (S&D)", "Oil S&D", "Crush", "Trade"},
    "Sunflower oil": {"Production", "Balance Sheet (S&D)"},
}

GREEN = "3C7D22"  # internal-workbook header convention
WHEAT = "F1E7C6"
THIN = Side(style="thin", color="D4D4CE")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
STATUSES = ["", "Planned", "In Progress", "Done", "N/A"]


def make_folders():
    created = []
    countries = sorted({c for cs in MATRIX.values() for c in cs})
    for c in countries:
        d = OILSEEDS / c
        if not d.exists():
            d.mkdir(parents=True, exist_ok=True)
            created.append(c)
    return created


def build_tracker():
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Coverage"

    ws["A1"] = "Pepsi / Helios Pilot — Country × Commodity Coverage Tracker"
    ws["A1"].font = Font(bold=True, size=13, color=GREEN, name="Calibri")
    ws["A2"] = ("Deliverable: price forecasts + reasons for soybean / canola / sunflower oil. "
                "Build the US sheet set out per country. Status dropdown per cell.")
    ws["A2"].font = Font(italic=True, size=9, color="6E7178", name="Calibri")

    headers = ["Commodity", "Country"] + SHEET_TYPES + ["Notes"]
    hr = 4
    for j, h in enumerate(headers, start=1):
        c = ws.cell(row=hr, column=j, value=h)
        c.fill = PatternFill("solid", fgColor=GREEN)
        c.font = Font(bold=True, color="FFFFFF", name="Calibri")
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        c.border = BORDER

    dv = DataValidation(type="list", formula1='"%s"' % ",".join(s for s in STATUSES if s),
                        allow_blank=True)
    ws.add_data_validation(dv)

    r = hr + 1
    for commodity, countries in MATRIX.items():
        for country in countries:
            ws.cell(row=r, column=1, value=commodity).font = Font(name="Calibri")
            cc = ws.cell(row=r, column=2, value=country)
            cc.font = Font(bold=(country == "United States"), name="Calibri")
            for j, st in enumerate(SHEET_TYPES, start=3):
                cell = ws.cell(row=r, column=j)
                cell.alignment = Alignment(horizontal="center")
                cell.border = BORDER
                dv.add(cell)
                if country == "United States" and st in US_DONE.get(commodity, set()):
                    cell.value = "Done"
                    cell.fill = PatternFill("solid", fgColor="E2EFD9")
            note = ws.cell(row=r, column=len(headers))
            note.border = BORDER
            if country == "United States":
                note.value = "Template — models/Oilseeds/United States/"
                note.font = Font(italic=True, size=9, color="6E7178", name="Calibri")
            r += 1
        r += 1  # blank spacer row between complexes

    widths = [16, 16, 12, 16, 10, 9, 9, 40]
    for j, w in enumerate(widths, start=1):
        ws.column_dimensions[openpyxl.utils.get_column_letter(j)].width = w
    ws.freeze_panes = "C5"

    # legend
    lr = r + 1
    ws.cell(row=lr, column=1, value="Sheet set (per pairing):").font = Font(bold=True, size=9, name="Calibri")
    ws.cell(row=lr + 1, column=1,
            value="Production · Balance Sheet (S&D) · Oil S&D · Crush · Trade — mirrors the US files.").font = \
        Font(size=9, color="6E7178", name="Calibri")

    TRACKER.parent.mkdir(parents=True, exist_ok=True)
    wb.save(TRACKER)


def main():
    created = make_folders()
    build_tracker()
    n_pairs = sum(len(v) for v in MATRIX.values())
    print(f"Tracker: {TRACKER}  ({n_pairs} country x commodity pairings, {len(SHEET_TYPES)} sheet types)")
    print(f"Folders created under models/Oilseeds/: {created or 'none (all existed)'}")


if __name__ == "__main__":
    main()
