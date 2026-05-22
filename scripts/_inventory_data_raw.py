"""Inventory data/raw/ for ingest planning.

Walks the directory tree, groups files by (subdirectory, source-entity),
and produces a markdown summary at
docs/specs/data_raw_ingest_inventory.md.

For each group, the report shows:
  - File count + total size
  - Date range covered (parsed from filenames where possible)
  - Format mix (zip / xlsx / csv / pdf / docx)
  - Mapped source entity (best-guess by filename patterns)
  - Suggested bronze target table
  - Current bronze coverage if any
"""
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

RAW = Path(r"C:\dev\RLC-Agent\data\raw")
OUT = Path(r"C:\dev\RLC-Agent\docs\specs\data_raw_ingest_inventory.md")

# -- Filename → source-entity mapping ----------------------------------
ENTITY_RULES = [
    # FAO bulk
    (r"production_crops_livestock", "FAO QCL — Production Crops + Livestock"),
    (r"^trade_detailedtradematrix", "FAO TM — Detailed Bilateral Trade Matrix"),
    (r"^trade_cropslivestock",      "FAO TCL — Trade Crops + Livestock"),
    (r"^population_e",              "FAO OA — Annual Population"),
    (r"commoditybalances",          "FAO CB — Commodity Balances (non-food)"),
    (r"^foodbalancesheet|^fbs",     "FAO FBS — Food Balance Sheets"),
    (r"^prices_e|^pp_e",            "FAO PP — Producer Prices"),
    (r"^value_of_production",       "FAO QV — Value of Production"),
    (r"^investment",                "FAO Investment"),
    (r"^macro",                     "FAO Macro-Economic Indicators"),
    (r"^forestry",                  "FAO Forestry"),
    (r"^food_security|^foodsec",    "FAO Food Security and Nutrition"),
    (r"^emissions",                 "FAO Climate-Change Emissions"),
    (r"^land_use|^landuse|land_inputs", "FAO Land/Inputs/Sustainability"),
    (r"^sdg",                       "FAO SDG Indicators"),

    # USDA
    (r"yearbookalltables_\d{4}",    "USDA ERS — Oil Crops Yearbook"),
    (r"feed grains yearbook tables", "USDA ERS — Feed Grains Yearbook"),
    (r"oil crops annual statistical|oilcropsalltables", "USDA ERS — Oil Crops Statistical"),
    (r"feed grain outlook report|fds-", "USDA ERS — Feed Grain Outlook (FDS)"),
    (r"wasde",                      "USDA WASDE"),
    (r"crop production",            "USDA NASS Crop Production"),
    (r"crop conditions data",       "USDA NASS Crop Conditions"),
    (r"acreage - ",                 "USDA NASS Acreage Report"),
    (r"agricultural prices",        "USDA NASS Agricultural Prices"),
    (r"agriculture in drought",     "USDA Drought Monitor"),
    (r"peanut stocks and processing", "USDA NASS Peanut Processing"),
    (r"grain stocks",               "USDA NASS Grain Stocks"),
    (r"balance sheet data",         "USDA Balance Sheet Data"),
    (r"bloomberg.*acreage|bloomberg.*stocks", "Bloomberg Survey"),
    (r"corn milling and products",  "USDA / FGIS — Corn Milling"),
    (r"^cy\d{4}\.csv|^cy\d{2}",     "FGIS Inspections (CY annual)"),

    # CONAB / Brazil
    (r"conab",                      "CONAB Brazil"),
    (r"levantamento.*safra",        "CONAB Brazil Safra Survey"),

    # Canada
    (r"canada - crop production",   "Canada AAFC — Crop Production"),
    (r"canada.*cgc",                "Canada CGC"),

    # EIA / energy
    (r"bbd feedstock use.*eia|biodiesel capacity.*eia|bbd us capacity.*eia|capacity.*eia",
                                    "EIA — BBD Capacity / Feedstock"),
    (r"eia",                        "EIA"),

    # Trade / market data
    (r"cash prices",                "AMS / Cash Prices snapshot"),
    (r"_export_sales_",             "FAS Export Sales — entity-specific xlsx"),
    (r"export.*\.xlsx|imports.*\.xlsx", "US Trade — generic"),
    (r"hb_cash_price",              "HB cash price extract"),
    (r"bloomber survey",            "Bloomberg Survey"),

    # MPOB / palm
    (r"mpob",                       "MPOB — Malaysian Palm Oil"),
    (r"^iso",                       "Other ISO source"),

    # Air permits etc
    (r"state_air_permits/mn",       "MN MPCA — Title V permits"),
    (r"state_air_permits/mo",       "MO DNR — Title V permits"),
    (r"wimn_bulk",                  "MN — Bulk Title V"),

    # Attaches
    (r"attache",                    "USDA FAS Attache Reports"),
    (r"drought monitor",            "USDA Drought Monitor"),
    (r"market news",                "AMS Market News"),
    (r"mmn report",                 "USDA MMN Reports"),

    # NDVI / weather
    (r"ndvi",                       "NDVI Satellite Data"),
    (r"weather|gfs|gefs",           "Weather / Forecast"),
]

DATE_RX = [
    re.compile(r"(\d{2})(\d{2})(20\d{2})"),                # MMDDYYYY
    re.compile(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})"),       # YYYYMMDD or YYYY-MM-DD
    re.compile(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s*[\-_]?\s*(\d{2,4})", re.I),
    re.compile(r"_(\d{4})\."),                              # YearbookAllTables_YYYY.zip
]


def classify(name: str, rel_path: str) -> str:
    s = name.lower()
    p = rel_path.lower()
    for pat, label in ENTITY_RULES:
        if re.search(pat, p) or re.search(pat, s):
            return label
    return "❓ Unmatched"


def parse_date(name: str):
    """Best-effort date parse from filename. Returns YYYY string."""
    for rx in DATE_RX:
        m = rx.search(name)
        if not m:
            continue
        g = m.groups()
        if rx == DATE_RX[0]:
            return g[2]
        elif rx == DATE_RX[1]:
            return g[0]
        elif rx == DATE_RX[2]:
            y = g[1]
            return ('20' + y) if len(y) == 2 else y
        elif rx == DATE_RX[3]:
            return g[0]
    return ""


# Walk
groups = defaultdict(list)
for root, _, files in os.walk(RAW):
    for fn in files:
        full = Path(root) / fn
        if full.suffix.lower() not in {'.zip', '.csv', '.xlsx', '.xls', '.txt', '.pdf', '.docx'}:
            continue
        rel = str(full.relative_to(RAW)).replace('\\', '/')
        ent = classify(fn, rel)
        try:
            size = full.stat().st_size
        except OSError:
            size = 0
        groups[ent].append({
            'name': fn,
            'rel': rel,
            'size_mb': size / (1024 * 1024),
            'year': parse_date(fn),
            'ext': full.suffix.lower(),
        })

# Build report
lines = []
P = lines.append
P("# Data raw inventory — ingest plan")
P("")
P(f"*Generated {datetime.utcnow():%Y-%m-%d %H:%M UTC}. Walked `C:/dev/RLC-Agent/data/raw/`.*")
P("")
P(f"**Files inventoried:** {sum(len(v) for v in groups.values())}  "
  f"**Entities identified:** {len(groups)}  "
  f"**Total size:** {sum(f['size_mb'] for v in groups.values() for f in v):,.0f} MB")
P("")
P("Files grouped by source entity. Year column is best-effort filename parse "
  "(blank when no date pattern detected).")
P("")
P("---")
P("")

# Sort entities by total size desc
sorted_groups = sorted(groups.items(), key=lambda kv: -sum(f['size_mb'] for f in kv[1]))

for ent, files in sorted_groups:
    total_mb = sum(f['size_mb'] for f in files)
    years = sorted({f['year'] for f in files if f['year']})
    year_range = (
        f"{years[0]} – {years[-1]}" if len(years) > 1
        else (years[0] if years else "—")
    )
    fmt_mix = ", ".join(sorted({f['ext'].lstrip('.') for f in files}))
    P(f"## {ent}")
    P("")
    P(f"- Files: **{len(files)}**, size **{total_mb:,.1f} MB**, "
      f"years: **{year_range}**, formats: `{fmt_mix}`")
    # Show up to 6 examples
    P(f"- Sample files:")
    for f in sorted(files, key=lambda x: x['name'])[:6]:
        P(f"  - `{f['rel']}` ({f['size_mb']:,.1f} MB)")
    if len(files) > 6:
        P(f"  - … and {len(files) - 6} more")
    P("")

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text("\n".join(lines), encoding="utf-8")
print(f"Wrote: {OUT}")
print(f"Files: {sum(len(v) for v in groups.values())}")
print(f"Entities: {len(groups)}")
print(f"Total: {sum(f['size_mb'] for v in groups.values() for f in v):,.0f} MB")
