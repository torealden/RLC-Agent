"""
Run the WASDE wheat collector, then regenerate us_wheat_wasde.xlsx.

Called by update_wheat_wasde.bat (in turn called by the "Update WASDE Data"
button in us_wheat_balance_sheet.xlsm). Exits non-zero on any failure so the
calling batch file / VBA macro can detect it and tell the user -- no silent
failures.
"""

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.agents.collectors.us.usda_wasde_collector import USDAWASPECollector  # noqa: E402

FLAT_FILE_PATH = r"\\RLC-SERVER\dev\RLC-Agent\models\Food Grains\us_wheat_wasde.xlsx"


def main():
    collector = USDAWASPECollector()
    result = collector.collect(
        commodities=["wheat"],
        countries=["US"],
        marketing_years=[date.today().year - 1, date.today().year],
        use_cache=False,
    )
    if not result.success:
        print(f"COLLECTOR FAILED: {result.error_message}")
        sys.exit(1)
    if result.warnings:
        print(f"Warnings: {result.warnings}")
    print(f"Collector OK: {result.records_fetched} bronze rows saved.")

    from scripts.collectors.export_wheat_wasde_flatfile import fetch_rows, write_workbook

    rows = fetch_rows()
    if not rows:
        print("EXPORT FAILED: no rows in gold.wheat_wasde_vintage_ladder")
        sys.exit(1)
    write_workbook(rows, Path(FLAT_FILE_PATH))
    print(f"Export OK: {len(rows)} rows written to {FLAT_FILE_PATH}")


if __name__ == "__main__":
    main()
