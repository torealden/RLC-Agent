#!/usr/bin/env python3
"""
Feed Grains Extractor for PostgreSQL
Extracts balance sheet data from Feed Grains Excel files into PostgreSQL.

Usage:
    python extract_feed_grains.py --analyze     # See what's in the files
    python extract_feed_grains.py --extract     # Extract to PostgreSQL
    python extract_feed_grains.py --list-files  # List available files
"""

import argparse
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
from sqlalchemy import create_engine, text

# === CONFIGURATION ===
# Update these paths for your system

FEED_GRAINS_DIR = Path(r"C:\Users\torem\Dropbox\RLC Documents\LLM Model and Documents\Projects\RLC-Agent\Models\Feed Grains")

# PostgreSQL connection
PG_PASSWORD = "SoupBoss1"  # PostgreSQL password
PG_URL = f"postgresql://postgres:{PG_PASSWORD}@localhost:5432/rlc_commodities"

# Files to extract (balance sheets only - skip price files for now)
BALANCE_SHEET_FILES = [
    "Balance Sheets - World Feed Grains.xlsx",
    "US Feed Grain Balance Sheets.xlsx",
    "World Corn Balance Sheets.xlsx",
    "US Ethanol Balance Sheet.xlsx",
]

# Commodity mapping based on filename
FILE_COMMODITY_MAP = {
    "Balance Sheets - World Feed Grains.xlsx": "feed_grains",
    "US Feed Grain Balance Sheets.xlsx": "us_feed_grains",
    "World Corn Balance Sheets.xlsx": "corn",
    "US Ethanol Balance Sheet.xlsx": "ethanol",
}


class FeedGrainsExtractor:
    """Extracts Feed Grains balance sheet data to PostgreSQL."""

    def __init__(self, source_dir: Path = FEED_GRAINS_DIR):
        self.source_dir = source_dir
        self.engine = None

    def connect_db(self):
        """Connect to PostgreSQL."""
        print(f"Connecting to PostgreSQL...")
        self.engine = create_engine(PG_URL)
        print("  ✓ Connected")

    def find_year_columns(self, df: pd.DataFrame) -> Dict[int, str]:
        """Find columns that represent marketing years."""
        year_columns = {}

        for col_idx, col in enumerate(df.columns):
            col_str = str(col).strip()

            # Pattern 1: "2023/24" or "23/24"
            match = re.search(r"(\d{2,4})/(\d{2})", col_str)
            if match:
                year1 = match.group(1)
                if len(year1) == 2:
                    year1 = "20" + year1 if int(year1) < 50 else "19" + year1
                year_columns[col_idx] = f"{year1}/{match.group(2)}"
                continue

            # Pattern 2: Just a year "2023" or "2024"
            match = re.search(r"(19|20)\d{2}$", col_str)
            if match:
                year = match.group(0)
                year_columns[col_idx] = year
                continue

            # Pattern 3: Two-digit year like "'23" or "23"
            match = re.search(r"'?(\d{2})$", col_str)
            if match:
                year_num = int(match.group(1))
                if 0 <= year_num <= 99:
                    full_year = 2000 + year_num if year_num < 50 else 1900 + year_num
                    if 1960 <= full_year <= 2030:
                        year_columns[col_idx] = str(full_year)

        return year_columns

    def is_complex_sheet(self, df: pd.DataFrame) -> bool:
        """Check if this is a complex balance sheet (has year columns and country data)."""
        year_cols = self.find_year_columns(df)
        if len(year_cols) < 3:
            return False

        # Look for balance sheet indicators
        text_content = df.astype(str).values.flatten()
        text_blob = " ".join(text_content[:500]).lower()

        indicators = ["production", "supply", "demand", "stocks", "exports", "imports", "consumption"]
        matches = sum(1 for ind in indicators if ind in text_blob)

        return matches >= 2

    def extract_sheet(self, df: pd.DataFrame, sheet_name: str,
                      source_file: str, commodity: str) -> List[Dict]:
        """Extract data from a single sheet."""
        records = []
        year_columns = self.find_year_columns(df)

        if not year_columns:
            return records

        current_country = None
        current_section = None

        for row_idx, row in df.iterrows():
            first_cell = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""

            # Skip empty rows
            if not first_cell or first_cell.lower() in ['nan', 'none', '']:
                continue

            # Detect country (usually bold/larger or followed by specific patterns)
            if self._looks_like_country(first_cell):
                current_country = first_cell
                current_section = None
                continue

            # Detect section headers
            if self._looks_like_section(first_cell):
                current_section = first_cell
                continue

            # Extract data row
            if current_country:
                metric = first_cell

                for col_idx, marketing_year in year_columns.items():
                    if col_idx < len(row):
                        value = row.iloc[col_idx]

                        if pd.notna(value) and self._is_numeric(value):
                            try:
                                numeric_value = float(str(value).replace(",", "").replace(" ", ""))

                                records.append({
                                    "source_file": source_file,
                                    "sheet_name": sheet_name,
                                    "commodity": commodity,
                                    "country": current_country,
                                    "section": current_section,
                                    "metric": metric,
                                    "marketing_year": marketing_year,
                                    "value": numeric_value,
                                    "unit": "1000 MT",  # Default assumption
                                    "extracted_at": datetime.now().isoformat()
                                })
                            except (ValueError, TypeError):
                                pass

        return records

    def _looks_like_country(self, text: str) -> bool:
        """Heuristic to detect country names."""
        countries = [
            "united states", "us", "usa", "brazil", "argentina", "china",
            "eu", "european union", "india", "russia", "ukraine", "canada",
            "australia", "mexico", "japan", "south korea", "indonesia",
            "world", "total", "other", "africa", "asia", "europe"
        ]
        text_lower = text.lower().strip()
        return any(c in text_lower for c in countries) and len(text) < 50

    def _looks_like_section(self, text: str) -> bool:
        """Heuristic to detect section headers."""
        sections = ["supply", "demand", "trade", "use", "disappearance"]
        text_lower = text.lower().strip()
        return any(s in text_lower for s in sections) and len(text) < 30

    def _is_numeric(self, value) -> bool:
        """Check if value is numeric."""
        try:
            val_str = str(value).replace(",", "").replace(" ", "").strip()
            float(val_str)
            return True
        except (ValueError, TypeError):
            return False

    def analyze_file(self, filepath: Path) -> Dict:
        """Analyze a single Excel file."""
        print(f"\nAnalyzing: {filepath.name}")

        try:
            xlsx = pd.ExcelFile(filepath, engine='openpyxl')
        except Exception as e:
            print(f"  ✗ Error opening file: {e}")
            return {"error": str(e)}

        results = {
            "filename": filepath.name,
            "sheets": [],
            "complex_sheets": [],
            "total_sheets": len(xlsx.sheet_names)
        }

        for sheet_name in xlsx.sheet_names:
            try:
                df = pd.read_excel(xlsx, sheet_name=sheet_name, header=None)
                year_cols = self.find_year_columns(df)
                is_complex = self.is_complex_sheet(df)

                sheet_info = {
                    "name": sheet_name,
                    "rows": len(df),
                    "cols": len(df.columns),
                    "year_columns": len(year_cols),
                    "is_complex": is_complex
                }
                results["sheets"].append(sheet_info)

                if is_complex:
                    results["complex_sheets"].append(sheet_name)
                    print(f"  ✓ {sheet_name}: {len(year_cols)} year columns (COMPLEX)")
                else:
                    print(f"    {sheet_name}: {len(year_cols)} year columns")

            except Exception as e:
                print(f"  ✗ {sheet_name}: Error - {e}")

        return results

    def extract_file(self, filepath: Path, commodity: str) -> int:
        """Extract all complex sheets from a file to PostgreSQL."""
        print(f"\nExtracting: {filepath.name}")

        try:
            xlsx = pd.ExcelFile(filepath, engine='openpyxl')
        except Exception as e:
            print(f"  ✗ Error opening file: {e}")
            return 0

        all_records = []

        for sheet_name in xlsx.sheet_names:
            try:
                df = pd.read_excel(xlsx, sheet_name=sheet_name, header=None)

                if self.is_complex_sheet(df):
                    records = self.extract_sheet(df, sheet_name, filepath.name, commodity)
                    all_records.extend(records)
                    print(f"  ✓ {sheet_name}: {len(records):,} records")

            except Exception as e:
                print(f"  ✗ {sheet_name}: Error - {e}")

        # Save to PostgreSQL
        if all_records:
            df_records = pd.DataFrame(all_records)
            df_records.to_sql(
                "commodity_balance_sheets",
                self.engine,
                if_exists="append",
                index=False
            )
            print(f"  → Saved {len(all_records):,} records to PostgreSQL")

        return len(all_records)

    def run_analysis(self):
        """Analyze all balance sheet files."""
        print("="*60)
        print("  FEED GRAINS FILE ANALYSIS")
        print("="*60)

        for filename in BALANCE_SHEET_FILES:
            filepath = self.source_dir / filename
            if filepath.exists():
                self.analyze_file(filepath)
            else:
                print(f"\n✗ File not found: {filename}")

    def run_extraction(self):
        """Extract all balance sheet files to PostgreSQL."""
        print("="*60)
        print("  FEED GRAINS EXTRACTION TO POSTGRESQL")
        print("="*60)

        self.connect_db()

        total_records = 0

        for filename in BALANCE_SHEET_FILES:
            filepath = self.source_dir / filename
            if filepath.exists():
                commodity = FILE_COMMODITY_MAP.get(filename, "unknown")
                records = self.extract_file(filepath, commodity)
                total_records += records
            else:
                print(f"\n✗ File not found: {filename}")

        print("\n" + "="*60)
        print(f"  EXTRACTION COMPLETE: {total_records:,} total records")
        print("="*60)

        # Show updated counts
        with self.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT commodity, COUNT(*) as count FROM commodity_balance_sheets GROUP BY commodity ORDER BY count DESC"
            ))
            print("\nRecords by commodity in database:")
            for row in result:
                print(f"  {row[0]}: {row[1]:,}")

    def list_files(self):
        """List available files in the Feed Grains directory."""
        print("="*60)
        print("  AVAILABLE FILES IN FEED GRAINS")
        print("="*60)

        for f in self.source_dir.glob("*.xlsx"):
            size_kb = f.stat().st_size / 1024
            in_extract_list = "✓" if f.name in BALANCE_SHEET_FILES else " "
            print(f"  [{in_extract_list}] {f.name} ({size_kb:.0f} KB)")

        print("\n[✓] = Will be extracted")


def main():
    parser = argparse.ArgumentParser(description="Feed Grains Extractor for PostgreSQL")
    parser.add_argument("--analyze", action="store_true", help="Analyze files without extracting")
    parser.add_argument("--extract", action="store_true", help="Extract data to PostgreSQL")
    parser.add_argument("--list-files", action="store_true", help="List available files")

    args = parser.parse_args()

    extractor = FeedGrainsExtractor()

    if args.list_files:
        extractor.list_files()
    elif args.analyze:
        extractor.run_analysis()
    elif args.extract:
        extractor.run_extraction()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
