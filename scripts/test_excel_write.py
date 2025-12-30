#!/usr/bin/env python3
"""
Simple test script to verify Excel writing works with win32com.
This will help diagnose why the Census script says "Updated X cells" but data doesn't appear.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
EXCEL_FILE = PROJECT_ROOT / "Models" / "Oilseeds" / "US Soybean Trade.xlsx"

def test_excel_write():
    """Test writing to a specific cell in the Excel file."""

    try:
        import win32com.client
        import pythoncom
    except ImportError:
        print("ERROR: pywin32 not installed. Run: pip install pywin32")
        return False

    print(f"Testing Excel write to: {EXCEL_FILE}")
    print(f"File exists: {EXCEL_FILE.exists()}")

    if not EXCEL_FILE.exists():
        print("ERROR: Excel file not found!")
        return False

    try:
        # Initialize COM
        pythoncom.CoInitialize()

        # Create Excel instance
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = False  # Set to True to watch it happen
        excel.DisplayAlerts = False

        print("Opening workbook...")
        wb = excel.Workbooks.Open(str(EXCEL_FILE.resolve()))

        # Get the Soybean Exports sheet
        sheet_name = "Soybean Exports"
        print(f"Accessing sheet: {sheet_name}")
        ws = wb.Sheets(sheet_name)

        # Test read first
        print("\n--- READING TEST ---")
        test_row = 10
        test_col = 50  # Should be a data column
        current_value = ws.Cells(test_row, test_col).Value
        print(f"Current value at ({test_row}, {test_col}): {current_value}")

        # Check cell A10 to see what country is there
        country_name = ws.Cells(test_row, 1).Value
        print(f"Country at row {test_row}: {country_name}")

        # Check column header
        header_value = ws.Cells(2, test_col).Value
        print(f"Column header at col {test_col}: {header_value}")

        # Test write
        print("\n--- WRITING TEST ---")
        test_value = 12345.678
        print(f"Writing test value {test_value} to ({test_row}, {test_col})...")
        ws.Cells(test_row, test_col).Value = test_value

        # Verify write before save
        verify_before = ws.Cells(test_row, test_col).Value
        print(f"Value after write (before save): {verify_before}")

        # Save
        print("Saving workbook...")
        wb.Save()

        # Verify write after save
        verify_after = ws.Cells(test_row, test_col).Value
        print(f"Value after save: {verify_after}")

        # Close
        wb.Close(SaveChanges=True)
        excel.Quit()
        pythoncom.CoUninitialize()

        print("\n--- RESULT ---")
        if verify_after == test_value:
            print(f"SUCCESS: Value was written correctly!")
            print(f"Check Excel at: {sheet_name}, row {test_row}, column {test_col}")
            print(f"(Country: {country_name}, Date: {header_value})")
            return True
        else:
            print(f"FAILED: Expected {test_value}, got {verify_after}")
            return False

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_excel_write()
    sys.exit(0 if success else 1)
