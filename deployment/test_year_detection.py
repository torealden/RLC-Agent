#!/usr/bin/env python3
"""Quick test to debug year detection"""
import openpyxl
import re

PATTERN_2DIGIT = re.compile(r'^(\d{2})/(\d{2})\s*$')
PATTERN_4DIGIT = re.compile(r'^(\d{4})/(\d{2})\s*$')

filepath = 'Models/Oilseeds/World Soybean Balance Sheets.xlsx'

print("Testing with read_only=True, data_only=True (like extractor):")
wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
sheet = wb['Brazil Soy Complex']

print(f"\nSheet max_row: {sheet.max_row}, max_column: {sheet.max_column}")

print("\nFirst 5 rows, cols 1-15:")
for row in range(1, 6):
    cells = []
    for col in range(1, 16):
        val = sheet.cell(row=row, column=col).value
        cells.append(val)
    print(f"  Row {row}: {cells}")

print("\nSearching for year patterns in first 10 rows, 50 cols:")
found = []
for row in range(1, 11):
    for col in range(1, 51):
        val = sheet.cell(row=row, column=col).value
        if val is not None:
            val_str = str(val).strip()
            if PATTERN_2DIGIT.match(val_str):
                found.append((row, col, val, "2-digit"))
            elif PATTERN_4DIGIT.match(val_str):
                found.append((row, col, val, "4-digit"))

print(f"  Found {len(found)} year patterns:")
for r, c, v, fmt in found[:20]:
    print(f"    Row {r}, Col {c}: '{v}' ({fmt})")
if len(found) > 20:
    print(f"    ... and {len(found) - 20} more")

wb.close()
