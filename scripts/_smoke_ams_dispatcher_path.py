"""Smoke-test the dispatcher path: instantiate via registry, call .collect().
This mirrors exactly what core.collection_status runs do.
"""
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()

from src.dispatcher.collector_registry import CollectorRegistry

reg = CollectorRegistry()
collector = reg.get_collector('usda_ams_cash_prices')
print(f"Got collector: {type(collector).__name__}")
print(f"Slug count: {len(collector.slug_ids)}")

# Call .collect() like the dispatcher does — for just yesterday/today
from datetime import date, timedelta
end = date.today()
start = end - timedelta(days=1)

print(f"\nRunning collector.collect(start={start}, end={end})...")
result = collector.collect(start_date=start, end_date=end)

print(f"\nResult:")
print(f"  success: {result.success}")
print(f"  records_fetched: {result.records_fetched}")
print(f"  period: {result.period_start} -> {result.period_end}")
print(f"  warnings: {len(result.warnings) if result.warnings else 0}")
print(f"  error: {result.error_message}")
