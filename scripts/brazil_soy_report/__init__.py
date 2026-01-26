"""
Brazil Soybean Weekly Report Pipeline

A modular pipeline for generating weekly Brazil Soybean market reports.

Usage:
    # Run full pipeline
    python -m scripts.brazil_soy_report.run_brazil_soy_report

    # Or import components
    from scripts.brazil_soy_report.brazil_soy_config import get_report_week_dates
    from scripts.brazil_soy_report.weekly_data_pack import generate_data_pack
"""

from .brazil_soy_config import (
    DATA_SOURCES,
    RAW_DIR,
    PROCESSED_DIR,
    OUTPUT_DIR,
    get_report_week_dates,
)

__all__ = [
    'DATA_SOURCES',
    'RAW_DIR',
    'PROCESSED_DIR',
    'OUTPUT_DIR',
    'get_report_week_dates',
]
