"""
Brazil Soybean Weekly Report - Configuration

This file defines all data sources, column mappings, and validation rules
for the weekly Brazil Soybean report pipeline.

Report Window: Wednesday to Wednesday (Wed close to Wed close)
"""

from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import date, timedelta

# =============================================================================
# PATHS
# =============================================================================

PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "brazil_soy"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR = DATA_DIR / "output"

# Ensure directories exist
for d in [DATA_DIR, RAW_DIR, PROCESSED_DIR, OUTPUT_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# =============================================================================
# DATE HELPERS
# =============================================================================

def get_report_week_dates(reference_date: date = None) -> Dict[str, date]:
    """
    Calculate the Wednesday-to-Wednesday report window.

    Args:
        reference_date: Any date in the target week. Defaults to today.

    Returns:
        Dict with 'start_wed' and 'end_wed' dates
    """
    if reference_date is None:
        reference_date = date.today()

    # Find the most recent Wednesday (end of report period)
    # Wednesday = 2 in Python's weekday()
    days_since_wed = (reference_date.weekday() - 2) % 7
    end_wed = reference_date - timedelta(days=days_since_wed)

    # Start Wednesday is 7 days before end
    start_wed = end_wed - timedelta(days=7)

    return {
        'start_wed': start_wed,
        'end_wed': end_wed,
        'week_label': f"{start_wed.strftime('%b %d')} - {end_wed.strftime('%b %d, %Y')}"
    }


# =============================================================================
# DATA SOURCE CONFIGURATIONS
# =============================================================================

@dataclass
class SourceConfig:
    """Configuration for a data source"""
    name: str
    description: str
    frequency: str  # daily, weekly, monthly
    file_pattern: str  # Expected filename pattern
    required_columns: List[str]
    value_column: str
    date_column: str
    unit: str
    notes: str = ""


# CEPEA - Paranagua daily cash price
CEPEA_PARANAGUA = SourceConfig(
    name="CEPEA_PARANAGUA",
    description="CEPEA Soy Paranagua daily cash price",
    frequency="daily",
    file_pattern="*CEPEA*SOY_PARANAGUA*.xls",
    required_columns=["Data", "À vista R$"],
    value_column="À vista R$",
    date_column="Data",
    unit="BRL/60kg",
    notes="Use 'À vista R$' column only. Ignore 'À prazo' (forward) prices."
)

# CEPEA - USD/BRL exchange rate
CEPEA_USDBRL = SourceConfig(
    name="CEPEA_USDBRL",
    description="CEPEA USD/BRL monthly exchange rate",
    frequency="monthly",
    file_pattern="*CEPEA*USDBRL*.xls",
    required_columns=["Mês", "Média"],
    value_column="Média",
    date_column="Mês",
    unit="BRL/USD",
    notes="Monthly average rate. Use for qualitative FX context."
)

# IMEA - Mato Grosso spot prices
IMEA_MT = SourceConfig(
    name="IMEA_MT",
    description="IMEA Mato Grosso soy spot prices",
    frequency="weekly",
    file_pattern="*IMEA*MT_SOY*.xls",
    required_columns=["Data", "Indicador", "Preço"],
    value_column="Preço",
    date_column="Data",
    unit="BRL/sc",
    notes="Filter: Indicador = 'Preço soja disponível compra' (spot). Ignore parity/contract."
)

# CONAB - State prices (monthly context)
CONAB_PRICES = SourceConfig(
    name="CONAB_PRICES",
    description="CONAB monthly soy prices by state",
    frequency="monthly",
    file_pattern="*CONAB*SOY_PRICES*.csv",
    required_columns=["Mês", "UF", "Preço"],
    value_column="Preço",
    date_column="Mês",
    unit="BRL/60kg",
    notes="Use MT column only for MVP. Monthly context, not weekly change."
)

# CBOT - Futures settlements
CBOT_FUTURES = SourceConfig(
    name="CBOT_FUTURES",
    description="CBOT Soy Futures daily settlements",
    frequency="daily",
    file_pattern="*CBOT*SOY_FUTURES*.csv",
    required_columns=["Date", "Contract", "Settle"],
    value_column="Settle",
    date_column="Date",
    unit="USc/bu",
    notes="Use nearby contract for weekly reference. Barchart format expected."
)

# ANEC - Export shipments (manual structured table)
ANEC_EXPORTS = SourceConfig(
    name="ANEC_EXPORTS",
    description="ANEC weekly soy/meal export forecasts",
    frequency="weekly",
    file_pattern="*ANEC*SHIPMENTS*.csv",
    required_columns=["Week", "Soy_MMT", "Meal_MMT", "YTD_2025", "YTD_2026"],
    value_column="Soy_MMT",
    date_column="Week",
    unit="MMT",
    notes="PDF source - maintain manual CSV with YTD 2025 vs 2026 comparison."
)

# NOAA - Weather signal (manual)
NOAA_WEATHER = SourceConfig(
    name="NOAA_WEATHER",
    description="NOAA Brazil 7-day precipitation signal",
    frequency="weekly",
    file_pattern="*NOAA*BRAZIL*.csv",
    required_columns=["Week", "Signal", "Notes"],
    value_column="Signal",
    date_column="Week",
    unit="categorical",
    notes="Manual entry: 'dry', 'neutral', or 'wet'. Store PNG map separately."
)


# All sources as a dict for easy lookup
DATA_SOURCES = {
    "cepea_paranagua": CEPEA_PARANAGUA,
    "cepea_usdbrl": CEPEA_USDBRL,
    "imea_mt": IMEA_MT,
    "conab_prices": CONAB_PRICES,
    "cbot_futures": CBOT_FUTURES,
    "anec_exports": ANEC_EXPORTS,
    "noaa_weather": NOAA_WEATHER,
}


# =============================================================================
# VALIDATION RULES
# =============================================================================

VALIDATION_RULES = {
    "cepea_paranagua": {
        "min_value": 50,      # BRL/60kg - reasonable floor
        "max_value": 300,     # BRL/60kg - reasonable ceiling
        "max_daily_change_pct": 10,  # Alert if >10% daily move
    },
    "cbot_futures": {
        "min_value": 800,     # USc/bu
        "max_value": 2000,    # USc/bu
        "max_daily_change_pct": 5,
    },
    "imea_mt": {
        "min_value": 50,      # BRL/sc
        "max_value": 250,     # BRL/sc
    },
}


# =============================================================================
# REPORT OUTPUT CONFIGURATION
# =============================================================================

REPORT_CONFIG = {
    "title": "Brazil Soybean Weekly Report",
    "sections": [
        "price_snapshot",      # CEPEA, IMEA, CBOT prices
        "price_changes",       # Wed-to-Wed changes
        "fx_context",          # USD/BRL level
        "exports_ytd",         # ANEC YTD comparison
        "weather_signal",      # NOAA summary
        "commentary_prompt",   # LLM prompt for narrative
    ],
    "charts": [
        "cepea_paranagua_7d",  # 7-day CEPEA price chart
        "cbot_vs_cepea",       # Overlay comparison
    ],
    "output_formats": ["json", "txt", "pdf"],
}


# =============================================================================
# FILE NAMING CONVENTION
# =============================================================================

def get_expected_filename(source: str, report_date: date) -> str:
    """
    Generate expected filename for a source file.

    Format: YYYY-MM-DD__SOURCE__CONTENT__DETAIL.ext

    Example: 2026-01-21__CEPEA__SOY_PARANAGUA_RS_SC_DAILY.xls
    """
    date_str = report_date.strftime("%Y-%m-%d")

    patterns = {
        "cepea_paranagua": f"{date_str}__CEPEA__SOY_PARANAGUA_RS_SC_DAILY.xls",
        "cepea_usdbrl": f"{date_str}__CEPEA__USDBRL_MONTHLY.xls",
        "imea_mt": f"{date_str}__IMEA__MT_SOY_PRICES_RS_SC.xls",
        "conab_prices": f"{date_str}__CONAB__SOY_PRICES_BY_STATE_RS_SC.csv",
        "cbot_futures": f"{date_str}__CBOT__SOY_FUTURES_SETTLEMENTS_BARCHART.csv",
        "anec_exports": f"{date_str}__ANEC__SOY_SHIPMENTS_WEEK{report_date.isocalendar()[1]:02d}_{report_date.year}.csv",
        "noaa_weather": f"{date_str}__NOAA__BRAZIL_WEATHER_SIGNAL.csv",
    }

    return patterns.get(source, f"{date_str}__{source.upper()}__DATA.csv")


if __name__ == "__main__":
    # Test the configuration
    print("Brazil Soy Report Configuration")
    print("=" * 50)

    week = get_report_week_dates()
    print(f"Report Week: {week['week_label']}")
    print(f"  Start: {week['start_wed']}")
    print(f"  End:   {week['end_wed']}")
    print()

    print("Data Sources:")
    for key, cfg in DATA_SOURCES.items():
        print(f"  - {cfg.name}: {cfg.description}")
        print(f"    Frequency: {cfg.frequency}, Unit: {cfg.unit}")
    print()

    print("Expected filenames for this week:")
    for source in DATA_SOURCES:
        fname = get_expected_filename(source, week['end_wed'])
        print(f"  {fname}")
