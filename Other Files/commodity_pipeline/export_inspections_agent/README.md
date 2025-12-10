# FGIS Export Inspections Data Collection Agent

A comprehensive data collection agent for US export grain inspection data from the Federal Grain Inspection Service (FGIS).

## Overview

This agent automates the collection, parsing, and storage of weekly export inspection data published by USDA FGIS. It supports:

- **Automatic downloading** of CSV files from FGIS website
- **Parsing** of the 112-column FGIS CSV format
- **Storage** in MySQL, PostgreSQL, or SQLite databases
- **Aggregation** into summary tables for efficient querying
- **Marketing year tracking** with commodity-specific start dates
- **Quality metrics** tracking (moisture, test weight, protein, mycotoxins, etc.)

## Project Structure

```
export_inspections_agent/
├── agents/
│   └── export_inspections_agent.py    # Main orchestrator
├── config/
│   └── settings.py                     # Configuration classes
├── database/
│   └── models.py                       # SQLAlchemy ORM models
├── services/
│   ├── aggregation_service.py          # Data aggregation
│   └── query_service.py                # Query/reporting service
├── utils/
│   ├── csv_parser.py                   # FGIS CSV parser
│   └── download_manager.py             # File download manager
├── tests/
│   └── test_agent.py                   # Test suite
├── data/                               # Downloaded files (created at runtime)
├── requirements.txt
└── README.md
```

## Installation

```bash
# Clone or copy the project
cd export_inspections_agent

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Quick Start

### Using SQLite (Default - Easiest for Development)

```python
from agents.export_inspections_agent import ExportInspectionsAgent
from config.settings import AgentConfig, DatabaseType

# Create agent with SQLite
config = AgentConfig()
config.database.db_type = DatabaseType.SQLITE
config.database.sqlite_path = "./data/export_inspections.db"

agent = ExportInspectionsAgent(config)

# Initialize lookup tables
agent.initialize_lookup_tables()

# Download current year data
result = agent.download_data()
print(f"Download: {result}")

# Load data into database
if result.success:
    load_result = agent.load_file(result.file_path)
    print(f"Loaded {load_result.records_inserted} records")

# Run aggregations
agent.run_aggregations()

# Check status
status = agent.get_status()
print(status)
```

### Using MySQL

```python
from config.settings import AgentConfig, DatabaseType

config = AgentConfig()
config.database.db_type = DatabaseType.MYSQL
config.database.host = "localhost"
config.database.port = 3306
config.database.database = "export_inspections"
config.database.username = "your_user"
config.database.password = "your_password"

agent = ExportInspectionsAgent(config)
```

### Using PostgreSQL

```python
config.database.db_type = DatabaseType.POSTGRESQL
config.database.host = "localhost"
config.database.port = 5432
config.database.database = "export_inspections"
config.database.username = "your_user"
config.database.password = "your_password"
```

## Command Line Interface

```bash
# Download current year data
python -m agents.export_inspections_agent download

# Download specific year
python -m agents.export_inspections_agent download --year 2024

# Load data
python -m agents.export_inspections_agent load

# Run aggregations
python -m agents.export_inspections_agent aggregate

# Run weekly update (download + load + aggregate)
python -m agents.export_inspections_agent update

# Load historical data (2015-present)
python -m agents.export_inspections_agent historical --start-year 2015

# Check status
python -m agents.export_inspections_agent status

# Use different database
python -m agents.export_inspections_agent status --db-type sqlite --db-path ./mydata.db
```

## Data Model

### Raw Data Table
- **InspectionRecord**: Certificate-level inspection data (maps to FGIS CSV)

### Summary Tables (Pre-aggregated for Performance)
- **WeeklyCommoditySummary**: Weekly totals by commodity
- **WeeklyCountryExports**: Weekly exports by destination country
- **WeeklyRegionExports**: Weekly exports by destination region (EU, Asia, etc.)
- **WeeklyPortExports**: Weekly exports by US port region (Gulf, Pacific, etc.)
- **WheatClassExports**: Wheat-specific class breakdown (HRW, HRS, SRW, etc.)
- **WeeklyQualityStats**: Aggregated quality metrics

### Lookup Tables
- **Commodity**: Reference data with bushel weights and MY start dates
- **Country**: Destination countries with region mapping
- **Port**: US ports with region mapping
- **Grade**: Grain grades

## Querying Data

```python
from services.query_service import QueryService
from datetime import date

# Get a session
session = agent.get_session()
query_service = QueryService(session)

# Get weekly summary
summaries = query_service.get_weekly_summary(date(2025, 1, 16))

# Get commodity trend
trend = query_service.get_commodity_trend("SOYBEANS", weeks=10)

# Get top destinations
top_dest = query_service.get_top_destinations("SOYBEANS", marketing_year=2024, limit=10)

# Get region breakdown
regions = query_service.get_region_breakdown("CORN", marketing_year=2024)

# Get port breakdown
ports = query_service.get_port_breakdown("SOYBEANS")

# Get wheat class breakdown
wheat = query_service.get_wheat_class_breakdown(marketing_year=2024)

# Get quality stats
quality = query_service.get_quality_stats("SOYBEANS", marketing_year=2024)

# Marketing year progress
progress = query_service.get_marketing_year_progress("SOYBEANS", 2024)

session.close()
```

## Configuration

### Environment Variables

```bash
export DB_TYPE=mysql
export DB_HOST=localhost
export DB_PORT=3306
export DB_NAME=export_inspections
export DB_USER=username
export DB_PASSWORD=password
export LOG_LEVEL=INFO
```

```python
config = AgentConfig.from_environment()
```

### Custom Region Mappings

Destination region and port region mappings can be customized in `config/settings.py`:

```python
config.regions.destination_regions["NEW COUNTRY"] = "ASIA_OCEANIA"
config.regions.port_region_mapping["NEW PORT"] = "GULF"
```

## Marketing Years

The agent automatically calculates marketing years based on commodity:

| Commodity | MY Start |
|-----------|----------|
| Wheat | June 1 |
| Corn | September 1 |
| Soybeans | September 1 |
| Sorghum | September 1 |
| Barley | June 1 |

## Data Source

Data is downloaded from:
- **URL**: https://fgisonline.ams.usda.gov/exportgrainreport/
- **Files**: CY{YEAR}.csv (e.g., CY2025.csv)
- **Update Schedule**: Weekly, typically Monday for week ending previous Thursday

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_agent.py -v

# Run with coverage
pytest tests/ --cov=. --cov-report=html
```

## Scheduling Weekly Updates

### Using cron (Linux)

```bash
# Edit crontab
crontab -e

# Add entry to run every Monday at 2 PM
0 14 * * 1 cd /path/to/export_inspections_agent && /path/to/venv/bin/python -m agents.export_inspections_agent update >> /var/log/export_inspections.log 2>&1
```

### Using Task Scheduler (Windows)

Create a scheduled task to run:
```
python -m agents.export_inspections_agent update
```

## Integration with RLC Data Pipeline

This agent is designed to work alongside other RLC data collection agents:

```python
# Example: Master orchestrator integration
from agents.export_inspections_agent import ExportInspectionsAgent

class MasterOrchestrator:
    def __init__(self):
        self.export_inspections = ExportInspectionsAgent(config)
        # ... other agents
    
    def run_daily_updates(self):
        # Run export inspections update
        result = self.export_inspections.run_weekly_update()
        return result
```

## License

Internal use - Round Lakes Commodities

## Support

For questions or issues, contact the data engineering team.
