# HB Weekly Report Writer Agent

An autonomous Python agent designed to generate the HigbyBarrett (HB) agricultural market report every Tuesday. This agent operates within the RLC Master Agent multi-agent ecosystem, coordinating with data pipelines and services to compile comprehensive market analysis documents.

## Overview

The HB Report Writer Agent automates the production of weekly agricultural commodity market reports, covering:

- **Corn** - Futures prices, export activity, supply/demand analysis
- **Wheat** - HRW/SRW prices, global trade developments
- **Soybeans** - Crush data, export pace, South American competition
- **Soybean Meal** - Domestic demand, export markets
- **Soybean Oil** - Biofuel demand, renewable diesel trends

Each report includes:
- Executive Summary
- Macro and Weather Update
- Commodity Deep Dives with bullish/bearish factors
- Price and Spread Tables
- Synthesis and Outlook
- Key Fundamental Triggers (Watchlist)

## Architecture

```
hb_weekly_report_writer/
├── config/
│   ├── __init__.py
│   └── settings.py              # Configuration dataclasses
├── agents/
│   ├── __init__.py
│   ├── internal_data_agent.py   # Dropbox/Database data fetching
│   ├── price_data_agent.py      # AMS API price retrieval
│   ├── market_research_agent.py # Bull/bear factor analysis
│   └── report_writer_agent.py   # Content generation
├── database/
│   ├── __init__.py
│   └── models.py                # SQLAlchemy ORM models
├── services/
│   ├── __init__.py
│   ├── orchestrator.py          # Main workflow coordinator
│   ├── scheduler.py             # Weekly scheduling
│   └── document_builder.py      # Word document generation
├── utils/
│   ├── __init__.py
│   ├── formatting.py            # Data formatting helpers
│   └── validation.py            # Data validation
├── templates/                    # Document templates (optional)
├── main.py                       # CLI entry point
├── requirements.txt
└── README.md
```

## Installation

```bash
# Navigate to the agent directory
cd commodity_pipeline/hb_weekly_report_writer

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# For LLM support (optional)
pip install openai  # or: pip install anthropic
```

## Configuration

The agent uses environment variables for configuration. Create a `.env` file or set these variables:

```bash
# Data Source
HB_DATA_SOURCE=dropbox          # or "database"

# Dropbox Integration
DROPBOX_ACCESS_TOKEN=your_token
DROPBOX_APP_KEY=your_key
DROPBOX_APP_SECRET=your_secret
DROPBOX_REFRESH_TOKEN=your_refresh_token

# API Manager
API_MANAGER_URL=http://localhost:8000
API_MANAGER_KEY=your_api_key
USDA_API_KEY=your_usda_key

# Database (if using database source)
HB_DB_TYPE=postgresql
HB_DB_HOST=localhost
HB_DB_PORT=5432
HB_DB_NAME=hb_market_data
HB_DB_USER=username
HB_DB_PASSWORD=password

# LLM (optional)
LLM_PROVIDER=openai              # or "ollama" or "anthropic"
LLM_MODEL=gpt-4
OPENAI_API_KEY=your_key
OLLAMA_URL=http://localhost:11434
OLLAMA_MODEL=llama2:13b

# Notifications
SMTP_SERVER=smtp.example.com
SMTP_PORT=587
SMTP_USER=user@example.com
SMTP_PASSWORD=password
NOTIFICATION_FROM=reports@example.com
REPORT_RECIPIENTS=analyst1@example.com,analyst2@example.com
ERROR_RECIPIENTS=admin@example.com
SLACK_WEBHOOK_URL=https://hooks.slack.com/...

# Logging
HB_LOG_LEVEL=INFO
```

## Usage

### Generate Report

```bash
# Generate report for today
python main.py generate

# Generate for specific date
python main.py generate --date 2024-12-03

# Output as JSON
python main.py generate --json
```

### Scheduling

```bash
# Start background scheduler
python main.py schedule --start

# Use APScheduler (production)
python main.py schedule --start --apscheduler

# Manually trigger generation
python main.py schedule --trigger

# Show cron expression
python main.py schedule --cron
```

### Status and Validation

```bash
# Show agent status
python main.py status

# Validate configuration
python main.py validate

# Run component tests
python main.py test
```

### As a Module

```python
from commodity_pipeline.hb_weekly_report_writer import (
    HBReportOrchestrator,
    HBWeeklyReportConfig,
)
from datetime import date

# Load configuration
config = HBWeeklyReportConfig.from_environment()

# Create orchestrator
orchestrator = HBReportOrchestrator(config)

# Generate report
result = orchestrator.run_weekly_report(date.today())

if result.success:
    print(f"Report saved to: {result.document_path}")
else:
    print(f"Errors: {result.errors}")
```

## Workflow

1. **Data Collection** (Tuesday morning)
   - Fetch internal spreadsheet from Dropbox
   - Retrieve market prices from API Manager
   - Gather commodity news and developments

2. **Analysis**
   - Identify bullish and bearish factors per commodity
   - Calculate price changes (week-over-week, year-over-year)
   - Generate swing factors and catalysts

3. **Content Generation**
   - Write executive summary
   - Generate commodity deep dives
   - Build price and spread tables
   - Compile key triggers watchlist

4. **Document Assembly**
   - Create Word document with styling
   - Add tables and formatting
   - Include metadata

5. **Delivery**
   - Save to local directory
   - Upload to Dropbox
   - Send notifications

## Error Handling

The agent handles missing data gracefully:

1. **Missing Internal Data**: Creates questions for human input
2. **Missing Prices**: Uses previous day or marks as N/A
3. **LLM Failures**: Falls back to template-based generation
4. **API Failures**: Retries with exponential backoff

Questions are logged and can be answered within a configurable timeout (default: 2 hours). If no answer is received, the agent proceeds with placeholders.

## Integration Points

### API Manager Agent

The agent expects the API Manager to provide endpoints:
- `GET /prices/{series_id}?date={date}` - Fetch price for series
- Response: `{"price": 450.25, "date": "2024-12-03", "unit": "cents/bu"}`

### Dropbox

Expected file structure:
```
/rlc documents/
├── llm model and documents/
│   ├── data/
│   │   └── HB Weekly Data.xlsx
│   └── reports/
│       └── [Generated reports]
```

### Database (Future)

When migrating to database, the agent will query:
- `supply_demand` table
- `forecasts` table
- `price_history` table

## Output

The agent generates a Word document (.docx) with:
- Styled headings (Heading 1, 2, 3)
- Formatted price tables
- Spread tables with % of full carry
- Professional formatting matching HB report style

Document metadata includes:
- Generation timestamp
- Data sources used
- Any placeholders or LLM estimates

## Development

### Running Tests

```bash
# Run component tests
python main.py test

# Run validation
python main.py validate
```

### Adding New Commodities

1. Update `CommodityConfig` in `config/settings.py`
2. Add analysis logic in `market_research_agent.py`
3. Add section generation in `report_writer_agent.py`

### Adding New Data Sources

1. Create new agent in `agents/` directory
2. Implement fetch interface with standard result dataclass
3. Integrate in `orchestrator.py`

## Support

For issues or questions:
- Check configuration with `python main.py validate`
- Review logs in `./logs/` directory
- Report issues at https://github.com/torealden/RLC-Agent/issues

## License

Proprietary - HigbyBarrett / RLC

## Version History

- **1.0.0** (December 2024) - Initial release
  - Dropbox integration for internal data
  - API Manager integration for prices
  - Word document generation
  - LLM-assisted content generation
  - Weekly scheduling
