# Wheat Tender Monitoring System

## Overview

The Wheat Tender Monitoring System automatically tracks international wheat tender announcements and results from major importing countries. It collects data from government agencies and news sources, parses tender details using NLP/regex patterns, and sends alerts when significant tenders are detected.

## Key Importing Countries

| Country | Agency | Typical Volume | Frequency |
|---------|--------|----------------|-----------|
| **Egypt** | Mostakbal Misr (formerly GASC) | 50-60K MT/tender | Every 10-12 days |
| **Algeria** | OAIC | 400-600K MT | Monthly |
| **Saudi Arabia** | SAGO | 500K+ MT | Periodic |
| **Iraq** | Grain Board of Iraq | Variable | Periodic |
| **Tunisia** | State Grains Agency | 50-100K MT | Monthly |
| **Morocco** | ONICL | Variable | Periodic |

## Data Sources

### Tier 1: Government Sources (Primary)

These are preferred as they provide authoritative tender information directly.

| Source | URL | Language | Status |
|--------|-----|----------|--------|
| **OAIC** (Algeria) | http://www.oaic.dz | French/Arabic | Implemented |
| **SAGO** (Saudi Arabia) | https://www.sago.gov.sa | Arabic | Implemented |
| **Egypt** | N/A (via news only) | - | News sources |

**Note:** Egypt's Mostakbal Misr does not publish tenders directly - they must be monitored via news sources.

### Tier 2: News Aggregators

Used when government sources are unavailable or for supplementary coverage.

| Source | URL | Type | Status |
|--------|-----|------|--------|
| **Agricensus** | https://www.agricensus.com | Web scraping | Implemented |
| **AgroChart** | https://www.agrochart.com | Web scraping | Implemented |

### Tier 3: Commercial (Future)

Premium sources that may be added if needed:
- Reuters Eikon (API, ~$12-22K/year)
- Bloomberg Terminal (API, ~$24K/year)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Wheat Tender Monitor                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ OAIC Scraper │  │ SAGO Scraper │  │  Agricensus  │   ...    │
│  │  (Algeria)   │  │   (S.Arabia) │  │   Scraper    │          │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘          │
│         │                 │                 │                   │
│         └────────────────┬┴─────────────────┘                   │
│                          ▼                                       │
│              ┌───────────────────────┐                          │
│              │    TenderParser       │                          │
│              │ (NLP/Regex Extraction)│                          │
│              └───────────┬───────────┘                          │
│                          ▼                                       │
│              ┌───────────────────────┐                          │
│              │    Alert Manager      │───┬──▶ Email (SendGrid)  │
│              │  (Trigger Evaluation) │   ├──▶ Slack (Webhook)   │
│              └───────────┬───────────┘   └──▶ SMS (Twilio)      │
│                          ▼                                       │
│              ┌───────────────────────┐                          │
│              │      Database         │                          │
│              │  (bronze/silver/gold) │                          │
│              └───────────────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

## Installation

### Prerequisites

```bash
pip install requests beautifulsoup4 feedparser pandas
```

### Optional (for alerts)

```bash
pip install sendgrid twilio
```

### Environment Variables

```bash
# Notifications (optional)
export SENDGRID_API_KEY="SG...."
export SENDGRID_FROM_EMAIL="alerts@your-domain.com"
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
export TWILIO_ACCOUNT_SID="AC..."
export TWILIO_AUTH_TOKEN="..."
export TWILIO_FROM_NUMBER="+1234567890"

# Translation (optional, for Arabic/French sources)
export GOOGLE_TRANSLATE_API_KEY="..."
```

## Usage

### Command Line

```bash
# Run once
python scripts/monitor_wheat_tenders.py

# Run as daemon (continuous monitoring)
python scripts/monitor_wheat_tenders.py --daemon

# Custom interval (30 minutes)
python scripts/monitor_wheat_tenders.py --daemon --interval 30

# Save results to file
python scripts/monitor_wheat_tenders.py --output results.json

# Verbose output
python scripts/monitor_wheat_tenders.py -v
```

### Direct Collector Usage

```python
from src.agents.collectors.tenders.wheat_tender_collector import (
    WheatTenderCollector,
    WheatTenderConfig,
)

# Initialize
config = WheatTenderConfig()
collector = WheatTenderCollector(config)

# Collect tenders
result = collector.collect()
print(f"Found {result.records_fetched} tenders")

# Get Egypt-specific tenders
egypt_tenders = collector.get_egypt_tenders()

# Scan for alerts
alerts = collector.scan_for_alerts()
for alert in alerts:
    print(f"Alert: {alert['message']}")
```

### Cron Job Setup

```bash
# Add to crontab (runs every hour)
0 * * * * /path/to/python /path/to/scripts/monitor_wheat_tenders.py >> /var/log/wheat_tenders.log 2>&1
```

### Systemd Service

```ini
# /etc/systemd/system/wheat-tender-monitor.service
[Unit]
Description=Wheat Tender Monitor
After=network.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/RLC-Agent
ExecStart=/path/to/python scripts/monitor_wheat_tenders.py --daemon
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Database Schema

### Bronze Layer (Raw Data)

```sql
CREATE TABLE bronze.wheat_tender_raw (
    id BIGSERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    source_article_id VARCHAR(200),
    captured_at TIMESTAMPTZ NOT NULL,
    headline TEXT,
    article_url TEXT,
    raw_text TEXT,
    country_raw VARCHAR(100),
    agency_raw VARCHAR(200),
    volume_value NUMERIC(15, 2),
    price_value NUMERIC(12, 4),
    price_type VARCHAR(20),
    origins_raw TEXT,
    tender_type VARCHAR(50),
    parse_confidence NUMERIC(5, 4),
    ...
);
```

### Silver Layer (Standardized)

```sql
CREATE TABLE silver.wheat_tender (
    id SERIAL PRIMARY KEY,
    raw_id BIGINT REFERENCES bronze.wheat_tender_raw(id),
    tender_type VARCHAR(50) NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    country_name VARCHAR(100) NOT NULL,
    agency_code VARCHAR(50),
    volume_mt NUMERIC(15, 2),
    price_usd_mt NUMERIC(12, 4),
    price_type VARCHAR(20),
    origins TEXT[],
    suppliers TEXT[],
    shipment_start DATE,
    shipment_end DATE,
    ...
);
```

### Gold Layer (Business Views)

```sql
-- Recent tenders
SELECT * FROM gold.recent_wheat_tenders;

-- Tenders by country
SELECT * FROM gold.wheat_tender_by_country;

-- Origin market share
SELECT * FROM gold.wheat_tender_origin_share;
```

## Data Captured

For each tender, the system captures:

| Field | Description | Example |
|-------|-------------|---------|
| `tender_date` | Announcement/result date | 2025-01-10 |
| `country` | Importing country | Egypt |
| `agency` | Purchasing agency | Mostakbal Misr |
| `commodity` | Wheat type | Milling wheat |
| `volume_mt` | Volume (MT) | 480,000 |
| `price_usd_mt` | Price per MT | $275.50 |
| `price_type` | FOB/C&F/CIF | C&F |
| `origins` | Supplier countries | Russia, France |
| `suppliers` | Trading companies | Cargill, Viterra |
| `shipment_period` | Delivery window | Jan 15-31, 2025 |

## Alert Configuration

### Default Alerts

| Alert | Trigger | Channels |
|-------|---------|----------|
| `egypt_tender` | Egypt + volume ≥ 50K MT | Email, Slack |
| `algeria_tender` | Algeria + volume ≥ 200K MT | Email |
| `large_tender` | Any + volume ≥ 500K MT | Email, Slack, SMS |
| `all_tenders` | All tenders | Email |

### Custom Alert Configuration

```python
from src.agents.collectors.tenders.alert_system import (
    TenderAlertManager,
    AlertConfig,
)

# Custom alert
custom_alert = AlertConfig(
    name="saudi_alert",
    description="Saudi Arabia wheat tenders",
    country_codes=["SA"],
    volume_threshold_mt=100000,
    notify_email=True,
    notify_slack=True,
    email_recipients=["analyst@company.com"],
)

manager = TenderAlertManager(configs=[custom_alert])
```

## Required External APIs/Subscriptions

### Required Now

| Service | Purpose | Status |
|---------|---------|--------|
| None | Basic scraping works without APIs | ✅ Ready |

### Recommended (For Alerts)

| Service | Purpose | Registration |
|---------|---------|--------------|
| SendGrid | Email alerts | https://sendgrid.com/ |
| Slack Webhooks | Slack notifications | https://api.slack.com/messaging/webhooks |
| Twilio | SMS alerts | https://www.twilio.com/ |

### Optional (For Enhanced Parsing)

| Service | Purpose | Registration |
|---------|---------|--------------|
| Google Translate API | Arabic/French translation | https://cloud.google.com/translate |

## Limitations

1. **Egypt Data**: No direct government source - relies on news monitoring
2. **Language**: Arabic (SAGO) and French (OAIC) content may need translation
3. **Rate Limiting**: Scrapers respect rate limits to avoid being blocked
4. **Premium Sources**: Reuters/Bloomberg require expensive subscriptions

## Future Enhancements

1. **NLP Enhancement**: Use named entity recognition for better extraction
2. **Price Analytics**: Track tender prices vs futures, calculate basis
3. **Volume Forecasting**: Predict tender volumes based on seasonality
4. **Origin Analytics**: Track origin market share trends
5. **Direct API Integration**: Partner with data providers for API access

## Testing

```bash
# Run unit tests
pytest tests/test_wheat_tender_collector.py -v

# Run with coverage
pytest tests/test_wheat_tender_collector.py --cov=src/agents/collectors/tenders
```

## Files

```
src/agents/collectors/tenders/
├── __init__.py                  # Package exports
├── wheat_tender_collector.py    # Main collector and scrapers
└── alert_system.py              # Notification system

scripts/
└── monitor_wheat_tenders.py     # Monitoring script

database/
└── 007_wheat_tenders.sql        # Database migration

tests/
└── test_wheat_tender_collector.py  # Unit tests
```

## Support

For issues or questions, check:
- Source code comments
- Test file examples
- Database schema documentation

---

*Last updated: January 2026*
