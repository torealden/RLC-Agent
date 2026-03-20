# Part 5: Adding New Data Sources

[← Back to Table of Contents](00_COVER_AND_TOC.md) | [← Previous: Working with Power BI](04_POWER_BI.md)

---

## 5.1 Planning a New Collector

Before writing code, thoroughly plan your new data source.

### Discovery Checklist

| Question | Why It Matters |
|----------|----------------|
| What is the data source URL/API? | Need to know where to fetch data |
| Is authentication required? | Determines if API key needed |
| What is the data format? | JSON, CSV, XML, PDF? |
| How often is data released? | Determines collection schedule |
| What is the data structure? | Maps to database schema |
| Is there rate limiting? | Affects collection timing |
| Is historical data available? | May need backfill strategy |

### Example: Planning a New Source

Let's say we want to add **Argentina Ministry of Agriculture** export data.

**Research findings:**

| Aspect | Details |
|--------|---------|
| Source | MagyP (Ministerio de Agricultura) |
| URL | `https://www.magyp.gob.ar/api/v1/exports` |
| Auth | API key required (free registration) |
| Format | JSON |
| Frequency | Weekly (Thursday) |
| Data | Grain exports by port and destination |
| Rate limit | 100 requests/hour |
| Historical | Last 2 years available |

### Mapping to Schema

Plan how the data maps to our medallion architecture:

```
API Response                    Bronze Table                Silver Table
─────────────────────────────────────────────────────────────────────────
{                               bronze.magyp_export_raw     silver.trade_flow
  "date": "2025-03-10",         → export_date               → observation_time
  "commodity": "SOJA",          → commodity_code            → (via series_id)
  "port": "Rosario",            → port_name                 → location_code
  "destination": "China",       → destination               → destination
  "quantity_mt": 125000,        → quantity_raw              → value
  "vessel": "MV Pacific Star"   → vessel_name               → (metadata)
}
```

**Gold View Design:**
- `gold.argentina_soybean_exports_weekly` — Aggregated by week and destination
- `gold.argentina_export_pace` — Cumulative vs. last year

---

## 5.2 Writing a Collector

All collectors inherit from `BaseCollector`, which provides standard functionality.

### File Location

Create your collector in the appropriate directory:

```
src/agents/collectors/
├── us/                    # US government sources
├── south_america/         # South American sources
│   ├── argentina/
│   │   └── magyp_collector.py    # <-- New file here
│   └── brazil/
└── global/                # International sources
```

### Collector Template

```python
"""
MagyP Argentina Export Collector

Collects weekly grain export data from Argentina's Ministry of Agriculture.
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from collectors.base.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class MagypExportCollector(BaseCollector):
    """Collector for Argentina MagyP export data."""

    # Required class attributes
    SOURCE_NAME = "magyp"
    SOURCE_DISPLAY_NAME = "Argentina MagyP Exports"
    BASE_URL = "https://www.magyp.gob.ar/api/v1"

    def __init__(self, api_key: Optional[str] = None):
        """Initialize the collector.

        Args:
            api_key: MagyP API key. If not provided, reads from environment.
        """
        super().__init__()
        self.api_key = api_key or self._get_env_key("MAGYP_API_KEY")

    def collect(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Collect export data for the specified date range.

        Args:
            start_date: Beginning of date range
            end_date: End of date range

        Returns:
            Dictionary containing collected data and metadata
        """
        logger.info(f"Collecting MagyP data from {start_date} to {end_date}")

        # Build request
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "commodity": "all",
            "format": "json"
        }
        headers = {"Authorization": f"Bearer {self.api_key}"}

        # Make API request (BaseCollector handles retries, rate limits)
        response = self.make_request(
            url=f"{self.BASE_URL}/exports",
            params=params,
            headers=headers
        )

        # Parse response
        data = response.json()
        records = self._parse_records(data)

        # Log collection
        self.log_data_save(
            table="bronze.magyp_export_raw",
            record_count=len(records),
            date_range=(start_date, end_date)
        )

        # Return structured result
        return {
            "source": self.SOURCE_NAME,
            "collected_at": datetime.utcnow().isoformat(),
            "record_count": len(records),
            "records": records
        }

    def _parse_records(self, raw_data: Dict) -> List[Dict]:
        """Parse API response into standardized records.

        Args:
            raw_data: Raw API response

        Returns:
            List of parsed records ready for database insertion
        """
        records = []
        for item in raw_data.get("exports", []):
            records.append({
                "export_date": item["date"],
                "commodity_code": self._normalize_commodity(item["commodity"]),
                "port_name": item["port"],
                "destination_country": item["destination"],
                "quantity_mt": float(item["quantity_mt"]),
                "vessel_name": item.get("vessel"),
                "raw_json": item  # Preserve original for audit
            })
        return records

    def _normalize_commodity(self, raw_code: str) -> str:
        """Normalize commodity codes to standard format."""
        mapping = {
            "SOJA": "soybeans",
            "MAIZ": "corn",
            "TRIGO": "wheat",
            "GIRASOL": "sunflower"
        }
        return mapping.get(raw_code.upper(), raw_code.lower())


# Entry point for manual testing
if __name__ == "__main__":
    from datetime import timedelta

    collector = MagypExportCollector()
    result = collector.collect(
        start_date=datetime.now() - timedelta(days=7),
        end_date=datetime.now()
    )
    print(f"Collected {result['record_count']} records")
```

### Key BaseCollector Features

Your collector inherits these capabilities:

| Method | Purpose |
|--------|---------|
| `make_request()` | HTTP request with retry, backoff, rate limiting |
| `log_data_save()` | Log collection for verification |
| `log_data_update()` | Log updates to existing records |
| `_get_env_key()` | Read API key from environment |

### Registration

After creating the collector, register it in `scripts/collect.py`:

```python
# In COLLECTOR_MAP dictionary
COLLECTOR_MAP = {
    # ... existing collectors ...
    'magyp': 'MagypExportCollector',
}
```

---

## 5.3 Database Schema Updates

### Step 1: Create Bronze Table

Create a migration file in `database/migrations/`:

```sql
-- database/migrations/027_magyp_exports.sql

-- =============================================================================
-- MagyP Argentina Export Data - Bronze Schema
-- =============================================================================

-- Create Bronze table for raw export records
CREATE TABLE IF NOT EXISTS bronze.magyp_export_raw (
    id BIGSERIAL PRIMARY KEY,

    -- Natural key
    export_date DATE NOT NULL,
    commodity_code VARCHAR(50) NOT NULL,
    port_name VARCHAR(100) NOT NULL,
    destination_country VARCHAR(100) NOT NULL,

    -- Data fields
    quantity_mt DECIMAL(15, 2),
    vessel_name VARCHAR(200),

    -- Audit fields
    raw_json JSONB,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ingest_run_id INTEGER REFERENCES audit.ingest_run(id),

    -- Unique constraint for idempotent writes
    CONSTRAINT uq_magyp_export UNIQUE (
        export_date, commodity_code, port_name, destination_country
    )
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_magyp_export_date
    ON bronze.magyp_export_raw(export_date DESC);
CREATE INDEX IF NOT EXISTS idx_magyp_export_commodity
    ON bronze.magyp_export_raw(commodity_code);
CREATE INDEX IF NOT EXISTS idx_magyp_export_dest
    ON bronze.magyp_export_raw(destination_country);

COMMENT ON TABLE bronze.magyp_export_raw IS
    'Raw export data from Argentina Ministry of Agriculture (MagyP)';
```

### Step 2: Register Data Source

Add to `core.data_source`:

```sql
INSERT INTO core.data_source (code, name, url, frequency, description)
VALUES (
    'magyp',
    'Argentina MagyP',
    'https://www.magyp.gob.ar',
    'weekly',
    'Argentina Ministry of Agriculture grain export data'
)
ON CONFLICT (code) DO NOTHING;
```

### Step 3: Create Series Entries

For each time series you'll track:

```sql
INSERT INTO core.series (
    data_source_code, series_key, name,
    commodity_code, location_code, unit_id, frequency
)
VALUES
    ('magyp', 'exports.soybeans.weekly', 'Argentina Soybean Exports',
     'soybeans', 'AR', (SELECT id FROM core.unit WHERE code = 'mt'), 'weekly'),
    ('magyp', 'exports.corn.weekly', 'Argentina Corn Exports',
     'corn', 'AR', (SELECT id FROM core.unit WHERE code = 'mt'), 'weekly')
ON CONFLICT (data_source_code, series_key) DO NOTHING;
```

### Step 4: Create Silver Transformation

Either in the collector or as a separate ETL script:

```sql
-- Transform Bronze to Silver
INSERT INTO silver.trade_flow (
    series_id,
    observation_time,
    origin_location,
    destination_location,
    value,
    unit_code,
    ingest_run_id
)
SELECT
    s.id AS series_id,
    r.export_date AS observation_time,
    'AR' AS origin_location,
    r.destination_country AS destination_location,
    r.quantity_mt AS value,
    'mt' AS unit_code,
    r.ingest_run_id
FROM bronze.magyp_export_raw r
JOIN core.series s ON s.series_key = 'exports.' || r.commodity_code || '.weekly'
ON CONFLICT (series_id, observation_time, destination_location)
DO UPDATE SET
    value = EXCLUDED.value,
    ingest_run_id = EXCLUDED.ingest_run_id;
```

---

## 5.4 Creating Gold Views

### Weekly Aggregation View

```sql
-- database/views/09_argentina_gold_views.sql

CREATE OR REPLACE VIEW gold.argentina_soybean_exports_weekly AS
SELECT
    date_trunc('week', r.export_date)::DATE AS week_ending,
    r.destination_country,
    SUM(r.quantity_mt) AS quantity_mt,
    COUNT(DISTINCT r.vessel_name) AS vessel_count
FROM bronze.magyp_export_raw r
WHERE r.commodity_code = 'soybeans'
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

COMMENT ON VIEW gold.argentina_soybean_exports_weekly IS
    'Weekly Argentine soybean exports by destination';
```

### Marketing Year Pace View

```sql
CREATE OR REPLACE VIEW gold.argentina_export_pace AS
WITH current_my AS (
    -- Argentina MY: April-March
    SELECT
        CASE
            WHEN EXTRACT(MONTH FROM CURRENT_DATE) >= 4
            THEN EXTRACT(YEAR FROM CURRENT_DATE)
            ELSE EXTRACT(YEAR FROM CURRENT_DATE) - 1
        END AS my_start_year
),
cumulative AS (
    SELECT
        r.commodity_code,
        cmy.my_start_year || '/' || (cmy.my_start_year + 1) AS marketing_year,
        SUM(r.quantity_mt) AS cumulative_exports_mt,
        MIN(r.export_date) AS first_export,
        MAX(r.export_date) AS last_export
    FROM bronze.magyp_export_raw r
    CROSS JOIN current_my cmy
    WHERE r.export_date >= make_date(cmy.my_start_year::int, 4, 1)
    GROUP BY r.commodity_code, cmy.my_start_year
)
SELECT
    c.commodity_code,
    c.marketing_year,
    c.cumulative_exports_mt,
    c.first_export,
    c.last_export,
    -- Add comparison to same point last year (would need history)
    NULL::decimal AS last_year_mt,
    NULL::decimal AS yoy_change_pct
FROM cumulative c
ORDER BY c.commodity_code, c.marketing_year DESC;

COMMENT ON VIEW gold.argentina_export_pace IS
    'Argentine export cumulative totals by marketing year';
```

---

## 5.5 Testing and Validation

### Unit Testing Your Collector

Create a test file in `tests/collectors/`:

```python
# tests/collectors/test_magyp_collector.py

import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from src.agents.collectors.south_america.argentina.magyp_collector import (
    MagypExportCollector
)


class TestMagypCollector:
    """Tests for MagyP Export Collector."""

    def setup_method(self):
        """Set up test fixtures."""
        self.collector = MagypExportCollector(api_key="test_key")

    def test_normalize_commodity(self):
        """Test commodity code normalization."""
        assert self.collector._normalize_commodity("SOJA") == "soybeans"
        assert self.collector._normalize_commodity("MAIZ") == "corn"
        assert self.collector._normalize_commodity("UNKNOWN") == "unknown"

    @patch.object(MagypExportCollector, 'make_request')
    def test_collect_parses_response(self, mock_request):
        """Test that collect() properly parses API response."""
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "exports": [
                {
                    "date": "2025-03-10",
                    "commodity": "SOJA",
                    "port": "Rosario",
                    "destination": "China",
                    "quantity_mt": 125000
                }
            ]
        }
        mock_request.return_value = mock_response

        # Run collection
        result = self.collector.collect(
            start_date=datetime(2025, 3, 1),
            end_date=datetime(2025, 3, 10)
        )

        # Verify
        assert result["record_count"] == 1
        assert result["records"][0]["commodity_code"] == "soybeans"
        assert result["records"][0]["quantity_mt"] == 125000

    def test_collect_handles_empty_response(self):
        """Test behavior with no data."""
        # Implementation...
        pass
```

### Integration Testing

Test the full pipeline from collection to Gold views:

```bash
# 1. Run collector with test date range
python scripts/collect.py --collector magyp --start 2025-03-01 --end 2025-03-07

# 2. Verify Bronze data
psql -c "SELECT COUNT(*) FROM bronze.magyp_export_raw WHERE collected_at > NOW() - INTERVAL '1 hour'"

# 3. Verify Silver transformation
psql -c "SELECT COUNT(*) FROM silver.trade_flow WHERE series_id IN (SELECT id FROM core.series WHERE data_source_code = 'magyp')"

# 4. Verify Gold view
psql -c "SELECT * FROM gold.argentina_soybean_exports_weekly LIMIT 5"
```

### Validation Checklist

Before deploying a new collector:

- [ ] Collector runs without errors
- [ ] API rate limits are respected
- [ ] Bronze table has correct data types
- [ ] Unique constraint works (re-running doesn't create duplicates)
- [ ] Silver transformation produces expected rows
- [ ] Gold views return data
- [ ] Documentation updated
- [ ] Collection schedule configured
- [ ] Monitoring/alerts configured

### Adding to Schedule

Update `config/collection_schedule.json`:

```json
{
  "magyp": {
    "name": "Argentina MagyP Exports",
    "frequency": "weekly",
    "day": "thursday",
    "time": "14:00",
    "timezone": "America/Buenos_Aires",
    "enabled": true,
    "priority": 2
  }
}
```

---

## Summary: New Data Source Workflow

```
1. RESEARCH          2. PLAN               3. BUILD
   └─ API docs          └─ Schema mapping     └─ Collector class
   └─ Auth needs        └─ Bronze table       └─ Database migration
   └─ Data format       └─ Silver transform   └─ Gold views
   └─ Schedule          └─ Gold views         └─ Tests

4. TEST              5. DEPLOY             6. MONITOR
   └─ Unit tests        └─ Run migration      └─ Add to dashboard
   └─ Integration       └─ Register source    └─ Set up alerts
   └─ Validation        └─ Schedule           └─ Document
```

---

[← Previous: Working with Power BI](04_POWER_BI.md) | [Next: Working with the LLM →](06_LLM_INTEGRATION.md)
