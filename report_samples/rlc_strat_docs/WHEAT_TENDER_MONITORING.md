<<<<<<< HEAD
# Wheat Tender Market Monitoring

## Overview

The wheat tender market is a critical source of demand signals for global grain markets. Major importing countries regularly issue tenders to purchase wheat and other grains, and the results of these tenders influence global wheat prices and trade flows.

## Key Importing Countries & Agencies

### Primary Markets (Most Active)

| Country | Agency | Typical Volume | Frequency |
|---------|--------|----------------|-----------|
| **Egypt** | ~~GASC~~ → Mostakbal Misr | 50K-60K MT/tender | Every 10-12 days (Jun-Feb) |
| **Algeria** | OAIC | 400K-600K MT | Monthly |
| **Saudi Arabia** | SAGO | 500K+ MT | Periodic |
| **Iraq** | Grain Board of Iraq | Variable | Periodic |
| **Tunisia** | State Grains Agency | 50K-100K MT | Monthly |
| **Morocco** | ONICL | Variable | Periodic |

### Secondary Markets

| Country | Agency | Notes |
|---------|--------|-------|
| **Jordan** | MIT | Regular tenders for wheat & barley |
| **Bangladesh** | Directorate of Food | Growing importer |
| **Indonesia** | Bulog | Rice & wheat |
| **Philippines** | NFA | Wheat & rice |
| **Pakistan** | PASSCO | Periodic tenders |

## Egyptian Tender Market (Most Watched)

### Recent Change (December 2024)
As of December 2024, **Mostakbal Misr** (an Egyptian military agency) has taken over from GASC for strategic commodity imports. This includes:
- International buying tenders
- Direct purchases
- All commodities previously managed by GASC

### Tender Structure
- **Typical shipment size**: 50,000-60,000 MT per cargo
- **Multiple cargoes** often awarded in single tender
- **Payment terms**: Usually L/C (Letter of Credit)
- **Shipment window**: Typically 25-45 days from award

### Origins Commonly Accepted
- Black Sea (Russia, Ukraine, Romania)
- EU (France, Germany)
- North America (US, Canada)
- South America (Argentina)
- Australia

## Monitoring Approaches

### 1. News Service Monitoring (Recommended)

**Primary Sources:**
- **Reuters Agriculture Wire** - Most timely, detailed coverage
- **Bloomberg Commodities** - Real-time alerts available
- **Agricensus** (https://www.agricensus.com) - Detailed tender coverage
- **AgroChart** (https://www.agrochart.com) - News feed aggregation

**Implementation:**
```python
# Example: RSS/API monitoring
sources = [
    'https://www.agricensus.com/feed/tenders',
    'https://www.agrochart.com/en/news/',
]
```

### 2. Direct Agency Monitoring

Some agencies publish tender announcements directly:

| Agency | Website | Language |
|--------|---------|----------|
| GASC/Mostakbal Misr | N/A (announcements via media) | Arabic/English |
| OAIC | http://www.oaic.dz | French/Arabic |
| SAGO | https://www.sago.gov.sa | Arabic |

### 3. Third-Party Data Providers (Commercial)

| Provider | Coverage | API Available |
|----------|----------|---------------|
| **Reuters Eikon** | Comprehensive | Yes (paid) |
| **Bloomberg Terminal** | Comprehensive | Yes (paid) |
| **Commodity3** | Tender tracking | Limited |
| **CMNavigator** | Weekly summaries | No |

## Automated Monitoring Solution

### Architecture Options

#### Option A: Web Scraping + Alert System
```
[News Sites] --> [Scraper] --> [NLP Filter] --> [Alert Engine] --> [Database]
                                    |                  |
                               "wheat tender"    Email/Slack/SMS
                               keyword match
```

#### Option B: RSS Feed Aggregation
```
[RSS Feeds] --> [Feed Parser] --> [Keyword Filter] --> [Storage]
                                        |
                                  tender keywords
```

#### Option C: Email Subscription Processing
```
[Email Subscriptions] --> [IMAP Client] --> [Parser] --> [Database]
       |
  Reuters, Bloomberg
  trade publications
```

### Implementation Skeleton

```python
"""
Wheat Tender Monitor

Monitors news sources for wheat tender announcements and results.
"""

import re
from datetime import datetime
from dataclasses import dataclass
from typing import List, Optional
import feedparser
import requests
from bs4 import BeautifulSoup

@dataclass
class TenderResult:
    date: datetime
    country: str
    agency: str
    commodity: str
    volume_mt: Optional[float]
    price_usd_mt: Optional[float]
    origin: Optional[str]
    supplier: Optional[str]
    shipment_period: Optional[str]
    source_url: str

class WheatTenderMonitor:
    """Monitor wheat tender announcements and results"""

    KEYWORDS = [
        'wheat tender', 'tender wheat', 'gasc wheat', 'oaic wheat',
        'algeria wheat', 'egypt wheat', 'tunisia wheat', 'sago wheat',
        'awarded wheat', 'wheat purchase', 'wheat import',
        'milling wheat', 'feed wheat'
    ]

    NEWS_SOURCES = [
        {
            'name': 'Agricensus',
            'url': 'https://www.agricensus.com/Article/latest',
            'type': 'html'
        },
        {
            'name': 'AgroChart',
            'url': 'https://www.agrochart.com/en/news/',
            'type': 'html'
        },
    ]

    def __init__(self):
        self.session = requests.Session()
        self.results = []

    def scan_sources(self) -> List[dict]:
        """Scan all news sources for tender-related articles"""
        articles = []

        for source in self.NEWS_SOURCES:
            try:
                response = self.session.get(source['url'], timeout=30)
                if response.status_code == 200:
                    found = self._parse_source(response.text, source)
                    articles.extend(found)
            except Exception as e:
                print(f"Error scanning {source['name']}: {e}")

        return articles

    def _parse_source(self, html: str, source: dict) -> List[dict]:
        """Parse HTML for tender-related articles"""
        articles = []
        soup = BeautifulSoup(html, 'html.parser')

        # Look for article links/headlines
        for link in soup.find_all('a', href=True):
            text = link.get_text().lower()

            if any(kw in text for kw in self.KEYWORDS):
                articles.append({
                    'source': source['name'],
                    'headline': link.get_text(),
                    'url': link['href'],
                    'found_at': datetime.now().isoformat()
                })

        return articles

    def parse_tender_result(self, article_text: str) -> Optional[TenderResult]:
        """Extract tender details from article text using NLP/regex"""

        # Volume pattern: "60,000 mt", "50000 tonnes"
        volume_match = re.search(
            r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(mt|tonnes?|metric tons?)',
            article_text,
            re.IGNORECASE
        )

        # Price pattern: "$250/mt", "USD 280 per tonne"
        price_match = re.search(
            r'\$?\s*(\d{2,4}(?:\.\d{2})?)\s*(?:usd\s*)?(?:per\s*|/)\s*(?:mt|tonne)',
            article_text,
            re.IGNORECASE
        )

        # Origin patterns
        origins = []
        for origin in ['Russia', 'Ukraine', 'France', 'Argentina', 'US', 'Canada', 'Australia', 'Romania']:
            if origin.lower() in article_text.lower():
                origins.append(origin)

        # Country patterns
        country = None
        for c in ['Egypt', 'Algeria', 'Tunisia', 'Morocco', 'Saudi Arabia', 'Iraq', 'Jordan']:
            if c.lower() in article_text.lower():
                country = c
                break

        if volume_match or price_match or country:
            return TenderResult(
                date=datetime.now(),
                country=country or 'Unknown',
                agency='',
                commodity='wheat',
                volume_mt=float(volume_match.group(1).replace(',', '')) if volume_match else None,
                price_usd_mt=float(price_match.group(1)) if price_match else None,
                origin=', '.join(origins) if origins else None,
                supplier=None,
                shipment_period=None,
                source_url=''
            )

        return None
```

## Data Points to Capture

For each tender announcement/result:

| Field | Description | Example |
|-------|-------------|---------|
| `date` | Announcement/result date | 2024-12-10 |
| `country` | Importing country | Egypt |
| `agency` | Purchasing agency | Mostakbal Misr |
| `commodity` | Wheat type | Milling wheat |
| `volume_mt` | Volume purchased | 480,000 MT |
| `price_usd_mt` | Price per MT C&F | $275.50 |
| `origin` | Supplier origin(s) | Russia, France |
| `supplier` | Trading company | Cargill, Viterra |
| `shipment_period` | Delivery window | Jan 15-31, 2025 |
| `freight_rate` | If reported | $35/MT |
| `payment_terms` | L/C terms | 180-day L/C |

## Integration with RLC-Agent

### Database Table Schema

```sql
CREATE TABLE wheat_tenders (
    id SERIAL PRIMARY KEY,
    tender_date DATE NOT NULL,
    result_date DATE,
    country VARCHAR(50) NOT NULL,
    agency VARCHAR(100),
    commodity VARCHAR(50) DEFAULT 'wheat',
    wheat_type VARCHAR(50),  -- milling, feed, durum
    volume_mt DECIMAL(12,2),
    price_usd_mt DECIMAL(10,2),
    price_type VARCHAR(20),  -- FOB, C&F, CIF
    origin VARCHAR(100),
    supplier VARCHAR(100),
    shipment_start DATE,
    shipment_end DATE,
    source_url TEXT,
    source_name VARCHAR(100),
    raw_text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_tender_date (tender_date),
    INDEX idx_country (country),
    INDEX idx_origin (origin)
);
```

### Alert Configuration

```python
TENDER_ALERTS = {
    'egypt': {
        'volume_threshold': 100000,  # MT
        'notify': ['email', 'slack'],
    },
    'algeria': {
        'volume_threshold': 200000,
        'notify': ['email'],
    },
    'large_tender': {
        'volume_threshold': 500000,
        'notify': ['email', 'slack', 'sms'],
    },
}
```

## Future Enhancements

1. **NLP Entity Extraction**: Use named entity recognition to extract suppliers, trading companies, and specific terms
2. **Price Analytics**: Track tender prices vs. futures, calculate basis trends
3. **Volume Forecasting**: Build models to predict upcoming tender volumes based on seasonality
4. **Origin Analytics**: Track origin share trends in major importing countries
5. **API Integration**: Partner with data providers for direct API access to tender data

## Related Data Sources

- **USDA FAS Attaché Reports**: Sometimes report upcoming tender expectations
- **IGC Market Reports**: Include tender results in weekly summaries
- **National Statistics Agencies**: Annual import data to validate tender tracking

---

*Note: This is a framework for wheat tender monitoring. Implementation requires ongoing maintenance as news sources change and agency policies evolve.*

*Last updated: December 2025*
=======
# Wheat Tender Market Monitoring

## Overview

The wheat tender market is a critical source of demand signals for global grain markets. Major importing countries regularly issue tenders to purchase wheat and other grains, and the results of these tenders influence global wheat prices and trade flows.

## Key Importing Countries & Agencies

### Primary Markets (Most Active)

| Country | Agency | Typical Volume | Frequency |
|---------|--------|----------------|-----------|
| **Egypt** | ~~GASC~~ → Mostakbal Misr | 50K-60K MT/tender | Every 10-12 days (Jun-Feb) |
| **Algeria** | OAIC | 400K-600K MT | Monthly |
| **Saudi Arabia** | SAGO | 500K+ MT | Periodic |
| **Iraq** | Grain Board of Iraq | Variable | Periodic |
| **Tunisia** | State Grains Agency | 50K-100K MT | Monthly |
| **Morocco** | ONICL | Variable | Periodic |

### Secondary Markets

| Country | Agency | Notes |
|---------|--------|-------|
| **Jordan** | MIT | Regular tenders for wheat & barley |
| **Bangladesh** | Directorate of Food | Growing importer |
| **Indonesia** | Bulog | Rice & wheat |
| **Philippines** | NFA | Wheat & rice |
| **Pakistan** | PASSCO | Periodic tenders |

## Egyptian Tender Market (Most Watched)

### Recent Change (December 2024)
As of December 2024, **Mostakbal Misr** (an Egyptian military agency) has taken over from GASC for strategic commodity imports. This includes:
- International buying tenders
- Direct purchases
- All commodities previously managed by GASC

### Tender Structure
- **Typical shipment size**: 50,000-60,000 MT per cargo
- **Multiple cargoes** often awarded in single tender
- **Payment terms**: Usually L/C (Letter of Credit)
- **Shipment window**: Typically 25-45 days from award

### Origins Commonly Accepted
- Black Sea (Russia, Ukraine, Romania)
- EU (France, Germany)
- North America (US, Canada)
- South America (Argentina)
- Australia

## Monitoring Approaches

### 1. News Service Monitoring (Recommended)

**Primary Sources:**
- **Reuters Agriculture Wire** - Most timely, detailed coverage
- **Bloomberg Commodities** - Real-time alerts available
- **Agricensus** (https://www.agricensus.com) - Detailed tender coverage
- **AgroChart** (https://www.agrochart.com) - News feed aggregation

**Implementation:**
```python
# Example: RSS/API monitoring
sources = [
    'https://www.agricensus.com/feed/tenders',
    'https://www.agrochart.com/en/news/',
]
```

### 2. Direct Agency Monitoring

Some agencies publish tender announcements directly:

| Agency | Website | Language |
|--------|---------|----------|
| GASC/Mostakbal Misr | N/A (announcements via media) | Arabic/English |
| OAIC | http://www.oaic.dz | French/Arabic |
| SAGO | https://www.sago.gov.sa | Arabic |

### 3. Third-Party Data Providers (Commercial)

| Provider | Coverage | API Available |
|----------|----------|---------------|
| **Reuters Eikon** | Comprehensive | Yes (paid) |
| **Bloomberg Terminal** | Comprehensive | Yes (paid) |
| **Commodity3** | Tender tracking | Limited |
| **CMNavigator** | Weekly summaries | No |

## Automated Monitoring Solution

### Architecture Options

#### Option A: Web Scraping + Alert System
```
[News Sites] --> [Scraper] --> [NLP Filter] --> [Alert Engine] --> [Database]
                                    |                  |
                               "wheat tender"    Email/Slack/SMS
                               keyword match
```

#### Option B: RSS Feed Aggregation
```
[RSS Feeds] --> [Feed Parser] --> [Keyword Filter] --> [Storage]
                                        |
                                  tender keywords
```

#### Option C: Email Subscription Processing
```
[Email Subscriptions] --> [IMAP Client] --> [Parser] --> [Database]
       |
  Reuters, Bloomberg
  trade publications
```

### Implementation Skeleton

```python
"""
Wheat Tender Monitor

Monitors news sources for wheat tender announcements and results.
"""

import re
from datetime import datetime
from dataclasses import dataclass
from typing import List, Optional
import feedparser
import requests
from bs4 import BeautifulSoup

@dataclass
class TenderResult:
    date: datetime
    country: str
    agency: str
    commodity: str
    volume_mt: Optional[float]
    price_usd_mt: Optional[float]
    origin: Optional[str]
    supplier: Optional[str]
    shipment_period: Optional[str]
    source_url: str

class WheatTenderMonitor:
    """Monitor wheat tender announcements and results"""

    KEYWORDS = [
        'wheat tender', 'tender wheat', 'gasc wheat', 'oaic wheat',
        'algeria wheat', 'egypt wheat', 'tunisia wheat', 'sago wheat',
        'awarded wheat', 'wheat purchase', 'wheat import',
        'milling wheat', 'feed wheat'
    ]

    NEWS_SOURCES = [
        {
            'name': 'Agricensus',
            'url': 'https://www.agricensus.com/Article/latest',
            'type': 'html'
        },
        {
            'name': 'AgroChart',
            'url': 'https://www.agrochart.com/en/news/',
            'type': 'html'
        },
    ]

    def __init__(self):
        self.session = requests.Session()
        self.results = []

    def scan_sources(self) -> List[dict]:
        """Scan all news sources for tender-related articles"""
        articles = []

        for source in self.NEWS_SOURCES:
            try:
                response = self.session.get(source['url'], timeout=30)
                if response.status_code == 200:
                    found = self._parse_source(response.text, source)
                    articles.extend(found)
            except Exception as e:
                print(f"Error scanning {source['name']}: {e}")

        return articles

    def _parse_source(self, html: str, source: dict) -> List[dict]:
        """Parse HTML for tender-related articles"""
        articles = []
        soup = BeautifulSoup(html, 'html.parser')

        # Look for article links/headlines
        for link in soup.find_all('a', href=True):
            text = link.get_text().lower()

            if any(kw in text for kw in self.KEYWORDS):
                articles.append({
                    'source': source['name'],
                    'headline': link.get_text(),
                    'url': link['href'],
                    'found_at': datetime.now().isoformat()
                })

        return articles

    def parse_tender_result(self, article_text: str) -> Optional[TenderResult]:
        """Extract tender details from article text using NLP/regex"""

        # Volume pattern: "60,000 mt", "50000 tonnes"
        volume_match = re.search(
            r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(mt|tonnes?|metric tons?)',
            article_text,
            re.IGNORECASE
        )

        # Price pattern: "$250/mt", "USD 280 per tonne"
        price_match = re.search(
            r'\$?\s*(\d{2,4}(?:\.\d{2})?)\s*(?:usd\s*)?(?:per\s*|/)\s*(?:mt|tonne)',
            article_text,
            re.IGNORECASE
        )

        # Origin patterns
        origins = []
        for origin in ['Russia', 'Ukraine', 'France', 'Argentina', 'US', 'Canada', 'Australia', 'Romania']:
            if origin.lower() in article_text.lower():
                origins.append(origin)

        # Country patterns
        country = None
        for c in ['Egypt', 'Algeria', 'Tunisia', 'Morocco', 'Saudi Arabia', 'Iraq', 'Jordan']:
            if c.lower() in article_text.lower():
                country = c
                break

        if volume_match or price_match or country:
            return TenderResult(
                date=datetime.now(),
                country=country or 'Unknown',
                agency='',
                commodity='wheat',
                volume_mt=float(volume_match.group(1).replace(',', '')) if volume_match else None,
                price_usd_mt=float(price_match.group(1)) if price_match else None,
                origin=', '.join(origins) if origins else None,
                supplier=None,
                shipment_period=None,
                source_url=''
            )

        return None
```

## Data Points to Capture

For each tender announcement/result:

| Field | Description | Example |
|-------|-------------|---------|
| `date` | Announcement/result date | 2024-12-10 |
| `country` | Importing country | Egypt |
| `agency` | Purchasing agency | Mostakbal Misr |
| `commodity` | Wheat type | Milling wheat |
| `volume_mt` | Volume purchased | 480,000 MT |
| `price_usd_mt` | Price per MT C&F | $275.50 |
| `origin` | Supplier origin(s) | Russia, France |
| `supplier` | Trading company | Cargill, Viterra |
| `shipment_period` | Delivery window | Jan 15-31, 2025 |
| `freight_rate` | If reported | $35/MT |
| `payment_terms` | L/C terms | 180-day L/C |

## Integration with RLC-Agent

### Database Table Schema

```sql
CREATE TABLE wheat_tenders (
    id SERIAL PRIMARY KEY,
    tender_date DATE NOT NULL,
    result_date DATE,
    country VARCHAR(50) NOT NULL,
    agency VARCHAR(100),
    commodity VARCHAR(50) DEFAULT 'wheat',
    wheat_type VARCHAR(50),  -- milling, feed, durum
    volume_mt DECIMAL(12,2),
    price_usd_mt DECIMAL(10,2),
    price_type VARCHAR(20),  -- FOB, C&F, CIF
    origin VARCHAR(100),
    supplier VARCHAR(100),
    shipment_start DATE,
    shipment_end DATE,
    source_url TEXT,
    source_name VARCHAR(100),
    raw_text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_tender_date (tender_date),
    INDEX idx_country (country),
    INDEX idx_origin (origin)
);
```

### Alert Configuration

```python
TENDER_ALERTS = {
    'egypt': {
        'volume_threshold': 100000,  # MT
        'notify': ['email', 'slack'],
    },
    'algeria': {
        'volume_threshold': 200000,
        'notify': ['email'],
    },
    'large_tender': {
        'volume_threshold': 500000,
        'notify': ['email', 'slack', 'sms'],
    },
}
```

## Future Enhancements

1. **NLP Entity Extraction**: Use named entity recognition to extract suppliers, trading companies, and specific terms
2. **Price Analytics**: Track tender prices vs. futures, calculate basis trends
3. **Volume Forecasting**: Build models to predict upcoming tender volumes based on seasonality
4. **Origin Analytics**: Track origin share trends in major importing countries
5. **API Integration**: Partner with data providers for direct API access to tender data

## Related Data Sources

- **USDA FAS Attaché Reports**: Sometimes report upcoming tender expectations
- **IGC Market Reports**: Include tender results in weekly summaries
- **National Statistics Agencies**: Annual import data to validate tender tracking

---

*Note: This is a framework for wheat tender monitoring. Implementation requires ongoing maintenance as news sources change and agency policies evolve.*

*Last updated: December 2025*
>>>>>>> b78b7f1580b097e459ea6b3c51e6e97fe5047b7d
