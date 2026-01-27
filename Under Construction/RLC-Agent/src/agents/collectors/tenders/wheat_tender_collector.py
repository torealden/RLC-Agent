"""
Wheat Tender Collector

Monitors international wheat tender announcements and results from:
- Government agencies (OAIC/Algeria, SAGO/Saudi Arabia, etc.)
- News aggregators (Agricensus, AgroChart)

Data collection priority:
1. Direct government agency sources (preferred)
2. Free news aggregators
3. Commercial data providers (last resort)

Note: Egypt (Mostakbal Misr) does not publish tenders directly -
must be monitored via news sources.

Data Sources:
- OAIC (Algeria): http://www.oaic.dz - French/Arabic
- SAGO (Saudi Arabia): https://www.sago.gov.sa - Arabic
- Agricensus: https://www.agricensus.com - English
- AgroChart: https://www.agrochart.com - English
"""

import logging
import re
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
from urllib.parse import urljoin, urlparse
import json

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

try:
    import feedparser
    FEEDPARSER_AVAILABLE = True
except ImportError:
    FEEDPARSER_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

# Import base classes
from src.agents.base.base_collector import (
    BaseCollector,
    CollectorConfig,
    CollectorResult,
    DataFrequency,
    AuthType
)

logger = logging.getLogger(__name__)


# =============================================================================
# DATA CLASSES
# =============================================================================

class TenderType(Enum):
    """Type of tender notification"""
    ANNOUNCEMENT = "announcement"
    RESULT = "result"
    AMENDMENT = "amendment"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class SourceType(Enum):
    """Type of data source"""
    GOVERNMENT = "government"          # Direct government agency
    NEWS_AGGREGATOR = "news_aggregator"  # News sites
    COMMERCIAL = "commercial"           # Paid data providers


@dataclass
class TenderAnnouncement:
    """Wheat tender announcement"""
    source: str
    source_url: str
    headline: str
    captured_at: datetime

    tender_type: TenderType = TenderType.UNKNOWN
    country: Optional[str] = None
    agency: Optional[str] = None
    deadline: Optional[datetime] = None

    article_text: Optional[str] = None
    article_date: Optional[datetime] = None


@dataclass
class TenderResult:
    """Parsed wheat tender result"""
    # Source info
    source: str
    source_url: str
    captured_at: datetime

    # Tender identification
    tender_type: TenderType = TenderType.RESULT
    tender_date: Optional[date] = None

    # Buyer
    country: Optional[str] = None
    country_code: Optional[str] = None
    agency: Optional[str] = None
    agency_code: Optional[str] = None

    # Commodity
    commodity: str = "wheat"
    wheat_type: Optional[str] = None  # milling, feed, durum

    # Volume
    volume_mt: Optional[float] = None
    num_cargoes: Optional[int] = None
    cargo_size_mt: Optional[float] = None

    # Price
    price_usd_mt: Optional[float] = None
    price_type: Optional[str] = None  # FOB, C&F, CIF
    freight_usd_mt: Optional[float] = None

    # Origins and suppliers
    origins: List[str] = field(default_factory=list)
    suppliers: List[str] = field(default_factory=list)

    # Shipment
    shipment_start: Optional[date] = None
    shipment_end: Optional[date] = None
    shipment_port: Optional[str] = None

    # Payment
    payment_terms: Optional[str] = None
    lc_days: Optional[int] = None

    # Raw data
    raw_text: Optional[str] = None
    headline: Optional[str] = None

    # Quality
    parse_confidence: float = 0.0


@dataclass
class WheatTenderConfig(CollectorConfig):
    """Configuration for Wheat Tender Collector"""
    source_name: str = "Wheat Tender Monitor"
    source_url: str = "https://www.agricensus.com"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.DAILY

    # Source priorities (lower = higher priority)
    source_priorities: Dict[str, int] = field(default_factory=lambda: {
        'OAIC': 1,           # Algeria - government
        'SAGO': 1,           # Saudi Arabia - government
        'AGRICENSUS': 2,     # News aggregator
        'AGROCHART': 2,      # News aggregator
        'USDA_FAS': 3,       # Supplementary
    })

    # Countries to monitor
    countries: List[str] = field(default_factory=lambda: [
        'Egypt', 'Algeria', 'Saudi Arabia', 'Iraq', 'Tunisia',
        'Morocco', 'Jordan', 'Bangladesh', 'Indonesia',
        'Philippines', 'Pakistan'
    ])

    # Keywords for tender detection
    tender_keywords: List[str] = field(default_factory=lambda: [
        'wheat tender', 'tender wheat', 'gasc wheat', 'oaic wheat',
        'mostakbal misr', 'egypt wheat', 'algeria wheat', 'sago wheat',
        'awarded wheat', 'wheat purchase', 'wheat import',
        'milling wheat', 'feed wheat', 'durum wheat',
        'tender result', 'grain tender', 'wheat bought',
        'appel d\'offres blé', 'مناقصة القمح'  # French and Arabic
    ])

    # Scrape settings
    scrape_interval_minutes: int = 60
    max_articles_per_source: int = 50

    # Translation settings (for Arabic/French sources)
    # NOTE: Requires Google Translate API key if enabled
    enable_translation: bool = False
    google_translate_api_key: Optional[str] = None


# =============================================================================
# TENDER PARSING ENGINE
# =============================================================================

class TenderParser:
    """
    Parses tender details from article text using regex and NLP patterns.

    Extracts:
    - Volume (MT)
    - Price (USD/MT)
    - Origins
    - Suppliers
    - Shipment dates
    - Payment terms
    """

    # Country mappings
    COUNTRY_MAPPINGS = {
        'egypt': ('EG', 'Egypt'),
        'egyptian': ('EG', 'Egypt'),
        'algeria': ('DZ', 'Algeria'),
        'algerian': ('DZ', 'Algeria'),
        'saudi arabia': ('SA', 'Saudi Arabia'),
        'saudi': ('SA', 'Saudi Arabia'),
        'iraq': ('IQ', 'Iraq'),
        'iraqi': ('IQ', 'Iraq'),
        'tunisia': ('TN', 'Tunisia'),
        'tunisian': ('TN', 'Tunisia'),
        'morocco': ('MA', 'Morocco'),
        'moroccan': ('MA', 'Morocco'),
        'jordan': ('JO', 'Jordan'),
        'jordanian': ('JO', 'Jordan'),
        'bangladesh': ('BD', 'Bangladesh'),
        'indonesian': ('ID', 'Indonesia'),
        'indonesia': ('ID', 'Indonesia'),
        'philippines': ('PH', 'Philippines'),
        'philippine': ('PH', 'Philippines'),
        'pakistan': ('PK', 'Pakistan'),
        'pakistani': ('PK', 'Pakistan'),
    }

    # Agency mappings
    AGENCY_MAPPINGS = {
        'mostakbal misr': ('MOSTAKBAL_MISR', 'Mostakbal Misr', 'EG'),
        'gasc': ('GASC', 'GASC', 'EG'),
        'oaic': ('OAIC', 'OAIC', 'DZ'),
        'sago': ('SAGO', 'SAGO', 'SA'),
        'grain board of iraq': ('GBI', 'Grain Board of Iraq', 'IQ'),
        'gbi': ('GBI', 'Grain Board of Iraq', 'IQ'),
        'onicl': ('ONICL', 'ONICL', 'MA'),
        'mit jordan': ('MIT_JORDAN', 'MIT Jordan', 'JO'),
        'bulog': ('BULOG', 'BULOG', 'ID'),
        'nfa': ('NFA_PH', 'NFA', 'PH'),
        'passco': ('PASSCO', 'PASSCO', 'PK'),
    }

    # Origin country patterns
    ORIGIN_COUNTRIES = [
        'Russia', 'Ukraine', 'France', 'Germany', 'Romania', 'Bulgaria',
        'United States', 'US', 'USA', 'Canada', 'Australia', 'Argentina',
        'Kazakhstan', 'Poland', 'Lithuania', 'Latvia', 'Estonia',
        'Black Sea', 'EU', 'European'
    ]

    # Major trading companies
    TRADING_COMPANIES = [
        'Cargill', 'ADM', 'Bunge', 'Louis Dreyfus', 'LDC', 'COFCO',
        'Glencore', 'Viterra', 'Olam', 'Nidera', 'Toepfer', 'Aston',
        'Kernel', 'Ameropa', 'Soufflet', 'Casillo', 'Miro', 'Holbud',
        'Solaris', 'CHS', 'Gavilon', 'Engelhart', 'Noble', 'Trafigura'
    ]

    # Regex patterns
    VOLUME_PATTERNS = [
        # "480,000 mt", "60,000 tonnes", "50000 metric tons"
        r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:000\s*)?(mt|mmt|tonnes?|metric\s*tons?)',
        # "480k mt", "60K tonnes"
        r'(\d+(?:\.\d+)?)\s*[kK]\s*(mt|tonnes?)',
        # "around 500,000 tonnes"
        r'(?:around|about|approximately|some|circa)\s*(\d{1,3}(?:,\d{3})*)\s*(mt|tonnes?)',
    ]

    PRICE_PATTERNS = [
        # "$275.50/mt", "USD 280/tonne"
        r'(?:\$|USD\s*|US\$\s*)(\d{2,4}(?:\.\d{1,2})?)\s*(?:per\s*|/)\s*(?:mt|tonne)',
        # "275.50 $/mt", "280 USD/t"
        r'(\d{2,4}(?:\.\d{1,2})?)\s*(?:\$|USD|US\$)\s*(?:per\s*|/)\s*(?:mt|t(?:onne)?)',
        # "at 275.50/mt C&F"
        r'at\s*(\d{2,4}(?:\.\d{1,2})?)\s*(?:/mt|/tonne)',
    ]

    PRICE_TYPE_PATTERNS = [
        (r'\b(C&F|C\+F|CFR|CNF)\b', 'C&F'),
        (r'\b(CIF)\b', 'CIF'),
        (r'\b(FOB)\b', 'FOB'),
        (r'\b(FAS)\b', 'FAS'),
    ]

    SHIPMENT_PATTERNS = [
        # "Jan 15-31" or "January 15-31, 2025"
        r'(?:shipment|delivery|ship)\s*(?:for|in|during)?\s*([A-Za-z]{3,9})\s*(\d{1,2})\s*[-–to]+\s*(\d{1,2})(?:,?\s*(\d{4}))?',
        # "late January", "early February"
        r'(early|mid|late)\s+(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)',
        # "Q1 2025", "Q2"
        r'Q([1-4])\s*(?:20)?(\d{2})?',
    ]

    WHEAT_TYPE_PATTERNS = [
        (r'\b(milling\s+wheat|bread\s+wheat|soft\s+wheat)\b', 'milling'),
        (r'\b(feed\s+wheat|fodder\s+wheat)\b', 'feed'),
        (r'\b(durum|semolina)\b', 'durum'),
        (r'\b(hard\s+(?:red\s+)?wheat|hrw|hrs)\b', 'hard'),
        (r'\b(soft\s+red\s+wheat|srw)\b', 'soft_red'),
    ]

    def __init__(self):
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

    def parse(self, text: str, headline: str = "", source: str = "",
              source_url: str = "") -> TenderResult:
        """
        Parse tender details from article text.

        Args:
            text: Full article text
            headline: Article headline
            source: Source name
            source_url: Source URL

        Returns:
            TenderResult with extracted data
        """
        result = TenderResult(
            source=source,
            source_url=source_url,
            captured_at=datetime.now(),
            raw_text=text,
            headline=headline,
        )

        # Combine headline and text for parsing
        full_text = f"{headline} {text}".lower()
        original_text = f"{headline} {text}"

        confidence_scores = []

        # Parse country and agency
        country_info = self._extract_country(full_text)
        if country_info:
            result.country_code, result.country = country_info
            confidence_scores.append(0.2)

        agency_info = self._extract_agency(full_text)
        if agency_info:
            result.agency_code, result.agency, inferred_country = agency_info
            if not result.country_code:
                result.country_code = inferred_country
                result.country = self.COUNTRY_MAPPINGS.get(
                    inferred_country.lower(), (None, inferred_country)
                )[1]
            confidence_scores.append(0.15)

        # Parse volume
        volume = self._extract_volume(full_text)
        if volume:
            result.volume_mt = volume
            confidence_scores.append(0.25)

        # Parse price
        price_info = self._extract_price(original_text)
        if price_info:
            result.price_usd_mt, result.price_type = price_info
            confidence_scores.append(0.2)

        # Parse origins
        origins = self._extract_origins(original_text)
        if origins:
            result.origins = origins
            confidence_scores.append(0.1)

        # Parse suppliers
        suppliers = self._extract_suppliers(original_text)
        if suppliers:
            result.suppliers = suppliers
            confidence_scores.append(0.05)

        # Parse wheat type
        wheat_type = self._extract_wheat_type(full_text)
        if wheat_type:
            result.wheat_type = wheat_type
            confidence_scores.append(0.05)

        # Parse shipment dates
        shipment = self._extract_shipment(full_text)
        if shipment:
            result.shipment_start, result.shipment_end = shipment

        # Determine tender type
        result.tender_type = self._determine_tender_type(full_text)

        # Calculate overall confidence
        result.parse_confidence = sum(confidence_scores)

        return result

    def _extract_country(self, text: str) -> Optional[Tuple[str, str]]:
        """Extract importing country from text"""
        for pattern, (code, name) in self.COUNTRY_MAPPINGS.items():
            if pattern in text:
                return (code, name)
        return None

    def _extract_agency(self, text: str) -> Optional[Tuple[str, str, str]]:
        """Extract purchasing agency from text"""
        for pattern, (code, name, country) in self.AGENCY_MAPPINGS.items():
            if pattern in text:
                return (code, name, country)
        return None

    def _extract_volume(self, text: str) -> Optional[float]:
        """Extract volume in metric tons"""
        for pattern in self.VOLUME_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    value_str = match.group(1).replace(',', '')
                    value = float(value_str)
                    unit = match.group(2).lower()

                    # Convert to MT
                    if 'k' in text[match.start():match.end()].lower():
                        value *= 1000
                    if 'mmt' in unit or 'million' in unit:
                        value *= 1_000_000
                    elif '000' in text[match.start()-5:match.start()]:
                        value *= 1000

                    # Sanity check - tenders are typically 10K - 1M MT
                    if 1000 <= value <= 2_000_000:
                        return value
                except (ValueError, IndexError):
                    continue
        return None

    def _extract_price(self, text: str) -> Optional[Tuple[float, str]]:
        """Extract price in USD/MT and price type (FOB, C&F, CIF)"""
        price = None
        price_type = None

        # Find price
        for pattern in self.PRICE_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    price = float(match.group(1))
                    # Sanity check - wheat prices typically $150-$500/MT
                    if 100 <= price <= 600:
                        break
                    else:
                        price = None
                except (ValueError, IndexError):
                    continue

        # Find price type
        for pattern, ptype in self.PRICE_TYPE_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                price_type = ptype
                break

        if price:
            return (price, price_type)
        return None

    def _extract_origins(self, text: str) -> List[str]:
        """Extract origin countries"""
        origins = []
        for origin in self.ORIGIN_COUNTRIES:
            # Use word boundaries to avoid partial matches
            if re.search(rf'\b{re.escape(origin)}\b', text, re.IGNORECASE):
                origins.append(origin)
        return origins

    def _extract_suppliers(self, text: str) -> List[str]:
        """Extract trading company names"""
        suppliers = []
        for company in self.TRADING_COMPANIES:
            if re.search(rf'\b{re.escape(company)}\b', text, re.IGNORECASE):
                suppliers.append(company)
        return suppliers

    def _extract_wheat_type(self, text: str) -> Optional[str]:
        """Extract wheat type/class"""
        for pattern, wheat_type in self.WHEAT_TYPE_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                return wheat_type
        return None

    def _extract_shipment(self, text: str) -> Optional[Tuple[date, date]]:
        """Extract shipment period dates"""
        # This is a simplified implementation - would need enhancement
        # for full date range parsing
        for pattern in self.SHIPMENT_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                # Would parse the match groups into actual dates
                # For now, return None and implement later
                pass
        return None

    def _determine_tender_type(self, text: str) -> TenderType:
        """Determine if this is an announcement, result, etc."""
        result_keywords = [
            'awarded', 'bought', 'purchased', 'secured',
            'won the tender', 'result', 'booked'
        ]
        announcement_keywords = [
            'seeking', 'looking to buy', 'issued tender',
            'invitation', 'deadline', 'to purchase'
        ]
        cancelled_keywords = [
            'cancelled', 'rejected', 'no award', 'void'
        ]

        for kw in cancelled_keywords:
            if kw in text:
                return TenderType.CANCELLED

        for kw in result_keywords:
            if kw in text:
                return TenderType.RESULT

        for kw in announcement_keywords:
            if kw in text:
                return TenderType.ANNOUNCEMENT

        return TenderType.UNKNOWN


# =============================================================================
# NEWS SOURCE SCRAPERS
# =============================================================================

class BaseNewsScraper:
    """Base class for news source scrapers"""

    def __init__(self, config: WheatTenderConfig):
        self.config = config
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry logic"""
        session = requests.Session()

        retry = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session.headers.update({
            'User-Agent': 'RLC-TenderMonitor/1.0 (Agricultural Research)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,fr;q=0.8,ar;q=0.7',
        })

        return session

    def fetch_articles(self) -> List[TenderAnnouncement]:
        """Fetch tender-related articles. Override in subclass."""
        raise NotImplementedError

    def _matches_keywords(self, text: str) -> bool:
        """Check if text contains tender keywords"""
        text_lower = text.lower()
        return any(kw.lower() in text_lower for kw in self.config.tender_keywords)


class AgricensusScraper(BaseNewsScraper):
    """
    Scraper for Agricensus (https://www.agricensus.com)

    Primary source for tender news coverage.
    Requires web scraping as no public API is available.

    NOTE: Agricensus has premium content behind paywall.
    Free tier provides headlines and article summaries.
    """

    SOURCE_NAME = "Agricensus"
    BASE_URL = "https://www.agricensus.com"
    NEWS_URL = "https://www.agricensus.com/Article/latest"

    def fetch_articles(self) -> List[TenderAnnouncement]:
        """Fetch tender-related articles from Agricensus"""
        articles = []

        try:
            response = self.session.get(
                self.NEWS_URL,
                timeout=30
            )

            if response.status_code != 200:
                self.logger.warning(
                    f"Agricensus returned status {response.status_code}"
                )
                return articles

            if not BS4_AVAILABLE:
                self.logger.error("BeautifulSoup not installed")
                return articles

            soup = BeautifulSoup(response.text, 'html.parser')

            # Find article links - selector may need adjustment
            # based on actual site structure
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                text = link.get_text(strip=True)

                # Filter for article links
                if '/Article/' not in href:
                    continue

                # Check for tender keywords
                if not self._matches_keywords(text):
                    continue

                full_url = urljoin(self.BASE_URL, href)

                articles.append(TenderAnnouncement(
                    source=self.SOURCE_NAME,
                    source_url=full_url,
                    headline=text,
                    captured_at=datetime.now(),
                ))

            self.logger.info(f"Found {len(articles)} tender articles on Agricensus")

        except Exception as e:
            self.logger.error(f"Error scraping Agricensus: {e}")

        return articles[:self.config.max_articles_per_source]


class AgroChartScraper(BaseNewsScraper):
    """
    Scraper for AgroChart (https://www.agrochart.com)

    Aggregates agricultural news from multiple sources.
    Free access to news feed.
    """

    SOURCE_NAME = "AgroChart"
    BASE_URL = "https://www.agrochart.com"
    NEWS_URL = "https://www.agrochart.com/en/news/"

    def fetch_articles(self) -> List[TenderAnnouncement]:
        """Fetch tender-related articles from AgroChart"""
        articles = []

        try:
            response = self.session.get(
                self.NEWS_URL,
                timeout=30
            )

            if response.status_code != 200:
                self.logger.warning(
                    f"AgroChart returned status {response.status_code}"
                )
                return articles

            if not BS4_AVAILABLE:
                self.logger.error("BeautifulSoup not installed")
                return articles

            soup = BeautifulSoup(response.text, 'html.parser')

            # Find news items - selector may need adjustment
            for item in soup.find_all(['a', 'div'], class_=re.compile(r'news|article', re.I)):
                text = item.get_text(strip=True)
                href = item.get('href', '')

                if not href and item.name == 'div':
                    link = item.find('a', href=True)
                    if link:
                        href = link.get('href', '')

                if not self._matches_keywords(text):
                    continue

                full_url = urljoin(self.BASE_URL, href) if href else self.NEWS_URL

                articles.append(TenderAnnouncement(
                    source=self.SOURCE_NAME,
                    source_url=full_url,
                    headline=text[:500],  # Truncate long text
                    captured_at=datetime.now(),
                ))

            self.logger.info(f"Found {len(articles)} tender articles on AgroChart")

        except Exception as e:
            self.logger.error(f"Error scraping AgroChart: {e}")

        return articles[:self.config.max_articles_per_source]


class OAICScraper(BaseNewsScraper):
    """
    Scraper for OAIC - Algeria (http://www.oaic.dz)

    GOVERNMENT SOURCE - Direct tender announcements
    Language: French/Arabic

    NOTE: This source publishes official tender announcements.
    May require French language parsing.

    REQUIRED DATA SOURCE: French NLP/translation for full extraction
    """

    SOURCE_NAME = "OAIC"
    BASE_URL = "http://www.oaic.dz"

    def fetch_articles(self) -> List[TenderAnnouncement]:
        """Fetch tender announcements from OAIC Algeria"""
        articles = []

        try:
            response = self.session.get(
                self.BASE_URL,
                timeout=30
            )

            if response.status_code != 200:
                self.logger.warning(
                    f"OAIC returned status {response.status_code}"
                )
                return articles

            if not BS4_AVAILABLE:
                self.logger.error("BeautifulSoup not installed")
                return articles

            soup = BeautifulSoup(response.text, 'html.parser')

            # French keywords for tenders
            french_keywords = [
                'appel d\'offres', 'avis d\'appel', 'importation',
                'blé', 'céréales', 'adjudication', 'marché'
            ]

            # Search for tender announcements
            for link in soup.find_all('a', href=True):
                text = link.get_text(strip=True).lower()
                href = link.get('href', '')

                # Check for French tender keywords
                if not any(kw in text for kw in french_keywords):
                    continue

                full_url = urljoin(self.BASE_URL, href)

                articles.append(TenderAnnouncement(
                    source=self.SOURCE_NAME,
                    source_url=full_url,
                    headline=link.get_text(strip=True),
                    captured_at=datetime.now(),
                    country="Algeria",
                    agency="OAIC",
                ))

            self.logger.info(f"Found {len(articles)} tender items on OAIC")

        except Exception as e:
            self.logger.error(f"Error scraping OAIC: {e}")

        return articles[:self.config.max_articles_per_source]


class SAGOScraper(BaseNewsScraper):
    """
    Scraper for SAGO - Saudi Arabia (https://www.sago.gov.sa)

    GOVERNMENT SOURCE - Direct tender announcements
    Language: Arabic (primary), English (some content)

    NOTE: This source publishes official tender announcements.
    May require Arabic language parsing.

    REQUIRED DATA SOURCE: Arabic NLP/translation for full extraction
    """

    SOURCE_NAME = "SAGO"
    BASE_URL = "https://www.sago.gov.sa"

    def fetch_articles(self) -> List[TenderAnnouncement]:
        """Fetch tender announcements from SAGO Saudi Arabia"""
        articles = []

        try:
            # Try English version first
            response = self.session.get(
                f"{self.BASE_URL}/en",
                timeout=30
            )

            if response.status_code != 200:
                self.logger.warning(
                    f"SAGO returned status {response.status_code}"
                )
                return articles

            if not BS4_AVAILABLE:
                self.logger.error("BeautifulSoup not installed")
                return articles

            soup = BeautifulSoup(response.text, 'html.parser')

            # Arabic keywords for tenders
            arabic_keywords = [
                'مناقصة', 'عطاء', 'قمح', 'شعير', 'حبوب',  # tender, bid, wheat, barley, grains
            ]

            # English keywords
            english_keywords = ['tender', 'bid', 'wheat', 'barley', 'grain', 'import']

            all_keywords = arabic_keywords + english_keywords

            for link in soup.find_all('a', href=True):
                text = link.get_text(strip=True)
                text_lower = text.lower()
                href = link.get('href', '')

                if not any(kw in text_lower or kw in text for kw in all_keywords):
                    continue

                full_url = urljoin(self.BASE_URL, href)

                articles.append(TenderAnnouncement(
                    source=self.SOURCE_NAME,
                    source_url=full_url,
                    headline=text,
                    captured_at=datetime.now(),
                    country="Saudi Arabia",
                    agency="SAGO",
                ))

            self.logger.info(f"Found {len(articles)} tender items on SAGO")

        except Exception as e:
            self.logger.error(f"Error scraping SAGO: {e}")

        return articles[:self.config.max_articles_per_source]


# =============================================================================
# MAIN COLLECTOR CLASS
# =============================================================================

class WheatTenderCollector(BaseCollector):
    """
    Collector for international wheat tender announcements and results.

    Monitors multiple sources:
    - Government agencies (OAIC, SAGO) - PRIMARY
    - News aggregators (Agricensus, AgroChart)
    - Supplementary sources (USDA FAS)

    Parses tender details using NLP/regex and stores in database.

    Usage:
        config = WheatTenderConfig()
        collector = WheatTenderCollector(config)
        result = collector.collect()
    """

    def __init__(self, config: WheatTenderConfig = None):
        config = config or WheatTenderConfig()
        super().__init__(config)
        self.config: WheatTenderConfig = config

        # Initialize parser
        self.parser = TenderParser()

        # Initialize scrapers (ordered by priority)
        self.scrapers = [
            OAICScraper(config),       # Government - Algeria
            SAGOScraper(config),       # Government - Saudi Arabia
            AgricensusScraper(config), # News - Primary
            AgroChartScraper(config),  # News - Secondary
        ]

        self.logger.info(
            f"Initialized WheatTenderCollector with {len(self.scrapers)} sources"
        )

    def get_table_name(self) -> str:
        """Get database table name"""
        return "bronze.wheat_tender_raw"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch wheat tender data from all sources.

        Args:
            start_date: Not used (real-time monitoring)
            end_date: Not used (real-time monitoring)
            **kwargs: Additional parameters

        Returns:
            CollectorResult with tender data
        """
        all_announcements = []
        all_results = []
        warnings = []

        # Collect from all scrapers
        for scraper in self.scrapers:
            try:
                announcements = scraper.fetch_articles()
                all_announcements.extend(announcements)
                self.logger.info(
                    f"Collected {len(announcements)} items from {scraper.SOURCE_NAME}"
                )
            except Exception as e:
                warnings.append(f"{scraper.SOURCE_NAME}: {str(e)}")
                self.logger.error(f"Error from {scraper.SOURCE_NAME}: {e}")

        # Parse announcements for tender details
        for announcement in all_announcements:
            try:
                # Fetch full article text if needed
                article_text = self._fetch_article_text(announcement.source_url)

                # Parse tender details
                result = self.parser.parse(
                    text=article_text or announcement.headline,
                    headline=announcement.headline,
                    source=announcement.source,
                    source_url=announcement.source_url
                )

                # Only include if we extracted meaningful data
                if result.parse_confidence > 0.3:
                    all_results.append(result)

            except Exception as e:
                self.logger.warning(f"Error parsing {announcement.source_url}: {e}")

        # Convert to records for storage
        records = self._results_to_records(all_results)

        if not records:
            return CollectorResult(
                success=True,  # Success even if no new tenders
                source=self.config.source_name,
                records_fetched=0,
                data=[],
                warnings=warnings
            )

        # Convert to DataFrame if pandas available
        if PANDAS_AVAILABLE:
            df = pd.DataFrame(records)
        else:
            df = records

        return CollectorResult(
            success=True,
            source=self.config.source_name,
            records_fetched=len(records),
            data=df,
            warnings=warnings
        )

    def _fetch_article_text(self, url: str) -> Optional[str]:
        """Fetch full article text from URL"""
        try:
            response = self.session.get(url, timeout=15)
            if response.status_code != 200:
                return None

            if not BS4_AVAILABLE:
                return None

            soup = BeautifulSoup(response.text, 'html.parser')

            # Remove script and style elements
            for script in soup(['script', 'style', 'nav', 'footer', 'header']):
                script.decompose()

            # Get text from article body - common selectors
            article = soup.find('article') or soup.find(class_=re.compile(r'article|content|post'))

            if article:
                return article.get_text(separator=' ', strip=True)

            # Fallback to main body
            body = soup.find('body')
            if body:
                return body.get_text(separator=' ', strip=True)[:5000]

            return None

        except Exception as e:
            self.logger.debug(f"Could not fetch article {url}: {e}")
            return None

    def _results_to_records(self, results: List[TenderResult]) -> List[Dict]:
        """Convert TenderResult objects to database records"""
        records = []

        for r in results:
            record = {
                'source_name': r.source,
                'source_article_id': hashlib.md5(r.source_url.encode()).hexdigest()[:32],
                'captured_at': r.captured_at.isoformat(),
                'headline': r.headline,
                'article_url': r.source_url,

                'country_raw': r.country,
                'agency_raw': r.agency,
                'commodity_raw': r.commodity,
                'wheat_type_raw': r.wheat_type,

                'volume_value': r.volume_mt,
                'volume_unit': 'MT',
                'price_value': r.price_usd_mt,
                'price_type': r.price_type,

                'origins_raw': ', '.join(r.origins) if r.origins else None,
                'suppliers_raw': ', '.join(r.suppliers) if r.suppliers else None,

                'tender_type': r.tender_type.value,
                'tender_date': r.tender_date.isoformat() if r.tender_date else None,

                'parse_confidence': r.parse_confidence,
                'raw_text': r.raw_text[:5000] if r.raw_text else None,
            }
            records.append(record)

        return records

    def parse_response(self, response_data: Any) -> Any:
        """Parse response data (passthrough for this collector)"""
        return response_data

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_recent_tenders(
        self,
        country: str = None,
        days: int = 7
    ) -> List[TenderResult]:
        """
        Get recent tenders, optionally filtered by country.

        Args:
            country: Filter by country name
            days: Number of days to look back

        Returns:
            List of TenderResult objects
        """
        result = self.collect(use_cache=False)

        if not result.success or result.data is None:
            return []

        if PANDAS_AVAILABLE and hasattr(result.data, 'to_dict'):
            records = result.data.to_dict('records')
        else:
            records = result.data

        # Filter by country if specified
        if country:
            records = [r for r in records if country.lower() in
                      (r.get('country_raw', '') or '').lower()]

        return records

    def get_egypt_tenders(self) -> List[TenderResult]:
        """Get recent Egyptian wheat tenders"""
        return self.get_recent_tenders(country='Egypt')

    def get_algeria_tenders(self) -> List[TenderResult]:
        """Get recent Algerian wheat tenders"""
        return self.get_recent_tenders(country='Algeria')

    def scan_for_alerts(self) -> List[Dict]:
        """
        Scan for tenders that should trigger alerts.

        Returns:
            List of alert dictionaries
        """
        result = self.collect(use_cache=False)

        if not result.success or result.data is None:
            return []

        alerts = []

        if PANDAS_AVAILABLE and hasattr(result.data, 'to_dict'):
            records = result.data.to_dict('records')
        else:
            records = result.data

        for record in records:
            volume = record.get('volume_value', 0) or 0
            country = record.get('country_raw', '')

            # Large tender alert (>500K MT)
            if volume >= 500000:
                alerts.append({
                    'type': 'large_tender',
                    'record': record,
                    'message': f"Large tender: {country} - {volume:,.0f} MT"
                })

            # Egypt tender
            if country and 'egypt' in country.lower():
                alerts.append({
                    'type': 'egypt_tender',
                    'record': record,
                    'message': f"Egypt tender: {volume:,.0f} MT" if volume else "Egypt tender detected"
                })

            # Algeria tender
            if country and 'algeria' in country.lower():
                alerts.append({
                    'type': 'algeria_tender',
                    'record': record,
                    'message': f"Algeria tender: {volume:,.0f} MT" if volume else "Algeria tender detected"
                })

        return alerts


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for Wheat Tender Collector"""
    import argparse

    parser = argparse.ArgumentParser(description='Wheat Tender Collector')

    parser.add_argument(
        'command',
        choices=['collect', 'scan', 'test', 'egypt', 'algeria'],
        help='Command to execute'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file (JSON or CSV)'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose output'
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Create collector
    config = WheatTenderConfig()
    collector = WheatTenderCollector(config)

    if args.command == 'test':
        success, message = collector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'} - {message}")
        return

    if args.command == 'collect':
        result = collector.collect()
        print(f"Success: {result.success}")
        print(f"Records: {result.records_fetched}")

        if result.warnings:
            print(f"Warnings: {result.warnings}")

        if args.output and result.data is not None:
            if args.output.endswith('.csv') and PANDAS_AVAILABLE:
                result.data.to_csv(args.output, index=False)
            else:
                with open(args.output, 'w') as f:
                    if PANDAS_AVAILABLE and hasattr(result.data, 'to_json'):
                        f.write(result.data.to_json(orient='records', indent=2))
                    else:
                        json.dump(result.data, f, indent=2, default=str)
            print(f"Saved to: {args.output}")

    elif args.command == 'scan':
        alerts = collector.scan_for_alerts()
        print(f"\nFound {len(alerts)} alerts:\n")
        for alert in alerts:
            print(f"  [{alert['type']}] {alert['message']}")

    elif args.command == 'egypt':
        tenders = collector.get_egypt_tenders()
        print(f"\nFound {len(tenders)} Egypt tenders:\n")
        for t in tenders:
            print(f"  - {t.get('headline', 'N/A')[:80]}")
            if t.get('volume_value'):
                print(f"    Volume: {t['volume_value']:,.0f} MT")

    elif args.command == 'algeria':
        tenders = collector.get_algeria_tenders()
        print(f"\nFound {len(tenders)} Algeria tenders:\n")
        for t in tenders:
            print(f"  - {t.get('headline', 'N/A')[:80]}")
            if t.get('volume_value'):
                print(f"    Volume: {t['volume_value']:,.0f} MT")


if __name__ == '__main__':
    main()
