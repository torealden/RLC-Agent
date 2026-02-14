#!/usr/bin/env python3
"""
USDA Cash Price Collector Agent

Collects cash grain, livestock, and specialty prices from USDA AMS Market News API.
Parses narrative text to extract structured price data.

Round Lakes Commodities
"""

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import requests
from dotenv import load_dotenv

# Setup paths
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
load_dotenv(project_root / ".env")

# Database
try:
    import psycopg2
    from psycopg2.extras import execute_values, Json
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('CashPriceCollector')

# =============================================================================
# CONFIGURATION
# =============================================================================

# Database config
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'rlc_commodities'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

# USDA API config
USDA_API_KEY = os.getenv('USDA_AMS_API_KEY', '')
USDA_BASE_URL = 'https://marsapi.ams.usda.gov/services/v1.2'

# Report catalog - organized by category
# has_narrative: True = report has parseable price narratives
#                False = prices are in structured data (not narrative text)
GRAIN_REPORTS = {
    # States with parseable price narratives
    '2850': {'title': 'Iowa Daily Cash Grain Bids', 'state': 'IA', 'city': 'Des Moines', 'has_narrative': True},
    '3192': {'title': 'Illinois Grain Bids', 'state': 'IL', 'city': 'Springfield', 'has_narrative': True},

    # States without parseable narratives (structured data only or no data)
    '2886': {'title': 'Kansas Daily Grain Bids', 'state': 'KS', 'city': 'Kansas City', 'has_narrative': False},
    '3225': {'title': 'Nebraska Daily Elevator Grain Bids', 'state': 'NE', 'city': 'Lincoln', 'has_narrative': False},
    '2851': {'title': 'Ohio Daily Grain Bids', 'state': 'OH', 'city': 'Columbus', 'has_narrative': False},
    '3463': {'title': 'Indiana Grain Bids', 'state': 'IN', 'city': 'Indianapolis', 'has_narrative': False},
    '3049': {'title': 'Southern Minnesota Daily Grain Bids', 'state': 'MN', 'city': 'Minneapolis', 'has_narrative': False},
    '2932': {'title': 'Missouri Daily Grain Bids', 'state': 'MO', 'city': 'Jefferson City', 'has_narrative': False},  # Has trends, not prices
    '2711': {'title': 'Texas Daily Grain Bids', 'state': 'TX', 'city': 'Amarillo', 'has_narrative': False},
    '3100': {'title': 'Oklahoma Daily Grain Bids', 'state': 'OK', 'city': 'Oklahoma City', 'has_narrative': False},
    '2960': {'title': 'Arkansas Daily Grain Bids', 'state': 'AR', 'city': 'Little Rock', 'has_narrative': False},
    '2928': {'title': 'Mississippi Daily Grain Bids', 'state': 'MS', 'city': 'Jackson', 'has_narrative': False},  # Has change info, not prices
    '3088': {'title': 'Tennessee Daily Grain Bids', 'state': 'TN', 'city': 'Nashville', 'has_narrative': False},
    '3186': {'title': 'South Dakota Daily Grain Bids', 'state': 'SD', 'city': 'Sioux Falls', 'has_narrative': False},
    '2771': {'title': 'Montana Daily Elevator Grain Bids', 'state': 'MT', 'city': 'Billings', 'has_narrative': False},
    '2912': {'title': 'Colorado Daily Grain Bids', 'state': 'CO', 'city': 'Denver', 'has_narrative': False},
    '3148': {'title': 'Portland Daily Grain Bids', 'state': 'OR', 'city': 'Portland', 'has_narrative': False},
}

# Specialty reports - Note: Most don't have parseable narratives in the API
# Prices may need to be obtained through PDF parsing or alternate data sources
SPECIALTY_REPORTS = {
    # Ethanol and co-products (metadata only - no price data in API)
    '3616': {'title': 'National Weekly Ethanol Report', 'category': 'energy', 'has_prices': False},
    '3617': {'title': 'National Daily Ethanol Report', 'category': 'energy', 'has_prices': False},
    # Feedstocks (metadata only - prices in PDF)
    '3511': {'title': 'National Grain and Oilseed Processor Feedstuff Report', 'category': 'feedstock', 'has_prices': False},
    '3510': {'title': 'National Animal By-Product Feedstuff Report', 'category': 'feedstock', 'has_prices': False},
    # Tallow and fats (metadata only - prices in PDF)
    '2837': {'title': 'USDA Tallow & Protein Report', 'category': 'feedstock', 'has_prices': False},
    '2839': {'title': 'Weekly Tallow & Protein Report', 'category': 'feedstock', 'has_prices': False},
}

# Livestock reports with price narratives (to be expanded)
LIVESTOCK_REPORTS = {
    # TODO: Identify livestock auction reports with parseable price narratives
    # Many regional auctions have narrative summaries like "Steers 800-900 lbs 165.00-175.00"
}

# =============================================================================
# PRICE PARSERS
# =============================================================================

@dataclass
class ParsedPrice:
    """Parsed price from narrative text"""
    commodity: str
    price: Optional[float] = None
    price_low: Optional[float] = None
    price_high: Optional[float] = None
    basis: Optional[float] = None
    basis_month: Optional[str] = None
    change: Optional[float] = None
    change_direction: Optional[str] = None
    unit: str = '$/bushel'


class GrainNarrativeParser:
    """Parses grain price narratives from USDA reports"""

    # Patterns for different narrative formats
    PATTERNS = {
        # "Corn -- $3.91 (-.33H) Up 3 cents" (Iowa style)
        'standard': re.compile(
            r'(?P<commodity>corn|soybeans?|wheat|oats|milo|sorghum)\s*[-–]+\s*'
            r'\$?(?P<price>[\d.]+)\s*'
            r'\((?P<basis>[+-]?[\d.]+)(?P<month>[A-Z])\)\s*'
            r'(?P<direction>up|down|unchanged|steady)?\s*'
            r'(?P<change>[\d.]+)?\s*(?:cents?)?',
            re.IGNORECASE
        ),
        # "Corn: 4.10 (-21H)" (Illinois style - basis in cents as integer)
        'colon_cents': re.compile(
            r'(?P<commodity>corn|soybeans?|wheat|oats|milo|sorghum):\s*'
            r'\$?(?P<price>[\d.]+)\s*'
            r'\((?P<basis>[+-]?\d+)(?P<month>[A-Z])\)',
            re.IGNORECASE
        ),
        # "Corn 4.10 -21H" (no parens, basis in cents)
        'no_parens': re.compile(
            r'(?P<commodity>corn|soybeans?|wheat|oats|milo|sorghum)\s+'
            r'\$?(?P<price>[\d.]+)\s+'
            r'(?P<basis>[+-]?\d+)(?P<month>[A-Z])(?:\s|$)',
            re.IGNORECASE
        ),
        # "Corn: $3.91, Basis: -.33H"
        'alternate': re.compile(
            r'(?P<commodity>corn|soybeans?|wheat):\s*\$?(?P<price>[\d.]+)',
            re.IGNORECASE
        ),
        # "State Average Price: Corn -- $3.91"
        'state_avg': re.compile(
            r'state\s+average.*?(?P<commodity>corn|soybeans?|wheat)\s*[-–]+\s*\$?(?P<price>[\d.]+)',
            re.IGNORECASE
        ),
        # "Corn  4.10 to 4.25" (price range - must be > $1 to be a price)
        'range': re.compile(
            r'(?P<commodity>corn|soybeans?|wheat|oats|milo|sorghum)\s+'
            r'\$(?P<low>\d+\.\d{2})\s*(?:to|-)\s*\$?(?P<high>\d+\.\d{2})',
            re.IGNORECASE
        ),
    }

    @classmethod
    def _normalize_commodity(cls, commodity: str) -> str:
        """Normalize commodity name"""
        commodity = commodity.lower()
        if commodity.startswith('soybean'):
            return 'soybeans'
        return commodity

    @classmethod
    def _is_integer_basis(cls, basis_str: str) -> bool:
        """Check if basis is integer (cents) vs decimal (dollars)"""
        # If no decimal point and value > 1, it's cents
        if '.' not in basis_str:
            try:
                val = abs(int(basis_str))
                return val > 1  # Anything > 1 cent is likely in cents
            except:
                return False
        return False

    @classmethod
    def parse(cls, narrative: str) -> List[ParsedPrice]:
        """Parse narrative text to extract prices"""
        if not narrative:
            return []

        results = []
        found_commodities = set()  # Track to avoid duplicates

        # Try standard pattern first (Iowa style: "Corn -- $3.91 (-.33H) Up 3 cents")
        for match in cls.PATTERNS['standard'].finditer(narrative):
            commodity = cls._normalize_commodity(match.group('commodity'))
            if commodity in found_commodities:
                continue
            found_commodities.add(commodity)

            basis_str = match.group('basis')
            basis_val = float(basis_str) if basis_str else None

            price = ParsedPrice(
                commodity=commodity,
                price=float(match.group('price')) if match.group('price') else None,
                basis=basis_val,
                basis_month=match.group('month') if match.group('month') else None,
            )

            # Parse change
            direction = match.group('direction')
            if direction:
                direction = direction.lower()
                price.change_direction = direction
                change_val = match.group('change')
                if change_val:
                    change_cents = float(change_val) / 100  # Convert cents to dollars
                    price.change = change_cents if direction == 'up' else -change_cents
                elif direction == 'unchanged' or direction == 'steady':
                    price.change = 0.0

            results.append(price)

        # Try colon_cents pattern (Illinois style: "Corn: 4.10 (-21H)")
        if not results:
            for match in cls.PATTERNS['colon_cents'].finditer(narrative):
                commodity = cls._normalize_commodity(match.group('commodity'))
                if commodity in found_commodities:
                    continue
                found_commodities.add(commodity)

                basis_str = match.group('basis')
                # Convert integer cents to decimal dollars (e.g., -21 -> -0.21)
                if basis_str:
                    if cls._is_integer_basis(basis_str):
                        basis_val = int(basis_str) / 100.0
                    else:
                        basis_val = float(basis_str)
                else:
                    basis_val = None

                results.append(ParsedPrice(
                    commodity=commodity,
                    price=float(match.group('price')) if match.group('price') else None,
                    basis=basis_val,
                    basis_month=match.group('month') if match.group('month') else None,
                ))

        # Try no_parens pattern (e.g., "Corn 4.10 -21H")
        if not results:
            for match in cls.PATTERNS['no_parens'].finditer(narrative):
                commodity = cls._normalize_commodity(match.group('commodity'))
                if commodity in found_commodities:
                    continue
                found_commodities.add(commodity)

                basis_str = match.group('basis')
                if basis_str and cls._is_integer_basis(basis_str):
                    basis_val = int(basis_str) / 100.0
                else:
                    basis_val = float(basis_str) if basis_str else None

                results.append(ParsedPrice(
                    commodity=commodity,
                    price=float(match.group('price')) if match.group('price') else None,
                    basis=basis_val,
                    basis_month=match.group('month') if match.group('month') else None,
                ))

        # Try range pattern (e.g., "Corn 4.10 to 4.25")
        if not results:
            for match in cls.PATTERNS['range'].finditer(narrative):
                commodity = cls._normalize_commodity(match.group('commodity'))
                if commodity in found_commodities:
                    continue
                found_commodities.add(commodity)

                low = float(match.group('low'))
                high = float(match.group('high'))
                results.append(ParsedPrice(
                    commodity=commodity,
                    price=(low + high) / 2,  # Use average
                    price_low=low,
                    price_high=high,
                ))

        # Try state_avg pattern last
        if not results:
            for match in cls.PATTERNS['state_avg'].finditer(narrative):
                commodity = cls._normalize_commodity(match.group('commodity'))
                if commodity in found_commodities:
                    continue
                found_commodities.add(commodity)

                results.append(ParsedPrice(
                    commodity=commodity,
                    price=float(match.group('price')) if match.group('price') else None,
                ))

        return results


class SpecialtyPriceParser:
    """Parses specialty prices (ethanol, feedstocks, etc.)"""

    PATTERNS = {
        # Ethanol: "Ethanol $1.45-1.50 per gallon"
        'ethanol': re.compile(
            r'ethanol\s*\$?([\d.]+)\s*[-–]?\s*([\d.]+)?\s*(?:per\s+)?(?P<unit>gallon|gal)',
            re.IGNORECASE
        ),
        # DDGS: "DDG 165.00-170.00 per ton"
        'ddgs': re.compile(
            r'(?:DDG[S]?|dried\s+distillers)\s*\$?([\d.]+)\s*[-–]?\s*([\d.]+)?\s*(?:per\s+)?(?P<unit>ton|cwt)',
            re.IGNORECASE
        ),
        # Generic price range: "Product Name 45.00-50.00"
        'generic': re.compile(
            r'(?P<product>[\w\s]+?)\s+\$?([\d.]+)\s*[-–]\s*([\d.]+)',
            re.IGNORECASE
        ),
    }

    @classmethod
    def parse(cls, narrative: str, category: str) -> List[Dict]:
        """Parse specialty price narrative"""
        if not narrative:
            return []

        results = []

        # Ethanol
        for match in cls.PATTERNS['ethanol'].finditer(narrative):
            low = float(match.group(1))
            high = float(match.group(2)) if match.group(2) else low
            results.append({
                'commodity': 'ethanol',
                'category': 'energy',
                'price_low': low,
                'price_high': high,
                'price_avg': (low + high) / 2,
                'unit': '$/gallon',
            })

        # DDGS
        for match in cls.PATTERNS['ddgs'].finditer(narrative):
            low = float(match.group(1))
            high = float(match.group(2)) if match.group(2) else low
            unit = match.group('unit').lower()
            results.append({
                'commodity': 'DDGS',
                'category': 'co_product',
                'price_low': low,
                'price_high': high,
                'price_avg': (low + high) / 2,
                'unit': f'$/{unit}',
            })

        return results


# =============================================================================
# COLLECTOR
# =============================================================================

class CashPriceCollector:
    """Collects cash prices from USDA AMS Market News API"""

    def __init__(self):
        self.session = requests.Session()
        self.session.auth = (USDA_API_KEY, '')
        self.session.headers.update({'Accept': 'application/json'})
        self.conn = None
        self.cities_found = set()  # Track cities for weather integration

    def connect_db(self):
        """Connect to PostgreSQL"""
        if not DB_AVAILABLE:
            logger.warning("psycopg2 not available, running without database")
            return False
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False

    def close_db(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()

    def fetch_report(self, slug_id: str, days_back: int = 30) -> List[Dict]:
        """Fetch report data from USDA API"""
        try:
            url = f"{USDA_BASE_URL}/reports/{slug_id}"
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()

            data = resp.json()
            results = data.get('results', [])

            # Filter by date if needed
            if days_back and results:
                cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%m/%d/%Y')
                results = [r for r in results
                          if r.get('report_date', '01/01/1900') >= cutoff]

            return results

        except Exception as e:
            logger.error(f"Failed to fetch report {slug_id}: {e}")
            return []

    def save_bronze(self, records: List[Dict], slug_id: str) -> int:
        """Save raw records to bronze layer"""
        if not self.conn:
            return 0

        saved = 0
        cur = self.conn.cursor()

        for record in records:
            try:
                report_date = datetime.strptime(
                    record.get('report_date', '01/01/2000'),
                    '%m/%d/%Y'
                ).date()

                cur.execute("""
                    INSERT INTO bronze.price_report_raw (
                        slug_id, slug_name, report_title, report_date,
                        published_date, office_name, office_city, office_state,
                        market_type, report_narrative, raw_response, source
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (slug_id, report_date) DO UPDATE SET
                        report_narrative = EXCLUDED.report_narrative,
                        collected_at = NOW()
                    RETURNING id
                """, (
                    slug_id,
                    record.get('slug_name'),
                    record.get('report_title'),
                    report_date,
                    record.get('published_date'),
                    record.get('office_name'),
                    record.get('office_city'),
                    record.get('office_state'),
                    record.get('market_type'),
                    record.get('report_narrative'),
                    Json(record),
                    'USDA_AMS'
                ))
                saved += 1

                # Track city
                city = record.get('office_city')
                state = record.get('office_state')
                if city and state:
                    self.cities_found.add((city, state))

            except Exception as e:
                logger.warning(f"Failed to save bronze record: {e}")
                self.conn.rollback()
                continue

        self.conn.commit()
        return saved

    def process_grain_prices(self, slug_id: str, report_info: Dict) -> int:
        """Process grain prices from bronze to silver"""
        if not self.conn:
            return 0

        cur = self.conn.cursor()
        processed = 0

        # Get unprocessed bronze records
        cur.execute("""
            SELECT id, report_date, report_narrative, office_state
            FROM bronze.price_report_raw
            WHERE slug_id = %s
            AND report_narrative IS NOT NULL
        """, (slug_id,))

        for bronze_id, report_date, narrative, state in cur.fetchall():
            parsed = GrainNarrativeParser.parse(narrative)

            for price in parsed:
                try:
                    cur.execute("""
                        INSERT INTO silver.cash_price (
                            report_date, commodity, location_state, location_name,
                            price_cash, basis, basis_month,
                            change_daily, change_direction,
                            source, slug_id, bronze_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (report_date, commodity, location_state, slug_id)
                        DO UPDATE SET
                            price_cash = EXCLUDED.price_cash,
                            basis = EXCLUDED.basis,
                            change_daily = EXCLUDED.change_daily,
                            parsed_at = NOW()
                    """, (
                        report_date,
                        price.commodity,
                        state or report_info.get('state'),
                        report_info.get('title', ''),
                        price.price,
                        price.basis,
                        price.basis_month,
                        price.change,
                        price.change_direction,
                        'USDA_AMS',
                        slug_id,
                        bronze_id
                    ))
                    processed += 1
                except Exception as e:
                    logger.warning(f"Failed to save silver record: {e}")
                    self.conn.rollback()

        self.conn.commit()
        return processed

    def collect_grain_prices(self, days_back: int = 30, narrative_only: bool = True) -> Dict[str, int]:
        """Collect all grain prices

        Args:
            days_back: Number of days of history to collect
            narrative_only: If True, only collect from states with parseable narratives
        """
        results = {}

        for slug_id, info in GRAIN_REPORTS.items():
            # Skip states without narratives if narrative_only is True
            if narrative_only and not info.get('has_narrative', False):
                continue

            logger.info(f"Collecting {info['title']}...")

            # Fetch from API
            records = self.fetch_report(slug_id, days_back)
            logger.info(f"  Fetched {len(records)} records")

            if not records:
                results[slug_id] = {'fetched': 0, 'bronze': 0, 'silver': 0}
                continue

            # Save to bronze
            bronze_count = self.save_bronze(records, slug_id)
            logger.info(f"  Saved {bronze_count} to bronze")

            # Process to silver (only if has_narrative)
            silver_count = 0
            if info.get('has_narrative', False):
                silver_count = self.process_grain_prices(slug_id, info)
                logger.info(f"  Processed {silver_count} to silver")
            else:
                logger.info(f"  Skipping silver (no narrative)")

            results[slug_id] = {
                'fetched': len(records),
                'bronze': bronze_count,
                'silver': silver_count
            }

        return results

    def get_new_cities_for_weather(self) -> List[Dict]:
        """Return cities found that might need weather tracking"""
        return [{'city': city, 'state': state} for city, state in self.cities_found]


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='USDA Cash Price Collector')
    parser.add_argument('--days', type=int, default=30,
                       help='Days of history to collect')
    parser.add_argument('--report', type=str,
                       help='Specific report slug to collect')
    parser.add_argument('--list-reports', action='store_true',
                       help='List available reports')
    parser.add_argument('--test', action='store_true',
                       help='Test API connection')
    parser.add_argument('--show-cities', action='store_true',
                       help='Show cities found for weather integration')
    args = parser.parse_args()

    if args.list_reports:
        print("\nGrain Reports:")
        for slug, info in GRAIN_REPORTS.items():
            print(f"  {slug}: {info['title']} ({info['state']})")
        print("\nSpecialty Reports:")
        for slug, info in SPECIALTY_REPORTS.items():
            print(f"  {slug}: {info['title']} ({info['category']})")
        return

    collector = CashPriceCollector()

    if args.test:
        print("Testing USDA API connection...")
        records = collector.fetch_report('2850', days_back=7)
        print(f"Fetched {len(records)} records from Iowa Daily Cash Grain Bids")
        if records:
            print(f"Latest: {records[0].get('report_date')}")
            print(f"Narrative: {records[0].get('report_narrative', '')[:200]}...")

            # Test parser
            parsed = GrainNarrativeParser.parse(records[0].get('report_narrative', ''))
            print(f"\nParsed {len(parsed)} prices:")
            for p in parsed:
                print(f"  {p.commodity}: ${p.price} (basis: {p.basis}{p.basis_month}) {p.change_direction}")
        return

    # Connect to database
    if not collector.connect_db():
        logger.error("Cannot proceed without database connection")
        return

    try:
        # Collect prices
        if args.report:
            if args.report in GRAIN_REPORTS:
                info = GRAIN_REPORTS[args.report]
                records = collector.fetch_report(args.report, args.days)
                collector.save_bronze(records, args.report)
                collector.process_grain_prices(args.report, info)
            else:
                logger.error(f"Unknown report: {args.report}")
        else:
            results = collector.collect_grain_prices(args.days)
            print("\n=== Collection Summary ===")
            total_silver = 0
            for slug, counts in results.items():
                print(f"{slug}: {counts['silver']} prices parsed")
                total_silver += counts['silver']
            print(f"\nTotal: {total_silver} price records")

        # Show cities for weather integration
        if args.show_cities or True:  # Always show
            cities = collector.get_new_cities_for_weather()
            if cities:
                print(f"\n=== Cities for Weather Integration ({len(cities)}) ===")
                for c in cities:
                    print(f"  {c['city']}, {c['state']}")

    finally:
        collector.close_db()


if __name__ == '__main__':
    main()
