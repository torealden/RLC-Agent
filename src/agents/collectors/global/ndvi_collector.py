"""
NDVI (Normalized Difference Vegetation Index) Collector

Collects vegetation health indices for crop monitoring from:
1. USDA Crop Explorer - Chart images for visual analysis
2. NASA AppEEARS API - Time series data (requires Earthdata credentials)

NDVI values range from -1 to 1:
- Values > 0.6: Dense, healthy vegetation
- Values 0.2-0.6: Sparse to moderate vegetation
- Values < 0.2: Bare soil, water, or stressed vegetation

Usage:
    python ndvi_collector.py charts --commodity corn --countries US BR AR
    python ndvi_collector.py charts --all
    python ndvi_collector.py timeseries --region US_CORN_BELT --start 2024-01-01
"""

import os
import sys
import json
import logging
import argparse
import requests
import time
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('NDVICollector')

# =============================================================================
# Constants
# =============================================================================

BASE_URL = "https://ipad.fas.usda.gov"

# USDA Crop Explorer chart types
CHART_TYPES = {
    'ndvi': {'type_id': 47, 'attr_id': 1022, 'name': 'NDVI'},
    'ndvi_anomaly': {'type_id': 48, 'attr_id': 1024, 'name': 'NDVI Anomaly'},
    'precipitation': {'type_id': 60, 'attr_id': 1092, 'name': 'Cumulative Precipitation'},
    'soil_moisture': {'type_id': 56, 'attr_id': 1058, 'name': 'Soil Moisture'},
    'temperature': {'type_id': 61, 'attr_id': 1093, 'name': 'Temperature'},
}

# Commodity codes for Crop Explorer
COMMODITY_CODES = {
    'corn': '0440000',
    'soybeans': '2222000',
    'wheat': '0410000',
    'rice': '0422110',
    'cotton': '2631000',
    'sorghum': '0459100',
    'barley': '0430000',
    'sunflowerseed': '1206000',
    'rapeseed': '1205000',
}

# Country/region codes for USDA
# 'code' is for web interface, 'chart_code' is for thumbnail chart filenames
REGION_CODES = {
    'US': {'code': 'us', 'chart_code': 'USA', 'name': 'United States'},
    'BR': {'code': 'br', 'chart_code': 'BRA', 'name': 'Brazil'},
    'AR': {'code': 'ar', 'chart_code': 'ARG', 'name': 'Argentina'},
    'CN': {'code': 'ch', 'chart_code': 'CHN', 'name': 'China'},
    'IN': {'code': 'in', 'chart_code': 'IND', 'name': 'India'},
    'AU': {'code': 'as', 'chart_code': 'AUS', 'name': 'Australia'},
    'UA': {'code': 'up', 'chart_code': 'UKR', 'name': 'Ukraine'},
    'RU': {'code': 'rs', 'chart_code': 'RUS', 'name': 'Russia'},
    'EU': {'code': 'e4', 'chart_code': 'EUE', 'name': 'European Union'},
    'CA': {'code': 'ca', 'chart_code': 'CAN', 'name': 'Canada'},
    'MX': {'code': 'mx', 'chart_code': 'MEX', 'name': 'Mexico'},
    'ZA': {'code': 'sf', 'chart_code': 'ZAF', 'name': 'South Africa'},
    'TH': {'code': 'th', 'chart_code': 'THA', 'name': 'Thailand'},
    'VN': {'code': 'vm', 'chart_code': 'VNM', 'name': 'Vietnam'},
    'ID': {'code': 'id', 'chart_code': 'IDN', 'name': 'Indonesia'},
}

# Key crop regions for NDVI monitoring
CROP_REGIONS = {
    'US_CORN_BELT': {
        'name': 'US Corn Belt',
        'commodities': ['corn', 'soybeans'],
        'countries': ['US'],
        'critical_months': [5, 6, 7, 8],  # May-August
    },
    'BR_CENTER_WEST': {
        'name': 'Brazil Center-West',
        'commodities': ['soybeans', 'corn'],
        'countries': ['BR'],
        'critical_months': [12, 1, 2, 3],  # Dec-March
    },
    'AR_PAMPAS': {
        'name': 'Argentina Pampas',
        'commodities': ['soybeans', 'corn', 'wheat'],
        'countries': ['AR'],
        'critical_months': [12, 1, 2, 3],  # Dec-March
    },
    'BLACK_SEA': {
        'name': 'Black Sea Region',
        'commodities': ['wheat', 'corn', 'sunflowerseed'],
        'countries': ['UA', 'RU'],
        'critical_months': [5, 6, 7],  # May-July
    },
    'AUSTRALIA': {
        'name': 'Australia Grain Belt',
        'commodities': ['wheat', 'barley'],
        'countries': ['AU'],
        'critical_months': [8, 9, 10],  # Aug-Oct
    },
}

# Output directories
OUTPUT_DIR = PROJECT_ROOT / "data" / "ndvi"
CHARTS_DIR = OUTPUT_DIR / "charts"


@dataclass
class NDVIChart:
    """Represents a downloaded NDVI chart."""
    region_code: str
    commodity: str
    commodity_code: str
    chart_type: str
    year: int
    file_path: Path
    source_url: str
    downloaded_at: datetime


class NDVICollector:
    """Collects NDVI data from USDA and NASA sources."""

    def __init__(self, output_dir: Path = None):
        self.output_dir = output_dir or CHARTS_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'RLC-Agent NDVI Collector/1.0'
        })

    def get_chart_url(
        self,
        commodity_code: str,
        region_code: str,
        chart_code: str,
        chart_type: str = 'ndvi',
        year: int = None,
        thumbnail: bool = True
    ) -> str:
        """Generate URL for USDA Crop Explorer chart."""
        year = year or datetime.now().year
        chart_info = CHART_TYPES.get(chart_type, CHART_TYPES['ndvi'])

        if thumbnail:
            # Thumbnail format: {year}_{commodity}_{attr_id}_{chart_code}.png
            # chart_code uses 3-letter ISO code (USA, BRA, ARG, etc.)
            return f"{BASE_URL}/rssiws/images/thumbnail_charts/{year}_{commodity_code}_{chart_info['attr_id']}_{chart_code}.png"
        else:
            # Full chart via web interface
            return f"{BASE_URL}/cropexplorer/cropview/comm_chartview.aspx?fattributeid=1&cropid={commodity_code}&startrow=1&sel_year={year}&ftypeid={chart_info['type_id']}&regionid={region_code.lower()}&cntryid={region_code.upper()}&nationalGraph=True"

    def download_chart(
        self,
        commodity: str,
        region_code: str,
        chart_type: str = 'ndvi',
        year: int = None,
        force: bool = False
    ) -> Optional[NDVIChart]:
        """Download a single NDVI chart."""
        year = year or datetime.now().year
        commodity_code = COMMODITY_CODES.get(commodity)

        if not commodity_code:
            logger.warning(f"Unknown commodity: {commodity}")
            return None

        region_info = REGION_CODES.get(region_code)
        if not region_info:
            logger.warning(f"Unknown region: {region_code}")
            return None

        # Create output path
        region_dir = self.output_dir / region_code.lower()
        region_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{region_code.lower()}_{commodity}_{chart_type}_{year}.png"
        output_path = region_dir / filename

        # Skip if exists (unless force)
        if output_path.exists() and not force:
            logger.debug(f"Skipping existing: {filename}")
            return NDVIChart(
                region_code=region_code,
                commodity=commodity,
                commodity_code=commodity_code,
                chart_type=chart_type,
                year=year,
                file_path=output_path,
                source_url="cached",
                downloaded_at=datetime.fromtimestamp(output_path.stat().st_mtime)
            )

        # Build URL and download
        url = self.get_chart_url(
            commodity_code=commodity_code,
            region_code=region_info['code'],
            chart_code=region_info['chart_code'],
            chart_type=chart_type,
            year=year,
            thumbnail=True
        )

        try:
            response = self.session.get(url, timeout=30)
            if response.status_code == 200:
                output_path.write_bytes(response.content)
                logger.info(f"Downloaded: {filename}")
                return NDVIChart(
                    region_code=region_code,
                    commodity=commodity,
                    commodity_code=commodity_code,
                    chart_type=chart_type,
                    year=year,
                    file_path=output_path,
                    source_url=url,
                    downloaded_at=datetime.now()
                )
            else:
                logger.warning(f"HTTP {response.status_code} for {filename}")
                return None

        except Exception as e:
            logger.error(f"Error downloading {filename}: {e}")
            return None

    def download_region_charts(
        self,
        region_code: str,
        commodities: List[str] = None,
        chart_types: List[str] = None,
        years: List[int] = None,
        force: bool = False
    ) -> List[NDVIChart]:
        """Download all charts for a region."""
        commodities = commodities or list(COMMODITY_CODES.keys())
        chart_types = chart_types or ['ndvi', 'ndvi_anomaly', 'precipitation']
        years = years or [datetime.now().year]

        charts = []
        for year in years:
            for commodity in commodities:
                for chart_type in chart_types:
                    chart = self.download_chart(
                        commodity=commodity,
                        region_code=region_code,
                        chart_type=chart_type,
                        year=year,
                        force=force
                    )
                    if chart:
                        charts.append(chart)
                    time.sleep(0.3)  # Rate limiting

        return charts

    def download_all_charts(
        self,
        chart_types: List[str] = None,
        years: List[int] = None,
        force: bool = False
    ) -> Dict[str, List[NDVIChart]]:
        """Download charts for all key regions and commodities."""
        chart_types = chart_types or ['ndvi', 'ndvi_anomaly']
        years = years or [datetime.now().year]

        results = {}
        for region_id, region_info in CROP_REGIONS.items():
            logger.info(f"Processing region: {region_info['name']}")
            region_charts = []

            for country in region_info['countries']:
                for commodity in region_info['commodities']:
                    for chart_type in chart_types:
                        for year in years:
                            chart = self.download_chart(
                                commodity=commodity,
                                region_code=country,
                                chart_type=chart_type,
                                year=year,
                                force=force
                            )
                            if chart:
                                region_charts.append(chart)
                            time.sleep(0.3)

            results[region_id] = region_charts
            logger.info(f"  Downloaded {len(region_charts)} charts for {region_info['name']}")

        return results

    def save_to_database(self, charts: List[NDVIChart], conn=None) -> int:
        """Save chart metadata to database."""
        if not charts:
            return 0

        should_close = False
        if conn is None:
            try:
                import psycopg2
                # Load env vars
                from dotenv import load_dotenv
                load_dotenv(PROJECT_ROOT / '.env')

                conn = psycopg2.connect(
                    host=os.environ.get('DB_HOST', 'localhost'),
                    port=os.environ.get('DB_PORT', 5432),
                    database=os.environ.get('DB_NAME', 'rlc_commodities'),
                    user=os.environ.get('DB_USER', 'postgres'),
                    password=os.environ.get('DB_PASSWORD', '')
                )
                should_close = True
            except Exception as e:
                logger.error(f"Database connection error: {e}")
                return 0

        count = 0
        try:
            cur = conn.cursor()
            for chart in charts:
                cur.execute("""
                    INSERT INTO bronze.ndvi_chart (
                        region_code, commodity, commodity_code, chart_type,
                        year, chart_date, file_path, source_url
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (region_code, commodity_code, chart_type, year)
                    DO UPDATE SET
                        file_path = EXCLUDED.file_path,
                        source_url = EXCLUDED.source_url,
                        collected_at = NOW()
                """, (
                    chart.region_code,
                    chart.commodity,
                    chart.commodity_code,
                    chart.chart_type,
                    chart.year,
                    date.today(),
                    str(chart.file_path),
                    chart.source_url
                ))
                count += 1

            conn.commit()
            logger.info(f"Saved {count} chart records to database")

        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")

        finally:
            if should_close:
                conn.close()

        return count

    def generate_chart_inventory(self) -> Dict:
        """Generate inventory of all downloaded charts."""
        inventory = {
            'generated_at': datetime.now().isoformat(),
            'charts_dir': str(self.output_dir),
            'regions': {}
        }

        for region_dir in self.output_dir.iterdir():
            if region_dir.is_dir() and not region_dir.name.startswith('_'):
                charts = list(region_dir.glob('*.png'))
                inventory['regions'][region_dir.name] = {
                    'count': len(charts),
                    'files': [f.name for f in charts]
                }

        inventory['total_charts'] = sum(
            r['count'] for r in inventory['regions'].values()
        )

        return inventory


class AppEEARSClient:
    """
    NASA AppEEARS API Client for MODIS NDVI time series data.

    Requires NASA Earthdata credentials stored in environment variables:
    - NASA_EARTHDATA_TOKEN: JWT bearer token

    API Documentation: https://appeears.earthdatacloud.nasa.gov/api/
    """

    BASE_URL = "https://appeears.earthdatacloud.nasa.gov/api"

    # MODIS NDVI Products
    PRODUCTS = {
        'MOD13Q1': {'name': 'MODIS Terra Vegetation Indices 16-Day 250m', 'resolution': 250},
        'MYD13Q1': {'name': 'MODIS Aqua Vegetation Indices 16-Day 250m', 'resolution': 250},
        'MOD13A1': {'name': 'MODIS Terra Vegetation Indices 16-Day 500m', 'resolution': 500},
        'MOD13A2': {'name': 'MODIS Terra Vegetation Indices 16-Day 1km', 'resolution': 1000},
    }

    def __init__(self, username: str = None, password: str = None):
        from dotenv import load_dotenv
        load_dotenv(PROJECT_ROOT / '.env')

        self.username = username or os.environ.get('NASA_EARTHDATA_USERNAME')
        self.password = password or os.environ.get('NASA_EARTHDATA_PASSWORD')
        self.token = None

        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json'
        })

        # Note: AppEEARS requires login via username/password to get a bearer token
        # The Earthdata JWT token is for different NASA services
        if self.username and self.password:
            self._login()
        else:
            logger.info("AppEEARS: No credentials found. Use login() method or set NASA_EARTHDATA_USERNAME/PASSWORD")

    def _login(self) -> bool:
        """Login to AppEEARS and get bearer token."""
        try:
            response = requests.post(
                f"{self.BASE_URL}/login",
                auth=(self.username, self.password)
            )
            if response.status_code == 200:
                self.token = response.json().get('token')
                self.session.headers['Authorization'] = f'Bearer {self.token}'
                logger.info("AppEEARS login successful")
                return True
            else:
                logger.error(f"AppEEARS login failed: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"AppEEARS login error: {e}")
            return False

    def login(self, username: str, password: str) -> bool:
        """Manual login with credentials."""
        self.username = username
        self.password = password
        return self._login()

    def list_products(self) -> List[Dict]:
        """List available data products."""
        try:
            response = self.session.get(f"{self.BASE_URL}/product")
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to list products: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Error listing products: {e}")
            return []

    def get_product_layers(self, product_id: str) -> List[Dict]:
        """Get available layers for a product."""
        try:
            response = self.session.get(f"{self.BASE_URL}/product/{product_id}")
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get product layers: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Error getting product layers: {e}")
            return []

    def submit_point_request(
        self,
        coordinates: List[Tuple[float, float]],
        product_id: str = 'MOD13Q1',
        layer: str = '_250m_16_days_NDVI',
        start_date: str = None,
        end_date: str = None,
        task_name: str = None
    ) -> Optional[str]:
        """
        Submit a point extraction request for NDVI time series.

        Args:
            coordinates: List of (latitude, longitude) tuples
            product_id: MODIS product ID
            layer: Data layer to extract
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            task_name: Name for the task

        Returns:
            Task ID if successful, None otherwise
        """
        if not self.token:
            logger.error("No Earthdata token available")
            return None

        start_date = start_date or (datetime.now() - timedelta(days=365)).strftime('%m-%d-%Y')
        end_date = end_date or datetime.now().strftime('%m-%d-%Y')
        task_name = task_name or f"NDVI_Extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Build coordinates list
        coords = [
            {
                'id': f'point_{i}',
                'latitude': lat,
                'longitude': lon,
                'category': 'crop_region'
            }
            for i, (lat, lon) in enumerate(coordinates)
        ]

        payload = {
            'task_type': 'point',
            'task_name': task_name,
            'params': {
                'dates': [
                    {'startDate': start_date, 'endDate': end_date}
                ],
                'layers': [
                    {'product': product_id, 'layer': layer}
                ],
                'coordinates': coords
            }
        }

        try:
            response = self.session.post(
                f"{self.BASE_URL}/task",
                json=payload
            )
            if response.status_code in [200, 202]:
                result = response.json()
                task_id = result.get('task_id')
                logger.info(f"Submitted AppEEARS task: {task_id}")
                return task_id
            else:
                logger.error(f"Failed to submit task: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error submitting task: {e}")
            return None

    def check_task_status(self, task_id: str) -> Dict:
        """Check status of a submitted task."""
        try:
            response = self.session.get(f"{self.BASE_URL}/status/{task_id}")
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to check status: {response.status_code}")
                return {}
        except Exception as e:
            logger.error(f"Error checking status: {e}")
            return {}

    def download_task_results(self, task_id: str, output_dir: Path = None) -> List[Path]:
        """Download results from a completed task."""
        output_dir = output_dir or OUTPUT_DIR / 'appeears'
        output_dir.mkdir(parents=True, exist_ok=True)

        downloaded = []
        try:
            # Get bundle info
            response = self.session.get(f"{self.BASE_URL}/bundle/{task_id}")
            if response.status_code != 200:
                logger.error(f"Failed to get bundle: {response.status_code}")
                return []

            bundle = response.json()
            files = bundle.get('files', [])

            for file_info in files:
                file_id = file_info.get('file_id')
                file_name = file_info.get('file_name')

                # Download file
                dl_response = self.session.get(
                    f"{self.BASE_URL}/bundle/{task_id}/{file_id}",
                    allow_redirects=True
                )
                if dl_response.status_code == 200:
                    output_path = output_dir / file_name
                    output_path.write_bytes(dl_response.content)
                    downloaded.append(output_path)
                    logger.info(f"Downloaded: {file_name}")

        except Exception as e:
            logger.error(f"Error downloading results: {e}")

        return downloaded


def main():
    parser = argparse.ArgumentParser(description='NDVI Data Collector')
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Charts command
    charts_parser = subparsers.add_parser('charts', help='Download NDVI charts')
    charts_parser.add_argument('--commodity', '-c', type=str, help='Specific commodity')
    charts_parser.add_argument('--countries', nargs='+', help='Country codes (e.g., US BR AR)')
    charts_parser.add_argument('--types', nargs='+', default=['ndvi', 'ndvi_anomaly'],
                              help='Chart types (ndvi, ndvi_anomaly, precipitation)')
    charts_parser.add_argument('--years', nargs='+', type=int,
                              default=[datetime.now().year],
                              help='Years to download')
    charts_parser.add_argument('--all', action='store_true', help='Download all key regions')
    charts_parser.add_argument('--force', '-f', action='store_true', help='Force re-download')
    charts_parser.add_argument('--save-db', action='store_true', help='Save to database')

    # Inventory command
    inv_parser = subparsers.add_parser('inventory', help='Show chart inventory')

    # Regions command
    reg_parser = subparsers.add_parser('regions', help='List key crop regions')

    args = parser.parse_args()

    collector = NDVICollector()

    if args.command == 'charts':
        if args.all:
            results = collector.download_all_charts(
                chart_types=args.types,
                years=args.years,
                force=args.force
            )
            all_charts = [c for charts in results.values() for c in charts]
            print(f"\nTotal charts downloaded: {len(all_charts)}")

            if args.save_db:
                collector.save_to_database(all_charts)

        elif args.countries:
            all_charts = []
            commodities = [args.commodity] if args.commodity else None
            for country in args.countries:
                charts = collector.download_region_charts(
                    region_code=country,
                    commodities=commodities,
                    chart_types=args.types,
                    years=args.years,
                    force=args.force
                )
                all_charts.extend(charts)

            print(f"\nTotal charts downloaded: {len(all_charts)}")

            if args.save_db:
                collector.save_to_database(all_charts)
        else:
            parser.print_help()

    elif args.command == 'inventory':
        inventory = collector.generate_chart_inventory()
        print(json.dumps(inventory, indent=2))

    elif args.command == 'regions':
        print("\nKey Crop Regions for NDVI Monitoring:")
        print("=" * 60)
        for region_id, info in CROP_REGIONS.items():
            print(f"\n{region_id}: {info['name']}")
            print(f"  Commodities: {', '.join(info['commodities'])}")
            print(f"  Countries: {', '.join(info['countries'])}")
            print(f"  Critical months: {info['critical_months']}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
