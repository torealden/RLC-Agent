"""
GFS Weather Forecast Collector for Crop Yield Prediction

Collects and processes NOAA GFS (Global Forecast System) data:
- Source: AWS Open Data (primary) or NOAA NOMADS (fallback)
- Format: GRIB2 → aggregated crop region metrics
- Output: PostgreSQL silver.weather_forecast_daily

Architecture follows: GRIB2 → Filter → Aggregate → PostgreSQL → JSON for LLMs
Key principle: Store derived agronomic metrics, not raw meteorology

Dependencies:
    pip install xarray cfgrib eccodes boto3 numpy

Usage:
    python gfs_forecast_collector.py collect --date 2026-01-30
    python gfs_forecast_collector.py collect --latest
    python gfs_forecast_collector.py status
"""

import os
import sys
import json
import logging
import argparse
import tempfile
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import numpy as np

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('GFSCollector')

# =============================================================================
# Constants
# =============================================================================

# AWS Open Data bucket for NOAA GFS
AWS_GFS_BUCKET = "noaa-gfs-bdp-pds"
AWS_REGION = "us-east-1"

# NOAA NOMADS as fallback
NOMADS_BASE_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"

# GFS variables needed for crop analysis
GFS_VARIABLES = {
    'TMP_2m': {'grib_name': 't2m', 'level': '2 m above ground', 'unit': 'K'},
    'TMAX_2m': {'grib_name': 'tmax', 'level': '2 m above ground', 'unit': 'K'},
    'TMIN_2m': {'grib_name': 'tmin', 'level': '2 m above ground', 'unit': 'K'},
    'APCP_sfc': {'grib_name': 'tp', 'level': 'surface', 'unit': 'kg m-2'},  # Total precip
    'RH_2m': {'grib_name': 'r2', 'level': '2 m above ground', 'unit': '%'},
    'UGRD_10m': {'grib_name': 'u10', 'level': '10 m above ground', 'unit': 'm s-1'},
    'VGRD_10m': {'grib_name': 'v10', 'level': '10 m above ground', 'unit': 'm s-1'},
    'DSWRF_sfc': {'grib_name': 'ssrd', 'level': 'surface', 'unit': 'W m-2'},  # Solar
}

# Crop regions with bounding boxes for extraction
# Format: (min_lat, max_lat, min_lon, max_lon)
CROP_REGION_BOUNDS = {
    'US_CORN_BELT': {'bounds': (38, 48, -98, -82), 'crop': 'corn'},
    'US_SOY_BELT': {'bounds': (36, 48, -98, -82), 'crop': 'soybeans'},
    'US_WHEAT_WINTER': {'bounds': (34, 42, -104, -96), 'crop': 'wheat'},
    'US_WHEAT_SPRING': {'bounds': (44, 49, -104, -96), 'crop': 'wheat'},
    'BR_MATO_GROSSO': {'bounds': (-18, -8, -62, -50), 'crop': 'soybeans'},
    'BR_PARANA': {'bounds': (-26, -22, -54, -48), 'crop': 'soybeans'},
    'BR_RIO_GRANDE': {'bounds': (-32, -28, -56, -50), 'crop': 'soybeans'},
    'AR_PAMPAS': {'bounds': (-38, -30, -64, -58), 'crop': 'soybeans'},
    'UA_CENTRAL': {'bounds': (48, 52, 30, 38), 'crop': 'wheat'},
    'RU_SOUTHERN': {'bounds': (44, 50, 38, 48), 'crop': 'wheat'},
    'AU_EASTERN': {'bounds': (-36, -26, 144, 152), 'crop': 'wheat'},
    'IN_PUNJAB': {'bounds': (28, 32, 74, 78), 'crop': 'wheat'},
    'CN_NORTHEAST': {'bounds': (40, 50, 120, 132), 'crop': 'corn'},
    'EU_FRANCE': {'bounds': (44, 50, -2, 8), 'crop': 'wheat'},
}

# Forecast hours to collect (0-384 for GFS, but we focus on 0-16 days)
FORECAST_HOURS = list(range(0, 385, 3))[:129]  # Every 3 hours up to 16 days


@dataclass
class RegionForecast:
    """Aggregated forecast for a crop region."""
    region_code: str
    forecast_date: date
    valid_date: date
    model: str = 'GFS'

    # Core metrics
    precip_mm: float = 0.0
    tmin_c: float = 0.0
    tmax_c: float = 0.0
    tavg_c: float = 0.0

    # Derived agronomic metrics
    gdd_base10: float = 0.0
    gdd_corn: float = 0.0  # With 30°C cap
    heat_stress_hours: int = 0
    extreme_heat_hours: int = 0
    frost_risk: bool = False

    # Moisture
    consecutive_dry_days: int = 0
    excess_moisture: bool = False

    # Additional
    rh_pct: float = 0.0
    wind_ms: float = 0.0
    solar_mj: float = 0.0

    # Quality
    coverage_pct: float = 100.0


class GFSCollector:
    """
    Collects GFS forecast data and aggregates to crop regions.

    Data flow:
    1. Download GRIB2 from AWS/NOMADS
    2. Filter to needed variables and regions
    3. Aggregate to crop region means
    4. Calculate agronomic metrics (GDD, stress)
    5. Store in PostgreSQL
    """

    def __init__(self, use_aws: bool = True):
        self.use_aws = use_aws
        self.temp_dir = Path(tempfile.gettempdir()) / "gfs_cache"
        self.temp_dir.mkdir(exist_ok=True)

        # Check for required libraries
        self._check_dependencies()

    def _check_dependencies(self):
        """Check if required libraries are available."""
        self.has_xarray = False
        self.has_cfgrib = False
        self.has_boto3 = False

        try:
            import xarray
            self.has_xarray = True
        except ImportError:
            logger.warning("xarray not installed - full GRIB processing unavailable")

        try:
            import cfgrib
            self.has_cfgrib = True
        except ImportError:
            logger.warning("cfgrib not installed - GRIB decoding unavailable")

        try:
            import boto3
            self.has_boto3 = True
        except ImportError:
            logger.warning("boto3 not installed - AWS access unavailable")

    def get_gfs_file_url(
        self,
        run_date: date,
        run_hour: int = 0,
        forecast_hour: int = 0,
        use_aws: bool = True
    ) -> str:
        """Generate URL for GFS GRIB2 file."""
        date_str = run_date.strftime('%Y%m%d')

        if use_aws:
            # AWS Open Data path
            return f"s3://{AWS_GFS_BUCKET}/gfs.{date_str}/{run_hour:02d}/atmos/gfs.t{run_hour:02d}z.pgrb2.0p25.f{forecast_hour:03d}"
        else:
            # NOMADS path
            return f"{NOMADS_BASE_URL}/gfs.{date_str}/{run_hour:02d}/atmos/gfs.t{run_hour:02d}z.pgrb2.0p25.f{forecast_hour:03d}"

    def calculate_gdd(
        self,
        tmin_c: float,
        tmax_c: float,
        base_temp: float = 10.0,
        cap_temp: float = None
    ) -> float:
        """
        Calculate Growing Degree Days.

        For corn: base=10°C, cap=30°C
        For wheat: base=0°C or 4.4°C depending on growth stage
        """
        if cap_temp:
            tmax_c = min(tmax_c, cap_temp)

        tavg = (tmin_c + tmax_c) / 2
        gdd = max(0, tavg - base_temp)
        return round(gdd, 2)

    def calculate_stress_metrics(
        self,
        hourly_temps: List[float]
    ) -> Tuple[int, int, bool]:
        """
        Calculate heat stress and frost metrics from hourly temps.

        Returns: (heat_stress_hours, extreme_heat_hours, frost_risk)
        """
        heat_stress = sum(1 for t in hourly_temps if t > 30)
        extreme_heat = sum(1 for t in hourly_temps if t > 35)
        frost = any(t < 0 for t in hourly_temps)
        return heat_stress, extreme_heat, frost

    def aggregate_to_region(
        self,
        data: dict,
        region_code: str,
        forecast_date: date,
        valid_date: date
    ) -> RegionForecast:
        """
        Aggregate gridded forecast data to a crop region.

        This is where the 1000x data reduction happens.
        """
        bounds = CROP_REGION_BOUNDS.get(region_code, {}).get('bounds')
        if not bounds:
            logger.warning(f"No bounds defined for region: {region_code}")
            return None

        forecast = RegionForecast(
            region_code=region_code,
            forecast_date=forecast_date,
            valid_date=valid_date
        )

        # Extract regional means from data
        # In real implementation, this filters the xarray dataset by bounds
        # For now, using placeholder structure
        if 'tmin' in data:
            forecast.tmin_c = data['tmin']
        if 'tmax' in data:
            forecast.tmax_c = data['tmax']
        if 'precip' in data:
            forecast.precip_mm = data['precip']

        # Calculate derived metrics
        forecast.tavg_c = (forecast.tmin_c + forecast.tmax_c) / 2
        forecast.gdd_base10 = self.calculate_gdd(forecast.tmin_c, forecast.tmax_c, 10)
        forecast.gdd_corn = self.calculate_gdd(forecast.tmin_c, forecast.tmax_c, 10, 30)

        # Stress indicators
        forecast.frost_risk = forecast.tmin_c < 0
        forecast.heat_stress_hours = 1 if forecast.tmax_c > 30 else 0
        forecast.extreme_heat_hours = 1 if forecast.tmax_c > 35 else 0
        forecast.excess_moisture = forecast.precip_mm > 25

        return forecast

    def collect_forecast(
        self,
        run_date: date = None,
        regions: List[str] = None,
        max_lead_days: int = 16
    ) -> List[RegionForecast]:
        """
        Collect GFS forecast and aggregate to regions.

        This is a simplified version that works without full GRIB processing.
        For production, integrate xarray + cfgrib.
        """
        run_date = run_date or date.today()
        regions = regions or list(CROP_REGION_BOUNDS.keys())

        logger.info(f"Collecting GFS forecast for {run_date}")
        logger.info(f"Regions: {len(regions)}, Max lead days: {max_lead_days}")

        forecasts = []

        # For each day in forecast horizon
        for lead_day in range(1, max_lead_days + 1):
            valid = run_date + timedelta(days=lead_day)

            for region_code in regions:
                # In production: download GRIB, filter, aggregate
                # Placeholder with realistic structure
                forecast = RegionForecast(
                    region_code=region_code,
                    forecast_date=run_date,
                    valid_date=valid
                )
                forecasts.append(forecast)

        logger.info(f"Generated {len(forecasts)} forecast records")
        return forecasts

    def fetch_from_aws(
        self,
        run_date: date,
        run_hour: int = 0,
        variables: List[str] = None
    ) -> Optional[Path]:
        """
        Fetch GFS data from AWS Open Data.

        Returns path to downloaded/cached file.
        """
        if not self.has_boto3:
            logger.error("boto3 required for AWS access")
            return None

        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config

        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

        # Download subset of forecast hours
        date_str = run_date.strftime('%Y%m%d')
        prefix = f"gfs.{date_str}/{run_hour:02d}/atmos/"

        try:
            # List available files
            response = s3.list_objects_v2(
                Bucket=AWS_GFS_BUCKET,
                Prefix=prefix,
                MaxKeys=100
            )

            if 'Contents' not in response:
                logger.warning(f"No GFS data found for {run_date}")
                return None

            # Filter to 0.25 degree files
            files = [
                obj['Key'] for obj in response['Contents']
                if 'pgrb2.0p25' in obj['Key']
            ]

            logger.info(f"Found {len(files)} GFS files for {run_date}")
            return files

        except Exception as e:
            logger.error(f"AWS fetch error: {e}")
            return None

    def save_to_database(
        self,
        forecasts: List[RegionForecast],
        conn=None
    ) -> int:
        """Save forecast data to PostgreSQL."""
        if not forecasts:
            return 0

        should_close = False
        if conn is None:
            try:
                import psycopg2
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
            for f in forecasts:
                cur.execute("""
                    INSERT INTO silver.weather_forecast_daily (
                        forecast_date, valid_date, model, region_code,
                        precip_mm, tmin_c, tmax_c, tavg_c,
                        gdd_base10, gdd_corn,
                        heat_stress_hours, extreme_heat_hours, frost_risk,
                        consecutive_dry_days, excess_moisture_flag,
                        coverage_pct
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (model, forecast_date, model_run_hour, valid_date, region_code)
                    DO UPDATE SET
                        precip_mm = EXCLUDED.precip_mm,
                        tmin_c = EXCLUDED.tmin_c,
                        tmax_c = EXCLUDED.tmax_c,
                        tavg_c = EXCLUDED.tavg_c,
                        gdd_base10 = EXCLUDED.gdd_base10,
                        gdd_corn = EXCLUDED.gdd_corn,
                        heat_stress_hours = EXCLUDED.heat_stress_hours,
                        extreme_heat_hours = EXCLUDED.extreme_heat_hours,
                        frost_risk = EXCLUDED.frost_risk,
                        created_at = NOW()
                """, (
                    f.forecast_date,
                    f.valid_date,
                    f.model,
                    f.region_code,
                    f.precip_mm,
                    f.tmin_c,
                    f.tmax_c,
                    f.tavg_c,
                    f.gdd_base10,
                    f.gdd_corn,
                    f.heat_stress_hours,
                    f.extreme_heat_hours,
                    f.frost_risk,
                    f.consecutive_dry_days,
                    f.excess_moisture,
                    f.coverage_pct
                ))
                count += 1

            conn.commit()
            logger.info(f"Saved {count} forecast records to database")

        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")

        finally:
            if should_close:
                conn.close()

        return count


def check_aws_availability():
    """Check if AWS GFS data is accessible."""
    try:
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config

        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

        # Check latest date
        today = date.today()
        for days_back in range(3):
            check_date = today - timedelta(days=days_back)
            date_str = check_date.strftime('%Y%m%d')

            response = s3.list_objects_v2(
                Bucket=AWS_GFS_BUCKET,
                Prefix=f"gfs.{date_str}/00/atmos/",
                MaxKeys=5
            )

            if 'Contents' in response and len(response['Contents']) > 0:
                print(f"GFS data available for: {check_date}")
                print(f"  Files found: {len(response.get('Contents', []))}")
                return True

        print("No recent GFS data found on AWS")
        return False

    except ImportError:
        print("boto3 not installed - run: pip install boto3")
        return False
    except Exception as e:
        print(f"AWS check error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='GFS Weather Forecast Collector')
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Collect command
    collect_parser = subparsers.add_parser('collect', help='Collect GFS forecast')
    collect_parser.add_argument('--date', '-d', type=str, help='Forecast date (YYYY-MM-DD)')
    collect_parser.add_argument('--latest', action='store_true', help='Collect latest available')
    collect_parser.add_argument('--regions', nargs='+', help='Specific regions to collect')
    collect_parser.add_argument('--days', type=int, default=16, help='Forecast horizon (days)')
    collect_parser.add_argument('--save-db', action='store_true', help='Save to database')

    # Status command
    status_parser = subparsers.add_parser('status', help='Check data availability')

    # Regions command
    regions_parser = subparsers.add_parser('regions', help='List crop regions')

    args = parser.parse_args()

    if args.command == 'collect':
        collector = GFSCollector()

        if args.date:
            run_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        else:
            run_date = date.today()

        forecasts = collector.collect_forecast(
            run_date=run_date,
            regions=args.regions,
            max_lead_days=args.days
        )

        print(f"\nCollected {len(forecasts)} forecast records")

        if args.save_db:
            collector.save_to_database(forecasts)

    elif args.command == 'status':
        print("\nChecking GFS data availability...")
        print("=" * 50)
        check_aws_availability()

    elif args.command == 'regions':
        print("\nCrop Regions for Weather Aggregation:")
        print("=" * 50)
        for code, info in CROP_REGION_BOUNDS.items():
            bounds = info['bounds']
            print(f"\n{code}")
            print(f"  Crop: {info['crop']}")
            print(f"  Bounds: Lat {bounds[0]}°-{bounds[1]}°, Lon {bounds[2]}°-{bounds[3]}°")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
