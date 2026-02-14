"""
GEFS Ensemble Forecast Collector for Yield Risk Assessment

GEFS (Global Ensemble Forecast System) provides 31 ensemble members:
- 1 control run + 30 perturbed members
- Captures forecast uncertainty and tail risks
- Critical for yield risk modeling (P10/P50/P90 scenarios)

Source: AWS Open Data (noaa-gefs-pds bucket)
Resolution: ~50km, 0-16 days

Usage:
    python gefs_ensemble_collector.py collect --date 2026-01-30
    python gefs_ensemble_collector.py collect --latest --save-db
    python gefs_ensemble_collector.py status
"""

import os
import sys
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
logger = logging.getLogger('GEFSCollector')

# =============================================================================
# Constants
# =============================================================================

AWS_GEFS_BUCKET = "noaa-gefs-pds"
AWS_REGION = "us-east-1"

# GEFS has 31 members: gec00 (control) + gep01-gep30 (perturbed)
ENSEMBLE_MEMBERS = ['gec00'] + [f'gep{i:02d}' for i in range(1, 31)]

# Import crop regions from GFS collector
from gfs_forecast_collector import CROP_REGION_BOUNDS, GFSCollector


@dataclass
class EnsembleForecast:
    """Ensemble forecast statistics for a crop region."""
    region_code: str
    forecast_date: date
    valid_date: date
    model: str = 'GEFS'
    ensemble_members: int = 31

    # Precipitation percentiles
    precip_p10: float = 0.0
    precip_p25: float = 0.0
    precip_p50: float = 0.0
    precip_p75: float = 0.0
    precip_p90: float = 0.0
    precip_mean: float = 0.0
    precip_std: float = 0.0

    # Temperature percentiles
    tmax_p10: float = 0.0
    tmax_p50: float = 0.0
    tmax_p90: float = 0.0
    tmin_p10: float = 0.0
    tmin_p50: float = 0.0
    tmin_p90: float = 0.0

    # GDD percentiles
    gdd_p10: float = 0.0
    gdd_p50: float = 0.0
    gdd_p90: float = 0.0

    # Risk metrics
    prob_heat_stress: float = 0.0      # % members with tmax > 30
    prob_extreme_heat: float = 0.0     # % members with tmax > 35
    prob_frost: float = 0.0            # % members with tmin < 0
    prob_dry_week: float = 0.0         # % members with < 5mm/week
    prob_wet_week: float = 0.0         # % members with > 50mm/week

    # Spread (uncertainty indicator)
    precip_spread: float = 0.0         # P90 - P10
    temp_spread: float = 0.0


class GEFSCollector(GFSCollector):
    """
    Collects GEFS ensemble forecasts and calculates probabilistic metrics.

    Extends GFSCollector with ensemble-specific functionality.
    """

    def __init__(self):
        super().__init__(use_aws=True)
        self.ensemble_members = ENSEMBLE_MEMBERS

    def fetch_ensemble_from_aws(
        self,
        run_date: date,
        run_hour: int = 0,
        members: List[str] = None
    ) -> Dict[str, List[str]]:
        """
        Fetch GEFS ensemble data from AWS.

        Returns dict mapping member ID to list of file paths.
        """
        if not self.has_boto3:
            logger.error("boto3 required for AWS access")
            return {}

        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config

        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        members = members or self.ensemble_members[:5]  # Default to subset for testing

        date_str = run_date.strftime('%Y%m%d')
        member_files = {}

        for member in members:
            prefix = f"gefs.{date_str}/{run_hour:02d}/atmos/pgrb2ap5/{member}"

            try:
                response = s3.list_objects_v2(
                    Bucket=AWS_GEFS_BUCKET,
                    Prefix=prefix,
                    MaxKeys=50
                )

                if 'Contents' in response:
                    files = [obj['Key'] for obj in response['Contents']]
                    member_files[member] = files
                    logger.debug(f"Found {len(files)} files for {member}")

            except Exception as e:
                logger.warning(f"Error fetching {member}: {e}")

        return member_files

    def calculate_ensemble_stats(
        self,
        member_values: List[float]
    ) -> Dict[str, float]:
        """Calculate percentile statistics from ensemble members."""
        if not member_values:
            return {}

        arr = np.array(member_values)
        return {
            'p10': float(np.percentile(arr, 10)),
            'p25': float(np.percentile(arr, 25)),
            'p50': float(np.percentile(arr, 50)),
            'p75': float(np.percentile(arr, 75)),
            'p90': float(np.percentile(arr, 90)),
            'mean': float(np.mean(arr)),
            'std': float(np.std(arr)),
        }

    def calculate_probability(
        self,
        member_values: List[float],
        threshold: float,
        above: bool = True
    ) -> float:
        """Calculate probability of exceeding (or falling below) threshold."""
        if not member_values:
            return 0.0

        if above:
            count = sum(1 for v in member_values if v > threshold)
        else:
            count = sum(1 for v in member_values if v < threshold)

        return round(count / len(member_values) * 100, 1)

    def aggregate_ensemble_to_region(
        self,
        member_data: Dict[str, dict],
        region_code: str,
        forecast_date: date,
        valid_date: date
    ) -> EnsembleForecast:
        """
        Aggregate ensemble member forecasts to probabilistic regional metrics.

        This is where ensemble spread becomes yield risk information.
        """
        forecast = EnsembleForecast(
            region_code=region_code,
            forecast_date=forecast_date,
            valid_date=valid_date,
            ensemble_members=len(member_data)
        )

        # Collect values across members
        precip_values = []
        tmax_values = []
        tmin_values = []
        gdd_values = []

        for member_id, data in member_data.items():
            if 'precip' in data:
                precip_values.append(data['precip'])
            if 'tmax' in data:
                tmax_values.append(data['tmax'])
            if 'tmin' in data:
                tmin_values.append(data['tmin'])
            if 'gdd' in data:
                gdd_values.append(data['gdd'])

        # Calculate precipitation statistics
        if precip_values:
            stats = self.calculate_ensemble_stats(precip_values)
            forecast.precip_p10 = stats['p10']
            forecast.precip_p25 = stats['p25']
            forecast.precip_p50 = stats['p50']
            forecast.precip_p75 = stats['p75']
            forecast.precip_p90 = stats['p90']
            forecast.precip_mean = stats['mean']
            forecast.precip_std = stats['std']
            forecast.precip_spread = stats['p90'] - stats['p10']

        # Calculate temperature statistics
        if tmax_values:
            stats = self.calculate_ensemble_stats(tmax_values)
            forecast.tmax_p10 = stats['p10']
            forecast.tmax_p50 = stats['p50']
            forecast.tmax_p90 = stats['p90']

            # Heat stress probabilities
            forecast.prob_heat_stress = self.calculate_probability(tmax_values, 30)
            forecast.prob_extreme_heat = self.calculate_probability(tmax_values, 35)

        if tmin_values:
            stats = self.calculate_ensemble_stats(tmin_values)
            forecast.tmin_p10 = stats['p10']
            forecast.tmin_p50 = stats['p50']
            forecast.tmin_p90 = stats['p90']

            # Frost probability
            forecast.prob_frost = self.calculate_probability(tmin_values, 0, above=False)

            # Temperature spread
            if tmax_values:
                forecast.temp_spread = forecast.tmax_p90 - forecast.tmin_p10

        # GDD statistics
        if gdd_values:
            stats = self.calculate_ensemble_stats(gdd_values)
            forecast.gdd_p10 = stats['p10']
            forecast.gdd_p50 = stats['p50']
            forecast.gdd_p90 = stats['p90']

        return forecast

    def collect_ensemble_forecast(
        self,
        run_date: date = None,
        regions: List[str] = None,
        max_lead_days: int = 16,
        n_members: int = 10  # Subset for efficiency
    ) -> List[EnsembleForecast]:
        """
        Collect GEFS ensemble forecast and calculate probabilistic metrics.
        """
        run_date = run_date or date.today()
        regions = regions or list(CROP_REGION_BOUNDS.keys())

        logger.info(f"Collecting GEFS ensemble for {run_date}")
        logger.info(f"Members: {n_members}, Regions: {len(regions)}")

        # Check data availability
        member_files = self.fetch_ensemble_from_aws(
            run_date,
            members=ENSEMBLE_MEMBERS[:n_members]
        )

        if not member_files:
            logger.warning("No GEFS data available")
            return []

        logger.info(f"Found data for {len(member_files)} ensemble members")

        forecasts = []

        # For each forecast day
        for lead_day in range(1, max_lead_days + 1):
            valid = run_date + timedelta(days=lead_day)

            for region_code in regions:
                # In production: process GRIB files for each member
                # Generate placeholder with realistic structure
                member_data = {}
                for member in list(member_files.keys()):
                    # Simulated member data - replace with actual GRIB processing
                    member_data[member] = {
                        'precip': np.random.uniform(0, 20),
                        'tmax': np.random.uniform(20, 35),
                        'tmin': np.random.uniform(10, 20),
                        'gdd': np.random.uniform(5, 15),
                    }

                forecast = self.aggregate_ensemble_to_region(
                    member_data, region_code, run_date, valid
                )
                forecasts.append(forecast)

        logger.info(f"Generated {len(forecasts)} ensemble forecast records")
        return forecasts

    def save_ensemble_to_database(
        self,
        forecasts: List[EnsembleForecast],
        conn=None
    ) -> int:
        """Save ensemble forecast data to PostgreSQL."""
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
                        ensemble_members,
                        precip_p10, precip_p50, precip_p90,
                        temp_p10, temp_p50, temp_p90
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s,
                        %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (model, forecast_date, model_run_hour, valid_date, region_code)
                    DO UPDATE SET
                        ensemble_members = EXCLUDED.ensemble_members,
                        precip_p10 = EXCLUDED.precip_p10,
                        precip_p50 = EXCLUDED.precip_p50,
                        precip_p90 = EXCLUDED.precip_p90,
                        temp_p10 = EXCLUDED.temp_p10,
                        temp_p50 = EXCLUDED.temp_p50,
                        temp_p90 = EXCLUDED.temp_p90,
                        created_at = NOW()
                """, (
                    f.forecast_date,
                    f.valid_date,
                    f.model,
                    f.region_code,
                    f.precip_p50,  # Use median as point estimate
                    f.tmin_p50,
                    f.tmax_p50,
                    (f.tmin_p50 + f.tmax_p50) / 2,
                    f.ensemble_members,
                    f.precip_p10,
                    f.precip_p50,
                    f.precip_p90,
                    f.tmin_p10,
                    (f.tmin_p50 + f.tmax_p50) / 2,
                    f.tmax_p90
                ))
                count += 1

            conn.commit()
            logger.info(f"Saved {count} ensemble records to database")

        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")

        finally:
            if should_close:
                conn.close()

        return count


def check_gefs_availability():
    """Check GEFS data availability on AWS."""
    try:
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config

        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

        today = date.today()
        for days_back in range(3):
            check_date = today - timedelta(days=days_back)
            date_str = check_date.strftime('%Y%m%d')

            # Check control run
            response = s3.list_objects_v2(
                Bucket=AWS_GEFS_BUCKET,
                Prefix=f"gefs.{date_str}/00/atmos/pgrb2ap5/gec00",
                MaxKeys=5
            )

            if 'Contents' in response and len(response['Contents']) > 0:
                print(f"GEFS data available for: {check_date}")

                # Count members
                for member in ['gec00', 'gep01', 'gep10', 'gep20', 'gep30']:
                    resp = s3.list_objects_v2(
                        Bucket=AWS_GEFS_BUCKET,
                        Prefix=f"gefs.{date_str}/00/atmos/pgrb2ap5/{member}",
                        MaxKeys=1
                    )
                    status = "✓" if 'Contents' in resp else "✗"
                    print(f"  {member}: {status}")

                return True

        print("No recent GEFS data found")
        return False

    except ImportError:
        print("boto3 not installed")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='GEFS Ensemble Forecast Collector')
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Collect command
    collect_parser = subparsers.add_parser('collect', help='Collect GEFS ensemble')
    collect_parser.add_argument('--date', '-d', type=str, help='Forecast date')
    collect_parser.add_argument('--latest', action='store_true', help='Latest available')
    collect_parser.add_argument('--members', type=int, default=10, help='Number of members')
    collect_parser.add_argument('--regions', nargs='+', help='Specific regions')
    collect_parser.add_argument('--days', type=int, default=16, help='Forecast horizon')
    collect_parser.add_argument('--save-db', action='store_true', help='Save to database')

    # Status command
    status_parser = subparsers.add_parser('status', help='Check availability')

    args = parser.parse_args()

    if args.command == 'collect':
        collector = GEFSCollector()

        if args.date:
            run_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        else:
            run_date = date.today()

        forecasts = collector.collect_ensemble_forecast(
            run_date=run_date,
            regions=args.regions,
            max_lead_days=args.days,
            n_members=args.members
        )

        print(f"\nCollected {len(forecasts)} ensemble forecast records")

        # Show sample
        if forecasts:
            f = forecasts[0]
            print(f"\nSample ({f.region_code}, {f.valid_date}):")
            print(f"  Precip P10/P50/P90: {f.precip_p10:.1f}/{f.precip_p50:.1f}/{f.precip_p90:.1f} mm")
            print(f"  Tmax P10/P50/P90: {f.tmax_p10:.1f}/{f.tmax_p50:.1f}/{f.tmax_p90:.1f} °C")
            print(f"  Heat stress prob: {f.prob_heat_stress}%")
            print(f"  Frost prob: {f.prob_frost}%")

        if args.save_db:
            collector.save_ensemble_to_database(forecasts)

    elif args.command == 'status':
        print("\nChecking GEFS ensemble availability...")
        print("=" * 50)
        check_gefs_availability()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
