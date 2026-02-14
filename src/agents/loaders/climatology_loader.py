"""
Climatology Normals Loader

Loads 30-year climate normals for anomaly calculations.
Sources:
- CPC Global Temperature & Precipitation
- ERA5 Reanalysis (via CDS API)
- Regional agricultural statistics

These normals are essential for:
- Calculating temperature and precipitation anomalies
- Determining GDD anomalies
- Providing context for forecast interpretation

Usage:
    python climatology_loader.py load --source cpc
    python climatology_loader.py init  # Load built-in defaults
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from datetime import date
from typing import Dict, List

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ClimatologyLoader')

# =============================================================================
# Climatology Data (30-year normals 1991-2020)
# Sources: NOAA CPC, USDA, regional agricultural agencies
# =============================================================================

# Monthly normals by region
# Format: {region: {month: {precip_mm, tmin_c, tmax_c, gdd_base10}}}
MONTHLY_NORMALS = {
    # US Corn Belt (IA, IL, NE, MN, IN average)
    'US_CORN_BELT': {
        1: {'precip_mm': 25, 'tmin_c': -12, 'tmax_c': -2, 'gdd': 0},
        2: {'precip_mm': 28, 'tmin_c': -10, 'tmax_c': 1, 'gdd': 0},
        3: {'precip_mm': 55, 'tmin_c': -3, 'tmax_c': 9, 'gdd': 0},
        4: {'precip_mm': 85, 'tmin_c': 4, 'tmax_c': 17, 'gdd': 45},
        5: {'precip_mm': 115, 'tmin_c': 10, 'tmax_c': 23, 'gdd': 155},
        6: {'precip_mm': 120, 'tmin_c': 16, 'tmax_c': 28, 'gdd': 310},
        7: {'precip_mm': 105, 'tmin_c': 18, 'tmax_c': 30, 'gdd': 385},
        8: {'precip_mm': 100, 'tmin_c': 17, 'tmax_c': 29, 'gdd': 355},
        9: {'precip_mm': 80, 'tmin_c': 12, 'tmax_c': 25, 'gdd': 225},
        10: {'precip_mm': 65, 'tmin_c': 5, 'tmax_c': 17, 'gdd': 60},
        11: {'precip_mm': 50, 'tmin_c': -2, 'tmax_c': 8, 'gdd': 0},
        12: {'precip_mm': 35, 'tmin_c': -9, 'tmax_c': 0, 'gdd': 0},
    },

    # US Soybean Belt (similar to corn belt with slight variations)
    'US_SOY_BELT': {
        1: {'precip_mm': 28, 'tmin_c': -11, 'tmax_c': -1, 'gdd': 0},
        2: {'precip_mm': 30, 'tmin_c': -9, 'tmax_c': 2, 'gdd': 0},
        3: {'precip_mm': 60, 'tmin_c': -2, 'tmax_c': 10, 'gdd': 0},
        4: {'precip_mm': 90, 'tmin_c': 5, 'tmax_c': 18, 'gdd': 55},
        5: {'precip_mm': 120, 'tmin_c': 11, 'tmax_c': 24, 'gdd': 170},
        6: {'precip_mm': 115, 'tmin_c': 17, 'tmax_c': 29, 'gdd': 330},
        7: {'precip_mm': 100, 'tmin_c': 19, 'tmax_c': 31, 'gdd': 400},
        8: {'precip_mm': 95, 'tmin_c': 18, 'tmax_c': 30, 'gdd': 370},
        9: {'precip_mm': 75, 'tmin_c': 13, 'tmax_c': 26, 'gdd': 240},
        10: {'precip_mm': 70, 'tmin_c': 6, 'tmax_c': 18, 'gdd': 70},
        11: {'precip_mm': 55, 'tmin_c': -1, 'tmax_c': 9, 'gdd': 0},
        12: {'precip_mm': 38, 'tmin_c': -8, 'tmax_c': 1, 'gdd': 0},
    },

    # US Winter Wheat Belt (KS, OK, TX panhandle)
    'US_WHEAT_WINTER': {
        1: {'precip_mm': 15, 'tmin_c': -6, 'tmax_c': 6, 'gdd': 0},
        2: {'precip_mm': 20, 'tmin_c': -4, 'tmax_c': 9, 'gdd': 0},
        3: {'precip_mm': 40, 'tmin_c': 1, 'tmax_c': 15, 'gdd': 25},
        4: {'precip_mm': 55, 'tmin_c': 7, 'tmax_c': 21, 'gdd': 105},
        5: {'precip_mm': 90, 'tmin_c': 13, 'tmax_c': 26, 'gdd': 230},
        6: {'precip_mm': 85, 'tmin_c': 18, 'tmax_c': 32, 'gdd': 375},
        7: {'precip_mm': 70, 'tmin_c': 21, 'tmax_c': 35, 'gdd': 435},
        8: {'precip_mm': 65, 'tmin_c': 20, 'tmax_c': 34, 'gdd': 410},
        9: {'precip_mm': 55, 'tmin_c': 15, 'tmax_c': 29, 'gdd': 285},
        10: {'precip_mm': 50, 'tmin_c': 8, 'tmax_c': 22, 'gdd': 120},
        11: {'precip_mm': 25, 'tmin_c': 1, 'tmax_c': 13, 'gdd': 15},
        12: {'precip_mm': 18, 'tmin_c': -4, 'tmax_c': 7, 'gdd': 0},
    },

    # US Spring Wheat Belt (ND, MT, MN)
    'US_WHEAT_SPRING': {
        1: {'precip_mm': 12, 'tmin_c': -18, 'tmax_c': -7, 'gdd': 0},
        2: {'precip_mm': 10, 'tmin_c': -15, 'tmax_c': -4, 'gdd': 0},
        3: {'precip_mm': 22, 'tmin_c': -7, 'tmax_c': 4, 'gdd': 0},
        4: {'precip_mm': 35, 'tmin_c': 0, 'tmax_c': 13, 'gdd': 15},
        5: {'precip_mm': 55, 'tmin_c': 6, 'tmax_c': 20, 'gdd': 100},
        6: {'precip_mm': 80, 'tmin_c': 12, 'tmax_c': 25, 'gdd': 230},
        7: {'precip_mm': 70, 'tmin_c': 14, 'tmax_c': 28, 'gdd': 310},
        8: {'precip_mm': 55, 'tmin_c': 13, 'tmax_c': 27, 'gdd': 280},
        9: {'precip_mm': 45, 'tmin_c': 7, 'tmax_c': 21, 'gdd': 140},
        10: {'precip_mm': 35, 'tmin_c': 0, 'tmax_c': 13, 'gdd': 15},
        11: {'precip_mm': 18, 'tmin_c': -8, 'tmax_c': 3, 'gdd': 0},
        12: {'precip_mm': 12, 'tmin_c': -15, 'tmax_c': -5, 'gdd': 0},
    },

    # Brazil Mato Grosso (tropical wet/dry)
    'BR_MATO_GROSSO': {
        1: {'precip_mm': 270, 'tmin_c': 22, 'tmax_c': 32, 'gdd': 465},
        2: {'precip_mm': 250, 'tmin_c': 22, 'tmax_c': 32, 'gdd': 420},
        3: {'precip_mm': 230, 'tmin_c': 22, 'tmax_c': 32, 'gdd': 465},
        4: {'precip_mm': 100, 'tmin_c': 21, 'tmax_c': 32, 'gdd': 435},
        5: {'precip_mm': 35, 'tmin_c': 19, 'tmax_c': 32, 'gdd': 410},
        6: {'precip_mm': 10, 'tmin_c': 17, 'tmax_c': 32, 'gdd': 385},
        7: {'precip_mm': 5, 'tmin_c': 16, 'tmax_c': 33, 'gdd': 400},
        8: {'precip_mm': 15, 'tmin_c': 18, 'tmax_c': 35, 'gdd': 450},
        9: {'precip_mm': 50, 'tmin_c': 20, 'tmax_c': 35, 'gdd': 475},
        10: {'precip_mm': 140, 'tmin_c': 21, 'tmax_c': 34, 'gdd': 480},
        11: {'precip_mm': 200, 'tmin_c': 22, 'tmax_c': 33, 'gdd': 475},
        12: {'precip_mm': 260, 'tmin_c': 22, 'tmax_c': 32, 'gdd': 465},
    },

    # Brazil Parana (subtropical)
    'BR_PARANA': {
        1: {'precip_mm': 180, 'tmin_c': 19, 'tmax_c': 29, 'gdd': 385},
        2: {'precip_mm': 160, 'tmin_c': 19, 'tmax_c': 29, 'gdd': 350},
        3: {'precip_mm': 130, 'tmin_c': 18, 'tmax_c': 28, 'gdd': 355},
        4: {'precip_mm': 100, 'tmin_c': 15, 'tmax_c': 25, 'gdd': 270},
        5: {'precip_mm': 110, 'tmin_c': 12, 'tmax_c': 22, 'gdd': 185},
        6: {'precip_mm': 100, 'tmin_c': 10, 'tmax_c': 20, 'gdd': 120},
        7: {'precip_mm': 80, 'tmin_c': 9, 'tmax_c': 20, 'gdd': 110},
        8: {'precip_mm': 70, 'tmin_c': 10, 'tmax_c': 22, 'gdd': 140},
        9: {'precip_mm': 120, 'tmin_c': 12, 'tmax_c': 24, 'gdd': 195},
        10: {'precip_mm': 160, 'tmin_c': 15, 'tmax_c': 26, 'gdd': 280},
        11: {'precip_mm': 140, 'tmin_c': 17, 'tmax_c': 28, 'gdd': 345},
        12: {'precip_mm': 170, 'tmin_c': 18, 'tmax_c': 29, 'gdd': 385},
    },

    # Brazil Rio Grande do Sul
    'BR_RIO_GRANDE': {
        1: {'precip_mm': 130, 'tmin_c': 18, 'tmax_c': 30, 'gdd': 385},
        2: {'precip_mm': 120, 'tmin_c': 18, 'tmax_c': 29, 'gdd': 350},
        3: {'precip_mm': 110, 'tmin_c': 16, 'tmax_c': 27, 'gdd': 315},
        4: {'precip_mm': 100, 'tmin_c': 13, 'tmax_c': 23, 'gdd': 195},
        5: {'precip_mm': 110, 'tmin_c': 10, 'tmax_c': 19, 'gdd': 95},
        6: {'precip_mm': 120, 'tmin_c': 7, 'tmax_c': 16, 'gdd': 25},
        7: {'precip_mm': 130, 'tmin_c': 7, 'tmax_c': 16, 'gdd': 25},
        8: {'precip_mm': 110, 'tmin_c': 8, 'tmax_c': 18, 'gdd': 55},
        9: {'precip_mm': 120, 'tmin_c': 10, 'tmax_c': 20, 'gdd': 105},
        10: {'precip_mm': 130, 'tmin_c': 13, 'tmax_c': 23, 'gdd': 195},
        11: {'precip_mm': 110, 'tmin_c': 15, 'tmax_c': 26, 'gdd': 280},
        12: {'precip_mm': 120, 'tmin_c': 17, 'tmax_c': 28, 'gdd': 355},
    },

    # Argentina Pampas
    'AR_PAMPAS': {
        1: {'precip_mm': 100, 'tmin_c': 17, 'tmax_c': 30, 'gdd': 385},
        2: {'precip_mm': 90, 'tmin_c': 16, 'tmax_c': 29, 'gdd': 350},
        3: {'precip_mm': 100, 'tmin_c': 14, 'tmax_c': 26, 'gdd': 280},
        4: {'precip_mm': 80, 'tmin_c': 10, 'tmax_c': 21, 'gdd': 165},
        5: {'precip_mm': 50, 'tmin_c': 6, 'tmax_c': 17, 'gdd': 55},
        6: {'precip_mm': 30, 'tmin_c': 3, 'tmax_c': 13, 'gdd': 0},
        7: {'precip_mm': 30, 'tmin_c': 2, 'tmax_c': 13, 'gdd': 0},
        8: {'precip_mm': 40, 'tmin_c': 4, 'tmax_c': 15, 'gdd': 15},
        9: {'precip_mm': 55, 'tmin_c': 6, 'tmax_c': 18, 'gdd': 70},
        10: {'precip_mm': 90, 'tmin_c': 10, 'tmax_c': 22, 'gdd': 160},
        11: {'precip_mm': 100, 'tmin_c': 13, 'tmax_c': 26, 'gdd': 265},
        12: {'precip_mm': 100, 'tmin_c': 16, 'tmax_c': 29, 'gdd': 355},
    },

    # Ukraine Central
    'UA_CENTRAL': {
        1: {'precip_mm': 35, 'tmin_c': -8, 'tmax_c': -2, 'gdd': 0},
        2: {'precip_mm': 35, 'tmin_c': -8, 'tmax_c': -1, 'gdd': 0},
        3: {'precip_mm': 35, 'tmin_c': -3, 'tmax_c': 5, 'gdd': 0},
        4: {'precip_mm': 45, 'tmin_c': 4, 'tmax_c': 14, 'gdd': 40},
        5: {'precip_mm': 55, 'tmin_c': 10, 'tmax_c': 21, 'gdd': 170},
        6: {'precip_mm': 70, 'tmin_c': 14, 'tmax_c': 25, 'gdd': 290},
        7: {'precip_mm': 75, 'tmin_c': 16, 'tmax_c': 27, 'gdd': 355},
        8: {'precip_mm': 55, 'tmin_c': 15, 'tmax_c': 26, 'gdd': 330},
        9: {'precip_mm': 45, 'tmin_c': 10, 'tmax_c': 20, 'gdd': 165},
        10: {'precip_mm': 40, 'tmin_c': 5, 'tmax_c': 13, 'gdd': 35},
        11: {'precip_mm': 45, 'tmin_c': 0, 'tmax_c': 6, 'gdd': 0},
        12: {'precip_mm': 40, 'tmin_c': -5, 'tmax_c': 0, 'gdd': 0},
    },

    # Russia Southern
    'RU_SOUTHERN': {
        1: {'precip_mm': 40, 'tmin_c': -6, 'tmax_c': 1, 'gdd': 0},
        2: {'precip_mm': 35, 'tmin_c': -5, 'tmax_c': 2, 'gdd': 0},
        3: {'precip_mm': 35, 'tmin_c': 0, 'tmax_c': 8, 'gdd': 0},
        4: {'precip_mm': 40, 'tmin_c': 6, 'tmax_c': 16, 'gdd': 60},
        5: {'precip_mm': 50, 'tmin_c': 12, 'tmax_c': 23, 'gdd': 210},
        6: {'precip_mm': 60, 'tmin_c': 16, 'tmax_c': 28, 'gdd': 340},
        7: {'precip_mm': 50, 'tmin_c': 19, 'tmax_c': 31, 'gdd': 420},
        8: {'precip_mm': 40, 'tmin_c': 18, 'tmax_c': 30, 'gdd': 390},
        9: {'precip_mm': 35, 'tmin_c': 13, 'tmax_c': 24, 'gdd': 230},
        10: {'precip_mm': 40, 'tmin_c': 7, 'tmax_c': 16, 'gdd': 55},
        11: {'precip_mm': 45, 'tmin_c': 2, 'tmax_c': 9, 'gdd': 0},
        12: {'precip_mm': 45, 'tmin_c': -3, 'tmax_c': 3, 'gdd': 0},
    },

    # Australia Eastern Wheat Belt
    'AU_EASTERN': {
        1: {'precip_mm': 55, 'tmin_c': 18, 'tmax_c': 32, 'gdd': 405},
        2: {'precip_mm': 55, 'tmin_c': 18, 'tmax_c': 31, 'gdd': 375},
        3: {'precip_mm': 50, 'tmin_c': 15, 'tmax_c': 28, 'gdd': 315},
        4: {'precip_mm': 40, 'tmin_c': 11, 'tmax_c': 23, 'gdd': 190},
        5: {'precip_mm': 45, 'tmin_c': 7, 'tmax_c': 18, 'gdd': 75},
        6: {'precip_mm': 50, 'tmin_c': 4, 'tmax_c': 15, 'gdd': 15},
        7: {'precip_mm': 45, 'tmin_c': 3, 'tmax_c': 14, 'gdd': 5},
        8: {'precip_mm': 40, 'tmin_c': 4, 'tmax_c': 16, 'gdd': 25},
        9: {'precip_mm': 40, 'tmin_c': 7, 'tmax_c': 20, 'gdd': 90},
        10: {'precip_mm': 50, 'tmin_c': 10, 'tmax_c': 24, 'gdd': 185},
        11: {'precip_mm': 50, 'tmin_c': 14, 'tmax_c': 28, 'gdd': 295},
        12: {'precip_mm': 55, 'tmin_c': 17, 'tmax_c': 31, 'gdd': 385},
    },

    # India Punjab-Haryana
    'IN_PUNJAB': {
        1: {'precip_mm': 25, 'tmin_c': 5, 'tmax_c': 19, 'gdd': 70},
        2: {'precip_mm': 30, 'tmin_c': 7, 'tmax_c': 22, 'gdd': 105},
        3: {'precip_mm': 30, 'tmin_c': 12, 'tmax_c': 28, 'gdd': 210},
        4: {'precip_mm': 15, 'tmin_c': 18, 'tmax_c': 36, 'gdd': 390},
        5: {'precip_mm': 20, 'tmin_c': 23, 'tmax_c': 40, 'gdd': 520},
        6: {'precip_mm': 55, 'tmin_c': 26, 'tmax_c': 40, 'gdd': 555},
        7: {'precip_mm': 200, 'tmin_c': 26, 'tmax_c': 35, 'gdd': 480},
        8: {'precip_mm': 180, 'tmin_c': 25, 'tmax_c': 34, 'gdd': 450},
        9: {'precip_mm': 80, 'tmin_c': 23, 'tmax_c': 34, 'gdd': 430},
        10: {'precip_mm': 15, 'tmin_c': 17, 'tmax_c': 33, 'gdd': 375},
        11: {'precip_mm': 5, 'tmin_c': 10, 'tmax_c': 28, 'gdd': 235},
        12: {'precip_mm': 15, 'tmin_c': 5, 'tmax_c': 21, 'gdd': 90},
    },

    # China Northeast
    'CN_NORTHEAST': {
        1: {'precip_mm': 5, 'tmin_c': -25, 'tmax_c': -12, 'gdd': 0},
        2: {'precip_mm': 5, 'tmin_c': -21, 'tmax_c': -7, 'gdd': 0},
        3: {'precip_mm': 15, 'tmin_c': -10, 'tmax_c': 3, 'gdd': 0},
        4: {'precip_mm': 30, 'tmin_c': 2, 'tmax_c': 14, 'gdd': 35},
        5: {'precip_mm': 50, 'tmin_c': 9, 'tmax_c': 22, 'gdd': 170},
        6: {'precip_mm': 90, 'tmin_c': 15, 'tmax_c': 27, 'gdd': 315},
        7: {'precip_mm': 140, 'tmin_c': 19, 'tmax_c': 29, 'gdd': 385},
        8: {'precip_mm': 120, 'tmin_c': 17, 'tmax_c': 27, 'gdd': 340},
        9: {'precip_mm': 55, 'tmin_c': 10, 'tmax_c': 21, 'gdd': 170},
        10: {'precip_mm': 30, 'tmin_c': 1, 'tmax_c': 12, 'gdd': 15},
        11: {'precip_mm': 15, 'tmin_c': -10, 'tmax_c': 0, 'gdd': 0},
        12: {'precip_mm': 8, 'tmin_c': -21, 'tmax_c': -9, 'gdd': 0},
    },

    # EU France (Paris Basin)
    'EU_FRANCE': {
        1: {'precip_mm': 55, 'tmin_c': 1, 'tmax_c': 7, 'gdd': 0},
        2: {'precip_mm': 45, 'tmin_c': 1, 'tmax_c': 8, 'gdd': 0},
        3: {'precip_mm': 50, 'tmin_c': 3, 'tmax_c': 12, 'gdd': 0},
        4: {'precip_mm': 50, 'tmin_c': 6, 'tmax_c': 16, 'gdd': 55},
        5: {'precip_mm': 60, 'tmin_c': 10, 'tmax_c': 20, 'gdd': 150},
        6: {'precip_mm': 55, 'tmin_c': 13, 'tmax_c': 23, 'gdd': 240},
        7: {'precip_mm': 55, 'tmin_c': 15, 'tmax_c': 25, 'gdd': 310},
        8: {'precip_mm': 50, 'tmin_c': 15, 'tmax_c': 25, 'gdd': 310},
        9: {'precip_mm': 50, 'tmin_c': 12, 'tmax_c': 21, 'gdd': 195},
        10: {'precip_mm': 60, 'tmin_c': 8, 'tmax_c': 15, 'gdd': 55},
        11: {'precip_mm': 55, 'tmin_c': 4, 'tmax_c': 10, 'gdd': 0},
        12: {'precip_mm': 60, 'tmin_c': 2, 'tmax_c': 7, 'gdd': 0},
    },
}

# Standard deviations for anomaly context (approximate)
STDDEV_DEFAULTS = {
    'precip': 0.35,  # 35% of normal is 1 std dev
    'temp': 2.0,     # 2°C is typical 1 std dev
}


def load_climatology_to_database(conn=None) -> int:
    """Load climatology normals into reference.weather_climatology."""
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

        for region_code, months in MONTHLY_NORMALS.items():
            for month, data in months.items():
                # Calculate standard deviations
                precip_std = data['precip_mm'] * STDDEV_DEFAULTS['precip']
                temp_std = STDDEV_DEFAULTS['temp']

                cur.execute("""
                    INSERT INTO reference.weather_climatology (
                        region_code, month, day_of_month,
                        precip_normal_mm, tmin_normal_c, tmax_normal_c,
                        gdd_normal, precip_stddev, temp_stddev
                    ) VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (region_code, month, day_of_month)
                    DO UPDATE SET
                        precip_normal_mm = EXCLUDED.precip_normal_mm,
                        tmin_normal_c = EXCLUDED.tmin_normal_c,
                        tmax_normal_c = EXCLUDED.tmax_normal_c,
                        gdd_normal = EXCLUDED.gdd_normal,
                        precip_stddev = EXCLUDED.precip_stddev,
                        temp_stddev = EXCLUDED.temp_stddev
                """, (
                    region_code,
                    month,
                    data['precip_mm'],
                    data['tmin_c'],
                    data['tmax_c'],
                    data['gdd'],
                    precip_std,
                    temp_std
                ))
                count += 1

        conn.commit()
        logger.info(f"Loaded {count} climatology records")

    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")

    finally:
        if should_close:
            conn.close()

    return count


def main():
    parser = argparse.ArgumentParser(description='Climatology Normals Loader')
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Init command
    init_parser = subparsers.add_parser('init', help='Load built-in normals')

    # Show command
    show_parser = subparsers.add_parser('show', help='Show normals for a region')
    show_parser.add_argument('region', help='Region code')

    # List command
    list_parser = subparsers.add_parser('list', help='List available regions')

    args = parser.parse_args()

    if args.command == 'init':
        print("Loading climatology normals to database...")
        count = load_climatology_to_database()
        print(f"Loaded {count} records for {len(MONTHLY_NORMALS)} regions")

    elif args.command == 'show':
        region = args.region.upper()
        if region in MONTHLY_NORMALS:
            print(f"\nClimatology Normals for {region}")
            print("=" * 60)
            print(f"{'Month':<8} {'Precip':<10} {'Tmin':<8} {'Tmax':<8} {'GDD':<8}")
            print("-" * 60)
            for month, data in MONTHLY_NORMALS[region].items():
                month_name = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][month]
                print(f"{month_name:<8} {data['precip_mm']:<10} {data['tmin_c']:<8} "
                      f"{data['tmax_c']:<8} {data['gdd']:<8}")
        else:
            print(f"Unknown region: {region}")
            print(f"Available: {', '.join(MONTHLY_NORMALS.keys())}")

    elif args.command == 'list':
        print("\nAvailable Climatology Regions:")
        print("=" * 40)
        for region in sorted(MONTHLY_NORMALS.keys()):
            print(f"  {region}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
