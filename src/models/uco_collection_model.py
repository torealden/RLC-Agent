"""
UCO (Used Cooking Oil) Collection Estimation Model — v2 (Top-Down)

Estimates monthly US UCO collection starting from total edible vegetable oil
consumption, not restaurant counts. This approach is:
  1. Grounded in physical fundamentals (oil in → waste oil out)
  2. Consistent across historical and forecast periods
  3. Exportable to other countries using FAS PSD data

Model:
  UCO_collected = Σ(oil_consumption[i] - biofuel_use[i]) × recovery_rate × collection_rate

Where:
  - oil_consumption: FAS PSD domestic consumption by oil type (1000 MT → mil lbs)
  - biofuel_use: BBD feedstock consumption by oil type (from balance sheets)
  - recovery_rate: fraction of non-biofuel oil that becomes collectible waste
    (compounds frying share, non-household share, physical recovery)
  - collection_rate: fraction of recoverable oil actually collected
    (infrastructure-dependent, varies by country/time)
  - Monthly distribution via ERS FAFH+FAH spending seasonal index

For the US:
  - Non-biofuel edible oil consumption: ~22-27B lbs/year (growing)
  - Effective recovery × collection: ~12-15% → 2.5-4B lbs/year UCO
  - This is the range industry sources cite and implies reasonable BBD yields

Usage:
    from src.models.uco_collection_model import UCOCollectionModel
    model = UCOCollectionModel()
    estimates = model.estimate_monthly(start_year=2015, end_year=2026)
"""

import logging
import os
from collections import defaultdict
from datetime import date

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("uco_model")

# ── Model Parameters ──────────────────────────────────────────────────

# Effective recovery rate: what fraction of non-biofuel edible oil consumption
# becomes collectible waste cooking oil. This compounds:
#   - Frying/cooking share (~50-60% of commercial oil goes to frying)
#   - Commercial vs household split (~70% commercial, ~30% household)
#   - Physical recovery fraction (~60-70% of frying oil is recoverable)
#   - Household collection rate (~0% in US)
# Net: ~50% × 70% × 65% × (1.0 commercial + 0.0 household) ≈ 22% of commercial
# But ~30% of consumption is household (0% recovery), so net ~15% of total
EFFECTIVE_RECOVERY_RATE = 0.15

# Collection rate: fraction of recoverable oil that is actually collected
# US has mature collection infrastructure (DAR PRO, Mahoney/Neste, Baker, etc.)
# Large chains ~90%, independents ~60%, food manufacturing ~95%
# Blended average for US:
COLLECTION_RATE = 0.75

# Corn oil addition: not in FAS PSD oil tables but significant (~4B lbs/yr)
# DCO from ethanol plants + wet mill corn oil
# We estimate from NASS data when available, otherwise use a default
CORN_OIL_DEFAULT_MIL_LBS = 4000  # ~4B lbs/yr total, most goes to biofuel
CORN_OIL_FOOD_SHARE = 0.30  # ~30% of corn oil goes to food (rest is biofuel)

# Peanut oil: small but real (~300M lbs/yr)
PEANUT_OIL_DEFAULT_MIL_LBS = 300

# FAS PSD oils to sum for the base
PSD_OIL_COMMODITIES = [
    'soybean_oil', 'rapeseed_oil', 'palm_oil',
    'sunflowerseed_oil', 'cottonseed_oil',
]

# Biofuel feedstock use by oil (approximate annual mil lbs, for subtraction)
# These should eventually come from the balance sheets dynamically
# For now, use representative values that grow over time
BIOFUEL_USE_DEFAULTS = {
    2015: 5000,   # ~5B lbs total BBD feedstock from veg oils
    2016: 5500,
    2017: 6000,
    2018: 7000,
    2019: 8000,
    2020: 9000,
    2021: 10000,
    2022: 14000,  # RD boom
    2023: 17000,
    2024: 18000,
    2025: 19000,
    2026: 20000,
}


class UCOCollectionModel:
    """Estimates monthly US UCO collection from edible oil consumption."""

    def __init__(self, db_conn_func=None):
        self.db_conn_func = db_conn_func or self._default_conn

    @staticmethod
    def _default_conn():
        return psycopg2.connect(
            host=os.getenv('RLC_PG_HOST'),
            dbname='rlc_commodities',
            user=os.getenv('RLC_PG_USER'),
            password=os.getenv('RLC_PG_PASSWORD'),
            sslmode='require',
        )

    def load_oil_consumption(self):
        """
        Load annual US edible oil domestic consumption from FAS PSD.
        Returns: {year: total_mil_lbs} (INCLUDES biofuel use)
        """
        conn = self.db_conn_func()
        cur = conn.cursor()
        cur.execute("""
            SELECT marketing_year,
                SUM(max_dc) * 2.20462 as total_mil_lbs
            FROM (
                SELECT commodity, marketing_year, MAX(domestic_consumption) as max_dc
                FROM bronze.fas_psd
                WHERE country_code = 'US'
                  AND commodity = ANY(%s)
                  AND marketing_year BETWEEN 2010 AND 2030
                GROUP BY commodity, marketing_year
            ) sub
            GROUP BY marketing_year ORDER BY marketing_year
        """, (PSD_OIL_COMMODITIES,))
        data = {}
        for year, total in cur.fetchall():
            data[int(year)] = float(total)
        conn.close()
        logger.info(f"Loaded oil consumption: {len(data)} years")
        return data

    def load_fafh_spending(self):
        """Load monthly FAFH spending for seasonal index."""
        conn = self.db_conn_func()
        cur = conn.cursor()
        cur.execute("""
            SELECT year, month, SUM(sales_value) as total_fafh
            FROM bronze.ers_food_sales_monthly
            WHERE food_category = 'Food away from home'
            GROUP BY year, month
            ORDER BY year, month
        """)
        data = {}
        for year, month, total in cur.fetchall():
            data[(year, month)] = float(total)
        conn.close()
        logger.info(f"Loaded FAFH spending: {len(data)} months")
        return data

    def compute_non_biofuel_consumption(self, oil_consumption):
        """
        Subtract estimated biofuel feedstock use from total oil consumption.
        Returns: {year: non_biofuel_mil_lbs}
        """
        result = {}
        for year, total in oil_consumption.items():
            biofuel = BIOFUEL_USE_DEFAULTS.get(year)
            if biofuel is None:
                # Extrapolate
                if year < min(BIOFUEL_USE_DEFAULTS):
                    biofuel = BIOFUEL_USE_DEFAULTS[min(BIOFUEL_USE_DEFAULTS)]
                else:
                    biofuel = BIOFUEL_USE_DEFAULTS[max(BIOFUEL_USE_DEFAULTS)]

            # Add corn oil food-use portion and peanut oil
            additional = (CORN_OIL_DEFAULT_MIL_LBS * CORN_OIL_FOOD_SHARE +
                          PEANUT_OIL_DEFAULT_MIL_LBS)

            non_bio = total - biofuel + additional
            # Floor at a reasonable minimum
            non_bio = max(non_bio, 10000)
            result[year] = non_bio

        return result

    def compute_seasonal_index(self, fafh_spending):
        """Monthly FAFH seasonal index (avg month = 1.0)."""
        by_year = defaultdict(dict)
        for (year, month), val in fafh_spending.items():
            by_year[year][month] = val

        index = {}
        for year, months in by_year.items():
            if len(months) < 6:
                continue
            avg = sum(months.values()) / len(months)
            if avg <= 0:
                continue
            for month, val in months.items():
                index[(year, month)] = val / avg

        return index

    def estimate_monthly(self, start_year=2015, end_year=2026):
        """
        Estimate monthly UCO collection.

        Returns list of dicts with year, month, uco_mil_lbs, etc.
        """
        oil_consumption = self.load_oil_consumption()
        fafh_spending = self.load_fafh_spending()

        non_bio = self.compute_non_biofuel_consumption(oil_consumption)
        seasonal = self.compute_seasonal_index(fafh_spending)

        # Interpolate/extrapolate for missing years
        known_years = sorted(non_bio.keys())
        if not known_years:
            logger.error("No oil consumption data available")
            return []

        results = []
        for year in range(start_year, end_year + 1):
            # Get annual non-biofuel consumption
            if year in non_bio:
                annual_non_bio = non_bio[year]
            elif year < known_years[0]:
                annual_non_bio = non_bio[known_years[0]]
            elif year > known_years[-1]:
                annual_non_bio = non_bio[known_years[-1]] * (1.01 ** (year - known_years[-1]))
            else:
                lo = max(y for y in known_years if y <= year)
                hi = min(y for y in known_years if y >= year)
                frac = (year - lo) / (hi - lo) if hi != lo else 0
                annual_non_bio = non_bio[lo] + frac * (non_bio[hi] - non_bio[lo])

            # Annual UCO = non-biofuel consumption × recovery × collection
            annual_uco = annual_non_bio * EFFECTIVE_RECOVERY_RATE * COLLECTION_RATE
            monthly_base = annual_uco / 12.0

            for month in range(1, 13):
                idx = seasonal.get((year, month), 1.0)
                uco_mil_lbs = monthly_base * idx

                results.append({
                    'year': year,
                    'month': month,
                    'uco_mil_lbs': round(uco_mil_lbs, 2),
                    'annual_non_bio_mil_lbs': round(annual_non_bio, 0),
                    'annual_uco_mil_lbs': round(annual_uco, 0),
                    'fafh_index': round(idx, 4),
                })

        logger.info(f"Estimated {len(results)} monthly UCO values "
                    f"({start_year}-{end_year})")
        return results

    def estimate_yg_complex(self, start_year=2015, end_year=2026,
                            nass_yg_data=None):
        """
        Build the full yellow grease complex:
          - UCO collection (model estimate)
          - YG ex-UCO (NASS total minus UCO; or estimated if suppressed)
          - Combined (UCO + YG ex-UCO)
        """
        uco_estimates = self.estimate_monthly(start_year, end_year)
        if nass_yg_data is None:
            nass_yg_data = {}

        results = []
        for est in uco_estimates:
            year, month = est['year'], est['month']
            uco = est['uco_mil_lbs']
            nass_total = nass_yg_data.get((year, month))

            if nass_total is not None and nass_total > uco:
                yg_ex_uco = nass_total - uco
                combined = nass_total
                source = 'nass_minus_model'
            elif nass_total is not None:
                # UCO exceeds NASS — cap UCO at 70% of NASS
                combined = nass_total
                uco = nass_total * 0.70
                yg_ex_uco = combined - uco
                source = 'nass_capped'
            else:
                # NASS suppressed — estimate combined from UCO
                # UCO is ~65-70% of total YG complex
                combined = uco / 0.675
                yg_ex_uco = combined - uco
                source = 'model_estimated'

            results.append({
                'year': year,
                'month': month,
                'uco_mil_lbs': round(uco, 2),
                'yg_ex_uco_mil_lbs': round(yg_ex_uco, 2),
                'combined_mil_lbs': round(combined, 2),
                'nass_total_mil_lbs': round(nass_total, 2) if nass_total else None,
                'source': source,
                'fafh_index': est['fafh_index'],
            })

        return results
