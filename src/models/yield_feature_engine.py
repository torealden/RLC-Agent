"""
Yield Feature Engineering Pipeline

Transforms raw weather, CPC gridded, NASS tabular, NDVI, and World Weather
data into weekly feature vectors stored in silver.yield_features.

One row per (state, crop, year, week) — the input to the yield prediction model.

Usage:
    python -m src.models.yield_feature_engine --state IA --crop corn --year 2024
    python -m src.models.yield_feature_engine --all --year 2024
    python -m src.models.yield_feature_engine --verify
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load crop-specific thresholds
THRESHOLDS_PATH = PROJECT_ROOT / "config" / "weather_yield_thresholds.json"
THRESHOLDS = {}
if THRESHOLDS_PATH.exists():
    with open(THRESHOLDS_PATH) as f:
        THRESHOLDS = json.load(f)

# State abbreviation mapping
US_STATES = {
    'AL': 'Alabama', 'AR': 'Arkansas', 'AZ': 'Arizona', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida',
    'GA': 'Georgia', 'IA': 'Iowa', 'ID': 'Idaho', 'IL': 'Illinois',
    'IN': 'Indiana', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana',
    'MD': 'Maryland', 'MI': 'Michigan', 'MN': 'Minnesota', 'MO': 'Missouri',
    'MS': 'Mississippi', 'MT': 'Montana', 'NC': 'North Carolina',
    'ND': 'North Dakota', 'NE': 'Nebraska', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NY': 'New York', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'VA': 'Virginia',
    'WA': 'Washington', 'WI': 'Wisconsin', 'WV': 'West Virginia', 'WY': 'Wyoming',
}
STATE_FULL_TO_ABBREV = {v.upper(): k for k, v in US_STATES.items()}

# Crop name mapping between systems
CROP_CPC_MAP = {'corn': 'corn', 'soybeans': 'soybeans', 'winter_wheat': 'winter_wheat', 'cotton': 'cotton'}
CROP_NASS_MAP = {'corn': 'CORN', 'soybeans': 'SOYBEANS', 'winter_wheat': 'WHEAT_ALL', 'cotton': 'COTTON'}

# NASS week → approximate calendar date (week 1 starts early April)
def nass_week_to_date(year: int, week: int) -> date:
    """Convert NASS week number to approximate week-ending date (Sunday)."""
    # NASS crop progress weeks roughly start in early April (week ~14 in ISO)
    # Week 1 ≈ first full week of April
    jan1 = date(year, 1, 1)
    # Approximate: NASS week 1 ≈ ISO week 14
    iso_week = week + 13
    iso_week = min(iso_week, 52)
    target = jan1 + timedelta(weeks=iso_week - 1)
    # Move to Sunday
    days_to_sunday = (6 - target.weekday()) % 7
    return target + timedelta(days=days_to_sunday)


def date_to_nass_week(d: date) -> int:
    """Convert a date to approximate NASS week number."""
    iso_week = d.isocalendar()[1]
    nass_week = iso_week - 13
    return max(1, min(nass_week, 40))


def get_db_connection():
    """Get PostgreSQL connection."""
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
    password = (
        os.environ.get("RLC_PG_PASSWORD")
        or os.environ.get("DATABASE_PASSWORD")
        or os.environ.get("DB_PASSWORD")
    )
    return psycopg2.connect(
        host=os.environ.get("DATABASE_HOST", "localhost"),
        port=os.environ.get("DATABASE_PORT", "5432"),
        database=os.environ.get("DATABASE_NAME", "rlc_commodities"),
        user=os.environ.get("DATABASE_USER", "postgres"),
        password=password,
    )


class YieldFeatureEngine:
    """
    Transforms raw weather, CPC, NASS, NDVI, and World Weather data
    into weekly feature vectors stored in silver.yield_features.
    """

    def __init__(self, conn=None):
        self._conn = conn
        self._own_conn = False
        self.thresholds = THRESHOLDS

    def _get_conn(self):
        if self._conn is None:
            self._conn = get_db_connection()
            self._own_conn = True
        return self._conn

    def close(self):
        if self._own_conn and self._conn:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # PUBLIC API
    # ------------------------------------------------------------------

    def build_features(self, state: str, crop: str, year: int,
                       week_start: int = 1, week_end: int = 40) -> int:
        """Build feature vectors for a specific state/crop/year."""
        conn = self._get_conn()
        cur = conn.cursor()
        count = 0

        state_abbrev = state if len(state) == 2 else STATE_FULL_TO_ABBREV.get(state.upper(), state)

        for week in range(week_start, week_end + 1):
            week_date = nass_week_to_date(year, week)
            if week_date > date.today():
                break

            features = {}

            # Weather
            wx = self._get_weather_features(cur, state_abbrev, crop, year, week, week_date)
            features.update(wx)

            # CPC gridded condition/progress (national)
            cpc = self._get_cpc_features(cur, crop, year, week)
            features.update(cpc)

            # NASS tabular progress/condition (state-level)
            nass = self._get_nass_progress(cur, crop, state_abbrev, year, week, week_date)
            features.update(nass)

            # NDVI (may be empty)
            ndvi = self._get_ndvi_features(cur, state_abbrev, crop, year, week, week_date)
            features.update(ndvi)

            # World Weather signals
            ww = self._get_ww_signals(cur, year, week, week_date)
            features.update(ww)

            # Growth stage
            growth_stage = self._determine_growth_stage(crop, week)
            features['growth_stage'] = growth_stage

            # Upsert
            cur.execute("""
                INSERT INTO silver.yield_features
                    (state, crop, year, week, week_ending_date,
                     gdd_cum, gdd_vs_normal_pct, precip_cum_mm, precip_vs_normal_pct,
                     stress_days_heat, stress_days_drought, excess_moisture_days, frost_events,
                     tmax_weekly_avg, tmin_weekly_avg, tavg_weekly,
                     ndvi_mean, ndvi_anomaly, ndvi_trend_4wk,
                     condition_index, condition_vs_5yr, progress_index, progress_vs_normal,
                     pct_planted, pct_emerged, pct_silking, pct_dough, pct_mature, pct_harvested,
                     good_excellent_pct,
                     ww_risk_score, ww_outlook_sentiment,
                     growth_stage, feature_version)
                VALUES (%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'v1')
                ON CONFLICT (state, crop, year, week)
                DO UPDATE SET
                    week_ending_date = EXCLUDED.week_ending_date,
                    gdd_cum = EXCLUDED.gdd_cum,
                    gdd_vs_normal_pct = EXCLUDED.gdd_vs_normal_pct,
                    precip_cum_mm = EXCLUDED.precip_cum_mm,
                    precip_vs_normal_pct = EXCLUDED.precip_vs_normal_pct,
                    stress_days_heat = EXCLUDED.stress_days_heat,
                    stress_days_drought = EXCLUDED.stress_days_drought,
                    excess_moisture_days = EXCLUDED.excess_moisture_days,
                    frost_events = EXCLUDED.frost_events,
                    tmax_weekly_avg = EXCLUDED.tmax_weekly_avg,
                    tmin_weekly_avg = EXCLUDED.tmin_weekly_avg,
                    tavg_weekly = EXCLUDED.tavg_weekly,
                    ndvi_mean = EXCLUDED.ndvi_mean,
                    ndvi_anomaly = EXCLUDED.ndvi_anomaly,
                    ndvi_trend_4wk = EXCLUDED.ndvi_trend_4wk,
                    condition_index = EXCLUDED.condition_index,
                    condition_vs_5yr = EXCLUDED.condition_vs_5yr,
                    progress_index = EXCLUDED.progress_index,
                    progress_vs_normal = EXCLUDED.progress_vs_normal,
                    pct_planted = EXCLUDED.pct_planted,
                    pct_emerged = EXCLUDED.pct_emerged,
                    pct_silking = EXCLUDED.pct_silking,
                    pct_dough = EXCLUDED.pct_dough,
                    pct_mature = EXCLUDED.pct_mature,
                    pct_harvested = EXCLUDED.pct_harvested,
                    good_excellent_pct = EXCLUDED.good_excellent_pct,
                    ww_risk_score = EXCLUDED.ww_risk_score,
                    ww_outlook_sentiment = EXCLUDED.ww_outlook_sentiment,
                    growth_stage = EXCLUDED.growth_stage,
                    updated_at = NOW()
            """, (
                state_abbrev, crop, year, week, week_date,
                features.get('gdd_cum'), features.get('gdd_vs_normal_pct'),
                features.get('precip_cum_mm'), features.get('precip_vs_normal_pct'),
                features.get('stress_days_heat', 0), features.get('stress_days_drought', 0),
                features.get('excess_moisture_days', 0), features.get('frost_events', 0),
                features.get('tmax_weekly_avg'), features.get('tmin_weekly_avg'),
                features.get('tavg_weekly'),
                features.get('ndvi_mean'), features.get('ndvi_anomaly'),
                features.get('ndvi_trend_4wk'),
                features.get('condition_index'), features.get('condition_vs_5yr'),
                features.get('progress_index'), features.get('progress_vs_normal'),
                features.get('pct_planted'), features.get('pct_emerged'),
                features.get('pct_silking'), features.get('pct_dough'),
                features.get('pct_mature'), features.get('pct_harvested'),
                features.get('good_excellent_pct'),
                features.get('ww_risk_score'), features.get('ww_outlook_sentiment'),
                growth_stage,
            ))
            count += 1

        conn.commit()
        logger.info(f"Built {count} feature rows for {state_abbrev}/{crop}/{year}")
        return count

    def build_all_features(self, year: int, crops: list = None,
                           states: list = None) -> dict:
        """Build features for all state/crop combinations."""
        conn = self._get_conn()
        cur = conn.cursor()

        if crops is None:
            crops = ['corn', 'soybeans', 'winter_wheat', 'cotton']

        if states is None:
            # Get states with yield data for these crops
            crop_db_names = [CROP_NASS_MAP.get(c, c.upper()) for c in crops]
            cur.execute("""
                SELECT DISTINCT state_abbrev
                FROM bronze.nass_state_yields
                WHERE commodity = ANY(%s)
                  AND state_abbrev IS NOT NULL
                  AND yield_per_acre IS NOT NULL
                ORDER BY state_abbrev
            """, (crop_db_names,))
            states = [r[0] for r in cur.fetchall()]

        summary = {'total_rows': 0, 'crops': {}}

        for crop in crops:
            crop_count = 0
            crop_db = CROP_NASS_MAP.get(crop, crop.upper())

            # Only process states that actually grow this crop
            cur.execute("""
                SELECT DISTINCT state_abbrev
                FROM bronze.nass_state_yields
                WHERE commodity = %s
                  AND state_abbrev IS NOT NULL
                  AND yield_per_acre IS NOT NULL
            """, (crop_db,))
            crop_states = [r[0] for r in cur.fetchall()]
            relevant_states = [s for s in states if s in crop_states]

            for state in relevant_states:
                count = self.build_features(state, crop, year)
                crop_count += count

            summary['crops'][crop] = {'rows': crop_count, 'states': len(relevant_states)}
            summary['total_rows'] += crop_count
            logger.info(f"  {crop}: {crop_count} rows across {len(relevant_states)} states")

        return summary

    # ------------------------------------------------------------------
    # FEATURE EXTRACTORS
    # ------------------------------------------------------------------

    def _get_weather_features(self, cur, state_abbrev: str, crop: str,
                              year: int, week: int, week_date: date) -> dict:
        """Extract weather features from silver.weather_observation."""
        result = {
            'gdd_cum': None, 'gdd_vs_normal_pct': None,
            'precip_cum_mm': None, 'precip_vs_normal_pct': None,
            'stress_days_heat': 0, 'stress_days_drought': 0,
            'excess_moisture_days': 0, 'frost_events': 0,
            'tmax_weekly_avg': None, 'tmin_weekly_avg': None, 'tavg_weekly': None,
        }

        # location_ids use format "city_ST" — match by state suffix
        state_lower = state_abbrev.lower()

        crop_cfg = self.thresholds.get('crops', {}).get(
            'wheat' if 'wheat' in crop else crop, {}
        )
        gdd_base_c = crop_cfg.get('gdd_base_c', 10)
        gdd_cap_c = crop_cfg.get('gdd_cap_c')
        heat_thresh_c = crop_cfg.get('severe_heat_threshold_c', 35)
        frost_thresh_c = crop_cfg.get('frost_threshold_c', 0)
        drought_mm_week = crop_cfg.get('drought_threshold_mm_week', 13)
        excess_mm_week = crop_cfg.get('excess_moisture_mm_week', 75)

        # Determine planting date for cumulative calculations
        stages = crop_cfg.get('growth_stages', crop_cfg.get('growth_stages_winter', {}))
        planting = stages.get('planting', {})
        plant_month = planting.get('start_month', 4)
        plant_day = planting.get('start_day', 15)
        planting_date = date(year, plant_month, plant_day)

        week_start = week_date - timedelta(days=6)

        # Current week temperature summary
        # silver.weather_observation uses: observation_date (date), temp_high_f,
        # temp_low_f, temp_avg_f, precipitation_in, precipitation_mm,
        # location_id (e.g. 'des_moines_ia')
        cur.execute("""
            SELECT
                AVG(temp_avg_f) as avg_temp,
                AVG(temp_high_f) as max_temp,
                AVG(temp_low_f) as min_temp,
                SUM(COALESCE(precipitation_mm, 0)) as precip_mm,
                COUNT(*) as obs_days
            FROM silver.weather_observation
            WHERE location_id LIKE '%%_' || %s
              AND observation_date >= %s AND observation_date <= %s
        """, (state_lower, week_start, week_date))
        row = cur.fetchone()

        if row and row[0] is not None:
            result['tavg_weekly'] = round((float(row[0]) - 32) * 5 / 9, 1)
            result['tmax_weekly_avg'] = round((float(row[1]) - 32) * 5 / 9, 1)
            result['tmin_weekly_avg'] = round((float(row[2]) - 32) * 5 / 9, 1)

        # Cumulative GDD and precip from planting date
        cur.execute("""
            SELECT
                observation_date as obs_date,
                AVG(temp_high_f) as daily_max_f,
                AVG(temp_low_f) as daily_min_f,
                SUM(COALESCE(precipitation_mm, 0)) as daily_precip_mm
            FROM silver.weather_observation
            WHERE location_id LIKE '%%_' || %s
              AND observation_date >= %s AND observation_date <= %s
            GROUP BY observation_date
            ORDER BY obs_date
        """, (state_lower, planting_date, week_date))
        daily_rows = cur.fetchall()

        if daily_rows:
            gdd_cum = 0.0
            precip_cum = 0.0
            heat_days = 0
            frost_events = 0
            excess_days = 0
            consecutive_dry = 0
            max_consecutive_dry = 0

            for obs_date, max_f, min_f, precip_mm in daily_rows:
                max_c = (float(max_f) - 32) * 5 / 9 if max_f else 0
                min_c = (float(min_f) - 32) * 5 / 9 if min_f else 0

                # GDD
                tmax_adj = min(max_c, gdd_cap_c) if gdd_cap_c else max_c
                tavg = (min_c + tmax_adj) / 2
                gdd = max(0, tavg - gdd_base_c)
                gdd_cum += gdd

                # Precip
                precip_mm = float(precip_mm) if precip_mm else 0
                precip_cum += precip_mm

                # Stress indicators
                if max_c > heat_thresh_c:
                    heat_days += 1
                if min_c < frost_thresh_c:
                    frost_events += 1
                if precip_mm > excess_mm_week / 7:
                    excess_days += 1

                # Consecutive dry days
                if precip_mm < 1.0:
                    consecutive_dry += 1
                    max_consecutive_dry = max(max_consecutive_dry, consecutive_dry)
                else:
                    consecutive_dry = 0

            result['gdd_cum'] = round(gdd_cum, 1)
            result['precip_cum_mm'] = round(precip_cum, 1)
            result['stress_days_heat'] = heat_days
            result['frost_events'] = frost_events
            result['excess_moisture_days'] = excess_days
            result['stress_days_drought'] = max_consecutive_dry

            # Compare to climatology normals
            cur.execute("""
                SELECT region_code,
                       SUM(gdd_normal) as total_gdd_normal,
                       SUM(precip_normal_mm) as total_precip_normal
                FROM reference.weather_climatology
                WHERE region_code IN ('US_CORN_BELT', 'US_SOY_BELT', 'US_WHEAT_WINTER', 'US_WHEAT_SPRING')
                  AND month >= %s AND month <= %s
                GROUP BY region_code
            """, (plant_month, week_date.month))
            clim_rows = cur.fetchall()

            if clim_rows:
                # Use the first matching region as proxy
                _, gdd_normal, precip_normal = clim_rows[0]
                if gdd_normal and float(gdd_normal) > 0:
                    result['gdd_vs_normal_pct'] = round(
                        (gdd_cum - float(gdd_normal)) / float(gdd_normal) * 100, 1
                    )
                if precip_normal and float(precip_normal) > 0:
                    result['precip_vs_normal_pct'] = round(
                        (precip_cum - float(precip_normal)) / float(precip_normal) * 100, 1
                    )

        return result

    def _get_cpc_features(self, cur, crop: str, year: int, week: int) -> dict:
        """Extract CPC gridded condition/progress features (national level)."""
        result = {
            'condition_index': None, 'condition_vs_5yr': None,
            'progress_index': None, 'progress_vs_normal': None,
        }

        cpc_crop = CROP_CPC_MAP.get(crop, crop)

        # Condition from gold view
        cur.execute("""
            SELECT condition_mean
            FROM gold.cpc_condition_weekly
            WHERE crop = %s AND year = %s AND nass_week = %s AND region_id = 'US'
            LIMIT 1
        """, (cpc_crop, year, week))
        row = cur.fetchone()
        if row and row[0] is not None:
            result['condition_index'] = float(row[0])

        # YoY comparison
        cur.execute("""
            SELECT vs_5yr_avg
            FROM gold.cpc_condition_yoy
            WHERE crop = %s AND nass_week = %s
            LIMIT 1
        """, (cpc_crop, week))
        row = cur.fetchone()
        if row and row[0] is not None:
            result['condition_vs_5yr'] = float(row[0])

        # Progress from gold view
        cur.execute("""
            SELECT progress_mean
            FROM gold.cpc_progress_weekly
            WHERE crop = %s AND year = %s AND nass_week = %s AND region_id = 'US'
            LIMIT 1
        """, (cpc_crop, year, week))
        row = cur.fetchone()
        if row and row[0] is not None:
            result['progress_index'] = float(row[0])

            # Compare progress to 5-year average
            cur.execute("""
                SELECT AVG(progress_mean)
                FROM gold.cpc_progress_weekly
                WHERE crop = %s AND nass_week = %s AND region_id = 'US'
                  AND year BETWEEN %s AND %s
            """, (cpc_crop, week, year - 5, year - 1))
            avg_row = cur.fetchone()
            if avg_row and avg_row[0] is not None:
                result['progress_vs_normal'] = round(
                    float(row[0]) - float(avg_row[0]), 3
                )

        return result

    def _get_nass_progress(self, cur, crop: str, state_abbrev: str,
                           year: int, week: int, week_date: date) -> dict:
        """Extract NASS tabular crop progress and condition data.

        Note: NASS condition/progress data is currently national-level only
        (state = 'US'). We use national data as a proxy for all states.
        Condition data is stored as rows per category (EXCELLENT, GOOD, etc.).
        """
        result = {
            'pct_planted': None, 'pct_emerged': None, 'pct_silking': None,
            'pct_dough': None, 'pct_mature': None, 'pct_harvested': None,
            'good_excellent_pct': None,
        }

        nass_commodity = CROP_NASS_MAP.get(crop, crop.upper()).lower()
        week_start = week_date - timedelta(days=6)

        # Condition ratings — pivoted: one row per condition_category per week
        # Use 'US' for national level since state data may not be available
        cur.execute("""
            SELECT condition_category, value
            FROM bronze.nass_crop_condition
            WHERE commodity = %s
              AND week_ending >= %s AND week_ending <= %s
            ORDER BY week_ending DESC
        """, (nass_commodity, week_start, week_date))
        condition_vals = {}
        for cat, val in cur.fetchall():
            # Take the first (most recent) value for each category
            if cat not in condition_vals and val is not None:
                condition_vals[cat] = float(val)

        excellent = condition_vals.get('EXCELLENT', 0)
        good = condition_vals.get('GOOD', 0)
        if excellent or good:
            result['good_excellent_pct'] = excellent + good

        # Progress — national level
        cur.execute("""
            SELECT value
            FROM bronze.nass_crop_progress
            WHERE commodity = %s
              AND week_ending >= %s AND week_ending <= %s
            ORDER BY week_ending DESC LIMIT 1
        """, (nass_commodity, week_start, week_date))
        row = cur.fetchone()
        if row and row[0] is not None:
            result['pct_planted'] = float(row[0])

        return result

    def _get_ndvi_features(self, cur, state_abbrev: str, crop: str,
                           year: int, week: int, week_date: date) -> dict:
        """Extract NDVI features (may be empty — graceful fallback).

        bronze.ndvi_observation is currently empty. This function will
        work once NDVI data is ingested via the AppEEARS pipeline.
        Falls back gracefully with None values.
        """
        result = {
            'ndvi_mean': None, 'ndvi_anomaly': None, 'ndvi_trend_4wk': None,
        }

        try:
            # Try bronze.ndvi_observation (region_code, observation_date, ndvi_value, ndvi_anomaly)
            cur.execute("""
                SELECT ndvi_value, ndvi_anomaly
                FROM bronze.ndvi_observation
                WHERE region_code = %s
                  AND observation_date >= %s - interval '10 days'
                  AND observation_date <= %s
                ORDER BY observation_date DESC LIMIT 1
            """, (state_abbrev, week_date, week_date))
            row = cur.fetchone()
            if row and row[0] is not None:
                result['ndvi_mean'] = float(row[0])
                if row[1] is not None:
                    result['ndvi_anomaly'] = float(row[1])

                # 4-week trend
                four_weeks_ago = week_date - timedelta(weeks=4)
                cur.execute("""
                    SELECT ndvi_value, observation_date
                    FROM bronze.ndvi_observation
                    WHERE region_code = %s
                      AND observation_date >= %s AND observation_date <= %s
                    ORDER BY observation_date
                """, (state_abbrev, four_weeks_ago, week_date))
                trend_rows = cur.fetchall()
                if len(trend_rows) >= 2:
                    vals = [float(r[0]) for r in trend_rows if r[0] is not None]
                    if len(vals) >= 2:
                        x = np.arange(len(vals))
                        coeffs = np.polyfit(x, vals, 1)
                        result['ndvi_trend_4wk'] = round(coeffs[0], 4)
        except Exception:
            # Table may not exist or be empty — that's OK
            pass

        return result

    def _get_ww_signals(self, cur, year: int, week: int, week_date: date) -> dict:
        """Extract World Weather signals from email content."""
        result = {
            'ww_risk_score': None, 'ww_outlook_sentiment': None,
        }

        week_start = week_date - timedelta(days=6)

        cur.execute("""
            SELECT weather_summary
            FROM bronze.weather_email_extract
            WHERE email_date >= %s AND email_date <= %s + interval '1 day'
            ORDER BY email_date DESC LIMIT 5
        """, (week_start, week_date))
        rows = cur.fetchall()

        if not rows:
            return result

        # Simple keyword-based risk/sentiment scoring
        risk_keywords = {
            'drought': 3, 'flooding': 3, 'flood': 2, 'excessive': 2,
            'stress': 2, 'drier-bias': 1, 'net drying': 1, 'significant': 1,
            'severe': 2, 'heat': 1, 'frost': 2, 'freeze': 3,
        }
        favorable_keywords = {
            'favorable': -1, 'adequate': -1, 'improved': -1, 'beneficial': -1,
            'normal': -0.5, 'no significant': -0.5, 'unchanged': -0.5,
        }

        risk_total = 0
        sentiment_total = 0
        email_count = 0

        for (text,) in rows:
            if not text:
                continue
            text_lower = text.lower()
            email_count += 1

            for kw, weight in risk_keywords.items():
                if kw in text_lower:
                    risk_total += weight
            for kw, weight in favorable_keywords.items():
                if kw in text_lower:
                    sentiment_total += weight

        if email_count > 0:
            result['ww_risk_score'] = min(10.0, round(risk_total / email_count, 1))
            raw_sentiment = -sentiment_total / email_count  # flip so positive = bullish
            result['ww_outlook_sentiment'] = max(-1.0, min(1.0, round(raw_sentiment / 3, 2)))

        return result

    def _determine_growth_stage(self, crop: str, week: int) -> str:
        """Map NASS week to growth stage using thresholds config."""
        week_date = nass_week_to_date(2024, week)  # Use any year for month/day
        month = week_date.month
        day = week_date.day

        crop_key = 'wheat' if 'wheat' in crop else crop
        crop_cfg = self.thresholds.get('crops', {}).get(crop_key, {})
        stages = crop_cfg.get('growth_stages', crop_cfg.get('growth_stages_winter', {}))

        for stage_name, period in stages.items():
            s_month = period.get('start_month', 1)
            s_day = period.get('start_day', 1)
            e_month = period.get('end_month', 12)
            e_day = period.get('end_day', 28)

            start = (s_month, s_day)
            end = (e_month, e_day)
            current = (month, day)

            if start <= current <= end:
                # Map to simplified stage names
                if stage_name in ('planting', 'emergence'):
                    return 'planting'
                elif stage_name in ('vegetative', 'fall_tillering', 'greenup'):
                    return 'vegetative'
                elif stage_name in ('pollination', 'flowering', 'heading', 'seed_fill', 'grain_fill'):
                    return 'reproductive'
                elif stage_name in ('maturity', 'harvest', 'dormancy'):
                    return 'maturity'
                return stage_name

        # Default based on month
        if month <= 4:
            return 'pre_planting'
        elif month <= 6:
            return 'vegetative'
        elif month <= 8:
            return 'reproductive'
        else:
            return 'maturity'


def verify():
    """Print summary of feature data."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT crop, year, COUNT(*) as rows,
                   COUNT(DISTINCT state) as states,
                   MIN(week) as min_week, MAX(week) as max_week,
                   COUNT(gdd_cum) as has_gdd,
                   COUNT(condition_index) as has_condition,
                   COUNT(good_excellent_pct) as has_ge
            FROM silver.yield_features
            GROUP BY crop, year
            ORDER BY crop, year
        """)
        rows = cur.fetchall()

        print(f"\n{'='*80}")
        print(f"  Yield Features — silver.yield_features")
        print(f"{'='*80}")

        if not rows:
            print("  No data")
        else:
            print(f"\n  {'Crop':<16} {'Year':>5} {'Rows':>6} {'States':>7} {'Weeks':>10} {'w/GDD':>6} {'w/Cond':>7} {'w/GE':>5}")
            print(f"  {'-'*16} {'-'*5} {'-'*6} {'-'*7} {'-'*10} {'-'*6} {'-'*7} {'-'*5}")
            total = 0
            for crop, year, count, states, min_wk, max_wk, gdd, cond, ge in rows:
                print(f"  {crop:<16} {year:>5} {count:>6} {states:>7} {min_wk:>3}-{max_wk:<4} {gdd:>6} {cond:>7} {ge:>5}")
                total += count
            print(f"  {'TOTAL':<16} {'':>5} {total:>6}")

        print(f"{'='*80}\n")

    finally:
        cur.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Yield Feature Engineering Pipeline")
    parser.add_argument("--state", type=str, help="State abbreviation (e.g., IA)")
    parser.add_argument("--crop", type=str, help="Crop name (corn, soybeans, winter_wheat, cotton)")
    parser.add_argument("--year", type=int, help="Year to process")
    parser.add_argument("--all", action="store_true", help="Process all states/crops for the year")
    parser.add_argument("--verify", action="store_true", help="Print verification summary")
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.verify:
        verify()
        return

    if not args.year:
        parser.error("--year is required (use --verify to check existing data)")

    engine = YieldFeatureEngine()
    try:
        if args.all:
            crops = [args.crop] if args.crop else None
            states = [args.state] if args.state else None
            summary = engine.build_all_features(args.year, crops=crops, states=states)
            print(f"\nFeature Summary for {args.year}:")
            for crop, info in summary['crops'].items():
                print(f"  {crop}: {info['rows']} rows, {info['states']} states")
            print(f"  Total: {summary['total_rows']} rows")
        elif args.state and args.crop:
            count = engine.build_features(args.state, args.crop, args.year)
            print(f"Built {count} feature rows for {args.state}/{args.crop}/{args.year}")
        else:
            parser.error("Use --all or provide both --state and --crop")
    finally:
        engine.close()


if __name__ == "__main__":
    main()
