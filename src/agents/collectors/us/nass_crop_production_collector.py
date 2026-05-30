"""USDA NASS Crop Production collector — long/tidy with revision history.

Feeds silver.crop_production (defined in mig 122). Designed for append-only
upserts keyed on (commodity, class, statistic, agg_level, state_fips,
crop_year, reference_period, release_date, asd_code, county_ansi).

Bronze->silver mapping (NASS QuickStats field -> silver column):

  commodity_desc           -> commodity                (lowercased, normalized)
  class_desc               -> class                    ("ALL CLASSES" -> "all_classes",
                                                        else lowercased + spaces->underscores)
  statisticcat_desc        -> statistic                ("AREA PLANTED" -> "area_planted")
  year                     -> crop_year
  reference_period_desc    -> reference_period         (verbatim)
                              + is_forecast            (TRUE if contains "FORECAST")
  load_time (date part)    -> release_date
  agg_level_desc           -> agg_level
  state_alpha              -> state_alpha
  state_fips_code          -> state_fips
  asd_code                 -> asd_code                 (DISTRICT level)
  county_code              -> county_ansi              (COUNTY level)
  Value                    -> value                    (NULL on (D)/(NA)/(X)/(-))
  unit_desc                -> unit
  CV (%)                   -> cv_pct                   (parsed if present)
  short_desc               -> short_desc               (verbatim)

source_report derivation:
  - area_planted + reference_period='YEAR' + release_month in (3) -> 'Prospective Plantings'
  - area_planted + reference_period='YEAR' + release_month in (6) -> 'Acreage'
  - reference_period contains 'FORECAST' -> 'Crop Production'
  - reference_period='YEAR' + release_month=1 -> 'Annual Crop Production Summary'
  - else -> 'Crop Production' (revision)
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Iterable

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / '.env')

from src.services.database.db_config import get_connection

logger = logging.getLogger(__name__)

NASS_URL = 'https://quickstats.nass.usda.gov/api/api_GET/'

# Commodity + class taxonomy. NASS class names are kept where standard;
# normalized form is used in silver.commodity / silver.class.
# 'classes' = the NASS class_desc values to query (None means leave class_desc
# off the query — fetches ALL CLASSES rollup).
COMMODITIES: Dict[str, Dict] = {
    'soybeans': {
        'nass_commodity': 'SOYBEANS',
        'classes': [None],  # NASS only has ALL CLASSES for soybean
    },
    'corn': {
        'nass_commodity': 'CORN',
        # Corn has util_practice splits (GRAIN vs SILAGE), not class_desc.
        # We probe with both via util_practice on the query — handled below.
        'classes': [None],
        'util_practices': ['GRAIN', 'SILAGE', 'ALL UTILIZATION PRACTICES'],
    },
    'wheat': {
        'nass_commodity': 'WHEAT',
        'classes': [None, 'WINTER', 'SPRING, (EXCL DURUM)', 'DURUM'],
    },
    'sorghum': {
        'nass_commodity': 'SORGHUM',
        'classes': [None],
        'util_practices': ['GRAIN', 'SILAGE', 'ALL UTILIZATION PRACTICES'],
    },
    'canola': {
        'nass_commodity': 'CANOLA',
        'classes': [None],
    },
    'sunflower': {
        'nass_commodity': 'SUNFLOWER',
        'classes': [None, 'OIL TYPE', 'NON-OIL TYPE'],
    },
    'cotton': {
        'nass_commodity': 'COTTON',
        'classes': [None, 'UPLAND', 'PIMA'],
    },
    'peanuts': {
        'nass_commodity': 'PEANUTS',
        'classes': [None, 'RUNNERS', 'SPANISH', 'VIRGINIAS & VALENCIAS'],
    },
}

STATISTICS = ['AREA PLANTED', 'AREA HARVESTED', 'YIELD', 'PRODUCTION', 'PRODUCTION VALUE']

# ============================================================
# Normalization helpers
# ============================================================
def _norm_commodity(nass: str) -> str:
    """NASS commodity_desc -> normalized silver commodity slug."""
    n = (nass or '').strip().upper()
    if n == 'SOYBEANS':           return 'soybeans'
    if n == 'CORN':               return 'corn'
    if n == 'WHEAT':              return 'wheat'
    if n == 'SORGHUM':            return 'sorghum'
    if n == 'CANOLA':             return 'canola'
    if n == 'SUNFLOWER':          return 'sunflower'
    if n == 'COTTON':             return 'cotton'
    if n == 'COTTONSEED':         return 'cottonseed'
    if n == 'PEANUTS':            return 'peanuts'
    return n.lower()


def _norm_class(nass_class: str, util_practice: str = '') -> str:
    """NASS class_desc + util_practice_desc -> normalized silver class slug.

    For corn/sorghum, NASS uses util_practice to split GRAIN vs SILAGE
    rather than class_desc. We treat that as a class in our taxonomy.
    """
    cls = (nass_class or 'ALL CLASSES').strip().upper()
    up  = (util_practice or 'ALL UTILIZATION PRACTICES').strip().upper()

    # Corn / sorghum util practice splits
    if up == 'GRAIN':                          return 'grain'
    if up == 'SILAGE':                         return 'silage'

    if cls in ('ALL CLASSES', '') :            return 'all_classes'
    if cls == 'WINTER':                        return 'winter'
    if cls == 'SPRING, (EXCL DURUM)':          return 'spring'
    if cls == 'DURUM':                         return 'durum'
    if cls == 'OIL TYPE':                      return 'oil_type'
    if cls == 'NON-OIL TYPE':                  return 'confection'
    if cls == 'UPLAND':                        return 'upland'
    if cls == 'PIMA':                          return 'pima'
    if cls == 'RUNNERS':                       return 'runner'
    if cls == 'SPANISH':                       return 'spanish'
    if cls == 'VIRGINIAS & VALENCIAS':         return 'virginia_valencia'
    return cls.lower().replace(' ', '_').replace(',', '').replace('-', '_')


def _norm_statistic(nass_stat: str) -> str:
    """NASS statisticcat_desc -> normalized silver statistic slug."""
    s = (nass_stat or '').strip().upper()
    if s == 'AREA PLANTED':                    return 'area_planted'
    if s == 'AREA HARVESTED':                  return 'area_harvested'
    if s == 'YIELD':                           return 'yield'
    if s == 'PRODUCTION':                      return 'production'
    if s == 'PRODUCTION, MEASURED IN $':       return 'production_value'
    return s.lower().replace(' ', '_').replace(',', '')


def _parse_value(raw) -> Optional[float]:
    """Parse NASS Value string. Returns None for suppressed/NA."""
    s = str(raw or '').strip().replace(',', '')
    if not s or s in ('(D)', '(NA)', '(X)', '(-)'):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_cv(raw) -> Optional[float]:
    """Parse NASS CV(%) field."""
    s = str(raw or '').strip()
    if not s or s in ('(NA)', '(L)', ''):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_release_date(load_time_str: str) -> date:
    """NASS load_time looks like '2026-02-26 15:00:00.000' -> date."""
    if not load_time_str:
        return date.today()
    try:
        return datetime.strptime(load_time_str[:10], '%Y-%m-%d').date()
    except ValueError:
        return date.today()


def _is_forecast(reference_period: str) -> bool:
    return 'FORECAST' in (reference_period or '').upper()


def _derive_source_report(statistic: str, reference_period: str,
                          release_date: date) -> str:
    """Derive source_report from statistic + reference_period + release month.

    NASS uses 'YEAR - MAR ACREAGE' and 'YEAR - JUN ACREAGE' for the
    Prospective Plantings (March 31) and June Acreage (June 30) reports.
    The bare 'YEAR' reference_period is used both for the Acreage report's
    surprise-month value AND for revisions in monthly Crop Production
    releases and the Annual Crop Production Summary (Jan).
    """
    rp = (reference_period or '').upper()
    if rp == 'YEAR - MAR ACREAGE':
        return 'Prospective Plantings'
    if rp == 'YEAR - JUN ACREAGE':
        return 'Acreage'
    if 'FORECAST' in rp:
        return 'Crop Production'
    if rp == 'YEAR':
        m = release_date.month
        if m == 1:
            return 'Annual Crop Production Summary'
        if m == 3 and statistic == 'area_planted':
            return 'Prospective Plantings'
        if m == 6 and statistic == 'area_planted':
            return 'Acreage'
        return 'Crop Production'    # revisions
    return 'Crop Production'        # fallback


# ============================================================
# Collector
# ============================================================
class NASSCropProductionCollector:
    """Fetches NASS QuickStats CROPS data and writes to silver.crop_production."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get('NASS_API_KEY') or \
                       os.environ.get('USDA_NASS_API_KEY')
        if not self.api_key:
            raise RuntimeError('NASS_API_KEY not set')
        self.session = requests.Session()

    # --------------------------------------------------------
    # Fetch one (commodity, class, statistic, year-range) query
    # --------------------------------------------------------
    def _fetch_query(
        self,
        nass_commodity: str,
        nass_class: Optional[str],
        statisticcat: str,
        util_practice: Optional[str],
        year_ge: int,
        year_le: int,
        agg_levels: Iterable[str] = ('NATIONAL', 'STATE'),
    ) -> List[Dict]:
        """One NASS API call; returns parsed list of records (untransformed)."""
        params = {
            'key': self.api_key,
            'commodity_desc': nass_commodity,
            'statisticcat_desc': statisticcat,
            'year__GE': str(year_ge),
            'year__LE': str(year_le),
            'format': 'JSON',
        }
        if nass_class is not None:
            params['class_desc'] = nass_class
        if util_practice:
            params['util_practice_desc'] = util_practice

        all_rows = []
        for agg in agg_levels:
            p = dict(params)
            p['agg_level_desc'] = agg
            try:
                r = self.session.get(NASS_URL, params=p, timeout=60)
            except requests.RequestException as e:
                logger.warning(f'NASS HTTP error ({nass_commodity}/{statisticcat}/{agg}): {e}')
                continue
            if r.status_code == 200:
                data = r.json().get('data', [])
                all_rows.extend(data)
            elif r.status_code in (204, 404):
                continue
            else:
                logger.warning(f'NASS {r.status_code} for {nass_commodity}/{statisticcat}/{agg}')
        return all_rows

    def _transform(self, raw: Dict) -> Optional[Dict]:
        """Transform one NASS record to silver.crop_production row dict."""
        commodity_norm = _norm_commodity(raw.get('commodity_desc', ''))
        class_norm     = _norm_class(raw.get('class_desc', ''),
                                     raw.get('util_practice_desc', ''))
        statistic_norm = _norm_statistic(raw.get('statisticcat_desc', ''))

        ref_period     = raw.get('reference_period_desc', '') or ''
        release_dt     = _parse_release_date(raw.get('load_time', ''))

        # Skip cumulative "AUG THRU" type records — those are not standalone
        # forecasts/estimates
        if 'THRU' in ref_period.upper():
            return None

        # Don't bother storing rows with no usable value
        value = _parse_value(raw.get('Value'))
        cv    = _parse_cv(raw.get('CV (%)'))

        agg_level   = (raw.get('agg_level_desc', '') or '').upper()
        state_alpha = (raw.get('state_alpha', '') or '').upper()
        state_fips  = raw.get('state_fips_code', '') or '99'
        asd_code    = raw.get('asd_code', '') or ''
        county_ansi = raw.get('county_code', '') or ''
        short_desc  = raw.get('short_desc', '')
        unit        = raw.get('unit_desc', '') or ''
        crop_year   = int(raw.get('year', 0))

        return {
            'commodity':       commodity_norm,
            'class':           class_norm,
            'statistic':       statistic_norm,
            'crop_year':       crop_year,
            'reference_period':ref_period,
            'is_forecast':     _is_forecast(ref_period),
            'source_report':   _derive_source_report(statistic_norm, ref_period, release_dt),
            'release_date':    release_dt,
            'agg_level':       agg_level,
            'state_alpha':     state_alpha,
            'state_fips':      state_fips,
            'asd_code':        asd_code,
            'county_ansi':     county_ansi,
            'value':           value,
            'unit':            unit,
            'cv_pct':          cv,
            'short_desc':      short_desc,
        }

    # --------------------------------------------------------
    # Save to silver
    # --------------------------------------------------------
    def save(self, rows: List[Dict]) -> int:
        if not rows:
            return 0
        saved = 0
        with get_connection() as conn:
            with conn.cursor() as cur:
                for rec in rows:
                    cur.execute('SAVEPOINT row_sp')
                    try:
                        cur.execute("""
                            INSERT INTO silver.crop_production (
                                commodity, class, statistic,
                                crop_year, reference_period, is_forecast,
                                source_report, release_date,
                                agg_level, state_alpha, state_fips,
                                asd_code, county_ansi,
                                value, unit, cv_pct, short_desc
                            ) VALUES (
                                %(commodity)s, %(class)s, %(statistic)s,
                                %(crop_year)s, %(reference_period)s, %(is_forecast)s,
                                %(source_report)s, %(release_date)s,
                                %(agg_level)s, %(state_alpha)s, %(state_fips)s,
                                %(asd_code)s, %(county_ansi)s,
                                %(value)s, %(unit)s, %(cv_pct)s, %(short_desc)s
                            )
                            ON CONFLICT ON CONSTRAINT crop_production_natural_key
                            DO UPDATE SET
                                value      = EXCLUDED.value,
                                unit       = EXCLUDED.unit,
                                cv_pct     = EXCLUDED.cv_pct,
                                short_desc = EXCLUDED.short_desc,
                                load_ts    = NOW()
                        """, rec)
                        cur.execute('RELEASE SAVEPOINT row_sp')
                        saved += 1
                    except Exception as e:
                        cur.execute('ROLLBACK TO SAVEPOINT row_sp')
                        logger.warning(f'row fail: {e}  ({rec.get("short_desc","")[:40]})')
            conn.commit()
        return saved

    # --------------------------------------------------------
    # Top-level driver
    # --------------------------------------------------------
    def collect(
        self,
        commodities: Optional[List[str]] = None,
        statistics: Optional[List[str]] = None,
        year_ge: int = 2000,
        year_le: Optional[int] = None,
        agg_levels: Iterable[str] = ('NATIONAL', 'STATE'),
    ) -> Dict[str, int]:
        """Pull data for the given commodities/statistics/year range,
        upsert into silver.crop_production. Returns per-commodity row counts."""
        commodities = commodities or list(COMMODITIES.keys())
        statistics  = statistics  or STATISTICS
        year_le     = year_le or date.today().year

        results = {}
        for c in commodities:
            cfg = COMMODITIES[c]
            cls_list = cfg['classes']
            up_list_full = cfg.get('util_practices', [None])
            total = 0
            for stat in statistics:
                # AREA PLANTED isn't split by util_practice (you plant before
                # deciding grain vs silage). NASS returns HTTP 400 for those
                # combinations — skip them and only query the rollup.
                if stat == 'AREA PLANTED' and cfg.get('util_practices'):
                    up_list = ['ALL UTILIZATION PRACTICES']
                else:
                    up_list = up_list_full
                for cls in cls_list:
                    for up in up_list:
                        raw_rows = self._fetch_query(
                            nass_commodity = cfg['nass_commodity'],
                            nass_class     = cls,
                            statisticcat   = stat,
                            util_practice  = up,
                            year_ge        = year_ge,
                            year_le        = year_le,
                            agg_levels     = agg_levels,
                        )
                        parsed = [t for t in (self._transform(r) for r in raw_rows) if t]
                        saved = self.save(parsed)
                        total += saved
                        logger.info(
                            f'{c} {stat} class={cls} up={up} raw={len(raw_rows)} '
                            f'parsed={len(parsed)} saved={saved}'
                        )
            results[c] = total
        return results


# CLI entry
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--commodity', action='append', default=None,
                        help='Commodity key (repeatable). Default: all.')
    parser.add_argument('--year-ge', type=int, default=2000)
    parser.add_argument('--year-le', type=int, default=None)
    parser.add_argument('--agg', default='NATIONAL,STATE',
                        help='Comma-separated agg_levels.')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(message)s')
    c = NASSCropProductionCollector()
    result = c.collect(
        commodities=args.commodity,
        year_ge=args.year_ge,
        year_le=args.year_le,
        agg_levels=tuple(s.strip() for s in args.agg.split(',')),
    )
    print('\nResults:')
    for k, v in result.items():
        print(f'  {k}: {v} rows')
