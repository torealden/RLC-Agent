"""EIA v2 API collector — comprehensive energy series coverage.

Replaces the older eia_petroleum_collector + ad-hoc ingestion paths with a
single unified collector that:
  - Hits the EIA v2 API (https://api.eia.gov/v2/...) with proper pagination
  - Lands all observations in bronze.eia_observations (mig 127)
  - Supports daily / weekly / monthly frequencies in one schema
  - Backfills from 1990-01-01 per Tore (2026-05-31)

Series catalog covers (~60 series):
  Crude prices:        WTI Cushing daily, Brent daily
  Refined wholesale:   RBOB Gulf+NYH, ULSD Gulf+NYH, Jet A Gulf,
                       Propane Mont Belvieu
  Retail prices:       US retail gasoline / diesel / ULSD
  Crude balance:       production, imports, exports, stocks (incl. Cushing, SPR)
  Refined balance:     gasoline/distillate/jet/propane stocks + supplied + production
  Refining ops:        crude inputs, utilization
  Ethanol:             production, stocks, blender input
  Natural gas:         Henry Hub daily, L48 + regional working storage
"""
from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / '.env')

from src.services.database.db_config import get_connection

logger = logging.getLogger(__name__)

EIA_V2_BASE = 'https://api.eia.gov/v2'


# ============================================================
# Series catalog
# ============================================================
# Each entry:
#   key (slug)        — short human name used in queries
#   series_id         — EIA series code (used as facets[series][] value)
#   route             — v2 API endpoint path (e.g., 'petroleum/pri/spt/data')
#   frequency         — 'daily', 'weekly', 'monthly'
#   description       — short text description
#   facets            — additional facets if needed beyond series (typically None)
#
# Naming convention: snake_case keys grouped by category for readability.

SERIES_CATALOG: Dict[str, Dict] = {
    # ============ PETROLEUM PRICES (DAILY) ============
    'wti_cushing':            {'series_id': 'RWTC',     'route': 'petroleum/pri/spt/data',  'frequency': 'daily',
                               'description': 'WTI Cushing spot $/bbl'},
    'brent':                  {'series_id': 'RBRTE',    'route': 'petroleum/pri/spt/data',  'frequency': 'daily',
                               'description': 'Europe Brent spot $/bbl'},

    # ============ NATURAL GAS PRICES (DAILY) ============
    'henry_hub':              {'series_id': 'RNGWHHD',  'route': 'natural-gas/pri/fut/data', 'frequency': 'daily',
                               'description': 'Henry Hub spot $/MMBtu'},

    # ============ REFINED WHOLESALE SPOT (WEEKLY) ============
    # Note: RBOB Gulf spot does not exist as an EIA series — EIA only publishes
    # RBOB at LA and NY Harbor (and Gulf conventional, which is a different
    # product). For Gulf gasoline benchmark use ULSD Gulf paired with the
    # NY Harbor RBOB spread.
    'rbob_la':                {'series_id': 'EER_EPMRR_PF4_Y05LA_DPG', 'route': 'petroleum/pri/spt/data', 'frequency': 'weekly',
                               'description': 'LA Reformulated RBOB spot $/gal'},
    'ulsd_gulf':              {'series_id': 'EER_EPD2DXL0_PF4_RGC_DPG',  'route': 'petroleum/pri/spt/data', 'frequency': 'weekly',
                               'description': 'ULSD Gulf Coast spot $/gal'},
    'ulsd_nyh':               {'series_id': 'EER_EPD2DXL0_PF4_Y35NY_DPG','route': 'petroleum/pri/spt/data', 'frequency': 'weekly',
                               'description': 'ULSD NY Harbor spot $/gal'},
    'jet_a_gulf':             {'series_id': 'EER_EPJK_PF4_RGC_DPG',     'route': 'petroleum/pri/spt/data', 'frequency': 'weekly',
                               'description': 'Jet A Gulf Coast spot $/gal'},
    'propane_mb':             {'series_id': 'EER_EPLLPA_PF4_Y44MB_DPG', 'route': 'petroleum/pri/spt/data', 'frequency': 'weekly',
                               'description': 'Mont Belvieu propane spot $/gal'},

    # ============ RETAIL PRICES (WEEKLY) ============
    'retail_gasoline_us':     {'series_id': 'EMM_EPM0_PTE_NUS_DPG',     'route': 'petroleum/pri/gnd/data', 'frequency': 'weekly',
                               'description': 'US retail gasoline avg $/gal'},
    'retail_diesel_us':       {'series_id': 'EMD_EPD2D_PTE_NUS_DPG',    'route': 'petroleum/pri/gnd/data', 'frequency': 'weekly',
                               'description': 'US retail No 2 diesel avg $/gal'},

    # ============ CRUDE BALANCE (WEEKLY) ============
    'crude_production_us':    {'series_id': 'WCRFPUS2',  'route': 'petroleum/sum/sndw/data', 'frequency': 'weekly',
                               'description': 'US crude production kbd'},
    'crude_imports_us':       {'series_id': 'WCRIMUS2',  'route': 'petroleum/sum/sndw/data', 'frequency': 'weekly',
                               'description': 'US crude imports kbd'},
    'crude_exports_us':       {'series_id': 'WCREXUS2',  'route': 'petroleum/sum/sndw/data', 'frequency': 'weekly',
                               'description': 'US crude exports kbd'},
    'crude_stocks_us':        {'series_id': 'WCESTUS1',  'route': 'petroleum/stoc/wstk/data', 'frequency': 'weekly',
                               'description': 'US crude stocks ex-SPR kb'},
    'crude_stocks_cushing':   {'series_id': 'W_EPC0_SAX_YCUOK_MBBL', 'route': 'petroleum/stoc/wstk/data', 'frequency': 'weekly',
                               'description': 'Cushing crude stocks kb'},
    'crude_stocks_spr':       {'series_id': 'WCSSTUS1',  'route': 'petroleum/stoc/wstk/data', 'frequency': 'weekly',
                               'description': 'US SPR stocks kb'},

    # ============ GASOLINE BALANCE (WEEKLY) ============
    'gasoline_stocks_us':     {'series_id': 'WGTSTUS1',  'route': 'petroleum/stoc/wstk/data', 'frequency': 'weekly',
                               'description': 'US gasoline stocks kb'},
    'gasoline_production_us': {'series_id': 'WGFRPUS2',  'route': 'petroleum/sum/sndw/data', 'frequency': 'weekly',
                               'description': 'US gasoline production kbd'},
    'gasoline_supplied_us':   {'series_id': 'WGFUPUS2',  'route': 'petroleum/sum/sndw/data', 'frequency': 'weekly',
                               'description': 'US gasoline product supplied (demand) kbd'},
    'gasoline_imports_us':    {'series_id': 'WGFIMUS2',  'route': 'petroleum/sum/sndw/data', 'frequency': 'weekly',
                               'description': 'US gasoline imports kbd'},

    # ============ DISTILLATE BALANCE (WEEKLY) ============
    'distillate_stocks_us':   {'series_id': 'WDISTUS1',  'route': 'petroleum/stoc/wstk/data', 'frequency': 'weekly',
                               'description': 'US distillate stocks kb'},
    'distillate_production_us':{'series_id': 'WDIRPUS2', 'route': 'petroleum/sum/sndw/data', 'frequency': 'weekly',
                                'description': 'US distillate production kbd'},
    'distillate_supplied_us': {'series_id': 'WDIUPUS2',  'route': 'petroleum/sum/sndw/data', 'frequency': 'weekly',
                               'description': 'US distillate product supplied kbd'},
    'distillate_imports_us':  {'series_id': 'WDIIMUS2',  'route': 'petroleum/sum/sndw/data', 'frequency': 'weekly',
                               'description': 'US distillate imports kbd'},

    # ============ JET FUEL BALANCE (WEEKLY) ============
    'jet_stocks_us':          {'series_id': 'WKJSTUS1', 'route': 'petroleum/stoc/wstk/data', 'frequency': 'weekly',
                               'description': 'US jet fuel stocks kb'},
    'jet_supplied_us':        {'series_id': 'WKJUPUS2',  'route': 'petroleum/sum/sndw/data', 'frequency': 'weekly',
                               'description': 'US jet fuel supplied kbd'},
    'jet_production_us':      {'series_id': 'WKJRPUS2',  'route': 'petroleum/sum/sndw/data', 'frequency': 'weekly',
                               'description': 'US jet fuel production kbd'},

    # ============ PROPANE BALANCE (WEEKLY) ============
    'propane_stocks_us':      {'series_id': 'W_EPLLPZ_SAE_NUS_MBBL', 'route': 'petroleum/stoc/wstk/data', 'frequency': 'weekly',
                               'description': 'US propane/propylene stocks kb'},

    # ============ REFINERY OPS (WEEKLY) ============
    'refinery_crude_input':   {'series_id': 'WCRRIUS2',  'route': 'petroleum/sum/sndw/data', 'frequency': 'weekly',
                               'description': 'US refinery crude input kbd'},
    'refinery_utilization':   {'series_id': 'WPULEUS3',  'route': 'petroleum/sum/sndw/data', 'frequency': 'weekly',
                               'description': 'US refinery utilization pct'},

    # ============ ETHANOL (WEEKLY) ============
    'ethanol_production_us':  {'series_id': 'W_EPOOXE_YOP_NUS_MBBLD', 'route': 'petroleum/sum/sndw/data', 'frequency': 'weekly',
                               'description': 'US ethanol production kbd'},
    'ethanol_stocks_us':      {'series_id': 'W_EPOOXE_SAE_NUS_MBBL',  'route': 'petroleum/stoc/wstk/data', 'frequency': 'weekly',
                               'description': 'US ethanol stocks kb'},
    'ethanol_blender_input':  {'series_id': 'W_EPOOX_YIB_NUS_MBBLD',  'route': 'petroleum/sum/sndw/data', 'frequency': 'weekly',
                               'description': 'US ethanol blender input kbd'},

    # ============ NATURAL GAS BALANCE (WEEKLY) ============
    'natgas_storage_l48':     {'series_id': 'NW2_EPG0_SWO_R48_BCF',  'route': 'natural-gas/stor/wkly/data', 'frequency': 'weekly',
                               'description': 'L48 natural gas working storage bcf'},
    'natgas_storage_east':    {'series_id': 'NW2_EPG0_SWO_R31_BCF',  'route': 'natural-gas/stor/wkly/data', 'frequency': 'weekly',
                               'description': 'East natural gas storage bcf'},
    'natgas_storage_midwest': {'series_id': 'NW2_EPG0_SWO_R32_BCF',  'route': 'natural-gas/stor/wkly/data', 'frequency': 'weekly',
                               'description': 'Midwest natural gas storage bcf'},
    'natgas_storage_south_central': {'series_id': 'NW2_EPG0_SWO_R34_BCF', 'route': 'natural-gas/stor/wkly/data', 'frequency': 'weekly',
                                     'description': 'South Central natural gas storage bcf'},
    'natgas_storage_mountain': {'series_id': 'NW2_EPG0_SWO_R35_BCF', 'route': 'natural-gas/stor/wkly/data', 'frequency': 'weekly',
                                'description': 'Mountain natural gas storage bcf'},
    'natgas_storage_pacific': {'series_id': 'NW2_EPG0_SWO_R36_BCF',  'route': 'natural-gas/stor/wkly/data', 'frequency': 'weekly',
                               'description': 'Pacific natural gas storage bcf'},
}


# ============================================================
# Collector
# ============================================================
@dataclass
class EIAV2Config:
    api_key: str
    timeout: int = 60
    rate_limit_sec: float = 0.5
    max_rows_per_call: int = 5000


class EIAV2Collector:
    """Comprehensive EIA v2 API collector. Long/tidy bronze persistence."""

    def __init__(self, config: Optional[EIAV2Config] = None):
        if config is None:
            key = os.environ.get('EIA_API_KEY', '')
            if not key:
                raise RuntimeError('EIA_API_KEY not set in env')
            config = EIAV2Config(api_key=key)
        self.config = config
        self.session = requests.Session()

    def _fetch_series(self, series_key: str, start: date, end: date) -> List[Dict]:
        """Hit EIA v2 for one series. Handles pagination. Returns rich rows."""
        spec = SERIES_CATALOG[series_key]
        url = f'{EIA_V2_BASE}/{spec["route"]}'
        all_rows: List[Dict] = []
        offset = 0
        while True:
            params = {
                'api_key': self.config.api_key,
                'frequency': spec['frequency'],
                'data[0]': 'value',
                'facets[series][]': spec['series_id'],
                'start': start.strftime('%Y-%m-%d'),
                'end': end.strftime('%Y-%m-%d'),
                'sort[0][column]': 'period',
                'sort[0][direction]': 'asc',
                'offset': offset,
                'length': self.config.max_rows_per_call,
            }
            try:
                r = self.session.get(url, params=params, timeout=self.config.timeout)
            except requests.RequestException as e:
                logger.warning(f'{series_key} HTTP error: {e}')
                break
            if r.status_code != 200:
                logger.warning(f'{series_key} HTTP {r.status_code}: {r.text[:200]}')
                break
            try:
                payload = r.json()
            except Exception:
                logger.warning(f'{series_key}: bad JSON')
                break
            resp = payload.get('response', {})
            data = resp.get('data', [])
            all_rows.extend(data)
            total = resp.get('total', 0)
            try: total = int(total)
            except Exception: total = 0
            offset += self.config.max_rows_per_call
            if not data or offset >= total:
                break
        return all_rows

    def _save_rows(self, series_key: str, rows: List[Dict]) -> int:
        """Bulk upsert into bronze.eia_observations via execute_values."""
        if not rows:
            return 0
        spec = SERIES_CATALOG[series_key]
        import json as _json
        from psycopg2.extras import execute_values

        # Pre-filter + shape into tuples
        batch = []
        for raw in rows:
            val = raw.get('value')
            if val in (None, '', '.'):
                continue
            try:
                v = float(val)
            except (TypeError, ValueError):
                continue
            period = raw.get('period')
            if not period:
                continue
            batch.append((
                spec['series_id'], period, v,
                raw.get('units'),
                spec['frequency'],
                series_key,
                spec['description'],
                spec['route'],
                _json.dumps(raw),
            ))

        if not batch:
            return 0

        sql = """
            INSERT INTO bronze.eia_observations (
                series_id, period, value, unit, frequency,
                series_name, description, api_route, raw_payload
            ) VALUES %s
            ON CONFLICT (series_id, period) DO UPDATE SET
                value        = EXCLUDED.value,
                unit         = EXCLUDED.unit,
                series_name  = EXCLUDED.series_name,
                description  = EXCLUDED.description,
                raw_payload  = EXCLUDED.raw_payload,
                collected_at = NOW()
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, batch, page_size=1000)
            conn.commit()
        return len(batch)

    def collect(self, series_keys: Optional[List[str]] = None,
                start: Optional[date] = None,
                end: Optional[date] = None) -> Dict[str, int]:
        """Pull + save for the given series_keys (default: all in catalog)."""
        series_keys = series_keys or list(SERIES_CATALOG.keys())
        start = start or date(1990, 1, 1)
        end = end or date.today()
        results = {}
        for sk in series_keys:
            rows = self._fetch_series(sk, start, end)
            n = self._save_rows(sk, rows)
            results[sk] = n
            logger.info(f'{sk}: fetched={len(rows)} saved={n}')
        return results


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--series', action='append', help='Repeatable. Default: all.')
    parser.add_argument('--start', default='1990-01-01')
    parser.add_argument('--end', default=None)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    c = EIAV2Collector()
    start = datetime.strptime(args.start, '%Y-%m-%d').date()
    end = datetime.strptime(args.end, '%Y-%m-%d').date() if args.end else date.today()
    res = c.collect(args.series, start, end)
    print('\nResults:')
    for sk, n in res.items():
        print(f'  {sk}: {n} rows')
