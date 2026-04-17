"""
Retry-until-success probe of NASS Quickstats for PEANUTS commodity.

Discovers the actual short_desc patterns we need for:
- bronze.nass_processing collector config
- silver.crush_attribute_reference header_pattern rows

Outputs to: scripts/_peanut_nass_probe_output.txt

Retries with 60-second backoff up to ~30 minutes. NASS API is frequently slow
or returns 500 on broad queries.
"""
import os
import sys
import time
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / '.env')

BASE_URL = 'https://quickstats.nass.usda.gov/api/api_GET/'
KEY = os.environ['NASS_API_KEY']
OUTPUT = PROJECT_ROOT / 'scripts' / '_peanut_nass_probe_output.txt'

# All short_desc we want to catalog for the peanut_crush tab
# Queries constrained narrowly to avoid NASS 500s
PROBES = [
    # (label, params)
    ('PEANUTS/CRUSHED/2025',
     dict(commodity_desc='PEANUTS', statisticcat_desc='CRUSHED',
          source_desc='SURVEY', freq_desc='MONTHLY', year='2025',
          agg_level_desc='NATIONAL')),
    ('PEANUTS/MILLED/2025',
     dict(commodity_desc='PEANUTS', statisticcat_desc='MILLED',
          source_desc='SURVEY', freq_desc='MONTHLY', year='2025',
          agg_level_desc='NATIONAL')),
    ('PEANUTS/STOCKS/2025',
     dict(commodity_desc='PEANUTS', statisticcat_desc='STOCKS',
          source_desc='SURVEY', freq_desc='MONTHLY', year='2025',
          agg_level_desc='NATIONAL')),
    ('PEANUTS/USAGE/2025',
     dict(commodity_desc='PEANUTS', statisticcat_desc='USAGE',
          source_desc='SURVEY', freq_desc='MONTHLY', year='2025',
          agg_level_desc='NATIONAL')),
    ('PEANUTS/PRODUCTION/2025',
     dict(commodity_desc='PEANUTS', statisticcat_desc='PRODUCTION',
          source_desc='SURVEY', freq_desc='MONTHLY', year='2025',
          agg_level_desc='NATIONAL')),
    ('PEANUTS/DISAPPEARANCE/2025',
     dict(commodity_desc='PEANUTS', statisticcat_desc='DISAPPEARANCE',
          source_desc='SURVEY', freq_desc='MONTHLY', year='2025',
          agg_level_desc='NATIONAL')),
    ('PEANUTS/DELIVERIES/2025',
     dict(commodity_desc='PEANUTS', statisticcat_desc='DELIVERIES',
          source_desc='SURVEY', freq_desc='MONTHLY', year='2025',
          agg_level_desc='NATIONAL')),
    ('CAKE&MEAL/PEANUT/PRODUCTION/2025',
     dict(commodity_desc='CAKE & MEAL', class_desc='PEANUT',
          statisticcat_desc='PRODUCTION', source_desc='SURVEY',
          freq_desc='MONTHLY', year='2025', agg_level_desc='NATIONAL')),
    ('CAKE&MEAL/PEANUT/STOCKS/2025',
     dict(commodity_desc='CAKE & MEAL', class_desc='PEANUT',
          statisticcat_desc='STOCKS', source_desc='SURVEY',
          freq_desc='MONTHLY', year='2025', agg_level_desc='NATIONAL')),
    ('OIL/PEANUT/PRODUCTION/2025',
     dict(commodity_desc='OIL', class_desc='PEANUT',
          statisticcat_desc='PRODUCTION', source_desc='SURVEY',
          freq_desc='MONTHLY', year='2025', agg_level_desc='NATIONAL')),
    ('OIL/PEANUT/STOCKS/2025',
     dict(commodity_desc='OIL', class_desc='PEANUT',
          statisticcat_desc='STOCKS', source_desc='SURVEY',
          freq_desc='MONTHLY', year='2025', agg_level_desc='NATIONAL')),
]


def try_probe(params, timeout=90):
    params = {**params, 'key': KEY, 'format': 'JSON'}
    try:
        r = requests.get(BASE_URL, params=params, timeout=timeout)
        return r.status_code, r
    except requests.exceptions.Timeout:
        return 'TIMEOUT', None
    except requests.exceptions.RequestException as e:
        return f'ERROR:{e}', None


def main():
    results = {}
    with open(OUTPUT, 'w') as f:
        f.write(f'NASS Peanut Probe\nstarted: {time.ctime()}\n\n')
        f.flush()

    for label, params in PROBES:
        print(f'[{label}]', flush=True)
        attempt = 0
        while attempt < 20:  # ~20 minutes max per probe
            attempt += 1
            status, r = try_probe(params)
            if status == 200:
                try:
                    data = r.json().get('data', [])
                    by_short = {}
                    for d in data:
                        sd = d.get('short_desc', '')
                        u = d.get('unit_desc', '')
                        by_short.setdefault(sd, {'unit': u, 'count': 0, 'sample_value': None})
                        by_short[sd]['count'] += 1
                        if by_short[sd]['sample_value'] is None:
                            by_short[sd]['sample_value'] = d.get('Value')
                    results[label] = {'records': len(data), 'series': by_short}
                    print(f'  OK: {len(data)} records, {len(by_short)} unique short_descs', flush=True)
                    with open(OUTPUT, 'a') as f:
                        f.write(f'=== {label} ===\n')
                        f.write(f'  records: {len(data)}, unique short_descs: {len(by_short)}\n')
                        for sd, meta in sorted(by_short.items()):
                            f.write(f'    [{meta["count"]}x]  {sd}  [{meta["unit"]}]  sample={meta["sample_value"]}\n')
                        f.write('\n')
                        f.flush()
                except Exception as e:
                    print(f'  parse error: {e}', flush=True)
                break
            elif status == 'TIMEOUT':
                print(f'  attempt {attempt}: TIMEOUT, backing off 60s', flush=True)
                time.sleep(60)
            elif status == 500:
                print(f'  attempt {attempt}: HTTP 500, backing off 60s', flush=True)
                time.sleep(60)
            else:
                print(f'  attempt {attempt}: {status}, backing off 60s', flush=True)
                time.sleep(60)
        else:
            print(f'  GAVE UP after {attempt} attempts', flush=True)
            with open(OUTPUT, 'a') as f:
                f.write(f'=== {label} === GAVE UP\n\n')
                f.flush()

    with open(OUTPUT, 'a') as f:
        f.write(f'\nfinished: {time.ctime()}\n')
    print(f'DONE. Output: {OUTPUT}', flush=True)


if __name__ == '__main__':
    main()
