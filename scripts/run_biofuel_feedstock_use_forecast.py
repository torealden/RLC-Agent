"""Run the biofuel_feedstock_use_forecast callable and PUBLISH it into the forecast book.

This is the impure orchestrator that proves the D5 path end-to-end (design forecast_layer_design_v1.md):

    callable.run()  ->  core.forecast_run (one row, retain gate + assumptions jsonb)
                    ->  silver.soybean_oil_series (banded rank-1 rows, each carrying run_id)
                    ->  collision guard (D7)

  retain=true  (default): rows are written into silver.soybean_oil_series (the book) and will flow to
                          the flat file via write_oils_supply_flat_files.py.
  --scenario   (retain=false): logs the run to core.forecast_run for provenance ONLY; writes NO series
                          rows. Lets a what-if be recorded without polluting the forecast of record.

Idempotent: a retained run first DELETEs this callable's existing rank-1 rows from soybean_oil_series,
then inserts the fresh forecast carrying the NEW run_id. Superseded core.forecast_run rows stay as
history (the ledger is append-only); their series rows are simply replaced.

Usage:
    python scripts/run_biofuel_feedstock_use_forecast.py            # publish (retain=true)
    python scripts/run_biofuel_feedstock_use_forecast.py --scenario # provenance only, no series rows
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(r"C:/dev/RLC-Agent")
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from src.services.database.db_config import get_connection
from src.forecast.guards import assert_no_maxrank_collision
from src.kg.callables import biofuel_feedstock_use_forecast as cb

SERIES_TABLE = "silver.soybean_oil_series"
SERIES_COLS = ['commodity', 'class', 'series', 'marketing_year', 'period_type', 'period',
               'vintage', 'vintage_rank', 'value', 'unit', 'source', 'release_date', 'revision',
               'value_low', 'value_high', 'run_id']
# the key that identifies one balance-sheet cell in the oils flat file
KEY_COLS = ['commodity', 'class', 'series', 'marketing_year', 'period']


def main(retain: bool):
    with get_connection() as conn:
        cur = conn.cursor()
        result = cb.run(cur)
        rows = result['rows']
        a = result['assumptions']
        diag = result['diagnostics']

        # target keys the run wrote / would write (for the ledger)
        target_keys = sorted({r['series'] for r in rows})

        # 1. log the invocation to the provenance ledger (always, retain flags publish vs scenario)
        cur.execute("""
            INSERT INTO core.forecast_run
                (callable, callable_version, assumptions, input_snapshot_ref,
                 produced_vintage, produced_rank, target_keys, retain)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING run_id
        """, (
            cb.CALLABLE, cb.CALLABLE_VERSION,
            json.dumps({'assumptions': a, 'diagnostics_fuels': diag['fuels'],
                        'horizon': diag['horizon'], 'trailing_window': diag['trailing_window']}),
            result['input_snapshot_ref'],
            cb.PRODUCED_VINTAGE, cb.PRODUCED_RANK,
            json.dumps({'series': target_keys, 'horizon': [diag['horizon'][0], diag['horizon'][-1]]}),
            retain,
        ))
        run_id = cur.fetchone()['run_id']
        print(f"core.forecast_run logged: run_id={run_id}  retain={retain}  "
              f"callable={cb.CALLABLE} {cb.CALLABLE_VERSION}")

        if not retain:
            print("--scenario: provenance only, NO series rows written. Rolling nothing back; "
                  "the forecast_run row stays as a logged what-if.")
            print(f"  would have written {len(rows)} rows for series {target_keys}")
            return

        # 2. replace this callable's prior rank-1 forecast rows, then insert fresh with the new run_id
        cur.execute(f"DELETE FROM {SERIES_TABLE} WHERE source=%s AND vintage_rank=%s",
                    (cb.CALLABLE, cb.PRODUCED_RANK))
        deleted = cur.rowcount
        vals = [(
            r['commodity'], r['class'], r['series'], r['marketing_year'], r['period_type'],
            r['period'], r['vintage'], r['vintage_rank'], r['value'], r['unit'], r['source'],
            None, None, r['value_low'], r['value_high'], run_id,
        ) for r in rows]
        placeholders = "(" + ",".join(["%s"] * len(SERIES_COLS)) + ")"
        args_sql = ",".join(cur.mogrify(placeholders, v).decode() for v in vals)
        cur.execute(f"INSERT INTO {SERIES_TABLE} ({','.join(SERIES_COLS)}) VALUES {args_sql}")
        print(f"{SERIES_TABLE}: deleted {deleted} prior rank-1 rows, inserted {len(vals)} fresh "
              f"(vintage={cb.PRODUCED_VINTAGE}, rank={cb.PRODUCED_RANK}, run_id carried)")

        # 3. D7 guard: no key may carry >1 vintage at its max rank (SUMIFS would double-count)
        assert_no_maxrank_collision(cur, SERIES_TABLE, KEY_COLS)
        print("collision guard: 0 collisions on soybean_oil_series")

        # 4. prove the one-hop provenance path row -> run -> assumptions
        cur.execute(f"""
            SELECT s.series, count(*) n, fr.callable, fr.retain
            FROM {SERIES_TABLE} s JOIN core.forecast_run fr ON fr.run_id = s.run_id
            WHERE s.run_id = %s GROUP BY 1,3,4 ORDER BY 1
        """, (run_id,))
        print("published (row -> run join):")
        for r in cur.fetchall():
            print(f"  {r['series']:32s} {r['n']:2d} rows -> run {r['callable']} retain={r['retain']}")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--scenario', action='store_true',
                    help='retain=false: log to core.forecast_run for provenance only, write no series rows')
    args = ap.parse_args()
    main(retain=not args.scenario)
