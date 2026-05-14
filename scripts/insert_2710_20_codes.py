"""Insert HS 2710.20.x codes (biodiesel/petroleum diesel blends) into
silver.trade_commodity_reference.

Background: HS 3826 is pure-or-mostly-biodiesel. HS 2710.20.x is petroleum
diesel BLENDED WITH biodiesel — where blended-fuel imports land in the
customs schedule, including potentially some HVO RD that's blended.

HTSUS 2710.20 sub-codes (effective 2012+):
  2710.20.05  — Petroleum oils mixed w/ biodiesel, <70% petroleum oils
                (i.e., ≥30% biodiesel content; high-biodiesel blends)
  2710.20.10  — Mixed w/ biodiesel, ≥70% petroleum oils, ULSD (≤15 ppm S)
  2710.20.15  — Mixed w/ biodiesel, ≥70% petroleum oils, diesel >15 ppm S
  2710.20.25  — Mixed w/ biodiesel, other distillate fuel oils
  2710.20.90  — Mixed w/ biodiesel, other (residual etc.)

Commodity group assigned RENEWABLE_DIESEL so volumes appear in the RD
import flow alongside the HS 3826 split — Tore's purpose is to capture
RD trade exposure that's hidden in petroleum-classified blends.

Schedule B (export) uses similar but not identical codes. Including both
flow types; collector will skip if Census returns no data.
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
from dotenv import load_dotenv; load_dotenv('.env')
from src.services.database.db_config import get_connection
import psycopg2.extras

# HS_CODE_10, HS_CODE_6, group, name, source_unit, display_unit, cf, notes
NEW_CODES = [
    ('2710200500', '271020', 'RENEWABLE_DIESEL',
     'Petroleum oil w/ biodiesel, <70% petroleum (high-BD blends)',
     'LT', '000 gallons', 0.000264172,
     'High-biodiesel blends — potential RD/B100 home in customs schedule'),
    ('2710201000', '271020', 'RENEWABLE_DIESEL',
     'Petroleum oil w/ biodiesel, >=70% petroleum, ULSD <=15 ppm S',
     'LT', '000 gallons', 0.000264172,
     'Low-biodiesel ULSD blends'),
    ('2710201500', '271020', 'RENEWABLE_DIESEL',
     'Petroleum oil w/ biodiesel, >=70% petroleum, diesel >15 ppm S',
     'LT', '000 gallons', 0.000264172,
     'Low-biodiesel higher-sulfur diesel blends'),
    ('2710202500', '271020', 'RENEWABLE_DIESEL',
     'Petroleum oil w/ biodiesel, other distillate fuel oils',
     'LT', '000 gallons', 0.000264172,
     NULL := None),
    ('2710209000', '271020', 'RENEWABLE_DIESEL',
     'Petroleum oil w/ biodiesel, other (residual etc.)',
     'LT', '000 gallons', 0.000264172,
     'Catch-all'),
]

with get_connection() as conn:
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    inserted = 0
    skipped = 0
    for entry in NEW_CODES:
        hs10, hs6, group, name, source_unit, display_unit, cf, notes = entry
        for flow in ('IMPORTS', 'EXPORTS'):
            try:
                cur.execute("""
                    INSERT INTO silver.trade_commodity_reference
                        (hs_code_10, hs_code_6, commodity_group, commodity_name,
                         flow_type, source_unit, display_unit, conversion_factor,
                         is_active, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, true, %s)
                    ON CONFLICT DO NOTHING
                """, (hs10, hs6, group, name, flow, source_unit, display_unit, cf, notes))
                if cur.rowcount:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"  FAIL {hs10}/{flow}: {e}", flush=True)
    conn.commit()
    print(f"Inserted {inserted} rows, skipped {skipped} (already existed)", flush=True)

    cur.execute("""SELECT hs_code_10, commodity_group, flow_type FROM silver.trade_commodity_reference
WHERE hs_code_10 LIKE '271020%' ORDER BY hs_code_10, flow_type""")
    print("\n=== HS 271020 codes now in reference table ===", flush=True)
    for r in cur.fetchall():
        print(f"  {r['hs_code_10']} {r['commodity_group']:18s} {r['flow_type']}", flush=True)
