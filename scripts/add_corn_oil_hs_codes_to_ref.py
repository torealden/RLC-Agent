"""
Add missing crude corn oil HS codes to silver.trade_commodity_reference.

Per project_dco_corn_oil_trade_split.md (asymmetric convention):
- Schedule B (US exports): 1515210010 = food-crude, 1515210050 = NESOI/DCO
- HTS (US imports): single 1515210000 bucket — already in ref table
- 1515290020 (once-refined corn oil) was missing from ref table for both flows

After this runs, the Census collector will pick up the new codes on its
next invocation and start populating bronze.census_trade with them.
"""
from dotenv import load_dotenv
load_dotenv()

from src.services.database.db_config import get_connection


# (hs_code_10, hs_code_6, commodity_group, commodity_name, flow_type)
# Conversion factor matches existing rows: KG → 000 Pounds (2.20462/1000)
NEW_ROWS = [
    # Schedule B exports — crude corn oil split (HTS imports has no equivalent split)
    ('1515210010', '151521', 'CORN_OIL', 'Corn oil, crude (food-grade)',  'EXPORTS'),
    ('1515210050', '151521', 'CORN_OIL', 'Corn oil, crude (NESOI / industrial = DCO)', 'EXPORTS'),
    # Once-refined corn oil — both directions (was missing entirely)
    ('1515290020', '151529', 'CORN_OIL', 'Corn oil, once refined', 'EXPORTS'),
    ('1515290020', '151529', 'CORN_OIL', 'Corn oil, once refined', 'IMPORTS'),
]


def main():
    with get_connection() as conn:
        cur = conn.cursor()
        for hs10, hs6, group, name, flow in NEW_ROWS:
            cur.execute(
                """
                INSERT INTO silver.trade_commodity_reference
                  (hs_code_10, hs_code_6, commodity_group, commodity_name,
                   flow_type, source_unit, display_unit, conversion_factor,
                   is_active, notes)
                VALUES
                  (%s, %s, %s, %s, %s, 'KG', '000 Pounds', 0.002204622, true,
                   'KG to 000 Pounds (2.20462/1000) — added 2026-05-04 per DCO trade split convention')
                ON CONFLICT (hs_code_10, flow_type) DO UPDATE SET
                  commodity_group = EXCLUDED.commodity_group,
                  commodity_name = EXCLUDED.commodity_name,
                  is_active = true,
                  notes = EXCLUDED.notes
                """,
                (hs10, hs6, group, name, flow),
            )
        conn.commit()

        # Verify
        import psycopg2.extras
        cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur2.execute("""
            SELECT hs_code_10, flow_type, commodity_name, is_active
            FROM silver.trade_commodity_reference
            WHERE hs_code_6 IN ('151521', '151529')
            ORDER BY hs_code_10, flow_type
        """)
        print("Final corn oil reference table state:")
        for r in cur2.fetchall():
            mark = "✓" if r['is_active'] else "✗"
            print(f"  {mark} {r['hs_code_10']}  {r['flow_type']:8s}  {r['commodity_name']}")


if __name__ == "__main__":
    main()
