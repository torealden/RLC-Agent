"""
Seed silver.facility_state with the AGP Eagle Grove baseline row.

This is the *initial* state for facility-agent simulation. As we wire up live
data sources (NOPA monthly crush capacity-share, AMS basis, AGP daily bid),
this row will be updated daily by an upstream loader. For now we plant a
reasonable baseline derived from:
  - reference.oilseed_crush_facilities (nameplate capacity)
  - bronze.state_air_permits (Title V rated capacities)
  - silver.ia_implied_monthly_crush (NOPA-derived state share)
  - oilseed_crush.md diagnostic ratios (yield, hexane, throughput days)

All values are baselines — flagged via 'updated_at' so we know when to refresh.
"""
from datetime import date
from dotenv import load_dotenv
load_dotenv()

from src.services.database.db_config import get_connection


FACILITY_ID = "ia.agp_eagle_grove"

# Derived from the v2 Title V extraction (extractor EU-036 = 153.75 tons/hr)
# and reference table (operating_days_year=350)
RATED_TONS_PER_HOUR = 153.75
OPERATING_HOURS_PER_DAY = 24.0
OPERATING_DAYS_YEAR = 350

NAMEPLATE_TPD = RATED_TONS_PER_HOUR * OPERATING_HOURS_PER_DAY  # = 3690 tpd at design
ANNUAL_TONS = NAMEPLATE_TPD * OPERATING_DAYS_YEAR              # = 1,291,500 t/yr
ANNUAL_BU = ANNUAL_TONS * 36.74                                # = 47.4 mil bu/yr at 100% util

# Operating assumption: facilities typically run at ~90-95% of design rated.
# Use 92% as a baseline until we wire NOPA-share for facility-specific actuals.
ASSUMED_UTILIZATION = 0.92
CURRENT_TPD = NAMEPLATE_TPD * ASSUMED_UTILIZATION


def main():
    with get_connection() as conn:
        cur = conn.cursor()

        # Upsert (facility_id, as_of_date) — primary key per spec
        cur.execute("""
            INSERT INTO silver.facility_state (
                facility_id, as_of_date,
                bean_inventory_bu,
                oil_inventory_lbs,
                meal_inventory_tons,
                bean_purchases_committed_bu,
                oil_sold_forward_lbs,
                meal_sold_forward_tons,
                days_of_coverage,
                current_crush_rate_tpd,
                last_30d_crush_bu,
                updated_at
            ) VALUES (
                %s, %s,
                0, 0, 0,
                0, 0, 0,
                0,
                %s,
                %s,
                NOW()
            )
            ON CONFLICT (facility_id) DO UPDATE SET
                as_of_date             = EXCLUDED.as_of_date,
                current_crush_rate_tpd = EXCLUDED.current_crush_rate_tpd,
                last_30d_crush_bu      = EXCLUDED.last_30d_crush_bu,
                updated_at             = NOW()
        """, (
            FACILITY_ID,
            date.today(),
            CURRENT_TPD,
            CURRENT_TPD * 36.74 * 30,   # 30 days at current rate, in bushels
        ))
        conn.commit()
        print(f"Seeded silver.facility_state for {FACILITY_ID}")
        print(f"  current_crush_rate_tpd: {CURRENT_TPD:.1f}")
        print(f"  implied annual:         {CURRENT_TPD * OPERATING_DAYS_YEAR / 1000:.0f} kt = "
              f"{CURRENT_TPD * OPERATING_DAYS_YEAR * 36.74 / 1e6:.1f} mil bu/yr at {ASSUMED_UTILIZATION:.0%} util")


if __name__ == "__main__":
    main()
