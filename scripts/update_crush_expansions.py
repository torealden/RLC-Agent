"""Update crush facilities DB with 2023-2026 new plants and expansions from web research."""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def main():
    conn = psycopg2.connect(
        host=os.getenv('RLC_PG_HOST'), dbname='rlc_commodities',
        user=os.getenv('RLC_PG_USER'), password=os.getenv('RLC_PG_PASSWORD'),
        sslmode='require',
    )
    cur = conn.cursor()

    # (country, company, facility_name, city, state,
    #  tons_per_year, bushels_per_year, refining_tpy, oil_mil_lbs,
    #  has_refining, oilseed, status, year_built, year_closed,
    #  details, investment, source)
    new_facilities = [
        # ── New greenfield plants (now operating) ──
        ('US', 'ADM / Marathon', 'Green Bison Soy Processing', 'Spiritwood', 'ND',
         None, 55_000_000, None, None, True, 'soybeans', 'operating', 2023, None,
         'JV with Marathon. Oil feeds Marathon RD refinery in Dickinson (75 MMgy). NDs first soy crush. 150K bpd.',
         350, 'ASA 2025 update + web research'),

        ('US', 'Bartlett', 'Bartlett Cherryvale', 'Cherryvale', 'KS',
         None, 49_000_000, None, None, True, 'soybeans', 'operating', 2024, None,
         'Grand opening Sep 20, 2024. Nearly doubled KS crush capacity (38% to 70% of state production). 110K bpd.',
         375, 'ASA 2025 update + web research'),

        ('US', 'CGB / MnSP JV', 'North Dakota Soybean Processors', 'Casselton', 'ND',
         None, 42_500_000, None, None, False, 'soybeans', 'operating', 2024, None,
         'Ribbon cutting Aug 2024. 125K bpd. ~75 employees. NDs second crush plant.',
         350, 'ASA 2025 update + web research'),

        ('US', 'Platinum Crush LLC', 'Platinum Crush', 'Alta', 'IA',
         None, 40_000_000, None, 204.5, False, 'soybeans', 'operating', 2024, None,
         '115K bpd. 450M lbs SBO/yr, 847K tons meal/yr, 77K tons hulls/yr. NOPA member. ~60 employees.',
         None, 'ASA 2025 update + web research'),

        ('US', 'Norfolk Crush LLC', 'Norfolk Crush', 'Norfolk', 'NE',
         None, 38_500_000, None, 204.5, False, 'soybeans', 'operating', 2024, None,
         'Fall 2024. 450M lbs crude SBO/yr. 847K tons meal. NOPA member. NEs 4th soy plant. 480 acres.',
         375, 'ASA 2025 update + web research'),

        ('US', 'SDSP / BP JV', 'High Plains Processing', 'Mitchell', 'SD',
         None, 35_000_000, None, None, False, 'soybeans', 'operating', 2025, None,
         'JV of SD Soybean Processors and BP. Multi-seed switch (soy, sunflower, camelina, canola). Oct 2025.',
         500, 'ASA 2025 update + web research'),

        # ── Under construction / coming 2025-2026 ──
        ('US', 'AGP', 'AGP David City', 'David City', 'NE',
         None, 50_000_000, None, None, True, 'soybeans', 'under_construction', None, None,
         'AGPs 11th plant. 150K bpd. Grand opening Jul 2025. 275 acres. Degumming: 1.8M lbs crude SBO/day.',
         700, 'ASA 2025 update + web research'),

        ('US', 'Louis Dreyfus', 'LDC Upper Sandusky', 'Upper Sandusky', 'OH',
         1_500_000, 55_000_000, 320_000, 145.1, True, 'soybeans', 'under_construction', None, None,
         'Announced Oct 2023, groundbreaking Jun 2024. 1.5M MT/yr. 320K MT/yr RBD oil + lecithin. Target Mar 2026.',
         500, 'ASA 2025 update + web research'),

        ('US', 'Bunge Chevron Ag Renewables', 'Bunge Chevron Destrehan', 'Destrehan', 'LA',
         None, 50_000_000, None, None, True, 'soybeans', 'under_construction', None, None,
         'Groundbreaking Mar 2024. Flexible: soy + softseeds + CoverCress + winter canola. Oil for Chevron RD.',
         None, 'ASA 2025 update + web research'),

        ('US', 'United Cooperative', 'United Cooperative Waupun', 'Waupun', 'WI',
         None, 7_500_000, None, None, False, 'soybeans', 'under_construction', None, None,
         'Three-phase: feed + grain handling + soy processing. 67 acres. ~50 jobs. Target Dec 2025.',
         100, 'ASA 2025 update + web research'),

        # ── Announced / planning ──
        ('US', 'White River Soy Processing', 'White River Soy', 'Hershey', 'NE',
         None, None, None, None, False, 'soybeans', 'announced', None, None,
         'Dec 2023 development rights. 3K TPD industrial + 1K TPD specialty multi-seed. Fundraising stage.',
         None, 'ASA 2025 update + web research'),

        ('US', 'Bunge', 'Bunge Morristown SPC', 'Morristown', 'IN',
         None, 4_500_000, None, None, False, 'soybeans', 'under_construction', None, None,
         '$550M soy protein concentrate (SPC) and textured SPC facility. Mid-2025.',
         550, 'ASA 2025 update + web research'),
    ]

    inserted = 0
    for f in new_facilities:
        cur.execute("""
            INSERT INTO reference.oilseed_crush_facilities
                (country, company, facility_name, city, state,
                 crush_capacity_tons_per_year, crush_capacity_bushels_per_year,
                 refining_capacity_tons_per_year, oil_production_capacity_mil_lbs,
                 has_oil_refining, primary_oilseed, status,
                 year_built, year_closed, expansion_details, investment_mil_usd,
                 source, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (country, company, city, state) DO UPDATE SET
                crush_capacity_bushels_per_year = GREATEST(
                    EXCLUDED.crush_capacity_bushels_per_year,
                    reference.oilseed_crush_facilities.crush_capacity_bushels_per_year),
                crush_capacity_tons_per_year = COALESCE(
                    EXCLUDED.crush_capacity_tons_per_year,
                    reference.oilseed_crush_facilities.crush_capacity_tons_per_year),
                has_oil_refining = COALESCE(
                    EXCLUDED.has_oil_refining,
                    reference.oilseed_crush_facilities.has_oil_refining),
                status = EXCLUDED.status,
                expansion_details = EXCLUDED.expansion_details,
                investment_mil_usd = COALESCE(
                    EXCLUDED.investment_mil_usd,
                    reference.oilseed_crush_facilities.investment_mil_usd),
                source = EXCLUDED.source,
                updated_at = NOW()
        """, f)
        inserted += 1

    # Update existing facilities with known capacity bumps from expansions
    expansion_updates = [
        # Cargill Sidney OH - nearly doubled, now ~55M bu/yr
        ('Cargill', 'Sidney', 'OH', 55_000_000,
         'Expanded Sep 2023, nearly doubled capacity. Part of $475M multi-state. Oil refining: 320K MT/yr RBD.'),
        # CHS Fairmont MN - +30%, now ~47M bu/yr (was ~36M)
        ('CHS', 'Fairmont', 'MN', 47_000_000,
         'Phase 1 (2021): +30% crush. Phase 2 (late 2023): +30% throughput, 850K bu storage. $100M+$105M.'),
        # Bunge Council Bluffs IA - now ~77M bu/yr
        ('Bunge', 'Council Bluffs', 'IA', 77_000_000,
         'Expanded 2023 to ~77M bu/yr. Balanced crush and refining capacity.'),
        # ADM Frankfort IN - +10% per 2024 permit modification
        ('ADM', 'Frankfort', 'IN', 53_000_000,
         'Indiana air permit 2024: modification from 1.31M to 1.44M TPY throughput (+10%). Max 900 TPH.'),
    ]

    for company, city, state, new_cap, details in expansion_updates:
        cur.execute("""
            UPDATE reference.oilseed_crush_facilities
            SET crush_capacity_bushels_per_year = GREATEST(%s, crush_capacity_bushels_per_year),
                expansion_details = %s,
                source = 'ASA 2025 update + web research',
                updated_at = NOW()
            WHERE country = 'US' AND company = %s
              AND city ILIKE %s AND state = %s
        """, (new_cap, details, company, f'%{city}%', state))
        if cur.rowcount > 0:
            print(f"  Updated {company} {city} {state}: {new_cap/1e6:.0f}M bu/yr")
        else:
            print(f"  WARNING: No match for {company} {city} {state}")

    conn.commit()

    # Final summary
    cur.execute("""
        SELECT status, COUNT(*),
            COALESCE(SUM(crush_capacity_bushels_per_year), 0) / 1e6 as mil_bu
        FROM reference.oilseed_crush_facilities WHERE country = 'US'
        GROUP BY status ORDER BY mil_bu DESC
    """)
    print(f"\n=== US Soybean Crush Facilities (Final) ===")
    total_plants = 0
    total_cap = 0
    for r in cur.fetchall():
        cnt = r[1]
        cap = float(r[2])
        total_plants += cnt
        total_cap += cap
        print(f"  {r[0]:25s}  {cnt:3d} plants  {cap:>10,.1f} M bu/yr")
    print(f"  {'TOTAL':25s}  {total_plants:3d} plants  {total_cap:>10,.1f} M bu/yr")

    # Compare to USDA
    print(f"\n  USDA MY2025/26 crush forecast: 2,580 M bu")
    print(f"  Our operating capacity:         {total_cap:,.0f} M bu")
    print(f"  Gap to USDA:                    {2580 - total_cap:,.0f} M bu")

    conn.close()
    print(f"\nInserted {inserted} new + updated expansion facilities")


if __name__ == '__main__':
    main()
