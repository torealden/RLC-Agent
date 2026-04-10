"""
Load international oilseed crush facilities into reference.oilseed_crush_facilities.
Covers Argentina, Brazil, and other key crushing countries.

Data sources:
  - Web research on CIARA/ABIOVE/industry reports
  - FAS PSD for national crush volumes (validation)
  - Dropbox balance sheet files for context

Usage:
    python scripts/load_intl_crush_facilities.py
    python scripts/load_intl_crush_facilities.py --country AR
"""

import argparse
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_conn():
    return psycopg2.connect(
        host=os.getenv('RLC_PG_HOST'), dbname='rlc_commodities',
        user=os.getenv('RLC_PG_USER'), password=os.getenv('RLC_PG_PASSWORD'),
        sslmode='require',
    )


# ══════════════════════════════════════════════════════════════════════
# ARGENTINA — Parana River Crush Complex
# ══════════════════════════════════════════════════════════════════════
# Total capacity: ~67 MMT/yr (world's largest oilseed processing complex)
# Actual crush: 36-43 MMT/yr (50-65% utilization due to export taxes + drought)
# Key corridor: Gran Rosario (Timbues to Villa Gobernador Galvez, ~70km)
#
# Sources: CIARA-CEC, Bolsa de Comercio de Rosario, USDA FAS GAIN reports,
#          Oil World, Reuters, S&P Global Platts

ARGENTINA_FACILITIES = [
    # (company, facility_name, city, province, capacity_mt_day, capacity_mt_year,
    #  has_refining, oilseed, status, year_built, notes)

    # ── Gran Rosario Corridor (Santa Fe) ──
    ('Cargill', 'Cargill Quebracho', 'Quebracho', 'Santa Fe',
     12000, 3_600_000, True, 'soybeans', 'operating', None,
     'One of largest single-line crush plants globally. Port facility on Parana.'),
    ('Cargill', 'Cargill Villa Gobernador Galvez', 'Villa Gobernador Galvez', 'Santa Fe',
     6000, 1_800_000, True, 'soybeans', 'operating', None,
     'Southern end of Gran Rosario corridor.'),
    ('Bunge', 'Bunge Timbues', 'Timbues', 'Santa Fe',
     12000, 3_600_000, True, 'soybeans', 'operating', 2018,
     'Massive greenfield plant. Bunge flagship in Argentina. Deep-water port.'),
    ('Bunge', 'Bunge Puerto General San Martin', 'Puerto General San Martin', 'Santa Fe',
     5000, 1_500_000, True, 'soybeans', 'operating', None,
     'Legacy Bunge plant. Port facility.'),
    ('Terminal 6', 'Terminal 6 (Bunge/AGD JV)', 'Puerto General San Martin', 'Santa Fe',
     10000, 3_000_000, True, 'soybeans', 'operating', None,
     'Joint venture Bunge and AGD. Major export terminal + crush complex.'),
    ('AGD', 'AGD Puerto General San Martin', 'Puerto General San Martin', 'Santa Fe',
     8000, 2_400_000, True, 'soybeans', 'operating', None,
     'Aceitera General Deheza. Family-owned Argentine company.'),
    ('AGD', 'AGD General Deheza', 'General Deheza', 'Cordoba',
     6000, 1_800_000, True, 'soybeans', 'operating', None,
     'AGD headquarters and original plant. Inland Cordoba.'),
    ('Louis Dreyfus', 'LDC General Lagos', 'General Lagos', 'Santa Fe',
     10000, 3_000_000, True, 'soybeans', 'operating', None,
     'Major LDC crush and export facility.'),
    ('Louis Dreyfus', 'LDC Timbues', 'Timbues', 'Santa Fe',
     6000, 1_800_000, True, 'soybeans', 'operating', None,
     'LDC deep-water terminal and crush.'),
    ('COFCO / Nidera', 'COFCO Timbues', 'Timbues', 'Santa Fe',
     5000, 1_500_000, True, 'soybeans', 'operating', None,
     'Formerly Nidera, acquired by COFCO (China). Port facility.'),
    ('COFCO / Nidera', 'COFCO San Lorenzo', 'San Lorenzo', 'Santa Fe',
     4000, 1_200_000, True, 'soybeans', 'operating', None,
     'Formerly Noble Group plant, now COFCO.'),
    ('Vicentin', 'Vicentin San Lorenzo', 'San Lorenzo', 'Santa Fe',
     6000, 1_800_000, True, 'soybeans', 'idle', None,
     'Vicentin filed for bankruptcy/restructuring 2020. Status uncertain. Was major crusher.'),
    ('Renova', 'Renova Timbues', 'Timbues', 'Santa Fe',
     10000, 3_000_000, True, 'soybeans', 'operating', 2012,
     'JV Glencore + Vicentin (now Glencore control after Vicentin bankruptcy). Mega-plant.'),
    ('Molinos Agro', 'Molinos San Lorenzo', 'San Lorenzo', 'Santa Fe',
     6000, 1_800_000, True, 'soybeans', 'operating', None,
     'Molinos Agro (Perez Companc family). San Lorenzo cluster.'),
    ('Molinos Agro', 'Molinos Timbues', 'Timbues', 'Santa Fe',
     4000, 1_200_000, True, 'soybeans', 'operating', None,
     'Newer Molinos facility.'),
    ('ACA', 'ACA San Lorenzo', 'San Lorenzo', 'Santa Fe',
     3000, 900_000, True, 'soybeans', 'operating', None,
     'Asociacion de Cooperativas Argentinas. Cooperative crusher.'),
    ('Buyatti', 'Buyatti Puerto General San Martin', 'Puerto General San Martin', 'Santa Fe',
     3000, 900_000, True, 'soybeans', 'operating', None,
     'Buyatti Group.'),
    ('Vicentini', 'Avellaneda Oil', 'Avellaneda', 'Santa Fe',
     2000, 600_000, False, 'soybeans', 'operating', None,
     'Smaller regional crusher.'),

    # ── Bahia Blanca / Southern Buenos Aires ──
    ('Cargill', 'Cargill Bahia Blanca', 'Bahia Blanca', 'Buenos Aires',
     4000, 1_200_000, True, 'soybeans', 'operating', None,
     'Southern port. Also processes sunflower.'),
    ('Oleaginosa Moreno', 'Oleaginosa Moreno Bahia Blanca', 'Bahia Blanca', 'Buenos Aires',
     2500, 750_000, True, 'multi', 'operating', None,
     'Sunflower + soy processing. Also Daireaux plant.'),

    # ── Necochea / Buenos Aires Province ──
    ('Oleaginosa Moreno', 'Oleaginosa Moreno Necochea', 'Necochea', 'Buenos Aires',
     2000, 600_000, True, 'sunflower', 'operating', None,
     'Primary sunflower crusher.'),

    # ── Entre Rios ──
    ('Bunge', 'Bunge Campana', 'Campana', 'Buenos Aires',
     3000, 900_000, True, 'soybeans', 'operating', None,
     'Buenos Aires province facility.'),

    # ── Cordoba ──
    ('Bunge', 'Bunge Cordoba', 'Cordoba', 'Cordoba',
     3000, 900_000, True, 'soybeans', 'operating', None,
     'Interior Cordoba province.'),
]


# ══════════════════════════════════════════════════════════════════════
# BRAZIL — Distributed crush across soy-producing states
# ══════════════════════════════════════════════════════════════════════
# Total capacity: ~65-70 MMT/yr (growing with biodiesel mandate expansion)
# Actual crush: 54-62 MMT/yr (higher utilization than Argentina ~85%)
# Key states: MT (Mato Grosso), PR (Parana), RS (Rio Grande do Sul),
#            GO (Goias), MS (Mato Grosso do Sul), BA (Bahia)
#
# Sources: ABIOVE, CONAB, USDA FAS GAIN, Reuters, industry reports

BRAZIL_FACILITIES = [
    # ── Mato Grosso (largest soy state, ~30% of national production) ──
    ('Bunge', 'Bunge Rondonopolis', 'Rondonopolis', 'MT',
     5000, 1_500_000, True, 'soybeans', 'operating', None,
     'Major Bunge crush in MT. Expanding with biodiesel.'),
    ('Bunge', 'Bunge Nova Mutum', 'Nova Mutum', 'MT',
     3500, 1_050_000, True, 'soybeans', 'operating', None,
     'Interior MT facility.'),
    ('Cargill', 'Cargill Primavera do Leste', 'Primavera do Leste', 'MT',
     4000, 1_200_000, True, 'soybeans', 'operating', None,
     'Eastern MT. Biodiesel co-located.'),
    ('Cargill', 'Cargill Lucas do Rio Verde', 'Lucas do Rio Verde', 'MT',
     3000, 900_000, True, 'soybeans', 'operating', None,
     'Central MT soy belt.'),
    ('ADM', 'ADM Rondonopolis', 'Rondonopolis', 'MT',
     4000, 1_200_000, True, 'soybeans', 'operating', None,
     'ADM major MT crush facility.'),
    ('Amaggi', 'Amaggi Lucas do Rio Verde', 'Lucas do Rio Verde', 'MT',
     4000, 1_200_000, True, 'soybeans', 'operating', None,
     'Andre Maggi Group. Largest Brazilian-owned agribusiness.'),
    ('Amaggi', 'Amaggi Itacoatiara', 'Itacoatiara', 'AM',
     2000, 600_000, False, 'soybeans', 'operating', None,
     'Amazon river port crush. Unusual northern location.'),
    ('Louis Dreyfus', 'LDC Alto Araguaia', 'Alto Araguaia', 'MT',
     3500, 1_050_000, True, 'soybeans', 'operating', None,
     'Southern MT, near GO border.'),
    ('COFCO', 'COFCO Rondonopolis', 'Rondonopolis', 'MT',
     3000, 900_000, True, 'soybeans', 'operating', None,
     'Formerly Noble Group. COFCO expanding in Brazil.'),
    ('Caramuru', 'Caramuru Sorriso', 'Sorriso', 'MT',
     2500, 750_000, True, 'soybeans', 'operating', None,
     'Brazilian-owned. Sorriso is "soy capital of the world".'),

    # ── Parana (2nd largest crush state) ──
    ('Bunge', 'Bunge Ponta Grossa', 'Ponta Grossa', 'PR',
     5000, 1_500_000, True, 'soybeans', 'operating', None,
     'Major crush hub. Rail/road logistics center.'),
    ('Cargill', 'Cargill Ponta Grossa', 'Ponta Grossa', 'PR',
     4000, 1_200_000, True, 'soybeans', 'operating', None,
     'Ponta Grossa crush cluster.'),
    ('Cargill', 'Cargill Mairinque', 'Mairinque', 'SP',
     3500, 1_050_000, True, 'soybeans', 'operating', None,
     'Sao Paulo state. Near consumer market.'),
    ('Louis Dreyfus', 'LDC Ponta Grossa', 'Ponta Grossa', 'PR',
     3000, 900_000, True, 'soybeans', 'operating', None,
     'LDC Parana operations.'),
    ('COAMO', 'COAMO Campo Mourao', 'Campo Mourao', 'PR',
     2500, 750_000, True, 'soybeans', 'operating', None,
     'Major cooperative crusher. COAMO is worlds largest soy cooperative.'),
    ('Cocamar', 'Cocamar Maringa', 'Maringa', 'PR',
     2000, 600_000, True, 'soybeans', 'operating', None,
     'Cooperative. Parana interior.'),
    ('Imcopa', 'Imcopa Araucaria', 'Araucaria', 'PR',
     3000, 900_000, True, 'soybeans', 'operating', None,
     'Near Curitiba. SPC production (specialty soy protein).'),

    # ── Rio Grande do Sul ──
    ('Bunge', 'Bunge Canoas', 'Canoas', 'RS',
     3000, 900_000, True, 'soybeans', 'operating', None,
     'Porto Alegre metro. Legacy plant.'),
    ('Olvebra / Oleoplan', 'Oleoplan Porto Alegre', 'Porto Alegre', 'RS',
     2000, 600_000, True, 'soybeans', 'operating', None,
     'RS domestic market.'),
    ('Granol', 'Granol Cachoeira do Sul', 'Cachoeira do Sul', 'RS',
     2000, 600_000, True, 'soybeans', 'operating', None,
     'Brazilian-owned processor. Biodiesel producer.'),
    ('Camera', 'Camera Santa Rosa', 'Santa Rosa', 'RS',
     2000, 600_000, True, 'soybeans', 'operating', None,
     'RS northwest. Cooperative.'),
    ('BSBios', 'BSBios Passo Fundo', 'Passo Fundo', 'RS',
     2500, 750_000, True, 'soybeans', 'operating', None,
     'ECB Group. Major biodiesel producer. Soy crush + BD.'),

    # ── Goias ──
    ('Caramuru', 'Caramuru Itumbiara', 'Itumbiara', 'GO',
     4000, 1_200_000, True, 'soybeans', 'operating', None,
     'Caramuru headquarters. Major crush + refining complex.'),
    ('Cargill', 'Cargill Rio Verde', 'Rio Verde', 'GO',
     3000, 900_000, True, 'soybeans', 'operating', None,
     'Central GO soy belt.'),
    ('Granol', 'Granol Anapolis', 'Anapolis', 'GO',
     3000, 900_000, True, 'soybeans', 'operating', None,
     'Near Goiania. Soy + biodiesel.'),
    ('ADM', 'ADM Rio Verde', 'Rio Verde', 'GO',
     2000, 600_000, True, 'soybeans', 'operating', None,
     'ADM GO operations.'),

    # ── Mato Grosso do Sul ──
    ('ADM', 'ADM Campo Grande', 'Campo Grande', 'MS',
     3000, 900_000, True, 'soybeans', 'operating', None,
     'State capital. ADM operations.'),
    ('Bunge', 'Bunge Dourados', 'Dourados', 'MS',
     2500, 750_000, True, 'soybeans', 'operating', None,
     'Southern MS. Near Paraguay border.'),
    ('Louis Dreyfus', 'LDC Tres Lagoas', 'Tres Lagoas', 'MS',
     2000, 600_000, True, 'soybeans', 'operating', None,
     'Eastern MS.'),

    # ── Bahia / MATOPIBA ──
    ('Bunge', 'Bunge Luiz Eduardo Magalhaes', 'Luiz Eduardo Magalhaes', 'BA',
     3000, 900_000, True, 'soybeans', 'operating', None,
     'MATOPIBA frontier. Western Bahia soy expansion.'),
    ('Cargill', 'Cargill Barreiras', 'Barreiras', 'BA',
     2000, 600_000, True, 'soybeans', 'operating', None,
     'Western Bahia.'),

    # ── Minas Gerais ──
    ('ADM', 'ADM Uberlandia', 'Uberlandia', 'MG',
     3000, 900_000, True, 'soybeans', 'operating', None,
     'Triangulo Mineiro. Near Goias border.'),

    # ── Recent expansions / new ──
    ('Cargill', 'Cargill Sorriso', 'Sorriso', 'MT',
     3000, 900_000, True, 'soybeans', 'under_construction', None,
     'New Cargill crush in soy capital. Announced 2024.'),
    ('Bunge', 'Bunge Catalao', 'Catalao', 'GO',
     3000, 900_000, True, 'soybeans', 'under_construction', None,
     'New Bunge plant in eastern Goias. Part of Brazil expansion.'),
]


def load_country(cur, country_code, facilities):
    """Load a list of facility tuples into the database."""
    inserted = 0
    for f in facilities:
        company, facility_name, city, state, cap_tpd, cap_tpy, \
            has_refining, oilseed, status, year_built, notes = f

        # Convert MT/yr to bushels/yr for soybeans (1 MT = 36.74 bu)
        cap_bpy = None
        if cap_tpy and oilseed == 'soybeans':
            cap_bpy = int(cap_tpy * 36.74)

        cur.execute("""
            INSERT INTO reference.oilseed_crush_facilities
                (country, company, facility_name, city, state,
                 crush_capacity_tons_per_day, crush_capacity_tons_per_year,
                 crush_capacity_bushels_per_year,
                 has_oil_refining, primary_oilseed, status,
                 year_built, notes, source, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (country, company, city, state) DO UPDATE SET
                crush_capacity_tons_per_day = COALESCE(EXCLUDED.crush_capacity_tons_per_day,
                    reference.oilseed_crush_facilities.crush_capacity_tons_per_day),
                crush_capacity_tons_per_year = COALESCE(EXCLUDED.crush_capacity_tons_per_year,
                    reference.oilseed_crush_facilities.crush_capacity_tons_per_year),
                crush_capacity_bushels_per_year = COALESCE(EXCLUDED.crush_capacity_bushels_per_year,
                    reference.oilseed_crush_facilities.crush_capacity_bushels_per_year),
                has_oil_refining = EXCLUDED.has_oil_refining,
                status = EXCLUDED.status,
                notes = EXCLUDED.notes,
                source = EXCLUDED.source,
                updated_at = NOW()
        """, (country_code, company, facility_name, city, state,
              cap_tpd, cap_tpy, cap_bpy,
              has_refining, oilseed, status,
              year_built, notes,
              'CIARA/ABIOVE/industry reports + web research'))
        inserted += 1

    return inserted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--country', help='Only load this country (AR, BR)')
    args = parser.parse_args()

    conn = get_conn()
    cur = conn.cursor()

    countries = {
        'AR': ('Argentina', ARGENTINA_FACILITIES),
        'BR': ('Brazil', BRAZIL_FACILITIES),
    }

    if args.country:
        countries = {args.country: countries[args.country]}

    for code, (name, facilities) in countries.items():
        n = load_country(cur, code, facilities)
        print(f"{name}: loaded {n} facilities")

    conn.commit()

    # Summary
    cur.execute("""
        SELECT country, status, COUNT(*),
            COALESCE(SUM(crush_capacity_tons_per_year), 0) / 1e6 as mmt
        FROM reference.oilseed_crush_facilities
        WHERE country IN ('AR', 'BR')
        GROUP BY country, status
        ORDER BY country, mmt DESC
    """)
    print(f"\n=== International Crush Facilities ===")
    for r in cur.fetchall():
        print(f"  {r[0]:3s}  {r[1]:25s}  {r[2]:3d} plants  {float(r[3]):>6.1f} MMT/yr")

    # Totals per country
    for code in ['AR', 'BR']:
        cur.execute("""
            SELECT COUNT(*), SUM(crush_capacity_tons_per_year) / 1e6
            FROM reference.oilseed_crush_facilities WHERE country = %s
        """, (code,))
        r = cur.fetchone()
        print(f"\n  {code} TOTAL: {r[0]} plants, {float(r[1] or 0):,.1f} MMT/yr")

    # Grand total across all countries
    cur.execute("""
        SELECT country, COUNT(*), COALESCE(SUM(crush_capacity_tons_per_year), 0) / 1e6
        FROM reference.oilseed_crush_facilities
        GROUP BY country ORDER BY 3 DESC
    """)
    print(f"\n=== Global Crush Facility Summary ===")
    for r in cur.fetchall():
        print(f"  {r[0]:3s}  {r[1]:3d} plants  {float(r[2]):>8.1f} MMT/yr")

    conn.close()


if __name__ == '__main__':
    main()
