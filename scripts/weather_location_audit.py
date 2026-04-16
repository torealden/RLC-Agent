"""
Weather Location Coverage Audit
===============================

Diffs cities/regions mentioned in meteorologist emails (bronze.weather_email_extract)
against the canonical weather_location + weather_location_alias tables.

Two products:
  1. Coverage report — which mentioned entities are matched vs unmatched
  2. Auto-alias suggestions — canonical entities with proximity-based matches

Also seeds obvious aliases that are missing (regions → nearest city, state names, etc).

Design note:
  - Regions like "Corn Belt", "Midwest", "Great Plains", "Delta" don't have a single
    weather_location point — they're aggregations. We log them as `REGION_PLACEHOLDER`
    instead of forcing a lat/lon match. Future enhancement: aggregate multiple city
    observations into a region index.
"""

import os
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / '.env')


def connect():
    return psycopg2.connect(
        host=os.environ['RLC_PG_HOST'],
        port=os.environ.get('RLC_PG_PORT', 5432),
        database=os.environ.get('RLC_PG_DATABASE', 'rlc_commodities'),
        user=os.environ['RLC_PG_USER'],
        password=os.environ['RLC_PG_PASSWORD'],
        sslmode='require',
    )


# Regions the emails frequently name. These have no single city point — flag them.
KNOWN_REGIONS = {
    'corn belt', 'soy belt', 'soybean belt', 'midwest', 'great plains',
    'delta', 'southeast', 'mid-south', 'pacific northwest', 'pnw',
    'northern plains', 'southern plains', 'wheat belt',
}

# Curated aliases we want to add. These map real email mentions to canonical
# weather_location IDs (picked as "best representative city within the region").
ALIAS_SEEDS = [
    # Brazil state/region -> representative production city
    ('mato grosso',      'sorriso_mt'),
    ('mt',               'sorriso_mt'),
    ('parana',           'londrina_pr'),
    ('pr',               'londrina_pr'),
    ('rio grande do sul', 'porto_alegre_rs'),
    ('rs',               'porto_alegre_rs'),
    ('goias',            'goiania_go'),
    ('go',               'goiania_go'),
    ('santa fe',         'rosario_sf'),
    ('sf',               'rosario_sf'),
    ('sao paulo',        'sao_paulo_sp'),
    ('sp',               'sao_paulo_sp'),
    # Argentina
    ('buenos aires',     'buenos_aires_ar'),
    ('cordoba',          'cordoba_ar'),
    # US states/commonly-written abbreviations
    ('la',               'new_orleans_la'),
    ('louisiana',        'new_orleans_la'),
    ('ms',               'memphis_tn'),   # Mississippi -> Memphis as closest production center
    ('columbus',         'indianapolis_in'),  # Columbus OH -> closest covered city (imperfect)
]


def audit(cur):
    # Distinct extracted locations over the last 365 days
    cur.execute("""
        SELECT LOWER(TRIM(city)) AS lc, COUNT(*) AS mentions
        FROM bronze.weather_email_extract,
             LATERAL UNNEST(extracted_locations) AS city
        WHERE email_date > NOW() - INTERVAL '365 days'
          AND city IS NOT NULL AND city <> ''
        GROUP BY 1 ORDER BY 2 DESC
    """)
    mentioned = {row[0]: row[1] for row in cur.fetchall()}

    # Known aliases (including any we're about to seed)
    cur.execute("SELECT LOWER(TRIM(alias)) FROM public.weather_location_alias")
    aliases = {row[0] for row in cur.fetchall()}

    # Canonical location names (primary key lookup)
    cur.execute("SELECT LOWER(TRIM(name)), id FROM public.weather_location WHERE is_active = true")
    loc_by_name = {row[0]: row[1] for row in cur.fetchall()}

    matched, unmatched, regions = [], [], []
    for city, mentions in mentioned.items():
        if city in KNOWN_REGIONS:
            regions.append((city, mentions))
        elif city in aliases:
            matched.append((city, mentions, 'alias'))
        elif city in loc_by_name:
            matched.append((city, mentions, f'direct:{loc_by_name[city]}'))
        else:
            unmatched.append((city, mentions))

    return matched, regions, unmatched


def seed_aliases(cur):
    """Upsert the curated alias mappings."""
    seeded = 0
    for alias, loc_id in ALIAS_SEEDS:
        # Verify the target location exists
        cur.execute("SELECT 1 FROM public.weather_location WHERE id=%s", (loc_id,))
        if not cur.fetchone():
            print(f"  ! skipping alias '{alias}' -> '{loc_id}' (location_id not in weather_location)")
            continue
        cur.execute("""
            INSERT INTO public.weather_location_alias (alias, location_id, created_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (alias) DO UPDATE SET location_id = EXCLUDED.location_id
        """, (alias.lower(), loc_id))
        seeded += 1
    return seeded


def main():
    conn = connect()
    cur = conn.cursor()

    print("=" * 70)
    print("WEATHER LOCATION COVERAGE AUDIT")
    print("=" * 70)

    # Pass 1: audit current state
    print("\n--- Before seeding aliases ---")
    matched, regions, unmatched = audit(cur)
    print(f"Matched entities: {len(matched)}")
    for c, m, how in matched[:15]:
        print(f"  {c} ({m}x) via {how}")
    print(f"\nRegions (no single point): {len(regions)}")
    for c, m in regions:
        print(f"  {c} ({m}x)")
    print(f"\nUnmatched: {len(unmatched)}")
    for c, m in unmatched[:15]:
        print(f"  {c} ({m}x)")

    # Pass 2: seed aliases
    print("\n--- Seeding aliases ---")
    n = seed_aliases(cur)
    conn.commit()
    print(f"Seeded/updated {n} aliases")

    # Pass 3: re-audit after seeding
    print("\n--- After seeding aliases ---")
    matched, regions, unmatched = audit(cur)
    print(f"Matched: {len(matched)}  |  Regions: {len(regions)}  |  Unmatched: {len(unmatched)}")
    if unmatched:
        print("Still unmatched — add to ALIAS_SEEDS and re-run, or extend weather_location:")
        for c, m in unmatched:
            print(f"  - {c} ({m}x)")

    conn.close()


if __name__ == '__main__':
    main()
