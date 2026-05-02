"""
Extended centroid seeding for state-level cardinal-direction sub-regions.

The basic seeding script (seed_basis_region_centroids.py) covered IA/IL/MO/NE
in detail. This pass adds the OH/KS/CO/MS/AR/MN/TN/OK/SD/IN/TX sub-regions
that emerged from the May 2 state backfill.

Approach: for states without detailed crop-reporting-district maps in our
seed data, derive sub-region centroids from the state centroid +/- a fixed
offset per cardinal direction. This is approximate — fine for IDW v1, can
be tightened later from actual elevator survey data.
"""
from dotenv import load_dotenv
load_dotenv()

from src.services.database.db_config import get_connection


# State centroids (rough, agricultural-belt-weighted, not geographic centroid)
STATE_CENTROID = {
    "OH": (40.30, -82.85),  # central Ohio
    "KS": (38.50, -98.40),  # central KS
    "MS": (32.95, -89.85),  # central MS
    "AR": (34.85, -91.75),  # central AR
    "TN": (35.95, -87.10),  # central TN
    "OK": (35.55, -97.50),  # central OK
    "SD": (44.30, -97.85),  # eastern SD (where the grain belt is)
    "IN": (40.25, -86.30),  # central IN
    "TX": (34.85, -101.80), # TX High Plains
    "MN": (44.35, -94.45),  # central-south MN
    "CO": (39.50, -104.55), # eastern CO ag belt
}

# Cardinal-direction offsets (degrees lat, degrees lon)
# For most agricultural states, ~1.5° offset = ~100mi at mid-latitudes
DIRECTION_OFFSETS = {
    "Central":     (0.0, 0.0),
    "C":           (0.0, 0.0),
    "North":       (1.5, 0.0),
    "N":           (1.5, 0.0),
    "South":       (-1.5, 0.0),
    "S":           (-1.5, 0.0),
    "East":        (0.0, 1.5),
    "E":           (0.0, 1.5),
    "West":        (0.0, -1.5),
    "W":           (0.0, -1.5),
    "Northeast":   (1.0, 1.0),
    "NE":          (1.0, 1.0),
    "Northwest":   (1.0, -1.0),
    "NW":          (1.0, -1.0),
    "Southeast":   (-1.0, 1.0),
    "SE":          (-1.0, 1.0),
    "Southwest":   (-1.0, -1.0),
    "SW":          (-1.0, -1.0),
    "North Central":   (1.5, 0.0),
    "South Central":   (-1.5, 0.0),
    "East Central":    (0.0, 1.5),
    "West Central":    (0.0, -1.5),
}

# Bespoke entries that don't fit the cardinal pattern: explicit (lat, lon)
BESPOKE = [
    # source         state, region_name,            delivery_point,            lat,    lon,    radius_mi, notes
    ("AMS_2851", "OH", "Ohio River",             "Barge Loading Elevators", 39.10, -84.50, 60, "Ohio River reaches"),
    ("AMS_2851", "OH", "Toledo - Off River",     "Country Elevators",       41.55, -83.85, 30, "NW OH, off Maumee River"),
    ("AMS_2851", "OH", "Toledo - On River",      "Barge Loading Elevators", 41.65, -83.55, 25, "Toledo terminals on Maumee"),
    ("AMS_2851", "OH", "Grain - Ohio",           None,                      40.30, -82.85, 130, "Ohio statewide proxy"),
    ("AMS_2960", "AR", "Mississippi River",      "Barge Loading Elevators", 34.50, -90.65, 60, "AR east-side MS River barge"),
    ("AMS_2960", "AR", "Arkansas River",         "Barge Loading Elevators", 34.85, -92.30, 50, "AR River barge corridor"),
    ("AMS_2960", "AR", "Grain - Arkansas",       None,                      34.85, -91.75, 110, "Arkansas statewide proxy"),
    ("AMS_2928", "MS", "Grain - Mississippi",    None,                      32.95, -89.85, 110, "MS statewide proxy"),
    ("AMS_3088", "TN", "Mississippi River",      "Barge Loading Elevators", 35.85, -89.95, 50, "TN MS River barge"),
    ("AMS_3463", "IN", "Ohio River",             "Barge Loading Elevators", 38.30, -86.25, 60, "Southern IN OH River"),
    ("AMS_3463", "IN", "Grain - Indiana",        None,                      40.25, -86.30, 120, "Indiana statewide proxy"),
]

# State lists that follow the cardinal pattern (use STATE_CENTROID + DIRECTION_OFFSETS)
# Format: source -> state -> [region_name, region_name, ...]
CARDINAL_STATES = {
    "AMS_2851": ("OH", ["Central", "East"]),
    "AMS_2886": ("KS", ["Central", "East", "North", "Northeast", "Northwest", "South", "Southeast", "Southwest", "West"]),
    "AMS_2912": ("CO", ["Northeast", "East"]),
    "AMS_3049": ("MN", ["South"]),
    "AMS_3088": ("TN", ["North Central", "Northwest", "South Central", "West", "West Central"]),
    "AMS_3100": ("OK", ["Central", "North Central", "Northwest", "West"]),
    "AMS_3186": ("SD", ["Central", "East Central", "North Central", "Northeast", "Southeast"]),
    "AMS_3463": ("IN", ["Central", "East", "North", "Northeast", "Northwest", "Southwest", "West"]),
}


def main():
    rows_to_seed = []

    # Cardinal entries
    for source, (state, regions) in CARDINAL_STATES.items():
        cx, cy = STATE_CENTROID[state]
        for r in regions:
            if r not in DIRECTION_OFFSETS:
                print(f"  WARN: no offset for region '{r}' in {state}")
                continue
            dlat, dlon = DIRECTION_OFFSETS[r]
            lat = cx + dlat
            lon = cy + dlon
            rows_to_seed.append((source, state, r, "Country Elevators", lat, lon, 60,
                                 f"{state} {r} sub-region (cardinal-derived)"))

    # Bespoke entries
    for tup in BESPOKE:
        rows_to_seed.append(tup)

    print(f"Seeding {len(rows_to_seed)} additional centroids...")

    with get_connection() as conn:
        cur = conn.cursor()
        for tup in rows_to_seed:
            cur.execute("""
                INSERT INTO reference.basis_region_centroid
                  (source, state, region_name, delivery_point, centroid_lat, centroid_lon,
                   coverage_radius_mi, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, state, region_name, delivery_point) DO UPDATE SET
                  centroid_lat = EXCLUDED.centroid_lat,
                  centroid_lon = EXCLUDED.centroid_lon,
                  coverage_radius_mi = EXCLUDED.coverage_radius_mi,
                  notes = EXCLUDED.notes
            """, tup)
        conn.commit()
        # Verify
        import psycopg2.extras
        cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur2.execute("SELECT state, COUNT(*) AS n FROM reference.basis_region_centroid GROUP BY state ORDER BY state")
        print("\nFinal centroid counts by state:")
        total = 0
        for r in cur2.fetchall():
            print(f"  {r['state']}  n={r['n']}")
            total += r['n']
        print(f"Total centroids: {total}")


if __name__ == "__main__":
    main()
