"""
Seed reference.basis_region_centroid with lat/lon for AMS regional labels.

These centroids are approximate centers of the AMS-defined geographic regions
within each state's daily grain bid report. They're used to attach lat/lon
to regional aggregate prices so we can use them as spatial samples in the
basis field. Centroids should be tightened over time using actual elevator
locations within each region.

Coverage: state reports we ingest (slugs added in 2026-05-02 catalog update).
Sources for centroid coordinates:
- IA: Iowa State University crop reporting district map
- IL: USDA NASS IL crop reporting districts
- MO: USDA NASS MO crop reporting districts
- NE: USDA NASS NE crop reporting districts
- River reaches: midpoint of the named river segment in the relevant state
"""
from dotenv import load_dotenv
load_dotenv()

from src.services.database.db_config import get_connection


# ============================================================================
# Centroid lookups — (state, region_name, lat, lon, radius_mi, notes)
# ============================================================================

# Iowa Crop Reporting Districts (NASS)
IA_REGIONS = [
    ("Northwest",     43.05, -95.30, 60, "NW IA: Sioux/Plymouth/O'Brien/Buena Vista/etc."),
    ("North Central", 42.85, -93.85, 60, "NC IA: Wright/Hamilton/Hancock/Cerro Gordo/etc."),
    ("Northeast",     43.00, -91.85, 60, "NE IA: Allamakee/Winneshiek/Fayette/Black Hawk/etc."),
    ("West Central",  41.95, -94.85, 60, "WC IA: Carroll/Greene/Audubon/Guthrie/etc."),
    ("Central",       41.85, -93.40, 50, "Central IA: Polk/Story/Boone/Marshall/etc."),
    ("East Central",  41.85, -91.55, 60, "EC IA: Linn/Johnson/Cedar/Clinton/etc."),
    ("Southwest",     41.10, -94.95, 60, "SW IA: Pottawattamie/Mills/Fremont/etc."),
    ("South Central", 41.00, -93.30, 60, "SC IA: Madison/Warren/Lucas/Wapello/etc."),
    ("Southeast",     40.95, -91.70, 60, "SE IA: Lee/Henry/Des Moines/Washington/etc."),
    ("Grain - Iowa",  41.85, -93.40, 200, "Statewide Iowa proxy"),
]

# Illinois — region names from AMS_3192
IL_REGIONS = [
    ("North",                 41.55, -89.20, 70, "Northern IL counties"),
    ("North Central",         40.65, -89.25, 60, "Central-north band IL"),
    ("Central",               39.80, -89.35, 50, "Central IL incl. Springfield"),
    ("South Central",         38.85, -89.05, 60, "South-central IL"),
    ("South",                 38.10, -88.55, 70, "Southern IL"),
    ("West",                  40.20, -90.95, 60, "Western IL adjacent to MS River"),
    ("East",                  40.20, -88.20, 60, "Eastern IL near IN border"),
    ("Wabash",                38.35, -87.80, 60, "Wabash river region"),
    ("Little Egypt",          37.55, -88.85, 60, "Far southern IL ('Little Egypt')"),
    # Terminal / barge points
    ("Chicago",               41.85, -87.65, 30, "Chicago terminal market"),
    ("Mississippi River",     39.50, -90.85, 50, "MS River barge loading IL side"),
    ("North Illinois River",  41.30, -89.10, 40, "Upper IL River barge"),
    ("South Illinois River",  39.85, -90.20, 40, "Lower IL River barge"),
]

# Missouri — region names from AMS_2932
MO_REGIONS = [
    ("Northwest",   40.15, -94.40, 60, "NW MO"),
    ("North Central", 39.95, -92.95, 50, "NC MO"),
    ("Northeast",   39.85, -91.65, 60, "NE MO"),
    ("West Central", 38.85, -93.85, 50, "WC MO"),
    ("Central",     38.95, -92.45, 50, "Central MO"),
    ("Southwest",   37.20, -93.85, 60, "SW MO incl. Springfield"),
    ("Southeast",   37.10, -89.85, 60, "SE MO 'Bootheel'"),
    ("South Central", 37.20, -92.35, 50, "SC MO incl. Mtn Home"),
    # Terminals / rivers
    ("Kansas City",                 39.10, -94.60, 30, "KC terminal"),
    ("St. Joseph",                  39.77, -94.85, 25, "St. Joseph MO terminal"),
    ("St. Louis MS River",          38.65, -90.20, 25, "St. Louis MS River terminals"),
    ("Northeast Missouri MS River", 39.70, -91.40, 40, "NE MO MS River barge"),
    ("Southeast Missouri MS River", 36.85, -89.55, 40, "SE MO MS River barge"),
]

# Nebraska — region names from AMS_3225
NE_REGIONS = [
    ("Northeast",   42.10, -97.05, 60, "NE NE"),
    ("Northwest",   42.55, -102.05, 80, "NW NE (Sandhills/Panhandle)"),
    ("Central",     41.30, -99.05, 60, "Central NE"),
    ("East",        41.05, -96.45, 50, "Eastern NE incl. Omaha-Lincoln corridor"),
    ("South",       40.40, -98.95, 60, "Southern NE"),
    ("Southeast",   40.50, -96.30, 50, "SE NE"),
    ("Southwest",   40.35, -101.15, 70, "SW NE"),
]

# Other states — coarser since fewer sub-regions
OTHER_REGIONS = [
    # OH (slug 2851)
    ("OH", "AMS_2851", "Ohio", 40.40, -82.85, 120, None,         "Ohio statewide"),
    ("OH", "AMS_2851", "Ohio River Barge Loading Elevator", 39.10, -84.50, 60, None, "OH River reaches"),
    # KS (slug 2886)
    ("KS", "AMS_2886", "Kansas", 38.50, -98.40, 130, None, "Kansas statewide"),
    # MN (slug 3046 = Minneapolis terminal, 3049 = Southern MN)
    ("MN", "AMS_3046", "Minneapolis", 44.97, -93.27, 30, None, "Minneapolis-Twin Cities terminal"),
    ("MN", "AMS_3046", "GR - MGEX", 44.97, -93.27, 30, None, "MGEX (Mpls Grain Exchange)"),
    ("MN", "AMS_3049", "Southern MN", 43.95, -94.45, 80, None, "Southern MN crop reporting district"),
    # SD (slug 3186 = East River SD)
    ("SD", "AMS_3186", "East River SD", 44.30, -97.85, 100, None, "Eastern SD agricultural belt"),
    # IN (slug 3463)
    ("IN", "AMS_3463", "Indiana", 40.25, -86.30, 120, None, "Indiana statewide"),
    # AR (slug 2960)
    ("AR", "AMS_2960", "Arkansas", 34.85, -91.75, 110, None, "Arkansas statewide"),
    # MS (slug 2928)
    ("MS", "AMS_2928", "Mississippi", 32.95, -89.85, 110, None, "MS statewide"),
    # TN (slug 3088)
    ("TN", "AMS_3088", "Tennessee", 35.95, -87.10, 130, None, "TN statewide"),
    # OK (slug 3100)
    ("OK", "AMS_3100", "Oklahoma", 35.55, -97.50, 120, None, "OK statewide"),
    # CO/WY via slug 2912
    ("CO", "AMS_2912", "Colorado", 39.50, -104.55, 100, None, "Eastern CO grain belt"),
    # TX (slug 2711)
    ("TX", "AMS_2711", "TX High Plains", 34.85, -101.80, 90, None, "TX panhandle / High Plains"),
    # MT (slug 2771, 3148)
    ("MT", "AMS_2771", "Montana", 47.30, -110.10, 200, None, "MT statewide (durum/barley)"),
    ("OR", "AMS_3148", "Portland", 45.55, -122.65, 30, None, "Portland terminal"),
]


def main():
    rows_to_insert = []

    # IA
    for region, lat, lon, rad, notes in IA_REGIONS:
        for dp in ["Country Elevators", "Mills and Processors", None]:
            rows_to_insert.append(("AMS_2850", "IA", region, dp, lat, lon, rad, notes))

    # IL
    for region, lat, lon, rad, notes in IL_REGIONS:
        # delivery_point varies: country / mills / barge
        if "River" in region or region == "Chicago":
            dp = "Barge Loading Elevators" if "River" in region else "Terminal Elevators"
        else:
            dp = "Country Elevators"
        rows_to_insert.append(("AMS_3192", "IL", region, dp, lat, lon, rad, notes))
        # Also add Mills and Processors mapping for inland regions
        if "River" not in region and region != "Chicago":
            rows_to_insert.append(("AMS_3192", "IL", region, "Mills and Processors", lat, lon, rad, notes))

    # MO
    for region, lat, lon, rad, notes in MO_REGIONS:
        if "River" in region or region in ("Kansas City", "St. Joseph"):
            dp = "Terminals/Mills/Processors" if region in ("Kansas City", "St. Joseph") else "Barge Loading Elevators"
            if region == "St. Louis MS River":
                dp = "Terminals/Mills/Processors"
        else:
            dp = "Country Elevators"
        rows_to_insert.append(("AMS_2932", "MO", region, dp, lat, lon, rad, notes))

    # NE
    for region, lat, lon, rad, notes in NE_REGIONS:
        rows_to_insert.append(("AMS_3225", "NE", region, "Country Elevators", lat, lon, rad, notes))

    # Other states (already have full tuple)
    for state, source, region, lat, lon, rad, _, notes in OTHER_REGIONS:
        rows_to_insert.append((source, state, region, None, lat, lon, rad, notes))

    print(f"Seeding {len(rows_to_insert)} centroid rows...")

    with get_connection() as conn:
        cur = conn.cursor()
        for source, state, region, dp, lat, lon, rad, notes in rows_to_insert:
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
            """, (source, state, region, dp, lat, lon, rad, notes))
        conn.commit()
        # Verify
        cur.execute("SELECT state, COUNT(*) AS n FROM reference.basis_region_centroid GROUP BY state ORDER BY n DESC")
        print("Centroid counts by state:")
        for state, n in cur.fetchall():
            print(f"  {state}  n={n}")
        cur.execute("SELECT COUNT(*) FROM reference.basis_region_centroid")
        print(f"Total: {cur.fetchone()[0]}")


if __name__ == "__main__":
    main()
