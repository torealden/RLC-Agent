"""
Geocode the 20 IA crushing facilities and populate lat/lon.

Uses Nominatim (OpenStreetMap) — free, but rate-limited to 1 req/sec per
their usage policy. Sets a custom User-Agent per their requirements.

For each facility:
1. Build a search query from operator + city + ', Iowa, USA'
2. Hit Nominatim, take the highest-confidence result
3. UPDATE reference.oilseed_crush_facilities
4. Also update the corresponding KG node properties

Idempotent — re-run only updates rows where lat IS NULL (or pass --force).
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import requests

USER_AGENT = "RLC-Agent/1.0 (toremalden@gmail.com; oilseed crush facility research)"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
RATE_LIMIT_SECONDS = 1.1  # be slightly above their 1 req/sec policy


def geocode_one(operator: str, city: str, state: str = "IA") -> tuple[float, float] | None:
    """Return (lat, lon) for the facility or None if not found."""
    # Try operator + city first (most specific)
    queries = [
        f"{operator} {city} {state} USA",
        f"{operator} {city} {state}",
        f"{city} {state} USA",  # fallback to city centroid
    ]
    for q in queries:
        params = {"q": q, "format": "json", "limit": 1, "countrycodes": "us"}
        headers = {"User-Agent": USER_AGENT}
        try:
            r = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=10)
            if r.ok:
                data = r.json()
                if data:
                    return float(data[0]["lat"]), float(data[0]["lon"])
            time.sleep(RATE_LIMIT_SECONDS)
        except Exception as e:
            print(f"  geocode error for '{q}': {e}")
        time.sleep(RATE_LIMIT_SECONDS)
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="Re-geocode even if lat/lon already set")
    ap.add_argument("--state", default="IA", help="State to geocode (default IA)")
    args = ap.parse_args()

    from src.services.database.db_config import get_connection

    import psycopg2.extras
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # Filter to canonical only — superseded duplicates inherit geo from canonical.
        # Treat lat=0 (sentinel for "not yet geocoded") same as NULL.
        if args.force:
            cur.execute(
                "SELECT facility_id, operator, city, state FROM reference.oilseed_crush_facilities "
                "WHERE state = %s AND COALESCE(is_canonical, TRUE) = TRUE "
                "ORDER BY facility_id",
                (args.state,)
            )
        else:
            cur.execute(
                "SELECT facility_id, operator, city, state FROM reference.oilseed_crush_facilities "
                "WHERE state = %s AND COALESCE(is_canonical, TRUE) = TRUE "
                "AND (lat IS NULL OR lat = 0) ORDER BY facility_id",
                (args.state,)
            )
        rows = cur.fetchall()

    print(f"{len(rows)} {args.state} facilities to geocode")
    if not rows:
        return

    results = []
    for r in rows:
        coords = geocode_one(r["operator"], r["city"], r["state"])
        if coords:
            lat, lon = coords
            results.append((r["facility_id"], lat, lon))
            print(f"  [OK] {r['facility_id']:42s}  ({lat:.4f}, {lon:.4f})")
        else:
            print(f"  [FAIL] {r['facility_id']:42s}  no result")

    print(f"\n{len(results)}/{len(rows)} geocoded successfully")

    if not results:
        return

    # Persist
    with get_connection() as conn:
        cur = conn.cursor()
        for facility_id, lat, lon in results:
            cur.execute("""
                UPDATE reference.oilseed_crush_facilities
                SET lat = %s, lon = %s, updated_at = NOW()
                WHERE facility_id = %s
            """, (lat, lon, facility_id))
            # Also update the KG node properties
            cur.execute("""
                UPDATE core.kg_node
                SET properties = jsonb_set(jsonb_set(properties, '{lat}', to_jsonb(%s::float)),
                                            '{lon}', to_jsonb(%s::float))
                WHERE node_key = %s AND node_type = 'facility'
            """, (lat, lon, facility_id))
        conn.commit()
        print(f"\nUpdated {len(results)} reference rows + matching KG nodes")


if __name__ == "__main__":
    main()
