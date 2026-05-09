"""
FIC Layer 3 — facility on-boarding hook.

When a new facility lands in `reference.facility_master` or
`reference.oilseed_crush_facilities`, run this to:

  1. Compute geographic edges to same-industry peers within range (uses
     haversine; classes the radius based on industry conventions).
  2. Add parent-company edges to siblings under the same operator.
  3. Seed a Google News RSS query for (operator, city) — additive to
     reference.news_source.
  4. Surface whether the operator maps to a public ticker so we can
     trigger an SEC filing pull (separate manual command for now).

The function `onboard(facility_id)` returns a dict describing every
action taken, ready for display in the FIC after a form submit. It's
idempotent: running again on a facility that's already on-boarded is
safe — edges and sources use ON CONFLICT DO NOTHING / UPDATE.

Usage:
    # Run after creating a facility via the FIC form (called by index.py)
    python -m scripts.onboard_facility --facility-id ia.poet_marion

    # Inspect what would happen, no writes
    python -m scripts.onboard_facility --facility-id ia.poet_marion --dry-run

    # Force re-run of all steps even if some are already done
    python -m scripts.onboard_facility --facility-id ia.poet_marion --force
"""

from __future__ import annotations

import argparse
import math
import re
import sys
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import psycopg2.extras
from src.services.database.db_config import get_connection


# Per-industry geographic radius for distance edges.
# These reflect realistic catchment / competitive ranges.
INDUSTRY_DRAW_RADIUS_MILES = {
    "oilseed_crush":     50,
    "ethanol":           50,
    "biodiesel":         100,
    "renewable_diesel":  150,
    "pork_packing":      100,
    "beef_packing":      150,
    "egg_layers":        50,
    "pig_finishing":     50,
    "grain_handling":    25,
    "rail_terminal":     150,
    "river_terminal":    250,
    "feed_mill":         50,
    "default":           50,
}

GNEWS_BASE = ('https://news.google.com/rss/search?q={query}'
              '&hl=en-US&gl=US&ceid=US:en')

# Public ticker map (extend as we learn). Used only to flag whether
# the operator's SEC filings are pullable; does not pull automatically.
OPERATOR_TICKER_MAP = [
    ("archer-daniels-midland", "ADM"),
    ("archer daniels", "ADM"),
    ("adm", "ADM"),
    ("bunge", "BG"),
    ("tyson", "TSN"),
    ("hormel", "HRL"),
    ("jbs", "JBS"),
    ("green plains", "GPRE"),
    ("ingredion", "INGR"),
    ("valero", "VLO"),
    ("chevron renewable", "CVX"),
    ("chevron", "CVX"),
    ("darling", "DAR"),
    ("smithfield", "SFD"),
    ("corteva", "CTVA"),
    ("nutrien", "NTR"),
    ("mosaic", "MOS"),
    ("cf industries", "CF"),
    ("intrepid", "IPI"),
    ("fmc", "FMC"),
    ("calmaine", "CALM"),
    ("cal-maine", "CALM"),
    ("pilgrim", "PPC"),
    ("post holdings", "POST"),
    ("conagra", "CAG"),
    ("ricebran", "RIBT"),
    ("rex american", "REX"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def haversine_miles(lat1, lon1, lat2, lon2) -> float:
    R = 3958.7613  # earth radius in miles
    p1, p2 = math.radians(float(lat1)), math.radians(float(lat2))
    dl, dp = math.radians(float(lon2) - float(lon1)), p2 - p1
    a = math.sin(dp/2)**2 + math.cos(p1) * math.cos(p2) * math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(a))


def slugify(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def operator_to_ticker(operator: str | None) -> str | None:
    if not operator:
        return None
    op = operator.lower().strip()
    for needle, ticker in OPERATOR_TICKER_MAP:
        if needle in op:
            return ticker
    return None


def fetch_facility(cur, facility_id: str) -> dict | None:
    cur.execute(
        """
        SELECT facility_id, name, industry_code, operator, parent_company,
               city, county, state, lat, lon, status,
               'facility_master' AS source_table
        FROM reference.facility_master
        WHERE facility_id = %s
        UNION ALL
        SELECT facility_id, name, 'oilseed_crush' AS industry_code,
               operator, parent_company, city, county, state, lat, lon,
               status, 'oilseed_crush_facilities' AS source_table
        FROM reference.oilseed_crush_facilities
        WHERE facility_id = %s AND is_canonical = TRUE
        LIMIT 1
        """,
        (facility_id, facility_id),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def fetch_peers(cur, fac: dict) -> list[dict]:
    """All facilities in the same industry that have lat/lon."""
    cur.execute(
        """
        SELECT facility_id, operator, parent_company, lat, lon, state
        FROM reference.facility_master
        WHERE industry_code = %s
          AND lat IS NOT NULL AND lon IS NOT NULL
          AND facility_id <> %s
        UNION ALL
        SELECT facility_id, operator, parent_company, lat, lon, state
        FROM reference.oilseed_crush_facilities
        WHERE %s = 'oilseed_crush'
          AND is_canonical = TRUE
          AND lat IS NOT NULL AND lon IS NOT NULL
          AND facility_id <> %s
        """,
        (fac["industry_code"], fac["facility_id"],
         fac["industry_code"], fac["facility_id"]),
    )
    return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# On-boarding steps
# ---------------------------------------------------------------------------

def step_distance_edges(cur, fac: dict, dry_run: bool) -> dict:
    """Step 1: insert distance edges to same-industry peers within radius."""
    if not (fac.get("lat") and fac.get("lon")):
        return {"status": "skipped", "reason": "no coordinates",
                "edges_added": 0, "details": []}

    radius = INDUSTRY_DRAW_RADIUS_MILES.get(
        fac.get("industry_code") or "default",
        INDUSTRY_DRAW_RADIUS_MILES["default"],
    )
    peers = fetch_peers(cur, fac)
    nearby = []
    for p in peers:
        d = haversine_miles(fac["lat"], fac["lon"], p["lat"], p["lon"])
        if d <= radius:
            nearby.append((p, d))
    nearby.sort(key=lambda x: x[1])

    edges = []
    for p, d in nearby:
        # Weight inversely with distance: 1.0 at 0mi, 0.0 at radius
        weight = max(0.0, 1.0 - (d / radius))
        edges.append((p["facility_id"], "draw_region", round(weight, 3),
                      f"distance: {d:.1f}mi"))

    if dry_run:
        return {"status": "dry-run", "edges_added": len(edges),
                "details": [f"{e[0]} ({e[3]}, w={e[2]})" for e in edges[:10]]}

    market_id = "us_oilseed_crush" if fac.get("industry_code") == "oilseed_crush" \
                else f"us_{fac.get('industry_code', 'unknown')}"

    n = 0
    for target, etype, weight, notes in edges:
        cur.execute(
            """
            INSERT INTO reference.facility_edge_weights
                (market_id, source_facility_id, target_facility_id,
                 edge_type, weight, notes, is_active, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW(), NOW())
            ON CONFLICT DO NOTHING
            """,
            (market_id, fac["facility_id"], target, etype, weight, notes),
        )
        if cur.rowcount > 0:
            n += 1
        # Add the inverse direction too — relationships are usually symmetric
        # for distance edges.
        cur.execute(
            """
            INSERT INTO reference.facility_edge_weights
                (market_id, source_facility_id, target_facility_id,
                 edge_type, weight, notes, is_active, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW(), NOW())
            ON CONFLICT DO NOTHING
            """,
            (market_id, target, fac["facility_id"], etype, weight, notes),
        )

    return {"status": "ok", "edges_added": n, "radius_miles": radius,
            "peers_scanned": len(peers),
            "details": [f"{e[0]} ({e[3]}, w={e[2]})" for e in edges[:10]]}


def step_parent_company_edges(cur, fac: dict, dry_run: bool) -> dict:
    """Step 2: insert parent_company edges to siblings."""
    parent = fac.get("parent_company") or fac.get("operator")
    if not parent:
        return {"status": "skipped", "reason": "no parent_company / operator",
                "edges_added": 0, "details": []}

    cur.execute(
        """
        SELECT facility_id FROM reference.facility_master
        WHERE LOWER(parent_company) = LOWER(%s) OR LOWER(operator) = LOWER(%s)
              AND facility_id <> %s
        UNION ALL
        SELECT facility_id FROM reference.oilseed_crush_facilities
        WHERE (LOWER(parent_company) = LOWER(%s) OR LOWER(operator) = LOWER(%s))
              AND is_canonical = TRUE AND facility_id <> %s
        """,
        (parent, parent, fac["facility_id"],
         parent, parent, fac["facility_id"]),
    )
    siblings = [dict(r)["facility_id"] for r in cur.fetchall()]

    if dry_run:
        return {"status": "dry-run", "edges_added": len(siblings),
                "details": siblings[:10]}

    n = 0
    for sib in siblings:
        for src, tgt in [(fac["facility_id"], sib), (sib, fac["facility_id"])]:
            cur.execute(
                """
                INSERT INTO reference.facility_edge_weights
                    (market_id, source_facility_id, target_facility_id,
                     edge_type, weight, notes, is_active, created_at, updated_at)
                VALUES (NULL, %s, %s, 'parent_company', 1.0, %s,
                        TRUE, NOW(), NOW())
                ON CONFLICT DO NOTHING
                """,
                (src, tgt, f"shared parent: {parent}"),
            )
            if cur.rowcount > 0:
                n += 1

    return {"status": "ok", "edges_added": n,
            "siblings": siblings[:10],
            "parent_company": parent}


def step_seed_gnews(cur, fac: dict, dry_run: bool) -> dict:
    """Step 3: insert a Google News RSS query for this (operator, city)."""
    operator = fac.get("operator") or ""
    city = fac.get("city") or ""
    if not operator or not city:
        return {"status": "skipped", "reason": "missing operator/city",
                "added": False}

    # Strip directional suffixes for the city query
    clean_city = re.sub(r"\s+(East|West|North|South)$", "", city.strip(),
                        flags=re.IGNORECASE)
    state = fac.get("state") or ""

    query = f'"{operator}" "{clean_city}" {state}'.strip()
    url = GNEWS_BASE.format(query=urllib.parse.quote(query))
    source_name = f"gnews_{slugify(operator)}_{slugify(clean_city)}"

    if dry_run:
        return {"status": "dry-run", "added": False,
                "source_name": source_name,
                "query": query,
                "url": url}

    cur.execute(
        """
        INSERT INTO reference.news_source
            (source_name, source_type, url_template,
             polling_frequency_minutes, default_locality,
             default_topic_focus, source_weight, is_active)
        VALUES (%s, 'rss', %s, 720, 'local',
                ARRAY['competitor_activity', 'policy_state_local']::text[],
                0.85, TRUE)
        ON CONFLICT (source_name) DO UPDATE SET
            url_template = EXCLUDED.url_template,
            is_active = TRUE
        """,
        (source_name, url),
    )

    return {"status": "ok", "added": True, "source_name": source_name,
            "query": query}


def step_sentiment_loop_check(cur, fac: dict) -> dict:
    """Step 4: report whether this facility will be picked up by the
    daily sentiment update loop. The loop currently reads
    reference.oilseed_crush_facilities only — multi-industry support
    is a future enhancement."""
    if fac.get("source_table") == "oilseed_crush_facilities":
        return {"status": "auto-enrolled",
                "note": "Daily Market Field loop reads oilseed_crush_facilities; "
                        "next run picks up this facility."}
    return {"status": "deferred",
            "note": ("Market Field daily loop is currently scoped to oilseed_crush. "
                     "This facility's sentiment will be tracked once the loop is "
                     "extended to its industry. Edges and news sources are still "
                     "useful for downstream queries.")}


def step_ticker_check(fac: dict) -> dict:
    """Step 5: surface whether the operator maps to a public ticker so the
    user knows SEC filings can be pulled."""
    ticker = operator_to_ticker(fac.get("operator"))
    if not ticker:
        return {"status": "private",
                "note": f"Operator '{fac.get('operator')}' is private or not "
                        "in the ticker map. SEC filings not applicable."}
    return {"status": "public",
            "ticker": ticker,
            "note": (f"Operator maps to ticker {ticker}. "
                     f"To pull filings: "
                     f"`python scripts/sec_edgar_puller.py --ticker {ticker} "
                     f"--form 8-K,10-K,10-Q --since 2022-01-01`")}


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------

def onboard(facility_id: str, dry_run: bool = False) -> dict:
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        fac = fetch_facility(cur, facility_id)
        if not fac:
            return {"facility_id": facility_id, "error": "not found"}

        result = {
            "facility_id": facility_id,
            "industry": fac.get("industry_code"),
            "operator": fac.get("operator"),
            "city": fac.get("city"),
            "state": fac.get("state"),
            "dry_run": dry_run,
        }

        result["distance_edges"]    = step_distance_edges(cur, fac, dry_run)
        result["parent_edges"]      = step_parent_company_edges(cur, fac, dry_run)
        result["news_source"]       = step_seed_gnews(cur, fac, dry_run)
        result["sentiment_loop"]    = step_sentiment_loop_check(cur, fac)
        result["public_ticker"]     = step_ticker_check(fac)

        if not dry_run:
            conn.commit()

    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--facility-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    result = onboard(args.facility_id, dry_run=args.dry_run)

    import json
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
