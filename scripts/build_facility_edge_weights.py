"""
Market Field Layer 1 — Facility graph edge-weight builder.

Computes directed influence weights between facilities for the sentiment-
diffusion network, populates reference.facility_edge_weights. Idempotent
on (market_id, source_facility_id, target_facility_id, edge_type).

Edge types built here:
  parent_company  — same parent company           weight = 1.0
  draw_region     — distance-decayed proximity     weight = exp(-d/50) for d < 200mi
  industry        — same market baseline           weight = 0.05

Edge types NOT built here (handled at update time):
  trade           — no per-pair counterparty data yet
  weak_random     — sampled stochastically each daily update for the
                    "small probability of unrelated spread" term

Filtering:
  - Only canonical facilities (is_canonical = TRUE)
  - Only facilities with valid coordinates (lat is not null AND not 0)
  - Only the configured state (default IA for the pilot)
  - Only the configured market (default us_oilseed_crush)

Run:
  python scripts/build_facility_edge_weights.py             # default IA pilot
  python scripts/build_facility_edge_weights.py --state IA --dry-run

The --dry-run flag prints the proposed edge population without writing.
"""
from __future__ import annotations

import argparse
import math
from dataclasses import dataclass

from dotenv import load_dotenv
load_dotenv()

import psycopg2.extras
from src.services.database.db_config import get_connection


MARKET_ID = 'us_oilseed_crush'

# --- Edge weight calibration ---
# These values are deliberately conservative. The Market Field update
# equation row-normalises across edge types at query time, so absolute
# magnitudes matter less than relative ordering.
PARENT_COMPANY_WEIGHT = 1.0
INDUSTRY_BASE_WEIGHT  = 0.05
DRAW_REGION_DECAY_MI  = 50.0   # exp(-d/D); halves at ~35mi
DRAW_REGION_CAP_MI    = 200.0  # zero beyond this distance


@dataclass
class Facility:
    facility_id: str
    operator: str
    parent_company: str
    lat: float
    lon: float


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in statute miles."""
    R = 3958.7613  # Earth radius, miles
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    return R * c


def load_facilities(state: str) -> list[Facility]:
    """Load canonical, geocoded facilities for the given state."""
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT facility_id, operator, parent_company, lat, lon
            FROM reference.oilseed_crush_facilities
            WHERE state = %s
              AND is_canonical = TRUE
              AND lat IS NOT NULL AND lat <> 0
              AND lon IS NOT NULL AND lon <> 0
            ORDER BY facility_id
            """,
            (state,),
        )
        return [Facility(
            facility_id=r['facility_id'],
            operator=r['operator'] or '',
            parent_company=r['parent_company'] or '',
            lat=float(r['lat']),
            lon=float(r['lon']),
        ) for r in cur.fetchall()]


def build_edges(facilities: list[Facility]) -> list[tuple]:
    """Compute all directed edges across the facility set.

    Returns rows ready for upsert into reference.facility_edge_weights:
      (market_id, source_id, target_id, edge_type, weight, notes)
    """
    edges = []
    for src in facilities:
        for tgt in facilities:
            if src.facility_id == tgt.facility_id:
                continue

            # parent_company edges
            if src.parent_company and tgt.parent_company \
               and src.parent_company == tgt.parent_company:
                edges.append((
                    MARKET_ID, src.facility_id, tgt.facility_id,
                    'parent_company', PARENT_COMPANY_WEIGHT,
                    f'same parent: {src.parent_company}',
                ))

            # draw_region edges (haversine distance with exp decay)
            d = haversine_miles(src.lat, src.lon, tgt.lat, tgt.lon)
            if d < DRAW_REGION_CAP_MI:
                w = math.exp(-d / DRAW_REGION_DECAY_MI)
                edges.append((
                    MARKET_ID, src.facility_id, tgt.facility_id,
                    'draw_region', round(w, 6),
                    f'distance {d:.1f} mi (decay D={DRAW_REGION_DECAY_MI:.0f})',
                ))

            # industry baseline
            edges.append((
                MARKET_ID, src.facility_id, tgt.facility_id,
                'industry', INDUSTRY_BASE_WEIGHT,
                'same market: us_oilseed_crush',
            ))
    return edges


def upsert_edges(edges: list[tuple]) -> int:
    """Upsert edges to reference.facility_edge_weights. Returns rows touched."""
    sql = """
        INSERT INTO reference.facility_edge_weights
            (market_id, source_facility_id, target_facility_id,
             edge_type, weight, notes, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (market_id, source_facility_id, target_facility_id, edge_type)
        DO UPDATE SET
            weight     = EXCLUDED.weight,
            notes      = EXCLUDED.notes,
            updated_at = NOW(),
            is_active  = TRUE
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.executemany(sql, edges)
        conn.commit()
    return len(edges)


def report(edges: list[tuple], facilities: list[Facility]) -> None:
    """Print a readable summary of what was generated."""
    from collections import Counter
    by_type = Counter(e[3] for e in edges)
    print(f'Facilities loaded: {len(facilities)}')
    print(f'Total edges generated: {len(edges)}')
    for t, n in sorted(by_type.items()):
        print(f'  {t:18s} {n:>5,} edges')
    print()

    # parent_company groupings
    from collections import defaultdict
    pc_groups = defaultdict(list)
    for f in facilities:
        pc_groups[f.parent_company].append(f.facility_id)
    print('Parent-company clusters (parent_company edges fire within each):')
    for pc, members in sorted(pc_groups.items()):
        if len(members) > 1:
            print(f'  {pc:25s} ({len(members)}): {members}')
        else:
            print(f'  {pc:25s} (1 — singleton, no parent_company edges)')

    # Effective influence per facility (sum of incoming weights across edge types)
    print()
    print('Aggregate incoming influence per facility (sum of weights, all types):')
    incoming = defaultdict(float)
    for src, tgt, etype, w in ((e[1], e[2], e[3], e[4]) for e in edges):
        incoming[tgt] += w
    for fac_id in sorted(incoming):
        print(f'  {fac_id:42s} sum_w = {incoming[fac_id]:6.2f}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--state', default='IA',
                    help='Filter facilities to this state (pilot: IA)')
    ap.add_argument('--dry-run', action='store_true',
                    help='Compute + report only, do not write to DB')
    args = ap.parse_args()

    facilities = load_facilities(args.state)
    if not facilities:
        raise SystemExit(
            f'No canonical, geocoded facilities found for state={args.state}. '
            'Did the dedup migration run, and are coords populated?'
        )
    edges = build_edges(facilities)
    report(edges, facilities)

    if args.dry_run:
        print()
        print('DRY-RUN: nothing written. Re-run without --dry-run to persist.')
        return

    n = upsert_edges(edges)
    print()
    print(f'Persisted {n:,} rows to reference.facility_edge_weights '
          f'(market_id={MARKET_ID}).')


if __name__ == '__main__':
    main()
