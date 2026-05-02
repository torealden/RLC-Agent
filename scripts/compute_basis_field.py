"""
Compute the basis field via IDW (inverse-distance weighting) interpolation.

Reads bronze.cash_bid_observation samples for a given (commodity, date range,
delivery_month) and writes a regular grid of interpolated values to
silver.basis_field_grid.

This is v1 — IDW with power=2 and a max-distance cutoff. Kriging with
proper uncertainty estimates is v2.

Grid:
  - Continental US bounded by IA crusher footprint at minimum
  - 0.25° resolution = ~17 mi at IA latitude
  - Default extent: 35°-49°N, -104° to -82°W (Corn Belt + a bit)

Usage:
    # Compute for soybeans, spot, today
    python -m scripts.compute_basis_field

    # Specific date / commodity / delivery
    python -m scripts.compute_basis_field --commodity soybeans --date 2026-04-30 --delivery spot

    # Regenerate last 30 days
    python -m scripts.compute_basis_field --backfill-days 30
"""
from __future__ import annotations

import argparse
import math
from datetime import date, timedelta
from typing import Iterable

from dotenv import load_dotenv
load_dotenv()

import psycopg2.extras
from src.services.database.db_config import get_connection


# Grid extent — Corn Belt + a little extra
GRID_LAT_MIN, GRID_LAT_MAX = 35.0, 49.0
GRID_LON_MIN, GRID_LON_MAX = -104.0, -82.0
GRID_RESOLUTION_DEG = 0.25  # ~17mi at IA latitude

# IDW parameters
IDW_POWER = 2.0
IDW_MAX_DISTANCE_MI = 250.0   # samples beyond this don't contribute
IDW_MIN_SAMPLES = 3           # need at least this many in range to interpolate

METHOD = "idw_v1"
METHOD_VERSION = f"power={IDW_POWER},maxd={IDW_MAX_DISTANCE_MI}mi,res={GRID_RESOLUTION_DEG}deg"


def haversine_mi(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance in statute miles."""
    R_MI = 3958.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2 * R_MI * math.asin(math.sqrt(a))


def grid_cells() -> Iterable[tuple[float, float]]:
    """Yield (lat, lon) grid cell centers."""
    n_lat = int((GRID_LAT_MAX - GRID_LAT_MIN) / GRID_RESOLUTION_DEG)
    n_lon = int((GRID_LON_MAX - GRID_LON_MIN) / GRID_RESOLUTION_DEG)
    for i in range(n_lat):
        lat = GRID_LAT_MIN + (i + 0.5) * GRID_RESOLUTION_DEG
        for j in range(n_lon):
            lon = GRID_LON_MIN + (j + 0.5) * GRID_RESOLUTION_DEG
            yield (round(lat, 4), round(lon, 4))


def idw_predict(target_lat: float, target_lon: float,
                samples: list[tuple[float, float, float, float]]) -> tuple[float, float, int, float]:
    """Inverse distance weighted prediction.

    samples: list of (lat, lon, value, weight)
    Returns (predicted_value, std_err_proxy, n_samples_used, nearest_distance_mi)
    """
    weighted_vals = []
    weights_used = []
    nearest_d = float("inf")
    for s_lat, s_lon, val, w in samples:
        d_mi = haversine_mi(target_lat, target_lon, s_lat, s_lon)
        nearest_d = min(nearest_d, d_mi)
        if d_mi > IDW_MAX_DISTANCE_MI:
            continue
        if d_mi < 0.5:
            # Nearly coincident — return that value directly
            return (val, 0.0, 1, d_mi)
        idw_w = w / (d_mi ** IDW_POWER)
        weighted_vals.append(val * idw_w)
        weights_used.append(idw_w)

    if len(weights_used) < IDW_MIN_SAMPLES:
        return (None, None, len(weights_used), nearest_d)

    pred = sum(weighted_vals) / sum(weights_used)
    # Crude std_err proxy: weighted-distance stddev of values from prediction
    var = 0.0
    for s_lat, s_lon, val, w in samples:
        d_mi = haversine_mi(target_lat, target_lon, s_lat, s_lon)
        if 0.5 <= d_mi <= IDW_MAX_DISTANCE_MI:
            idw_w = w / (d_mi ** IDW_POWER)
            var += idw_w * ((val - pred) ** 2)
    std_err = math.sqrt(var / sum(weights_used)) if sum(weights_used) > 0 else None
    return (pred, std_err, len(weights_used), nearest_d)


def compute_field_for(date_: date, commodity: str, delivery: str = "spot") -> int:
    """Compute basis field for one (date, commodity, delivery) combination.
    Returns the number of grid cells written."""
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Pull samples for this date+delivery_month (or nearest preceding date)
        cur.execute("""
            SELECT lat, lon, basis_cents, cash_price, sample_weight, location_label
            FROM bronze.cash_bid_observation
            WHERE commodity = %s AND delivery_month = %s
              AND observation_date = (
                SELECT MAX(observation_date)
                FROM bronze.cash_bid_observation
                WHERE commodity = %s AND delivery_month = %s
                  AND observation_date <= %s
              )
              AND basis_cents IS NOT NULL
        """, (commodity, delivery, commodity, delivery, date_))
        rows = cur.fetchall()

        if not rows:
            return 0

        # Build sample list for IDW
        samples = [
            (float(r['lat']), float(r['lon']),
             float(r['basis_cents']), float(r['sample_weight'] or 1.0))
            for r in rows
        ]

        # Compute futures contract for the day (front month) for cash_price reconstruction
        # For now, store basis only and let downstream join to futures
        cur.execute("""
            DELETE FROM silver.basis_field_grid
            WHERE observation_date = %s AND commodity = %s
              AND delivery_month = %s AND method = %s
        """, (date_, commodity, delivery, METHOD))

        # Iterate grid and insert predictions
        n_written = 0
        rows_to_insert = []
        for cell_lat, cell_lon in grid_cells():
            pred, std_err, n_samples, nearest = idw_predict(cell_lat, cell_lon, samples)
            if pred is None:
                continue
            rows_to_insert.append((
                date_, commodity, delivery,
                cell_lat, cell_lon, GRID_RESOLUTION_DEG,
                round(pred, 2), None,  # cash_price computed later if needed
                round(std_err, 2) if std_err else None,
                n_samples, round(nearest, 1),
                METHOD, METHOD_VERSION,
            ))

        # Bulk insert
        psycopg2.extras.execute_values(cur, """
            INSERT INTO silver.basis_field_grid (
                observation_date, commodity, delivery_month,
                cell_lat, cell_lon, grid_resolution_deg,
                basis_cents, cash_price, std_err, n_samples, nearest_sample_mi,
                method, method_version
            ) VALUES %s
        """, rows_to_insert)
        n_written = len(rows_to_insert)
        conn.commit()
        return n_written


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commodity", default="soybeans")
    ap.add_argument("--delivery", default="spot")
    ap.add_argument("--date", type=lambda s: date.fromisoformat(s), default=None)
    ap.add_argument("--backfill-days", type=int, default=0,
                    help="Generate fields for last N days")
    args = ap.parse_args()

    if args.backfill_days > 0:
        end = date.today()
        for offset in range(args.backfill_days):
            d = end - timedelta(days=offset)
            n = compute_field_for(d, args.commodity, args.delivery)
            print(f"  {d}  {args.commodity}/{args.delivery}  cells={n}")
    else:
        d = args.date or date.today()
        n = compute_field_for(d, args.commodity, args.delivery)
        print(f"\nField computed: {d}  {args.commodity}/{args.delivery}  cells={n}")
        print(f"Method: {METHOD} ({METHOD_VERSION})")


if __name__ == "__main__":
    main()
