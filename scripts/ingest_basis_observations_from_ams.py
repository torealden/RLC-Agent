"""
Ingest AMS regional cash bids into bronze.cash_bid_observation as spatial
sample points.

Joins bronze.ams_price_record × reference.basis_region_centroid to attach
lat/lon to each observed regional bid. Each row becomes one sample point
in the basis-field interpolation domain.

Idempotent — re-run any time after AMS data refreshes.
"""
from datetime import date, timedelta
from dotenv import load_dotenv
load_dotenv()

import psycopg2.extras
from src.services.database.db_config import get_connection


def main():
    # Map AMS delivery_period strings → canonical CME futures contract codes.
    # Physical-delivery months map to the futures contract month commonly used
    # for hedging that delivery (the "next-rolling" contract):
    #   Jan-Feb delivery → F (Jan futures); Mar-Apr → H (Mar); May-Jun → K (May);
    #   Jul → N; Aug → Q; Sep → U; Oct-Nov → X; Dec → F (next year).
    # For soybeans this matches the CBOT/CME contract calendar.
    MONTH_TO_CONTRACT = {
        # (delivery_month_int, year_suffix_offset) → contract letter
        1: ("F", 0),  2: ("H", 0),  3: ("H", 0),  4: ("K", 0),
        5: ("K", 0),  6: ("N", 0),  7: ("N", 0),  8: ("Q", 0),
        9: ("U", 0), 10: ("X", 0), 11: ("X", 0), 12: ("F", 1),
    }
    DELIVERY_MAP_SQL = """
    CASE
      -- AMS uses 'Yes' to mean current/spot delivery
      WHEN delivery_period IS NULL OR delivery_period = '' OR delivery_period = 'Yes' THEN 'spot'
      -- Explicit YYYY-MM-DD delivery dates → contract letter+year
      WHEN delivery_period ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN
        CASE EXTRACT(MONTH FROM delivery_period::date)::int
          WHEN 1  THEN 'F' || RIGHT(EXTRACT(YEAR FROM delivery_period::date)::text, 2)
          WHEN 2  THEN 'H' || RIGHT(EXTRACT(YEAR FROM delivery_period::date)::text, 2)
          WHEN 3  THEN 'H' || RIGHT(EXTRACT(YEAR FROM delivery_period::date)::text, 2)
          WHEN 4  THEN 'K' || RIGHT(EXTRACT(YEAR FROM delivery_period::date)::text, 2)
          WHEN 5  THEN 'K' || RIGHT(EXTRACT(YEAR FROM delivery_period::date)::text, 2)
          WHEN 6  THEN 'N' || RIGHT(EXTRACT(YEAR FROM delivery_period::date)::text, 2)
          WHEN 7  THEN 'N' || RIGHT(EXTRACT(YEAR FROM delivery_period::date)::text, 2)
          WHEN 8  THEN 'Q' || RIGHT(EXTRACT(YEAR FROM delivery_period::date)::text, 2)
          WHEN 9  THEN 'U' || RIGHT(EXTRACT(YEAR FROM delivery_period::date)::text, 2)
          WHEN 10 THEN 'X' || RIGHT(EXTRACT(YEAR FROM delivery_period::date)::text, 2)
          WHEN 11 THEN 'X' || RIGHT(EXTRACT(YEAR FROM delivery_period::date)::text, 2)
          WHEN 12 THEN 'F' || RIGHT((EXTRACT(YEAR FROM delivery_period::date)::int + 1)::text, 2)
        END
      ELSE 'spot'
    END
    """

    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Insert into bronze.cash_bid_observation by joining AMS records to
        # centroid lookup. We compute basis as the explicit basis field if
        # present, otherwise (price - futures_settle * 100) when both available.
        cur.execute("""
            WITH ams_with_centroid AS (
                SELECT
                    a.report_date AS observation_date,
                    LOWER(a.commodity) AS commodity_norm,
                    -- Normalize commodity names
                    CASE
                        WHEN LOWER(a.commodity) LIKE '%soybean meal%' THEN 'soybean_meal'
                        WHEN LOWER(a.commodity) LIKE '%soybean oil%' THEN 'soybean_oil'
                        WHEN LOWER(a.commodity) LIKE '%soybean%' THEN 'soybeans'
                        WHEN LOWER(a.commodity) LIKE '%corn%' THEN 'corn'
                        WHEN LOWER(a.commodity) LIKE '%wheat%' THEN 'wheat'
                        WHEN LOWER(a.commodity) LIKE '%sorghum%' THEN 'sorghum'
                        WHEN LOWER(a.commodity) LIKE '%oat%' THEN 'oats'
                        WHEN LOWER(a.commodity) LIKE '%barley%' THEN 'barley'
                        ELSE LOWER(a.commodity)
                    END AS commodity,
                    -- Map AMS delivery_period to canonical CME contract code
                    (CASE
                      WHEN a.delivery_period IS NULL OR a.delivery_period = '' OR a.delivery_period = 'Yes' THEN 'spot'
                      WHEN a.delivery_period ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN
                        CASE EXTRACT(MONTH FROM a.delivery_period::date)::int
                          WHEN 1  THEN 'F' || RIGHT(EXTRACT(YEAR FROM a.delivery_period::date)::text, 2)
                          WHEN 2  THEN 'H' || RIGHT(EXTRACT(YEAR FROM a.delivery_period::date)::text, 2)
                          WHEN 3  THEN 'H' || RIGHT(EXTRACT(YEAR FROM a.delivery_period::date)::text, 2)
                          WHEN 4  THEN 'K' || RIGHT(EXTRACT(YEAR FROM a.delivery_period::date)::text, 2)
                          WHEN 5  THEN 'K' || RIGHT(EXTRACT(YEAR FROM a.delivery_period::date)::text, 2)
                          WHEN 6  THEN 'N' || RIGHT(EXTRACT(YEAR FROM a.delivery_period::date)::text, 2)
                          WHEN 7  THEN 'N' || RIGHT(EXTRACT(YEAR FROM a.delivery_period::date)::text, 2)
                          WHEN 8  THEN 'Q' || RIGHT(EXTRACT(YEAR FROM a.delivery_period::date)::text, 2)
                          WHEN 9  THEN 'U' || RIGHT(EXTRACT(YEAR FROM a.delivery_period::date)::text, 2)
                          WHEN 10 THEN 'X' || RIGHT(EXTRACT(YEAR FROM a.delivery_period::date)::text, 2)
                          WHEN 11 THEN 'X' || RIGHT(EXTRACT(YEAR FROM a.delivery_period::date)::text, 2)
                          WHEN 12 THEN 'F' || RIGHT((EXTRACT(YEAR FROM a.delivery_period::date)::int + 1)::text, 2)
                        END
                      ELSE 'spot'
                    END) AS delivery_month,
                    a.grade,
                    a.id::text AS source_record_id,
                    'ams' AS source,
                    c.region_id,
                    c.centroid_lat::numeric AS lat,
                    c.centroid_lon::numeric AS lon,
                    'AMS_' || a.slug_id || ' ' || a.location ||
                      COALESCE(' (' || a.delivery_point || ')', '') AS location_label,
                    a.price AS cash_price,
                    a.basis AS basis_cents,
                    NULL::numeric AS futures_settle,
                    NULL::text AS futures_contract,
                    -- Regional aggregate is "indicative" (not single-elevator)
                    TRUE AS is_indicative,
                    -- Sample weight: regional aggregates carry less weight than
                    -- single-elevator observations once we have them
                    0.7 AS sample_weight
                FROM bronze.ams_price_record a
                JOIN LATERAL (
                    -- Pick the most specific centroid match (prefer
                    -- exact delivery_point match over NULL wildcard)
                    SELECT region_id, centroid_lat, centroid_lon
                    FROM reference.basis_region_centroid c
                    WHERE c.source = 'AMS_' || a.slug_id::text
                      AND c.region_name = a.location
                      AND (c.delivery_point IS NULL OR a.delivery_point = c.delivery_point)
                    ORDER BY (c.delivery_point IS NULL) ASC -- non-NULL first
                    LIMIT 1
                ) c ON TRUE
                WHERE a.transaction_type = 'Bid'
                  AND a.commodity IS NOT NULL
                  AND a.report_date >= CURRENT_DATE - INTERVAL '180 days'
                  AND (a.price IS NOT NULL OR a.basis IS NOT NULL)
            )
            INSERT INTO bronze.cash_bid_observation (
                observation_date, commodity, delivery_month, grade,
                source, source_record_id, region_id,
                lat, lon, location_label,
                cash_price, basis_cents, futures_settle, futures_contract,
                is_indicative, sample_weight
            )
            SELECT
                observation_date, commodity, delivery_month, grade,
                source, source_record_id, region_id,
                lat, lon, location_label,
                cash_price, basis_cents, futures_settle, futures_contract,
                is_indicative, sample_weight
            FROM ams_with_centroid
            ON CONFLICT (observation_date, source, source_record_id, commodity, delivery_month) DO UPDATE SET
                cash_price = EXCLUDED.cash_price,
                basis_cents = EXCLUDED.basis_cents,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                location_label = EXCLUDED.location_label,
                sample_weight = EXCLUDED.sample_weight,
                collected_at = NOW()
        """)
        n_ingested = cur.rowcount
        conn.commit()

        # Stats
        cur.execute("""
            SELECT commodity, COUNT(*) AS n,
                   COUNT(DISTINCT observation_date) AS dates,
                   COUNT(DISTINCT (lat, lon)) AS sample_locations,
                   MIN(observation_date) AS pmin, MAX(observation_date) AS pmax
            FROM bronze.cash_bid_observation
            WHERE source = 'ams'
            GROUP BY commodity
            ORDER BY n DESC
        """)
        rows = cur.fetchall()
        print(f"\nbronze.cash_bid_observation populated. Recent affected: {n_ingested}")
        print(f"\nFull AMS coverage:")
        print(f"  {'commodity':<18s} {'rows':>7s} {'dates':>6s} {'locs':>6s}  range")
        for r in rows:
            print(f"  {r['commodity']:<18s} {r['n']:>7d} {r['dates']:>6d} "
                  f"{r['sample_locations']:>6d}  {r['pmin']} → {r['pmax']}")


if __name__ == "__main__":
    main()
