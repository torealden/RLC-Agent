-- Migration 050: Gold view for US soybean oil export-implied refining margin
--
-- Date: 2026-05-06
--
-- Why:
--   USDA does not publish RBD soybean oil prices (verified across AMS MARS
--   API and ERS Oil Crops Yearbook). Census export trade data is the only
--   public source where we can compute an implied RBD price from
--   value-divided-by-quantity at the world-total level.
--
--   The series is a Gulf-export-implied price, NOT Central Illinois cash.
--   Geography differs and volumes are smaller than domestic flow, but
--   refining margins should track each other in direction. Useful as
--   an internal reference and as an external chart input when paired
--   with a clear caveat about what it is.
--
-- Method:
--   Crude exports HS 1507100000  (Schedule B "Crude soybean oil")
--   RBD exports   HS 1507904050  (Schedule B "Soybean oil, fully refined" =
--                                  food-grade RBD; "FLY-RFND" in Census)
--   Filter to country_code = '-' (World Total convention)
--   Implied unit value = value_usd / quantity_kg / 2.20462 * 100  (cents/lb)
--   Margin = RBD_cents_lb - crude_cents_lb
--
-- Limitations callers must understand:
--   - Months with crude_qty_kg < 5,000,000 are flagged volume_unreliable.
--     Crude exports collapsed in some months (notably 2022-2023) as US
--     shifted toward biofuel use; small-lot transactions during those
--     months produce non-representative implied prices.
--   - Gulf export pricing differs from Central Illinois cash; refiners
--     selling internationally may have different price structure.
--   - 3-month rolling smoother in margin_3mo_smoothed is volume-aware:
--     it includes only months where the underlying crude price is
--     representative. Use this column for charts.

DROP VIEW IF EXISTS gold.us_soybean_oil_export_refining_margin;

CREATE VIEW gold.us_soybean_oil_export_refining_margin AS
WITH crude AS (
    SELECT year, month,
           SUM(value_usd) AS val_usd,
           SUM(quantity)  AS qty_kg
    FROM bronze.census_trade
    WHERE hs_code = '1507100000'
      AND flow = 'exports'
      AND country_code = '-'
      AND quantity > 0
    GROUP BY year, month
),
rbd AS (
    SELECT year, month,
           SUM(value_usd) AS val_usd,
           SUM(quantity)  AS qty_kg
    FROM bronze.census_trade
    WHERE hs_code = '1507904050'
      AND flow = 'exports'
      AND country_code = '-'
      AND quantity > 0
    GROUP BY year, month
),
joined AS (
    SELECT COALESCE(c.year, r.year)   AS year,
           COALESCE(c.month, r.month) AS month,
           c.qty_kg AS crude_qty_kg,
           c.val_usd AS crude_val_usd,
           r.qty_kg AS rbd_qty_kg,
           r.val_usd AS rbd_val_usd,
           c.val_usd / NULLIF(c.qty_kg, 0) / 2.20462 * 100 AS crude_cents_lb,
           r.val_usd / NULLIF(r.qty_kg, 0) / 2.20462 * 100 AS rbd_cents_lb,
           CASE WHEN c.qty_kg < 5000000 THEN TRUE ELSE FALSE END AS volume_unreliable
    FROM crude c FULL OUTER JOIN rbd r USING (year, month)
)
SELECT year,
       month,
       (year || '-' || lpad(month::text, 2, '0'))::text AS year_month,
       crude_qty_kg,
       crude_val_usd,
       rbd_qty_kg,
       rbd_val_usd,
       ROUND(crude_cents_lb::numeric, 3) AS crude_cents_lb,
       ROUND(rbd_cents_lb::numeric, 3)   AS rbd_cents_lb,
       ROUND((rbd_cents_lb - crude_cents_lb)::numeric, 3) AS margin_cents_lb,
       volume_unreliable,
       -- Volume-aware 3-month rolling margin: averages last 3 months of
       -- non-flagged margins. Window function over chronological order.
       ROUND(
           AVG(CASE WHEN volume_unreliable THEN NULL
                    ELSE rbd_cents_lb - crude_cents_lb END)
               OVER (ORDER BY year, month
                     ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
           ::numeric, 3
       ) AS margin_3mo_smoothed
FROM joined
ORDER BY year, month;

COMMENT ON VIEW gold.us_soybean_oil_export_refining_margin IS
'US soybean oil refining margin, computed from Gulf export-implied unit values. Crude HS 1507100000 vs RBD HS 1507904050, both flow=exports country_code=''-'' (World Total). Months with crude_qty_kg < 5M flagged volume_unreliable; the 3-month smoother excludes those. NOT Central Illinois cash; use with caveats. Source: bronze.census_trade.';
