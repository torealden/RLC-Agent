-- Migration 110: Fix gold.feedstock_allocation_national view
--
-- The original view filtered to the latest run_id per scenario via a
-- correlated subquery with no period filter. Result: only ONE month
-- (the most recently inserted run_id) was visible.
--
-- The allocator creates a fresh run_id per month, so when we ran across
-- 189 months we ended up with 190 run_ids and the view showed only the
-- last one. This change selects the latest run_id PER (scenario, period)
-- via DISTINCT ON, so the view shows all months with the most recent
-- allocation pass for each period.

CREATE OR REPLACE VIEW gold.feedstock_allocation_national AS
WITH latest_per_period AS (
    SELECT DISTINCT ON (scenario, period)
           scenario, period, run_id
    FROM gold.feedstock_allocation
    ORDER BY scenario, period, created_at DESC
)
SELECT
    fa.period,
    fa.scenario,
    fa.fuel_type,
    fa.feedstock_code,
    fp.feedstock_name,
    fp.category AS feedstock_category,
    SUM(fa.allocated_mil_lbs) AS total_mil_lbs,
    SUM(fa.allocated_mil_gal) AS total_mil_gal,
    COUNT(DISTINCT fa.facility_id) AS facility_count
FROM gold.feedstock_allocation fa
JOIN latest_per_period lpp
  ON lpp.scenario = fa.scenario
 AND lpp.period   = fa.period
 AND lpp.run_id   = fa.run_id
JOIN reference.feedstock_properties fp
  ON fp.feedstock_code::text = fa.feedstock_code::text
GROUP BY fa.period, fa.scenario, fa.fuel_type, fa.feedstock_code,
         fp.feedstock_name, fp.category, fp.sort_order
ORDER BY fa.period, fa.fuel_type, fp.sort_order;
