-- Migration 109: Mark UNKNOWN destination as NOT regional_total
--
-- Migration 108 inserted UNKNOWN with is_regional_total=TRUE on the theory
-- that it acted like a region. But the .bas updaters filter regional-total
-- rows out of writes (because those rows are computed by in-sheet SUM
-- formulas, not populated from the DB). UNKNOWN is real data from the
-- FAS ESR feed and needs to be written — so it must be is_regional_total=FALSE.

UPDATE silver.trade_country_reference
SET is_regional_total = FALSE,
    updated_at = NOW()
WHERE country_name = 'UNKNOWN';
