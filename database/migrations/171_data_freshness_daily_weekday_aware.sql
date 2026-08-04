-- 171: kill phantom daily OVERDUE alerts (cme_settlements)
--
-- The old daily rule was a midnight-boundary test: run_finished_at < CURRENT_DATE,
-- i.e. "has not run since UTC midnight". A daily collector that fires late in the
-- UTC day (cme_settlements, weekdays 21:00 UTC) is ALWAYS stale by that test at
-- the 08:00 ET overdue check — 21 phantom schedule_overdue events in the last 30
-- days while every run succeeded.
--
-- New daily rule: expected-by, weekday-aware. A daily collector is overdue when
-- its last finished run predates the most recent expected fire instant:
--   expected day  = yesterday, rolled back to Friday when that lands on a weekend
--                   (every 'daily' source in this estate is a weekday market/gov
--                   release — CME settles, H.10, EIA dailies)
--   expected time = data_source.expected_release_time_et (ET), midnight ET when null
-- The 08:00 ET check therefore fires only when yesterday's (or Friday's) expected
-- run is actually missing. Weekly/monthly rules unchanged — usda_nass_stocks
-- (last run 2026-06-01) keeps alerting, correctly.

CREATE OR REPLACE VIEW core.data_freshness AS
SELECT cs.collector_name,
    ds.name AS display_name,
    ds.category,
    cs.run_finished_at AS last_collected,
    cs.status AS last_status,
    cs.rows_collected AS last_row_count,
    cs.data_period,
    cs.is_new_data,
    EXTRACT(epoch FROM now() - cs.run_finished_at) / 3600::numeric AS hours_since_collection,
    ds.expected_frequency,
    ds.expected_release_day,
    ds.expected_release_time_et,
        CASE
            WHEN ds.expected_frequency::text = 'daily'::text THEN
                cs.run_finished_at < (
                    (
                        CASE EXTRACT(dow FROM CURRENT_DATE)
                            WHEN 0 THEN CURRENT_DATE - 2   -- Sun -> Fri
                            WHEN 1 THEN CURRENT_DATE - 3   -- Mon -> Fri
                            WHEN 6 THEN CURRENT_DATE - 1   -- Sat -> Fri
                            ELSE CURRENT_DATE - 1          -- Tue-Fri -> yesterday
                        END
                        + COALESCE(ds.expected_release_time_et::time, '00:00'::time)
                    ) AT TIME ZONE 'America/New_York'
                )
            WHEN ds.expected_frequency::text = 'weekly'::text AND cs.run_finished_at < (CURRENT_DATE - '8 days'::interval) THEN true
            WHEN ds.expected_frequency::text = 'monthly'::text AND cs.run_finished_at < (CURRENT_DATE - '35 days'::interval) THEN true
            ELSE false
        END AS is_overdue
   FROM core.latest_collections cs
     LEFT JOIN data_source ds ON ds.collector_key = cs.collector_name;
