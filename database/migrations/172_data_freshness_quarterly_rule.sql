-- 172: quarterly rule for core.data_freshness + fix usda_nass_stocks metadata
--
-- usda_nass_stocks was classified expected_frequency='weekly' in data_source, so
-- the freshness view flagged it overdue weekly against a QUARTERLY release
-- (Grain Stocks: last biz day of Mar/Jun/Sep + mid-January annual recap) — 21
-- alerts in 30 days. Reclassifying to 'quarterly' alone would silence it forever
-- (the view had no quarterly branch -> ELSE false), so this migration adds one:
-- overdue when the last finished run is more than 110 days old. Max legitimate
-- gap between post-release collections is ~104 days (Oct 1 -> mid-Jan), so 110
-- flags only a fully missed release cycle.
--
-- Also tightens mig 171's daily rule with a 15-minute tolerance: APScheduler
-- fires cron jobs a second or two early, so a fast collector scheduled AT the
-- expected release minute (cme_settlements, 17:00 ET, ~1s runtime) can FINISH
-- fractionally before the expected instant and get flagged (observed 8/5).

UPDATE data_source
SET expected_frequency = 'quarterly',
    expected_release_day = NULL
WHERE collector_key = 'usda_nass_stocks';

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
                    - interval '15 minutes'
                )
            WHEN ds.expected_frequency::text = 'weekly'::text AND cs.run_finished_at < (CURRENT_DATE - '8 days'::interval) THEN true
            WHEN ds.expected_frequency::text = 'monthly'::text AND cs.run_finished_at < (CURRENT_DATE - '35 days'::interval) THEN true
            WHEN ds.expected_frequency::text = 'quarterly'::text AND cs.run_finished_at < (CURRENT_DATE - '110 days'::interval) THEN true
            ELSE false
        END AS is_overdue
   FROM core.latest_collections cs
     LEFT JOIN data_source ds ON ds.collector_key = cs.collector_name;
