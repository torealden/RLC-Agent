-- 155_reference_month_calendar.sql
--
-- A pure-calendar MONTH dimension, so any month-indexed series in the system can join on (year, month)
-- and know a fundamental fact about that month: WHICH DAY OF THE WEEK IT ENDS ON, and in particular
-- whether it ends on a weekend (Tore 2026-07-26).
--
-- WHY THIS IS FUNDAMENTAL, not a meal-specific hack. USDA's month-end STOCKS reports carry a
-- measurement artifact: when the calendar month ends on a weekend, the reported month-end stock tends
-- to run HIGH (the last physical count is the preceding Friday; two days of drawdown are still on the
-- books). Verified on US soybean-meal stocks / next-month domestic use (2020-2026, n=76): Sunday-ending
-- months average 0.110 vs 0.087 on weekday-ending months (+15%, +1.30 pp); Saturday-ending is only
-- mildly elevated. The effect is a property of the CALENDAR, not of meal, so it belongs in a shared
-- dimension every dated series can reference -- not re-derived in each model. (Scan other month-end
-- series for the same signature; the flag makes that a one-line join.)
--
-- GRAIN: one row per calendar month. Pure calendar -- no marketing-year mapping (MY start differs by
-- commodity; keep this layer commodity-agnostic and let each model bucket to its own MY).
-- iso_dow: 1=Mon .. 7=Sun (Postgres isodow), so weekend = (6,7), Sunday = 7.
-- Range 1990-2050 covers the balance-sheet grid (1990/91 anchor) with headroom past the 2045/46 tail.

CREATE TABLE IF NOT EXISTS reference.month_calendar (
    year                 integer NOT NULL,
    month                integer NOT NULL,
    month_start_date     date    NOT NULL,
    month_end_date       date    NOT NULL,
    days_in_month        integer NOT NULL,
    month_end_iso_dow    integer NOT NULL,   -- 1=Mon .. 7=Sun
    month_end_dow_name   text    NOT NULL,   -- 'Monday' .. 'Sunday'
    ends_on_weekend      boolean NOT NULL,   -- iso_dow IN (6,7)
    ends_on_saturday     boolean NOT NULL,   -- iso_dow = 6
    ends_on_sunday       boolean NOT NULL,   -- iso_dow = 7  (the strong signal)
    quarter              integer NOT NULL,
    is_quarter_end_month boolean NOT NULL,   -- month IN (3,6,9,12)
    PRIMARY KEY (year, month),
    CONSTRAINT month_calendar_month_ck CHECK (month BETWEEN 1 AND 12)
);

TRUNCATE reference.month_calendar;

INSERT INTO reference.month_calendar
SELECT
    EXTRACT(year  FROM d)::int,
    EXTRACT(month FROM d)::int,
    d::date,
    me::date,
    EXTRACT(day    FROM me)::int,
    EXTRACT(isodow FROM me)::int,
    TRIM(TO_CHAR(me, 'Day')),
    EXTRACT(isodow FROM me)::int IN (6, 7),
    EXTRACT(isodow FROM me)::int = 6,
    EXTRACT(isodow FROM me)::int = 7,
    EXTRACT(quarter FROM d)::int,
    EXTRACT(month   FROM d)::int IN (3, 6, 9, 12)
FROM generate_series('1990-01-01'::date, '2050-12-01'::date, interval '1 month') AS g(d)
CROSS JOIN LATERAL (SELECT (d + interval '1 month' - interval '1 day') AS me) x;

COMMENT ON TABLE reference.month_calendar IS
'Pure-calendar MONTH dimension (Tore 2026-07-26). One row per (year, month), 1990-2050. Carries the '
'fundamental fact that USDA month-end STOCKS reports need: whether the month ends on a weekend '
'(ends_on_weekend / ends_on_sunday) -- month-end stock counts run high when the month ends on a '
'weekend because the last physical count is the preceding Friday. Verified on soybean-meal stocks: '
'Sunday-ending +15% vs weekday-ending. Join any month-indexed series on (year, month). Commodity-'
'agnostic: no marketing-year mapping (MY start differs by commodity).';
COMMENT ON COLUMN reference.month_calendar.ends_on_sunday IS
'Month ends on a Sunday (iso_dow=7). The strongest weekend signal in the meal-stocks study -- Saturday-'
'ending months are only mildly elevated, Sunday-ending materially so.';
