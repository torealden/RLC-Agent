-- Migration 063: dedupe gold.cross_company_director_links
-- Each year's DEF 14A produces a fresh director_appointment row (we don't
-- enforce uniqueness, so we can keep year-by-year disclosures). The
-- cross-company view should aggregate across years rather than emit one
-- pair per disclosure year.
--
-- Strategy: aggregate by (operator pair, person), keep the most recent
-- disclosure metadata, OR-aggregate the both_active flag (any disclosure
-- where both currently serving = the link is currently active).

BEGIN;

CREATE OR REPLACE VIEW gold.cross_company_director_links AS
WITH pairs AS (
    SELECT
        LEAST(LOWER(d1.operator), LOWER(d2.operator))    AS operator_a,
        GREATEST(LOWER(d1.operator), LOWER(d2.operator)) AS operator_b,
        d1.person_normalized,
        d1.operator                                       AS d1_operator,
        d2.operator                                       AS d2_operator,
        d1.person_name                                    AS d1_name,
        d2.person_name                                    AS d2_name,
        d1.role                                           AS d1_role,
        d2.role                                           AS d2_role,
        d1.last_disclosed_year                            AS d1_year,
        d2.last_disclosed_year                            AS d2_year,
        (d1.departed_date IS NULL AND d2.departed_date IS NULL) AS both_active
    FROM silver.director_appointment d1
    JOIN silver.director_appointment d2
      ON d1.person_normalized = d2.person_normalized
     AND LOWER(d1.operator) < LOWER(d2.operator)
)
SELECT
    operator_a,
    operator_b,
    -- Pick a display name (first match — they should be identical anyway)
    MAX(CASE WHEN LOWER(d1_operator) = operator_a THEN d1_operator END) AS operator_a_display,
    MAX(CASE WHEN LOWER(d2_operator) = operator_b THEN d2_operator END) AS operator_b_display,
    person_normalized,
    MAX(d1_name)                                AS person_name_a,
    MAX(d2_name)                                AS person_name_b,
    MAX(d1_role)                                AS role_a,
    MAX(d2_role)                                AS role_b,
    MAX(d1_year)                                AS year_a,
    MAX(d2_year)                                AS year_b,
    BOOL_OR(both_active)                        AS both_active,
    COUNT(*)                                    AS disclosure_count
FROM pairs
GROUP BY operator_a, operator_b, person_normalized;

COMMIT;
