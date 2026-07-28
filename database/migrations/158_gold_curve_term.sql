-- 158_gold_curve_term.sql
--
-- Guidance Report price-feed layer, storage migration 3 of 3 (brief:
-- clients/Contracts/Helios/CLAUDE_CODE_BRIEF_price_feeds.md §"Storage contract" + §"Curve construction").
--
-- gold.curve_term is the IFV layer: a derived curve is stored ONLY as its named terms (board + freight
-- + duty + VAT + FX + basis residual, ...), one row per term, so a client can interrogate any component
-- instead of receiving an opaque single number. The headline value is the SUM of the terms and is NEVER
-- stored independently in this table -- if you want the headline, you add up the terms.
--
-- THE HARD TIE-OUT (brief: "terms must sum to the published value, fail loud, same discipline as the
-- band CHECK"):
--
--   A SUM-of-rows invariant cannot be a row-level CHECK. It is enforced by a DEFERRABLE CONSTRAINT
--   TRIGGER that fires at COMMIT, so a full stack (N term rows + the published headline mark) can be
--   written in any order inside one transaction and is validated once, atomically, at the end.
--
--   LINKAGE CONVENTION (this is a review point -- Tore approves methods): a derived curve's published
--   headline lives in silver.price_mark as a DERIVED_* row whose series_key EQUALS this table's
--   curve_key, on the same obs_date and tenor. So the tie-out is:
--
--       for each (curve_key, obs_date, tenor) touched, IF a silver.price_mark row exists with
--       series_key = curve_key, same obs_date, same tenor, and a DERIVED_* quality_rank,
--       THEN SUM(curve_term.term_value) over that group MUST equal that mark.value (within epsilon).
--
--   Direction of enforcement: this migration enforces it from the TERM side (you cannot leave terms
--   that contradict an existing derived headline). The converse -- forbidding a DERIVED_* headline mark
--   with NO backing terms -- is deliberately NOT enforced here; it belongs on price_mark and is left for
--   review so we do not block the AMS/official collectors (which write non-derived marks and no terms)
--   on a trigger they never touch. Flagged in the build summary.
--
--   EPSILON: abs(sum - value) <= greatest(0.01, abs(value) * 1e-4). Half-a-cent absolute floor for
--   penny-quoted boards, relaxing to 1 bp of the headline for large per-tonne values. A term
--   decomposition that misses by more than a rounding whisker is a modeling error and fails loud.
--
-- can_republish is NOT on curve_term: each term carries its own quality_rank and term_source, and the
-- headline mark in price_mark carries the republish right. A term row is our decomposition; the right
-- to publish the assembled number is decided on the mark it sums to.
--
-- NEW objects only; nothing to backfill (no derived curves exist yet).

BEGIN;

CREATE TABLE gold.curve_term (
    curve_key     text        NOT NULL,                 -- equals the derived series' price_mark.series_key
    obs_date      date        NOT NULL,
    tenor         text        NOT NULL,                 -- same tenor label vocabulary as price_mark
    term_name     text        NOT NULL,                 -- 'board' | 'freight' | 'duty_levy' | 'vat' | 'fx' | 'basis_residual' | ...
    term_value    numeric     NOT NULL,                 -- signed; the headline is SUM(term_value)
    term_source   text,                                 -- provenance of this specific term
    quality_rank  text        NOT NULL REFERENCES reference.price_quality_rank(rank_name),
    method_note   text,                                 -- how this term was derived
    collected_at  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT curve_term_pk PRIMARY KEY (curve_key, obs_date, tenor, term_name)
);

CREATE INDEX curve_term_key_date_idx ON gold.curve_term (curve_key, obs_date DESC, tenor);

COMMENT ON TABLE gold.curve_term IS
'IFV term stack: a derived curve stored ONLY as its named terms (one row per term). The headline value '
'is SUM(term_value) and is never stored here. A DEFERRABLE constraint trigger ties the sum to the '
'DERIVED_* headline in silver.price_mark (series_key = curve_key, same obs_date + tenor) within epsilon. '
'See migration 158 and the Helios price-feeds brief.';
COMMENT ON COLUMN gold.curve_term.term_value IS
'Signed term contribution. The published headline is the SUM of all term_value for (curve_key, obs_date, '
'tenor). Enforced against the price_mark headline by trigger curve_term_tieout.';
COMMENT ON COLUMN gold.curve_term.curve_key IS
'Curve identity. By convention EQUALS the published derived series'' silver.price_mark.series_key, which '
'is how the term-sum tie-out finds the headline to check against.';

-- ---------------------------------------------------------------------------------------------------
-- Term-sum tie-out. Deferred to COMMIT so a full stack lands in any order within one transaction.
-- ---------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gold.curve_term_tieout() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    k_curve   text;
    k_date    date;
    k_tenor   text;
    v_sum     numeric;
    v_head    numeric;
    v_eps     numeric;
BEGIN
    -- The row that fired us (INSERT/UPDATE -> NEW, DELETE -> OLD) identifies the group to re-check.
    IF TG_OP = 'DELETE' THEN
        k_curve := OLD.curve_key; k_date := OLD.obs_date; k_tenor := OLD.tenor;
    ELSE
        k_curve := NEW.curve_key; k_date := NEW.obs_date; k_tenor := NEW.tenor;
    END IF;

    -- Headline: the DERIVED_* published mark this curve produces. No headline yet => nothing to tie to
    -- (terms may legitimately be written before the mark, or exist for a non-published diagnostic curve).
    SELECT pm.value INTO v_head
    FROM silver.price_mark pm
    JOIN reference.price_quality_rank r ON r.rank_name = pm.quality_rank
    WHERE pm.series_key = k_curve
      AND pm.obs_date   = k_date
      AND pm.tenor      = k_tenor
      AND pm.quality_rank LIKE 'DERIVED\_%'
    ORDER BY r.rank_ordinal DESC
    LIMIT 1;

    IF v_head IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT COALESCE(SUM(term_value), 0) INTO v_sum
    FROM gold.curve_term
    WHERE curve_key = k_curve AND obs_date = k_date AND tenor = k_tenor;

    v_eps := GREATEST(0.01, ABS(v_head) * 1e-4);

    IF ABS(v_sum - v_head) > v_eps THEN
        RAISE EXCEPTION
            'curve_term tie-out FAILED for (%, %, %): terms sum to % but published headline is % (eps %)',
            k_curve, k_date, k_tenor, v_sum, v_head, v_eps
            USING ERRCODE = 'check_violation';
    END IF;

    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER curve_term_tieout
    AFTER INSERT OR UPDATE OR DELETE ON gold.curve_term
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION gold.curve_term_tieout();

COMMENT ON FUNCTION gold.curve_term_tieout() IS
'Hard term-sum tie-out for gold.curve_term (Helios price-feeds brief). At COMMIT, for each touched '
'(curve_key, obs_date, tenor) that has a DERIVED_* headline in silver.price_mark (series_key=curve_key), '
'asserts SUM(term_value) = headline within GREATEST(0.01, |headline|*1e-4). Fails loud otherwise. '
'Enforced from the term side only; mark-side enforcement is a separate review decision (migration 158).';

COMMIT;
