"""Standing flat-file guards (design forecast_layer_design_v1.md D7 item 2).

The vintage ladder's one hard invariant: within a single key, `vintage_rank` is UNIQUE. The balance
sheet reads a key with `SUMIFS(value, ..., vintage_rank = MAXIFS(rank, ...))`. That is single-valued
ONLY if exactly one row sits at the winning rank. If a future data add ever puts two distinct
vintages at the same max rank on one key, `DISTINCT ON (...vintage_rank)` in the writer would silently
drop one and `SUMIFS` would double-count — a silent, hard-to-find error.

This guard makes that failure LOUD. Call it in every flat-file build, on the source `silver.*`
table, BEFORE (or right after) writing the flat file. It raises `MaxRankCollision` if any key carries
more than one vintage at its max rank. Verified 0 collisions across silver.tallow_balance and
silver.wheat_series as of 2026-07-24; the guard exists so that stays true by assertion, not by hope.
"""
from __future__ import annotations


class MaxRankCollision(AssertionError):
    """Raised when >1 distinct vintage sits at the winning (max) vintage_rank for some key."""


def assert_no_maxrank_collision(cur, table: str, key_cols: list[str],
                                rank_col: str = "vintage_rank",
                                vintage_col: str = "vintage") -> int:
    """Assert the unique-rank-within-key invariant on `table`.

    Args:
        cur: an open DB cursor (RealDictCursor).
        table: fully-qualified source table, e.g. 'silver.tallow_balance'.
        key_cols: the key that identifies one balance-sheet cell, e.g.
            ['commodity','class','series','marketing_year','period'] for wheat, or
            ['class','series','period'] for tallow.
        rank_col / vintage_col: column names (defaults match the estate convention).

    Returns:
        0 on success (no collision).

    Raises:
        MaxRankCollision: listing up to 10 offending keys.
    """
    keys = ", ".join(key_cols)
    join = " AND ".join(f"mx.{k}=t.{k}" for k in key_cols)
    sql = f"""
        WITH mx AS (
            SELECT {keys}, max({rank_col}) AS mr
            FROM {table} GROUP BY {keys}
        )
        SELECT {', '.join('t.'+k for k in key_cols)},
               count(DISTINCT t.{vintage_col}) AS nv,
               string_agg(DISTINCT t.{vintage_col}, ',') AS vintages,
               mx.mr AS max_rank
        FROM {table} t
        JOIN mx ON {join} AND mx.mr = t.{rank_col}
        GROUP BY {', '.join('t.'+k for k in key_cols)}, mx.mr
        HAVING count(DISTINCT t.{vintage_col}) > 1
        ORDER BY nv DESC
        LIMIT 10
    """
    cur.execute(sql)
    bad = cur.fetchall()
    if bad:
        lines = [f"  {tuple(r[k] for k in key_cols)} -> {r['nv']} vintages "
                 f"[{r['vintages']}] at rank {r['max_rank']}" for r in bad]
        raise MaxRankCollision(
            f"{table}: {len(bad)}+ key(s) carry >1 vintage at max rank "
            f"(SUMIFS would double-count). First offenders:\n" + "\n".join(lines))
    return 0
