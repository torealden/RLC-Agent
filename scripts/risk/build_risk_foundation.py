"""Foundation for the quarterly VaR risk-budget process (BBD feedstocks).

Creates the `risk` schema and three foundation tables, then populates the two
that are pure data-derived:

  risk.feedstock_volatility   — annualized vol per feedstock (monthly log returns)
  risk.feedstock_correlation  — pairwise correlation of monthly returns (overlap window)
  risk.facility_budget_config — per-facility VaR budget + OVERRIDABLE initial coverage
                                (seeded with defaults; source='DEFAULT'. A calibrated
                                 real number from a facility drops in by UPDATE, no code
                                 change — "anchors are parameters, not code".)

Vol/corr feed the parametric variance-covariance VaR in the budget generator:
  VaR = z * sqrt( x' C x ),  x_i = $ position in feedstock i,  C_ij = vol_i vol_j corr_ij

Price proxies: tallow grades EBFT/IBFT use BFT prices; CSO uses SBO (thin/rare).
Common estimation window is data-driven per pair (fats/DCO/UCO start 2019); SBO/CO/PALM
run to 2000 but correlations are computed on the OVERLAP so the matrix stays consistent.

Idempotent: safe to re-run (tables created IF NOT EXISTS, rows re-computed via TRUNCATE).
"""
import sys
from pathlib import Path

ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
import numpy as np
import pandas as pd
from psycopg2.extras import execute_values
from src.services.database.db_config import get_connection

# allocator feedstock_code -> curated price series in silver.feedstock_supply.
# silver is the SAME price the allocator's margin engine consumes (one clean monthly
# number per feedstock), so VaR stays consistent with the economics. Raw bronze.feedstock_prices
# is a multi-region/margin grab-bag and is NOT used here.
PRICE_PROXY = {
    'SBO': 'SBO', 'CO': 'CO', 'DCO': 'DCO', 'UCO': 'UCO', 'CWG': 'CWG',
    'PF': 'PF', 'BFT': 'BFT', 'EBFT': 'BFT', 'IBFT': 'BFT',  # tallow grades share BFT price
    'YG': 'UCO',    # yellow grease -> UCO series (no distinct YG price in silver; YG_BIOFUEL=0)
    'CSO': 'CSO',   # cottonseed oil — flat placeholder, near-zero vol, rarely allocated
}
ALLOC_CODES = sorted(PRICE_PROXY.keys())
PRICE_CODES = sorted(set(PRICE_PROXY.values()))

# VaR budget defaults (facility-overridable)
DEFAULT_VAR_BUDGET_PCT = 0.08   # 95%/1-quarter VaR budget ≈ 8% of quarterly feedstock notional
DEFAULT_CONFIDENCE = 0.95
DEFAULT_HORIZON_MONTHS = 3       # one quarter


def monthly_returns(conn):
    """Monthly log returns per curated silver price series, then expanded to allocator
    codes via PRICE_PROXY. Returns a DataFrame indexed by month, columns = ALLOC_CODES."""
    cur = conn.cursor()
    cur.execute(
        """SELECT period, feedstock_code, avg_price_per_lb
           FROM silver.feedstock_supply
           WHERE feedstock_code = ANY(%s) AND avg_price_per_lb > 0
           ORDER BY period""", (PRICE_CODES,))
    df = pd.DataFrame([dict(r) for r in cur.fetchall()])
    df['period'] = pd.to_datetime(df['period'])
    df['avg_price_per_lb'] = df['avg_price_per_lb'].astype(float)
    # one clean monthly price per silver code (median across any PADD duplicates)
    piv = df.pivot_table(index='period', columns='feedstock_code',
                         values='avg_price_per_lb', aggfunc='median').sort_index()
    silver_rets = np.log(piv / piv.shift(1))
    # Winsorize monthly log returns at +/-0.5 (~ -40%/+65%). A real feedstock almost never
    # moves >50% in a month; anything beyond is a bad print (e.g. a $0.01 UCO outlier in the
    # early silver series) that would otherwise dominate vol. Robust, barely touches legit moves.
    silver_rets = silver_rets.clip(-0.5, 0.5)
    # expand to allocator codes (grades/proxies inherit their mapped series' returns)
    out = {code: silver_rets[PRICE_PROXY[code]] for code in ALLOC_CODES
           if PRICE_PROXY[code] in silver_rets.columns}
    return pd.DataFrame(out)


def main():
    with get_connection() as conn:
        cur = conn.cursor()

        # --- schema + tables ---
        cur.execute("CREATE SCHEMA IF NOT EXISTS risk")
        cur.execute("""CREATE TABLE IF NOT EXISTS risk.feedstock_volatility (
            feedstock_code text PRIMARY KEY, ann_vol numeric, mean_monthly_ret numeric,
            n_months int, window_start date, window_end date, computed_at timestamptz DEFAULT now())""")
        cur.execute("""CREATE TABLE IF NOT EXISTS risk.feedstock_correlation (
            feedstock_a text, feedstock_b text, corr numeric, n_overlap int,
            computed_at timestamptz DEFAULT now(), PRIMARY KEY (feedstock_a, feedstock_b))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS risk.facility_budget_config (
            facility_id int PRIMARY KEY,
            var_budget_pct numeric,          -- VaR budget as % of quarterly feedstock notional
            var_confidence numeric,          -- e.g. 0.95
            var_horizon_months int,          -- e.g. 3 (one quarter)
            coverage_override_pct numeric,   -- NULL => VaR-implied coverage; set => use this directly
            source text,                     -- DEFAULT | CALIBRATED_<company> | MANUAL
            notes text,
            updated_at timestamptz DEFAULT now())""")

        # --- vol + corr from monthly returns ---
        rets = monthly_returns(conn)
        vol_rows = []
        for code in rets.columns:
            r = rets[code].dropna()
            ann_vol = float(r.std() * np.sqrt(12))
            vol_rows.append((code, ann_vol, float(r.mean()), int(r.count()),
                             r.index.min().date(), r.index.max().date()))
        cur.execute("TRUNCATE risk.feedstock_volatility")
        execute_values(cur, """INSERT INTO risk.feedstock_volatility
            (feedstock_code, ann_vol, mean_monthly_ret, n_months, window_start, window_end)
            VALUES %s""", vol_rows)

        corr_rows = []
        codes = list(rets.columns)
        for i, a in enumerate(codes):
            for b in codes[i:]:
                if a == b:
                    ra = rets[a].dropna()
                    if len(ra) >= 12 and ra.std() > 0:
                        corr_rows.append((a, b, 1.0, len(ra)))
                    continue
                pair = rets[[a, b]].dropna()
                if len(pair) < 12 or pair[a].std() == 0 or pair[b].std() == 0:
                    continue  # skip degenerate/constant series (e.g. CSO flat placeholder)
                c = float(pair[a].corr(pair[b]))
                corr_rows.append((a, b, c, len(pair)))
                corr_rows.append((b, a, c, len(pair)))
        cur.execute("TRUNCATE risk.feedstock_correlation")
        execute_values(cur, """INSERT INTO risk.feedstock_correlation
            (feedstock_a, feedstock_b, corr, n_overlap) VALUES %s""", corr_rows)

        # --- seed facility_budget_config for all BBD facilities (defaults, overridable) ---
        cur.execute("""INSERT INTO risk.facility_budget_config
            (facility_id, var_budget_pct, var_confidence, var_horizon_months,
             coverage_override_pct, source, notes)
            SELECT DISTINCT f.facility_id, %s, %s, %s, NULL::numeric, 'DEFAULT',
                   'seeded default VaR budget; override coverage_override_pct if a facility calibrates'
            FROM reference.biofuel_facilities f
            WHERE f.fuel_type IN ('biodiesel','renewable_diesel','saf','coprocessing')
            ON CONFLICT (facility_id) DO NOTHING""",
            (DEFAULT_VAR_BUDGET_PCT, DEFAULT_CONFIDENCE, DEFAULT_HORIZON_MONTHS))

        conn.commit()

        # --- report ---
        print("risk.feedstock_volatility (annualized, monthly-return basis):")
        cur.execute("SELECT feedstock_code, round(ann_vol,4) v, n_months, window_start, window_end "
                    "FROM risk.feedstock_volatility ORDER BY ann_vol DESC")
        for r in cur.fetchall():
            print(f"   {r['feedstock_code']:6s} vol={float(r['v'])*100:5.1f}%/yr  n={r['n_months']:3d}  "
                  f"{r['window_start']}..{r['window_end']}")
        cur.execute("SELECT count(*) n FROM risk.facility_budget_config")
        print(f"\nrisk.facility_budget_config: {cur.fetchone()['n']} BBD facilities seeded "
              f"(VaR budget {DEFAULT_VAR_BUDGET_PCT:.0%}, {DEFAULT_CONFIDENCE:.0%}, {DEFAULT_HORIZON_MONTHS}mo)")
        print("\nSample correlations vs SBO:")
        cur.execute("SELECT feedstock_b, round(corr,3) c, n_overlap FROM risk.feedstock_correlation "
                    "WHERE feedstock_a='SBO' AND feedstock_b<>'SBO' ORDER BY corr DESC")
        for r in cur.fetchall():
            print(f"   SBO~{r['feedstock_b']:5s} {float(r['c']):+.3f}  (n={r['n_overlap']})")


if __name__ == "__main__":
    main()
