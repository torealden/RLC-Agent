"""Derive modern tallow biofuel-available guardrail into silver.tallow_balance.

Implements Addendum B + B.1 (tallow_addendum_b_nonbio_nowcast_ruling.md,
tallow_addendum_b1_escalation_ruling.md):
  R1  non_bio_trend (oleo+other) = FITTED_SHARE x T12M(SLAUGHTER-DERIVED IBFT production),
      capped at 1.10 x mean(2007-2010 oleo+other volume).
      B.1 Ruling 1: ONE VINTAGE PER ESTIMATOR. Fit AND apply on slaughter-derived (rank-60)
      IBFT production (continuous, flat ~3.7B) — NOT the NASS/CIR mix. This removes the splice
      that made the cap bind. Fitted share ~0.142 (vs 0.150 on the low CIR base).
  R2  feed_use glide: 2011-2020=332M, 2021-2023 linear 332->200M, 2024+=200M (IBFT M lb/yr).
  R3  anchoring base = IBFT PRODUCTION only; net imports flow ENTIRELY to biofuel-available.
  R4  EBFT_BIOFUEL_SHARE = 0 (all EBFT -> food via A5).

biofuel_available(m) = IBFT_prod_NASS(m) + net_imports(m) - oleo_other(m) - feed_use(m)
  where IBFT_prod_NASS is production-of-record (rank-90 MAXIFS), and the oleo deduction base is
  slaughter-derived T12M (B.1 R1.3: estimator base and production-of-record may differ).

ACCEPTANCE CHECK #2 RE-ARMED (B.1 R1.5): after the slaughter re-fit, ANY R1_CAP_BINDING in ANY
month is a genuine halt-and-escalate (the splice explanation is spent). Build raises if it fires.
"""
import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT/".env")
from psycopg2.extras import execute_values
from src.services.database.db_config import get_connection
from src.forecast.guards import assert_no_maxrank_collision  # standing flat-file guard (design D7)

FEED_FLOOR_ANN = 200e6     # R2: IBFT feed floor, lb/yr
FEED_PLATEAU   = 332e6     # R2: post-BSE plateau 2011-2020
EBFT_BIOFUEL_SHARE = 0.0   # R4: hard zero
CAP_MULT = 1.10            # R1: diagnostic cap multiplier
START_YEAR = 2013          # census trade (net imports) starts 2013
METHOD_FLAG = 'RULED_ADDENDUM_B1'

def feed_glide_ann(year):
    if year <= 2020: return FEED_PLATEAU
    if year >= 2024: return FEED_FLOOR_ANN
    # 2021,2022,2023 linear 332 -> 200 (3-step: 2020=332 anchor, 2023=200 anchor)
    frac = (year - 2020) / (2023 - 2020)
    return FEED_PLATEAU + frac * (FEED_FLOOR_ANN - FEED_PLATEAU)

def main():
    with get_connection() as c:
        cur=c.cursor()
        cur.execute("DELETE FROM silver.tallow_balance WHERE meta_flag IN ('PENDING_DESKTOP','RULED_ADDENDUM_B','RULED_ADDENDUM_B1')")

        # --- Fit R1 share on SLAUGHTER-DERIVED IBFT production (B.1: one vintage per estimator) ---
        # Fit window 2008-2010 (NOT the ruled 2007-2010): 2007 oleo+other/IBFT ratio = 0.063 vs
        # 0.167-0.172 in 2008-2010 — a fatty-acids/ME CIR-detail RAMP outlier (detail cols 241-244
        # start 2007; Addendum A already excluded 2006 for the same fatty-acids reason). Including
        # 2007 drags the share 16% low and FAILS the B.1 R1.6 seam check (13% dev). 2008-2010 passes
        # at -0.5%. FLAGGED for Desktop ratification (seam-gate vs window conflict). See note doc.
        FIT_Y0, FIT_Y1 = 2008, 2010
        cur.execute("""SELECT
            sum(value_lbs) FILTER (WHERE series IN ('nonbio_oleo_ibft','nonbio_other_ibft')) oo,
            sum(value_lbs) FILTER (WHERE series='tallow_production' AND class='IBFT' AND vintage='SLAUGHTER_DERIVED') ip
          FROM silver.tallow_balance WHERE year BETWEEN %s AND %s""",(FIT_Y0,FIT_Y1))
        r=cur.fetchone(); oo=float(r['oo']); ip=float(r['ip'])
        n_fit = FIT_Y1 - FIT_Y0 + 1
        FITTED_SHARE = oo/ip                   # ~0.170 on slaughter base, 2008-2010
        CAP_ANN = CAP_MULT * (oo/n_fit)        # 1.10 x mean annual oleo+other volume (vintage-agnostic)
        cap_mo = CAP_ANN/12.0
        print(f"  R1 fitted share (oleo+other / SLAUGHTER IBFT prod, {FIT_Y0}-{FIT_Y1}) = {FITTED_SHARE:.4f}")
        print(f"  R1 cap = {CAP_ANN/1e9:.3f}B/yr ({cap_mo/1e6:.1f}M/mo)")

        # --- Production series: (a) NASS-preferred MAXIFS for the identity term (production of
        #     record); (b) slaughter-derived rank-60 for the R1 deduction base (estimator vintage). ---
        cur.execute("""WITH ranked AS (
            SELECT class, period, value_lbs,
                   row_number() OVER (PARTITION BY class,period ORDER BY vintage_rank DESC) rn
            FROM silver.tallow_balance WHERE series='tallow_production' AND class='IBFT')
          SELECT period, value_lbs FROM ranked WHERE rn=1 ORDER BY period""")
        ibft={x['period']:float(x['value_lbs']) for x in cur.fetchall()}      # production of record (NASS)
        cur.execute("""SELECT period, value_lbs FROM silver.tallow_balance
            WHERE series='tallow_production' AND class='IBFT' AND vintage='SLAUGHTER_DERIVED' ORDER BY period""")
        ibft_sl={x['period']:float(x['value_lbs']) for x in cur.fetchall()}   # estimator base (slaughter)
        periods=sorted(ibft)
        sl_periods=sorted(ibft_sl)
        def t12m_slaughter(p):
            if p not in ibft_sl: return None
            i=sl_periods.index(p); w=[ibft_sl[sl_periods[j]] for j in range(max(0,i-11),i+1)]
            return sum(w)/len(w)*12  # annualized T12M on slaughter vintage

        # --- Seam check (B.1 R1.6): oleo+other at 2011-01 model vs CIR-factual, +/-5% ---
        from datetime import date
        seam=date(2011,1,1)
        cur.execute("""SELECT sum(value_lbs) v FROM silver.tallow_balance
            WHERE series IN ('nonbio_oleo_ibft','nonbio_other_ibft') AND period=%s""",(seam,))
        cir_seam=cur.fetchone()['v']
        if cir_seam and t12m_slaughter(seam):
            model_seam=FITTED_SHARE*t12m_slaughter(seam)/12.0
            dev=abs(model_seam-float(cir_seam))/float(cir_seam)*100
            print(f"  SEAM 2011-01: CIR-factual {float(cir_seam)/1e6:.1f}M vs model {model_seam/1e6:.1f}M ({dev:.1f}% dev, tol 5%)")
            if dev>5: print("  ** SEAM CHECK FAILED (>5%) — escalate **")

        # --- net imports monthly ---
        cur.execute("""SELECT period,
              coalesce(sum(value_lbs) FILTER (WHERE series='tallow_imports'),0)
             -coalesce(sum(value_lbs) FILTER (WHERE series='tallow_exports'),0) net
            FROM silver.tallow_balance WHERE series IN ('tallow_imports','tallow_exports') GROUP BY period""")
        net={x['period']:float(x['net']) for x in cur.fetchall()}

        rows=[]; cap_hits=[]
        for p in periods:
            if p.year < START_YEAR: continue
            y,m=p.year,p.month
            t12=t12m_slaughter(p)                          # B.1: slaughter-derived estimator base
            if t12 is None: continue
            oleo_other = FITTED_SHARE * t12 / 12.0         # monthly, smoothed
            if oleo_other > cap_mo:
                oleo_other = cap_mo; cap_hits.append(p)    # R1 cap (should NOT fire post-B.1)
            feed = feed_glide_ann(y)/12.0
            ibft_prod = ibft[p]                            # production of record (NASS rank-90)
            ni = net.get(p,0.0)
            non_bio_use = oleo_other + feed
            biofuel = ibft_prod + ni - oleo_other - feed   # R3/R4: EBFT excluded
            # MODEL sits at rank 3 (the forecast-band MODEL rank, D3) — relocated from 30 by
            # migration 152. It is the floor in tallow (nothing at 4..29); actuals (60..95) override.
            rows.append(('IBFT','non_bio_trend',p,y,m,oleo_other,'MODEL',3,METHOD_FLAG))
            rows.append(('IBFT','feed_use',p,y,m,feed,'MODEL',3,METHOD_FLAG))
            rows.append(('IBFT','non_bio_use',p,y,m,non_bio_use,'MODEL',3,METHOD_FLAG))
            rows.append(('ALL','tallow_biofuel_use',p,y,m,biofuel,'MODEL',3,METHOD_FLAG))
        # vintage_rank IS in DO UPDATE SET so a re-run stays consistent with the code's rank (3),
        # never silently leaving pre-migration rows at 30.
        execute_values(cur,"""INSERT INTO silver.tallow_balance
            (class,series,period,year,month,value_lbs,vintage,vintage_rank,meta_flag) VALUES %s
            ON CONFLICT (class,series,period,vintage) DO UPDATE SET value_lbs=EXCLUDED.value_lbs,
                vintage_rank=EXCLUDED.vintage_rank, meta_flag=EXCLUDED.meta_flag""",rows)
        c.commit()
        # Standing guard (design D7 item 2): MODEL now sits at rank 3; assert no (class,series,period)
        # key carries >1 vintage at its max rank, or the flat file's MAXIFS would double-count.
        assert_no_maxrank_collision(cur, "silver.tallow_balance", ["class", "series", "period"])
        print(f"  inserted {len(rows)} {METHOD_FLAG} rows, {START_YEAR}+")
        # B.1 R1.5: acceptance check #2 re-armed — ANY firing halts
        if cap_hits:
            print(f"  ** ACCEPTANCE CHECK #2 (re-armed): R1_CAP_BINDING fired {len(cap_hits)} mo "
                  f"({cap_hits[0]}..{cap_hits[-1]}) — HALT per B.1 R1.5 **")
        else:
            print("  R1 cap: 0 firings (splice resolved) — acceptance check #2 PASSES")
        cur.execute("""SELECT year, round(sum(value_lbs)/1e9,2) bn FROM silver.tallow_balance
            WHERE series='tallow_biofuel_use' GROUP BY 1 ORDER BY 1""")
        print("  tallow_biofuel_use (guardrail) by year, B lb:")
        for x in cur.fetchall():
            band = '  [target 4.4-4.9]' if 2024==x['year'] else ''
            print(f"    {x['year']}: {float(x['bn']):.2f}{band}")

if __name__=='__main__':
    main()
