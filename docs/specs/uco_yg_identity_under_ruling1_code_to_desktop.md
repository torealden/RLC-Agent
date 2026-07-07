# UCO/YG identity under Ruling 1 — Code→Desktop open item (blocks allocator wiring)

**From:** Claude Code | **To:** Claude Desktop | **Owner:** Tore | **Date:** 2026-07-07
**Parent:** UCO_ruling_doc_and_contract.md (2026-07-05) + Ruling 1 (Tore, 2026-07-06) + the tallow
canonical work (2026-07-07). **Status:** OPEN — blocks the UCO leg of `silver.feedstock_supply`
and therefore the allocator re-run. Everything upstream of the split is built and verified.

## What's settled and verified

- **UCO supply is canonical and checksum-clean.** `silver.uco_yg_balance` CY2024:
  `uco_biofuel_use` = collection 3.30B + net_imports 5.43B = **8.73B** (Ruling 1, uncapped).
- **§5 import checksums PASS** at country level: China 2.81 (target 2.8), Canada 0.61, Malaysia
  0.34, UK 0.22 — exact. (A `TOTAL` row coexists with country rows in `silver.uco_imports`;
  naive `SUM` double-counts to 10.86B — the build correctly used the un-doubled 5.43B. Flagging
  the TOTAL-row trap for any downstream consumer.)
- **Allocator blocker diagnosed:** `silver.feedstock_supply` has **0 UCO rows** (allocator has
  nothing to allocate → ~0 UCO). 175 facilities are UCO-eligible, so eligibility is NOT the issue.

## The conflict that blocks wiring

The UCO ruling doc (2026-07-05) predates Ruling 1 (2026-07-06). Its §2 identity is EIA-tied:
```
uco_biofuel_use = min(collection + net_imports, EIA_YG)      # capped
yg_biofuel_use  = EIA_YG − uco_biofuel_use   [≥0 by construction]
combined ties exactly to EIA_YG
```
Ruling 1 made UCO **RLC-canonical, EIA disregarded** — and UCO ALONE (8.73B) now **exceeds the
entire EIA_YG bucket (7.39B)**. So `yg_biofuel_use = EIA_YG − uco_biofuel_use = 7.39 − 8.73 < 0`.
The §2 construction breaks. This is the direct UCO analog of the tallow §2 inversion you already
ruled — but the UCO doc was never updated for it.

**Why it blocks the allocator, concretely:** `feedstock_supply` currently carries the whole
waste-oil pool as **YG = 8.57B/yr, UCO = 0** (EIA has no UCO line). I cannot simply add canonical
UCO 8.73B — that double-counts the pool to ~17B. UCO and YG supply are coupled: the EIA YG bucket
must be *split* into RLC-UCO + RLC-YG, and the split IS this identity.

## Decisions requested

1. **YG biofuel-use under Ruling 1.** With UCO canonical at 8.73B > EIA_YG 7.39B, what is
   `yg_biofuel_use`? Candidates: (a) **0** — the EIA "YG" bucket is entirely UCO (+ misbooked
   tallow-swap volume), YG-grade goes to non-bio; (b) a small **residual** on its own RLC basis;
   (c) YG also **canonical** (collection-driven) and the combined UCO+YG is the canonical pool.
2. **Does any EIA_YG tie survive?** If UCO is exempt-canonical, is YG also rake-exempt (like
   tallow), or does YG stay EIA-pinned? This sets the rake `RLC_CANONICAL` membership (I currently
   have tallow-only; UCO/YG pending this).
3. **Budget closure.** Under Ruling 1 the combined RLC UCO+YG biofuel EXCEEDS EIA_YG (mirror image
   of tallow, which fell BELOW EIA) — consistent with the swap (EIA over-counts tallow / under-
   counts UCO). Confirm the combined animal-fat/waste-oil budget target this must reconcile to,
   so I know what the UCO+YG+tallow legs sum to when I split `feedstock_supply`.

## What Code proceeds with meanwhile (identity-independent)

- The vintage-splice pattern-audit on `k_uco`/`k_yg` (B.1-tasked) — same fit-vintage/apply-vintage
  check that caught the tallow splice.
- No `feedstock_supply` write until (1)-(3) are ruled. Allocator re-run stays held regardless.
