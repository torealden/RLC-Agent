# Felipe Sandbox — setup & handoff

Stood up 2026-07-06 by Claude Code, per curriculum §2a / user-guide §8. This is the "build without
risk, without rebuild" environment. **Principle: promote code, not data.**

## What exists now
- **Git branch `felipe`** — his private copy of the code, invisible to `main` until reviewed & merged.
- **Sandbox schemas** (same DB `rlc_commodities`): `sandbox_bronze`, `sandbox_silver`, `sandbox_gold`,
  `sandbox_reference`. His new pipeline writes here.
- **Restricted role `felipe`** — LOGIN; **WRITE on `sandbox_*` only, READ-ONLY on production**
  (bronze/silver/gold/reference/core). Verified: writing to production bronze/reference is DENIED at
  the DB level. A learner + an LLM cannot corrupt production.
- **`sandbox_reference`** — a full copy of the 51 `reference.*` tables, so Felipe can add his country's
  config (commodities, codes, etc.) without touching production reference.

## The zero-retype mechanism (`src/services/database/schema_prefix.py`)
New code targets a layer with `sch('bronze')`, not hardcoded `'bronze'`:
- Production: `RLC_SANDBOX_PREFIX` unset → `sch('bronze') == 'bronze'`.
- Felipe: `RLC_SANDBOX_PREFIX=sandbox_` → `sch('bronze') == 'sandbox_bronze'`.
The **same code** runs in sandbox and production — the env var is the only difference. On promotion,
nothing is retyped. (Existing production code is untouched; this is opt-in for Felipe's new work.)

## Felipe's `.env` additions (Code sets up; TORE delivers credentials)
```
RLC_SANDBOX_PREFIX=sandbox_
RLC_PG_USER=felipe
RLC_PG_PASSWORD=<Tore delivers via Tailscale; rotate the temp password Code generated>
```
His collectors write to `sandbox_bronze`; his silver/gold builders read `sandbox_bronze` + can read
production for templates; his exporter writes flat files to `models/` on his branch.

## Promotion path (§2a ceremony, done with Tore)
1. Felipe commits + pushes his `felipe` branch.
2. Reviewed as a **pull request** (side-by-side diff) — Claude Code assists Tore.
3. **Merge** into `main`. Nothing retyped.
4. **Run** the merged migrations/collectors/exporter against production (prefix unset → real schemas).
5. **Re-verify** with the §9 battery against production output. Only then is it live.

## Credentials handoff (TORE)
Code generated a temporary password for `felipe` and reported it in chat (never committed). **Rotate
it and deliver to Felipe over Tailscale:** `ALTER ROLE felipe LOGIN PASSWORD '<new>';`. Nothing about
the password lives in git.

## Reset / rebuild
Re-run `FELIPE_PW=... python scripts/setup_felipe_sandbox.py` — idempotent (re-copies reference,
re-applies grants). To wipe his work between exercises: `DROP SCHEMA sandbox_bronze CASCADE; ...`
then re-run.
