# Permit Extraction — Spot-Check Queue

**Created 2026-06-20.** Purpose: verify LLM extraction accuracy on one
representative facility per industry, per the "one facility per industry =
customizable tool" goal. Template/structure is trusted; this queue is for the
*data-accuracy* pass that was deliberately deferred until publishing.

## How to spot-check one facility

1. Open the facility's `extraction_summary.md` — sanity-check operator, location,
   permit number, industry tag, unit count.
2. Open `source.pdf`, find the Equipment List / Emission Units table (IDNR format
   is usually pp. 4–6).
3. Cross-reference against `equipment_list.csv` — are the real emission units
   present? Any hallucinated/duplicated rows? Capacities transcribed correctly?
4. For oilseed_crush only: `process_flow_coverage.md` shows which canonical crush
   steps have equipment vs which are ⚠️ missing (could be LLM miss, permit doesn't
   itemize, or facility lacks the step).
5. Note discrepancies here under each facility.

> Extraction model: qwen2.5:7b, best-of-N union (drain) or chunked single-run
> (the 11 large permits — counts noisier, N=1). Large permits may **over-enumerate**
> (ADM Clinton = 526 "units"); treat high counts skeptically.

## Queue (representative per industry)

| Industry | Facility | State | Units | Archive folder |
|---|---|---|---|---|
| oilseed_crush | ADM – Des Moines Soybean | IA | 31 | `permits/oilseed_crush/ia/adm_des_moines/` |
| biodiesel | Ag Processing Inc. – Algona | IA | 12 | `permits/biodiesel/ia/agp_algona/` |
| renewable_diesel | Metro Methane Recovery ⚠️ | IA | 29 | `permits/renewable_diesel/ia/metro_methane_recovery_facility_mitchellville_294_47_kb_archived_pdf/` |
| ethanol | Green Plains Superior | IA | 58 | `permits/ethanol/ia/green_plains_superior_llc_superior_761_69_kb_archived_pdf/` |
| oil_refining | CHS – Council Bluffs | IA | 44 | `permits/oil_refining/ia/chs_mcpherson_refining_inc_council_bluffs_241_75_kb_archived_pdf/` |
| other (corn wet mill) | Vermeer Corporation | IA | 58 | `permits/other/ia/vermeer_manufacturing_co_pella_1_22_mb_archived_pdf/` |

⚠️ **Metro Methane tagged `renewable_diesel`** — almost certainly a mis-categorization
(landfill methane recovery, not RD). The industry-tag field is free-text from the LLM
and is noisy (gypsum has 3 spellings, etc.). Industry taxonomy cleanup is a separate
deferred task — re-derivable by tightening `industry_for()` in
`scripts/publish_permits_to_organized_archive.py` and re-running (idempotent).

## Known deferred data-quality items (not blocking, fix before public use)
- **Unit over-enumeration** on large chunked permits (ADM Clinton 526, Gable 393).
  Best-of-N was N=1 for the chunked retry; dedup/precision is lower.
- **Industry free-text variance** → folder sprawl (gypsum×3, electric×3). Normalize
  `industry_for()` and re-publish.
- **Facility-slug ugliness** — new permits use the filename stem
  (`archer_daniels_midland_clinton_3_48_mb_archived_pdf`) because the LLM didn't emit
  a clean `facility_id`. Cosmetic; clean if it bothers downstream joins.

## Findings log
_(record spot-check results here as they're done)_
