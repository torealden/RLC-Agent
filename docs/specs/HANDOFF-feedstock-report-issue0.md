# Handoff Spec — The Feedstock Report, Issue 0 (Maiden Run)

**From:** Claude Desktop (design) · **To:** Claude Code (implementation)
**Date:** 2026-08-06 · **Hard deadlines:** render pipeline green by **Fri Aug 7 EOD**; Issue 0 populated and rendered by **Sat Aug 8 09:00 ET**; Issue 1 publishes publicly **Tue Aug 11** (date is publicly committed — see Fallback).

## Scope

Implement ETL population and rendering for the `reports` schema (currently zero rows). Tables in scope: `feedstock_issue`, `section_content`, `news_items`, `credit_stack_snapshot`, `price_dashboard_snapshot`. Reconcile everything below against actual DDL; where a required field is missing, propose an ALTER in the PR rather than working around it.

## Task 0 — Freshness audit (do this first, report before building)

For every silver/bronze series that would feed `price_dashboard_snapshot` and `credit_stack_snapshot`, report: series id, source collector, last observed date, expected cadence. Known-dead going in: `usda_ams_feedstocks` UCO (dark since Aug 2025), BD/RD econ series (dark), poultry fat and palm (separate break, April 2025). **Unknown and must be verified: DCO and bleachable tallow.** **Source-side status verified by Desktop 2026-08-06: AMS is alive and publishing** (Tallow & Protein Report AMS_2839 current through late June 2026; National Animal By-Product Feedstuff Report AMS_3510 current through July 2026, incl. yellow grease, CWG, tallow) — so the breaks are collector-side. Likely cause is AMS report churn (Feb 2022 three-report merger; Sep 2022 PDF migration; Jun 2025 technical/edible tallow line-item split). **Rebuild against the MARS/My Market News API using report IDs (2839, 3510), not PDF scraping.** Backfill the full gap from API history. Treat item-absent-this-week as a normal state (voluntary reporting, thin loads), not a collector error. Do NOT map AMS yellow grease to the UCO code — YG and UCO are distinct codes in the canonical vocabulary; a UCO row requires CME UCO futures (Task 1) or stays on the coverage-gap line. Output a freshness table (db-side + source-side status) to Desktop before wiring the dashboard.

## Task 1 — Exchange series acquisition (Barchart) — DEMOTED, non-blocking

With AMS confirmed alive (Task 0), collector restoration is the primary dashboard path for animal fats/greases; AMS cash prints are the preferred basis over thin futures. Task 1 shrinks to: **Bursa Malaysia FCPO (palm)** — the one series with no AMS path — plus **CME Used Cooking Oil futures** as an optional later add for a true UCO row (verify liquidity; cash-settled, thin contract). Build as a Barchart API collector into the bronze/silver flow; do not hand-key. Not an Issue 0 or Issue 1 dependency. Poultry fat: expected in AMS_3510 — confirm in Task 0 audit; no futures contract exists as a fallback. Exchange settles publish with attribution (standard practice; confirm-later for counsel).

## Section registry

Render order and section codes (store code in `section_content.section_code` or equivalent):

| order | code | title | source |
|---|---|---|---|
| 1 | `masthead` | (issue header) | `feedstock_issue` |
| 2 | `signal` | The Signal | written (section_content) |
| 3 | `credit_stack` | Credit Stack Monitor | `credit_stack_snapshot` + written annotation |
| 4 | `dashboard` | Feedstock Price Dashboard | `price_dashboard_snapshot` |
| 5 | `ifv_leaderboard` | IFV Leaderboard | computed + written caption |
| 6 | `in_focus` | In Focus | written |
| 7 | `news` | News & Policy Watch | `news_items` + written annotations |
| 8 | `week_ahead` | The Week Ahead | written |
| 9 | `footer` | Methodology & Disclosures | static template + dynamic staleness notes |

## Row grain and required fields

- `feedstock_issue`: one row per issue. Required: issue_no (0, 1, …), issue_date, coverage window (**ends Monday settlement close** — ruled 2026-08-06; snapshots run Monday evening, publish Tuesday), status (`draft`/`locked`/`published`), free_mode boolean.
- `price_dashboard_snapshot`: one row per (issue, feedstock, series). Required: feedstock code from the 11-code canonical vocabulary (SBO, CAN, DCO, BFT, CWG, YG, PLT, UCO, CAM, CAR, OTH), price level, w/w change, unit, source, **last_observed date**, staleness flag.
- `credit_stack_snapshot`: one row per (issue, credit instrument). Same staleness fields.
- `news_items`: one row per item: headline, url, source, one-line annotation, rank.
- `section_content`: (issue, section_code, markdown body, author, updated_at).

## Staleness rules (renderer-enforced, non-negotiable)

- Every numeric row carries `last_observed`. **Carry-forward rule (ruled 2026-08-06):** rows with no print in the coverage week render the last actual print *with its as-of date visible* — never as a current-week price. W/w change computes only between actual prints; render "—" when the comparison would span a carried value. Rows older than **21 days** are excluded and moved to the one-line "coverage expanding" note. AMS-style thin/intermittent prints are the expected case, not an error.
- No silent staleness anywhere. Issue 0 must include one intentionally stale test row to verify both render paths.

## IFVS-008 compliance gates (renderer-enforced)

When `free_mode = true`:
1. IFV Leaderboard renders **rank + direction arrows only** — any numeric IFV value in that section is a render-time hard error, not a warning.
2. Citation whitelist check: every source string in rendered output must be in {CARB, NREL, EIA, IEA, Argus, OPIS, USDA} ∪ {exchange names for board data} (USDA added by ruling, 2026-08-06). Keep the whitelist as config, not hardcode.
3. Grep-level gate: the strings "HOBO" and any internal series identifiers must not appear in any rendered output. Hard error.
4. Argus/OPIS-sourced series render as w/w change or base-100 index, never levels, until the license ruling clears (config flag `licensed_levels_ok`, default false).

## Rendering — two outputs per issue

1. **`issue_{n}.html`** — single file, inline CSS. Brand: INK `#1B2A4A`, GOLD `#C8A951`, PAPER `#F7F3EB`; Georgia headers, Calibri body (with serif/sans-serif fallbacks). Max content width ~720px. Tables: INK header row with PAPER text, GOLD rules, zebra-free.
2. **LinkedIn kit** — directory per issue containing: (a) plain-text/markdown body with `[IMAGE: name]` placeholders, (b) PNG exports of every table and chart at 1400px wide (2x for 700px display), brand-styled. LinkedIn strips CSS/fonts; brand survives only in images.

Charts: matplotlib or similar is fine; PAPER background, INK text/axes, GOLD as the single accent series color. One chart max in `in_focus`, one for the teaser.

## CLI verbs (suggested shape — adjust to repo conventions)

- `report issue create --n 0 --date 2026-08-08 --free`
- `report snapshot prices --issue 0` / `report snapshot credits --issue 0`
- `report news add --issue 0 --url … --headline … --note …`
- `report section set --issue 0 --code signal --file signal.md`
- `report snapshot manual --issue 0 --file manual_prices.csv` — supervised manual-entry path for interim series (e.g. CME UCO settles hand-copied until Barchart lands). Loads into `price_dashboard_snapshot` through the same validation, staleness, and render gates as collector data. **Manual numbers never enter rendered output directly — database only.** CSV requires: feedstock code, price, unit, source, observation date.
- **UCO sourcing rule (ruled 2026-08-06): CME Group settlement values only, attributed to CME Group. Fastmarkets/Jacobsen assessments must never appear in any output, attributed or not** — not on the whitelist; treat any "Fastmarkets" string in rendered output as a hard error alongside the HOBO gate.
- `report render --issue 0` (emits both outputs, runs all gates)
- `report lock --issue 0`

## Acceptance criteria for Issue 0

1. Freshness audit delivered and dashboard membership agreed with Desktop.
2. All five tables populated for issue 0; render produces both outputs with zero gate errors.
3. Stale test rows render correctly in both paths: carry-forward (last print + as-of date + "—" change) and 21-day exclusion.
4. `free_mode` gates verified by attempting to render a numeric IFV value and confirming hard failure.
5. PNGs legible at LinkedIn display width on a phone.

## Fallback (deadline is public)

If the pipeline is not green by Fri EOD: stop, and Desktop assembles Issue 0 manually in the HTML template while Code continues on the pipeline targeting Issue 1. If the pipeline is not green by **Sun Aug 9 EOD**, Issue 1 is produced manually and the pipeline targets the Aug 18 issue. The Tuesday publish never slips; the automation does.

## Rulings log (2026-08-06, Desktop session)

1. USDA added to the public citation whitelist.
2. Argus/OPIS-derived series render as w/w change or base-100 index in free content — no levels — pending license review. Exchange settles render as levels with attribution.
3. Issue 1 In Focus = IFV framework explainer with explicitly illustrative, non-current numbers.
4. Dead cash series: fix collector if AMS still publishes; otherwise replace via exchange contracts (Task 1: CME UCO, Bursa FCPO); poultry fat rides the coverage-gap line if AMS is source-dead.
5. Coverage window ends Monday settlement close; automated snapshots Monday evening; Tuesday publish. Written annotations get a mandatory Monday-evening review pass against refreshed tables (characterize, never restate levels).

## Out of scope

Beehiiv/email delivery, site paywall, paid-issue access control, Kestra. Windows Scheduled Tasks under `\RLC\` remain the orchestration stack — add the weekly snapshot task there only after Issue 1 ships.
