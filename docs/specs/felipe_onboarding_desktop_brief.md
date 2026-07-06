# Felipe Onboarding — Code → Desktop initiating brief

**From:** Claude Code | **To:** Claude Desktop | **Owner:** Tore
**Date:** 2026-07-06 | **Deadline:** Felipe is on-site through **2026-07-14**, then remote.
**Ask:** Desktop produces three artifacts (§A–C below). Code supplies the accurate technical
file/capability index (§C) since it has the live codebase; Desktop owns the pedagogy and prose.

## 0. The goal (Tore's words, distilled)

Felipe leaves July 14 with the **smaller-GPU laptop** and a working ability to **use LLMs (Claude
Code / Desktop) to build and maintain the balance-sheet models** — the US sheets we've built, and
*new* sheets for the remaining country/market combinations RLC covers. Method: **US sheets are the
template; add/subtract tabs per country as the build reveals the need** — we do NOT pre-guess a
country's tabs. Each new model is a full loop: wire the source APIs → design/implement DB schema →
load data (new or existing VBA updaters) → **verify against a source document or our spreadsheets**.
The work is tracked from a punch-list file (`model_files_punchlist.xlsx` or similar).

## A. In-person curriculum (the ≤8 working days, hands-on)

Desktop drafts a day-by-day (or module) outline for what must be covered **while Tore and Felipe are
physically together** — reserving in-person time for what's hard to convey remotely. Cover at least:

1. **Orientation:** the medallion architecture (bronze/silver/gold), the dual-Claude workflow (Code =
   plumbing/DB/APIs, Desktop = models/formulas/prose, Notion = coordination), and where Felipe fits.
2. **The LLM-as-builder meta-skill** — *the core deliverable*. How to prompt Claude Code/Desktop to:
   scope a new country model, wire a collector, propose a schema, write a VBA updater, and — critically
   — **how to verify and push back on LLM output** (never trust unverified; the honest-eval rule).
3. **The template-to-country method:** open a US sheet, walk the flat-file contract (LONG/vintage
   ladder/MAXIFS), the `_meta` tab, and how tabs get added/dropped per country.
4. **The verification discipline:** checksums, MAXIFS-invariant, reconciling to a source document —
   the acceptance mindset we've used all through the feedstock build.
5. **Hands-on capstone (recommend this be the spine of the week):** build **one real country model
   end-to-end together** (candidate: the next wheat country, Brazil, or a Canada BBD sheet) — API →
   schema → load → verify. Learning by doing with Tore present beats any document.
6. **Guardrails & conventions:** git commit/push discipline, no destructive migrations, versioning
   before overwrite, the Fastmarkets "keep-don't-show" rule, source-vs-display units, gitignored dirs.

## B. User's guide (the take-home document)

A standalone guide Felipe uses **after** he leaves. Desktop owns structure/prose; Code supplies facts.
Cover: system overview & component parts (collectors, DB, VBA updaters, flat files, LLM tools); the
country/market scope RLC covers and which are built vs to-build; the daily/weekly workflow; how to
run each piece; the LLM-assisted build loop as a repeatable recipe; troubleshooting; **what Felipe
can run locally on the smaller GPU vs what needs cloud** (small local models for high-volume
deterministic tasks, cloud for reasoning/client-facing — reference the local-vs-cloud framework);
and the access/credentials checklist (API keys, DB, Dropbox delivery path — note **Felipe has no
Tailscale**, so anything he needs must reach him another way).

## C. File & capability index (Code will supply the verified version)

A reference index of significant file locations + what each does: the model/flat-file locations
(`models/`), scripts/collectors, DB schemas & key tables/views, VBA updaters + their Ctrl-shortcuts,
the domain-knowledge tree, the specs/contracts, and the KG/callable tooling. **Code owns accuracy
here** — Desktop should leave a slot for it and Code will generate the real index from the codebase
so no path is guessed.

## D. Open questions for Tore (calibrate the artifacts)

1. **Felipe's baseline** — his current level in Python / SQL / Excel-VBA / LLM-prompting sets the
   curriculum depth. Desktop should ask, or assume and flag.
2. **His scope/responsibilities** — is he owning specific countries, maintaining existing sheets
   (he already gets the weekly cash prices), or both? Changes what the capstone builds.
3. **`model_files_punchlist.xlsx`** — who authors it and its columns (country / market / commodity /
   source APIs / schema status / VBA / verification source / status)? Recommend Desktop specs it and
   Code seeds it from the covered-market list.

## E. Labor division

- **Desktop:** §A curriculum, §B user's guide (structure + prose), §C index structure + the
  `punchlist` schema.
- **Code:** §C verified file/capability index from the live codebase; seed the punch-list; any
  collector/schema scaffolding the capstone country needs.
