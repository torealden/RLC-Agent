# System graph — build v1 (Session 2)

**Status:** built and running. Design v1 steps 1–7 plus step 10 (checks) are done.
**Design:** `docs/specs/system_knowledge_graph_design_v1.md` — D1–D6, R1–R4.
**Session:** 2 of the system-graph workstream, `docs/SESSION_LEDGER.md`.

Everything below was measured against the live repo and live database on 2026-07-21/22 by
running the code in this repo. §7 lists what I did **not** verify.

---

## 1. The check that opened the session, and its answer

The design's §14 named the cheapest way for it to be wrong:

> Whether the 26 `.bas` files in git match the VBA actually embedded in the `.xlsm` workbooks.
> If they have diverged, the VBA extractor is reading fiction.

**They have diverged. The design's conclusion does not follow.** Both halves matter:

| | Result |
|---|---:|
| `.bas` modules tracked in git | 26 |
| Macro-enabled workbooks in the repo | 46 |
| Modules present in **both** git and a workbook | 8 |
| — byte-identical after normalising whitespace | **1** |
| — textually drifted | **7** |
| `.bas` in git, embedded in no workbook here | **18** |
| Standard modules embedded but untracked in git | **3** (`Module1`, `Module2`, `WASDECompUpdater`) |

So the identity check fails outright. But the facts an extractor would *pull* survived:
comparing the set of schema-qualified relation references and the set of procedure names
between each git file and each embedded copy, **all but three of 21 deployments were
fact-identical**, and those three differ by one private helper (`IsMonthHeaderCell`) in
archived workbooks. The drift is comments, headers and formatting — not SQL, not table names.

**Verdict: the VBA extractor is not reading fiction, but it was reading the wrong node.**
Three consequences, all now built in:

1. **`TradeUpdaterSQL` exists as six distinct code versions across eight workbooks.** One
   `.bas` name is not one module. The design's §5.1 natural key for `vba_module` — "`.bas`
   name" — collides six ways on that module alone.
2. **`WASDECompUpdater` is embedded in the live corn, wheat and soybean balance sheets and is
   not in git at all.** It reads `gold.fas_us_wasde_comp`. Untracked production VBA.
3. **The six `*WorkbookEvents.bas` files are not orphans** — they are deployed into
   `ThisWorkbook`, which the VBA project stores as a `.cls`, so a `.bas`-only comparison calls
   them missing. Verified: `EIAFeedstockWorkbookEvents.bas` is byte-for-byte the
   `ThisWorkbook.cls` of `eia_data.xlsm`. Its own header says "Paste into ThisWorkbook module."

### Model correction

`vba_module` is keyed `<workbook>#<ModuleName>` — the module that actually runs. The `.bas`
file in git is a separate `repo_file` node joined to it by a new edge type, `DEPLOYED_AS`,
carrying `identical: true|false`. A second new edge type, `DEFINES`, carries containment
(workbook → module → procedure, relation → column, sheet → block).

---

## 2. What was built

| Artifact | What it is |
|---|---|
| `database/migrations/146_sys_system_graph.sql` | the `sys` schema: `scan`, `node`, `edge`, `declaration`, `check_result`, `workbook_hash`, and the views |
| `database/migrations/147_sys_positional_key_scope.sql` | narrows the D3 no-positional-key constraint to the node types where it applies |
| `database/migrations/148_sys_no_inbound_excludes_phantoms.sql` | keeps phantom nodes off the R1 cleanup list |
| `src/sysgraph/store.py` | buffered node/edge writer; upsert + `last_seen_scan`, never delete |
| `src/sysgraph/catalog.py` | step 2 — relations, key columns, free `pg_depend` edges |
| `src/sysgraph/repo.py` | step 3 — tracked files, SQL scripts, registry/schedule join |
| `src/sysgraph/series.py` | step 4 — the series spine |
| `src/sysgraph/coderefs.py` | step 5 — Python AST / SQL / VBA → relation, resolve-or-flag |
| `src/sysgraph/workbooks.py` | step 6 — inventory, hashes, external links, embedded VBA |
| `src/sysgraph/blocks.py` | step 7 — sheet blocks and SUMIFS criteria mining |
| `src/sysgraph/trace.py` | the graph walk; `trace_series()` |
| `src/sysgraph/checks.py` | step 10 — the seven §8 assertions |
| `scripts/sysgraph_scan.py` | run a scan |
| `scripts/sysgraph_report.py` | read findings out of the graph |

### Graph size, one full scan

13,651 nodes · 15,241 edges.

```
worksheet   4232   DEFINES       8551
sheet_block 2787   READS         2451
data_series 2372   HAS_SERIES    2372
db_relation  927   WRITES         899
workbook     759   LINKS_TO       470
vba_proc     751   DERIVES_FROM   398
repo_file    664   SCHEDULED_AS    60
vba_module   445   DEPLOYED_AS     21
db_column    336   BINDS_TO        19
sql_script   300
sched_job     59
ff_series     19
```

### Cost — the design's §14 unmeasured number, now measured

**170 s for a complete cold scan** of the whole repo and the whole catalog.

| Step | Seconds | Note |
|---|---:|---|
| 2 catalog | 0.5 | 624 relations, 398 free edges |
| 3 repo inventory | 0.1 | 636 `.py`, 300 `.sql`, 26 `.bas` |
| 4 series | 55 | 242 candidate relations, `SELECT DISTINCT` per relation |
| 5 code refs | 1.3 | 962 files parsed |
| 6 workbooks | 39 | 627 workbooks hashed + zip-parsed |
| 7 blocks | 61 | 71 workbooks, 3.0M cells read, 2.1M formulas |

R3's hash gate is therefore not load-bearing for feasibility — a full scan is cheap enough to
run nightly as-is. It stays worth building because the *diff* is the useful output, not
because 170 s is expensive.

### Resolution rate by extraction method

| Method | Resolved | Edges |
|---|---:|---:|
| `pg_catalog` | 100.0% | 3,106 |
| `xlsx_formula` | 100.0% | 3,304 |
| `vba_parse` | 99.3% | 1,338 |
| `xlsx_extlink` | 92.3% | 4,204 |
| `regex` | 90.3% | 62 |
| `python_ast` | 89.0% | 1,015 |
| `sql_parse` | 70.8% | 2,212 |

`sql_parse` is the weak one and the reason is legible: `database/schemas/` and
`database/views/` contain DDL for relations that were renamed or dropped years ago. That is a
finding for the R1 cleanup pass, not a parser defect.

---

## 3. The three questions

### Q1 — blast radius of `silver.monthly_realized[attribute=oil_stocks]`

Answered, in under a second.

| Depth | Nodes |
|---|---:|
| 1 | the relation |
| ≤ 2 | 18 — 10 Python files, 4 SQL scripts, 2 gold views, 1 VBA procedure |
| ≤ 8 | 61 — adds 5 workbooks, 4 VBA modules, 22 relations |

Direct consumers at depth 2 include `scripts/write_oils_supply_flat_files.py` (the flat-file
writer that feeds the balance sheets), `src/services/forecast/auto_actuals.py`,
`src/knowledge_graph/pace_calculator.py` and `gold.abiove_soy_complex_monthly`. This was an
afternoon of grep, and the afternoon of grep is the step that actually gets skipped.

**One traversal rule makes this work and is worth knowing.** Containment edges (`HAS_SERIES`,
`DEFINES`) are stored container → member because that is how a person reads them, but they are
only ever *traversed* member → container. From `[attribute=oil_stocks]` you reach
`silver.monthly_realized` and therefore everything consuming it — while never reaching the 228
sibling series in the same table, which have nothing to do with the defect.

**A second rule keeps it honest.** ~4,600 of the extracted references are bare mentions — a
`gold.x` in a docstring or a log line, with no SQL verb in front. They are stored at
confidence 0.40 and `trace()` excludes anything below 0.50 by default. Including them turned
the oil_stocks blast radius from 61 nodes into 164, most of them unrelated tables reached
through a script that happens to touch twenty of them. They remain in the graph and remain
queryable at `min_confidence=0.0`; they just do not get to claim they are lineage.

### Q2 — which of two disagreeing sources is actually wired in

Answered, and it reproduces the design's hand count **exactly**, from a different direction.

```
576 cells  models/Biofuels/eia_data.xlsm -> US SOYBEAN OIL CO-PROCESSING USE
576 cells  models/Biofuels/eia_data.xlsm -> US SOYBEAN OIL SUSTAINABLE AVIATION FUEL USE
576 cells  models/Biofuels/eia_data.xlsm -> US SOYBEAN OIL RENEWABLE DIESEL USE
564 cells  models/Biofuels/eia_data.xlsm -> US SOYBEAN OIL BIODIESEL USE
                                            ------
                                             2,292
```

The design read the workbook by hand and reported "rows 115/131/147/163 … 2,292 cells." The
extractor, mining `[1]` external-link indices out of formulas and attributing them to blocks
by column-A title, gets the same 2,292 across the same four blocks — without ever knowing what
row anything sits on. `bronze.historical_feedstock_allocation` → `eia_data.xlsm` → the
production soybean-oil balance sheet is live, machine-visible, and now standing.

The same query shows what else feeds that sheet: `us_soy_crush.xlsm` into PRODUCTION (383
cells) and MONTH-ENDING STOCKS (517), `us_soy_complex_trade.xlsm` into IMPORTS and EXPORTS
(387 each).

### Q3 — is this code alive?

Half-answered, which is what the design promised at file granularity.

| Claim | Count |
|---|---:|
| `repo_file` with no inbound edge | 323 |
| `sql_script` with no inbound edge | 16 |
| `workbook` with no *code* reference | 525 of 627 |
| Registered collectors (`COLLECTOR_MAP`) | 52 |
| Jobs in `collection_schedule.json` | 7 |

Two corrections to the design's §1/§5.1 estimates fall out: `collector_registry` has **52**
entries, not the estimated 163; and the workbook estate is **627** macro/spreadsheet files
repo-wide, not 467 — the design counted only `models/`, `clients/` and `data/`, missing
`archive/Spreadsheets`, `domain_knowledge/spreadsheet_samples` and `output/`.

Per R4 the workbook number is deliberately *not* a death list. It is "no code references
this," and a human opens a workbook by double-clicking it.

---

## 4. Findings the build produced on its way past

**Broken external-link targets — 83 pointing at bare `models/Oilseeds/*`.** The design's §14
flagged this as a suspicion; it is now a measured count. Twenty workbooks store a link to
`models/Oilseeds/us_oilseed_crush.xlsm`, which does not exist — the live file is in
`models/Oilseeds/United States/`. Same shape for `us_oilseed_complex_trade.xlsm` (7),
`us_sunflower_balance_sheets.xlsx` (6), `us_minor_oilseed_trade.xlsm` (6),
`us_soy_complex_trade.xlsm` (6). **Not a conclusion:** this is what the *stored* target says.
Excel may repair a moved link when it opens the file. I have not opened one and checked.

A further 154 links point outside the repo entirely — Dropbox, a CONAB network share
(`\\GEAME04\…`), Eurostat temp folders. Those are expected, not defects.

**302 distinct relation names referenced in code that do not exist in the catalog**, across
1,572 references. Concentrated in `database/schemas/` and `database/views/`.

**`ShortcutsHelper` has drifted from git in all five workbooks that carry it — and all five
workbook copies are identical to each other.** So the divergence is one-sided: git was edited,
or the deployment was, and the other side never followed. 52 lines either way.

---

## 5. Corrections to the design, all now in code

| Design says | Correction | Where |
|---|---|---|
| §5.1 `vba_module` key is the `.bas` name | keyed `workbook#Module`; `TradeUpdaterSQL` has six forks | `workbooks.py` |
| §5.2 edge-type list | added `DEPLOYED_AS` (git source → running module) and `DEFINES` (containment) | migration 146 |
| §8.2 forbid positional identity globally | scoped to `sheet_block` / `flat_file_series` — the global rule rejected a worksheet legitimately named `EU27` | migration 147 |
| §1 467 workbooks | 627 repo-wide | `workbooks.py` |
| §5.1 ~163 registry entries | 52 | `repo.py` |
| §1 ~530 live relations | 624 — the survey omitted `public` (10) and `sandbox_reference` (51) | `catalog.py` |
| §5.2 `INVOKES` (vba_procedure → relation) | not built. It runs against the fixed direction convention; VBA SQL emits `READS`/`WRITES` like any other code | `workbooks.py` |

---

## 6. Checks now running every scan (§8)

All seven pass on scan 9. Three are binding and fail the scan.

| # | Check | Binding | Current |
|---|---|---|---|
| 1 | catalog reconciliation, zero drift both ways | yes | pass, 624 = 624 |
| 2 | no positional identity | yes | pass |
| 3 | free-lineage floor ≥ 373 | yes | pass, 398 |
| 4 | resolution rate per method, no >10pt regression | no | pass, baseline recorded |
| 5 | declaration survival | no | pass, 0 declarations so far |
| 6 | Q1 regression — oil_stocks trace non-empty | no | pass, 61 downstream |
| 7 | never-hide — lifecycle may label, never shrink | no | pass |

---

## 7. What I did **not** verify

- **Whether Excel actually fails on the 83 stale `models/Oilseeds/*` link targets.** I read
  what is stored in the file. Excel repairs some moved links at open time. Opening one
  workbook and looking at Data → Edit Links settles it, and I did not.
- **Whether the column-letter → contract-column map generalises.** Confirmed on
  `us_soybean_complex_bal_sheets.xlsm` and `us_tallow_complex_balance.xlsx` — `$A`=commodity,
  `$B`=class, `$C`=series, `$E`=period_type, `$F`=period. Those are the only two workbooks
  that produced flat-file bindings at all (19 series). If a third workbook uses a different
  layout, its mined keys will be wrong in a way that looks right.
- **Why only 19 `flat_file_series` came out of 125,610 mined criteria.** My reading is that
  only two workbooks use the flat-file SUMIFS pattern and the rest are ordinary SUMIFS over
  local ranges — but I have not confirmed that by sampling the other workbooks' criteria. If
  the pattern is present and being missed, Q1 is under-answered downstream of the flat files.
- **The `flat_file_series` → `data_series` join is not built.** Step 7 stops at the workbook
  tab. Connecting `ff_sbo_supply[commodity=soybean_oil,series=production]` to the database
  series that the writer emitted is Session 3 work and is partly declared (§7 of the design).
  Until then a blast-radius trace crosses from DB to workbook only through external links and
  code, not through the flat files.
- **Block detection is heuristic and was wrong once already.** The first pass produced 21,938
  "blocks" because column-A country names on trade sheets look like titles. Requiring the row
  to be otherwise empty cut it to 2,787. I checked `soyoil_balance_sheet`: 21 blocks, all 21
  are real column-A titles, but two of them (`US OILSEEDS COMPLEX`, `US SOYBEAN COMPLEX`) are
  page headers rather than data blocks and one title (`US SOYBEAN OIL MONTH-ENDING STOCKS`)
  also appears in a second workbook. So the heuristic is loose at the top of a sheet rather
  than wrong. I have not checked the other 404 sheets.
- **`vba_procedure` boundaries come from a regex on `Sub`/`Function` headers**, not a parser.
  Nested or line-continued declarations would mis-split.
- **The 18 tracked `.bas` modules embedded in no workbook here.** Twelve are real standard
  modules (`RINUpdaterSQL`, `CornGrindUpdater`, `EthanolUpdater`, …) bound to keyboard
  shortcuts Tore uses. Absence from a repo workbook is not death — they may be deployed in a
  workbook outside the repo, or pasted straight into the VBE. Unresolved.
- **`silver.monthly_realized` was enumerated to 229 series and the cap is 400.** One relation
  hit the cap and was recorded as `skipped_too_many` rather than enumerated; 109 more were
  skipped as provenance-only. Those are marked on the relation node, but a series that exists
  and was never enumerated is invisible to a trace.
- **Nothing has been scheduled.** R3's nightly cheap scan is not wired to the dispatcher.
