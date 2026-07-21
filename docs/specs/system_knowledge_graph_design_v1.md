# System Knowledge Graph — design v1

**Status:** design, not built. Session 1 of the 2026-07-21 handoff plan. No code written.
**Scope:** lineage across three substrates — Python/VBA/SQL **code**, the PostgreSQL **database**,
and the Excel **spreadsheet** estate — plus the deliverables that sit on top.
**Not in scope:** the analyst knowledge graph (`core.kg_node/edge/context/callable`). §6 defines the
one seam between them.

Everything in §1 was measured against the live repo and live database on 2026-07-21. §14 lists what
I did **not** verify.

---

## 1. Ground truth, measured today

| Substrate | Measured | Source of the number |
|---|---:|---|
| Python files | 636 | `git ls-files '*.py'` |
| — of those, in `scripts/` | 248 | same |
| — of those, referencing a schema-qualified relation | 318 | grep `bronze.|silver.|gold.` |
| — of those, writing Excel | 103 | grep `openpyxl|xlsxwriter|to_excel` |
| VBA modules (`.bas` in git) | 26 | `git ls-files '*.bas'` |
| SQL files | 300 | `git ls-files '*.sql'` |
| Workbooks under `models/`, `clients/`, `data/` | 467 | `find`, excluding `~$` locks |
| — with archive/backup/old/conflicted in the name | 96 | same |
| Live relations, `bronze` | 107 tables | `information_schema.tables` |
| Live relations, `silver` | 75 tables + 48 views | same |
| Live relations, `gold` | 7 tables + 188 views | same |
| Live relations, `core` | 21 tables + 11 views | same |
| Other schemas | `reference` 57, `reports` 11, `audit` 10, `sales` 5, `risk` 5, `meta` 1, `config` 1 | same |
| View→source dependency edges available free | **373** | `pg_depend`/`pg_rewrite` |
| Distinct schema-qualified refs found by naive regex | 1,188 | grep over `.py`/`.sql`/`.bas` |
| — that resolve to a live relation | **539** | joined to catalog |
| — that do **not** (functions, filenames, drift) | **649** | same |
| Live relations never referenced anywhere in the repo | **8 of 530** | same |
| Distinct series in `silver.monthly_realized` alone | **229** | `SELECT DISTINCT commodity, attribute, source` |
| Formula cells in `us_soybean_complex_bal_sheets.xlsm` | 167,755 | openpyxl scan |
| Formula cells in the top oilseed workbook | 698,276 | `world_soybean_trade.xlsx` |

Two of these drive the whole design and are worth stating plainly:

**CLAUDE.md is stale on counts** — it says 89/93/180 bronze/silver/gold; live is 107/123/195. Not a
problem to fix here, but it is the exact class of drift this graph exists to make visible.

**Only 8 of 530 live relations are unreferenced.** The repo is a near-complete cover of the catalog.
So the hard problem is *not* finding references. It is **classifying** them: read vs write, live
script vs dead script, canonical artifact vs superseded copy. A graph that just records "file X
mentions table Y" adds nothing over grep.

---

## 2. What this must answer

Three questions, each drawn from a real open item in the 2026-07-21 handoff. If the design cannot
answer these three, it is not worth building.

**Q1 — Blast radius of a defective series.**
> `silver.monthly_realized` attribute `oil_stocks` is the once-refined series alone, ~1/6 of total.
> *Everything else reading it understates stocks ~6×.* — handoff §4

Answer required: the full downstream set — gold views, Python scripts, VBA modules, flat files,
workbook blocks, client deliverables. Today this is a grep and a guess.

**Q2 — Which of two disagreeing sources is actually wired in.**
> `bronze.historical_feedstock_allocation` still feeds `eia_data.xlsm` and disagrees with
> `gold.bbd_feedstock_raked` (+647 mil lb on CY2024 SBO). — handoff §4

Verified today, and this is the strongest evidence the design works: `us_soybean_complex_bal_sheets.xlsm`
carries external link 1 → `../../Biofuels/eia_data.xlsm`, and `soyoil_balance_sheet` rows 115/131/147/163
read `[1]biodiesel_monthly`, `[1]renewable_diesel_monthly`, `[1]sustainable_aviation_monthly`,
`[1]co_processing_monthly` — 2,292 cells. The stale chain is not hypothetical; it is live in the
production balance sheet and it is machine-visible.

**Q3 — Is this code alive?**
248 files in `scripts/`. Which are wired to a scheduled job, which are one-shot migrations that
already ran, which are abandoned? The recorded failure mode (`feedback_orphan_code_hunt`) is
building a collector that already exists somewhere unregistered.

A fourth, cheaper win falls out for free: **canonical vs stale artifact**. Extraction found workbook
external links pointing at bare `models/Oilseeds/*.xlsm` paths while the active flat files were
relocated to `models/Oilseeds/United States/`. Flagged, not concluded — see §14.

---

## 3. Prior art: `audit.lineage_edge` already exists, and it failed

This repo already has a lineage model, from `005_transformation_logging.sql`:

| Table | Rows today |
|---|---:|
| `audit.lineage_edge` | 29 |
| `audit.transformation_operation` | 55 |
| `audit.transformation_session` | 55 |
| `audit.ingest_run` | 3 |
| `audit.output_artifact` | **0** |
| `audit.validation_status` | **0** |

Its shape is fine — `source_type/schema/name/column → target_*`, `relationship_type`, `is_current`.
It failed for one reason: **it is runtime instrumentation.** Every script must voluntarily call
`audit.add_lineage_edge()`. Across 636 Python files written over a year, adoption was ~0. 29 edges
against 373 that `pg_depend` gives away for nothing.

**Ruling: v1 is static extraction, not instrumentation.** Scan the repo, scan the catalog, scan the
workbooks; rebuild the graph from scratch on demand. Consequences, stated honestly:

- *Gain:* idempotent, complete on day one, covers dead code (a feature — you want to know what an
  abandoned script *claims* to write), no cooperation required from 636 files.
- *Lose:* no row counts, no "when did this last actually run", no runtime branch resolution.
- *Mitigation:* `core.collection_status` and `core.event_log` already carry run-time facts. Join to
  them; do not re-collect them. `audit.*` stays where it is — v1 neither extends nor deletes it.

---

## 4. Design decisions

### D1 — Separate schema `sys`, not more rows in `core.kg_*`

The system graph does **not** go in `core.kg_node`/`kg_edge`.

| | analyst KG (`core.kg_*`) | system graph (`sys.*`) |
|---|---|---|
| Size | 436 nodes | est. 15k–25k nodes |
| Origin | hand-curated from reports | machine-extracted |
| Lifecycle | append, rarely delete | full rebuild every scan |
| `confidence` means | how sure is the analyst | how sure is the *parser* |
| Loss if wrong | a year of curation | re-run the scan |

Putting a rebuildable 20k-node graph in the same table as 436 irreplaceable curated nodes means one
buggy rebuild script can delete the curation. That alone settles it. Secondary: `search_knowledge_graph`
would return `scripts/write_oils_supply_flat_files.py` when an analyst asks about soybean oil.

The obvious counter — *one graph means one query surface* — is real and is handled at the query
layer, not the storage layer: a union view plus a single MCP tool (§6). Storage separate, access unified.

### D2 — The spine is the **series**, not the table and not the column

This is the decision that makes the graph useful rather than merely tidy, and it is where an
off-the-shelf lineage tool (dbt, OpenLineage, sqlglot column lineage) would get it wrong.

The `oil_stocks` defect in Q1 is not a table and not a column. It is
`silver.monthly_realized WHERE attribute='oil_stocks'` — a **value in a key column**. Column-level
lineage says "everything reads `realized_value`", which is true and useless.

The tall/narrow shape is pervasive, not incidental: 87 relations carry a `commodity` column, 10 carry
`attribute`, 14 `category`, 8 `series_id`, 5 `series`, 4 `metric`. `silver.monthly_realized` alone
holds **229 distinct series**.

So the atom is:

> **`sys.data_series`** = a relation plus a resolvable key selector.
> `silver.monthly_realized[commodity=soybeans, attribute=oil_stocks, source=NASS_FATS_OILS]`

This grain is not invented for the graph. It is already the shared vocabulary of the whole system:

- `docs/specs/flat_file_contract.md` §2 defines exactly these key columns —
  `commodity, class, series, marketing_year, period_type, period, vintage, vintage_rank`.
- The analyst KG's 81 `data_series` nodes speak the same language (`eia.ethanol`, `census.fat_grease_trade`).
- **The balance-sheet formulas literally contain the keys as string literals.** Verified —
  `soyoil_balance_sheet!F69`:
  ```
  =IF(COUNTIFS(ff_sbo_supply!$A:$A,"soybean_oil", $B:$B,"ALL", $C:$C,"imports",
               $D:$D,LEFT(F$3,4)*1, $E:$E,"cal_month", $F:$F,"M10")=0,
      '[3]Soybean Oil Imports'!AR$217/1000,
      SUMIFS(... $H:$H, MAXIFS($H:$H, ...same keys...))/1000000)
  ```
  `"soybean_oil"` / `"ALL"` / `"imports"` / `"cal_month"` / `"M10"` are commodity / class / series /
  period_type / period. The `MAXIFS`-on-`vintage_rank` is contract §4 verbatim.

Because the series key is a *string literal in the formula*, the spreadsheet→series edge is
**auto-extractable at high confidence**, not something a human must declare. That was the open risk
in this design and it is now closed.

Series grain is also what makes the graph queryable in the language Tore already uses. "What reads
soybean oil stocks" is a question about a series. It is not a question about a table.

### D3 — Node identity is a stable natural key; never a position

Excel positions rot. `us_soybean_complex_bal_sheets.xlsm` holds 167,755 formula cells; the oilseed
folder alone exceeds 2.5 million. Cell nodes are off the table on both rot and cardinality grounds.

The stable spreadsheet anchors, verified in the live workbook:

- **Block title in column A** — `US SOYBEAN OIL PRODUCTION` (r35), `... IMPORTS` (r67),
  `... EXPORTS` (r83), `... BIODIESEL USE` (r115), `... RENEWABLE DIESEL USE` (r131), 16-row pitch.
  `SoyOilRepointToFlatFile.bas` already binds by exactly these title strings.
- **Flat-file series key** — published in the writer-emitted `_wide_index` tab.

So `sys` stores `(workbook, sheet, block_title)` and `(flat_file, tab, series_key)`. It **never**
stores a cell address as identity. Resolving a block to a row range is a runtime lookup against
`_wide_index` and a title scan — which is what the repoint macro already does, and why its header
says *"read it; never count rows."* The graph adopts the same rule.

Cell addresses may be stored as **observations** on an edge (`evidence: {cells: 72000, sample: "F69"}`)
— useful for sizing blast radius, never used to join.

### D4 — Every extracted fact carries its method, and must resolve or be flagged

1,188 naive refs, 539 resolve, 649 do not. Building without validation yields a graph that is half
fiction — precisely the failure the CLAUDE.md verify rule exists to prevent.

Every node and edge carries:

| Field | Values |
|---|---|
| `extraction_method` | `pg_catalog` · `sql_parse` · `python_ast` · `vba_parse` · `xlsx_formula` · `xlsx_extlink` · `regex` · `declared` |
| `confidence` | 1.00 catalog · 0.90 AST/parsed · 0.70 formula-mined · 0.40 regex · declared inherits its author |
| `resolution_status` | `resolved` · `unresolved` · `ambiguous` |
| `evidence` | JSONB — file, line, formula excerpt, cell sample |
| `scan_id` | which scan produced it |

**Binding rule:** an edge whose endpoint does not resolve against the live catalog or filesystem is
**still stored**, marked `unresolved`, and **excluded from every default query.** It is not silently
dropped — 649 unresolved refs are themselves a finding (drift, renames, dead code). They surface in a
report, not in an answer.

### D5 — File-level Python granularity in v1, not function-level

Q1–Q3 are all answered at file granularity. Function-level symbol extraction across 636 files roughly
triples parse cost, produces the fastest-rotting nodes in the graph, and answers no question currently
being asked. `CALLS` edges between Python symbols are explicitly **deferred to v2**.

Exception: **VBA procedures are nodes.** There are only 26 modules, the public `Sub`s *are* the user
interface (Ctrl+U → `FatsOilsUpdaterSQL`), and a module maps to several unrelated targets.

### D6 — Full breadth at coarse grain; depth only on the oils/fats chain

Coarse extraction is cheap and should cover everything on day one: catalog, `pg_depend`, file
inventory, regex+AST relation refs, workbook inventory, external links. That is the whole repo.

Expensive extraction — series-grain binding, workbook block parsing, formula criteria mining — is
scoped in v1 to the **vegetable-oil / fats-and-greases / BBD feedstock chain**: `models/Oilseeds/`,
`models/Fats and Greases/`, `models/Biofuels/`, their writers, and their gold views. That is where
Q1 and Q2 live, where the flat-file contract is actually implemented, and where the format is proven.

Breadth without depth still answers Q3 and still finds stale artifacts. Depth without breadth answers
nothing outside one chain. Do both, at different grains.

---

## 5. The model

### 5.1 Node types

| `node_type` | Natural key | Extraction | Est. count |
|---|---|---|---:|
| `db_relation` | `schema.name` | `pg_catalog` | ~530 |
| `db_column` | `schema.name.column` | `pg_catalog` | ~6k *(v1: only key columns of series-bearing relations)* |
| `data_series` | `schema.name[k1=v1,k2=v2]` | `SELECT DISTINCT` on key cols + literal mining | ~1–2k in-scope |
| `repo_file` | repo-relative path | `git ls-files` | ~1,000 |
| `vba_module` | `.bas` name | file scan | 26 |
| `vba_procedure` | `module.ProcName` | `vba_parse` | ~200 |
| `sql_script` | repo-relative path | file scan | 300 |
| `scheduled_job` | `schedule_key` | `collection_schedule.json` + `collector_registry.py` | ~163 registry entries |
| `workbook` | absolute path, normalised | file scan | 467 |
| `worksheet` | `workbook#sheet` | `xlsx` scan | ~3k |
| `sheet_block` | `workbook#sheet#BLOCK TITLE` | `xlsx_formula` | ~150 in-scope |
| `flat_file_series` | `workbook#tab#series_key` | `_wide_index` + LONG distinct | ~300 in-scope |
| `deliverable` | stable slug (`helios_pepsi_weekly`) | **declared** | ~15 |
| `external_source` | `EIA` · `NASS` · `CENSUS` · `HELIOS` … | declared | ~30 |

`workbook` carries `lifecycle ∈ {canonical, superseded, archive, backup, unknown}` — heuristic from
path and filename, **overridable by declaration**. 96 of 467 are archival by name alone; the rest
need the graph (nothing links to them) plus a human ruling.

### 5.2 Edge types

| `edge_type` | From → To | Method | Confidence |
|---|---|---|---|
| `DERIVES_FROM` | relation → relation | `pg_catalog` (373 edges free) | 1.00 |
| `HAS_SERIES` | relation → data_series | `SELECT DISTINCT` | 1.00 |
| `READS` | file/proc/sql → relation \| series | `python_ast` / `vba_parse` / `sql_parse` / `regex` | 0.4–0.9 |
| `WRITES` | file/proc/sql → relation \| series | same | 0.4–0.9 |
| `EMITS` | file → workbook \| flat_file_series | `python_ast` on writer paths | 0.90 |
| `SCHEDULED_AS` | scheduled_job → file | registry + schedule JSON | 1.00 |
| `INVOKES` | vba_procedure → relation/view | `vba_parse` of the SQL string | 0.90 |
| `LINKS_TO` | workbook → workbook | `xlsx_extlink` | 1.00 |
| `BINDS_TO` | sheet_block → data_series \| flat_file_series | `xlsx_formula` criteria mining | 0.70 |
| `PUBLISHED_IN` | series/block → deliverable | declared | declared |
| `SOURCED_FROM` | relation → external_source | declared + collector metadata | 0.90 |
| `SUPERSEDED_BY` | workbook → workbook | declared (heuristic proposes) | declared |
| **`SERVES`** | data_series → `core.kg_node` | **declared** — the seam, §6 | declared |

**Direction convention, fixed:** edges point **in the direction data flows**. `READS` is the one that
trips people (`file READS relation` runs against the flow), so it is stored as
`relation --READS--> file` with `edge_type='READS'`. Downstream traversal is then always
"follow edges forward" with no per-type special-casing. Write it down once; never re-litigate it.

### 5.3 Table shape

Illustrative only — **Session 2 writes and runs the migration.**

```
sys.scan          scan_id, started_at, finished_at, git_sha, extractor_version, stats jsonb
sys.node          id, node_type, node_key UNIQUE, label, properties jsonb,
                  lifecycle, extraction_method, confidence, resolution_status,
                  first_seen_scan, last_seen_scan
sys.edge          id, source_node_id, target_node_id, edge_type, properties jsonb,
                  evidence jsonb, extraction_method, confidence, resolution_status,
                  first_seen_scan, last_seen_scan
sys.declaration   id, subject_key, predicate, object_key, ruled_by, ruled_at, rationale
```

Two things this shape buys:

- **`first_seen_scan` / `last_seen_scan` instead of delete-and-reload.** A node absent from the
  current scan is *not removed* — its `last_seen_scan` simply stops advancing. That turns
  "what disappeared between scans" into a query, and makes rebuilds non-destructive.
- **`sys.declaration` is separate and never touched by a scan.** Every human ruling — this workbook
  is canonical, this series serves that analyst node, this deliverable publishes that block — lives
  here and is *replayed onto* the graph after each rebuild. Rebuilds cannot destroy rulings. This is
  the same instinct as D1, one level down.

---

## 6. The seam to the analyst KG

Exactly one edge type crosses: `data_series --SERVES--> core.kg_node`.

It must be **declared**, not inferred. Verified reason: the 81 analyst `data_series` nodes are keyed
by *source* (`eia.ethanol`, `census.fat_grease_trade`, `mpob.cpo_production`), while `sys` nodes are
keyed by *relation* (`bronze.eia_monthly_biofuels[...]`). No naming convention bridges them, and
guessing a mapping would fabricate exactly the kind of plausible-but-wrong fact CLAUDE.md forbids.
~81 declarations, written once, maintained by hand.

Access is unified even though storage is not:

- `sys.v_graph` — union view over `sys.*` + `core.kg_*`, one shape, `graph` column discriminates.
- **One MCP tool, `trace_series(series_key, direction)`** — walks upstream to external source or
  downstream to deliverable, crossing the seam automatically, returning the path with confidence and
  evidence on each hop. This is the tool that answers Q1 in one call.

Keep `search_knowledge_graph` pointed at `core.kg_*` only. Analysts should never get a Python file
back from a question about soybean oil.

---

## 7. Auto-extracted vs declared — no ambiguity

Anything auto-extracted must be reproducible by re-running the scan. Anything else is a declaration
with a name on it.

| Fact | How | Honest note |
|---|---|---|
| Relations, columns, view deps | `pg_catalog` | Exact. 373 edges free. |
| Series that **exist** | `SELECT DISTINCT` on key cols | Exact for what's in the table. Silent on what *should* be. |
| Series a script **writes** | literal mining of `INSERT`/param dicts | **Partial.** Computed keys are invisible. Mark `regex`/0.4. |
| Python → relation refs | AST for `execute()` strings, regex fallback | 539/1,188 resolved today; expect 0.85+ with AST. |
| VBA → relation | parse the embedded SQL string | 26 modules, high yield. |
| Workbook → workbook | `xlsx_extlink` | Exact. Already found the `eia_data.xlsm` chain. |
| Block → series | SUMIFS/COUNTIFS criteria mining | **Verified feasible** (§D2). Column-letter→contract-column map must be checked per file. |
| Writer → flat file | AST on `wb.save()` / `to_excel()` paths | Paths are often computed; expect ~70% and declare the rest. |
| Job → script | `collector_registry.py` + `collection_schedule.json` | Exact for registered. **Says nothing about `scripts/`** — see §11. |
| **Canonical vs stale** | — | **Declared.** Heuristics propose; a human rules. |
| **Series → analyst node** | — | **Declared.** §6. |
| **Deliverable → series** | — | **Declared.** ~15 deliverables. |
| **Read vs write intent** | — | Partly declared. A bare `gold.x` string in a docstring is neither. |

---

## 8. Checks that live in the code, not in anyone's head

Per CLAUDE.md — the durable fix is an assertion that runs every scan. These are **binding**; the
extractor fails, not warns, on the first three.

1. **Catalog reconciliation.** Every `db_relation` node exists in `information_schema`, and every
   live relation has a node. Zero drift either way.
2. **No positional identity.** No `node_key` matches `[A-Z]{1,3}[0-9]+` in a position-bearing slot.
   Mechanically forbids cell-address identity (D3).
3. **Free-lineage floor.** `DERIVES_FROM` edge count ≥ the live `pg_depend` count (373 today). A
   regression means the catalog extractor silently broke.
4. **Resolution rate.** Report `resolved / (resolved + unresolved)` per extraction method per scan.
   Fail the scan if any method drops >10 points below its previous run.
5. **Declaration survival.** Every `sys.declaration` row re-attaches after rebuild, or is reported as
   orphaned. A declaration that silently stops applying is worse than no declaration.
6. **Q1 regression test.** `trace_series('silver.monthly_realized[attribute=oil_stocks]', 'down')`
   must return a non-empty, human-checked-once downstream set. If a refactor empties it, the graph
   broke — and the test says so.

---

## 9. Build order for Session 2

Each step is independently useful; stop anywhere and keep the value.

| # | Step | Gets you |
|---|---|---|
| 1 | `sys` migration + `sys.scan` | somewhere to put it |
| 2 | Catalog extractor — relations, columns, `pg_depend` | ~530 nodes, 373 edges, checks 1 & 3 green |
| 3 | Repo inventory — files, VBA modules, SQL scripts, registry/schedule join | Q3 half-answered |
| 4 | Series extractor over in-scope relations | the spine exists |
| 5 | Code→relation extractor (AST first, regex fallback, resolve-or-flag) | Q3 answered; 649-unresolved report |
| 6 | Workbook inventory + external links | Q2 answered — the `eia_data.xlsm` chain lights up |
| 7 | Block + formula-criteria extractor, oils/fats chain only | Q1 answered end to end |
| 8 | `sys.declaration` + the ~81 `SERVES` rulings | seam closed |
| 9 | `trace_series` MCP tool + `sys.v_graph` | it becomes usable |
| 10 | Checks §8 wired into the scan | it stays true |

Steps 2, 3 and 6 are mechanical and should land in one sitting. Step 7 is the one with real
uncertainty — budget for the column-letter mapping to differ per workbook.

---

## 10. Honest assessment of what this is worth

It is not free. Steps 1–6 are perhaps a day; step 7 is unpredictable; the declarations are ongoing
hand-maintenance forever.

**Worth it because** the `oil_stocks` bug is not a one-off. A wrong series hid behind a correct total
for months (handoff §7), and the only reason it surfaced was Tore reading a number he recognised as
wrong. That does not scale, and it is the same failure mode as the 2011–15 gap and the tallow
discrepancy. The graph does not find defects. It answers *"now that we found one, what else did it
touch"* in seconds instead of an afternoon of grep — and the afternoon of grep is the step that
actually gets skipped.

**Where I would push back on the plan as written:** calling this a "KG" invites merging it into the
analyst KG. D1 says don't, and the naming should follow — call it the **system graph** and keep
"knowledge graph" for the analyst brain. Related: `sys.declaration` should be reviewed as
carefully as any ruling doc. It is a place where an assumption can be written down once and inherited
forever without re-verification, which is the exact mechanism behind several of 2026-07-21's four
corrections.

---

## 11. Open questions — need Tore

1. **`scripts/` liveness (248 files).** The registry covers collectors. It says nothing about
   `scripts/`. Options: (a) git-recency heuristic, (b) declare a live set once and treat the
   remainder as archive candidates, (c) leave `unknown` and let the graph's inbound-edge count
   speak. **Recommend (c) then (b)** — let the graph propose the archive list, then rule on it.
2. **Delete or keep `audit.*`?** `lineage_edge` has 29 rows, `output_artifact` 0. Leaving both a dead
   and a live lineage model invites the wrong one being read. **Recommend:** keep the tables, add a
   comment pointing at `sys`, revisit after v1 proves out.
3. **Scan cadence.** On demand? Pre-commit? Nightly via dispatcher? Nightly makes drift a report
   rather than a discovery, but a nightly openpyxl pass over 467 workbooks is not cheap.
   **Recommend:** on-demand for v1, split cheap-nightly (catalog + repo) from expensive-on-demand
   (workbooks) once cost is measured.
4. **Does the graph get a lifecycle opinion on the 96 archival workbooks** — propose deletion, or
   only ever mark and report? Bears on whether this becomes a cleanup tool or stays an atlas.

---

## 12. Deliberately deferred to v2

Function-level Python symbols and `CALLS` edges (D5) · runtime row counts and last-run-actually-ran ·
column-level lineage *within* a SQL transform (`sqlglot` gives it; nothing asks for it yet) ·
`clients/` and `dashboards/` depth · git history as a liveness signal · anything auto-writing to
`sys.declaration`.

---

## 13. Nothing was built

No migration written, no table created, no extractor coded. Read-only queries against the live
database and read-only scans of repo files only. Session 2 owns all construction.

---

## 14. What I did **not** verify

- **Whether the 26 `.bas` files in git match the VBA actually embedded in the `.xlsm` workbooks.**
  They could have diverged. If they have, the VBA extractor is reading fiction. **Check this first
  in Session 2** — it is the cheapest way for this design to be wrong.
- **Whether the bare `models/Oilseeds/*.xlsm` external-link targets are genuinely stale.** Observed:
  live workbooks link to `models/Oilseeds/us_oilseed_crush.xlsm` while active flat files sit in
  `models/Oilseeds/United States/`. Memory says the bare copies are stale. I did not open both and
  compare. **Stated as a flag, not a finding.**
- **The column-letter → flat-file-contract-column mapping generalises.** Confirmed for
  `us_soybean_complex_bal_sheets.xlsm` (`$A`=commodity … `$F`=period, `$H`=vintage_rank, `$I`=value),
  which matches contract §2 exactly. Not checked on any other workbook.
- **Node-count estimates in §5.1** are extrapolations from measured file counts, not counted.
- **Whether the 41 collectors in `core.collection_status` match the 163 `collector_registry` entries.**
  Not compared. Bears directly on Q3.
- **openpyxl reads every workbook in the estate.** It read the ones I tried. 467 files, some `.xlsm`,
  some with recovered links (`RecoveredExternalLink1` appeared in one backup) — expect failures and
  budget for them.
- **Extraction cost at full scale.** I scanned a handful of workbooks, not 467. One held 698k formula
  cells. §11.3 depends on a number I have not measured.
