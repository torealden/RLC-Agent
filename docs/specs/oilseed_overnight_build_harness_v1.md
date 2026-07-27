# Oilseed Overnight Build Harness — Specification v1

**Status:** draft v1, 2026-07-27. The local-LLM (Ollama) grind that builds out country oilseed monthly
blocks overnight, gated by a deterministic verifier. Companion to
`oilseed_balance_file_spec_v1.md` (the file it builds) and `write_balance_sheet.py` (the generator core).

**Decision locked (2026-07-27, Tore):** the LLM produces **plumbing + code, gated** — it writes/fixes
per-country collectors and recipes; a deterministic Python generator builds the workbook; a win32com
recalc is the pass/fail. The LLM never lays out cells or self-certifies. It optimizes toward one signal:
*make the verifier go green.*

---

## 0. The principle

An LLM free-handing a spreadsheet is non-deterministic and reproduces the US reference's fragility
(external links, missing rollups, position refs). So we bound it: **the only thing the LLM can change
is code that must pass a mechanical gate.** Its output can be wrong a hundred times overnight and the
model stays clean, because nothing it writes is trusted until the recalc verifier (spec §7) says green.
This is `feedback_gate_beats_parameter` applied to an autonomous agent.

Compute goes where it belongs: the big NVIDIA card runs the 30B coder all night on the bounded task
(write a parser to a fixed schema, read the concrete error, fix, retry); the deterministic parts
(layout, formulas, tie-outs, the gate) stay in Python where they're fast and correct.

---

## 1. What each side owns

| Deterministic Python (never the LLM) | Local LLM (qwen3-coder:30b via Ollama) |
|---|---|
| Workbook layout, formulas, mirror tabs, guards (`write_oilseed_complex.py`) | Write/fix the per-country **collector** (source → bronze/silver long table) |
| The recalc gate (`verify_oilseed_recalc.py`, guards 1–5) | Fill/repair the **recipe** (`_recipe_<complex>.yaml`, spec §11) |
| The conductor loop, queue, retries, logging | Map source columns → canonical series/unit/MY-basis |
| The rake + monthly-identity math | Diagnose a gate failure and propose the code change |
| Marking coverage state on green | Emit a structured **blocked** report when no monthly source exists |

---

## 2. Architecture

```
                        ┌─────────────────────────────────────────────┐
 queue.jsonl  ────────► │ CONDUCTOR  (deterministic Python, no LLM)    │
 (country,complex,      │  for each job, up to N iterations:           │
  member,series)        │    1. build prompt = spec + recipe + last err│
                        │    2. call Ollama  ──────────►  qwen3-coder  │
                        │    3. write returned collector.py to sandbox │
                        │    4. run collector (isolated) → long table  │
                        │    5. run generator → workbook               │
                        │    6. run verifier (win32com) → GATE         │
                        │       PASS → commit artifacts, mark state    │
                        │       FAIL → append error to prompt, retry   │
                        │       no source → write blocked-report, next │
                        │  append every step to run log + journal      │
                        └─────────────────────────────────────────────┘
                                        │
                               morning_report.md  (green / blocked / failed + diffs)
```

Nothing in the loop trusts the model. The generator and verifier are the same code a human runs; the
LLM only feeds them a collector + recipe and reads their verdict.

---

## 3. The conductor (deterministic driver) — sketch

`scripts/overnight/conductor.py` (Python, no LLM logic of its own):

```python
for job in queue:                                  # (country, complex, member, series)
    ctx = load_spec_excerpt(job) + load_recipe(job) + load_source_probe(job)
    last_err = None
    for attempt in range(MAX_ATTEMPTS):            # default 4
        prompt = build_prompt(ctx, last_err)       # §4 — tight, schema-anchored
        code   = ollama_generate(MODEL, prompt)    # §5 — local, /api/generate
        path   = write_sandbox(job, code)          # scripts/overnight/_work/<job>/collector.py
        r_run  = run_isolated(path, timeout=600)    # collector → writes long-form rows
        if not r_run.ok:
            last_err = r_run.stderr; continue
        r_gen  = run(["python","scripts/write_oilseed_complex.py", job.key])
        if not r_gen.ok:
            last_err = r_gen.stderr; continue
        r_ver  = run(["python","scripts/verify_oilseed_recalc.py", job.workbook])
        if r_ver.passed:
            commit(job, code); mark_state(job, r_ver.level); break     # level: annual|done
        last_err = r_ver.failure_digest            # TIE bad / rake off / identity / errors
    else:
        record_blocked_or_failed(job, last_err)    # into morning_report
```

- **Isolation:** each collector runs in its own subprocess with a timeout and a scratch cwd; a hung or
  runaway collector kills that job, not the run. (No `--force`; `feedback_llm_extraction_variance`.)
- **Idempotent + resumable:** the queue records per-job status; a killed run resumes where it stopped.
- **Determinism of the gate, not the model:** two different collectors that both pass the verifier are
  both acceptable; the model's run-to-run variance is absorbed by the gate.

---

## 4. The prompt contract (bounded, schema-anchored)

Each call gets exactly what it needs and nothing it can wander into:
- the relevant **spec excerpt** (the long-form schema §6.1, the series' `my_basis`, the rake rule);
- the **recipe** row for this series (source name/table/column/unit);
- the existing **collector base** it must subclass (`src.agents.base.base_collector.BaseCollector`,
  `db_config.get_connection()` context manager — the repo's real conventions, so it doesn't invent);
- a **source probe**: the first ~30 rows / column headers of the actual source (so it maps real columns,
  not guessed ones);
- on retry, the **verbatim gate failure** (`feedback_read_errors_fully` — full error, not a summary).

Output contract: a single self-contained `collector.py` writing rows in the 11-col long schema, or the
literal token `BLOCKED: <reason>` if the named source has no monthly data. Nothing else.

---

## 5. Ollama integration

- **Model:** `qwen3-coder:30b` (per `user_hardware_ollama`; 16GB VRAM). Config in
  `scripts/overnight/config.yaml` (model, `MAX_ATTEMPTS`, timeouts, temperature≈0.1 for determinism).
- **Call:** local `POST /api/generate` (or `/api/chat`), stream off, low temp, `num_ctx` sized to fit
  the prompt contract (§4). GPU-resident; `reference_ollama_gpu_cpu_fallback` — if it spills to CPU the
  job just runs slower, doesn't fail.
- **No cloud, no cost.** This is exactly the `reference_local_vs_cloud_llm` split: local for
  high-volume deterministic grind, cloud (me) reserved for the judgment/design and client-facing work.

---

## 6. The queue & morning report

- **Queue** `scripts/overnight/queue.jsonl`: one line per (country, complex, member, series). Seed it
  from the recipes; the 10 remaining Tier-A cells × ~4 series each ≈ 40 jobs.
- **Morning report** `docs/overnight/<date>_build_report.md`:
  - ✅ **green**: series that raked + verified (with the rake residual and identity max).
  - ⛔ **blocked**: no monthly source — names the series and the decision needed (find source vs
    seasonalize; e.g. MPOB monthly for Malaysia palm, StatCan for Canada canola).
  - ❌ **failed**: hit MAX_ATTEMPTS — the last gate failure + the last collector diff, for a human
    5-minute look, not a rebuild.
- Tore's morning loop: skim green, decide the blocked ones, glance at failed. Minutes, not a day.

---

## 7. Honest expectations (do not oversell)

A 30B local model is good at *bounded* code against a clear schema with a concrete error to fix. It is
**not** good at: JS-rendered scrapes (MPOB moved to a JS/AJAX site — spec handoff flagged it a rebuild),
authenticated portals, or ambiguous multi-file source formats. Realistically, night one:
- **db-sourced series close well** — Brazil soy (ABIOVE already in DB) and any country whose monthly
  source is already ingested. These are mostly a mapping problem the model handles.
- **api/csv series** — mixed; the model will get the straightforward ones, thrash on the odd ones.
- **scrape series (MPOB, some customs portals)** — will mostly land in **blocked**, correctly, with a
  "needs a browser-render collector" note rather than a fabricated table.

That is still a large step forward: the mechanical 80% becomes deterministic and instant, and the model
clears the tractable data-mapping overnight, leaving you a short morning list of genuine judgment calls
(which source, whether to seasonalize) instead of hand-building 40 monthly blocks. If night one clears
even the db- and api-sourced series, the remaining hard scrapes are a known, bounded backlog — and the
harness re-runs them free every night as sources get wired.

---

## 8. Build order

1. `write_oilseed_complex.py` (generator, monthly) + `verify_oilseed_recalc.py` (guards 1–5). **I build
   these — deterministic, my lane.**
2. Brazil soy recipe + generate + verify to prove the golden path end-to-end (data's in DB).
3. `conductor.py` + prompt contract + Ollama config + queue seeding.
4. First supervised overnight run on the db-sourced subset; read the morning report; iterate the prompt.
5. Widen to api/scrape series; wire the hard sources (MPOB browser-render) as they surface in blocked.

Steps 1–2 are the critical path and gate everything else — the harness has nothing to grade until the
generator + verifier exist. Nothing here runs an LLM against a spreadsheet; the LLM only ever writes a
collector that must survive the recalc.
