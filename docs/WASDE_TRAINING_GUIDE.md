# WASDE Report Training Guide

## Overview

This guide walks you through training the LLM to produce publishable WASDE analysis reports. The system uses a 4-phase approach: start with simple data accuracy, then add complexity until the LLM generates professional-quality market commentary.

**Goal:** The LLM autonomously writes a comprehensive WASDE summary each month that accurately analyzes balance sheet changes, draws correct inferences from the knowledge graph, and produces publication-ready market commentary — with minimal human supervision.

---

## Quick Start

```bash
# From the project root (C:\dev\rlc-agent)

# Phase 1: Test data accuracy (start here)
python -m src.training.wasde_trainer --phase 1

# Check the output
python -m src.training.wasde_trainer --review

# Read a specific iteration
python -m src.training.wasde_trainer --show 1

# Add your feedback
python -m src.training.wasde_trainer --feedback 1 --rating 3 --comment "Corn stocks number is wrong"

# Run again (incorporates feedback in phases 3-4)
python -m src.training.wasde_trainer --phase 1 --iterations 3

# Check overall progress
python -m src.training.wasde_trainer --progress
```

---

## Training Phases

### Phase 1: Data Accuracy (Start Here)

**Objective:** The LLM correctly reports balance sheet numbers.

**What the LLM sees:** US balance sheet table only (corn, soybeans, wheat). No changes, no global data, no knowledge graph.

**What you check:**
- [ ] Production numbers match the WASDE report
- [ ] Ending stocks are correct
- [ ] Exports are correct
- [ ] Stocks-to-use percentages are calculated correctly
- [ ] Units are labeled (1000 MT)

**Pass criteria:** Rating 4+ with no wrong numbers across 3 consecutive iterations.

```bash
python -m src.training.wasde_trainer --phase 1 --iterations 3
```

### Phase 2: Delta Analysis

**Objective:** The LLM correctly describes month-over-month changes.

**What the LLM sees:** Balance sheet table + MoM revisions.

**What you check:**
- [ ] Direction is correct for each revision (raised vs cut)
- [ ] Magnitude of changes matches
- [ ] The 2-3 "headline changes" are actually the most significant ones
- [ ] No changes are fabricated

**Pass criteria:** Rating 4+ with all delta directions correct across 3 consecutive iterations.

```bash
python -m src.training.wasde_trainer --phase 2 --iterations 3
```

### Phase 3: Context & Inference

**Objective:** The LLM draws correct inferences using Knowledge Graph context and global data.

**What the LLM sees:** Full balance sheet + deltas + global S&D (Brazil, Argentina, China) + Knowledge Graph analyst context.

**What you check:**
- [ ] Inferences are supported by the data (not hallucinated)
- [ ] Cross-market links make sense (e.g., Brazil production → US exports)
- [ ] KG context is used appropriately (seasonal patterns, positioning, etc.)
- [ ] Global context is accurate

**Pass criteria:** Rating 4+ with correct inferences across 3 consecutive iterations.

```bash
python -m src.training.wasde_trainer --phase 3 --iterations 3
```

### Phase 4: Full Report

**Objective:** Publication-quality WASDE analysis with all 6 sections.

**What the LLM sees:** Everything from Phase 3 + August WASDE detection + accumulated feedback.

**What you check:**
- [ ] All 6 sections present and well-structured
- [ ] Professional tone (comparable to sample reports in `domain_knowledge/sample_reports/`)
- [ ] Under 600 words
- [ ] Market implications are actionable
- [ ] Would you publish this as-is?

**Pass criteria:** Rating 5 ("publishable") on 3 consecutive iterations.

```bash
python -m src.training.wasde_trainer --phase 4 --iterations 3
```

---

## Feedback Guide

### Rating Scale

| Rating | Meaning | Action |
|--------|---------|--------|
| 1 | Unusable — major errors, hallucinations | Re-run same phase |
| 2 | Poor — several wrong numbers or missed sections | Add detailed feedback, re-run |
| 3 | Acceptable — minor issues | Add specific corrections, advance to next iteration |
| 4 | Good — publish with minor edits | Close to passing, refine |
| 5 | Publishable — no changes needed | Phase complete, advance to next phase |

### Giving Effective Feedback

**Be specific.** Instead of "numbers are wrong," say:

```bash
python -m src.training.wasde_trainer --feedback 5 --rating 2 \
  --comment "Corn ending stocks should be 44,195 not 44,500. Soybean exports section is missing."
```

**Common feedback categories:**
- `wrong_number` — A specific number is incorrect
- `missing_section` — A required section is missing
- `hallucination` — A number or fact was fabricated
- `tone` — Writing style needs adjustment
- `inference` — An inference is unsupported or wrong

### When to Advance Phases

Move to the next phase when:
1. Three consecutive iterations score 4+ from human review
2. Auto-evaluation overall score is consistently > 0.8
3. No "critical" issues in the last 3 iterations

---

## Architecture

### How It Works

```
gather_data()  →  build_prompt()  →  call_llm()  →  evaluate()  →  save_to_db()
     ↑                  ↑                                              ↓
   WASDE         Phase config +                              training_iterations
   template     prior feedback                                (scores + narrative)
                                                                       ↓
                                                              Human reviews
                                                              (rating + feedback)
                                                                       ↓
                                                              Next iteration prompt
                                                              includes feedback
```

### Data Flow

1. **WASDeAnalysisTemplate.gather_data()** queries `bronze.fas_psd` for US + global balance sheets
2. **Phase config** determines what data the LLM sees (balance sheet only → full context)
3. **Prior feedback** (phases 3-4) is injected into the prompt so the LLM learns from corrections
4. **Auto-evaluator** scores the output on 4 dimensions before human review
5. **Everything is saved** to `core.training_iterations` for tracking progress over time

### Auto-Evaluation Scores

| Score | Weight | What It Measures |
|-------|--------|-----------------|
| data_accuracy | 35% | Numbers in text match DB values |
| delta_accuracy | 30% | MoM direction words (raised/cut) match actual changes |
| no_hallucination | 20% | No fabricated numbers > 100 |
| completeness | 15% | Required sections present |

### Database Tables

| Table | Purpose |
|-------|---------|
| `core.training_runs` | One row per training session (phase + config) |
| `core.training_iterations` | One row per LLM generation (narrative + scores) |
| `core.training_feedback` | Structured corrections for prompt refinement |
| `core.training_progress` | View summarizing runs with best/avg scores |

---

## Model Selection

By default, the trainer uses the model router:
- **Phases 1-2:** `claude-sonnet-4-20250514` (medium complexity)
- **Phases 3-4:** `claude-opus-4-20250514` (high complexity, upgraded by router)

Override with `--model`:
```bash
# Use a specific model
python -m src.training.wasde_trainer --phase 1 --model claude-haiku-4-5-20251001

# Use local model (requires Ollama)
python -m src.training.wasde_trainer --phase 1 --model llama3.1:70b
```

---

## Troubleshooting

### "Data not ready" error
The WASDE template needs at least one commodity in `bronze.fas_psd` with `ending_stocks IS NOT NULL`. Check:
```sql
SELECT commodity, COUNT(*), MAX(report_date)
FROM bronze.fas_psd
WHERE country_code = 'US' AND ending_stocks IS NOT NULL
GROUP BY commodity;
```

### Low data_accuracy score
Compare the narrative numbers against the actual DB data:
```bash
python -m src.training.wasde_trainer --show <iteration_id>
```
The `data_snapshot` field shows exactly what data the LLM received.

### "ANTHROPIC_API_KEY not set"
Make sure `.env` is loaded:
```bash
# Check your .env has the key
grep ANTHROPIC_API_KEY .env
```

### Cost Management
Each Phase 1 iteration costs ~$0.01-0.03 (Sonnet, ~800 tokens out).
Phase 4 iterations cost ~$0.05-0.15 (Opus, ~1800 tokens out).
Track costs:
```sql
SELECT run_id, SUM(cost_usd) as total_cost, COUNT(*) as iterations
FROM core.training_iterations
GROUP BY run_id ORDER BY run_id DESC;
```
