# Knowledge Graph Extraction Pipeline: Processing Workflow

**Created:** 2026-02-14  
**Extends:** WIRING_PLAN.md, Part 3 (Knowledge Graph)  
**Schema:** 014_kg_source_registry.sql

---

## How This Works

You have hundreds (eventually thousands) of reports, articles, and analysis documents spanning 7+ years. Each one contains fragments of your analytical framework — causal relationships, seasonal patterns, risk thresholds, cross-market dynamics — expressed in the context of whatever was happening that week.

This pipeline extracts that framework systematically, tracks what we've processed, and ensures we never waste time re-reading the same document.

---

## The Processing Loop

Every time you add new documents to the Google Drive folder and start a processing session:

```
┌─────────────────────────────────────────────────────────┐
│                   PROCESSING SESSION                     │
│                                                          │
│  1. DISCOVER                                             │
│     Scan Google Drive folder for all documents           │
│     Compare against core.kg_source registry              │
│     Identify: NEW | CHANGED | ALREADY_PROCESSED          │
│                                                          │
│  2. REGISTER                                             │
│     Add new documents to kg_source (status='pending')    │
│     Flag changed documents for re-processing             │
│     Skip unchanged documents entirely                    │
│                                                          │
│  3. EXTRACT (per document)                               │
│     Read document content                                │
│     Extract KG entities:                                 │
│       - Nodes (commodities, data series, concepts)       │
│       - Edges (causal links, cross-market, substitutes)  │
│       - Contexts (seasonal norms, rules, thresholds)     │
│     Match against existing graph (deduplicate/reinforce)  │
│                                                          │
│  4. LOAD                                                 │
│     INSERT new nodes/edges/contexts                      │
│     UPDATE confidence on reinforced entries               │
│     INSERT provenance links (entity ↔ source document)   │
│     UPDATE kg_source status = 'completed'                │
│                                                          │
│  5. SUMMARIZE                                            │
│     Report: X new docs processed, Y skipped              │
│     New KG entries created, existing entries reinforced   │
│     Confidence changes on key relationships              │
└─────────────────────────────────────────────────────────┘
```

---

## What Gets Extracted

When reading a report, we look for these categories of knowledge:

### Nodes (Entities)
Things that exist in the commodity world:
- **Data series:** CFTC corn net position, USDA ending stocks, EIA ethanol production
- **Commodities:** Corn, soybeans, soy oil, palm oil, ethanol, renewable diesel
- **Regions:** US, Brazil, Argentina, EU, China, India
- **Reports:** WASDE, Crop Progress, Export Sales, COT
- **Market participants:** Managed money, commercials, index funds
- **Seasonal events:** Planting, pollination, harvest, safrinha, Argentine crush season
- **Policy mechanisms:** RFS, RVO, blending mandates, tariffs, 45Z credit
- **Balance sheet lines:** Ending stocks, stocks-to-use, export commitments pace

### Edges (Relationships)
How things connect — this is the core of the analyst's brain:
- **CAUSES:** Late safrinha planting → tighter global corn S&D → higher CBOT corn
- **COMPETES_WITH:** Soy oil ↔ palm oil in biodiesel feedstock
- **SUBSTITUTES:** UCO ↔ animal fats ↔ DCRO in renewable diesel
- **LEADS:** Fund positioning changes → price direction (with lag)
- **SEASONAL_PATTERN:** Export commitments front-loaded in MY → pace assessment
- **RISK_THRESHOLD:** Stocks-to-use below 8% → rationing mode
- **CROSS_MARKET:** Corn/ethanol grind margin → ethanol production incentive
- **TRIGGERS:** WASDE revision, crop condition drop, demand surprise
- **SUPPLIES / CONSUMES:** Brazil supplies global soy; China consumes US soybeans

### Contexts (Enrichment)
The "so what" for each node:
- **Seasonal norms:** What's normal for this metric at this time of year?
- **Percentile ranges:** Where does the current value sit historically?
- **Risk thresholds:** At what level does behavior change? (tight vs comfortable S&D)
- **Historical analogs:** When have we seen this before, and what happened?
- **Expert rules:** Your rules of thumb ("extended positioning + pre-planting = liquidation risk")
- **Causal chains:** Multi-step logic chains you apply in analysis

---

## Deduplication & Reinforcement Logic

The same relationship will appear in many reports. That's a feature, not a bug.

### When we find a relationship that already exists in the graph:

```
Report says: "Brazilian safrinha delays typically support corn prices"

Check kg_edge: source='brazil.safrinha.planting' → target='cbot.corn.price'
  type='CAUSES'

EXISTS? 
  YES → Add provenance link (this doc also says this)
       → Increment source_count on the edge
       → If new details/nuance, update edge properties
       → Bump confidence toward 1.0
       
  NO  → Create new edge
       → Add provenance link
       → Set source_count = 1
       → Set confidence based on how explicitly stated
```

### Confidence scoring:

| Source Count | Confidence Boost | Interpretation |
|-------------|-----------------|----------------|
| 1 | Base (0.7-0.9) | Mentioned once — could be situational |
| 2-5 | +0.05 per source | Recurring theme — likely part of framework |
| 6-15 | +0.02 per source | Core analytical principle |
| 16+ | Cap at 0.99 | Foundational — you think about this constantly |

Additional factors:
- **Time span:** Same relationship mentioned in 2019 AND 2025 = very stable
- **Explicit vs implicit:** "I always watch X" > "X happened to affect Y this week"
- **Specificity:** Quantified thresholds ("above 90th percentile") > vague ("extended")

---

## Session Management: How We Track Progress

### Starting a New Session
```
Human: "I added 50 new reports to the Drive folder. Let's process them."

Claude: 
  1. Scans Google Drive folder
  2. Checks kg_source registry (via this conversation's working memory)
  3. Reports: "Found 50 new documents. 12 are weekly reports (2023-2024), 
     8 are daily notes, 30 are third-party articles. Ready to process?"
```

### Within a Session
Since Claude's context window has limits, we process in batches:
- ~5-10 documents per batch (depending on length)
- After each batch: output SQL INSERT statements or JSON
- Track which documents are done in the conversation

### Between Sessions
The kg_source table in PostgreSQL is the permanent record. At the start of each
new session, we can query it to know exactly where we left off.

Until the database tables are actually created and connected, we'll maintain a
**processing log as a Google Doc or Notion page** that tracks:
- Document name
- Processing date
- Status (processed / skipped / needs-reprocess)
- Extraction summary (X nodes, Y edges, Z contexts)

---

## Practical Workflow for Each Session

### Step 1: You tell me what's new
"I added 20 reports from 2023 to the folder" or "Process whatever's new"

### Step 2: I scan and triage
- List what I find
- Categorize by type, date range, commodity focus
- Propose a processing order (usually chronological — older reports first
  so we build the foundation before layering on recent nuance)

### Step 3: I read and extract (in batches)
- Read each document
- Extract nodes, edges, contexts
- Check against what we've already built
- Flag anything ambiguous for your confirmation

### Step 4: I output structured results
- SQL INSERT statements ready for your database
- Or JSON files matching the kg_node/kg_edge/kg_context schema
- Processing log updates

### Step 5: You review and approve
- Especially for edges (relationships) — these encode YOUR thinking
- Computed things like seasonal percentiles can be auto-approved
- Expert rules and causal chains should get your eyes

---

## File Organization in Google Drive

Suggested structure for the Reports folder:

```
G:\My Drive\LLM Model and Documents\Reports\
├── Weekly Reports/           -- Your weekly market analysis
│   ├── 2019/
│   ├── 2020/
│   ├── ...
│   └── 2026/
├── Daily Notes/              -- Quick daily observations
├── Client Reports/           -- Formal deliverables
├── Third Party/              -- Articles, research you found valuable
│   ├── USDA/
│   ├── Industry/
│   └── Research/
├── Biofuels/                 -- Biofuel-specific analysis
├── Trade & Policy/           -- Tariffs, RFS, mandates
└── _Processing Log.gsheet    -- Tracking spreadsheet (optional)
```

You don't have to organize it perfectly — I can work with a flat pile of files.
But any structure you add helps me categorize and prioritize.

---

## What This Produces Over Time

After processing a few hundred reports, the knowledge graph will contain:

- **~50-100 nodes** covering the key entities in your analytical universe
- **~200-500 edges** mapping the relationships between them
- **~300-800 contexts** providing the "analyst brain" enrichment
- **Full provenance** — every entry traced back to specific reports
- **Confidence scores** — backed by how many times and over how long you've referenced each relationship
- **A living document** of your analytical framework that no competitor can replicate

The graph becomes the foundation that makes the Desktop LLM actually think
like a commodity analyst — specifically, like YOU.
