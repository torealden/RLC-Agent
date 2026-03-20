# Part 6: Working with the LLM

[← Back to Table of Contents](00_COVER_AND_TOC.md) | [← Previous: Adding New Data Sources](05_ADDING_DATA_SOURCES.md)

---

## 6.1 LLM Capabilities Overview

The RLC platform integrates Large Language Models (LLMs) to enhance analysis and automate routine tasks. The LLM can:

- **Query data** using natural language
- **Generate reports** from database content
- **Explain market movements** by synthesizing multiple data sources
- **Answer questions** about historical patterns and relationships
- **Write analysis** for specific commodities or time periods

### How It Works

```
┌─────────────────────────────────────────────────────────────────────┐
│                         YOUR QUESTION                                │
│         "What drove the change in soybean ending stocks?"           │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                           LLM AGENT                                  │
│  • Interprets your question                                          │
│  • Identifies relevant data sources                                  │
│  • Formulates database queries                                       │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
        ┌──────────┐    ┌──────────┐    ┌──────────┐
        │ Database │    │   Web    │    │ Domain   │
        │  Queries │    │  Search  │    │Knowledge │
        └────┬─────┘    └────┬─────┘    └────┬─────┘
              │              │               │
              └──────────────┼───────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        LLM SYNTHESIS                                 │
│  • Combines data from multiple sources                               │
│  • Applies commodity market expertise                                │
│  • Generates coherent analysis                                       │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         YOUR ANSWER                                  │
│  "Soybean ending stocks fell 50M bu due to stronger-than-expected   │
│   crush demand (+25M) and higher exports to China (+30M), partially │
│   offset by a 5M bu increase in the production estimate..."         │
└─────────────────────────────────────────────────────────────────────┘
```

### Available LLM Modes

| Mode | Interface | Best For |
|------|-----------|----------|
| **Interactive** | Command line | Ad-hoc questions, exploration |
| **Report Generation** | Automated | Weekly reports, scheduled analysis |
| **Claude Code** | VS Code / Terminal | Development, data exploration |

---

## 6.2 Querying Data with Natural Language

### Interactive Mode

Start an interactive session:

```bash
cd C:\dev\RLC-Agent
python -m src.main interactive
```

You can then ask questions naturally:

```
You: What were the biggest changes in the January WASDE?

Agent: The January 2025 WASDE made several significant revisions:

CORN:
- Production: +33 million bushels (14,867 → 14,900)
- Ending stocks: +15 million bushels
- Stocks-to-use: 10.2% (unchanged)

SOYBEANS:
- Crush: +10 million bushels (higher domestic demand)
- Exports: -15 million bushels (slower China pace)
- Ending stocks: +5 million bushels (350 → 355)

WHEAT:
- No significant changes to US balance sheet
- World ending stocks: -2 MMT (lower Australia production)

The corn production increase reflects updated yield data from NASS.
The soybean export reduction aligns with slower inspection pace,
currently running 8% behind last year.
```

### Example Questions

**Balance Sheet Analysis:**
- "What is the current corn stocks-to-use ratio and how does it compare to the 5-year average?"
- "How have soybean ending stocks changed over the last 6 WASDE reports?"
- "What's driving the tighter wheat balance sheet this year?"

**Trade Flow Analysis:**
- "Show me soybean export pace vs. last year by destination"
- "Which countries have increased corn imports the most this year?"
- "Are we on track to meet the USDA export forecast?"

**Positioning:**
- "Where are managed money positions in corn relative to historical range?"
- "How has speculative positioning changed since the last WASDE?"

**Price Analysis:**
- "What's the current corn basis in the Gulf?"
- "How do soybean crush margins compare to last year?"

**Cross-Market:**
- "How is Brazilian soybean production affecting US export prospects?"
- "What's the relationship between ethanol production and corn demand?"

### Tips for Better Queries

| Instead of... | Try... |
|---------------|--------|
| "Tell me about corn" | "What are the key changes in the US corn balance sheet this month?" |
| "What's happening with exports?" | "How do soybean exports to China compare to this time last year?" |
| "Is wheat bullish?" | "What factors could tighten the wheat stocks-to-use ratio?" |

**Be specific about:**
- Time period (this month, this marketing year, vs. last year)
- Commodity (corn, soybeans, wheat, etc.)
- Geography (US, World, Brazil, etc.)
- Metric (production, exports, stocks-to-use, etc.)

---

## 6.3 Report Generation

The LLM can automatically generate market reports using database content.

### Weekly Market Summary

Generate a weekly report:

```bash
python -m src.main report --type weekly --commodity corn
```

**Output:** A markdown document summarizing:
- Key data releases from the past week
- Price action and positioning changes
- Notable trade flow developments
- Upcoming events to watch

### WASDE Summary Report

Generate after a WASDE release:

```bash
python -m src.main report --type wasde --release-date 2025-01-12
```

**Output:** Analysis of changes from prior month, comparison to expectations, and market implications.

### Custom Report Templates

Reports follow templates in `templates/reports/`:

```
templates/reports/
├── weekly_summary.md
├── wasde_analysis.md
├── export_pace.md
└── positioning_review.md
```

Each template defines:
- Required data queries
- Section structure
- Analysis prompts

**Example template structure:**
```markdown
# {{commodity}} Weekly Summary
Week ending: {{week_ending_date}}

## Key Data This Week
{{#each data_releases}}
- **{{source}}**: {{summary}}
{{/each}}

## Price Action
{{price_analysis}}

## Export Pace Update
{{export_pace_analysis}}

## Week Ahead
{{upcoming_events}}
```

### Scheduling Reports

Add to the scheduler for automatic generation:

```python
# In config/report_schedule.json
{
    "weekly_corn_summary": {
        "template": "weekly_summary",
        "commodity": "corn",
        "schedule": "Friday 4:00 PM",
        "output": "reports/corn_weekly_{date}.md",
        "distribute": ["dropbox", "email"]
    }
}
```

---

## 6.4 Best Practices

### Effective Prompting

**DO:**
- ✅ Ask specific, focused questions
- ✅ Specify time periods and commodities
- ✅ Request comparisons (vs. last year, vs. average)
- ✅ Ask for sources/data behind conclusions

**DON'T:**
- ❌ Ask vague questions ("What should I know?")
- ❌ Expect predictions ("Will corn go up?")
- ❌ Ask about real-time prices (data may be delayed)
- ❌ Assume the LLM has today's news (web search needed)

### Verifying LLM Output

The LLM is helpful but not infallible. Always:

1. **Check the data sources** — Ask "What data did you use for this?"
2. **Verify key numbers** — Spot-check important figures against Gold views
3. **Consider context** — The LLM may not have the latest news
4. **Use professional judgment** — LLM analysis augments, not replaces, expertise

### Data Freshness Awareness

The LLM queries the database, so its knowledge is only as current as the latest collection:

```
You: What's the latest on soybean exports?

Agent: Based on data through March 14, 2025:
- Weekly inspections: 1.2 MMT
- Cumulative MY: 35.5 MMT
- Pace vs. USDA: 78% (vs. 81% LY)

Note: This reflects data collected this morning.
For the most current figures, check the USDA website.
```

### When to Use LLM vs. Direct Query

| Use LLM When... | Use Direct Query When... |
|-----------------|--------------------------|
| You need synthesis across sources | You need exact numbers |
| You want natural language explanation | You're building a report |
| You're exploring a topic | You need data for calculations |
| You want market context | You need to export data |

### Combining LLM with Power BI

A powerful workflow:

1. **Use LLM** to identify interesting patterns or questions
2. **Query database** to get precise data
3. **Build Power BI visual** to present findings
4. **Use LLM** to generate narrative for the visual

**Example:**
```
You: What's unusual about recent corn export pace?

Agent: Corn exports to Mexico are running 25% ahead of last year,
which is unusual given the stronger dollar. This is worth investigating.

You: [Builds Power BI chart showing corn exports to Mexico by month]

You: Write a caption for this chart.

Agent: "US corn exports to Mexico have surged in 2024/25, reaching
15 MMT through February—25% above last year's pace. Strong Mexican
feed demand and competitive US prices (basis Gulf -10¢ vs. LY) have
overcome currency headwinds. If the pace continues, full-year exports
could exceed the USDA's 14.5 MMT forecast by 2-3 MMT."
```

---

## Troubleshooting LLM Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "I don't have access to that data" | Missing series/view | Check if data source is configured |
| Outdated information | Collection not run | Check Operations Dashboard for freshness |
| Incorrect calculations | LLM math error | Verify with direct SQL query |
| "I can't provide predictions" | Asking for forecasts | Rephrase as analysis of historical patterns |
| Slow response | Large data query | Be more specific about time range |

### Getting Help

If the LLM isn't providing useful answers:

1. **Check data freshness** — Is the data current?
2. **Simplify the question** — Break complex questions into parts
3. **Be explicit** — Specify exactly what data you want
4. **Try direct query** — Some questions are better answered with SQL

---

## Quick Reference: Common LLM Commands

```bash
# Start interactive session
python -m src.main interactive

# Ask a single question
python -m src.main query "What changed in the January WASDE?"

# Generate weekly report
python -m src.main report --type weekly --commodity corn

# Generate WASDE analysis
python -m src.main report --type wasde --release-date 2025-01-12

# Search for specific data
python -m src.main query "Show me soybean exports to China this MY"
```

---

[← Previous: Adding New Data Sources](05_ADDING_DATA_SOURCES.md) | [Next: Appendix A - File List →](APPENDIX_A_FILE_LIST.md)
