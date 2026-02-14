# HATBA Implementation Plan
## Practical Migration Strategy for RLC-Agent

**Prepared:** February 5, 2026
**Status:** Draft for Review

---

## 1. Executive Assessment

The HATBA analysis document proposes a 16-week migration to implement multi-hop reasoning capabilities. However, this timeline assumes dedicated focus, which is unrealistic given:

1. **Ongoing US Census Trade Data Work** - We're actively building and validating trade data pipelines for 18+ commodity groups
2. **International Trade Expansion** - After US Census, we need Eurostat, Brazil (Comex Stat), Canada (StatCan), China (GAC)
3. **Operational Data Collection** - The 46-agent architecture requires maintenance and expansion
4. **Limited Human Resources** - Round Lakes is a startup with constrained analyst/developer capacity

**Recommendation:** Pursue a **parallel-track approach** where HATBA infrastructure is built incrementally alongside trade data work, with each HATBA component designed to immediately improve the trade analysis workflow.

---

## 2. Resource Assessment

### Current Capabilities
| Resource | Status | Notes |
|----------|--------|-------|
| PostgreSQL Database | ✅ Production | 324K+ trade records, medallion architecture |
| Local LLM (Ollama) | ✅ Available | 184 tok/sec on RTX 5080 |
| Claude Code | ✅ Active | Primary development interface |
| Data Collection Agents | ✅ 46 agents | USDA, Census, EIA, NOAA |
| Domain Knowledge | ✅ Documented | Data dictionaries, marketing years |

### Additional Capacity Required
| Capability | Purpose | Acquisition Method | Timeline |
|------------|---------|-------------------|----------|
| Apache AGE Extension | Graph queries on PostgreSQL | `CREATE EXTENSION age` | 1 day |
| NetworkX Python | Graph prototyping | `pip install networkx` | 1 hour |
| Sentence-Transformers | Local template matching | `pip install sentence-transformers` | 1 hour |
| Additional Storage | Knowledge graph + provenance | Existing MSI Aegis sufficient | N/A |

### Human Resource Allocation
Given that Claude Code is the primary development resource, I propose:

| Activity | Time Allocation | Rationale |
|----------|-----------------|-----------|
| Trade Data Operations | 60% | Core business deliverable |
| HATBA Infrastructure | 30% | Long-term capability building |
| Maintenance/Support | 10% | Bug fixes, user support |

---

## 3. Revised Migration Timeline

Instead of the document's proposed 16 consecutive weeks, I recommend **24 weeks (6 months)** with work distributed to accommodate ongoing trade data priorities.

### Phase 0: Trade Data Foundation (Current - Week 4)
**Priority: Trade data operations**

Continue and complete US Census Bureau integration:
- [x] Fix Census API collector (YEAR/MONTH variables)
- [x] Create commodity reference table with unit conversions
- [x] Build gold views for VBA export
- [ ] Validate all 18 commodity groups against Census totals
- [ ] Document HS code mappings for all commodities
- [ ] Create automated data quality checks

**HATBA prep during this phase:**
- Document entity relationships observed during trade data work
- Note cross-commodity dependencies (canola oil → soybean oil substitution)
- Identify common analytical patterns ("how does X affect Y basis?")

### Phase 1: Knowledge Graph Foundation (Weeks 5-10)
**HATBA Focus: 2-3 days per week, trade data continues in parallel**

**Week 5-6: Entity Ontology Design**
- Define core entity types from existing data:
  ```
  Commodity → (CORN, SOYBEANS, WHEAT, CANOLA, ...)
  Product → (MEAL, OIL, SEED)
  Geography → (US, Brazil, Argentina, EU-27, ...)
  DataSource → (USDA_PSD, CENSUS_TRADE, EIA, ...)
  TimePeriod → (MarketingYear, CalendarMonth, ...)
  ```
- Map relationships from existing reference tables:
  ```
  CANOLA -[PRODUCES]-> CANOLA_MEAL
  CANOLA -[PRODUCES]-> CANOLA_OIL
  US -[EXPORTS_TO]-> MEXICO
  CENSUS_TRADE -[REPORTS_ON]-> CANOLA.EXPORTS
  ```

**Week 7-8: Graph Construction**
- Create `rlc_agent/knowledge_graph/` package
- Build graph from existing PostgreSQL tables:
  - `silver.trade_commodity_reference` → Commodity nodes
  - `silver.trade_country_reference` → Geography nodes
  - `bronze.fas_psd` → Supply/Demand relationship edges
- Implement basic query functions:
  - `find_related_commodities(commodity)`
  - `trace_supply_chain(product)`
  - `get_data_sources_for(entity, metric)`

**Week 9-10: Integration with Trade Workflow**
- Add graph queries to VBA helper functions
- Create `gold.commodity_relationships` view
- Test: "What data sources should I check when Brazilian soybean production changes?"

**Deliverable:** A queryable knowledge graph that enhances trade data analysis

### Phase 1.5: International Trade Expansion (Weeks 11-16)
**Priority: Trade data operations**

Build collectors for international trade data:
- **Eurostat** (EU-27 trade flows)
- **Brazil Comex Stat** (MDIC export data)
- **Statistics Canada** (CATSNET)
- **China GAC** (if accessible)

**HATBA parallel work:**
- Extend knowledge graph with international entities
- Add cross-border trade relationships
- Document multi-hop patterns: "Brazil drought → US export opportunity → which ports?"

### Phase 2: Analytical Template Library (Weeks 17-20)
**HATBA Focus: 3 days per week**

**Week 17-18: Template Design**
- Create 5-7 core templates based on observed analytical patterns:
  1. **Supply Shock Analysis** - "How will [event] affect [commodity] in [region]?"
  2. **Export Flow Analysis** - "What is [commodity] export pace to [destination]?"
  3. **Cross-Commodity Spread** - "How does [commodity_A] affect [commodity_B]?"
  4. **Seasonal Comparison** - "How does [metric] compare to [year] at this point?"
  5. **Balance Sheet Update** - "What changed in [commodity] S&D this month?"

**Week 19-20: Template Matcher**
- Implement template classification using local embeddings
- Create `rlc_agent/templates/` package with:
  - YAML template definitions
  - Template matcher (sentence-transformers)
  - Template executor (orchestrates data retrieval)

**Deliverable:** A template library that accelerates routine analysis

### Phase 3: Context Propagation (Weeks 21-24)
**HATBA Focus: Integration with existing infrastructure**

**Week 21-22: Context Envelope Design**
- Define `ContextEnvelope` dataclass:
  ```python
  @dataclass
  class ContextEnvelope:
      query_context: str  # Original question
      template_id: str    # Which template is being executed
      relevance_weights: Dict[str, float]  # What matters most
      reasoning_chain: List[str]  # Conclusions so far
      confidence: float   # Cumulative confidence
      provenance: List[DataSource]  # Audit trail
  ```

**Week 23-24: Agent Integration**
- Modify Master Agent to create/propagate envelopes
- Add context awareness to data collection agents
- Implement confidence-based alerts

**Deliverable:** Inter-agent context passing that prevents information loss

---

## 4. Phase 4 Deferral

The Multi-Hop Reasoning Engine (original weeks 13-16) should be **deferred to Q3 2026** after:
1. Knowledge graph is stable and populated
2. Template library has been tested on real analyses
3. Context propagation is proven to work
4. International trade data is integrated

This allows the foundational components to mature before building the orchestration layer.

---

## 5. Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Trade data work delayed by HATBA | Strict time allocation (60/30/10 split) |
| Knowledge graph becomes maintenance burden | Start with read-only graph over existing tables |
| Templates don't match real questions | Build templates from actual analyst questions, not theory |
| Over-engineering before proving value | Each phase must deliver standalone value |
| LLM costs for multi-hop reasoning | Use local LLM (Ollama) for template matching, reserve cloud for complex queries |

---

## 6. Success Metrics by Phase

| Phase | Metric | Target |
|-------|--------|--------|
| Phase 0 | Trade data validation | All 18 commodity groups match Census ±3% |
| Phase 1 | Graph query utility | Graph queries used in 5+ trade analyses |
| Phase 1.5 | International coverage | 4 new country data sources integrated |
| Phase 2 | Template coverage | 80% of routine questions match a template |
| Phase 3 | Context preservation | Downstream agents correctly use upstream context |

---

## 7. Immediate Next Steps

### This Week (Trade Data Focus)
1. Complete validation of canola, sunflower, and remaining commodity complexes
2. Document any cross-commodity patterns observed
3. Create a running list of "multi-hop questions" we encounter

### Next 2 Weeks (HATBA Prep)
1. Install Apache AGE extension on PostgreSQL
2. Create initial entity list from existing reference tables
3. Sketch relationship diagram for top 5 commodities

### Month 1 Milestone
- US Census trade data fully validated
- Knowledge graph design document complete
- First graph queries operational

---

## 8. Questions for Review

1. **Time allocation:** Is 60/30/10 (trade/HATBA/maintenance) realistic given business priorities?

2. **International trade priority:** Which countries are most critical after US?
   - EU (Eurostat) - major soybean meal importer
   - Brazil (Comex Stat) - competitor in exports
   - Canada (StatCan) - canola trade partner
   - China (GAC) - demand driver

3. **Template priorities:** Which analytical questions do you answer most frequently?
   - These become the first templates

4. **Phase 4 timing:** Is Q3 2026 acceptable for multi-hop reasoning, or is there business pressure to accelerate?

---

## Appendix: Trade Data Work Remaining

### US Census (Current)
| Commodity Group | Status | Validation |
|-----------------|--------|------------|
| SOYBEANS | ✅ Complete | Verified |
| SOYBEAN_MEAL | ✅ Complete | Verified |
| SOYBEAN_OIL | ✅ Complete | Verified |
| CORN | ✅ Data loaded | Needs validation |
| WHEAT | ✅ Data loaded | Needs validation |
| CANOLA | ✅ Data loaded | In progress |
| CANOLA_MEAL | ✅ Data loaded | Needs validation |
| CANOLA_OIL | ✅ Data loaded | Needs validation |
| SUNFLOWER | ✅ Data loaded | Needs validation |
| SUNFLOWER_MEAL | ✅ Data loaded | Needs validation |
| SUNFLOWER_OIL | ✅ Data loaded | Needs validation |
| DDGS | ✅ Data loaded | Needs validation |
| PALM_OIL | ✅ Data loaded | Needs validation |
| PALM_KERNEL_OIL | ✅ Data loaded | Needs validation |
| COTTONSEED | ✅ Data loaded | Needs validation |
| COTTONSEED_MEAL | ✅ Data loaded | Needs validation |
| CORN_OIL | ✅ Data loaded | Needs validation |
| CORN_GLUTEN | ✅ Data loaded | Needs validation |

### International (Planned)
| Country | Data Source | Priority | Complexity |
|---------|-------------|----------|------------|
| EU-27 | Eurostat Comext | High | Medium |
| Brazil | Comex Stat (MDIC) | High | Medium |
| Canada | StatCan CATSNET | Medium | Low |
| Argentina | INDEC | Medium | High |
| China | GAC | Low | High (access issues) |
| Australia | ABS | Low | Low |
