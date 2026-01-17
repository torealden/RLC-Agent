# RLC-Agent: Incomplete Tasks Summary

**Generated**: January 2026
**Total Tasks**: 54
**Source**: Codebase analysis of TODOs, planning docs, and registry gaps

---

## Quick Stats

| Category | Count | Critical |
|----------|-------|----------|
| BioTrack Rail Monitoring | 6 | 1 |
| Data Collectors | 13 | 2 |
| Agent Architecture | 7 | 0 |
| Runbooks/Automation | 6 | 0 |
| System Integration | 7 | 0 |
| Forecast Tracker | 4 | 0 |
| File Organization | 5 | 1 |
| Main Application | 9 | 0 |

---

## CRITICAL / URGENT Items

| Task | Category | Why Critical |
|------|----------|--------------|
| Rotate API keys in .env | Security | Keys exposed in api Manager folder |
| Build USDA WASDE Collector | Data | Key monthly S&D data for all reports |
| Build NOPA US Soy Crush Collector | Data | Critical gap for soybean analysis |

---

## 1. BioTrack Rail Monitoring

The rail car tracking system is in **simulation mode only**.

| Task | Priority | Status |
|------|----------|--------|
| **Test rail car tracker with real data** | High | Not Started |
| Satellite Integration (Sentinel-2) | Medium | Planned Phase 2 |
| IoT Sensor Integration | Medium | Planned Phase 2 |
| Multi-camera Fusion | Medium | Planned Phase 2 |
| Predictive Volume Models | Low | Planned Phase 3 |
| Anomaly Detection | Low | Planned Phase 3 |

**Source**: `biotrack/` directory

---

## 2. Data Collectors - Missing

### Critical Priority
| Data Source | Description | Impact |
|-------------|-------------|--------|
| **USDA WASDE** | Monthly supply/demand balances | Required for HB reports |
| **NOPA US Soy Crush** | Monthly crush data | Critical for soy analysis |
| **CME Futures Prices** | Daily settlement prices | Core price data |
| **UN Comtrade** | Global trade statistics | Trade flow analysis |

### Regional Data (Medium Priority)
| Data Source | Region |
|-------------|--------|
| USDA ERS Wheat Yearbook | US |
| Eurostat EU Ag Data | Europe |
| EU MARS Bulletin | Europe |
| Russia SovEcon | Russia |
| China GACC Imports | China |
| Australia ABARES | Australia |
| Ukraine Ministry Ag | Ukraine |
| Argentina Rosario Exchange | South America |
| Indonesia GAPKI | Southeast Asia |

**Source**: `docs/DATA_SOURCE_REGISTRY.md`

---

## 3. Agent Architecture

These agents are **designed but not implemented**.

| Agent | Purpose | Priority |
|-------|---------|----------|
| **Master Scheduler** | Automated collection scheduling | High |
| **Analyst Agent** | Central orchestrator | High |
| **Data Checker Agent** | Source vs DB validation | High |
| **Tool Registry** | Modular tool access | High |
| Trade Flow Analysis Agent | Trade pattern analysis | Medium |
| Price Analysis Agent | Price/spread analysis | Medium |
| Reporting Team Agents | Automated report generation | Medium |

**Source**: `docs/ARCHITECTURE_PLAN.md`

---

## 4. Runbooks / HB Report Automation

Data integrations documented but not connected:

| Integration | Data Type | Priority |
|-------------|-----------|----------|
| USDA WASDE → HB Report | Monthly S&D | High |
| USDA Export Sales → HB Report | Weekly exports | High |
| NOPA Crush → HB Report | Monthly crush | High |
| CME Futures → HB Report | Daily prices | High |
| USDA Crop Progress → HB Report | Planting/harvest | Medium |
| CFTC COT Reports → HB Report | Positioning | Medium |

**Source**: `docs/HB_REPORT_AUTOMATION_GUIDE.md`

---

## 5. System Integration

Three isolated systems need to be connected:

| System | Lines of Code | Status |
|--------|---------------|--------|
| rlc_master_agent/ | 5,500 | Isolated |
| deployment/ | 2,747 | Isolated |
| commodity_pipeline/ | 42,407 | **Disconnected** |

### Integration Tasks
| Task | Priority | Status |
|------|----------|--------|
| Connect Notion memory integration | High | In Progress |
| Connect 25 data collectors to tool registry | High | Not Started |
| Integrate rlc_master_agent with commodity_pipeline | High | Not Started |
| Add report generation tools | Medium | Not Started |
| Add trading/analysis tools | Medium | Not Started |
| Expand tools from 17 to ~50 | Medium | Not Started |
| Clean up 100+ duplicate files | Low | Not Started |

**Source**: `docs/CONSOLIDATION_PLAN.md`

---

## 6. Forecast Tracker

| Task | Priority | Status |
|------|----------|--------|
| Implement forecast bias analysis | Medium | Documented Only |
| Implement automated improvement suggestions | Medium | Not Implemented |
| Implement PowerBI integration | Medium | Not Implemented |
| Implement website display integration | Low | Not Implemented |

**Source**: `deployment/forecast_tracker.py`

---

## 7. File Organization & Cleanup

| Task | Priority | Status |
|------|----------|--------|
| **Rotate API keys in .env** | **URGENT** | Security Issue |
| Create archive/ directory | Medium | Not Started |
| Move debug scripts to archive | Medium | Not Started |
| Configure Dropbox selective sync | Low | Not Started |
| Remove duplicate files | Medium | Not Started |

**Source**: `docs/FILE_ORGANIZATION_PLAN.md`

---

## 8. Main Application TODOs

Stub implementations in `src/main.py`:

| Line | TODO | Priority |
|------|------|----------|
| 94 | Load from config file | High |
| 134 | Implement data collection using collectors | High |
| 143 | Use master scheduler for collection | High |
| 178 | Implement market analysis using agents | High |
| 204 | Implement price forecasting | High |
| 235 | Use HB report orchestrator | High |
| 255 | Implement reporting agents | Medium |
| 288 | Implement publishing agents | Medium |
| 318 | Implement RAG query system | High |
| 338 | Implement master scheduler | High |
| 404 | Implement REPL interface | Low |

---

## Recommended Priority Order

### Immediate (Do First)
1. Rotate exposed API keys (SECURITY)
2. Build USDA WASDE collector
3. Build NOPA Crush collector
4. Test BioTrack rail tracker with real data

### Short Term
5. Build CME Futures collector
6. Implement Master Scheduler
7. Connect Notion memory integration
8. Integrate rlc_master_agent with commodity_pipeline

### Medium Term
9. Build remaining regional data collectors
10. Implement Analyst Agent and Tool Registry
11. Connect data collectors to tool registry
12. Implement HB Report data integrations

### Long Term
13. Implement forecast tracking features
14. Build out remaining analysis agents
15. File organization and cleanup
16. Implement REPL and web interfaces

---

## Files Referenced

- `docs/ARCHITECTURE_PLAN.md` - Agent design
- `docs/CONSOLIDATION_PLAN.md` - Integration roadmap
- `docs/DATA_SOURCE_REGISTRY.md` - Data source audit
- `docs/FILE_ORGANIZATION_PLAN.md` - Cleanup tasks
- `docs/HB_REPORT_AUTOMATION_GUIDE.md` - Report automation
- `deployment/forecast_tracker.py` - Forecast system
- `src/main.py` - Main application TODOs
- `biotrack/` - Rail monitoring system
