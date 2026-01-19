# RLC-Agent File Organization Plan
## Cleanup and Archive Recommendations

---

## Current State Assessment

The project has grown organically. Here's a suggested restructuring:

---

## Proposed Directory Structure

```
RLC-Agent/
│
├── README.md                      # Project overview
├── requirements.txt               # Python dependencies
├── .env.example                   # Environment template
│
├── deployment/                    # Core tools (KEEP)
│   ├── agent_tools.py             # LLM tool definitions
│   ├── balance_sheet_extractor.py # Original extractor
│   ├── document_rag.py            # RAG system
│   ├── fast_extractor.py          # Pandas extractor (PRIMARY)
│   ├── forecast_tracker.py        # Forecast accuracy system
│   ├── powerbi_export.py          # PowerBI data export
│   ├── start_agent.py             # Agent launcher
│   └── usda_api.py                # USDA data integration
│
├── biotrack/                      # BioTrack AI (NEW)
│   ├── biotrack_ai.py             # Main system
│   ├── dashboard.py               # Streamlit dashboard
│   ├── requirements.txt           # BioTrack dependencies
│   ├── README.md                  # BioTrack documentation
│   ├── config/
│   │   └── facilities.csv         # Facility database
│   ├── data/                      # BioTrack data (gitignored)
│   └── models/                    # Trained models (gitignored)
│
├── data/                          # Data files (GITIGNORED)
│   ├── rlc_commodities.db         # Main database
│   ├── powerbi_exports/           # Export files
│   └── forecast_reports/          # Generated reports
│
├── docs/                          # Documentation (KEEP)
│   ├── BALANCE_SHEET_KNOWLEDGE.md
│   ├── CREDENTIALS_REQUIRED.md
│   ├── DATA_SOURCE_REGISTRY.md
│   ├── FILE_ORGANIZATION_PLAN.md
│   ├── FORECAST_TRACKING_GUIDE.md
│   ├── LLM_TRAINING_STRATEGY.md
│   ├── POWERBI_DASHBOARD_GUIDE.md
│   ├── POWERBI_ODBC_SETUP.md
│   ├── SCREEN_RECORDING_FOR_LLM.md
│   └── VISUALIZATION_INSPIRATION.md
│
├── archive/                       # Old/deprecated files (NEW)
│   ├── old_scripts/
│   ├── experiments/
│   └── README.md                  # What's archived and why
│
└── tests/                         # Test files
    └── test_*.py
```

---

## Files to Archive

### Move to `archive/old_scripts/`

| File | Reason |
|------|--------|
| `test_year_v2.py` | One-time debugging script |
| `test_year_detection.py` | One-time debugging script |
| Any `*_backup.py` | Superseded versions |
| Any `*_old.py` | Superseded versions |

### Move to `archive/experiments/`

| File | Reason |
|------|--------|
| Jupyter notebooks (if any) | Exploratory work |
| One-off analysis scripts | Not part of core system |

---

## Files to Delete (After Review)

| Pattern | Action |
|---------|--------|
| `*.pyc` | Delete (compiled Python) |
| `__pycache__/` | Delete (cached) |
| `.DS_Store` | Delete (macOS artifacts) |
| `Thumbs.db` | Delete (Windows artifacts) |
| `*.log` (old) | Archive or delete |
| Duplicate data files | Keep one, delete rest |

---

## Dropbox-Specific Recommendations

Since you're using Dropbox sync:

### Exclude from Sync (Large/Generated Files)

Add to `.dropboxignore` or selective sync settings:
```
data/rlc_commodities.db          # Large database
data/powerbi_exports/*.xlsx      # Generated exports
biotrack/data/frames/            # Captured frames
biotrack/models/                 # ML models (large)
venv/                            # Virtual environment
__pycache__/                     # Python cache
.git/                            # Git internals
```

### Keep in Sync
```
deployment/                      # Core code
docs/                            # Documentation
biotrack/*.py                    # BioTrack code
biotrack/config/                 # Configuration
*.md                             # Markdown docs
```

---

## Git Recommendations

### Update `.gitignore`

```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
venv/
.env

# Data (large files)
*.db
*.sqlite
data/
biotrack/data/
biotrack/models/

# Exports
*.xlsx
*.csv
!biotrack/config/*.csv  # Allow config CSVs

# OS
.DS_Store
Thumbs.db

# IDE
.idea/
.vscode/
*.swp

# Logs
*.log
logs/

# Archive (optional - might want in git for history)
# archive/
```

### Large File Handling

For the database and models, consider:

1. **Git LFS** (Large File Storage)
   ```bash
   git lfs install
   git lfs track "*.db"
   git lfs track "biotrack/models/*.pt"
   ```

2. **Separate data repo** - Keep code and data in different repos

3. **Cloud storage** - Keep data in cloud (S3, GCS) with download scripts

---

## Cleanup Commands

### Find large files
```bash
find . -size +10M -type f | head -20
```

### Find duplicate files
```bash
fdupes -r .
```

### Remove Python cache
```bash
find . -type d -name __pycache__ -exec rm -rf {} +
find . -type f -name "*.pyc" -delete
```

### List files by modification date (find stale files)
```bash
find . -type f -mtime +90 -name "*.py" | head -20
```

---

## Documentation Consolidation

Current docs are good! Suggested consolidation:

| Keep As-Is | Why |
|------------|-----|
| POWERBI_ODBC_SETUP.md | Primary reference |
| FORECAST_TRACKING_GUIDE.md | Complete system doc |
| LLM_TRAINING_STRATEGY.md | Strategic roadmap |
| BALANCE_SHEET_KNOWLEDGE.md | Domain knowledge |
| VISUALIZATION_INSPIRATION.md | Design reference |
| SCREEN_RECORDING_FOR_LLM.md | Process guide |

| Consider Merging | Into |
|------------------|------|
| POWERBI_DASHBOARD_GUIDE.md | POWERBI_ODBC_SETUP.md |
| CONSOLIDATION_PLAN.md | This file |

---

## Action Items

### Immediate (Do Now)
- [ ] Create `archive/` directory
- [ ] Move debug scripts to archive
- [ ] Update `.gitignore`
- [ ] Remove `__pycache__` directories

### Short-term (This Week)
- [ ] Configure Dropbox selective sync
- [ ] Review and delete duplicate files
- [ ] Consolidate redundant docs

### Long-term (When Ready)
- [ ] Consider Git LFS for large files
- [ ] Set up automated backups for database
- [ ] Create data refresh scripts

---

## Maintenance Schedule

| Task | Frequency |
|------|-----------|
| Clear Python cache | Weekly |
| Archive old scripts | Monthly |
| Review large files | Monthly |
| Backup database | Weekly |
| Update documentation | As needed |

---

*Organization creates clarity. Clarity enables productivity.*
