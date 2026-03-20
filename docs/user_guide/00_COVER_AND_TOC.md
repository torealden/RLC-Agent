# RLC Commodities Intelligence Platform

## User Guide

**Version 1.0**
**March 2026**

---

```
 ____  _     ____    ____                                    _ _ _   _
|  _ \| |   / ___|  / ___|___  _ __ ___  _ __ ___   ___   __| (_) |_(_) ___  ___
| |_) | |  | |     | |   / _ \| '_ ` _ \| '_ ` _ \ / _ \ / _` | | __| |/ _ \/ __|
|  _ <| |__| |___  | |__| (_) | | | | | | | | | | | (_) | (_| | | |_| |  __/\__ \
|_| \_\_____\____|  \____\___/|_| |_| |_|_| |_| |_|\___/ \__,_|_|\__|_|\___||___/

```

**Round Lakes Commodities**
*Automated Market Intelligence*

---

## About This Guide

This guide provides comprehensive instructions for installing, configuring, and operating the RLC Commodities Intelligence Platform. It is intended for analysts, developers, and administrators who will be working with the system.

**Intended Audience:**
- Data analysts accessing commodity data
- Developers extending the platform
- Administrators managing the system

**Prerequisites:**
- Basic familiarity with databases (SQL)
- Basic command line knowledge
- Access to the RLC cloud database

---

## Table of Contents

### Part 1: Getting Started
- [1.1 Platform Overview](01_GETTING_STARTED.md#11-platform-overview)
- [1.2 System Architecture](01_GETTING_STARTED.md#12-system-architecture)
- [1.3 Installation](01_GETTING_STARTED.md#13-installation)
- [1.4 Configuration](01_GETTING_STARTED.md#14-configuration)
- [1.5 Verifying Your Setup](01_GETTING_STARTED.md#15-verifying-your-setup)

### Part 2: Understanding the Data
- [2.1 Data Sources Overview](02_UNDERSTANDING_DATA.md#21-data-sources-overview)
- [2.2 The Medallion Architecture](02_UNDERSTANDING_DATA.md#22-the-medallion-architecture)
- [2.3 Database Schema Reference](02_UNDERSTANDING_DATA.md#23-database-schema-reference)
- [2.4 Key Tables and Views](02_UNDERSTANDING_DATA.md#24-key-tables-and-views)

### Part 3: Daily Operations
- [3.1 The Operations Dashboard](03_DAILY_OPERATIONS.md#31-the-operations-dashboard)
- [3.2 Running Data Collections](03_DAILY_OPERATIONS.md#32-running-data-collections)
- [3.3 Monitoring Data Quality](03_DAILY_OPERATIONS.md#33-monitoring-data-quality)
- [3.4 Troubleshooting Common Issues](03_DAILY_OPERATIONS.md#34-troubleshooting-common-issues)

### Part 4: Working with Power BI
- [4.1 Connecting to the Database](04_POWER_BI.md#41-connecting-to-the-database)
- [4.2 Available Data Tables](04_POWER_BI.md#42-available-data-tables)
- [4.3 Building Dashboards](04_POWER_BI.md#43-building-dashboards)
- [4.4 Template Dashboards](04_POWER_BI.md#44-template-dashboards)

### Part 5: Adding New Data Sources
- [5.1 Planning a New Collector](05_ADDING_DATA_SOURCES.md#51-planning-a-new-collector)
- [5.2 Writing a Collector](05_ADDING_DATA_SOURCES.md#52-writing-a-collector)
- [5.3 Database Schema Updates](05_ADDING_DATA_SOURCES.md#53-database-schema-updates)
- [5.4 Creating Gold Views](05_ADDING_DATA_SOURCES.md#54-creating-gold-views)
- [5.5 Testing and Validation](05_ADDING_DATA_SOURCES.md#55-testing-and-validation)

### Part 6: Working with the LLM
- [6.1 LLM Capabilities Overview](06_LLM_INTEGRATION.md#61-llm-capabilities-overview)
- [6.2 Querying Data with Natural Language](06_LLM_INTEGRATION.md#62-querying-data-with-natural-language)
- [6.3 Report Generation](06_LLM_INTEGRATION.md#63-report-generation)
- [6.4 Best Practices](06_LLM_INTEGRATION.md#64-best-practices)

### Appendices
- [A. File List for New Users](APPENDIX_A_FILE_LIST.md)
- [B. API Key Registration](APPENDIX_B_API_KEYS.md)
- [C. Database Quick Reference](APPENDIX_C_DATABASE_REFERENCE.md)
- [D. Troubleshooting Guide](APPENDIX_D_TROUBLESHOOTING.md)
- [E. Graphic Specifications](APPENDIX_E_GRAPHICS.md)

---

## Document Conventions

| Convention | Meaning |
|------------|---------|
| `monospace` | Code, commands, file paths, or database objects |
| **Bold** | Important terms or UI elements |
| *Italic* | Emphasis or first use of a term |
| `>` | Menu navigation (e.g., File > Open) |
| ⚠️ | Warning or important note |
| 💡 | Tip or best practice |

---

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | March 2026 | RLC Team | Initial release |

---

*For questions or support, contact the RLC data team.*
