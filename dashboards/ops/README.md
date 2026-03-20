# RLC Operations Dashboard

System health monitoring dashboard for RLC-Agent data collection.

## Setup (One-Time)

1. **Install Python dependencies** (from repo root):
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure database connection**:
   ```bash
   cd dashboards/ops
   cp .env.example .env
   ```
   Then edit `.env` with the cloud database credentials.

3. **Run the dashboard**:
   ```bash
   streamlit run dashboards/ops/app.py
   ```
   Or double-click `scripts/launch_data_dashboard.bat`

## What You'll See

- **Health Score** - Overall system status (0-100)
- **Data Freshness** - When each data source was last collected
- **Active Alerts** - Unacknowledged warnings/errors
- **Recent Failures** - Collection errors from the last 7 days
- **Collection Trends** - Success/failure rates over 30 days
- **Schedule Overview** - Expected collection times for each source
