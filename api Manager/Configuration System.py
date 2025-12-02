# config/api_sources.json
{
    "eia": {
        "name": "EIA Biodiesel Data",
        "enabled": true,
        "schedule": "daily",
        "url": "https://www.eia.gov/biofuels/biodiesel/production/",
        "tables": ["eia_feedstock", "eia_production"],
        "retry_attempts": 3,
        "timeout": 30
    },
    "gtt": {
        "name": "Global Trade Tracker",
        "enabled": true,
        "schedule": "weekly", 
        "auth_type": "token",
        "credentials": {
            "username_env": "GTT_USERNAME",
            "password_env": "GTT_PASSWORD"
        },
        "start_date": "2020-01",
        "end_date": "current",
        "tables": ["gtt"],
        "retry_attempts": 5,
        "rate_limit": 10
    },
    "lcfs": {
        "name": "LCFS California Data",
        "enabled": true,
        "schedule": "monthly",
        "url": "https://ww3.arb.ca.gov/fuels/lcfs/lrtqsummaries.htm",
        "tables": ["lcfs_feedstock", "lcfs_credit", "lcfs_deficit"],
        "retry_attempts": 3
    }
}