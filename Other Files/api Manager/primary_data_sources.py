# Core data sources configuration
DATA_SOURCES = {
    'usda_ams': {
        'base_url': 'https://marsapi.ams.usda.gov/services/v1.2/',
        'priority': 1,
        'frequency': 'daily',
        'commodities': ['corn', 'soybeans', 'wheat', 'sorghum', 'oats', 'barley'],
        'reports': [
            {'id': '2849', 'name': 'Daily Grain Review'},
            {'id': '3617', 'name': 'Daily Ethanol Report'},
            {'id': 'sunflower_canola', 'name': 'Daily Oilseed Report'}
        ]
    },
    'census_bureau': {
        'base_url': 'https://api.census.gov/data/',
        'priority': 2,
        'frequency': 'monthly',
        'datasets': ['timeseries/intltrade/exports', 'timeseries/intltrade/imports']
    }
}