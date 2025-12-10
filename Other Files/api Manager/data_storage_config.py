# Storage configuration
STORAGE_CONFIG = {
    'local_development': {
        'database': 'sqlite',
        'path': './data/rlc_commodities.db',
        'file_storage': './data/lake/'
    },
    'production': {
        'database': 'postgresql',
        'host': 'rlc-postgres.amazonaws.com',
        'data_lake': 's3://rlc-data-lake/',
        'data_warehouse': 'redshift://rlc-warehouse'
    }
}