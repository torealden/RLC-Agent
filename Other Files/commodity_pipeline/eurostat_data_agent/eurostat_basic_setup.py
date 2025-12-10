import os
from dotenv import load_dotenv  # Assuming you have python-dotenv installed; if not, use os.environ directly
import requests
import json  # For parsing JSON
import pandas as pd  # For data processing (available in your env)

# Load .env file
load_dotenv()

# Get config from .env
enabled = os.getenv('EUROSTAT_ENABLED', 'false').lower() == 'true'
base_url = os.getenv('EUROSTAT_BASE_URL')
lang = os.getenv('EUROSTAT_LANG', 'EN')
format = os.getenv('EUROSTAT_FORMAT', 'JSON')

if not enabled:
    raise ValueError("Eurostat integration is disabled in .env")

# Example: Function to fetch data from Statistics API
def fetch_eurostat_data(dataset_code, filters=None):
    """
    Fetches data from Eurostat Statistics API.
    :param dataset_code: str, e.g., 'nama_10_gdp' for GDP data
    :param filters: dict, e.g., {'time': '2019', 'geo': 'FR'}
    :return: dict or pd.DataFrame
    """
    url = f"{base_url}/statistics/1.0/data/{dataset_code}"
    params = {'format': format, 'lang': lang}
    
    if filters:
        for key, value in filters.items():
            # Handle multiple values as list
            if isinstance(value, list):
                for v in value:
                    params[key] = v
            else:
                params[key] = value
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()  # JSON-stat format
        # Convert to DataFrame for easier handling (optional)
        # This is a simple conversion; use jsonstat.py lib for full JSON-stat parsing if needed
        df = pd.json_normalize(data)  # Adjust based on structure
        return df
    elif response.status_code == 413:  # Asynchronous
        print("Request is asynchronous. Poll the URL again later.")
        return None
    else:
        error = response.json().get('error', {})
        raise Exception(f"Error {response.status_code}: {error.get('label', 'Unknown error')}")

# Usage example: Fetch GDP data for France in 2019
try:
    data = fetch_eurostat_data('nama_10_gdp', filters={'time': '2019', 'geo': 'FR'})
    print(data.head())  # Or save to file/DB
except Exception as e:
    print(e)