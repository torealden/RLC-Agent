def fetch_sdmx_data(flow_ref, filters=None):
    url = f"{base_url}/sdmx/2.1/data/{flow_ref}"
    params = {'format': 'JSON'}  # Or 'SDMX-ML'
    if filters:
        # SDMX filters use dot notation or specific syntax; adjust accordingly
        key = '.'.join([f"{k}={v}" for k, v in filters.items()]) if filters else ''
        url = f"{url}/{key}"
    
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()  # Or parse XML if using SDMX-ML
    else:
        raise Exception(f"Error {response.status_code}: {response.text}")

# Example: Fetch annual data for France, Luxembourg, Germany
fetch_sdmx_data('A.....FR+LU+DE')