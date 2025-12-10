import requests
import psycopg2
from datetime import datetime
import json

# API configuration
API_KEY = "your_usda_ams_api_key"  # Replace with your actual API key
SLUG_ID = "3192"  # Confirmed for Illinois Grain Bids (includes central Illinois corn prices)
START_DATE = "01/01/2020"  # mm/dd/yyyy format
END_DATE = "07/01/2025"   # mm/dd/yyyy format
BASE_URL = f"https://marsapi.ams.usda.gov/services/v1.2/reports/{SLUG_ID}"

# PostgreSQL configuration
DB_CONFIG = {
    "dbname": "your_database_name",  # Replace with your PostgreSQL database name
    "user": "your_username",         # Replace with your PostgreSQL username
    "password": "your_password",     # Replace with your PostgreSQL password
    "host": "localhost",             # Assuming local setup; change if remote
    "port": "5432"                   # Default PostgreSQL port
}

def fetch_corn_prices(start_date, end_date):
    # Build the 'q' parameter for date range filtering
    q_param = f"report_begin_date={start_date}:{end_date}"
    params = {
        "q": q_param,
        "allSections": "true"  # Ensures full report sections are returned
    }
    
    try:
        # Use basic authentication (API key as username, empty password)
        response = requests.get(BASE_URL, auth=(API_KEY, ''), params=params)
        response.raise_for_status()  # Raises an error for non-200 status codes
        data = response.json()
        return data.get("results", [])  # Return the results list; assume no pagination for now
    except requests.exceptions.RequestException as e:
        raise Exception(f"API request failed: {e} - {response.text if 'response' in locals() else ''}")

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS corn_prices (
                id SERIAL PRIMARY KEY,
                slug_id VARCHAR(10),
                report_date DATE,
                published_date TIMESTAMP,
                price_data JSONB,  -- Stores full JSON for flexibility (e.g., prices, locations)
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

def insert_data(conn, data):
    with conn.cursor() as cur:
        for item in data:
            # Parse dates; adjust format based on API response (e.g., "%m/%d/%Y")
            report_date = datetime.strptime(item.get("report_date", ""), "%m/%d/%Y").date() if item.get("report_date") else None
            published_date = datetime.strptime(item.get("published_date", ""), "%m/%d/%Y %H:%M:%S") if item.get("published_date") else None
            cur.execute("""
                INSERT INTO corn_prices (slug_id, report_date, published_date, price_data)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING;  -- Avoid duplicates if re-running
            """, (
                item.get("slug_id"),
                report_date,
                published_date,
                json.dumps(item)  # Store full item as JSON for all details
            ))
        conn.commit()

def main():
    print(f"Fetching corn prices for slug_id {SLUG_ID} from {START_DATE} to {END_DATE}...")
    data = fetch_corn_prices(START_DATE, END_DATE)
    print(f"Fetched {len(data)} records.")

    # Connect to PostgreSQL and store data
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        create_table(conn)
        print("Inserting data into PostgreSQL...")
        insert_data(conn, data)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()