import requests
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB

load_dotenv()

# Credentials config
API_KEY = os.getenv("HAM_API_KEY")
PASS = os.getenv("SUPAPASS")
HAM_API_URL = "https://api.hamburg-airport.de/v2/flights/arrivals"
HEADERS = {
    "Ocp-Apim-Subscription-Key": API_KEY
}
DB_URL = "postgresql://postgres.pzvirfmbpttqkxzbltgd:PASS@aws-1-eu-central-1.pooler.supabase.com:6543/postgres"

def fetch_and_load():
    try:
        print("Attempting fetch from HAM API...")
        response = requests.get(HAM_API_URL, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data:
            print("Warning: Fetch returned empty list!")
            return

        df = pd.DataFrame(data)
        print(f"Success: Fetched {len(df)} records")

        engine = create_engine(DB_URL)

        df.to_sql(
            'raw_arrivals',
            engine,
            schema='raw',
            if_exists='replace',
            index=False
        )

        print("Success: Data loaded to schema 'raw'")
    
    except requests.exceptions.HTTPError as errh:
        print(f"Fail: HTTP Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Fail: Connection Error: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Fail: Timeout Error: {errt}")
    except Exception as e:
        print(f"Fail: Unexpected Error: {e}")

if __name__ == "__main__":
    fetch_and_load