import requests
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB

load_dotenv()

# Credentials config
API_KEY = os.getenv("HAM_API_KEY")
SUPABASE = os.getenv("SUPA_URL")

HEADERS = {
    "Ocp-Apim-Subscription-Key": API_KEY
}
DB_URL = SUPABASE

def fetch_and_load(endpoint):
    URL = f"https://rest.api.hamburg-airport.de/v2/flights/{endpoint}"
    try:
        print(f"Attempting fetch {endpoint} from HAM API...")
        response = requests.get(URL, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data:
            print("Warning: Fetch returned empty list!")
            return

        df = pd.DataFrame(data)
        print(f"Success: Fetched {len(df)} records")

        table_name = f"raw_{endpoint}"
        engine = create_engine(DB_URL)

        with engine.begin() as connection:
            connection.execute(text(f"TRUNCATE TABLE raw.{table_name}"))

        df.to_sql(
            table_name,
            engine,
            schema='raw',
            if_exists='append',
            index=False
        )

        print(f"Success: Data loaded to schema {table_name}")
    
    except requests.exceptions.HTTPError as errh:
        print(f"Fail: HTTP Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Fail: Connection Error: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Fail: Timeout Error: {errt}")
    except Exception as e:
        print(f"Fail: Unexpected Error: {e}")

if __name__ == "__main__":
    for direction in ['arrivals', 'departures']:
        fetch_and_load(direction)