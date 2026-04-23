from sqlalchemy import create_engine
from loaders.airport_api import fetch_airport
from loaders.weather_api import fetch_weather
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("SUPA_URL")
engine = create_engine(DB_URL)

def run_fetch():
    print("Starting to fetch airport and weather data")

    try: 

        for direction in ['arrivals', 'departures']:
            fetch_airport(direction, engine)
        
        fetch_weather(engine)

        print(" SUCCESS! ")

    except Exception as e:
        print(f"Fail: Unexpected Error: {e}")

if __name__ == "__main__":
    run_fetch()