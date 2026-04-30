import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import text, inspect

def fetch_weather(engine):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 53.33,
        "longitude": 10,
        "hourly": "temperature_2m,precipitation,wind_speed_10m,visibility,weather_code",
        "timezone": "Europe/Berlin",
        "past_days": 2,
        "forecast_days": 1
    }
    try:
        print('Attempting fetching weather data')
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()["hourly"]

        if not data:
            print("Warning! Fetch returned empty list.")

        df = pd.DataFrame(data)
        df['time'] = pd.to_datetime(df['time'])

        with engine.begin() as connection:
            inspector = inspect(engine)
            if inspector.has_table("raw_weather", schema="raw"):
                connection.execute(text("TRUNCATE TABLE raw.raw_weather"))
            else:
                print("Table does not exist. Skipping truncate")
            df.to_sql(
                'raw_weather',
                connection,
                schema='raw',
                if_exists='append',
                index=False
            )
        
        print(f"Weather data loaded: {len(df)} records.")
    
    except requests.exceptions.HTTPError as errh:
        print(f"Fail: HTTP Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Fail: Connection Error: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Fail: Timeout Error: {errt}")
    except Exception as e:
        print(f"Fail: Unexpected Error: {e}")