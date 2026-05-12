import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv
import streamlit as st
import src.queries as q

load_dotenv()
PASS = os.getenv("SUPA_PASS")
HOST = os.getenv("SUPA_HOST")
USR = os.getenv("SUPA_USER")

@st.cache_data
def get_data():
    connect = psycopg2.connect(
        host=HOST,
        port="6543",
        database="postgres",
        user=USR,
        password=PASS
    )

    # mart_weather_impact
    df_weather = pd.read_sql(q.FLIGHTS_WEATHER, connect)

    #mart_flights
    df_all_flights = pd.read_sql(q.ALL_FLIGHTS, connect)

    #mart_flights
    df_volume = pd.read_sql(q.FLIGHTS_VOLUME, connect)
    #transform date into ordinal label
    df_volume['date'] = pd.to_datetime(df_volume['date']).dt.strftime('%Y-%m-%d')
    df_volume = df_volume.sort_values('date')

    
    df_delays_current = df_all_flights.sort_values('minutes_delay', ascending=False).head(5)

    df_delays_date = pd.read_sql(q.AVG_DELAY_DATE, connect)

    # fct_weather_history
    df_weather_history = pd.read_sql(q.WEATHER_HISTORY, connect)
    
    # mart_airport_hourly_stats
    df_airport_stats = pd.read_sql(q.AIRPORT_STATS, connect)

    # mart_airlines_stats
    df_airlines_stats = pd.read_sql(q.AIRLINES_STATS, connect)

    # mart_airport_delays
    df_airport_delays = pd.read_sql(q.ORIGIN_DELAYS, connect)

    # mart_data_quality
    df_data_quality = pd.read_sql(q.DATA_QUALITY, connect)

    connect.close()
    return (
        df_weather,
        df_all_flights,
        df_volume,
        df_delays_current,
        df_delays_date,
        df_weather_history,
        df_airport_stats,
        df_airlines_stats,
        df_airport_delays,
        df_data_quality
        )