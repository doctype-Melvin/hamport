import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv
import streamlit as st

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

    query_flights_weather = """
        SELECT
            *
        FROM analytics.mart_weather_impact
        ORDER BY 1 desc
    """

    df_weather = pd.read_sql(query_flights_weather, connect)

    query_all = """
        SELECT
            *
        FROM analytics.mart_flights
    """
    df_all_flights = pd.read_sql(query_all, connect)

    query_volume = """
        SELECT
            date,
            count(*) as flight_count
        FROM analytics.mart_flights
        WHERE date > '2026-04-30'
        GROUP BY 1
        ORDER BY 1
    """
    df_volume = pd.read_sql(query_volume, connect)
    df_volume['date'] = pd.to_datetime(df_volume['date']).dt.strftime('%Y-%m-%d')
    df_volume = df_volume.sort_values('date')

    query_delays = """
        SELECT
            direction,
            round(minutes_delay, 0) as "Delay Minutes",
            flight_id,
            airline,
            airline_group,
            airport_location
        FROM analytics.mart_flights
        WHERE minutes_delay > 0
        and time_actual is not null
        ORDER BY minutes_delay desc
        LIMIT 5
    """
    df_delays = pd.read_sql(query_delays, connect)

    query_weather_history = """
        SELECT
        *
        FROM analytics.fct_weather_history
        ORDER BY 2 desc
        LIMIT 7
    """
    df_weather_history = pd.read_sql(query_weather_history, connect)
    
    query_airport_stats = """
        SELECT
        *
        FROM analytics.mart_airport_hourly_stats
    """
    df_airport_stats = pd.read_sql(query_airport_stats, connect)

    query_airlines_stats = """
        SELECT
        *
        FROM analytics.mart_airlines_stats
    """

    df_airlines_stats = pd.read_sql(query_airlines_stats, connect)

    connect.close()
    return (
        df_weather,
        df_all_flights,
        df_volume,
        df_delays,
        df_weather_history,
        df_airport_stats,
        df_airlines_stats
        )