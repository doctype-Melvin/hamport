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
        FROM analytics.analysis_weather_impact
        ORDER BY 1 desc
    """

    df_weather = pd.read_sql(query_flights_weather, connect)

    query_all = "SELECT * FROM analytics.fct_flights"
    df_all_flights = pd.read_sql(query_all, connect)

    query_volume = """
        SELECT
            date_trunc('hour', planned_time) as hour,
            count(*) as flight_count
        FROM analytics.fct_flights
        GROUP BY 1
        ORDER BY 1
    """
    df_volume = pd.read_sql(query_volume, connect)

    query_delays = """
        SELECT
            delay_minutes,
            airline,
            flight_id,
            airport_location,
            direction
        FROM analytics.fct_flights
        WHERE delay_minutes > 0
        and actual_time is not null
        ORDER BY delay_minutes desc
        LIMIT 10
    """
    df_delays = pd.read_sql(query_delays, connect)

    connect.close()
    return df_weather, df_all_flights, df_volume, df_delays