import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
PASS = os.getenv("SUPA_PASS")
HOST = os.getenv("SUPA_HOST")
USR = os.getenv("SUPA_USER")

st.set_page_config(page_title="Hamburg Airport Data", layout="wide")

st.title("Hamburg Airport Flights Data")

def get_data():
    connect = psycopg2.connect(
        host=HOST,
        port="6543",
        database="postgres",
        user=USR,
        password=PASS
    )
    query_test = "SELECT * FROM analytics.fct_flights LIMIT 10"
    df_test = pd.read_sql(query_test, connect)

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
        WHERE delay_minutes > 5
        and actual_time is not null
        ORDER BY delay_minutes desc
        LIMIT 10
    """
    df_delays = pd.read_sql(query_delays, connect)

    connect.close()
    return df_test, df_volume, df_delays

try:
    df_test, df_volume, df_delays = get_data()
    st.write('### Preview of Flights Table')
    st.dataframe(df_test)
    st.subheader('Flight Volume by Hour')
    st.line_chart(data=df_volume, x='hour', y='flight_count')
    st.subheader('Delays in current time window')
    st.table(df_delays)
except Exception as e:
    st.error(f"Connection failed: {e}")