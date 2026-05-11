import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from src.loader import get_data
from src.charts import barchart
from src.metrix import bans



st.set_page_config(page_title="Hamburg Airport Data", layout="wide")

st.title("Flights and Weather Data")
st.write("Helmut Schmidt Airport - Hamburg-Fuhlsbüttel")

try:
    (
        df_weather,
        df_all_flights,
        df_volume,
        df_delays,
        df_weather_history,
        df_airport_stats,
        df_airlines_stats
    ) = get_data()
    
    today = df_weather_history.weather_at[0].strftime("%d.%m.%Y")
    condition = df_weather_history.condition[0]
    col1, col2 = st.columns(2)

    with col1:
        st.header(f"Date: {today}")
    with col2:
        st.header(f"Condition: {condition}")

    st.write('Flight Volume by Date')
    st.bar_chart(
        data=df_volume,
        x='date',
        x_label='Date',
        y='flight_count',
        y_label='Count'
    )
    
    st.write('Airline stats')
    data_top5_airlines = df_airlines_stats.sort_values('number_of_flights', ascending=False).head(5)
    data_busy_hours = df_airport_stats.sort_values('number_of_flights', ascending=False).head(5)
    col1, col2 = st.columns(2)
    with col1:
        st.bar_chart(
            data=data_top5_airlines,
            x='airline_group',
            x_label='Airline Group',
            y='number_of_flights',
            y_label='Flights',
            horizontal=True,
            sort='-number_of_flights'
        )
    with col2:
        st.bar_chart(
            data=data_busy_hours,
            x='planned_hour',
            x_label='Hour of Day',
            y='number_of_flights',
            y_label='Flights',
            horizontal=True,
            sort='-number_of_flights'
        )



    
    st.write('Delays in current time window')
    st.table(df_delays)
except Exception as e:
    st.error(f"Connection failed: {e}")