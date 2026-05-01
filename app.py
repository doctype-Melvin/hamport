import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from src.loader import get_data
from src.charts import linechart
from src.metrix import bans



st.set_page_config(page_title="Hamburg Airport Data", layout="wide")

st.title("Flights and Weather Data")
st.write("Helmut Schmidt Airport - Hamburg-Fuhlsbüttel")



try:
    df_weather, df_all_flights, df_volume, df_delays, df_weather_history = get_data()
    
    today = df_weather_history.weather_at[0].strftime("%d.%m.%Y")
    condition = df_weather_history.condition[0]
    col1, col2 = st.columns(2)
    with col1:
        st.header(f"Date: {today}")
    with col2:
        st.header(f"Condition: {condition}")

    st.write('Flight Volume by Hour')
    linechart(df_volume)

    st.write('Weather and Delays')
    st.table(df_weather)
    
    st.write('Delays in current time window')
    st.table(df_delays)
except Exception as e:
    st.error(f"Connection failed: {e}")