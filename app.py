import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from src.loader import get_data


st.set_page_config(page_title="Hamburg Airport Data", layout="wide")

st.title("Hamburg Airport Flights Data")



try:
    df_weather, df_all_flights, df_volume, df_delays = get_data()
    st.write('### Preview of Flights Table')
    st.dataframe(df_weather)
    st.subheader('Flight Volume by Hour')
    st.line_chart(data=df_volume, x='hour', y='flight_count')
    st.subheader('Delays in current time window')
    st.table(df_delays)
except Exception as e:
    st.error(f"Connection failed: {e}")