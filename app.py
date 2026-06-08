import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from src.loader import get_data
from src.charts import barchart
from src.metrix import bans

st.set_page_config(page_title="Hamburg Airport Data", page_icon="✈", layout="wide")

if "data" not in st.session_state:
    airport_data = get_data()

    if airport_data is not None:
        st.session_state["data"] = airport_data
        st.session_state["fetch_error"] = False
    else:
        st.session_state["data"] = None
        st.session_state["fetch_error"] = True

if st.session_state.get("fetch_error", True) or st.session_state["data"] is None:
    st.error("⚠️ Unable to load flight data right now. Please check your database connection or try again later.")
else:
    d = st.session_state["data"]

today = d["weather_history"].weather_at[0].strftime("%d.%m.%Y")
condition = d["weather_history"].condition[0]
metrics_dict = d["data_quality"].to_dict('records')

col1, col2 = st.columns(2)

with col1:
    st.markdown(f"{today} - Conditions in Hamburg: :yellow[{condition}]")
#     st.markdown('''
#     This report visualizes information pulled from :red[Hamburg Airport Open API]  
#                 and :blue[open-meteo.com].
# ''')
with col2:
    st.subheader('Data Quality assessment', text_alignment='center')
    cols = st.columns(len(metrics_dict))
    for i, row in enumerate(metrics_dict):
        direction = row['direction']
        total = row['scheduled_flights']
        tracked = row['tracked_flights']
        pct = row['completeness_pct']

        cols[i].metric(
            label=f"{direction}s",
            value=f"{pct}%",
            delta=f"{tracked} tracked flights",
            delta_arrow="off",
            delta_color="normal" if pct > 80 else "inverse",
            border=True
        )

st.divider()

# Big Awesome Numbers - BANs    
total_flights = len(d["all_flights"])
avg_delay = round(d["all_flights"]['minutes_delay'].mean(), 1)
avg_daily_flights = round(total_flights / d["all_flights"]['date'].nunique(), 0)
airports = d["all_flights"]['airport_location'].nunique()

print(metrics_dict)

cols = st.columns(4)
with cols[0]:
    st.metric(label='Total flights in database', value=total_flights, border=True)
with cols[1]:
    st.metric(label='Avg Flight Delay', value=f'{avg_delay} minutes', border=True)
with cols[2]:
    st.metric(label='Hub Reach', value=f'{airports} airports', border=True)
with cols[3]:
    st.metric(label='Flights per Day', value=f'Ø {avg_daily_flights}', border=True)



# Data Quality

cols = st.columns(len(metrics_dict)+1)

tab_dashboard, tab_flights_airlines, tab_delays = st.tabs([
    "Overview",
    "Airlines and Flights",
    "Delays Analysis"
])







