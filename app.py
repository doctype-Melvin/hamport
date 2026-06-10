import streamlit as st
import pandas as pd
from src.loader import get_data
from src.tabs.overview import render_overview
from src.tabs.airlines_flights import render_airlines

st.set_page_config(page_title="Hamburg Airport Data", page_icon="✈", layout="wide")

d = None

if d == None:
    data = get_data()

    if data is not None:
        st.toast('Data ready!', icon='✅', duration='short')
        d = data
    else:
        st.toast('Unable to fetch data. Please refresh page.', icon='🔄')
        st.stop()

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
formatted_total = f"{total_flights:_}".replace("_", ".")
avg_delay = round(d["all_flights"]['minutes_delay'].mean(), 1)
airports = d["all_flights"]['airport_location'].nunique()
avg_flights_count = int(round(d["volume"]["flight_count"].mean(), 0))

cols = st.columns(4)
with cols[0]:
    st.metric(label='Total flights in database', value=formatted_total, border=True)
with cols[1]:
    st.metric(label='Avg Flight Delay', value=f'{avg_delay} minutes', border=True)
with cols[2]:
    st.metric(label='Hub Reach', value=f'{airports} airports', border=True)
with cols[3]:
    st.metric(label='Flights per Day', value=f'Ø {avg_flights_count}', border=True)

# Data Quality

cols = st.columns(len(metrics_dict)+1)

tab_overview, tab_flights_airlines, tab_delays = st.tabs([
    "Overview",
    "Airlines and Flights",
    "Delays Analysis"
])

with tab_overview:
    render_overview(data=d)

with tab_flights_airlines:
    render_airlines(data=d)








