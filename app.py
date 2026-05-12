import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from src.loader import get_data
from src.charts import barchart
from src.metrix import bans

st.set_page_config(page_title="Hamburg Airport Data", layout="wide")

st.title("Flights, Delays and Weather")
st.write("Helmut Schmidt Airport - Hamburg-Fuhlsbüttel")

try:
    (
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
    
    st.header('Airlines and busy hours')
    data_top5_airlines = df_airlines_stats.sort_values('number_of_flights', ascending=False).head(5)
    data_busy_hours = df_airport_stats.sort_values('avg_hourly_flights', ascending=False).head(5)
    
    col1, col2 = st.columns(2)
    with col1:
        st.write('Airlines with the most flights')
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
        st.write('Busy hours at Hamburg Airport')
        st.bar_chart(
            data=data_busy_hours,
            x='planned_hour',
            x_label='Hour of Day',
            y='avg_hourly_flights',
            y_label='Flights',
            horizontal=True,
            sort='-avg_hourly_flights'
        )
    
    st.header('Delay Stats')
    st.area_chart(
        data=df_delays_date,
        x='date',
        x_label='Date',
        y='date_delay',
        y_label='Minutes of Delay',
        color='direction'
    )

    st.write('Top 5 delays by origin airport')
    data_top5_origin_delays = df_airport_delays.sort_values('avg_arrival_delay', ascending=False).head(5)

    st.bar_chart(
        data=data_top5_origin_delays,
        x='airport_location',
        x_label='Airport Location',
        y='avg_arrival_delay',
        y_label='AVG Delay',
        horizontal=True,
        sort='-avg_arrival_delay'
    )

    st.subheader('Data Quality assessment')
    metrics_dict = df_data_quality.to_dict('records')
    
    cols = st.columns(len(metrics_dict)+1)

    with cols[0]:
        st.info('Tracking arrivals is automatic, Departures are manually tracked.')

    for i, row in enumerate(metrics_dict):
        direction = row['direction']
        total = row['scheduled_flights']
        tracked = row['tracked_flights']
        pct = row['completeness_pct']

        cols[i+1].metric(
            label=f"{direction}",
            value=f"{pct}%",
            delta=f"{tracked} tracked flights",
            delta_arrow="off",
            delta_color="normal" if pct > 80 else "inverse"
        )


except Exception as e:
    st.error(f"Connection failed: {e}")