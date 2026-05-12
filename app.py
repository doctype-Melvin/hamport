import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from src.loader import get_data
from src.charts import barchart
from src.metrix import bans

st.set_page_config(page_title="Hamburg Airport Data", layout="wide")

st.title("Aviation Report", text_alignment='center')
st.subheader("Helmut Schmidt Airport - Hamburg-Fuhlsbüttel (HAM)", text_alignment='center')
st.divider()

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
    
    # Description and current weather
    today = df_weather_history.weather_at[0].strftime("%d.%m.%Y")
    condition = df_weather_history.condition[0]
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('''
        This report visualizes information pulled from :yellow[Hamburg Airport Open API]  
                    and :yellow[open-meteo.com].
    ''')
    with col2:
        st.info(f"Date: {today} - Weather in Hamburg: {condition}")
    
    st.divider()
    
    # Big Awesome Numbers - BANs    
    total_flights = len(df_all_flights)
    avg_delay = round(df_all_flights['minutes_delay'].mean(), 1)
    avg_daily_flights = round(total_flights / df_all_flights['date'].nunique(), 0)
    airports = df_all_flights['airport_location'].nunique()

    cols = st.columns(4)
    with cols[0]:
        st.metric(label='Total flights in database', value=total_flights, border=True)
    with cols[1]:
        st.metric(label='Avg Flight Delay', value=f'{avg_delay} minutes', border=True)
    with cols[2]:
        st.metric(label='Hub Reach', value=f'{airports} airports', border=True)
    with cols[3]:
        st.metric(label='Flights per Day', value=f'Ø {avg_daily_flights}', border=True)
    st.divider()
    # Data Quality
    st.header('Data Quality assessment', text_alignment='center')
    metrics_dict = df_data_quality.to_dict('records')
    
    cols = st.columns(len(metrics_dict)+1)

    with cols[0]:
        st.warning('Tracking arrivals is automatic, Departures are manually tracked.')

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
            delta_color="normal" if pct > 80 else "inverse",
            border=True
        )
    
    st.divider()

    st.header('Flight Volume by Date', text_alignment='center')
    st.subheader('How many flights arrive at and leave from Hamburg Airport?')
    st.bar_chart(
        data=df_volume,
        x='date',
        x_label='Date',
        y='flight_count',
        y_label='Count'
    )
    
    st.divider()

    st.header('Airlines and busy hours', text_alignment='center')
    data_top5_airlines = df_airlines_stats.sort_values('number_of_flights', ascending=False).head(5)
    data_busy_hours = df_airport_stats.sort_values('avg_hourly_flights', ascending=False).head(5)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader('Airlines with the most flights')
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
        st.subheader('Busy hours at Hamburg Airport')
        st.bar_chart(
            data=data_busy_hours,
            x='planned_hour',
            x_label='Hour of Day',
            y='avg_hourly_flights',
            y_label='Flights',
            horizontal=True,
            sort='-avg_hourly_flights'
        )
    
    st.divider()

    st.header('Delays and Cancellations Stats', text_alignment='center')
    st.subheader('Delays over time')
    st.warning('The peak in delays on May 3, 2026 was caused by a small plane accident.')
    st.area_chart(
        data=df_delays_date,
        x='date',
        x_label='Date',
        y='date_delay',
        y_label='Minutes of Delay',
        color='direction'
    )

    # Origin Airport Delays Top5
    data_top5_origin_delays = df_airport_delays.sort_values('avg_arrival_delay', ascending=False).head(5)
    
    # Airlines cancellation rate
    # 1. Group by airline and sum cancelled flights per airline group
    airline_stats = df_all_flights.groupby('airline_group').agg(
        total_flights=('flight_id', 'count'),
        cancelled_flights=('cancelled_fl', lambda x: (x == 1).sum())
    ).reset_index()

    # 2. Calculate share of cancelled flights
    airline_stats['cancel_rate_pct'] = (
        airline_stats['cancelled_flights'] / airline_stats['total_flights']
        ) * 100
    airline_stats = airline_stats.sort_values('cancel_rate_pct', ascending=False).head(5)

    cols = st.columns(2)

    with cols[0]:
        st.write('Which airline highest rate of cancelled flights?')

        st.bar_chart(
            data=airline_stats,
            x='airline_group',
            x_label='Airline Group',
            y='cancel_rate_pct',
            y_label='Cancelled Flights %',
            horizontal=True
        )

    with cols[1]:
        st.write('Which airport causes the biggest delays?')
        st.bar_chart(
            data=data_top5_origin_delays,
            x='airport_location',
            x_label='Airport Location',
            y='avg_arrival_delay',
            y_label='AVG Delay',
            horizontal=True,
            sort='-avg_arrival_delay'
        )

    


except Exception as e:
    st.error(f"Connection failed: {e}")