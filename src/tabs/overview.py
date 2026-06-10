import streamlit as st

def render_overview(data):
    data_busy_hours = data["airport_stats"].sort_values('avg_hourly_flights', ascending=False)

    st.subheader('Daily flight volume', text_alignment='center')
    st.area_chart(
        data=data["volume"],
        x='date',
        x_label='Date',
        y='flight_count',
        y_label='Count'
    )
    
    st.subheader('Busy hours at Hamburg Airport')
    st.scatter_chart(
        data=data_busy_hours,
        x='planned_hour',
        x_label='Hour of Day',
        y='avg_hourly_flights',
        y_label='Flights',
        color='direction'
    )
