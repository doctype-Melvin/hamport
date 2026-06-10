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

    col1, col2, col3 = st.columns([0.5, 0.1, 0.5])

    with col1:
        st.subheader('Busy hours at Hamburg Airport', text_alignment='center')
        st.scatter_chart(
            data=data_busy_hours,
            x='planned_hour',
            x_label='Hour of Day',
            y='avg_hourly_flights',
            y_label='Flights',
            size='avg_hourly_flights',
            color='direction'
        )

    with col3:
        st.subheader('Average delays by hour', text_alignment='center')
        st.bar_chart(
            data=data["airport_stats"],
            x='planned_hour',
            x_label='Hour of Day',
            y='avg_hourly_delay',
            y_label='Delay in minutes',
            stack=False,
            color='direction',
        )
