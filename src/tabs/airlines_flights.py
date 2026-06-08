import streamlit as st

def render_airlines(data):
    data_top5_airlines = data["airlines_stats"].sort_values('number_of_flights', ascending=False).head(5)
    data_busy_hours = data["airport_stats"].sort_values('avg_hourly_flights', ascending=False).head(5)

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