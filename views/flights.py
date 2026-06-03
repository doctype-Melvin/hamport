import streamlit as st


if st.session_state.get("fetch_error", True) or st.session_state["data"] is None:
    st.error("⚠️ Unable to load flight data right now. Please check your database connection or try again later.")
else:
    d = st.session_state["data"]

st.header('Flight Volume by Date', text_alignment='center')
st.subheader('How many flights arrive at and leave from Hamburg Airport?')
st.bar_chart(
    data=d["volume"],
    x='date',
    x_label='Date',
    y='flight_count',
    y_label='Count'
)

st.divider()

st.header('Airlines and busy hours', text_alignment='center')
data_top5_airlines = d["airlines_stats"].sort_values('number_of_flights', ascending=False).head(5)
data_busy_hours = d["airport_stats"].sort_values('avg_hourly_flights', ascending=False).head(5)

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

