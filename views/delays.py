import streamlit as st

if st.session_state.get("fetch_error", True) or st.session_state["data"] is None:
    st.error("⚠️ Unable to load flight data right now. Please check your database connection or try again later.")
else:
    d = st.session_state["data"]

st.header('Delays and Cancellations Stats', text_alignment='center')
st.subheader('Delays over time')
st.warning('The peak in delays on May 3, 2026 was caused by a small plane accident.')
st.area_chart(
    data=d["delays_date"],
    x='date',
    x_label='Date',
    y='date_delay',
    y_label='Minutes of Delay',
    color='direction'
)

# Origin Airport Delays Top5
data_top5_origin_delays = d["airport_delays"].sort_values('avg_arrival_delay', ascending=False).head(5)

# Airlines cancellation rate
# 1. Group by airline and sum cancelled flights per airline group
airline_stats = d["all_flights"].groupby('airline_group').agg(
    total_flights=('flight_id', 'count'),
    cancelled_flights=('cancelled_fl', lambda x: (x == 1).sum())
).reset_index()

# 2. Calculate share of cancelled flights
airline_stats['cancel_rate_pct'] = (
    airline_stats['cancelled_flights'] / airline_stats['total_flights']
    ) * 100
airline_stats = (
    airline_stats.sort_values('cancel_rate_pct', ascending=False)
    .head(5)
    .reset_index()
    )

airline_stats['airline_group'] = [
    f"{i+1}. {name}" for i, name in enumerate(airline_stats['airline_group'])
]

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