import streamlit as st

def render_airlines(data):
    data_top5_airlines = data["airlines_stats"].sort_values('number_of_flights', ascending=False).head(5)

    st.metric(label='Active Airlines', value=len(data["airlines_stats"]), border=True, width='content')

    st.subheader('Top 5 Airlines by flight volume', text_alignment='center')
    st.bar_chart(
        data=data_top5_airlines,
        x='airline_group',
        x_label='Airline Group',
        y='number_of_flights',
        y_label='Flights',
        horizontal=True,
        sort='-number_of_flights'
    )

    st.subheader('Flight stats by airline', text_alignment='center')
    st.dataframe(data=data["airlines_stats"], column_config={
        "airline_group": "Airline",
        "number_of_flights": "Total Flights",
        "share": "Share",
        "tracked_flights": "Tracked Flights",
        "cancelled_flights": "Cancelled Flights",
        "avg_delay": "Average Delay",
        "total_delay": "Total Delay"
        
    },
    hide_index=True)

 