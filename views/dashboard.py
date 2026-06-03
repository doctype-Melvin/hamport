import streamlit as st

if st.session_state.get("fetch_error", True) or st.session_state["data"] is None:
    st.error("⚠️ Unable to load flight data right now. Please check your database connection or try again later.")
else:
    d = st.session_state["data"]

# Description and current weather
today = d["weather_history"].weather_at[0].strftime("%d.%m.%Y")
condition = d["weather_history"].condition[0]
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
total_flights = len(d["all_flights"])
avg_delay = round(d["all_flights"]['minutes_delay'].mean(), 1)
avg_daily_flights = round(total_flights / d["all_flights"]['date'].nunique(), 0)
airports = d["all_flights"]['airport_location'].nunique()

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
metrics_dict = d["data_quality"].to_dict('records')

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