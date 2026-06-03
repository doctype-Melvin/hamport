import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from src.loader import get_data
from src.charts import barchart
from src.metrix import bans

st.set_page_config(page_title="Hamburg Airport Data", page_icon="✈", layout="wide")

st.sidebar.title("Aviation Report", text_alignment='center')
st.sidebar.subheader("Helmut Schmidt Airport - Hamburg-Fuhlsbüttel (HAM)", text_alignment='center')
st.sidebar.divider()

if "data" not in st.session_state:
    airport_data = get_data()

    if airport_data is not None:
        st.session_state["data"] = airport_data
        st.session_state["fetch_error"] = False
    else:
        st.session_state["data"] = None
        st.session_state["fetch_error"] = True

print(st.session_state["data"])

dashboard_view = st.Page("views/dashboard.py", title="Hamburg Airport Overview", default=True)
flights_view = st.Page("views/flights.py", title="Flights and Airlines")
delays_view = st.Page("views/delays.py", title="Delays at Hamburg Airport")

pg = st.navigation([dashboard_view, flights_view, delays_view])
pg.run()








