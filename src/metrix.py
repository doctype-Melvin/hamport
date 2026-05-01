import streamlit as st

def bans(df):
    col1, col2 = st.columns(2)
    with col1:
        st.metric = ('Date', df[0].weather_at)
    return col1, col2