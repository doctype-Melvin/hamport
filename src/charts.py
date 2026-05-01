import streamlit as st

def linechart(df):
    return st.line_chart(data=df, x='hour', y='flight_count')
