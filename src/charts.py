import streamlit as st

def barchart(df):
    return st.bar_chart(
        data=df,
        x='date',
        x_label='Date',
        y='flight_count',
        y_label='Count')

