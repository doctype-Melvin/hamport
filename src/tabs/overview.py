import streamlit as st

def render_overview(data):
    st.subheader('Daily flight volume', text_alignment='center')
    st.area_chart(
        data=data["volume"],
        x='date',
        x_label='Date',
        y='flight_count',
        y_label='Count'
    )
    
    
