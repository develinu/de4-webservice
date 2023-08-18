import datetime

import streamlit as st

if st.button("Say hello"):
    st.write("Say hello")
else:
    st.write("Goodbye")

agree = st.checkbox("I agree")
if agree:
    st.write("checked")

genre = st.radio(
    "What\'s your favorite movie genre?",
    ("Comedy", "Drama", "Documentary")
)

if genre == "Comedy":
    st.write("Selected : comedy")
elif genre == "Drama":
    st.write("Selected : drama")
else:
    st.write("Selected : documentary")

genre = st.selectbox(
    "What\'s your favorite movie genre?",
    ("Comedy", "Drama", "Documentary")
)

st.write("Selected : ", genre)

genre = st.multiselect(
    "What\'s your favorite movie genre?",
    ("Comedy", "Drama", "Documentary"),
    ("Comedy", "Drama")
)

st.write("Selected : ", genre)

date_picker = st.date_input(
    "Select date",
    (datetime.date(2023, 8, 1), datetime.date.today()),
    datetime.date(2023, 8, 1),
    datetime.date(2023, 8, 31)
)

st.write("Selected date : ", date_picker)