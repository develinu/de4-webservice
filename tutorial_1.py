import pandas as pd
import streamlit as st

st.write("Hello, *World* :sunglasses:")

df = pd.DataFrame({
    "first column": [1, 2, 3, 4],
    "second column": [10, 20, 30, 40]
})

st.write(df)

st.write(12345)

st.markdown("Hello, **World**")

st.title("The app title")

st.header("This is a header")

st.subheader("This is a subheader")

st.caption("This is a caption")

st.code("""
    def hello():
        print("hello, Streamlit!")
""", language="python")

st.latex("\int a x^2 \,dx")

st.divider()

st.data_editor(df, num_rows="dynamic")

st.json({
    "foo": "bar",
    "baz": "boz",
    "stuff": [
        "stuff 1",
        "stuff 2",
        "stuff 3",
        "stuff 4",
        "stuff 5",
    ]
})