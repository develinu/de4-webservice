from time import sleep

import streamlit as st
from st_on_hover_tabs import on_hover_tabs
import pandas as pd


def batch_page():
    st.title("Batch data")

    data_load_state = st.text("데이터를 가져오는 중입니다...")
    # data load logic
    sleep(1)
    data_load_state.text("")

    start_datetime = "2023-08-18 00:00"
    end_datetime = "2023-08-18 12:00"
    st.text(f"집계 기간 : {start_datetime} ~ {end_datetime}")

    st.divider()

    metric_1 = st.columns(3)
    metric_2 = st.columns(3)

    metric_1[0].metric(
        label="PM10",
        value=0.12,
        delta=-0.01
    )

    metric_1[1].metric(
        label="PM10",
        value=0.12,
        delta=-0.01
    )

    metric_1[2].metric(
        label="PM10",
        value=0.12,
        delta=-0.01
    )

    metric_2[0].metric(
        label="PM10",
        value=0.12,
        delta=-0.01
    )

    metric_2[1].metric(
        label="PM10",
        value=0.12,
        delta=-0.01
    )

    st.divider()
    st.text("데이터 출처 : 한국환경공단 에어코리아 대기오염정보(https://www.data.go.kr/data/15073861/openapi.do)")


st.set_page_config(layout="wide")
st.header("Data Engineering Project")
st.markdown("<style>" + open("style.css").read() + "</style>", unsafe_allow_html=True)

with st.sidebar:
    tabs = on_hover_tabs(tabName=["배치", "스트리밍"],
                         iconName=["dashboard", "monitoring"],
                         default_choice=0)

if tabs == "배치":
    batch_page()
elif tabs == "스트리밍":
    st.write("스트리밍")
