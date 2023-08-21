from time import sleep

import streamlit as st
from st_on_hover_tabs import on_hover_tabs
import pandas as pd


def load_db():
    conn = st.experimental_connection("mysql", type="sql")
    df = conn.query(
        f"""
        select 
            origin.id,
            origin.processing_time,
            origin.start_event_time,
            origin.end_event_time,
            origin.station_id,
            origin.stat_id,
            origin.stat_value,
            CASE
                WHEN origin.stat_value - prev.stat_value IS NULL THEN 0
                ELSE origin.stat_value - prev.stat_value
            END AS stat_value_diff
        from (
            select
                id,
                processing_time,
                start_event_time,
                end_event_time,
                stat_id,
                stat_value,
                station_id,
                ROW_NUMBER() OVER (PARTITION BY stat_id ORDER BY end_event_time DESC) AS row_num
            from airdata_statistics
        ) origin
        left join (
            select 
                id,
                stat_id,
                start_event_time,
                end_event_time,
                LAG(stat_value) OVER (PARTITION BY stat_id ORDER BY end_event_time) AS stat_value
            from airdata_statistics
        ) prev
        on origin.stat_id = prev.stat_id
            and origin.end_event_time = prev.end_event_time
        where origin.row_num = 1
        order by origin.stat_id, origin.end_event_time
        """,
        ttl=60
    )
    return df


def batch_page():
    st.title("Batch data")

    data_load_state = st.text("데이터를 가져오는 중입니다...")
    data = load_db()
    data_load_state.text("")

    datetime_format = "%y.%m.%d %H:%M"
    start_datetime = data.iloc[0]["start_event_time"]
    end_datetime = data.iloc[0]["end_event_time"]
    st.text(f"집계 기간 : {start_datetime.strftime(datetime_format)} ~ {end_datetime.strftime(datetime_format)}")

    st.divider()

    pm_10, o3, no2 = st.columns(3)
    co, so2, _ = st.columns(3)

    pm_10_value = data[data["stat_id"] == 1].iloc[0]
    pm_10.metric(
        label="PM10",
        value=round(pm_10_value["stat_value"], 3),
        delta=round(pm_10_value["stat_value_diff"], 3)
    )

    o3_value = data[data["stat_id"] == 2].iloc[0]
    o3.metric(
        label="O3",
        value=round(o3_value["stat_value"], 3),
        delta=round(o3_value["stat_value_diff"], 3)
    )

    no2_value = data[data["stat_id"] == 3].iloc[0]
    no2.metric(
        label="NO2",
        value=round(no2_value["stat_value"], 3),
        delta=round(no2_value["stat_value_diff"], 3)
    )

    co_value = data[data["stat_id"] == 4].iloc[0]
    co.metric(
        label="CO",
        value=round(co_value["stat_value"], 3),
        delta=round(co_value["stat_value_diff"], 3)
    )

    so2_value = data[data["stat_id"] == 5].iloc[0]
    so2.metric(
        label="SO2",
        value=round(so2_value["stat_value"], 3),
        delta=round(so2_value["stat_value_diff"], 3)
    )

    st.divider()
    st.text("데이터 출처 : 한국환경공단 에어코리아 대기오염정보(https://www.data.go.kr/data/15073861/openapi.do)")


def streaming_page():
    st.title("Streaming Data")

    target_datetime = "2023-08-21 09:00"
    st.text(f"기준 시각 : {target_datetime}")

    st.divider()

    pm_10, o3, no2 = st.columns(3)
    co, so2, _ = st.columns(3)

    pm_10.metric(
        label="PM10",
        value=1,
        delta=0.1
    )

    o3.metric(
        label="O3",
        value=1,
        delta=0.1
    )

    no2.metric(
        label="NO2",
        value=1,
        delta=0.1
    )

    co.metric(
        label="CO",
        value=1,
        delta=0.1
    )

    so2.metric(
        label="SO2",
        value=1,
        delta=0.1
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
    streaming_page()
