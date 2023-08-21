import json
from time import sleep
from uuid import uuid4
from dateutil import parser

import streamlit as st
from st_on_hover_tabs import on_hover_tabs
import pandas as pd
from confluent_kafka import Consumer


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


def get_kafka_consumer():
    consumer = Consumer({
        "bootstrap.servers": st.secrets["kafka_bootstrap_servers"],
        "group.id": uuid4(),
        "auto.offset.reset": "latest"
    })
    consumer.subscribe([st.secrets["kafka_topic"]])
    return consumer


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


def init_stream_session_state(states):
    for state in states:
        if state not in st.session_state:
            st.session_state[state] = 0.0


def streaming_page(data):
    st.title("Streaming Data")

    target_datetime = parser.isoparse(data["end"])
    st.text(f"기준 시각 : {target_datetime.strftime('%y.%m.%d %H')}")

    st.divider()

    pm_10, o3, no2 = st.columns(3)
    co, so2, _ = st.columns(3)

    pm_10.metric(
        label="PM10",
        value=round(data["pm_10"], 3),
        delta=round(data["pm_10"] - st.session_state["pm_10"], 3)
    )
    st.session_state["pm_10"] = data["pm_10"]

    o3.metric(
        label="O3",
        value=round(data["o3"], 3),
        delta=round(data["o3"] - st.session_state["o3"], 3)
    )
    st.session_state["o3"] = data["o3"]

    no2.metric(
        label="NO2",
        value=round(data["no2"], 3),
        delta=round(data["no2"] - st.session_state["no2"], 3)
    )
    st.session_state["no2"] = data["no2"]

    co.metric(
        label="CO",
        value=round(data["co"], 3),
        delta=round(data["co"] - st.session_state["co"], 3)
    )
    st.session_state["co"] = data["co"]

    so2.metric(
        label="SO2",
        value=round(data["so2"], 3),
        delta=round(data["so2"] - st.session_state["so2"], 3)
    )
    st.session_state["so2"] = data["so2"]

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
    p1 = st.empty()
    with p1.container():
        batch_page()
elif tabs == "스트리밍":
    p2 = st.empty()
    states = ["pm_10", "o3", "no2", "co", "so2"]
    init_stream_session_state(states)
    consumer = get_kafka_consumer()

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error : {msg.error()}")
            continue

        with p2.container():
            data = json.loads(msg.value().decode("utf-8"))
            streaming_page(data)
            sleep(1)

    consumer.close()
