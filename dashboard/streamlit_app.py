import streamlit as st
from streamlit_autorefresh import st_autorefresh
import os
import psycopg
import pandas as pd
from psycopg import errors
import pydeck as pdk
import json
# import time

POSTGRES_DSN = os.getenv("POSTGRES_DSN")

st.set_page_config(page_title="NYC Taxi Zone Durations", layout="wide")
st_autorefresh(interval=5000, key="data_refresh")


@st.cache_data
def load_zones():
    with open("/app/data/taxi-zones.geojson") as f:
        return json.load(f)


def make_map():
    df = pd.read_sql_query("SELECT * FROM taxi_zone_durations ORDER BY avg_duration_mins DESC", conn)
    max_dur = float(df["avg_duration_mins"].max())

    

    def ramp(v):
        if v is None or pd.isna(v):
            return [200, 200, 200, 60]
        t = min(float(v) / max_dur, 1.0)
        return [int(255 * t), int(60 * (1 - t)), int(255 * (1 - t)), 190]
    
    values = df.set_index("pickup_locationid")["avg_duration_mins"].to_dict()
    names  = df.set_index("pickup_locationid")["zone"].to_dict()
    # print(type(names.keys()[0]))
    # print(type(values.keys()[0]))


    zones = load_zones()
    for feat in zones["features"]:
        lid = int(feat["properties"]["LocationID"])
        feat["properties"]["avg_duration"] = values.get(lid)
        feat["properties"]["zone_name"]    = names.get(lid)
        feat["properties"]["color"]        = ramp(values.get(lid))

    layer = pdk.Layer(
        "GeoJsonLayer",
        zones,
        pickable=True,
        stroked=False,
        filled=True,
        extruded=False,
        auto_highlight=True,
        opacity=0.8,
        get_fill_color="properties.color",
    )

    st.pydeck_chart(
        pdk.Deck(
            layers=[layer],
            initial_view_state=pdk.ViewState(latitude=40.73, longitude=-73.95, zoom=9.5),
            tooltip={
            "html": "<b>{properties.zone_name}</b><br/>Avg duration: {properties.avg_duration} mins",
            "style": {"backgroundColor": "steelblue", "color": "white"},
        },
        ),
        use_container_width=True,
    )


try:
    with psycopg.connect(POSTGRES_DSN) as conn:
        st.header("Which NYC taxi pickup zones have the longest average trip durations?")
        df = pd.read_sql_query("""
        SELECT borough as "Borough", zone as "Zone", avg_duration_mins as "Average Trip Duration (in minutes)" FROM taxi_zone_durations ORDER BY avg_duration_mins DESC
        """, conn)
        st.dataframe(df, width="stretch", hide_index=True)
        make_map()
except errors.UndefinedTable:
    st.info("Table `taxi_zone_durations` does not exist yet. Waiting for the Airflow DAG to run…")
    st.stop()
except psycopg.OperationalError as e:
    st.warning(f"Database not reachable yet: {e}")
    st.stop()
except FileNotFoundError as e:
    st.warning(f"Zones data no found!")
    st.stop()


