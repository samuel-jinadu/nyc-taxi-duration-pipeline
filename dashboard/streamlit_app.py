import streamlit as st
from streamlit_autorefresh import st_autorefresh
import os
import psycopg
import pandas as pd
from psycopg import errors
# import time

POSTGRES_DSN = os.getenv("POSTGRES_DSN")

st_autorefresh(interval=2000, key="data_refresh")

st.header("Which NYC taxi pickup zones have the longest average trip durations?")

try:
    with psycopg.connect(POSTGRES_DSN) as conn:
        df = pd.read_sql_query("SELECT * FROM taxi_zone_durations ORDER BY taxi_zone_durations DESC", conn)
    st.dataframe(df, width="stretch", hide_index=True)
except errors.UndefinedTable:
    st.info("Table `taxi_zone_durations` does not exist yet. Waiting for the Airflow DAG to run…")
except psycopg.OperationalError as e:
    st.warning(f"Database not reachable yet: {e}")