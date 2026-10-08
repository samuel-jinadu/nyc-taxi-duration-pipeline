import streamlit as st
from streamlit_autorefresh import st_autorefresh
import os
import psycopg
import pandas as pd

POSTGRES_DSN = os.getenv("POSTGRES_DSN")

st_autorefresh(interval=2000, key="data_refresh")

st.title("Which NYC taxi pickup zones have the longest average trip durations?")

with psycopg.Connection(POSTGRES_DSN) as conn, conn.cursor() as cur:
    df = pd.DataFrame(cur.execute("SELECT * FROM taxi_zone_durations").fetchall())
    st.dataframe(df, width="stretch", hide_index=True)