from airflow.sdk import DAG
import requests
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
import pandas as pd
# import pyarrow
from pyarrow import parquet, csv
from airflow.sdk.bases.hook import BaseHook
from sqlalchemy import create_engine, text
from datetime import timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="main-with-postgres",
    start_date=pendulum.datetime(2026, 10, 7, tz="UTC"),
    schedule=None,
    catchup = False,
    default_args = default_args
):
    download_script = """

    DATA_YEAR=2026
    month="01"
    url_prefix="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${DATA_YEAR}"
    wget "${url_prefix}-${month}.parquet" -O "/data/taxi-data-raw.parquet"

    """

    download_taxi_data = BashOperator(
        task_id = "download_taxi_data", 
        bash_command = download_script
    )

    def _convert_pq_to_csv():
        columns=["tpep_pickup_datetime","tpep_dropoff_datetime","trip_distance","PULocationID","DOLocationID"]
        
        table = parquet .read_table(f"/data/taxi-data-raw.parquet", columns=columns)
        csv.write_csv(table, "/data/taxi-data-raw.csv")

    convert_parquet_to_csv = PythonOperator(
        task_id = "convert_parquet_to_csv",
        python_callable = _convert_pq_to_csv
    )

    def _transform_taxi_data():
        """Average trip duration per pickup zone"""
        df = pd.read_csv("/data/taxi-data-raw.csv")

        df["trip_duration"] = (
            pd.to_datetime(df["tpep_dropoff_datetime"]) 
            - pd.to_datetime(df["tpep_pickup_datetime"])
            )
        df["trip_duration"] = df["trip_duration"].dt.total_seconds()

        summary_df = (
            df.groupby("PULocationID")["trip_duration"]
                .mean()
                .reset_index()
                .rename(columns={
                    "trip_duration": "avg_duration_seconds", 
                    "PULocationID": "pickup_locationid"
                    })
            )

        summary_df.to_csv("/data/taxi_summary.csv", index=False)
        print(f"Computed averages for {len(summary_df)} zones")

    transform_taxi_data = PythonOperator(
        task_id = "transform_taxi_data", 
        python_callable = _transform_taxi_data
    )

    def _load_to_database(**context):
        df = pd.read_csv("/data/taxi_summary.csv")

        df["execution_date"] = context["data_interval_start"].date()

        hook = PostgresHook(postgres_conn_id="summary_db")
        engine = hook.get_sqlalchemy_engine()

        with engine.begin() as conn:
            query1 = """
                CREATE TABLE IF NOT EXISTS taxi_zone_durations (
                    pickup_locationid INTEGER,
                    avg_duration_seconds FLOAT,
                    execution_date DATE
                );
            """
            conn.execute(text(query1))

            query2 = """
                DELETE FROM taxi_zone_durations WHERE execution_date = :execution_date
            """
            conn.execute(text(query2), {"execution_date": context["data_interval_start"].date()})

            df.to_sql( "taxi_zone_durations", con=conn, if_exists="append", index=False) 

    load_to_postgres = PythonOperator(
        task_id="load_to_postgres", 
        python_callable=_load_to_database
        )
    


    download_taxi_data >> convert_parquet_to_csv >> transform_taxi_data >> load_to_postgres