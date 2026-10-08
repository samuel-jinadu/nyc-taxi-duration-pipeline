from airflow.sdk import DAG
import requests
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
import pandas as pd
import pyarrow


with DAG(
    dag_id="main",
    start_date=pendulum.datetime(2026, 10, 7, tz="UTC"),
    schedule=None,
    catchup = False
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
        table = pyarrow.parquet .read_table(f"/data/taxi-data-raw.parquet", columns=columns)
        pyarrow.csv.write_csv(table, "/data/taxi-data-raw.csv")

    convert_parquet_to_csv = PythonOperator(
        task_id = "convert_parquet_to_csv",
        python_callable = _convert_pq_to_csv
    )

    def _transform_taxi_data():
        """Average trip duration per pickup zone"""
        df = pd.read_csv("/data/taxi-data-raw.csv")

        df["trip_duration"] = (pd.to_datetime(df["dropoff_datetime"]) - pd.to_datetime(df["pickup_datetime"]))
        df["trip_duration"] = df["trip_duration"].dt.total_seconds()

        summary_df = (
            df.groupby("pickup_locationid")["trip_duration"]
                .mean()
                .reset_index()
                .rename(columns={"trip_duration": "avg_duration_seconds"})
            )

        summary_df.to_csv("/data/taxi_summary.csv", index=False)
        print(f"Computed averages for {len(summary_df)} zones")

    transform_taxi_data = PythonOperator(
        task_id = "transform_taxi_data", 
        python_callable = _transform_taxi_data
    )

    download_taxi_data >> convert_parquet_to_csv >> transform_taxi_data