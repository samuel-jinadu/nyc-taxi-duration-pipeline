from airflow.sdk import DAG
import requests
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
import pandas as pd
# import pyarrow
import geopandas as gpd
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
    download_yellow_taxi_trip_records_script = """
        DATA_YEAR=2026
        month="01"
        url_prefix="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${DATA_YEAR}"
        wget "${url_prefix}-${month}.parquet" -O "/data/taxi-data-raw.parquet"
    """

    download_yellow_taxi_trip_records = BashOperator(
        task_id = "download_yellow_taxi_trip_records", 
        bash_command = download_yellow_taxi_trip_records_script
    )

    download_taxi_zone_lookup_script = """
        url="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
        wget "${url}" -O "/data/taxi-zone-lookup.csv"
    """

    download_taxi_zone_lookup = BashOperator(
            task_id = "download_taxi_zone_lookup", 
            bash_command = download_taxi_zone_lookup_script
        )

    get_taxi_shapefile_script = """
        url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
        wget "${url}" -O "/data/taxi-zones.zip"
        python -c "import zipfile; zipfile.ZipFile('/data/taxi-zones.zip').extractall('/data/taxi-zones')"
    """

    get_taxi_shapefile = BashOperator(
            task_id = "get_taxi_shapefile", 
            bash_command = get_taxi_shapefile_script
        )

    def _convert_shapefile_to_geojson():
        shapefile_path = "/data/taxi-zones/taxi_zones.shp"
        geojson_path = "/data/taxi-zones.geojson"
        gdf = gpd.read_file(shapefile_path).to_crs(4326)
        gdf.to_file(geojson_path, driver="GeoJSON")

    convert_shapefile_to_geojson = PythonOperator(
        task_id = "convert_shapefile_to_geojson",
        python_callable = _convert_shapefile_to_geojson
    )
    


    def _convert_pq_to_csv():
        columns=["tpep_pickup_datetime","tpep_dropoff_datetime","trip_distance","PULocationID","DOLocationID"]
        
        table = parquet.read_table(f"/data/taxi-data-raw.parquet", columns=columns)
        csv.write_csv(table, "/data/taxi-data-raw.csv")

    convert_parquet_to_csv = PythonOperator(
        task_id = "convert_parquet_to_csv",
        python_callable = _convert_pq_to_csv
    )

    def _enrich_taxi_trip_records()-> pd.DataFrame:
        taxi_records_df = pd.read_csv("/data/taxi-data-raw.csv")
        taxi_lookup_df = pd.read_csv("/data/taxi-zone-lookup.csv")

        enriched_taxi_records_df = taxi_records_df.merge(
            taxi_lookup_df[["LocationID", "Borough", "Zone"]].rename(columns={
                "LocationID": "PULocationID",
                "Borough":"borough",
                "Zone":"zone"
                }),
            on="PULocationID", how="left"
        )

        return enriched_taxi_records_df

    def _enrich_taxi_summary(summary_df: pd.DataFrame)-> pd.DataFrame:
        gdf = gpd.read_file("/data/taxi-zones.geojson").to_crs(4326)
        pts = gdf.geometry.representative_point()
        centroids = pd.DataFrame({
            "pickup_locationid": gdf["LocationID"].astype(int),
            "lat": pts.y.astype(float),
            "lon": pts.x.astype(float),
        })
        return summary_df.merge(centroids, on="pickup_locationid", how="left")


    def _transform_taxi_trip_records():
        """Average trip duration per pickup zone"""
        enriched_taxi_records_df = _enrich_taxi_trip_records()

        delta = (
            pd.to_datetime(enriched_taxi_records_df["tpep_dropoff_datetime"]) 
            - pd.to_datetime(enriched_taxi_records_df["tpep_pickup_datetime"])
            )
        # enriched_taxi_records_df["trip_duration"] = delta.dt.total_seconds().round(2)
        enriched_taxi_records_df["trip_duration_min"] = (delta / pd.Timedelta("1min")).round(2)

        summary_df = (
            enriched_taxi_records_df.groupby("PULocationID")[["trip_duration_min"]]
                .mean()
                .reset_index()
                .rename(columns={
                    "PULocationID": "pickup_locationid",
                    "trip_duration_min":"avg_duration_mins"
                    })
            )

        summary_df = _enrich_taxi_summary(summary_df)
        summary_df.to_csv("/data/taxi_summary.csv", index=False)
        print(f"Computed averages for {len(summary_df)} zones")

    transform_taxi_data = PythonOperator(
        task_id = "transform_taxi_data", 
        python_callable = _transform_taxi_trip_records
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
                    avg_duration_mins FLOAT,
                    trip_count           INTEGER,
                    zone                 TEXT,
                    borough              TEXT,
                    lat                  FLOAT,
                    lon                  FLOAT,
                    execution_date       DATE
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
    


    download_yellow_taxi_trip_records >> convert_parquet_to_csv >> transform_taxi_data >> load_to_postgres
    download_taxi_zone_lookup >> transform_taxi_data
    get_taxi_shapefile >> convert_shapefile_to_geojson >> transform_taxi_data