from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum


def say_hello():
    print("Hello from Airflow!")
    return "done"


with DAG(
    dag_id="00_hello",
    start_date=pendulum.datetime(2026, 10, 7, tz="UTC"),
    schedule=None,
):
    PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )