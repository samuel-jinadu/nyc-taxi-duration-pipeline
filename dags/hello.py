from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum


def say_hello():
    print("Hello from Airflow!")
    return "done"


with DAG(
    dag_id="00_hello",
    start_date=pendulum.today("UTC").add(days=-1),
    schedule=None,
):
    PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )