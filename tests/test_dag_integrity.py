import glob
import os
import pytest
from airflow.models import DagBag

DAG_PATH = os.path.join(os.path.dirname(__file__), "..", "dags/**/*.py")
DAG_FILES = glob.glob(DAG_PATH, recursive=True)

@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_integrity(dag_file):
    DagBag(dag_folder=dag_file, include_examples=False)