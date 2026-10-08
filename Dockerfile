FROM apache/airflow:slim-3.3.2-python3.14

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN if [ -s /tmp/requirements.txt ]; then \
      pip install --no-cache-dir -r /tmp/requirements.txt; \
    fi