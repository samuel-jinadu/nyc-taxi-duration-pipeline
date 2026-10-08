FROM apache/airflow:slim-3.3.2-python3.14

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN if [ -s /tmp/requirements.txt ]; then \
      pip install --no-cache-dir -r /tmp/requirements.txt; \
    fi

# Copy the startup script and make it executable
COPY ./scripts/start-airflow.sh /start-airflow.sh
USER root
RUN chmod +x /start-airflow.sh
USER airflow

# Override the default command with our script
CMD ["bash", "/start-airflow.sh"]