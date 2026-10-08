#!/bin/bash
set -e

echo "Running Airflow database migrations..."
airflow db migrate

echo "Starting Airflow Standalone (Scheduler, API Server, Triggerer, DAG Processor)..."
exec airflow standalone