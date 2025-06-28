import os
from datetime import datetime, timedelta
import pandas as pd

from scripts.extract import extract
from scripts.transform import transform
from scripts.load import load
from scripts.validation import validation

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.Operator.python import PythonOperator

default_args = {
    "owner": "airflow_etl",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id='etl_pipeline',
    start_date=datetime(2025, 6, 26),
    schedule_interval="@daily"
    default_args=default_args,
    sla=timedelta(hours=2)
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
        op_kwargs= {
            "url": "https://data.tmd.go.th/api/WeatherToday/V2/?uid=api&ukey=api12345",
            
        }
    )