import os
from datetime import datetime, timedelta
import pandas as pd

from scripts.extract import extract
from scripts.transform import transform
from scripts.load import load
from scripts.validation import validation

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow_etl",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id='etl_pipeline',
    start_date=datetime(2025, 6, 26),
    schedule_interval="@daily",
    default_args=default_args,
    sla=timedelta(hours=2)
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract,
        op_kwargs= {
            "url": "https://data.tmd.go.th/api/WeatherToday/V2/?uid=api&ukey=api12345",
            "output_path": "etl_project1/data/raw_data.csv"
        }
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform,
        op_kwargs= {
            "input_path": "etl_project1/data/raw_data.csv",
            "output_path": "etl_project1/data/cleaned_data.csv"
        }
    )

    validation_task = PythonOperator(
        task_id="validation_task",
        python_callable=validation,
        op_kwargs= {
            "file_path": "etl_project1/data/cleaned_data.csv"
        }
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load,
        op_kwargs= {
            "input_path": "etl_project1/data/cleaned_data.csv",
            "output_path": "bigquery_data_warehouse" 
        }
    )

extract_task >> transform_task >> validation_task >> load_task