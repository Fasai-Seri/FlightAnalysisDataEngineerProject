"""
Improved version using PySpark to clean data
"""

from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os

# get bucket name
bucket_name = os.getenv('BUCKET_NAME')
if not bucket_name:
    raise ValueError("Bucket name not set in environment variables")

# default arguments
default_args = {
    'owner': 'Fasai',
}

get_airport_data_from_gcs_pyspark_job = {
    "reference": {"project_id": "dataengineerproject-430202"},
    "placement": {"cluster_name": "cluster-flight-analysis"},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{bucket_name}/scripts/get_airport_data_from_gcs.py",
        "args": [
            '--bucket_name', bucket_name
        ]
    },
}

get_country_data_from_gcs_pyspark_job = {
    "reference": {"project_id": "dataengineerproject-430202"},
    "placement": {"cluster_name": "cluster-flight-analysis"},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{bucket_name}/scripts/get_country_data_from_gcs.py",
        "args": [
            '--bucket_name', bucket_name
        ]
    },
}

merge_booking_flight_and_clean_pyspark_job = {
    "reference": {"project_id": "dataengineerproject-430202"},
    "placement": {"cluster_name": "cluster-flight-analysis"},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{bucket_name}/scripts/merge_booking_flight_and_clean.py",
        "args": [
            '--bucket_name', bucket_name
        ]
    },
}

@dag(default_args=default_args, schedule_interval='@once', start_date=days_ago(1), tags=['practice'])
def flight_data_pipeline():
    """
    Extract datasets from sources clean and load into BigQuery
    """

    t1 = DataprocSubmitJobOperator(
        task_id = 'get_airport_data_from_gcs',
        region = 'us-central1',
        job = get_airport_data_from_gcs_pyspark_job,
    )

    t2 = DataprocSubmitJobOperator(
        task_id = 'get_country_data_from_gcs',
        region = 'us-central1',
        job = get_country_data_from_gcs_pyspark_job
    )

    t3 = DataprocSubmitJobOperator(
        task_id = 'merge_booking_flight_and_clean',
        region = 'us-central1',
        job = merge_booking_flight_and_clean_pyspark_job
    )

    t4 = GCSToBigQueryOperator(
        task_id = 'get_customer_data_from_gcs',
        bucket = bucket_name,           
        source_objects = ['data/customer.csv'],      
        destination_project_dataset_table = 'dataengineerproject-430202:flight_analysis.customer', 
        source_format = 'CSV',                    
        create_disposition = 'CREATE_IF_NEEDED',     
        write_disposition = 'WRITE_TRUNCATE',        
        schema_fields = [
            {'name': 'customer_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'phone', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'country_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
    )
    # no dependency, run every task independently

flight_data_pipeline()