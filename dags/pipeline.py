from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd

# default arguments
default_args = {
    'owner': 'datath',
}

# output path for saving data
flight_data_output_path = '/home/airflow/gcs/data/cleaned_flight_data.parquet'
airport_data_output_path = '/home/airflow/gcs/data/cleaned_airport_data.parquet'
booking_data_output_path = '/home/airflow/gcs/data/cleaned_booking_data.parquet'
customer_data_output_path = '/home/airflow/gcs/data/cleaned_customer_data.parquet'