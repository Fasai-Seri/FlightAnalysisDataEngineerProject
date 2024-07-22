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

@task()
def get_flight_data_from_gcs(output_path):
    df = pd.read_csv('data/flight.csv')

    # flight can't depart and arrive in the same airport
    df.drop(index=df[df['departure_airport'] == df['destination_airport']].index, inplace=True)

    '''assume that the range of each class is according to this table
    Flight Class     Minimum(Baht)     Maximum(Baht)
    Economy          1,000             30,000
    Business         10,000            100,000
    First            30,000            200,000'''
    # select flight aligning with the assumption
    df = df[((df['flight_class'] == 'Economy') & (df['price(baht)'] >= 1000) & (df['price(baht)'] <=30000)) |
        ((df['flight_class'] == 'Business') & (df['price(baht)'] >= 10000) & (df['price(baht)'] <=100000)) |
        ((df['flight_class'] == 'First') & (df['price(baht)'] >= 30000) & (df['price(baht)'] <=200000))]

    # convert to datetime
    df['departing_timestamp'] = pd.to_datetime(df['departing_timestamp'])
    
    # save as parquet
    df.to_parquet(output_path, index=False)
    print(f"Output to {output_path}")