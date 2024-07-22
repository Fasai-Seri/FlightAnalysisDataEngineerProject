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
country_data_output_path = '/home/airflow/gcs/data/cleaned_country_data.parquet'

@task()
def get_flight_data_from_gcs(output_path):
    # read a file
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
    
@task()
def get_airport_data_from_gcs(output_path):
    # read a file
    df = pd.read_csv('data/airport.csv', ';')

    # transform column names
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # remove coordinates column
    df.drop(columns=['coordinates', 'country_name'], inplace=True)

    # clean and convert to appropriate data type
    df['city_name_geo_name_id'] = df['city_name_geo_name_id'].replace(r'\N', None).astype('Int64')
    df['country_name_geo_name_id'] = df['country_name_geo_name_id'].astype('Int64')
    
    # save as parquet
    df.to_parquet(output_path, index=False)
    print(f"output to {output_path}")

@task()
def get_country_data_from_website(output_path):
    # read data from html
    df = pd.read_html('https://countrycode.org/')
    df = df[0]
    
    # rename columns
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    # select only 2-digit iso codes
    df['iso_codes'] = df['iso_codes'].str.extract('(^\w{2})')
    df.rename(columns={'iso_codes': 'iso_code_2_digits'}, inplace=True)
    
    # select only necessary columns
    df = df.iloc[:,0:3]
    
    # save as parquet
    df.to_parquet(output_path, index=False)
    print(f'output to {output_path}')

@task()
def get_booking_data_from_gcs(output_path):  
    # read data
    df = pd.read_csv('data/booking.csv')
    
    # convert to datetime
    df['booking_timestamp'] = pd.to_datetime(df['booking_timestamp']) 
    
    # save as parquet
    df.to_parquet(output_path, index=False)
    print(f'output to {output_path}')
    
@task()
def get_customer_data_from_gcs(output_path):
    # read data
    df = pd.read_csv('data/customer.csv', dtype={'phone': str})
    
    # save as parquet 
    df.to_parquet(output_path, index=False)
    print(f'output to {output_path}')
    
@task()
def merge_booking_flight_and_clean(booking_data_path, flight_data_path):
    # read data
    booking = pd.read_parquet(booking_data_path)
    flight = pd.read_parquet(flight_data_path)
    
    # merge data
    df = pd.merge(booking, flight, how='inner', on='flight_id')
    
    # remove records with booking timestamp more than departure timestamp
    df = df[df['booking_timestamp'] < df['departing_timestamp']]
    
    
    
    
# @task()
# booking timestamp < departure timestamp

@dag(default_args=default_args, schedule_interval='@once', start_date=days_ago(1), tags=['practice'])
def flight_data_pipeline():
    """
    Extradt datasets from various sources and load into BigQuery
    """
    
    t1 = get_airport_data_from_gcs(output_path=airport_data_output_path)
    t2 = get_booking_data_from_gcs(output_path=booking_data_output_path)
    t3 = get_country_data_from_website(output_path=country_data_output_path)
    t4 = get_customer_data_from_gcs(output_path=customer_data_output_path)
    t5 = get_flight_data_from_gcs(output_path=flight_data_output_path)
    t6 = merge_booking_flight_and_clean(booking_data_path=booking_data_output_path, flight_data_path=flight_data_output_path)
    
    t7 = BashOperator(
        task_id = 'bq_load',
        bash_command = '''
        bq load --source_format=PARQUET flight_analysis.booking_with_flight gs://;
        bq load --source_format=PARQUET flight_analysis.airport gs://;
        bq load --source_format=PARQUET flight_analysis.country gs://;
        bq load --source_format=PARQUET flight_analysis.customer gs://
        '''
    )
    [t2, t5] >> t6
    [t1, t3, t4, t6] >> t7
    
    
    
    