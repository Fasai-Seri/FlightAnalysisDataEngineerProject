import argparse
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('clean_airport_data').getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--bucket_name", required=True)
args = parser.parse_args()

# read a file
airport_data = spark.read.csv(f'gs://{args.bucket_name}/data/airport.csv', header=True, inferSchema=True)

# transform column names
airport_new_col_name = {col: col.lower().replace(' ', '_') for col in airport_data.columns}
for old_col_name, new_column_name in airport_new_col_name.items():
    airport_data = airport_data.withColumnRenamed(old_col_name, new_column_name)

# remove coordinates column
airport_data = airport_data.drop('coordinates', 'country_name')

# remove missing data (consider important column)
airport_data = airport_data.na.drop(subset=['country_code'])

# clean city_name_geo_name_id column
airport_data = airport_data.replace(r'\N', None, subset=['city_name_geo_name_id'])

# convert to appropriate data type
airport_data = airport_data.withColumn('city_name_geo_name_id', airport_data['city_name_geo_name_id'].cast('int'))
airport_data = airport_data.withColumn('country_name_geo_name_id', airport_data['country_name_geo_name_id'].cast('int'))

# write in Google BigQuery
airport_data.write.format('bigquery') \
        .option("table", "flight_analysis.airport") \
        .option("temporaryGcsBucket", args.bucket_name) \
        .mode("overwrite") \
        .save()

spark.stop()