from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('clean_airport_data').getOrCreate()

# read a file
airport_data = spark.read.csv(input_path, header=True)

# transform column names
airport_new_col_name = {col: col.lower().replace(' ', '_') for col in airport_data.columns}
for old_col_name, new_column_name in airport_new_col_name.items():
    airport_data = airport_data.withColumnRenamed(old_col_name, new_column_name)

# remove coordinates column
airport_data = airport_data.drop('coordinates', 'country_name')

# remove missing data
airport_data = airport_data.na.drop(subset=['country_code'])

# clean city_name_geo_name_id column
airport_data = airport_data.replace(r'\N', None, subset=['city_name_geo_name_id'])

# convert to appropriate data type
airport_data = airport_data.withColumn('city_name_geo_name_id', airport_data['city_name_geo_name_id'].cast('int'))
airport_data = airport_data.withColumn('country_name_geo_name_id', airport_data['country_name_geo_name_id'].cast('int'))

# save as parquet
flight_data.write.parquet(output_path)