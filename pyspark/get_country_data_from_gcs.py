import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder.appName('clean_country_data').getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--bucket_name", required=True)
args = parser.parse_args()

# read a file
country_data = spark.read.csv(f'gs://{args.bucket_name}/data/country.csv', header=True, inferSchema=True)

# rename columns
country_new_col_name = {col: col.lower().replace(' ', '_') for col in country_data.columns}
for old_col_name, new_column_name in country_new_col_name.items():
    country_data = country_data.withColumnRenamed(old_col_name, new_column_name)

# select only 2-digit iso codes
country_data = country_data.withColumn('iso_code_2_digits', regexp_extract('iso_codes', '(^\w{2})', 1))

# select only necessary columns
country_data = country_data.select('country', 'iso_code_2_digits')

# write in Google BigQuery
country_data.write.format('bigquery') \
        .option("table", "flight_analysis.country") \
        .option("temporaryGcsBucket", args.bucket_name) \
        .mode("overwrite") \
        .save()
        
spark.stop()