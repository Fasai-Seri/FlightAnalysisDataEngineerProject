from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder.appName('clean_country_data').getOrCreate()

# read a file
country_data = spark.read.csv(input_path, header=True, inferSchema=True)

# rename columns
country_new_col_name = {col: col.lower().replace(' ', '_') for col in country_data.columns}
for old_col_name, new_column_name in country_new_col_name.items():
    country_data = country_data.withColumnRenamed(old_col_name, new_column_name)

# select only 2-digit iso codes
country_data = country_data.withColumn('iso_code_2_digits', regexp_extract('iso_codes', '(^\w{2})', 1))

# select only necessary columns
country_data = country_data.select('country', 'iso_code_2_digits')

# save as parquet
country_data.write.parquet(output_path)

spark.stop()