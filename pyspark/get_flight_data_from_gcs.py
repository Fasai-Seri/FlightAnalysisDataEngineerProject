from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('clean_flight_data').getOrCreate()

# read a file
flight_data = spark.read.csv(input_path, header=True, inferSchema=True)

# flight can't depart and arrive in the same airport
flight_data = flight_data.filter("departure_airport != destination_airport")

'''assume that the range of each class is according to this table
    Flight Class     Minimum(Baht)     Maximum(Baht)
    Economy          1,000             30,000
    Business         10,000            100,000
    First            30,000            200,000'''
# select flight aligning with the assumption
flight_data = flight_data.filter(((flight_data['flight_class'] == 'Economy') & (flight_data['price_baht'] >= 1000) & (flight_data['price_baht'] <= 30000)) |
                   ((flight_data['flight_class'] == 'Business') & (flight_data['price_baht'] >= 10000) & (flight_data['price_baht'] <= 100000)) |
                   ((flight_data['flight_class'] == 'First') & (flight_data['price_baht'] >= 30000) & (flight_data['price_baht'] <= 200000)))

# save as parquet
flight_data.write.parquet(output_path)

spark.stop()