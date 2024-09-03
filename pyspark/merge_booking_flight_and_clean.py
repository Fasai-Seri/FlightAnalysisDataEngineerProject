from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('clean_flight_data').getOrCreate()

def get_flight_data_from_gcs(input_path):
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

    return flight_data

def get_booking_data_from_gcs(input_path):
    booking_data = spark.read.csv(input_path, header=True, inferSchema=True)
    return booking_data

def merge_booking_flight_and_clean():
    # merge data
    joined_booking_flight = booking_data.join(flight_data, on='flight_id', how="inner")

    # remove records with booking timestamp more than departure timestamp
    joined_booking_flight = joined_booking_flight.filter('booking_timestamp < departing_timestamp')

    return joined_booking_flight

flight_data = get_flight_data_from_gcs(flight_input_path)
booking_data = get_booking_data_from_gcs(booking_input_path)
joined_booking_flight = merge_booking_flight_and_clean(booking_data, flight_data)

# save as parquet
joined_booking_flight.write.parquet(output_path)

spark.stop()