import pandas as pd
import requests
import random
import datetime
from faker import Faker

# generate flight dataset from extracted airport data
def random_timestamp(start_year=2010, end_year=2024):
    start_date = datetime.datetime(start_year, 1, 1)
    end_date = datetime.datetime(end_year, 12, 31, 23, 59)
    random_date = start_date + (end_date - start_date) * random.random()
    return random_date

airport = pd.read_csv('./data/airport.csv', delimiter=';')

with open('./data/flight.csv', 'w') as f:
    f.write('flight_id,departure_airport,destination_airport,departing_timestamp,duration(hours),price(baht),flight_class\n')
    for i in range(10000):
        departure_airport = random.choice(airport['Airport Code'])
        destination_airport = random.choice(airport['Airport Code'])
        departing_timestamp = random_timestamp()
        duration = random.randrange(0,15)
        price = random.randrange(500, 50000)
        flight_class = random.choice(['Economy', 'Business', 'First'])
        f.write(f'{i},{departure_airport},{destination_airport},{departing_timestamp},{duration},{price},{flight_class}\n')
        
# generate customer data
fake = Faker()

with open('./data/customer.csv', 'w') as f:
    f.write('customer_id,first_name,last_name,email,phone\n')
    for i in range(10000):
        f.write(f'{i},{fake.name().split()[0]},{fake.name().split()[0]},{fake.email()},{fake.phone_number()}\n')

# generate booking data
with open('./data/booking.csv', 'w') as f:
    f.write('booking_id,booking_timestamp,flight_id,customer_id,quantity\n')
    for i in range(100000):
        booking_timestamp = random_timestamp()
        flight_id = random.randrange(0, 9999)
        customer_id = random.randrange(0, 9999)
        quantity = random.randrange(1, 5)
        f.write(f'{i},{flight_id},{customer_id},{quantity}\n')