import random
import datetime
import pandas as pd
from faker import Faker

# extract airport data
df = pd.read_csv('https://data.opendatasoft.com/api/explore/v2.1/catalog/datasets/airports-code@public/exports/csv?lang=en&timezone=Asia%2FJakarta&use_labels=true&delimiter=%3B', delimiter=';')
df.to_csv('data/airport.csv', index=False)

# generate flight dataset from extracted airport data
def random_timestamp(start_year=2010, end_year=2024):
    start_date = datetime.datetime(start_year, 1, 1)
    end_date = datetime.datetime(end_year, 12, 31, 23, 59)
    random_date = start_date + (end_date - start_date) * random.random()
    return random_date

airport = pd.read_csv('./data/airport.csv')

with open('./data/flight.csv', 'w') as f:
    f.write('flight_id,departure_airport,destination_airport,departing_timestamp,duration_hours,price_baht,flight_class\n')
    for i in range(200000):
        departure_airport = random.choice(airport['Airport Code'])
        destination_airport = random.choice(airport['Airport Code'])
        departing_timestamp = random_timestamp()
        duration = random.randrange(1,15)
        price = random.randrange(500, 50000)
        flight_class = random.choice(['Economy', 'Business', 'First'])
        f.write(f'{i},{departure_airport},{destination_airport},{departing_timestamp},{duration},{price},{flight_class}\n')

# extract country data
df = pd.read_html('https://countrycode.org/')
df = df[0]
df.to_csv('./data/country.csv')
        
# generate customer data
fake = Faker()

country = pd.read_csv('./data/country.csv')

with open('./data/customer.csv', 'w') as f:
    f.write('customer_id,first_name,last_name,email,phone,country_code\n')
    for i in range(100000):
        choices = list('1234567890')
        phone = ''.join(['0'] + random.choices(choices, k=9))
        country_code = random.choice(list(country['ISO CODES'].apply(lambda s: s[:2])))
        f.write(f'{i},{fake.name().split()[0]},{fake.name().split()[0]},{fake.email()},{phone},{country_code}\n')

# generate booking data
with open('./data/booking.csv', 'w') as f:
    f.write('booking_id,booking_timestamp,flight_id,customer_id,quantity\n')
    for i in range(400000):
        booking_timestamp = random_timestamp()
        flight_id = random.randrange(0, 9999)
        customer_id = random.randrange(0, 9999)
        quantity = random.randrange(1, 5)
        f.write(f'{i},{booking_timestamp},{flight_id},{customer_id},{quantity}\n')