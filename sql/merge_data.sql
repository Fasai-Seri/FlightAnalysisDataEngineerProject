SELECT 
dep_a.airport_name AS dep_airport_name,
dep_a.city_name AS dep_city_name,
dep_a.country_code AS dep_country_code,
des_a.airport_name AS des_airport_name,
des_a.city_name AS des_city_name,
des_a.country_code AS des_country_code,
dep_c.country AS dep_country,
des_c.country AS des_country,
b.*,
cus.first_name, cus.last_name, cus.email, cus.phone, cus.country_code AS cus_country_code, cus_c.country

FROM flight_analysis.airport dep_a 
JOIN flight_analysis.booking_with_flight b ON dep_a.airport_code = b.departure_airport
JOIN flight_analysis.airport des_a ON des_a.airport_code = b.destination_airport
JOIN flight_analysis.country dep_c ON dep_c.iso_code_2_digits = dep_a.country_code
JOIN flight_analysis.country des_c ON des_c.iso_code_2_digits = des_a.country_code
JOIN flight_analysis.customer cus ON cus.customer_id = b.customer_id
JOIN flight_analysis.country cus_c ON cus_c.iso_code_2_digits = cus.country_code