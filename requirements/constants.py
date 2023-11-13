from pyspark.sql import Row
import datetime


timestamp_format = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}"
date_format = "[0-9]{4}-[0-9]{2}-[0-9]{2}"

select_columns_narrow = ["ID", "DATE_FLIGHT_TRIP", "DAY_OF_WEEK", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "DISTANCE",
                         "AIRLINE", "SCHEDULED_DEPARTURE", "SCHEDULED_ARRIVAL", "SCHEDULED_TIME", "DIFF_TIME_ZONE",
                         "CANCELLED", "CANCELLATION_REASON", "DIVERTED", "DEPARTURE_TIME", "WHEELS_OFF", "AIR_TIME",
                         "WHEELS_ON", "ARRIVAL_TIME", "ELAPSED_TIME", "WEATHER_DELAY", "LATE_AIRCRAFT_DELAY",
                         "AIRLINE_DELAY", "SECURITY_DELAY", "AIR_SYSTEM_DELAY", "ARRIVAL_DELAY", "DEPARTURE_DELAY"]

narrow_ddl_schema = 'ID STRING,DATE_FLIGHT_TRIP DATE,DAY_OF_WEEK INT,ORIGIN_AIRPORT STRING,DESTINATION_AIRPORT ' \
                    'STRING,DISTANCE DOUBLE,AIRLINE STRING,SCHEDULED_DEPARTURE TIMESTAMP,SCHEDULED_ARRIVAL ' \
                    'TIMESTAMP,SCHEDULED_TIME INT,DIFF_TIME_ZONE INT,CANCELLED INT,CANCELLATION_REASON STRING,' \
                    'DIVERTED INT,DEPARTURE_TIME TIMESTAMP,WHEELS_OFF TIMESTAMP,AIR_TIME INT,WHEELS_ON ' \
                    'TIMESTAMP,ARRIVAL_TIME TIMESTAMP,ELAPSED_TIME INT,WEATHER_DELAY INT,LATE_AIRCRAFT_DELAY ' \
                    'INT,AIRLINE_DELAY INT,SECURITY_DELAY INT,AIR_SYSTEM_DELAY INT,ARRIVAL_DELAY INT,' \
                    'DEPARTURE_DELAY INT'

select_columns_wide = ["ID", "DATE_FLIGHT_TRIP", "DAY_OF_WEEK", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "DISTANCE",
                       "AIRLINE", "SCHEDULED_DEPARTURE", "SCHEDULED_ARRIVAL", "SCHEDULED_TIME", "DIFF_TIME_ZONE",
                       "CANCELLED", "CANCELLATION_REASON", "DIVERTED", "DEPARTURE_TIME", "WHEELS_OFF", "AIR_TIME",
                       "WHEELS_ON", "ARRIVAL_TIME", "ELAPSED_TIME", "WEATHER_DELAY", "LATE_AIRCRAFT_DELAY",
                       "AIRLINE_DELAY", "SECURITY_DELAY", "AIR_SYSTEM_DELAY", "ARRIVAL_DELAY", "DEPARTURE_DELAY",
                       "TOP_AIRLINE_ARRIVAL", "FLIGHT_NUMBER"]

wide_ddl_schema = 'ID STRING,DATE_FLIGHT_TRIP DATE,DAY_OF_WEEK INT,ORIGIN_AIRPORT STRING,DESTINATION_AIRPORT ' \
                  'STRING,DISTANCE DOUBLE,AIRLINE STRING,SCHEDULED_DEPARTURE TIMESTAMP,SCHEDULED_ARRIVAL ' \
                  'TIMESTAMP,SCHEDULED_TIME INT,DIFF_TIME_ZONE INT,CANCELLED INT,CANCELLATION_REASON STRING,' \
                  'DIVERTED INT,DEPARTURE_TIME TIMESTAMP,WHEELS_OFF TIMESTAMP,AIR_TIME INT,WHEELS_ON ' \
                  'TIMESTAMP,ARRIVAL_TIME TIMESTAMP,ELAPSED_TIME INT,WEATHER_DELAY INT,LATE_AIRCRAFT_DELAY ' \
                  'INT,AIRLINE_DELAY INT,SECURITY_DELAY INT,AIR_SYSTEM_DELAY INT,ARRIVAL_DELAY INT,' \
                  'DEPARTURE_DELAY INT,TOP_AIRLINE_ARRIVAL INT,FLIGHT_NUMBER STRING'

top_airline_arrival_output = [Row(AIRLINE='DL', TOP_AIRLINE_ARRIVAL=1, count=800329),
                              Row(AIRLINE='AS', TOP_AIRLINE_ARRIVAL=2, count=158054),
                              Row(AIRLINE='AA', TOP_AIRLINE_ARRIVAL=3, count=648694),
                              Row(AIRLINE='MQ', TOP_AIRLINE_ARRIVAL=4, count=272650),
                              Row(AIRLINE='UA', TOP_AIRLINE_ARRIVAL=5, count=469829),
                              Row(AIRLINE='WN', TOP_AIRLINE_ARRIVAL=6, count=1157339),
                              Row(AIRLINE='EV', TOP_AIRLINE_ARRIVAL=7, count=526249),
                              Row(AIRLINE='OO', TOP_AIRLINE_ARRIVAL=8, count=539544),
                              Row(AIRLINE='B6', TOP_AIRLINE_ARRIVAL=9, count=245135),
                              Row(AIRLINE='US', TOP_AIRLINE_ARRIVAL=10, count=198715),
                              Row(AIRLINE='VX', TOP_AIRLINE_ARRIVAL=11, count=56437),
                              Row(AIRLINE='HA', TOP_AIRLINE_ARRIVAL=12, count=70030),
                              Row(AIRLINE='F9', TOP_AIRLINE_ARRIVAL=13, count=82723),
                              Row(AIRLINE='NK', TOP_AIRLINE_ARRIVAL=14, count=107164)]

flight_number_output = [Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 7, 41), FLIGHT_NUMBER='OO-LAX-SAN-1-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 8, 0), FLIGHT_NUMBER='OO-LAX-SAN-2-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 9, 40), FLIGHT_NUMBER='OO-LAX-SAN-3-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 10, 20), FLIGHT_NUMBER='OO-LAX-SAN-4-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 10, 40), FLIGHT_NUMBER='OO-LAX-SAN-5-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 11, 35), FLIGHT_NUMBER='OO-LAX-SAN-6-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 11, 40), FLIGHT_NUMBER='OO-LAX-SAN-7-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 12, 55), FLIGHT_NUMBER='OO-LAX-SAN-8-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 13, 0), FLIGHT_NUMBER='OO-LAX-SAN-9-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 14, 19), FLIGHT_NUMBER='OO-LAX-SAN-10-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 14, 25), FLIGHT_NUMBER='OO-LAX-SAN-11-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 15, 35), FLIGHT_NUMBER='OO-LAX-SAN-12-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 16, 5), FLIGHT_NUMBER='OO-LAX-SAN-13-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 17, 5), FLIGHT_NUMBER='OO-LAX-SAN-14-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 17, 5), FLIGHT_NUMBER='OO-LAX-SAN-14-B'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 18, 20), FLIGHT_NUMBER='OO-LAX-SAN-15-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 19, 29), FLIGHT_NUMBER='OO-LAX-SAN-16-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 21, 0), FLIGHT_NUMBER='OO-LAX-SAN-17-A'),
                        Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 23, 10), FLIGHT_NUMBER='OO-LAX-SAN-18-A')]

query_1_output = [Row(AIRLINE='DL', TOP_AIRLINE_ARRIVAL=1, AIRLINE_NAME='Delta Air Lines Inc.'),
                  Row(AIRLINE='AS', TOP_AIRLINE_ARRIVAL=2, AIRLINE_NAME='Alaska Airlines Inc.'),
                  Row(AIRLINE='AA', TOP_AIRLINE_ARRIVAL=3, AIRLINE_NAME='American Airlines Inc.')]

query_2_output = [Row(AIRLINE='AA', AVG_WEATHER_DELAY=3.75, AVG_LATE_AIRCRAFT_DELAY=24.52, AVG_AIRLINE_DELAY=28.78,
                      AVG_SECURITY_DELAY=0.36, AVG_AIR_SYSTEM_DELAY=13.21),
                  Row(AIRLINE='AS', AVG_WEATHER_DELAY=2.64, AVG_LATE_AIRCRAFT_DELAY=26.6, AVG_AIRLINE_DELAY=23.18,
                      AVG_SECURITY_DELAY=0.49, AVG_AIR_SYSTEM_DELAY=14.72),
                  Row(AIRLINE='DL', AVG_WEATHER_DELAY=3.81, AVG_LATE_AIRCRAFT_DELAY=20.33, AVG_AIRLINE_DELAY=37.0,
                      AVG_SECURITY_DELAY=0.13, AVG_AIR_SYSTEM_DELAY=12.92)]

query_3_output = [
    Row(DAY_OF_WEEK='Friday', TOP=1, DEPARTURE_AIRPORT='Hartsfield-Jackson Atlanta International Airport',
        DEPARTURE_CITY='Atlanta', DEPARTURE_COUNT=50406,
        ARRIVAL_AIRPORT='Hartsfield-Jackson Atlanta International Airport', ARRIVAL_CITY='Atlanta',
        ARRIVAL_COUNT=50376),
    Row(DAY_OF_WEEK='Friday', TOP=2, DEPARTURE_AIRPORT="Chicago O'Hare International Airport",
        DEPARTURE_CITY='Chicago', DEPARTURE_COUNT=41198, ARRIVAL_AIRPORT="Chicago O'Hare International Airport",
        ARRIVAL_CITY='Chicago', ARRIVAL_COUNT=41127),
    Row(DAY_OF_WEEK='Friday', TOP=3, DEPARTURE_AIRPORT='Dallas/Fort Worth International Airport',
        DEPARTURE_CITY='Dallas-Fort Worth', DEPARTURE_COUNT=34024,
        ARRIVAL_AIRPORT='Dallas/Fort Worth International Airport', ARRIVAL_CITY='Dallas-Fort Worth',
        ARRIVAL_COUNT=34003),
    Row(DAY_OF_WEEK='Friday', TOP=4, DEPARTURE_AIRPORT='Denver International Airport', DEPARTURE_CITY='Denver',
        DEPARTURE_COUNT=28393, ARRIVAL_AIRPORT='Denver International Airport', ARRIVAL_CITY='Denver',
        ARRIVAL_COUNT=28349),
    Row(DAY_OF_WEEK='Friday', TOP=5, DEPARTURE_AIRPORT='Los Angeles International Airport',
        DEPARTURE_CITY='Los Angeles', DEPARTURE_COUNT=28078, ARRIVAL_AIRPORT='Los Angeles International Airport',
        ARRIVAL_CITY='Los Angeles', ARRIVAL_COUNT=28094),
    Row(DAY_OF_WEEK='Saturday', TOP=1, DEPARTURE_AIRPORT='Hartsfield-Jackson Atlanta International Airport',
        DEPARTURE_CITY='Atlanta', DEPARTURE_COUNT=40339,
        ARRIVAL_AIRPORT='Hartsfield-Jackson Atlanta International Airport', ARRIVAL_CITY='Atlanta',
        ARRIVAL_COUNT=40501),
    Row(DAY_OF_WEEK='Saturday', TOP=2, DEPARTURE_AIRPORT="Chicago O'Hare International Airport",
        DEPARTURE_CITY='Chicago', DEPARTURE_COUNT=32155, ARRIVAL_AIRPORT="Chicago O'Hare International Airport",
        ARRIVAL_CITY='Chicago', ARRIVAL_COUNT=32324),
    Row(DAY_OF_WEEK='Saturday', TOP=3, DEPARTURE_AIRPORT='Dallas/Fort Worth International Airport',
        DEPARTURE_CITY='Dallas-Fort Worth', DEPARTURE_COUNT=29126,
        ARRIVAL_AIRPORT='Dallas/Fort Worth International Airport', ARRIVAL_CITY='Dallas-Fort Worth',
        ARRIVAL_COUNT=29189),
    Row(DAY_OF_WEEK='Saturday', TOP=4, DEPARTURE_AIRPORT='Los Angeles International Airport',
        DEPARTURE_CITY='Los Angeles', DEPARTURE_COUNT=23865, ARRIVAL_AIRPORT='Los Angeles International Airport',
        ARRIVAL_CITY='Los Angeles', ARRIVAL_COUNT=23878),
    Row(DAY_OF_WEEK='Saturday', TOP=5, DEPARTURE_AIRPORT='Denver International Airport', DEPARTURE_CITY='Denver',
        DEPARTURE_COUNT=23131, ARRIVAL_AIRPORT='Denver International Airport', ARRIVAL_CITY='Denver',
        ARRIVAL_COUNT=23301),
    Row(DAY_OF_WEEK='Sunday', TOP=1, DEPARTURE_AIRPORT='Hartsfield-Jackson Atlanta International Airport',
        DEPARTURE_CITY='Atlanta', DEPARTURE_COUNT=49190,
        ARRIVAL_AIRPORT='Hartsfield-Jackson Atlanta International Airport', ARRIVAL_CITY='Atlanta',
        ARRIVAL_COUNT=49069),
    Row(DAY_OF_WEEK='Sunday', TOP=2, DEPARTURE_AIRPORT="Chicago O'Hare International Airport",
        DEPARTURE_CITY='Chicago', DEPARTURE_COUNT=38955, ARRIVAL_AIRPORT="Chicago O'Hare International Airport",
        ARRIVAL_CITY='Chicago', ARRIVAL_COUNT=38706),
    Row(DAY_OF_WEEK='Sunday', TOP=3, DEPARTURE_AIRPORT='Dallas/Fort Worth International Airport',
        DEPARTURE_CITY='Dallas-Fort Worth', DEPARTURE_COUNT=33042,
        ARRIVAL_AIRPORT='Dallas/Fort Worth International Airport', ARRIVAL_CITY='Dallas-Fort Worth',
        ARRIVAL_COUNT=32846),
    Row(DAY_OF_WEEK='Sunday', TOP=4, DEPARTURE_AIRPORT='Denver International Airport', DEPARTURE_CITY='Denver',
        DEPARTURE_COUNT=27871, ARRIVAL_AIRPORT='Denver International Airport', ARRIVAL_CITY='Denver',
        ARRIVAL_COUNT=27704),
    Row(DAY_OF_WEEK='Sunday', TOP=5, DEPARTURE_AIRPORT='Los Angeles International Airport',
        DEPARTURE_CITY='Los Angeles', DEPARTURE_COUNT=27404, ARRIVAL_AIRPORT='Los Angeles International Airport',
        ARRIVAL_CITY='Los Angeles', ARRIVAL_COUNT=27360)]
