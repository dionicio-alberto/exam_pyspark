import pyspark.sql.functions as f
from pyspark.sql import Column, Row, DataFrame, SparkSession
import datetime

spark = SparkSession.builder \
    .appName("exam") \
    .master("local[*]") \
    .getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "GMT-6")


def difference(l1, l2):
    return list(set(l1) - set(l2))


def get_column_name(col: Column) -> str:
    """
    PySpark doesn't allow you to directly access the column name with respect to aliases
    from an unbound column. We have to parse this out from the string representation.

    This works on columns with one or more aliases as well as unaliased columns.

    Returns:
        Col name as str, with respect to aliasing
    """
    c = str(col).removeprefix("Column<'").removesuffix("'>")
    return c.split(' AS ')[-1]


def convert_timestamp_to_HHmm(timestamp):
    """
    Converts a timestamp Colum to the HHmm format

    Returns:
        Column with the HHmm time format
    """
    if type(timestamp) == str:
        timestamp = f.col(timestamp)
    return f.concat(f.lpad(f.hour(timestamp), 2, "0"), f.lpad(f.minute(timestamp), 2, "0"))


def validate_regex_format(df, column_name, regex):
    """
    Validates a dataframe (df) into the given column (column) with the specified regex expression (regex)

    Returns:
        None
    """
    if type(column_name) == str:
        column_name = f.col(column_name)
    assert df.filter(~column_name.rlike(regex)).count() == 0


def validate_eq_null_safe_columns(df, col_1, col_2):
    """
    Validates a dataframe (df) with the secure null safe equal in the given columns col_1 and col_2

    Returns:
        None
    """
    if type(col_1) == str:
        col_1 = f.col(col_1)
    if type(col_2) == str:
        col_2 = f.col(col_2)
    assert df.filter(~col_1.eqNullSafe(col_2)).count() == 0


def validate_not_null_columns(df, col_name):
    """
    Validates a dataframe (df) into the given column (column) with not null values

    Returns:
        None
    """
    if type(col_name) == str:
        col_name = f.col(col_name)
    assert df.filter(col_name.isNull()).count() == 0


def schema_to_ddl(df):
    """
    Extracts the DDL-schema from the given dataframe df

    Returns:
        DDL-schema in str format
    """
    return spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(df.schema.json()).toDDL()


def validation_narrow_transformations(narrow_flights_df):
    # Validación de esquema de salida
    expected_ddl_schema = 'ID STRING,DATE_FLIGHT_TRIP DATE,DAY_OF_WEEK INT,ORIGIN_AIRPORT STRING,DESTINATION_AIRPORT ' \
                          'STRING,DISTANCE DOUBLE,AIRLINE STRING,SCHEDULED_DEPARTURE TIMESTAMP,SCHEDULED_ARRIVAL ' \
                          'TIMESTAMP,SCHEDULED_TIME INT,DIFF_TIME_ZONE INT,CANCELLED INT,CANCELLATION_REASON STRING,' \
                          'DIVERTED INT,DEPARTURE_TIME TIMESTAMP,WHEELS_OFF TIMESTAMP,AIR_TIME INT,WHEELS_ON ' \
                          'TIMESTAMP,ARRIVAL_TIME TIMESTAMP,ELAPSED_TIME INT,WEATHER_DELAY INT,LATE_AIRCRAFT_DELAY ' \
                          'INT,AIRLINE_DELAY INT,SECURITY_DELAY INT,AIR_SYSTEM_DELAY INT,ARRIVAL_DELAY INT,' \
                          'DEPARTURE_DELAY INT'
    validation_schema_df = narrow_flights_df \
        .select("ID", "DATE_FLIGHT_TRIP", "DAY_OF_WEEK", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "DISTANCE", "AIRLINE",
                "SCHEDULED_DEPARTURE", "SCHEDULED_ARRIVAL", "SCHEDULED_TIME", "DIFF_TIME_ZONE", "CANCELLED",
                "CANCELLATION_REASON", "DIVERTED", "DEPARTURE_TIME", "WHEELS_OFF", "AIR_TIME", "WHEELS_ON",
                "ARRIVAL_TIME", "ELAPSED_TIME", "WEATHER_DELAY", "LATE_AIRCRAFT_DELAY", "AIRLINE_DELAY",
                "SECURITY_DELAY", "AIR_SYSTEM_DELAY", "ARRIVAL_DELAY", "DEPARTURE_DELAY")
    assert schema_to_ddl(validation_schema_df).replace(" NOT NULL", "") == expected_ddl_schema

    timestamp_format = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}"
    date_format = "[0-9]{4}-[0-9]{2}-[0-9]{2}"

    data_validation_df = spark.read.parquet("../resources/data/exam/parquet/validation")

    validation_df: DataFrame = narrow_flights_df \
        .join(data_validation_df, ["ID"], "left") \
        .cache()

    # Validación de DATE_FLIGHT_TRIP
    assert validation_df.select(f.to_date("DATE_FLIGHT_TRIP").alias("date")).distinct().count() == 365
    validate_regex_format(validation_df, "DATE_FLIGHT_TRIP", date_format)
    assert validation_df \
               .select(f.min(f.to_date("DATE_FLIGHT_TRIP")).alias("min_date"),
                       f.max(f.to_date("DATE_FLIGHT_TRIP")).alias("max_date")) \
               .collect() == [Row(min_date=datetime.date(2015, 1, 1), max_date=datetime.date(2015, 12, 31))]

    # Validación de DAY_OF_WEEK
    validate_eq_null_safe_columns(validation_df, "DAY_OF_WEEK", "DAY_OF_WEEK_")
    assert validation_df \
               .groupBy("DAY_OF_WEEK").count() \
               .orderBy(f.col("DAY_OF_WEEK").asc()) \
               .collect() == [Row(DAY_OF_WEEK=1, count=865539),
                              Row(DAY_OF_WEEK=2, count=844596),
                              Row(DAY_OF_WEEK=3, count=855894),
                              Row(DAY_OF_WEEK=4, count=872519),
                              Row(DAY_OF_WEEK=5, count=862208),
                              Row(DAY_OF_WEEK=6, count=700541),
                              Row(DAY_OF_WEEK=7, count=817760)]

    # Validación de SCHEDULED_DEPARTURE
    validate_regex_format(validation_df, "SCHEDULED_DEPARTURE", timestamp_format)
    validate_eq_null_safe_columns(validation_df, convert_timestamp_to_HHmm("SCHEDULED_DEPARTURE"),
                                  "SCHEDULED_DEPARTURE_")

    # Validación de SCHEDULED_ARRIVAL
    validate_regex_format(validation_df, "SCHEDULED_ARRIVAL", timestamp_format)
    validate_eq_null_safe_columns(validation_df, convert_timestamp_to_HHmm("SCHEDULED_ARRIVAL"),
                                  "SCHEDULED_ARRIVAL_")

    # Validación de DEPARTURE_TIME
    validate_regex_format(validation_df, "DEPARTURE_TIME", timestamp_format)
    col_1 = convert_timestamp_to_HHmm(f.col("DEPARTURE_TIME"))
    col_2 = f.when(f.col("DEPARTURE_TIME_") == "2400", "0000").otherwise(f.col("DEPARTURE_TIME_"))
    validate_eq_null_safe_columns(validation_df, col_1, col_2)

    # Validación de WHEELS_OFF
    validate_regex_format(validation_df, "WHEELS_OFF", timestamp_format)
    col_1 = convert_timestamp_to_HHmm(f.col("WHEELS_OFF"))
    col_2 = f.when(f.col("WHEELS_OFF_") == "2400", "0000").otherwise(f.col("WHEELS_OFF_"))
    validate_eq_null_safe_columns(validation_df, col_1, col_2)

    # Validación de ARRIVAL_TIME
    validate_regex_format(validation_df, "ARRIVAL_TIME", timestamp_format)
    col_1 = convert_timestamp_to_HHmm(f.col("ARRIVAL_TIME"))
    col_2 = f.when(f.col("ARRIVAL_TIME_") == "2400", "0000").otherwise(f.col("ARRIVAL_TIME_"))
    validate_eq_null_safe_columns(validation_df, col_1, col_2)

    # Validación de WHEELS_ON
    validate_regex_format(validation_df, "WHEELS_ON", timestamp_format)
    col_1 = convert_timestamp_to_HHmm(f.col("WHEELS_ON"))
    col_2 = f.when(f.col("WHEELS_ON_") == "2400", "0000").otherwise(f.col("WHEELS_ON_"))
    validate_eq_null_safe_columns(validation_df, col_1, col_2)

    # Validación de AIR_TIME
    validate_eq_null_safe_columns(validation_df, "AIR_TIME", "AIR_TIME_")

    # Validación de ELAPSED_TIME
    validate_eq_null_safe_columns(validation_df, "ELAPSED_TIME", "ELAPSED_TIME_")

    # Validación de valores null
    validate_not_null_columns(validation_df, "WEATHER_DELAY")
    validate_not_null_columns(validation_df, "LATE_AIRCRAFT_DELAY")
    validate_not_null_columns(validation_df, "AIRLINE_DELAY")
    validate_not_null_columns(validation_df, "SECURITY_DELAY")
    validate_not_null_columns(validation_df, "AIR_SYSTEM_DELAY")
    validate_not_null_columns(validation_df, "ARRIVAL_DELAY")
    validate_not_null_columns(validation_df, "DEPARTURE_DELAY")

    validation_df.unpersist()


def validation_wide_transformations(wide_flights_df):
    # Validación de esquema de salida con las nuevas columnas agregadas
    expected_ddl_schema = 'ID STRING,DATE_FLIGHT_TRIP DATE,DAY_OF_WEEK INT,ORIGIN_AIRPORT STRING,DESTINATION_AIRPORT ' \
                          'STRING,DISTANCE DOUBLE,AIRLINE STRING,SCHEDULED_DEPARTURE TIMESTAMP,SCHEDULED_ARRIVAL ' \
                          'TIMESTAMP,SCHEDULED_TIME INT,DIFF_TIME_ZONE INT,CANCELLED INT,CANCELLATION_REASON STRING,' \
                          'DIVERTED INT,DEPARTURE_TIME TIMESTAMP,WHEELS_OFF TIMESTAMP,AIR_TIME INT,WHEELS_ON ' \
                          'TIMESTAMP,ARRIVAL_TIME TIMESTAMP,ELAPSED_TIME INT,WEATHER_DELAY INT,LATE_AIRCRAFT_DELAY ' \
                          'INT,AIRLINE_DELAY INT,SECURITY_DELAY INT,AIR_SYSTEM_DELAY INT,ARRIVAL_DELAY INT,' \
                          'DEPARTURE_DELAY INT,TOP_AIRLINE_ARRIVAL INT,FLIGHT_NUMBER STRING'
    validation_df = wide_flights_df \
        .select("ID", "DATE_FLIGHT_TRIP", "DAY_OF_WEEK", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "DISTANCE", "AIRLINE",
                "SCHEDULED_DEPARTURE", "SCHEDULED_ARRIVAL", "SCHEDULED_TIME", "DIFF_TIME_ZONE", "CANCELLED",
                "CANCELLATION_REASON", "DIVERTED", "DEPARTURE_TIME", "WHEELS_OFF", "AIR_TIME", "WHEELS_ON",
                "ARRIVAL_TIME", "ELAPSED_TIME", "WEATHER_DELAY", "LATE_AIRCRAFT_DELAY", "AIRLINE_DELAY",
                "SECURITY_DELAY", "AIR_SYSTEM_DELAY", "ARRIVAL_DELAY", "DEPARTURE_DELAY", "TOP_AIRLINE_ARRIVAL",
                "FLIGHT_NUMBER").cache()
    assert schema_to_ddl(validation_df).replace(" NOT NULL", "") == expected_ddl_schema

    # Validación de TOP_AIRLINE_ARRIVAL
    expected_output = [Row(AIRLINE='DL', TOP_AIRLINE_ARRIVAL=1, count=800329),
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
    assert validation_df \
               .groupBy("AIRLINE", "TOP_AIRLINE_ARRIVAL").count() \
               .orderBy(f.col("TOP_AIRLINE_ARRIVAL").asc()) \
               .collect() == expected_output

    # Validación de eliminación de filas
    assert validation_df.count() == 5332892
    assert validation_df.select("ORIGIN_AIRPORT").distinct().count() == 322
    assert validation_df.filter(f.length(f.col("ORIGIN_AIRPORT")) != 3).count() == 0
    assert validation_df.select("DESTINATION_AIRPORT").distinct().count() == 322
    assert validation_df.filter(f.length(f.col("DESTINATION_AIRPORT")) != 3).count() == 0

    # Validación de FLIGHT_NUMBER
    expected_output = [Row(SCHEDULED_DEPARTURE=datetime.datetime(2015, 1, 1, 7, 41), FLIGHT_NUMBER='OO-LAX-SAN-1-A'),
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
    assert validation_df \
               .select("SCHEDULED_DEPARTURE", "FLIGHT_NUMBER") \
               .filter(f.col("DATE_FLIGHT_TRIP") == "2015-01-01") \
               .filter(f.col("AIRLINE") == "OO") \
               .filter(f.col("ORIGIN_AIRPORT") == "LAX") \
               .filter(f.col("DESTINATION_AIRPORT") == "SAN") \
               .orderBy(f.col("SCHEDULED_DEPARTURE").asc(), f.col("FLIGHT_NUMBER").asc()) \
               .collect() == expected_output
    validation_df.unpersist()


def validation_query_1(df):
    expected_output = [Row(AIRLINE='DL', TOP_AIRLINE_ARRIVAL=1, AIRLINE_NAME='Delta Air Lines Inc.'),
                       Row(AIRLINE='AS', TOP_AIRLINE_ARRIVAL=2, AIRLINE_NAME='Alaska Airlines Inc.'),
                       Row(AIRLINE='AA', TOP_AIRLINE_ARRIVAL=3, AIRLINE_NAME='American Airlines Inc.')]
    assert len(df.columns) == 3
    assert df\
        .select("AIRLINE", "TOP_AIRLINE_ARRIVAL", "AIRLINE_NAME") \
        .orderBy(f.col("TOP_AIRLINE_ARRIVAL").asc()) \
        .collect() == expected_output


def validation_query_2(df):
    expected_output = [Row(AIRLINE='AA', AVG_WEATHER_DELAY=3.75, AVG_LATE_AIRCRAFT_DELAY=24.52, AVG_AIRLINE_DELAY=28.78,
                           AVG_SECURITY_DELAY=0.36, AVG_AIR_SYSTEM_DELAY=13.21),
                       Row(AIRLINE='AS', AVG_WEATHER_DELAY=2.64, AVG_LATE_AIRCRAFT_DELAY=26.6, AVG_AIRLINE_DELAY=23.18,
                           AVG_SECURITY_DELAY=0.49, AVG_AIR_SYSTEM_DELAY=14.72),
                       Row(AIRLINE='DL', AVG_WEATHER_DELAY=3.81, AVG_LATE_AIRCRAFT_DELAY=20.33, AVG_AIRLINE_DELAY=37.0,
                           AVG_SECURITY_DELAY=0.13, AVG_AIR_SYSTEM_DELAY=12.92)]
    assert len(df.columns) == 6
    assert df \
               .select("AIRLINE", "AVG_WEATHER_DELAY", "AVG_LATE_AIRCRAFT_DELAY", "AVG_AIRLINE_DELAY",
                       "AVG_SECURITY_DELAY", "AVG_AIR_SYSTEM_DELAY") \
               .orderBy(f.col("AIRLINE").asc()) \
               .collect() == expected_output


def validation_query_3(df):
    expected_schema = 'DAY_OF_WEEK STRING,TOP INT,DEPARTURE_AIRPORT STRING,DEPARTURE_CITY STRING,DEPARTURE_COUNT BIGINT,ARRIVAL_AIRPORT STRING,ARRIVAL_CITY STRING,ARRIVAL_COUNT BIGINT'
    expected_output = [
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
    assert len(df.columns) == 8
    validation_schema_df = df.select("DAY_OF_WEEK", "TOP", "DEPARTURE_AIRPORT", "DEPARTURE_CITY",
                                           "DEPARTURE_COUNT", "ARRIVAL_AIRPORT", "ARRIVAL_CITY", "ARRIVAL_COUNT")
    assert schema_to_ddl(validation_schema_df).replace(" NOT NULL", "") == expected_schema
    assert df.orderBy(f.col("DAY_OF_WEEK").asc(), f.col("TOP").asc()).collect() == expected_output
