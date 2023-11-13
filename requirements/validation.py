from typing import Union

import pyspark.sql.functions as f
from pyspark.sql import Column, DataFrame, SparkSession
from constants import *


spark = SparkSession.builder \
    .appName("exam") \
    .master("local[*]") \
    .getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "GMT-6")


def difference(l1: list, l2: list) -> list:
    return list(set(l1) - set(l2))


def get_column_name(col: Column) -> str:
    """
    PySpark doesn't allow you to directly access the column name with respect to aliases
    from an unbound column. We have to parse this out from the string representation.

    This works on columns with one or more aliases as well as un-aliased columns.

    Returns:
        Col name as str, with respect to aliasing
    """
    c = str(col).removeprefix("Column<'").removesuffix("'>")
    return c.split(' AS ')[-1]


def convert_timestamp_format(timestamp: Union[str, Column]) -> Column:
    """
    Converts a timestamp Colum to the HHmm format

    Returns:
        Column with the HHmm time format
    """
    if type(timestamp) == str:
        timestamp = f.col(timestamp)
    return f.concat(f.lpad(f.hour(timestamp), 2, "0"), f.lpad(f.minute(timestamp), 2, "0"))


def validate_regex_format(df: DataFrame, column_name: Union[str, Column], regex: str) -> None:
    """
    Validates a dataframe (df) into the given column (column) with the specified regex expression (regex)

    Returns:
        None
    """
    col = f.col(column_name) if type(column_name) == str else column_name
    assert df.filter(~col.rlike(regex)).count() == 0,\
        "Error al validar regex para la columna " + get_column_name(col)


def validate_eq_null_safe_columns(df: DataFrame, col_1: Union[str, Column], col_2: Union[str, Column]) -> None:
    """
    Validates a dataframe (df) with the secure null safe equal in the given columns: col_1 and col_2

    Returns:
        None
    """
    col_a = f.col(col_1) if type(col_1) == str else col_1
    col_b = f.col(col_2) if type(col_2) == str else col_2
    assert df.filter(~col_a.eqNullSafe(col_b)).count() == 0,\
        "Error al validar igualdad para las columnas " + get_column_name(col_a) + " y " + get_column_name(col_b)


def validate_not_null_columns(df: DataFrame, column_name: Union[str, Column]) -> None:
    """
    Validates a dataframe (df) into the given column (column) with not null values

    Returns:
        None
    """
    col = f.col(column_name) if type(column_name) == str else column_name
    assert df.filter(col.isNull()).count() == 0,\
        "Error al validar valores no null para la columna " + get_column_name(col)


def validate_schema(df: DataFrame, ddl_schema: str) -> None:
    assert schema_to_ddl(df).replace(" NOT NULL", "") == ddl_schema,\
        "Error al validar esquema"


def schema_to_ddl(df: DataFrame) -> str:
    """
    Extracts the DDL-schema from the given dataframe df

    Returns:
        DDL-schema in str format
    """
    return spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(df.schema.json()).toDDL()


def validation_narrow_transformations(narrow_flights_df: DataFrame) -> None:
    # Schema validation
    validate_schema(narrow_flights_df.select(*select_columns_narrow), narrow_ddl_schema)

    # Read data for column validation purposes
    data_validation_df: DataFrame = spark.read.parquet("../resources/data/exam/parquet/validation")
    validation_df: DataFrame = narrow_flights_df \
        .join(data_validation_df, ["ID"], "left") \
        .cache()

    # DATE_FLIGHT_TRIP column validation
    assert validation_df.select(f.to_date("DATE_FLIGHT_TRIP").alias("date")).distinct().count() == 365,\
        "La cantidad de fechas distintas para DATE_FLIGHT_TRIP debería ser 365"
    validate_regex_format(validation_df, "DATE_FLIGHT_TRIP", date_format)
    assert validation_df \
        .select(f.min(f.to_date("DATE_FLIGHT_TRIP")).alias("min_date"),
                f.max(f.to_date("DATE_FLIGHT_TRIP")).alias("max_date")) \
        .collect() == [Row(min_date=datetime.date(2015, 1, 1), max_date=datetime.date(2015, 12, 31))],\
        "La fecha minima para DATE_FLIGHT_TRIP debería ser 2015-01-01 y la fecha máxima 2015-12-31"

    # DAY_OF_WEEK column validation
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
                       Row(DAY_OF_WEEK=7, count=817760)],\
        "Error de validación al asignar los días de la semana en DAY_OF_WEEK"

    # SCHEDULED_DEPARTURE column validation
    validate_regex_format(validation_df, "SCHEDULED_DEPARTURE", timestamp_format)
    validate_eq_null_safe_columns(validation_df, convert_timestamp_format("SCHEDULED_DEPARTURE"),
                                  "SCHEDULED_DEPARTURE_")

    # SCHEDULED_ARRIVAL column validation
    validate_regex_format(validation_df, "SCHEDULED_ARRIVAL", timestamp_format)
    validate_eq_null_safe_columns(validation_df, convert_timestamp_format("SCHEDULED_ARRIVAL"),
                                  "SCHEDULED_ARRIVAL_")

    # DEPARTURE_TIME column validation
    validate_regex_format(validation_df, "DEPARTURE_TIME", timestamp_format)
    col_1 = convert_timestamp_format(f.col("DEPARTURE_TIME"))
    col_2 = f.when(f.col("DEPARTURE_TIME_") == "2400", "0000").otherwise(f.col("DEPARTURE_TIME_"))
    validate_eq_null_safe_columns(validation_df, col_1, col_2)

    # WHEELS_OFF column validation
    validate_regex_format(validation_df, "WHEELS_OFF", timestamp_format)
    col_1 = convert_timestamp_format(f.col("WHEELS_OFF"))
    col_2 = f.when(f.col("WHEELS_OFF_") == "2400", "0000").otherwise(f.col("WHEELS_OFF_"))
    validate_eq_null_safe_columns(validation_df, col_1, col_2)

    # ARRIVAL_TIME column validation
    validate_regex_format(validation_df, "ARRIVAL_TIME", timestamp_format)
    col_1 = convert_timestamp_format(f.col("ARRIVAL_TIME"))
    col_2 = f.when(f.col("ARRIVAL_TIME_") == "2400", "0000").otherwise(f.col("ARRIVAL_TIME_"))
    validate_eq_null_safe_columns(validation_df, col_1, col_2)

    # WHEELS_ON column validation
    validate_regex_format(validation_df, "WHEELS_ON", timestamp_format)
    col_1 = convert_timestamp_format(f.col("WHEELS_ON"))
    col_2 = f.when(f.col("WHEELS_ON_") == "2400", "0000").otherwise(f.col("WHEELS_ON_"))
    validate_eq_null_safe_columns(validation_df, col_1, col_2)

    # AIR_TIME column validation
    validate_eq_null_safe_columns(validation_df, "AIR_TIME", "AIR_TIME_")

    # ELAPSED_TIME column validation
    validate_eq_null_safe_columns(validation_df, "ELAPSED_TIME", "ELAPSED_TIME_")

    # NotNull values validation
    validate_not_null_columns(validation_df, "WEATHER_DELAY")
    validate_not_null_columns(validation_df, "LATE_AIRCRAFT_DELAY")
    validate_not_null_columns(validation_df, "AIRLINE_DELAY")
    validate_not_null_columns(validation_df, "SECURITY_DELAY")
    validate_not_null_columns(validation_df, "AIR_SYSTEM_DELAY")
    validate_not_null_columns(validation_df, "ARRIVAL_DELAY")
    validate_not_null_columns(validation_df, "DEPARTURE_DELAY")

    validation_df.unpersist()


def validation_wide_transformations(wide_flights_df: DataFrame) -> None:
    # Schema validation
    validation_df = wide_flights_df.select(*select_columns_wide).cache()
    validate_schema(validation_df, wide_ddl_schema)

    # TOP_AIRLINE_ARRIVAL column validation
    assert validation_df \
        .groupBy("AIRLINE", "TOP_AIRLINE_ARRIVAL").count() \
        .orderBy(f.col("TOP_AIRLINE_ARRIVAL").asc()) \
        .collect() == top_airline_arrival_output, "Error en la validación del resultado para TOP_AIRLINE_ARRIVAL"

    # ORIGIN_AIRPORT and DESTINATION_AIRPORT filtering validation
    assert validation_df.count() == 5332892, "El conteo de registros en la regla 2 es incorrcto"
    assert validation_df.filter(f.length(f.col("ORIGIN_AIRPORT")) != 3).count() == 0,\
        "En la salida de la regla 2 existen valores incorrectos para ORIGIN_AIRPORT"
    assert validation_df.filter(f.length(f.col("DESTINATION_AIRPORT")) != 3).count() == 0,\
        "En la salida de la regla 2 existen valores incorrectos para DESTINATION_AIRPORT"

    # FLIGHT_NUMBER column validation
    assert validation_df \
        .select("SCHEDULED_DEPARTURE", "FLIGHT_NUMBER") \
        .filter(f.col("DATE_FLIGHT_TRIP") == "2015-01-01") \
        .filter(f.col("AIRLINE") == "OO") \
        .filter(f.col("ORIGIN_AIRPORT") == "LAX") \
        .filter(f.col("DESTINATION_AIRPORT") == "SAN") \
        .orderBy(f.col("SCHEDULED_DEPARTURE").asc(), f.col("FLIGHT_NUMBER").asc()) \
        .collect() == flight_number_output, "Error en la validación del resultado para FLIGHT_NUMBER"

    validation_df.unpersist()


def validation_query_1(df: DataFrame) -> None:
    assert len(df.columns) == 3, "El total de columnas es incorrecto"
    assert df\
        .select("AIRLINE", "TOP_AIRLINE_ARRIVAL", "AIRLINE_NAME") \
        .orderBy(f.col("TOP_AIRLINE_ARRIVAL").asc()) \
        .collect() == query_1_output, "Error en la validación del resultado para la query"


def validation_query_2(df: DataFrame) -> None:
    assert len(df.columns) == 6, "El total de columnas es incorrecto"
    assert df \
        .select("AIRLINE", "AVG_WEATHER_DELAY", "AVG_LATE_AIRCRAFT_DELAY",
                "AVG_AIRLINE_DELAY", "AVG_SECURITY_DELAY", "AVG_AIR_SYSTEM_DELAY") \
        .orderBy(f.col("AIRLINE").asc()) \
        .collect() == query_2_output, "Error en la validación del resultado para la query"


def validation_query_3(df: DataFrame) -> None:
    assert len(df.columns) == 8, "El total de columnas es incorrecto"
    assert df\
        .select("DAY_OF_WEEK", "TOP", "DEPARTURE_AIRPORT", "DEPARTURE_CITY",
                "DEPARTURE_COUNT", "ARRIVAL_AIRPORT", "ARRIVAL_CITY", "ARRIVAL_COUNT") \
        .orderBy(f.col("DAY_OF_WEEK").asc(), f.col("TOP").asc()) \
        .collect() == query_3_output, "Error en la validación del resultado para la query"
