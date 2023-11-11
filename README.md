#### Examen del curso pySpark

En este notebook encontrarás el examen de desarrollo en pySpark, respira profundo y manos a la obra. Si te bloqueas puedes salir y respirar aire fresco, realiza alguna otra actividad y retoma el examen en cuanto puedas. Mucho éxito.

#### Cómo utilizar el repositorio localmente

Para poder trabajar con el repositorio en tu propio PC es necesario tener un IDE como entorno de desarrollo así como tener el entorno preparado.

1. Clona el repositorio utilizando git:
```
git clone https://github.com/javiermolinag/exam_pyspark.git
```
2. Recomendamos tener python 3.9. Crea un ambiente virtual llamado *venv* con:
```bash
python -m venv venv
```

3. Activa el ambiente virtual con:
```bash
venv\Scripts\activate
```

4. Instala las dependencias con:
```bash
python -m pip install -r requirements.txt
```

5. Utiliza el siguiente comando en la terminal para trabajar con el notebook: 
```bash
jupyter-notebook
```

#### Ruta de tablas a utilizar

Las tablas que necesitarás se encuentran en la carpeta: `/resources/data/exam/parquet/`

#### Descripción de tablas

###### Tabla AIRLINES
    * AIRLINE_CODE: Identificador de aerolínea
    * AIRLINE_NAME: Nombre del aeropuerto
###### Tabla AIRPORTS
    * AIRPORT_CODE: Identificador del aeropuerto
    * AIRPORT: Nombre del aeropuerto
    * CITY: Nombre de la ciudad del aeropuerto
    * STATE: Código estatal del aeropuerto
    * COUNTRY: Nombre del país del aeropuerto
    * LATITUDE: Latitud del aeropuerto
    * LONGITUDE: Longitud del aeropuerto
###### Tabla FLIGHTS
    * ID: Identificador único para la tabla
    * ORIGIN_AIRPORT: Aeropuerto de inicio
    * DESTINATION_AIRPORT: Aeropuerto de destino
    * DISTANCE: Distancia (km) entre ambos aeropuertos
    * AIRLINE: Identificador de aerolínea
    * DAY: Día programado para del vuelo
    * MONTH: Mes programado para del vuelo
    * YEAR: Año programado para del vuelo
    * SCHEDULED_DEPARTURE: Hora de salida prevista (HHmm)
    * TAXI_IN: El tiempo transcurrido (minutos) entre el inicio del viaje y la llegada a la puerta del aeropuerto de destino
    * TAXI_OUT: El tiempo transcurrido (minutos) entre la salida de la puerta del aeropuerto de origen y el inicio del viaje
    * SCHEDULED_TIME: Cantidad de tiempo (minutos) planificado necesario para el viaje en avión
    * DIFF_TIME_ZONE: Diferencia de las zonas horarias entre el aeropuerto destino y el aeropuerto origen
    * CANCELLED: Vuelo cancelado (1 = cancelado)
    * CANCELLATION_REASON: Motivo de la cancelación del vuelo:
        A. Aerolínea/Transportista
        B. El tiempo
        C. Sistema Aéreo Nacional
        D. Seguridad
    * DIVERTED: Indica si el avión aterrizó en aeropuerto fuera de lo previsto
    * WEATHER_DELAY: Retraso causado por el clima
    * LATE_AIRCRAFT_DELAY: Retraso causado por avión
    * AIRLINE_DELAY: Retraso causado por la aerolínea
    * SECURITY_DELAY: Retraso causado por seguridad
    * AIR_SYSTEM_DELAY: Retraso causado por el sistema de aire
    * ARRIVAL_DELAY: Retraso en llegada a la puerta del aeropuerto de destino
    * DEPARTURE_DELAY: Retraso en salida de la salida de la puerta del aeropuerto de origen

#### Un poco de ayuda

1. Antes de realizar las transformaciones requeridas por el cliente, puedes leer la documentación de las siguientes funciones que seguramente te ayudarán a que el barco donde navegas siga a flote.

    Funciones recomendadas:
    * [concat](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.concat.html)
    * [concat_ws](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.concat_ws.html)
    * [dayofweek](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dayofweek.html)
    * [lpad](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lpad.html)
    * [to_date](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_date.html)
    * [substring](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.substring.html)
    * [unix_timestamp](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.unix_timestamp.html)
    * [to_unix_timestamp](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_unix_timestamp.html)
    * [when](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.when.html)
    * [row_number](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.row_number.html)
    * [dense_rank](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dense_rank.html)
    * [rank](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rank.html)

2. Adicional te recomiendamos realizar un método (lo vas a utilizar mucho en las reglas a desarrollar), el cual recibirá tres parámetros:

    ```
    modify_minuts_to_timestamp(timestamp_col: Column, minutes_col: Column, operation_type: str) -> Column
    ```
    
    * El primer argumento será un `Column` de tipo `TimestampType` en el formato `yyyy-MM-dd HH:mm:ss`
    * El segundo argumento será un `Column` de tipo `IntegerType` y representará la cantidad de minutos a sumar o restar
    * El tercer argumento será de tipo `str` y deberá tener alguno de los siguientes valores:
        * `add`, en este caso se sumará la cantidad de minutos al timestamp.
        * `sub`, en este caso se restará la cantidad de minutos al timestamp.
    
    Ejemplo:
    
    ```
   from pyspark.sql import SparkSession
   import pyspark.sql.functions as f
   import pyspark.sql.types as t
   from pyspark.sql import Column
   import datetime
   
   spark = SparkSession.builder \
    .appName("exam") \
    .master("local[*]") \
    .getOrCreate()

    def modify_minuts_to_timestamp(timestamp_col: Column, minutes_col: Column, operation_type: str) -> Column:
        ## Aqui va tu código
    
    schema = t.StructType([
       t.StructField("timestamp_col", t.TimestampType(), True),
       t.StructField("minutes_col", t.IntegerType(), True)])
    
    df = spark.createDataFrame([
        (datetime.datetime(2016,4,8,13,21,0), 20), 
        (datetime.datetime(2016,12,10,13,21,0), 480),
        (datetime.datetime(2020,1,22,22,15,0), 180)], schema)
    
    df.show()
    Output:
        +-------------------+-----------+
        |      timestamp_col|minutes_col|
        +-------------------+-----------+
        |2016-04-08 13:21:00|         20|
        |2016-12-10 13:21:00|        480|
        |2020-01-22 22:15:00|        180|
        +-------------------+-----------+
    
    new_column = modify_minuts_to_timestamp(f.col("timestamp_col"),f.col("minutes_col"),"add")
    df.withColumn("new_column", new_column.cast(t.TimestampType())).show()
    Output:
        +-------------------+-----------+-------------------+
        |      timestamp_col|minutes_col|         new_column|
        +-------------------+-----------+-------------------+
        |2016-04-08 13:21:00|         20|2016-04-08 13:41:00|
        |2016-12-10 13:21:00|        480|2016-12-10 21:21:00|
        |2020-01-22 22:15:00|        180|2020-01-23 01:15:00|
        +-------------------+-----------+-------------------+
    ```