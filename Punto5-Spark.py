import requests
import sys
import configparser
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import expr

# Funcion que consulta el aeropuerto pasado por parametro.
def consultar_aeropuerto(iata_code, spark, config):

    # Obtengo configuracion de API
    api_url = config["API"]["url"]
    api_key = config["API"]["api_key"]

    # Armo el endpoint
    endpoint = f"{api_url}airports?iata_code={iata_code}&api_key={api_key}"

    # Hacer la solicitud al API de AirLabs para obtener información sobre vuelos activos
    response = requests.get(endpoint)

    # Verifico si la solicitud fue exitosa
    if response.status_code == 200:

        # Obtengo los datos de aeropuerto de la respuesta JSON
        data = response.json()

        # Tomo de la respuesta el Response
        response_data = data['response']

        # Creo DataFrame con la información del aeropuerto
        df_aeropuerto_info = spark.read.json(spark.sparkContext.parallelize(response_data))

        #df_aeropuerto_info.show()

        # Obtengo las variables para guardar en archivo
        output_path = config["OUTPUT"]["output_path"]
        output_filename = config["OUTPUT"]["output_filename"]
        output_format = config["OUTPUT"]["output_format"]
        output_delimiter = config["OUTPUT"]["output_delimiter"]
        output_encode = config["OUTPUT"]["output_encode"]
        
        # Guardo el DataFrame en el formato especificado en la configuración
        df_aeropuerto_info.coalesce(1).write \
            .mode("overwrite") \
            .format(output_format) \
            .option("header", "true") \
            .option("delimiter", output_delimiter) \
            .option("header", "true") \
            .option("encoding", output_encode) \
            .save(f"{output_path}/{output_filename}")
    
    else:
        # Si la solicitud falla, imprimo por consola el error.
        print("Error al obtener los datos de la API. Código de estado:", response.status_code)

# Funcion que consulta los vuelos proximos a llegar en 30 min a un aeropuerto
def vuelos_arrivo_proximos_30_min(config, spark):
    
    # Obtengo configuracion de API
    api_url = config["API"]["url"]
    api_key = config["API"]["api_key"]
    codigo_aerolinea = config["API"]["codigo_aerolinea"]
    status_vuelo = config["API"]["status_vuelo"]
    path_log = config["LOG"]["log_file_path"]

    # Armo el endpoint
    endpoint = f"{api_url}schedules?airline_iata={codigo_aerolinea}&status={status_vuelo}&api_key={api_key}"

    # Hacer la solicitud al API de AirLabs para obtener información sobre vuelos activos
    response = requests.get(endpoint)

    # Verifico si la solicitud fue exitosa
    if response.status_code == 200:
        
        # Obtengo los datos de vuelo de la respuesta JSON
        datos_vuelos = response.json()

        #Obtengo solo el response de la respuesta
        response_data = datos_vuelos['response']

        # Defino el esquema del DataFrame
        schema = StructType([
        StructField("airline_iata", StringType(), nullable=True),
        StructField("airline_icao", StringType(), nullable=True),
        StructField("flight_iata", StringType(), nullable=True),
        StructField("flight_icao", StringType(), nullable=True),
        StructField("flight_number", StringType(), nullable=True),
        StructField("dep_iata", StringType(), nullable=True),
        StructField("dep_icao", StringType(), nullable=True),
        StructField("dep_terminal", StringType(), nullable=True),
        StructField("dep_gate", StringType(), nullable=True),
        StructField("dep_time", StringType(), nullable=True),
        StructField("dep_time_utc", StringType(), nullable=True),
        StructField("dep_estimated", StringType(), nullable=True),
        StructField("dep_estimated_utc", StringType(), nullable=True),
        StructField("dep_actual", StringType(), nullable=True),
        StructField("dep_actual_utc", StringType(), nullable=True),
        StructField("arr_iata", StringType(), nullable=True),
        StructField("arr_icao", StringType(), nullable=True),
        StructField("arr_terminal", StringType(), nullable=True),
        StructField("arr_gate", StringType(), nullable=True),
        StructField("arr_baggage", StringType(), nullable=True),
        StructField("arr_time", StringType(), nullable=True),
        StructField("arr_time_utc", StringType(), nullable=True),
        StructField("arr_estimated", StringType(), nullable=True),
        StructField("arr_estimated_utc", StringType(), nullable=True),
        StructField("cs_airline_iata", StringType(), nullable=True),
        StructField("cs_flight_number", StringType(), nullable=True),
        StructField("cs_flight_iata", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("duration", IntegerType(), nullable=True),
        StructField("delayed", IntegerType(), nullable=True),
        StructField("dep_delayed", IntegerType(), nullable=True),
        StructField("arr_delayed", IntegerType(), nullable=True),
        StructField("aircraft_icao", StringType(), nullable=True),
        StructField("arr_time_ts", IntegerType(), nullable=True),
        StructField("dep_time_ts", IntegerType(), nullable=True),
        StructField("arr_estimated_ts", IntegerType(), nullable=True),
        StructField("dep_estimated_ts", IntegerType(), nullable=True),
        StructField("dep_actual_ts", IntegerType(), nullable=True)
        ])

        # Leo el JSON en un DataFrame de Spark
        df_vuelo_info = spark.createDataFrame(response_data, schema=schema)

        # Agrego al dataframe el current time stamp
        df = df_vuelo_info.withColumn("current_ts", expr("unix_timestamp(current_timestamp())"))

        # Calculo el tiempo restante hasta la llegada estimada
        df = df.withColumn("tiempo_restante", df.arr_estimated_ts - df.current_ts)

        # Filtro el DataFrame por llegada estimada cuando aún faltan 30 minutos para llegar
        df_filtrado = df.filter(df.tiempo_restante < 30 * 60)

        # Escribir el contenido del DataFrame en un JSON
        json_data = df_filtrado.toJSON().collect()

        # Escribir el JSON en el archivo de log
        with open(path_log, "w") as log_file:
            json.dump(json_data, log_file, indent=4)

        # Cierro sesion de Spark
        spark.stop()

    else:
        # Si la solicitud falla, registro el error en el archivo de log
        with open(path_log, 'a') as log_file:
            log_file.write(f"Error al hacer la solicitud al API de AirLabs. Código de estado: {response.status_code}\n")

# Funcion Main.
def main():
    # Verifico si se proporciona un argumento string desde la línea de comandos
    if len(sys.argv) != 2:
        print("Uso: python3 Punto5-Spark.py 'string' donde string es el codigo de aeropuerto")
        sys.exit(1)
    
    # El primer argumento (sys.argv[0]) es el nombre del script, por lo que el argumento string es sys.argv[1]
    codigo_aeropuerto = sys.argv[1]

    # Cargo configuración desde el archivo
    config = configparser.ConfigParser()
    config.read("configPunto5.cfg")

    # Creo SparkSession
    spark = SparkSession.builder.appName("Punto_5").getOrCreate()

    # Consulto información del aeropuerto
    consultar_aeropuerto(codigo_aeropuerto, spark, config)

    # Consulto vuelos a llegar en los proximos 30 min a un aeropuerto para Aerolineas Argentinas
    vuelos_arrivo_proximos_30_min(config, spark)

    # Detener la sesión de Spark
    spark.stop()

# Main execute
if __name__ == "__main__":
    main()