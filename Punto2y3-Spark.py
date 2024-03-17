from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import requests
import json
import configparser

# Funcion que obtiene el aeropuerto con mas arrivos
def obtener_aeropuerto_mas_arribos(config, spark):
    # Path de archivo de rutas
    file_tickets = config["DATASETS"]["file_tickets"]
    delimiter = config["INPUT"]["delimiter_pipe"]

    # Busco el aeropuerto con más llegadas
    df_routes = spark.read.csv(file_tickets, header=True, sep=delimiter)
    airport_mas_llegadas = df_routes.groupBy("AeropuertoDestinoID").count().orderBy(col("count").desc()).first()["AeropuertoDestinoID"]

    #Retorno el aeropuerto
    return airport_mas_llegadas

# Funcion que consulta la api de vuelos.
def consultar_api_vuelos(config, spark, airport_mas_llegadas):
    # Defino la URL de la API
    url = config["API"]["url"]

    # Realizo la solicitud HTTP a la API
    response = requests.get(url+airport_mas_llegadas)

    # Verifico el código de respuesta
    if response.status_code == 200:

        #PUNTO 2
        print("Punto 2 - Listar las rutas con los nombres de aerolineas y aeropuertos (NO Codigos) desde API")
        # Cargo los datos JSON en un diccionario
        data_dictionary = json.loads(response.text)

        # Convierto la lista de cadenas JSON en un DataFrame
        df_routes_airport = spark.read.json(spark.sparkContext.parallelize(data_dictionary))

        # Muestro el DataFrame resultante consumido desde la api
        df_routes_airport.show()
        #n=df_routes_airport.count(), truncate=False

        # Retorno dataframe para uso de la funcion del punto 3
        return df_routes_airport
    
    else:
        print("Error al obtener los datos de la API. Código de estado:", response.status_code)

# Funcion que devuelve los 5 vuelos actuales de las 10 aerolineas que aterrizan en el aeropurto de mas arribos.
def obtener_5_vuelos_actuales(df_routes_airport):

    #PUNTO 3
    # DataFrame con las 10 aerolineas que más aterrizan en el aeropuerto del DataFrame anterior (df_routes_airport)
    top_10_airlines = df_routes_airport.groupBy("aerolinea").count().orderBy(col("count").desc()).limit(10)

    # Muestro el DataFrame con las 10 Aerolineas
    print("Top 10 de Aerolineas")
    top_10_airlines.show()

    # Convierto las 10 aerolíneas a una lista
    top_10_airlines_list = top_10_airlines.select("aerolinea").rdd.map(lambda row: row[0]).collect()

    # Filtro los 5 vuelos actuales para incluir solo los vuelos de las 10 aerolíneas seleccionadas
    df_filtered_flights = df_routes_airport.filter(col("aerolinea").isin(top_10_airlines_list)).limit(5)

    # Mostrar el DataFrame resultante con los 5 vuelos
    print("Punto 3 - 5 Vuelos Actuales de top 10 Aerolineas")
    df_filtered_flights.show()         

# Funcion Main.
def main():

    # Creo una sesión de Spark
    spark = SparkSession.builder.appName("Punto_2_y_3").getOrCreate()

    # Cargo configuración desde el archivo
    config = configparser.ConfigParser()
    config.read("configuracion.cfg")

    # Obtengo el aeropuerto con mas arrivos
    airport_mas_llegadas = obtener_aeropuerto_mas_arribos(config, spark)

    # Consulta la api de vuelos.
    df_routes_airport = consultar_api_vuelos(config, spark, airport_mas_llegadas)

    # Obtengo los 5 vuelos actuales de las 10 aerolineas que aterrizan en el aeropurto de mas arribos.
    obtener_5_vuelos_actuales(df_routes_airport)

    # Detener la sesión de Spark
    spark.stop()

# Main execute
if __name__ == "__main__":
    main()
