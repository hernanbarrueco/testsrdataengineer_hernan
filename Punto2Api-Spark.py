from flask import Flask, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import configparser

app = Flask(__name__)

# Api /routes/<aeropuerto_mas_llegadas>
@app.get('/routes/<aeropuerto_mas_llegadas>')
def get_routes_dataframe(aeropuerto_mas_llegadas):

    # Creo una sesión de Spark
    spark = SparkSession.builder.appName("Punto_2_API").getOrCreate()

    # Cargo configuración desde el archivo
    config = configparser.ConfigParser()
    config.read("configuracion.cfg")

    # Path de archivos
    file_tickets = config["DATASETS"]["file_tickets"]
    file_airlines = config["DATASETS"]["file_airlines"]
    file_airports = config["DATASETS"]["file_airports"]
    delimiter_coma = config["INPUT"]["delimiter_coma"]
    delimiter_pipe= config["INPUT"]["delimiter_pipe"]

    # Leo los archivos como un DataFrame de Spark
    df_airlines = spark.read.csv(file_airlines, header=True, inferSchema=True)
    df_airports_origen = spark.read.csv(file_airports, header=True, sep=delimiter_coma)
    df_airports_destino = spark.read.csv(file_airports, header=True, sep=delimiter_coma)
    df_routes = spark.read.csv(file_tickets, header=True, sep=delimiter_pipe)

    # Obtengo las rutas asociadas con el aeropuerto pasado por parametro
    df_routes_airports_mayor_llegadas = df_routes.filter(col("AeropuertoDestinoID") == aeropuerto_mas_llegadas)

    # Convierto los códigos de aerolínea y aeropuertos a nombres
    df_result = df_routes_airports_mayor_llegadas \
    .join(df_airlines, df_routes_airports_mayor_llegadas["IDAerolinea"] == df_airlines["IDAerolinea"], "left") \
    .join(df_airports_origen, df_routes_airports_mayor_llegadas["AeropuertoOrigenID"] == df_airports_origen["IDAirport"], "left") \
    .join(df_airports_destino, df_routes_airports_mayor_llegadas["AeropuertoDestinoID"] == df_airports_destino["IDAirport"], "left") \
    .select(df_airlines["NombreAerolinea"].alias("Aerolinea"), 
            df_airports_origen["NombreAeropuerto"].alias("AeropuertoOrigen"),  
            df_airports_destino["NombreAeropuerto"].alias("AeropuertoDestino"),  
            df_routes_airports_mayor_llegadas["OperadoCarrier"], 
            df_routes_airports_mayor_llegadas["Stops"], 
            df_routes_airports_mayor_llegadas["Equipamiento"], 
            df_routes_airports_mayor_llegadas["TicketsVendidos"], 
            df_routes_airports_mayor_llegadas["Lugares"], 
            df_routes_airports_mayor_llegadas["PrecioTicket"], 
            df_routes_airports_mayor_llegadas["KilometrosTotales"], 
            df_routes_airports_mayor_llegadas["Fecha"])

    # Ordeno el DataFrame resultante por tiempo de vuelo de manera ascendente segun la cantidad de kilometros
    df_result = df_result.orderBy(col("KilometrosTotales").cast("float"))

    # Convierto el DataFrame a JSON y lo devuelvo como respuesta HTTP
    df_json = df_result.toJSON().collect()
    return jsonify(df_json)

# Main
if __name__ == '__main__':
    app.run()


