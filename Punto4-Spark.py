from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, countDistinct
import configparser

# Funcion que devuelve la cantidad de aeropuertos que hay por pais y por aerolinea
def cantidad_aeropuertos_x_pais_aerolinea(spark, config):

        # Path a los archivos
        file_tickets = config["DATASETS"]["file_tickets"]
        file_airlines = config["DATASETS"]["file_airlines"]
        file_airports = config["DATASETS"]["file_airports"]
        # delimitador
        delimiter_pipe = config["INPUT"]["delimiter_pipe"]
        delimiter_coma = config["INPUT"]["delimiter_coma"]

        # Leo los archivos como un DataFrame de Spark
        df_airlines = spark.read.csv(file_airlines, header=True, inferSchema=True)
        df_airports = spark.read.csv(file_airports, header=True, sep=delimiter_coma)
        df_rutas = spark.read.csv(file_tickets, header=True, sep=delimiter_pipe)

        # Defino las aerolíneas especificadas en el enunciado
        aerolineas_especificadas = config["AEROLINEAS"]["aerolineas_especificadas"].split(',')

        # Uno los DataFrames de rutas, aeropuertos y aerolíneas
        df_unido_destino= df_rutas.join(df_airlines, df_rutas.IDAerolinea == df_airlines.IDAerolinea, "inner") \
                        .join(df_airports, df_rutas.AeropuertoDestinoID == df_airports.IDAirport, "inner") \
                        .select(df_rutas["*"], 
                                df_airlines["NombreAerolinea"],
                                df_airports["Pais"])

        # Renombro la columna de pais
        df_unido_destino = df_unido_destino.withColumnRenamed("Pais", "PaisAeropuertoDestino")

        # Filtro las rutas para incluir solo las aerolíneas especificadas
        df_filtrado_por_airline = df_unido_destino.filter(col("NombreAerolinea").isin(aerolineas_especificadas))

        # Elimino las rutas ambiguas
        df_filtrado_por_airline = df_filtrado_por_airline.withColumn("ruta_ordenada", when(col("AeropuertoOrigen") < col("AeropuertoDestino"), col("AeropuertoOrigen")).otherwise(col("AeropuertoDestino")))

        # Cuento la cantidad de aeropuertos por país destino para cada aerolínea
        df_count_airports = df_filtrado_por_airline.groupBy("NombreAerolinea").pivot("PaisAeropuertoDestino").agg(countDistinct("ruta_ordenada"))

        # Relleno las columnas null con ceros
        df_count_airports = df_count_airports.fillna(0)

        # Calculo la columna final que totaliza la cantidad de aeropuertos por país
        df_count_airports = df_count_airports.withColumn("total", sum(col(c) for c in df_count_airports.columns[1:]))

        # Muestro el DataFrame resultante
        df_count_airports.show()

# Funcion Main.
def main():

    # Creo una sesión de Spark
    spark = SparkSession.builder.appName("Punto_4").getOrCreate()

    # Cargo configuración desde el archivo
    config = configparser.ConfigParser()
    config.read("configuracion.cfg")

    # Consulto la cantidad de aeropuertos por pais y por aerolinea
    cantidad_aeropuertos_x_pais_aerolinea(spark, config)

    # Detener la sesión de Spark
    spark.stop()

# Main execute
if __name__ == "__main__":
    main()