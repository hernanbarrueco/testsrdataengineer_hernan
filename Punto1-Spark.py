
#PUNTO 1 SPARK
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import configparser

# Funcion que genera archivo de ocupacion particionado por tipo de viaje
def generar_archivos_ocupacion(spark, config):
    # Path al archivo de rutas
    file_tickets = config["DATASETS"]["file_tickets"]

    # delimitador
    delimiter = config["INPUT"]["delimiter_pipe"]

    # Leo el archivo .csv con columnas separadas por pipe y guardo en un dataframe
    df_tickets = spark.read.csv(file_tickets, header=True, sep = delimiter)

    # Calculo el tipo de ocupación para cada viaje
    df_ocupacion = df_tickets.withColumn("ocupacion", (col("TicketsVendidos") / col("Lugares")) * 100) \
                            .withColumn("tipo_viaje", 
                                        when(col("ocupacion") < 25, "poca_concurrencia")
                                        .when((col("ocupacion") >= 25) & (col("ocupacion") <= 50), "ocupacion_media")
                                        .when((col("ocupacion") > 50) & (col("ocupacion") <= 85), "ocupacion_alta")
                                        .otherwise("ocupacion_plena"))

    # Cargo configuración desde el archivo
    path_output = config["OUTPUT"]["path_output"]
    output_format = config["OUTPUT"]["output_format"]
    output_delimiter = config["OUTPUT"]["output_delimiter"]
    output_encode = config["OUTPUT"]["output_encode"]
    output_partition = config["OUTPUT"]["output_partition"]

    # Guardo los datos particionados por tipo de viaje en archivos CSV
    df_ocupacion.coalesce(1).write \
        .partitionBy(output_partition) \
        .format(output_format) \
        .option("header", "true") \
        .option("encoding", output_encode) \
        .option("delimiter", output_delimiter) \
        .mode("overwrite") \
        .save(path_output)


# Funcion Main.
def main():

    # Cargo configuración desde el archivo
    config = configparser.ConfigParser()
    config.read("configuracion.cfg")

    # Creo una sesión de Spark
    spark = SparkSession.builder.appName("Punto_1").getOrCreate()

    # Consulto información del aeropuerto
    generar_archivos_ocupacion(spark, config)

    # Detener la sesión de Spark
    spark.stop()

# Main execute
if __name__ == "__main__":
    main()