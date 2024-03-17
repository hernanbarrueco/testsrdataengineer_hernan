import psycopg2 
import psycopg2.extras as extras 
import pandas as pd 
import configparser
  
# Funcion que inserta los datos en una base postgresSQL
def insert_values_in_db(conn, df, table): 
    
    # Convierto los datos del dataframe en tuplas
    tupla = [tuple(x) for x in df.to_numpy()] 
  
    # Extraigo el header y lo separo por ,
    cols = ','.join(list(df.columns)) 

    # SQL query para ejecutar
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols) 

    # Abro cursor
    cursor = conn.cursor() 
    try: 

        # Quito de las tublas los valores nulos y los \N y los inserto en null
        tupla = [tuple(None if pd.isnull(val) or val == "\\N" else val for val in tup) for tup in tupla]

        # Ejecuto la query.
        extras.execute_values(cursor, query, tupla) 

        #Commit de los cambios.
        conn.commit() 

    except (Exception, psycopg2.DatabaseError) as error: 
        print("Error al insertar los datos: %s" % error) 
        conn.rollback() 
        cursor.close() 
        return 1
    
    print("Se insertaron los datos de la tabla: "+table) 
    cursor.close()
  
# Funcion Main.
def main():

    # Cargo configuración desde el archivo
    config = configparser.ConfigParser()
    config.read("configLoadDB.cfg")

    # Path a los archivos
    file_tickets_1 = config["DATASETS"]["file_tickets_1"]
    file_tickets_2 = config["DATASETS"]["file_tickets_2"]
    file_airlines = config["DATASETS"]["file_airlines"]
    file_airports = config["DATASETS"]["file_airports"]
    
    # delimitador
    delimiter_pipe = config["DELIMITER"]["delimiter_pipe"]
    delimiter_coma = config["DELIMITER"]["delimiter_coma"]

    # database
    database_db=config["DATABASE"]["database"]
    user_db=config["DATABASE"]["user"]
    password_db=config["DATABASE"]["password"]
    host_db=config["DATABASE"]["host"]
    port_db=config["DATABASE"]["port"]
    table_airports=config["DATABASE"]["table_airports"]
    table_airlines=config["DATABASE"]["table_airlines"]
    table_routes=config["DATABASE"]["table_routes"]

    # Obtengo la conexion
    conn = psycopg2.connect( 
    database=database_db, user=user_db, password=password_db, host=host_db, port=port_db) 
  
    # Costruyo los dataframes con los archivos
    df_airlines = pd.read_csv(file_airlines, sep=delimiter_coma) 
    df_airports = pd.read_csv(file_airports, sep=delimiter_coma) 
    df_routes1 = pd.read_csv(file_tickets_1, sep=delimiter_pipe)
    df_routes2 = pd.read_csv(file_tickets_2, sep=delimiter_pipe)  

    print("Iniciando proceso de Insercion en la db......")

    # Inserto los datos en la base de datos.
    insert_values_in_db(conn, df_airlines, table_airlines)
    insert_values_in_db(conn, df_airports, table_airports) 
    insert_values_in_db(conn, df_routes1, table_routes)
    insert_values_in_db(conn, df_routes2, table_routes)   

    print("Proceso de Insercion finalizado.")
    
# Main execute
if __name__ == "__main__":
    main()