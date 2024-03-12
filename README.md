# Data Engineer Programming Test (SR)

## Datasets
<br>

<p>Las siguientes tres tablas describen los datasets que se utilizaran como material a lo largo del test. Estos describen distinta informacion sobre aeropuertos, aerolineas y rutas de vuelo.</p>
<br/><br/>
<table>
<tr><th>routesYYYYMMDD.csv </th><th>airports.dat</th><th>airlines.csv</th></tr>
<tr><td>

| Campo              | Tipo        | Descripcion   |  
|--------------------|-------------|---------------|
| CodAerolinea       | String      |       Codigo IATA de 2 letras  o codigo ICAO de 3 letras, si existen       | 
| IDAerolinea        | Integer     |    identificador unico de aerolineas       | 
| AeropuertoOrigen   | Varchar(3)  |        Codigo IATA de 3 letras o codigo ICAO de 4 letras, si existen       | 
| AeropuertoOrigenID | Integer     |        identificador unico de aeropierto origen     |
| AeropuertoDestino  | Varchar(3)  |        Codigo IATA de 3 letras o codigo ICAO de 4 letras, si existen      | 
| AeropuertoDestinoID| Integer     |        identificador unico de aeropierto destino      | 
| OperadoCarrier     | Char        |        "Y" si es operado por otro carrier, sino vacio     | 
| Stops              | Integer     |       cantidad de escalas hasta llegaar al aeropuerto destino       |
| Equipamiento       | String      |       codigos de equipamiento que lleva el avion que realiza la ruta       | 
| TicketsVendidos    | Integer     |          cantidad de tickets vendidos para este vuelo    | 
| Lugares            | Integer     |        cantidad de lugares disponibles para el vuelo      | 
| PrecioTicket       | Double(6,4) |         precio del ticket del vuelo    |
| KilometrosTotales  | Double(8,2) |       kilometros a recorrer       | 
| Fecha  | String|           fechas de vuelo   |

</td><td>

| Campo              | Tipo        | Descripcion   |  
|--------------------|-------------|---------------|
| IDAirport          | Integer     |      Identificador unico del aeropuerto         | 
| NombreAeropuerto   | String      |        Nombre del Aeropuerto, puede no tener el nombre de la ciudad o pais       | 
| Ciudad             | String      |       Ciudad donde se encuentra el aeropuerto        | 
| Pais               | String      |        Pais donde se encuentra el aeropuerto.       |
| CodigoAeropuerto   | String      |        codigo IATA de 3 letras o codigo ICAO de 4 letras      |  
| Latitud            | Double (3,6)|           grados decimales donde se encuntra el aeropuerto, negativo hace referencia al sur, positivo al norte      | 
| Longitud           | Double (3,6)|        grados decimales donde se encuntra el aeropuerto, negativo hace referencia al oeste, positivo al este       | 
| Altitud            | Integer     |   expresado en Pies.           |
| DifUTC             | Integer     |      zona UTC del aeropuerto, puede existir zonas decimales (ej, 5.5)       | 
| CodigoContinente   | Char        |          codigo de identificacion del continente donde se encuentra el aeropuerto.  E (Europa), A (EEUU/Canada), S (America del Sur), O (Australia), Z (Nueva Zealanda), N (None) or U (Desconocido).     | 
| TimezoneOlson      | String      |         zona temporal expresado en formato Olsen eg. "America/Los_Angeles"      | 

</td><td>

| Campo              | Tipo        | Descripcion   |  
|--------------------|-------------|---------------|
| IDAerolinea        | Integer     |             Identificador Unico de la Aerolinea  | 
| NombreAerolinea    | String      |       Nombre de la aerolinea         | 
| Alias              | String      |       Alias de la aerolinea.        | 
| IATA               | String      |      Codigo IATA de 2 letras si existe         |
| ICAO               | String      |     Codigo ICAO de 3 letras si existe          | 
| Callsign           | String      |      Nombre comercial de la Aerolinea.         | 
| Pais               | String      |         Pais o territorio del cual la aerolinea es proveniente.      | 
| Activa             | String      |       "Y" si la aerolinea esta activa, "N" si no lo esta        |

</td></tr> </table>
<br/><br/>

# SQL

1 - Considerando que la interfaz de aeropuertos es un maestro de frecuencia diaria, realizar una query para alimentar una tabla dimensional que agregue nuevos aeropuertos asegurandose de no generar registros duplicados.

2- Un dato de interes es saber el porcentaje de aviones que partieron con una capacidad mayor o igual al 85%. De los vuelos que tienen ocupacion del 85% y que salen y llegan al mismo pais, escriba una query para obtener el porcentaje de vuelos que tienen una diferencia de altitud de mas de 1000 metros entre aeropuertos.

3- Teniendo en cuenta el KPI del ejercicio anterior, obtener todas las aerolineas que tuvieron una ocupacion mayor o igual al 85% para la misma ruta para dias consecutivos.

<br/><br/>

# SPARK

**Aclaracion:**: Para estos ejercicios es indistinto utilizar routes20230101.csv o routes20230102.csv

En base a los datasets provistos se necesitan generar las queries necesarias para obtener los siguientes KPIs para los dashboards de las aerolineas:

1- Teniendo en cuenta los tickets vendidos y los lugares disponibles, y considerando que una ocupacion menor al 25% es un viaje de poca concurrencia, de 25 a 50% de ocupacion media, de 50 a 85% de ocupacion alta y de mayor a 85% un viaje de ocupacion plena, guardar esta informacion particionada por tipo de viaje. Cada particion debe contener un solo fichero, en csv, separado por "|" en UTF-8.

2- El aeropuerto que mas aviones recibio segun el dataset de rutas. Con el aeropuerto obtenido, consultar la api para retornar un dataframe con columnas donde el codigo de la aerolinea y de los aeropuertos esten expresados en palabras y no en codigo (Por ejemplo, ej CHS expresado como Charleston), y que ademas este ordenado por tiempo de vuelo de manera ascendente.

3- Retornar un dataframe con 5 vuelos actuales de las 10 aerolineas que mas aterrizan en el aeropuerto del dataframe anterior.

4- Para las aerolineas Aerolineas Argentinas, Air Europa e Iberia construir un dataframe que contenga una columna por pais destino y cuyo valor sea la cantidad de aeropuertos en ese pais, y una columna final totalizando la cantidad de aeropuertos. Por ejemplo:

| Campo              | Arg         | Brs   | Uru | total|
|--------------------|-------------|-------|-----|------|
| AerolineasArgentinas| 120         | 40    |15   |175   |

**Aclaracion:**  Considerar que si existen rutas ambiguas se deben contabilizar como un unico valor. Por ejemplo: BRU -> NDR y NDR -> BRU

5- Construir un archivo de configuracion para poder consular a la API publica de airlabs informacion de un aeropuerto y realizar el codigo que realice esta consulta y devuelva un dataframe con la informacion que considere importante, considerar en el archivo de configuracion, parametros que se requieran para el fichero de resultado (particiones, columnas, tipos de datos, etc). Luego, Construir una funcion que notifique si existe en el api de airlabs algun vuelo activo que este al menos a 30 minutos del arrivo estimado para aerolineas argentinas (Código IATA=AA). La funcion deberia registrarlos en un archivo de logeo.
<br/><br/>

# Preguntas Teoricas

1- Suponga que usted esta a cargo de una ingesta de una tabla que contiene billones de registros utilizando Apache Spark. Que parametros de spark tendria en cuenta a la hora de realizar dicha ingesta? Explique brevemente en que consta cada uno de ellos. En que formato de archivo escribiría los resultados? Por que?

2- Teniendo en cuenta la tabla de la pregunta anterior, describa como realizaria el proceso de ingesta diaria de dicha tabla hacia un datalake. Que herramientas/lenguajes utilizaria? por que?
<br/><br/>

# Bonus Track!

1- Describa brevemente implementaciones que haya hecho con Apache Spark o problemas con los que se haya topado y como logro solucionarlos.

2- Describa brevemente su nivel de experiencia con los distintos lenguajes de programacion que conoce.

3- Si cuenta con experiencia desarrollando sobre alguna nuble publica, detallar brevemente que proveedores cloud ha utilizado y que servicios de ellos conoce.

4- Comente brevemente su conocimiento o experiencia utilizando distintos tipos de bases NO SQL.