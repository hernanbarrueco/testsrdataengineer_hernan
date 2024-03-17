--Porcentaje de aviones que partieron con una capacidad mayor o igual al 85%
SELECT 
    COUNT(1) * 100.0 / 
    (SELECT COUNT(1) FROM public.fact_routes) AS porcentaje_capacidad_alta_ocupacion
FROM public.fact_routes fr
WHERE (fr.ticketsvendidos::float / fr.lugares) >= 0.85;

----------------------------------------------------------------------------------------------

--Porcentaje de vuelos que tienen una diferencia de altitud de mas de 1000 metros entre aeropuertos
--para los vuelos que tienen ocupacion del 85% y que salen y llegan al mismo pais
SELECT 
    COUNT(1) * 100.0 / 
    (
    --Cantidad de vuelos que tienen ocupacion del 85% y que salen y llegan al mismo pais
    SELECT COUNT(1) 
    FROM public.fact_routes fr
    INNER JOIN public.dim_airports ao ON fr.aeropuertoorigenid = ao.idairport
	INNER JOIN public.dim_airports ad ON fr.aeropuertodestinoid = ad.idairport
    WHERE (fr.ticketsvendidos::float / fr.lugares) >= 0.85 AND ao.pais = ad.pais
    ) AS porcentaje_altitud_mayor_1000
FROM public.fact_routes r
    INNER JOIN public.dim_airports ao ON r.aeropuertoorigenid = ao.idairport
	INNER JOIN public.dim_airports ad ON r.aeropuertodestinoid = ad.idairport
WHERE (r.ticketsvendidos::float / r.lugares) >= 0.85 AND ao.pais = ad.pais AND ABS(ao.altitud - ad.altitud) > 1000;