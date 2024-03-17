--obtener todas las aerolineas que tuvieron una ocupacion
--mayor o igual al 85% para la misma ruta para dias consecutivos.
SELECT DISTINCT da.nombreaerolinea 
FROM public.fact_routes fr1

INNER JOIN public.fact_routes fr2 ON fr1.idaerolinea = fr2.idaerolinea
    AND fr1.aeropuertoorigenid = fr2.aeropuertoorigenid
    AND fr1.aeropuertodestinoid= fr2.aeropuertodestinoid
    AND to_date(fr1.fecha, 'yyyy-mm-dd') = to_date(fr2.fecha, 'yyyy-mm-dd') - INTERVAL '1 day'
    
INNER JOIN public.dim_airlines da on da.idaerolinea = fr1.idaerolinea
WHERE (fr1.ticketsvendidos::float / fr1.lugares) >= 0.85
  AND (fr2.ticketsvendidos::float / fr2.lugares) >= 0.85;