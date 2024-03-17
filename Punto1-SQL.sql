INSERT INTO public.dim_airports
	(
	idairport,
	nombreaeropuerto,
	ciudad,
	pais,
	codigoaeropuerto,
	codigo,
	latitud,
	longitud,
	altitud,
	difutc,
	codigocontinente,
	timezoneolson
	)
SELECT 
	tmp.idairport,
	tmp.nombreaeropuerto,
	tmp.ciudad,
	tmp.pais,
	tmp.codigoaeropuerto,
	tmp.codigo,
	tmp.latitud,
	tmp.longitud,
	tmp.altitud,
	tmp.difutc,
	tmp.codigocontinente,
	tmp.timezoneolson
FROM 
    (
        SELECT 
				ta.idairport,
				ta.nombreaeropuerto,
				ta.ciudad,
				ta.pais,
				ta.codigoaeropuerto,
				ta.codigo,
				ta.latitud,
				ta.longitud,
				ta.altitud,
				ta.difutc,
				ta.codigocontinente,
				ta.timezoneolson
        from 
        		public.tmp_airports ta
    ) AS tmp
LEFT JOIN public.dim_airports da ON tmp.codigoaeropuerto = da.codigoaeropuerto
WHERE da.idairport IS NULL;