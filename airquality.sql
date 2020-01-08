CREATE TABLE pollutants
(
    pollutants_id SERIAL NOT NULL,
    name_pollutant character varying(8) COLLATE pg_catalog."default",
    code_pollutant integer NOT NULL,
    CONSTRAINT "Pollutants_pkey" PRIMARY KEY (pollutants_id)
);


INSERT INTO pollutants(
    pollutants_id, name_pollutant)
    VALUES (1,'SO2'),
    (2,'PM18'),
    (3,'O3'),
    (4,'NO2'),
    (5,'NOx'),
    (6,'CO'),
    (7,'C6H6'),
    (8,'NO'),
    (9,'PM2.5');

/* Κάνει αντικατάσταση και του γράμματος x ενώ δεν πρέπει.
SELECT pollutants_id,   regexp_replace(translate(regexp_replace(name_pollutant, '<.*?>', '', 'g'), '0123456789.', '₀₁₂₃₄₅₆₇₈₉.'), 'x(e[urs]|[^e]|$)', 'ₓ\1', 'ig')
    FROM public.pollutants; */

SELECT pollutants_id,   translate(regexp_replace(name_pollutant, '<.*?>', '', 'g'), '0123456789.', '₀₁₂₃₄₅₆₇₈₉.') 
    FROM public.pollutants;

CREATE TABLE stations
(
    station_id integer NOT NULL,
    station_name_en character varying(45) COLLATE pg_catalog."default" NOT NULL,
    station_name_gr character varying(45) COLLATE pg_catalog."default" NOT NULL,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    geometry character varying(145) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT "Station_pkey" PRIMARY KEY (station_id)
);

/*ALTER TABLE stations ALTER COLUMN geom TYPE GEOMETRY(POINT,4326);*/


insert into stations values (1, 'Nicosia - Traffic Station', 'Λευκωσία - Κυκλοφοριακός Σταθμός',
   '35.15192246950294', '33.347919957077806','POINT(35.15192246950294,33.347919957077806)'),
(2, 'Nicosia - Residential Station', 'Λευκωσία - Οικιστικός Σταθμός',
   '35.1269444', '33.33166670000003','POINT(35.1269444,33.33166670000003)');

CREATE TABLE update
(
    update_id SERIAL NOT NULL,
    date date NOT NULL,
    "time" time without time zone NOT NULL,
    datetime timestamp without time zone,
    CONSTRAINT "Update_pkey" PRIMARY KEY (update_id)
);

/* https://dba.stackexchange.com/questions/217593/drop-table-taking-too-long */

CREATE TABLE values (
    id_value SERIAL NOT NULL,
    pollutant_value double precision,
    id_stationFK integer,
    id_pollutantFK integer,
    id_updateFK integer,
    CONSTRAINT "Value_pkey" PRIMARY KEY (id_value,id_stationFK),
    FOREIGN KEY (id_stationFK) REFERENCES stations (station_id),
    FOREIGN KEY (id_pollutantFK) REFERENCES pollutants (pollutants_id),
    FOREIGN KEY (id_updateFK) REFERENCES update (update_id)
);

