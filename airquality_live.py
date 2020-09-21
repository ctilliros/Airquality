import time
from timeloop import Timeloop
from datetime import timedelta
from IPython.display import clear_output
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError
from datetime import datetime
import psycopg2
import sys 
conn = psycopg2.connect(host="postgres", options='-c statement_timeout=30s', port=5432, dbname="inicosia", user="admin", password="secret")
# conn = psycopg2.connect(host="localhost", options='-c statement_timeout=30s', port=5432, database="testing", user="postgres", password="9664241907")
if conn:
    print('Success!')
else:
    print('An error has occurred.')
cursor = conn.cursor()

def create_tables():
    sql = 'CREATE TABLE IF NOT EXISTS pollutants(pollutants_id SERIAL NOT NULL,\
    name_pollutant character varying(8) COLLATE pg_catalog."default",code_pollutant \
    integer,UNIQUE(name_pollutant),CONSTRAINT "Pollutants_pkey" PRIMARY KEY (pollutants_id));'
    cursor.execute(sql,)
    conn.commit()


    # sql = "INSERT INTO pollutants(pollutants_id, name_pollutant) VALUES (1,'SO2'),(2,'PM18'),\
    #     (3,'O3'),(4,'NO2'),(5,'NOx'),(6,'CO'),(7,'C6H6'),(8,'NO'),(9,'PM2.5') \
    #     ON CONFLICT (name_pollutant) DO NOTHING ;"
    
    # cursor.execute(sql,)
    # conn.commit()

    # sql = "SELECT pollutants_id,   translate(regexp_replace(name_pollutant, '<.*?>', '', 'g'), '0123456789.', '₀₁₂₃₄₅₆₇₈₉.') \
    #     FROM public.pollutants;"    
    # cursor.execute(sql,)
    # conn.commit()
    
    sql = 'CREATE TABLE IF NOT EXISTS  stations(station_id integer NOT NULL,station_name_en \
    character varying(45) COLLATE pg_catalog."default" NOT NULL,\
        station_name_gr character varying(45) COLLATE pg_catalog."default" NOT NULL,\
        latitude double precision NOT NULL,longitude double precision NOT NULL, \
        geometry character varying(145) COLLATE pg_catalog."default" NOT NULL, \
        UNIQUE(station_name_en),CONSTRAINT "Station_pkey" PRIMARY KEY (station_id));'
    cursor.execute(sql,)
    conn.commit()
    

    sql = "insert into stations values (1, 'Nicosia - Traffic Station', 'Λευκωσία - Κυκλοφοριακός Σταθμός',\
       '35.15192246950294', '33.347919957077806','POINT(35.15192246950294,33.347919957077806)'),\
    (2, 'Nicosia - Residential Station', 'Λευκωσία - Οικιστικός Σταθμός',\
       '35.1269444', '33.33166670000003','POINT(35.1269444,33.33166670000003)') ON CONFLICT (station_name_en) DO NOTHING ;"
    cursor.execute(sql,)
    conn.commit()
    
    sql = 'CREATE TABLE IF NOT EXISTS  update (update_id SERIAL NOT NULL,date date NOT NULL,\
        "time" time without time zone NOT NULL,datetime timestamp without time zone,\
        CONSTRAINT "Update_pkey" PRIMARY KEY (update_id));'
    cursor.execute(sql,)
    conn.commit()
    

    sql = 'CREATE TABLE IF NOT EXISTS  values (id_value SERIAL NOT NULL,pollutant_value double precision,\
        id_stationFK integer,id_pollutantFK integer,id_updateFK integer,CONSTRAINT "Value_pkey" PRIMARY KEY (id_value,id_stationFK),\
        FOREIGN KEY (id_stationFK) REFERENCES stations (station_id),\
        FOREIGN KEY (id_pollutantFK) REFERENCES pollutants (pollutants_id),FOREIGN KEY (id_updateFK) REFERENCES update (update_id));'
    cursor.execute(sql,)
    conn.commit()
    

def insert_values(pollutant_value, pollution_station_code, code_pol_id, update_id):
    sql_check_for_in = 'select * from values where \
        (pollutant_value = %s and id_stationFK = %s and id_pollutantFK = %s and id_updateFK = %s);'
    cursor.execute(sql_check_for_in,(pollutant_value, pollution_station_code, code_pol_id, update_id))
    row_count = cursor.rowcount
    if row_count == 0:
        print("Insert time: "+ str(datetime.now()))
        sql_insert_values = 'insert into values(pollutant_value, id_stationFK, id_pollutantFK,id_updateFK) values(%s,%s,%s,%s)'
        cursor.execute(sql_insert_values, (pollutant_value, pollution_station_code, code_pol_id, update_id))
        conn.commit()
    else:
        print("No new data for insert for time" + str(datetime.now()))

def parsedata(pollution_code, pollution_datetime,pollutant_value, pollution_station_code, pollutant_name ):
    pollution_date = pollution_datetime.strftime("%Y-%m-%d")
    pollution_time = pollution_datetime.strftime("%H:%M:%S")    
    sql_code_pol_id = 'select pollutants_id from pollutants where code_pollutant = %s'
    cursor.execute(sql_code_pol_id, (pollution_code,))
    code_pol_id = cursor.fetchone()
    row_count = cursor.rowcount
    if row_count == 0:
        sql_insert_pollutants = 'insert into pollutants (name_pollutant, code_pollutant) values (%s, %s);'
        cursor.execute(sql_insert_pollutants, (pollutant_name, pollution_code,))
        conn.commit()
        sql = "SELECT pollutants_id,   translate(regexp_replace(name_pollutant, '<.*?>', '', 'g'), '0123456789.', '₀₁₂₃₄₅₆₇₈₉.') \
        FROM public.pollutants;"    
        cursor.execute(sql,)
        conn.commit()
        sql_code_pol_id = 'select pollutants_id from pollutants where code_pollutant = %s'
        cursor.execute(sql_code_pol_id, (pollution_code,))
        code_pol_id = cursor.fetchone()[0]
    # else:
    #     code_pol_id =cursor.fetchone()[0]

    sql_date = 'select datetime, date from update where datetime = %s;'
    cursor.execute(sql_date, (pollution_datetime,))
    conn.commit()
    row_date_count = cursor.rowcount
    if row_date_count == 0:
        sql_insert_update = 'insert into update (date,time,datetime) values (%s,%s, %s);'
        cursor.execute(sql_insert_update, (pollution_date, pollution_time, pollution_datetime))
        conn.commit()
        sql_update_id = 'select max(update_id) from update;'
        cursor.execute(sql_update_id)
        update_id = cursor.fetchone()[0]
        conn.commit()
        insert_values(pollutant_value,pollution_station_code,code_pol_id, update_id)
    else:
        sql_update_id = 'select update_id from update where datetime = %s;'
        cursor.execute(sql_update_id, (pollution_datetime,))
        update_id = cursor.fetchone()[0]
        conn.commit()
        insert_values(pollutant_value, pollution_station_code, code_pol_id, update_id)

### Loop every 3600 seconds (one hour)
tl = Timeloop()
@tl.job(interval=timedelta(seconds=3600))
def sample_job_every_1000s():    
    dt = datetime.now()
    x = dt.strftime("%Y-%m-%d %H:%M:%S")
    clear_output(wait=False)
    session = requests.Session()
    date = dt.strftime("%Y-%m-%d")
    time = dt.strftime("%H:%M:%S")
    try:
        conn = psycopg2.connect(host="postgres", options='-c statement_timeout=30s', port=5432, dbname="inicosia", user="admin", password="secret")
        # conn = psycopg2.connect(host="localhost", options='-c statement_timeout=30s', port=5432, database="testing", user="postgres", password="9664241907")
        if conn:
            print('Success! ' + str(datetime.now()))
        else:
            print('An error has occurred.')
        cursor = conn.cursor()
        try:            
            mysession = session.get('https://www.airquality.dli.mlsi.gov.cy/all_stations_data').json()            
            for i in range(0, len(mysession['data']) - 7):
                station_id = i + 1                
                if (mysession['data']['station_{}'.format(station_id)]['name_en'] == 'Nicosia - Traffic Station'):
                    pollution_station_code = 1
                    pollutants_values = mysession['data']['station_{}'.format(station_id)]['pollutants']
                    for key, value in pollutants_values.items():
                        if 'date_time' in key:
                            pollution_datetime = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                        else:
                            for key2, value in pollutants_values[key].items():
                                if 'code' in key2:
                                    pollution_code = value
                                elif 'value' in key2:
                                    pollutant_value = value                            
                                elif 'notation' in key2:
                                    pollutant_name = value                                
                            parsedata(pollution_code, pollution_datetime, pollutant_value, pollution_station_code,pollutant_name)

                if (mysession['data']['station_{}'.format(station_id)]['name_en'] == 'Nicosia - Residential Station '):
                    pollution_station_code = 2
                    pollutants_values = mysession['data']['station_{}'.format(station_id)]['pollutants']
                    for key, value in pollutants_values.items():
                        if 'date_time' in key:
                            pollution_datetime = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                        else:
                            for key2, value in pollutants_values[key].items():
                                if 'code' in key2:
                                    pollution_code = value
                                elif 'value' in key2:
                                    pollutant_value = value
                                elif 'notation' in key2:
                                    pollutant_name = value                                    
                            parsedata(pollution_code, pollution_datetime, pollutant_value, pollution_station_code,pollutant_name)
        except ConnectionError as ce:
            # sys.stderr.write("Could not connect for colletion of data")
            print(ce)
            sys.exit(1)
    except ConnectionError as ce:
        print(ce)

if __name__ == "__main__":    
    # Open and read the file as a single buffer
    # fd = open('airquality.sql', 'r')
    # sqlFile = fd.read()
    # cursor.execute(sqlFile)  
    # fd.close()    
    create_tables()
    tl.start(block=True)
