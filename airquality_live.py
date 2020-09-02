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

conn = psycopg2.connect(host="localhost", options='-c statement_timeout=1000', database="testing", user="postgres", password="9664241907")
if conn:
    print('Success!')
else:
    print('An error has occurred.')
cursor = conn.cursor()

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

def parsedata(pollution_code, pollution_datetime,pollutant_value, pollution_station_code ):
    pollution_date = pollution_datetime.strftime("%Y-%m-%d")
    pollution_time = pollution_datetime.strftime("%H:%M:%S")
    sql_code_pol_id = 'select pollutants_id from pollutants where code_pollutant = %s'
    cursor.execute(sql_code_pol_id, (pollution_code,))
    code_pol_id = cursor.fetchone()[0]
    row_count = cursor.rowcount
    if row_count == 0:
        sql_insert_pollutants = 'insert into pollutants (name_pollutant, code_pollutant) values (%s, %s);'
        cursor.execute(sql_insert_pollutants, (code_pollutions, elem,))
        conn.commit()
        sql_code_pol_id = 'select pollutants_id from pollutants where code_pollutant = %s'
        cursor.execute(sql_code_pol_id, (pollution_code,))
        code_pol_id = cursor.fetchone()[0]

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
@tl.job(interval=timedelta(seconds=600))
def sample_job_every_1000s():
    dt = datetime.now()
    x = dt.strftime("%Y-%m-%d %H:%M:%S")
    clear_output(wait=False)
    session = requests.Session()
    date = dt.strftime("%Y-%m-%d")
    time = dt.strftime("%H:%M:%S")
    try:
        conn = psycopg2.connect(host="localhost", database="testing", user="postgres", password="9664241907")
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
                            parsedata(pollution_code, pollution_datetime, pollutant_value, pollution_station_code)

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
                            parsedata(pollution_code, pollution_datetime, pollutant_value, pollution_station_code)
        except:
            sys.stderr.write("Could not connect for colletion of data")
    except ConnectionError as ce:
        print(ce)

if __name__ == "__main__":
    tl.start(block=True)
