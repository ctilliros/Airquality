import requests
import psycopg2
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError
from datetime import datetime
import pandas as pd
import time
start_time = datetime.now()
print("Start Time: %s", start_time)
pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows',1000)


conn = psycopg2.connect(host="localhost", database="testing", user="postgres", password="9664241907")
if conn:
    print('Success!')
else:
    print('An error has occurred.')

cursor = conn.cursor()
session = requests.Session()

def insert_values(pollutant_value, pollution_station_code, code_pol_id, update_id):
    sql_insert_values = 'insert into values(pollutant_value, id_stationFK, id_pollutantFK,id_updateFK) values(%s,%s,%s,%s)'
    cursor.execute(sql_insert_values, (pollutant_value, pollution_station_code, code_pol_id, update_id))
    conn.commit()


mysession = session.get('https://www.airquality.dli.mlsi.gov.cy/all_stations_data_range/2019-01-01%2000:00/2019-12-31%2023:00').json()
values_df = pd.DataFrame(columns={'station_code','code_pollutant','name_pollutant','datetime','date','time','value'})
for i in range(0,len(mysession['data'])-7):
    station_id = i+1
    if (mysession['data']['station_{}'.format(station_id)]['name_en']== 'Nicosia - Traffic Station'):
        station_code = 1
        pollutants_values = mysession['data']['station_{}'.format(station_id)]['values']
        for key in pollutants_values.keys():
            for key2, value in pollutants_values[key].items():
                if 'date_time' in value:
                    datetime_pol = value['date_time']
                    datetime_pol = datetime.strptime(datetime_pol, '%Y-%m-%d %H:%M:%S')
                    date_pol = datetime_pol.strftime("%Y-%m-%d")
                    time_pol = datetime_pol.strftime("%H:%M:%S")
                for key3,value in pollutants_values[key][key2].items():
                    if 'code' in value:
                        code_pol = value['code']
                        value_pol = value['value']
                        notation_pol = value['notation']
                        values_df = values_df.append({'station_code': station_code, 'code_pollutant': code_pol,
                                                      'name_pollutant': notation_pol, 'datetime': datetime_pol,
                                                      'date': date_pol, 'time': time_pol, 'value':value_pol}, ignore_index=True)

    if (mysession['data']['station_{}'.format(station_id)]['name_en']== 'Nicosia - Residential Station '):
        station_code = 2
        pollutants_values = mysession['data']['station_{}'.format(station_id)]['values']
        for key in pollutants_values.keys():
            for key2,value in pollutants_values[key].items():
                if 'date_time' in value:
                    datetime_pol = value['date_time']
                    datetime_pol = datetime.strptime(datetime_pol, '%Y-%m-%d %H:%M:%S')
                    date_pol = datetime_pol.strftime("%Y-%m-%d")
                    time_pol = datetime_pol.strftime("%H:%M:%S")
                for key3,value in pollutants_values[key][key2].items():
                    if 'code' in value:
                        code_pol = value['code']
                        value_pol = value['value']
                        notation_pol = value['notation']
                        values_df = values_df.append({'station_code': station_code , 'code_pollutant': code_pol,
                                                      'name_pollutant': notation_pol,'datetime':datetime_pol,
                                                      'date': date_pol, 'time':time_pol, 'value':value_pol}, ignore_index=True)
for elem in values_df['code_pollutant'].values:
    sql_select_pollutants = 'SELECT code_pollutant,count(code_pollutant) from pollutants where code_pollutant = %s group by code_pollutant'
    exec_sql = cursor.execute(sql_select_pollutants, (elem,))
    code_pollutant_sql = cursor.fetchall()
    row_count = cursor.rowcount
    code_pollutions = values_df.loc[values_df['code_pollutant'] == elem, 'name_pollutant'].iloc[0]
    if row_count == 0:
        sql_insert_pollutants = 'insert into pollutants (name_pollutant, code_pollutant) values (%s, %s);'
        cursor.execute(sql_insert_pollutants, (code_pollutions, elem,))
        conn.commit()


for index, elem in values_df.iterrows():
    # code_pollutant', 'date', 'datetime', 'name_pollutant', 'station_code', 'time', 'value'
    pollution_code = elem['code_pollutant']
    sql_code_pol_id = 'select pollutants_id from pollutants where code_pollutant = %s'
    cursor.execute(sql_code_pol_id, (pollution_code,))
    code_pol_id = cursor.fetchone()[0]
    conn.commit()
    pollution_station_code = elem['station_code']
    pollution_date = elem['date']
    pollution_time = elem['time']
    pollution_datetime = elem['datetime']
    pollutant_value = elem['value']
    sql_date = 'select datetime, date from update where datetime = %s;'
    cursor.execute(sql_date, (pollution_datetime,))
    conn.commit()
    row_date_count = cursor.rowcount
    if row_date_count==0:
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
        cursor.execute(sql_update_id,(pollution_datetime,))
        update_id = cursor.fetchone()[0]
        conn.commit()
        insert_values(pollutant_value, pollution_station_code, code_pol_id, update_id)

end_time = datetime.now()
print("End Time: %s", end_time)
final_time = end_time - start_time
print("Total running time: %s", final_time)