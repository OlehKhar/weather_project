import json
import airflow
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime as dt

from credentials import api_key

dag = airflow.DAG(
    dag_id='weather_info_collector',
    start_date=dt(2023, 11, 1),
    schedule_interval='*/15 * * * *',  # frequency set to every 15 minutes
    catchup=False,
)

get_locations = PostgresOperator(
    task_id='get_locations',
    dag=dag,
    postgres_conn_id='weather_db',
    sql='''SELECT id, latitude, longitude 
           FROM locations 
           WHERE is_tracking'''
)


def _create_reading_list(**context):
    import requests

    locations_list = context['task_instance'].xcom_pull(
        task_ids='get_locations', key='return_value')
    readings_json_list = []
    for location in locations_list:
        reading = requests.get(
            f'https://api.openweathermap.org/data/2.5/weather?'
            f'lat={location[1]}&lon={location[2]}&appid={api_key}').json()
        reading['location_id'] = location[0]
        readings_json_list.append(reading)
    with open('readings.json', 'w') as f:
        f.write(json.dumps(readings_json_list, indent=4))


create_reading_list = PythonOperator(
    task_id='create_reading_list',
    dag=dag,
    python_callable=_create_reading_list,
)


def _transform_reading_json():
    import csv

    from datetime import timezone as tz
    with open('readings.json', 'r') as f:
        readings_json_list = json.loads(f.read())
    with open('filtered_readings.csv', 'w') as f:
        csvwriter = csv.writer(f)
        for reading in readings_json_list:
            weather_list = [
                reading['location_id'],
                str(dt.fromtimestamp(reading['dt'],
                                     tz.utc).replace(tzinfo=None)),
                str(dt.fromtimestamp(reading['dt'] + reading['timezone'],
                                     tz.utc).replace(tzinfo=None)),
                reading['main']['temp'],
                reading['main']['pressure'],
                reading['main']['humidity'],
                reading['wind']['speed'],
                reading['wind']['deg'],
            ]
            csvwriter.writerow(weather_list)


transform_reading_json = PythonOperator(
    task_id='transform_reading_json',
    dag=dag,
    python_callable=_transform_reading_json,
)


def _push_to_db():
    postgres_hook = PostgresHook(postgres_conn_id='weather_db')
    sql_query = """
            CREATE TEMPORARY TABLE temp_readings (
                id smallint,
                utc_time timestamp,
                local_time timestamp,
                temperature numeric(5, 2),
                pressure smallint,
                humidity smallint,
                wind_speed numeric(5, 2),
                wind_degree smallint
            );
            
            COPY temp_readings 
            FROM stdin 
            WITH CSV;
            
            INSERT INTO readings
            SELECT *
            FROM temp_readings
            ON CONFLICT DO NOTHING;
            """
    postgres_hook.copy_expert(sql_query, 'filtered_readings.csv')


push_to_db = PythonOperator(
    task_id='push_to_db',
    dag=dag,
    python_callable=_push_to_db,
)

get_locations >> create_reading_list >> transform_reading_json >> push_to_db
