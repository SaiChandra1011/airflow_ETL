from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.definitions.decorators import task
from pendulum import today
import requests
import json

# coordinates of hyderabad
LATITUDE = '17.384'
LONGITUDE = '78.456'

POSTGRES_CONN_ID = "postgres_default"

API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date' : today('UTC').add(days=-1)
}

with DAG(dag_id = 'weather_etl_pipeline',
         default_args=default_args,
         schedule = '@daily',
         catchup = False

) as dags:

    @task()
    def extract_weather_data():
        # extracting weather data from open meteo API using airflow connection

        # using HTTP Hook to get details from airflow
        http_hook = HttpHook(http_conn_id= API_CONN_ID, method = 'GET')

        # building api endpoint
        # https://api.open-meteo.com/v1/forecast?latitude=17.384&longitude=78.456&current_weather=true
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        


    # creating task to transform weather data
    @task()
    def transform_weather_data(weather_data):

        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude' : LATITUDE,
            'longitude' : LONGITUDE,
            'temperature' : current_weather['temperature'],
            'windspeed' : current_weather['windspeed'],
            'winddirection' : current_weather['winddirection'],
            'weathercode' : current_weather['weathercode']
        }


        return transformed_data


    # task to load data into database
    @task()
    def load_weather_data(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(""" 
        CREATE TABLE IF NOT EXISTS weather_data (
                       latitude FLOAT,
                       longitude FLOAT,
                       temperature FLOAT,
                       windspeed FLOAT,
                       winddirection FLOAT,
                       weathercode INT,
                       timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                       );
                       """)
        
        cursor.execute("""
                       INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       """, (
                            transformed_data['latitude'],
                            transformed_data['longitude'],
                            transformed_data['temperature'],
                            transformed_data['windspeed'],
                            transformed_data['winddirection'],
                            transformed_data['weathercode']
                       ))
        
        conn.commit()
        cursor.close()

    # workflow of tasks (ETL Pipeline)
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)


    
        


        




