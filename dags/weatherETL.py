from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.definitions.decorators import task
from pendulum import today


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
        # extracting weather data from open meteo API using airflow connectio
