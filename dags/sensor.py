import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.sensors.http_sensor import HttpSensor

dag = DAG('sensor',
          description='Sensor example', 
          schedule_interval=None,
          start_date=datetime(2025, 1, 1),
          catchup=False)

def call_api():
    resp = requests.get('https://rickandmortyapi.com/api')
    print(resp.json())

check_api = HttpSensor(
    task_id='check_api',
    http_conn_id='http_connection',
    endpoint='api',
    poke_interval=5,
    timeout=20,
    dag=dag
)

task = PythonOperator(
    task_id='call_api',
    python_callable=call_api,
    dag=dag
)

check_api >> task