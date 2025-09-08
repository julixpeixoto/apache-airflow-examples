import requests
from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

dag = DAG('hooks',
          description='Hooks database', 
          schedule_interval=None,
          start_date=datetime(2025, 1, 1),
          catchup=False)

def create_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres_connection')
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS api_data (
        id SERIAL PRIMARY KEY,
        data JSONB
    );
    """
    pg_hook.run(create_table_sql, autocommit=True)


def insert_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres_connection')
    insert_sql = """
    INSERT INTO api_data (data) VALUES ('{"sample_key": "sample_value"}');
    """
    pg_hook.run(insert_sql, autocommit=True)

def select_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_connection')
    select_sql = "SELECT * FROM api_data;"
    records = pg_hook.get_records(select_sql)
    kwargs['ti'].xcom_push(key='data', value=records)

def print_data_result(ti):
    data = ti.xcom_pull(task_ids='select_data', key='data')
    for row in data:
        print("database row:", row)

create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag
)

insert_data = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    dag=dag
)

select_data = PythonOperator(
    task_id='select_data',
    python_callable=select_data,
    provide_context=True,
    dag=dag
)

print_data = PythonOperator(
    task_id='print_data',
    python_callable=print_data_result,
    provide_context=True,
    dag=dag
)

create_table >> insert_data >> select_data >> print_data
