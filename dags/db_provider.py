import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG('db_provider',
          description='Database provider', 
          schedule_interval=None,
          start_date=datetime(2025, 1, 1),
          catchup=False)


def print_data_result(ti):
    data = ti.xcom_pull(task_ids='select_data')
    for row in data:
        print("database row:", row)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS api_data (
        id SERIAL PRIMARY KEY,
        data JSONB
    );
    """,
    dag=dag
)

insert_data = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='postgres_connection',
    sql="""
    INSERT INTO api_data (data) VALUES ('{"sample_key": "sample_value"}');
    """,
    dag=dag
)

select_data = PostgresOperator(
    task_id='select_data',
    postgres_conn_id='postgres_connection', 
    sql="SELECT * FROM api_data;",  
    dag=dag
)

print_data = PythonOperator(
    task_id='print_data',
    python_callable=print_data_result,
    provide_context=True,
    dag=dag
)

create_table >> insert_data >> select_data >> print_data
