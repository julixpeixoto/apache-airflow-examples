from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable

dag = DAG('dag_var',
          description='DAG with variable', 
          schedule_interval=None,
          start_date=datetime(2025, 1, 1),
          catchup=False)

def print_var(**context):
    apikey = Variable.get("apikey")
    print(f'Variable value: {apikey}')


task1 = PythonOperator(task_id='task1', python_callable=print_var, provide_context=True, dag=dag)

task1