from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

dag = DAG('dag_xcom',
          description='DAG with XCOM', 
          schedule_interval=None,
          start_date=datetime(2025, 1, 1),
          catchup=False)

def task_write(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key='number', value=33)

def task_read(**kwargs):
    ti = kwargs['ti']
    value = ti.xcom_pull(key='number')
    print(f'Retrieved XCOM value: {value}')

task1 = PythonOperator(task_id='task_write', python_callable=task_write, provide_context=True, dag=dag)
task2 = PythonOperator(task_id='task_read', python_callable=task_read, provide_context=True, dag=dag)

task1 >> task2