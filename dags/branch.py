from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime
import random

dag = DAG('branch',
          description='Branch tasks based on conditions', 
          schedule_interval=None,
          start_date=datetime(2025, 1, 1),
          catchup=False)


def generate_random_number():
    return random.randint(1, 100)

def avalidate_number(**context):
    number = int(context['task_instance'].xcom_pull(task_ids='generate_random_number'))
    return 'even_task' if number % 2 == 0 else 'odd_task'

generate_random_number_task = BashOperator(
    task_id='generate_random_number',
    bash_command=f'echo {generate_random_number()}',
    dag=dag
)

branch_task = BranchPythonOperator(
    task_id='validate_number',
    python_callable=avalidate_number,
    dag=dag
)

even_task = BashOperator(task_id='even_task', bash_command='echo "par"', dag=dag)
odd_task = BashOperator(task_id='odd_task', bash_command='echo "impar"', dag=dag)

generate_random_number_task >> branch_task >> [even_task, odd_task]
