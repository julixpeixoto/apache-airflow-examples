from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('dag_with_args',
          description='DAG with args', 
          schedule_interval='@hourly',
          start_date=datetime(2025, 1, 1),
          catchup=False,
          default_view='graph',
          default_args=args,
          tags=['process', 'pipeline'])

task1 = BashOperator(task_id='tsk1', bash_command='sleep 3', dag=dag, retries=3)
task2 = BashOperator(task_id='tsk2', bash_command='sleep 3', dag=dag)
task3 = BashOperator(task_id='tsk3', bash_command='sleep 3', dag=dag)

task1 >> task2 >> task3