from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup

dag = DAG('pool',
          description='Run tasks using pool', 
          schedule_interval=None,
          start_date=datetime(2025, 1, 1),
          catchup=False)

task1 = BashOperator(task_id='tsk1', bash_command='sleep 3', dag=dag, pool='custom_pool')
task2 = BashOperator(task_id='tsk2', bash_command='sleep 3', dag=dag, pool='custom_pool', priority_weight=5)
task3 = BashOperator(task_id='tsk3', bash_command='sleep 3', dag=dag, pool='custom_pool')
task4 = BashOperator(task_id='tsk4', bash_command='sleep 3', dag=dag, pool='custom_pool', priority_weight=10)

task1 
task2 
task3 
task4