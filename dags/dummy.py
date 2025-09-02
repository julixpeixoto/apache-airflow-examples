from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator

dag = DAG('dummy',
          description='dummy task', 
          schedule_interval=None,
          start_date=datetime(2025, 1, 1),
          catchup=False)

task1 = BashOperator(task_id='tsk1', bash_command='sleep 3', dag=dag)
task2 = BashOperator(task_id='tsk2', bash_command='sleep 3', dag=dag)
task3 = BashOperator(task_id='tsk3', bash_command='sleep 3', dag=dag)
task4 = BashOperator(task_id='tsk4', bash_command='sleep 3', dag=dag)
task5 = BashOperator(task_id='tsk5', bash_command='sleep 3', dag=dag)
task6 = BashOperator(task_id='tsk6', bash_command='sleep 3', dag=dag)
taskdummy = EmptyOperator(task_id="tskDummy", dag=dag)


[task1, task2, task3] >> taskdummy >> [task4, task5, task6]