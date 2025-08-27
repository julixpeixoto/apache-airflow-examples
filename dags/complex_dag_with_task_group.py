from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup

dag = DAG('complex_dag_with_task_group',
          description='Grouping tasks using TaskGroup', 
          schedule_interval=None,
          start_date=datetime(2025, 1, 1),
          catchup=False)

task1 = BashOperator(task_id='tsk1', bash_command='sleep 3', dag=dag)
task2 = BashOperator(task_id='tsk2', bash_command='sleep 3', dag=dag)
task3 = BashOperator(task_id='tsk3', bash_command='sleep 3', dag=dag)
task4 = BashOperator(task_id='tsk4', bash_command='sleep 3', dag=dag)
task5 = BashOperator(task_id='tsk5', bash_command='sleep 3', dag=dag)
task6 = BashOperator(task_id='tsk6', bash_command='sleep 3', dag=dag)

task_group = TaskGroup('task_group', dag=dag)

task7 = BashOperator(task_id='tsk7', bash_command='sleep 3', dag=dag, task_group=task_group)
task8 = BashOperator(task_id='tsk8', bash_command='sleep 3', dag=dag, task_group=task_group)
task9 = BashOperator(task_id='tsk9', bash_command='sleep 5', trigger_rule='one_failed', dag=dag, task_group=task_group)

task1 >> task2
task3 >> task4
[task2, task4] >> task5
task5 >> task6
task6 >> task_group
