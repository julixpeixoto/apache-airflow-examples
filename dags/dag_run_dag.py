from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.operators.dagrun_operator import TriggerDagRunOperator

dag = DAG('dag_run_dag',
          description='DAG run another DAG', 
          schedule_interval=None,
          start_date=datetime(2025, 1, 1),
          catchup=False)

task1 = BashOperator(task_id='tsk1', bash_command='sleep 3', dag=dag)
task2 = TriggerDagRunOperator(task_id='tsk2', trigger_dag_id='first_dag', dag=dag)

task1 >> task2