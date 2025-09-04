from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from airflow import Dataset

dag = DAG('producer',
          description='DAG with Dataset producer', 
          schedule_interval=None,
          start_date=datetime(2025, 1, 1),
          catchup=False)

mydataset = Dataset('/opt/airflow/data/Churn_new.csv')

def my_file():
    dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
    dataset.to_csv('/opt/airflow/data/Churn_new.csv', sep=';')


task1 = PythonOperator(task_id='task1', python_callable=my_file, dag=dag, outlets=[mydataset])

task1