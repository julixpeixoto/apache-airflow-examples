from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from airflow import Dataset

mydataset = Dataset('/opt/airflow/data/Churn_new.csv')

dag = DAG('consumer',
          description='DAG with Dataset consumer', 
          schedule=[mydataset],
          start_date=datetime(2025, 1, 1),
          catchup=False)

def my_file():
    dataset = pd.read_csv('/opt/airflow/data/Churn_new.csv', sep=';')
    dataset.to_csv('/opt/airflow/data/Churn_new2.csv', sep=';')


task1 = PythonOperator(task_id='task1', python_callable=my_file, dag=dag, provide_context=True)

task1