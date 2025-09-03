from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import statistics as stats

dag = DAG('python_operator',
          description='DAG with PythonOperator example', 
          schedule_interval=None,
          start_date=datetime(2025, 1, 1),
          catchup=False)

def data_cleaner(**context):
    dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
    dataset.columns = ['ID', 'Score', 'Estado', 'Genero', 'Idade', 'Patrimonio', 'Saldo', 
                       'Produtos', 'TemCartaoCred', 'Ativo', 'Salario', 'Saiu']
    
    salary_median = stats.median(dataset['Salario'])
    dataset['Salario'].fillna(salary_median, inplace=True)

    dataset['Genero'].fillna('Desconhecido', inplace=True)

    age_median = stats.median(dataset['Idade'])
    dataset['Idade'].fillna(age_median, inplace=True)

    dataset.drop_duplicates(subset='ID', keep='first', inplace=True)

    dataset.to_csv('/opt/airflow/data/Churn_cleaned.csv', index=False)


task1 = PythonOperator(task_id='task1', python_callable=data_cleaner, provide_context=True, dag=dag)

task1