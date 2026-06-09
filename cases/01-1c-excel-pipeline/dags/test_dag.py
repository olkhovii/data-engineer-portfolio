from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def hello_world():
    print("Hello from Data Engineer Portfolio!")
    return "Success"

with DAG(
    'test_hello_world',
    default_args=default_args,
    description='Тестовый DAG для проверки',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    test_task = PythonOperator(
        task_id='hello_task',
        python_callable=hello_world,
    )
    
    test_task
