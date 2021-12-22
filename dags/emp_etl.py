from datetime import timedelta, datetime
from airflow import DAG
import requests
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner' : 'airflow',
    'depend_on_past' : False,
    'email' : ['airflow@example.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retries_delay' : timedelta(minutes=5),
}

dag = DAG(
    dag_id='employee_etl',
    description='merge downloaded data in employee table',
    default_args=default_args,
    start_date=datetime(2021,12, 21),
    schedule_interval=timedelta(days = 1),
    catchup=False
)

def get_data():
    url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/pipeline_example.csv"
    response = requests.request("GET", url)

get_data = PythonOperator(
    task_id = 'get_data',
    python_callable= get_data,
    provide_context=True,
    dag = dag)
