# if two operators need to share information, like a filename or small amount
# of data, you should consider combining them into a single operator. If it
# absolutely can’t be avoided, Airflow does have a feature for
# operator cross-communication called XCom

import airflow
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Nilesh Varshney',
    # "start_date": datetime(2021, 4, 28)
    "start_date": airflow.utils.dates.days_ago(1)
}

DAG = DAG(
    dag_id="com_example",
    description='Simple xcom example of push and pull',
    schedule_interval="@daily",
    default_args=args
)

# python callable


def push_function(**kwargs):
    kwargs['ti'].xcom_push(key="message", value="This is my test message")

# python callable


def pull_function(**kwargs):
    pulled_message = kwargs['ti'].xcom_pull(key="message")
    print(pulled_message)


t1 = PythonOperator(
    task_id='push_task',
    python_callable=push_function,
    provide_context=True,
    dag=DAG)

t2 = PythonOperator(
    task_id='pull_task',
    python_callable=pull_function,
    provide_context=True,
    dag=DAG
)

t1 >> t2
