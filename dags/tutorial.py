from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from textwrap import dedent

default_args = {
    'owner' : 'airflow',
    'depend_on_past' : False,
    'email' : ['airflow@example.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1, 
    'retries_detay' : timedelta(minutes=5),
}

dag_id =  DAG(
    'airflow_tutorial',
    default_args=default_args,
    description='A simple tutorial dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 12, 21),
    catchup=False,
    tags=['example'],
) 


t1 = BashOperator(
    task_id = 'print_date',
    bash_command='date',
    dag= dag_id,
)

t2 = BashOperator(
    task_id = 'sleep',
    bash_command='sleep 5',
    retries=3,
    dag = dag_id,)

templated_command = dedent(
    """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""
)

t3 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
)

t1 >> [t2,t3]