# import the required libraries
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# define default arguments
args = {
    "owner": "Nilesh Varshney",
    "start_date" : datetime(2021,3,23),
    "retries" : 1,
    "retries_delay" : timedelta(seconds=10)
}

dag = DAG('assignment_1', default_args=args,schedule_interval=timedelta(days=1))

# task 1: create test_dir directory
task_1 = BashOperator(task_id='create_directory', bash_command='mkdir ~/dags/test_dir', dag=dag)

# task 2 : get shasum for test_dir directory
task_2  = BashOperator(task_id='get_shasum', bash_command='shasum ~/dags/test_dir', dag=dag)

# set the operator relationship
task_2.set_upstream(task_1)

