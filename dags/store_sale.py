from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BaseOperator

# default arguments
default_args = {
    'owner' : 'Airflow',
    'start_date' : datetime(2021, 8, 9),
    'retries' : 1,
    'retry_delay' : timedelta(seconds=5)
}


# dag
dag = DAG(
    'store_sale_pipeline', 
    default_args= default_args, 
    schedule_interval='@daily', 
    catchup=False
    )


# check existance of source file
t1 = BaseOperator(
    task_id="check_source_file_existance",  
    bash_command='shasum ~/store_files_airflow/raw_store_transactions.csv',
    retries=2,
    retry_delay=timedelta(seconds=15),
    dag=dag
)

