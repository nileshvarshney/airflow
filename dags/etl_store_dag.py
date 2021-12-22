# import python libraries
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datacleaner import data_cleaner
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator

yesterday_date = datetime.strftime((datetime.now() - timedelta(1) ),'%Y-%m-%d')

# default argument directory
default_args = {
    "owner" : "Nilesh Varshney",
    "start_date" : datetime(2021,3,21),
    "retries" : 1,
    "retries_delay" : timedelta(seconds=10)
}

dag = DAG('etl_store_dag', default_args=default_args, schedule_interval='@daily', 
 template_searchpath = ['/usr/local/airflow/sql_files'], catchup=False)

#========================================#
# Task section
#========================================#

#Task 1 : Check the source  file exist
check_source_file =  BashOperator(
    task_id = 'check_source_file',
    bash_command = 'shasum ~/store_files_airflow/raw_store_transactions.csv',
    retries=2,
    retry_delay=timedelta(seconds=15),
    dag = dag
)

#Task 2 : clean the input datafile
data_cleaning = PythonOperator(
    task_id = 'clean_raw_csv',
    python_callable = data_cleaner,
    dag = dag
)

#Task 3 : create mysql table
create_table = MySqlOperator(
    task_id = 'create_mysql_table',
    mysql_conn_id="mysql_conn",
    sql = "create_table.sql",
    dag= dag)

#Task 4 : Populate mysql table
populate_table = MySqlOperator(
    task_id = 'populate_table',
    mysql_conn_id="mysql_conn",
    sql = "load_data.sql",
    dag= dag)

# task 5: Generate Aggreegate data
output_report_generation = MySqlOperator(
    task_id = 'output_report_generation',
    mysql_conn_id="mysql_conn",
    sql = "daily_store_profit.sql",
    dag= dag)

# Task 6: To Raname the existing file  if it exists
rename_existing_report_01  = BashOperator(
    task_id =  'rename_existing_report_01',
    bash_command = 'cat ~/store_files_airflow/location_wise_daily_profit.csv && mv ~/store_files_airflow/location_wise_daily_profit.csv ~/store_files_airflow/location_wise_daily_profit_%s.csv' % yesterday_date,
    dag = dag
)

# Task 7: To Raname the existing file  if it exists
rename_existing_report_02  = BashOperator(
    task_id =  'rename_existing_report_02',
    bash_command = 'cat ~/store_files_airflow/store_wise_daily_profit.csv && mv ~/store_files_airflow/store_wise_daily_profit.csv ~/store_files_airflow/store_wise_daily_profit_%s.csv' % yesterday_date,
    dag = dag

)


check_source_file >>  data_cleaning >> create_table  >> populate_table >> output_report_generation >> [rename_existing_report_01,rename_existing_report_02]


