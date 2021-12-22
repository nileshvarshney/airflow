from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash_operator import  BashOperator 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import  MySqlOperator
from datacleaner import data_cleaner
from airflow.operators.email_operator import EmailOperator

yesterday_date = datetime.strftime((datetime.now() - timedelta(1) ),'%Y-%m-%d')

default_args = {
    'owner' : 'Nilesh Varshney',
    'start_date' : datetime(2021, 1, 22),
    'retries' : 1,
    'retry_delay' : timedelta(seconds=5)
}

########################################
# DAG
########################################
dag = DAG(
    'store', 
    default_args= default_args,
    schedule_interval='@daily',
    template_searchpath=['/usr/local/airflow/sql_files']
    #schedule_interval='*/2 * * * *'
)

########################################
# check source file exists?
########################################
check_source_file_exists =BashOperator(
    task_id='check_source_file_exists',
    bash_command='shasum ~/store_files_airflow/raw_store_transactions.csv',
    #bash_command='date',
    retries=2,
    retry_delay=timedelta(seconds=15),
    dag = dag
)

########################################
# Clean data file
########################################
clean_data_file = PythonOperator(
    task_id = 'clean_data_file',
    python_callable = data_cleaner,
    dag  = dag
)

########################################
# Create my sql table
########################################
create_table_clean_store_transaction = MySqlOperator(
    task_id = 'create_table_clean_store_transaction',
    mysql_conn_id = "mysql_conn",
    sql = "create_table.sql",
    dag = dag
)


########################################
# load data in clean_store_transaction
########################################
load_clean_store_transaction = MySqlOperator(
    task_id = 'load_clean_store_transaction',
    mysql_conn_id = "mysql_conn",
    sql = "load_data.sql",
    dag = dag
)


########################################
# Location wise profit
########################################
location_wise_profit = MySqlOperator(
    task_id = 'location_wise_profit',
    mysql_conn_id = "mysql_conn",
    sql = "daily_store_profit.sql",
    dag = dag
)

########################################
# Remove old files
########################################
# remove_old_files = BashOperator(
#     task_id = 'remove_old_files',
#     bash_command = 'rm ~/store_files_airflow/store_wise_daily_profit.csv ~/store_files_airflow/location_wise_daily_profit.csv',
#     dag = dag
# )

########################################
# Send  Email
########################################
# send_daily_email=EmailOperator(
#     task_id="send_daily_email",
#     to="example@example.com",
#     subject="Daily Report generated",
#     html_content="""<h1> Congratulations!! Your store report are ready. </h1>""",
#     files=['~/store_files_airflow/store_wise_daily_profit.csv', '~/store_files_airflow/location_wise_daily_profit.csv'],
#     dag = dag
# )

########################################
# Rename Raw File Email
########################################
# rename_old_raw_file = BashOperator(
#     task_id = 'rename_old_raw_file',
#     bash_command = 'mv ~/store_files_airflow/raw_store_transactions.csv ~/store_files_airflow/raw_store_transactions.csv_{$yesterday_date}',
#     dag = dag
# )

check_source_file_exists >> clean_data_file >> create_table_clean_store_transaction >> load_clean_store_transaction >> location_wise_profit
