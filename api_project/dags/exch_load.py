import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import requests
from datetime import datetime, timedelta
import os
from airflow.hooks.postgres_hook import PostgresHook


DAG_ID = os.path.basename(__file__).replace('.pyc', '').replace('.py', '')
CONN_ID = 'postgres_airflow'

WORKFLOW_DEFAULT_ARGS = {
    'owner': 'airflow',
    #'start_date': datetime(2022, 5, 24),
    #'depends_on_past': False,
    #'retries': 1,
    #'retry_delay': 300,
    #'email_on_retry': False
}

create_sql='CREATE SCHEMA IF NOT EXISTS raw_data\
            AUTHORIZATION airflow;\
            CREATE TABLE IF NOT EXISTS raw_data.exch_rate_BTC_USD\
            (\
                currency_pair character varying(10),\
                dt date,\
                rate numeric\
            ) ;'

def execute_query_with_hook(query):
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run(sql=query)

def get_rate_api():
    #create schema and table if not exists
    execute_query_with_hook(create_sql)
    #get the latest rate from api from pair BTC\USD
    url = 'https://api.exchangerate.host/latest?base=BTC&symbols=USD'
    response = requests.get(url)
    data = response.json()
    date =data['date']
    rate =data['rates']['USD']

    #insert data into pg
    insert_data_sql_query = f"insert into raw_data.exch_rate_BTC_USD (currency_pair, dt, rate) values('BTC/USD', '{date}',{rate}) ;"
    execute_query_with_hook(insert_data_sql_query)

dag_psql = DAG(
    dag_id = DAG_ID,
    default_args=WORKFLOW_DEFAULT_ARGS,
    schedule_interval='0 */3 * * *',
    #schedule_interval='@once',
    dagrun_timeout=timedelta(minutes=60),
    description='Loading exch rate data from api to pg',
    start_date = datetime(2022, 5, 24)
)

start = DummyOperator(task_id='Begin_loading')

rate_api=PythonOperator(
    task_id='rate_api',
    dag=dag_psql,
    python_callable=get_rate_api
)
finish = DummyOperator(task_id='Stop_loading')

start>>rate_api>>finish
