import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import requests
from datetime import datetime, timedelta,date
import os
from airflow.hooks.postgres_hook import PostgresHook


DAG_ID = os.path.basename(__file__).replace('.pyc', '').replace('.py', '')
CONN_ID = 'postgres_airflow'
#date from to load exchange rate
min_date=date(2021,1,1)

WORKFLOW_DEFAULT_ARGS = {
    'owner': 'airflow',
    #'start_date': datetime(2022, 5, 24),
    #'depends_on_past': False,
    #'retries': 3,
    #'retry_delay': 300,
    #'email_on_retry': False
}

dag_psql = DAG(
    dag_id = DAG_ID,
    default_args=WORKFLOW_DEFAULT_ARGS,
    schedule_interval='@once',	
    dagrun_timeout=timedelta(minutes=60),
    description='Loading exch rate data from api to pg',
    start_date = datetime(2022, 5, 24)
)

create_sql='CREATE SCHEMA IF NOT EXISTS raw_data\
            AUTHORIZATION airflow;\
            CREATE TABLE IF NOT EXISTS raw_data.exch_rate_BTC_USD\
            (\
                currency_pair character varying(10),\
                dt date,\
                rate numeric\
            );'

def execute_query_with_hook(query):
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run(sql=query)

def get_rate_api():
    execute_query_with_hook(create_sql)
    #delete all data from tbl and get new data from api
    insert_sql='delete from raw_data.exch_rate_BTC_USD ;\
                insert into raw_data.exch_rate_BTC_USD (currency_pair, dt, rate) values'
    for s_date in (min_date+timedelta(n) for n in range ((date.today()-min_date).days)):
        url = f'https://api.exchangerate.host/{s_date}?base=BTC&symbols=USD'
        response = requests.get(url)
        data = response.json()
        date_from_api =data['date']
        rate =data['rates']['USD']

        #insert data into pg
        #Cобраем единый insert 
        insert_sql = insert_sql+f" ('BTC/USD', '{date_from_api}',{rate}) ,"
        
    execute_query_with_hook(insert_sql[:-1])

start = DummyOperator(task_id='Begin_loading')

rate_api=PythonOperator(
    task_id='rate_api',
    dag=dag_psql,
    python_callable=get_rate_api
)
finish = DummyOperator(task_id='Stop_loading')

start>>rate_api>>finish
