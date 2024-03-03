# Импортируем библиотеки
import pandas as pd
import pendulum

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine

# Дефолтные аргументы дага. Емайил указываем для оповещениях об ошибках.
args = {
    'owner': 'airflow',
    'pool': 'default_pool',
    'depends_on_past': False,
    'email': 'email',
    'email_on_failure': True,
}

# Указываем данные для подключения
username = 'user'
password = 'password'
host = 'host'
port = 'port'
database = 'database'

# Переменные, исп. далее
airflow_dags_dir = 'Путь'                                                                               # Указываем директорию, где будет лежать скрипт

# Выгружаем данные, которые подготовили ранее в скрипте 'Тестовое'. 
query_dj = """
select id
from df
offset 99
;
"""


def push_result():
	gp_connection = PostgresHook(postgres_conn_id='greenplum')                                          # Для выгрузки используем агрументы, указанные на airflow
	dj = gp_connection.get_pandas_df(sql=query_dj)
    conn = create_engine('postgresql+psycopg2://username:password@host:port/database')                  # Для загрузки укажем данные вручную

    kirill = ('ёйцукенгшщзхъфывапролджэячсмитьбю')                                                      # Отберем буквы кирилллицы
    dj['username'] = dj['username'].replace(to_replace='[234567890{kirill}]', value = '', regex=True)   # Исключаем буквы киррилицы и все цифры кроме '1' 

	table_path = 'df'
	dj.to_sql(name=table_path, con=conn, schema='schema', if_exists='replace', index= False)


# ДАГ        
with DAG(dag_id='test_test',
        max_active_runs=3,
        schedule_interval = '0 0 */10 * *',
        start_date=pendulum.datetime(2024, 2, 24, tz="Europe/Moscow"),
        default_args=args
    ) as dag:
        
    push_result = PythonOperator(
        task_id='push_result',
        python_callable=push_result,
        provide_context=True,)

    push_result