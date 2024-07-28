from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from plugins.nyc_plugin import NycPersistDfDataToPostgresTable
from airflow.operators.python import BranchPythonOperator
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.variable import Variable

dag_owner = 'zeed'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='nyc-taxi',
        default_args=default_args,
        description='',
        start_date=datetime(year=2024, month=1, day=1),
        schedule_interval=timedelta(days=1),
        catchup=True,
        tags=['']
):
    
    def record_count(ti):
        date_str = ti.xcom_pull(key='return_value', task_ids='push_date_to_xcom')
        print(f'Record date is {date_str}')
        postgres_hook = PostgresHook('postgres-airflow')
        source_conn = postgres_hook.get_conn()
        query_cursor = source_conn.cursor()
        query_cursor.execute(f"select count(*) from taxi_data where date(tpep_pickup_datetime) = '{date_str}-1'")
        count = query_cursor.fetchone()[0]
        print(f'Result count is {count}')
        if count == 0:
            return 'download_file'
        else:
            return 'do_nothing'

    start = EmptyOperator(task_id='start')

    push_date_to_xcom = BashOperator(
        task_id="push_date_to_xcom",
        bash_command='STR={{ds}} && echo "${STR:0:7}"', do_xcom_push=True,
    )

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=record_count, provide_context=True
    )
    
    download_file = bash_task = BashOperator(
        task_id="download_file",
        bash_command='STR={{ds}} && curl -o {{var.value.DOWNLOAD_FILE_PATH}}/nyc-taxi-file-{{ ti.xcom_pull(task_ids=\'push_date_to_xcom\') }}.parquet {{var.value.NYC_TAXI_DOWNLOAD_LINK_BASE_URL}}yellow_tripdata_{{ ti.xcom_pull(task_ids=\'push_date_to_xcom\') }}.parquet',
    )

    @task
    def load_parquet_file(**kwargs):
        ti = kwargs['ti']
        taxi_data = pd.read_parquet(f"{Variable.get('DOWNLOAD_FILE_PATH')}/nyc-taxi-file-{ti.xcom_pull(task_ids='push_date_to_xcom')}.parquet")
        print(taxi_data.head())
        nyc_persist_hook = NycPersistDfDataToPostgresTable()
        nyc_persist_hook.execute(postgres_conn_id='postgres-airflow', dataframe=taxi_data)
        pass

    
    @task
    def do_nothing():
        print('Not processing further')

    end = EmptyOperator(task_id='end', trigger_rule='one_success')

    start >> push_date_to_xcom >> branch_task 
    branch_task >> do_nothing() >> end
    branch_task >> download_file >> load_parquet_file() >> end