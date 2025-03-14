from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import sys
import os

# Add the scripts directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts'))

# Import main.py and its classes
from main import main
from demographics_eda import Demographics
from complaints_eda import ComplaintsEDA
from postgres_manager import PostgreSQLManager

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_pipeline',
    default_args=default_args,
    description='ETL pipeline for complaints and demographics',
    schedule_interval='@daily',
    start_date=datetime(2025, 3, 14),
    catchup=False,
) as dag:
    upload_data = PythonOperator(
        task_id='run_etl',
        python_callable=main,
    )
    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres_default',
        sql='/opt/airflow/sql/creating-tables.sql',
    )
    insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='postgres_default',
        sql='/opt/airflow/sql/inserting-data.sql',
    )
    # run_analysis = PostgresOperator(
    #     task_id='run_analysis',
    #     postgres_conn_id='postgres_default',
    #     sql='/opt/airflow/sql/analysis.sql',
    # )

    upload_data >> create_tables >> insert_data 
    # >> run_analysis