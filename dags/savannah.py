import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# Add the project directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.extract_api_data import main as extract_data
from scripts.clean_transform_data import main as clean_data
from scripts.load_data import main as load_data

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 2),
    'retries': 1,
}

dag = DAG(
    'data_pipeline_dag',
    default_args=default_args,
    description='End-to-end data pipeline',
    schedule_interval=None,
)

task_extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

task_clean = PythonOperator(
    task_id='clean_transform_data',
    python_callable=clean_data,
    dag=dag,
)

task_load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

task_run_queries = BashOperator(
    task_id='run_bigquery_queries',
    bash_command='bq query --use_legacy_sql=false < sql/generate_insights.sql',
    dag=dag,
)

task_extract >> task_clean >> task_load >> task_run_queries
