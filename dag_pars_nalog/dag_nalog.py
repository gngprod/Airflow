from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.append('/home/ubuntuos/nalog/')
from task_download_data import d_download_file_main
from task_zip_etl_parquet import d_task_zip_etl_parquet


with DAG("A_dag_nalog", 
        start_date=datetime(2022, 6, 23), 
        schedule_interval="@daily", 
        catchup=False ) as dag:
    task_download_data = PythonOperator(
        task_id="task_download_data",
        python_callable = d_download_file_main
    )
    task_zip_etl_parquet = PythonOperator(
        task_id="task_zip_etl_parquet",
        python_callable = d_task_zip_etl_parquet
    )

    task_download_data >> task_zip_etl_parquet
