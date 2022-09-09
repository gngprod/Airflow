from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime
import sys
sys.path.append('/home/ubuntuos/nalog')
from task_download_data import d_download_file_main
from task_zip_etl_parquet import d_task_zip_etl_parquet
from airflow.models import Variable


def get_data_path ():
    return Variable.get("fns_data_path")


with DAG("A_dag_nalog", 
        start_date=datetime(2022, 6, 23), 
        schedule_interval="@daily", 
        catchup=False ) as dag:
    task_download_data = PythonOperator(
        task_id="task_download_data",
        python_callable = d_download_file_main,
        op_args=[get_data_path()]
    )
    task_zip_etl_parquet = PythonOperator(
        task_id="task_zip_etl_parquet",
        python_callable = d_task_zip_etl_parquet,
        op_args=[get_data_path()]
    )
    error_email = EmailOperator(
        trigger_rule='one_failed',
        task_id='error_email',
        to='giki2655@gmail.com',
        subject='Airflow failed',
        html_content=""" <h3> failed </h3> """
    )
    send_email = EmailOperator(
        trigger_rule='all_success',
        task_id='send_email',
        to='giki2655@gmail.com',
        subject='Airflow success',
        html_content=""" <h3> success </h3> """
    )
    task_download_data >> task_zip_etl_parquet >> [send_email, error_email]
