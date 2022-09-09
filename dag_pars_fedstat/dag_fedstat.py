from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email_operator import EmailOperator
import sys
sys.path.append('/home/ubuntuos/fedstat')
from task_pars_fedstat import d_task_pars_fedstat
from task_write_parquet import d_task_write_parquet


with DAG("dag_fedstat", start_date=datetime(2022, 6, 17), schedule_interval="@hourly", catchup=False ) as dag:
    task_pars_fedstat = PythonOperator(
        task_id="task_pars_fedstat",
        python_callable = d_task_pars_fedstat
    )
    task_write_parquet = PythonOperator(
        task_id="task_write_parquet",
        python_callable = d_task_write_parquet
    )
    error_email = EmailOperator(
        trigger_rule='one_failed',
        task_id='error_email',
        to='giki2655@gmail.com',
        subject='Airflow failed',
        html_content=""" <h3> pars_data_fedstat failed </h3> """
    )
    send_email = EmailOperator(
        trigger_rule='all_success',
        task_id='send_email',
        to='giki2655@gmail.com',
        subject='Airflow success',
        html_content=""" <h3> pars_data_fedstat success </h3> """
    )
    task_pars_fedstat >> task_write_parquet >> [send_email, error_email]

    
