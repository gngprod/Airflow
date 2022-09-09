from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.append('C:/Users/ngzirishvili/PycharmProjects/pythonProject6')
from users import d_users_df
from movies import d_movie_df
from ratings import d_ratings_df
from out import d_out


with DAG("dag_film", start_date=datetime(2022, 6, 9), schedule_interval="@daily", catchup=False ) as dag:
    users_df = PythonOperator(
        task_id="users_df",
        python_callable = d_users_df
    )
    movie_df = PythonOperator(
        task_id="movie_df",
        python_callable = d_movie_df
    )
    ratings_df = PythonOperator(
        task_id="ratings_df",
        python_callable = d_ratings_df
    )
    out = PythonOperator(
        task_id="out",
        python_callable = d_out
    )

    [users_df, movie_df, ratings_df] >> out
