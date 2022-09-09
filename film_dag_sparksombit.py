from datetime import datetime
from airflow import DAG
#from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
#import sys
#sys.path.append('/home/ubuntuos/film')
#from users import d_users_df
#from movies import d_movie_df
#from ratings import d_ratings_df
#from out import d_out
import os
os.environ['SPARK_HOME']="/opt/spark"
os.environ['AIRFLOW_HOME']="/home/ubuntuos/airflow"
os.environ['JAVA_HOME']="/usr/lib/jvm/java-1.8.0-openjdk-amd64"
os.environ['HADOOP_CONF_DIR']="/opt/spark"

dag_spark = DAG("A_film_dag_sparksobmit", 
                start_date=datetime(2022, 6, 15), 
                schedule_interval="@daily", 
                catchup=False )

users_df = SparkSubmitOperator(
    conf ={'spark.master': 'local[*]'},
    application ='/home/ubuntuos/film/users.py',
    conn_id= 'spark_local',
    task_id="users_df",
    dag = dag_spark
)
movie_df = SparkSubmitOperator(
    conf ={'spark.master': 'local[*]'},
    application ='/home/ubuntuos/film/movies.py',
    conn_id= 'spark_local',
    task_id="movie_df",
    dag = dag_spark
)
ratings_df = SparkSubmitOperator(
    conf ={'spark.master': 'local[*]'},
    application ='/home/ubuntuos/film/ratings.py',
    conn_id= 'spark_local',
    task_id="ratings_df",
    dag = dag_spark
)
out = SparkSubmitOperator(
    conf ={'spark.master': 'local[*]'}, 
    application ='/home/ubuntuos/film/out.py',
    conn_id= 'spark_local',
    task_id="out",
    dag = dag_spark
)

[users_df, movie_df, ratings_df] >> out
