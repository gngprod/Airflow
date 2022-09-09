import os
import shutil
import pandas as pd
from pyspark.sql import SparkSession, functions as f


def d_csv_pars_spark(out_dir, name):
    csv_file = os.path.join(out_dir, f'{name}.csv')
    parquet_file = os.path.join(out_dir, f'{name}.parquet')

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("sql-film") \
        .getOrCreate()
    spark.read \
        .format("csv") \
        .option("nullValue", "NULL") \
        .options(delimiter=',') \
        .load(csv_file) \
        .createOrReplaceTempView("df")
    df = spark.table('df')
    # df.show()
    df.write.parquet(parquet_file)
    df.drop()


def d_csv_par_panda(out_dir, name):
    csv_file = os.path.join(out_dir, f'{name}.csv')
    parquet_file = os.path.join(out_dir, f'{name}.parquet')
    df = pd.read_csv(csv_file, lineterminator='\n', low_memory=False, encoding="ISO-8859-1")
    df.to_parquet(parquet_file)


def d_read_csv(url, out_dir):
    file_name = url.split('fedstat.ru/')[1].replace('/', '_')
    shutil.rmtree(f'/home/ubuntuos/fedstat/data/{file_name}.parquet', ignore_errors=True)
    d_csv_pars_spark(out_dir, file_name)


def d_task_write_parquet():
    out_dir = '/home/ubuntuos/fedstat/data'

    url1 = 'https://www.fedstat.ru/indicator/59448'
    d_read_csv(url1, out_dir)
    url2 = 'https://www.fedstat.ru/indicator/42928'
    d_read_csv(url2, out_dir)
    url3 = 'https://www.fedstat.ru/indicator/31452'
    d_read_csv(url3, out_dir)


if __name__ == '__main__':
    out_dir = '/home/ubuntuos/fedstat/data'

    url1 = 'https://www.fedstat.ru/indicator/59448'
    d_read_csv(url1, out_dir)
    url2 = 'https://www.fedstat.ru/indicator/42928'
    d_read_csv(url2, out_dir)
    url3 = 'https://www.fedstat.ru/indicator/31452'
    d_read_csv(url3, out_dir)
