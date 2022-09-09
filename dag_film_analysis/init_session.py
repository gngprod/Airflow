from pyspark.sql import SparkSession


def d_spark_session():
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("sql-film") \
        .getOrCreate()
    return spark
