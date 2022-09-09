from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, \
    DoubleType, BooleanType, DateType
from init_session import d_spark_session


def d_ratings_df():
    spark = d_spark_session()
    # ratings(movie_id: Int, user_id: Int, rate: Int)
    ratings_schema = StructType([
        StructField("movie_id", IntegerType(), nullable=True),
        StructField("user_id", IntegerType(), nullable=True),
        StructField("rate", IntegerType(), nullable=True)
    ])
    spark.read \
        .format("csv") \
        .option("nullValue", "NULL") \
        .options(delimiter=',') \
        .schema(ratings_schema) \
        .load('C:\data_sql\learn3\movielens\\ratings\\ratings.csv') \
        .createTempView("ratings")
    ratings_df = spark.table('ratings')
    ratings_df.write.parquet("ratings_df.parquet")


if __name__ == '__main__':
    ratings_df = d_ratings_df()
