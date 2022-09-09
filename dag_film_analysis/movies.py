from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, \
    DoubleType, BooleanType, DateType
from init_session import d_spark_session


def d_movie_df():
    spark = d_spark_session()
    # movies(movie_id: Int,movie_name: String,genre: String)
    movies_schema = StructType([
        StructField("movie_id", IntegerType(), nullable=True),
        StructField("movie_name", StringType(), nullable=True),
        StructField("genre", StringType(), nullable=True)
    ])
    spark.read \
        .format("csv") \
        .option("nullValue", "NULL") \
        .options(delimiter=',') \
        .schema(movies_schema) \
        .load("C:\data_sql\learn3\movielens\movies\movies.csv") \
        .createTempView("movies")
    movies_df = spark.table('movies')
    movies_df.write.parquet("movies_df.parquet")


if __name__ == '__main__':
    movies_df = d_movie_df()
