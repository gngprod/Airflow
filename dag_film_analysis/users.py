from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, \
    DoubleType, BooleanType, DateType
from init_session import d_spark_session


def d_users_df():
    spark = d_spark_session()
    # users(user_id: Int,gender: String)
    users_schema = StructType([
                      StructField("user_id", IntegerType(), nullable=True),
                      StructField("gender", StringType(), nullable=True)
                      ])
    spark.read \
        .format("csv") \
        .option("nullValue", "NULL") \
        .options(delimiter=',') \
        .schema(users_schema) \
        .load('/mnt/c/data_sql/learn3/movielens/users/users.csv') \
        .createTempView("users")
    users_df = spark.table('users')
    users_df.write.parquet("users_df.parquet")


if __name__ == '__main__':
    users_df = d_users_df()
