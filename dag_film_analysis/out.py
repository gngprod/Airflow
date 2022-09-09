from pyspark.sql import Window, functions as f
from init_session import d_spark_session


def d_movieid_avgrate_genre(ratings_df, movies_df):  # __________________________________________movieid_avgrate_genre
    avg_rate = ratings_df.groupby('movie_id').avg('rate').orderBy('movie_id')\
        .select('movie_id', f.round('avg(rate)', 2).alias('avg_movie_rate'))
    # print('avg_rate'), avg_rate.show()

    genre_rdd = movies_df.select(f.col('movie_id'), f.split(f.col('genre'), '\|').alias('genre'))
    movie_rdd = genre_rdd.select(f.col('movie_id'), f.explode(genre_rdd.genre).alias('genre'))
    # print('movie_rdd'), movie_rdd.show()

    movieid_avgrate_genre = movie_rdd.join(avg_rate, movie_rdd.movie_id == avg_rate.movie_id, 'Inner')\
        .select('movies.movie_id', 'avg_movie_rate',  'genre').orderBy('movies.movie_id')
    # print('movieid_avgrate_genre'), movieid_avgrate_genre.show()
    return movieid_avgrate_genre


def d_userid_genre_avgrate (movies_df, users_df, ratings_df):  # ______________________________________________userid_genre_avgrate
    genre_rdd = movies_df.select(f.col('movie_id'), f.split(f.col('genre'), '\|').alias('genre'))
    movie_rdd = genre_rdd.select(f.col('movie_id'), f.explode(genre_rdd.genre).alias('genre'))
    join_users_ratings_movies = users_df.join(ratings_df, users_df.user_id == ratings_df.user_id, 'Inner') \
        .join(movie_rdd, movie_rdd.movie_id == ratings_df.movie_id, 'Inner') \
        .orderBy('users.user_id', 'ratings.movie_id') \
        .select(f.col('users.user_id'),
                f.col('gender'),
                f.col('ratings.movie_id'),
                f.col('rate'),
                f.col('genre'))
    # print('join_users_ratings_movies'), join_users_ratings_movies.show()

    w1 = Window().partitionBy('users.user_id', 'genre')
    userid_genre_avgrate = join_users_ratings_movies.select(f.col('users.user_id'),
                                                            f.col('genre'),
                                                            f.avg('rate').over(w1).alias('user_avg_rate')).distinct() \
        .orderBy('users.user_id', f.desc('user_avg_rate'))
    # print('userid_genre_avgrate'), userid_genre_avgrate.show()
    return userid_genre_avgrate


def search (x, userid_genre_avgrate, movieid_avgrate_genre,movies_df, ratings_df):
    top_rate_user = userid_genre_avgrate.filter(f.col('user_id') == x)
    # print('top_rate_user'), top_rate_user.show()
    y = 0
    while True:
        # print(top_rate_user.collect()[y][1])

        df = movieid_avgrate_genre.filter(f.col('genre') == top_rate_user.collect()[y][1]).orderBy(f.desc('avg_movie_rate'))
        # print('df'), df.show()
        # print(df.collect()[0][0])

        max_movie_id = movies_df.select(f.max('movie_id'))
        # print('max_movie_id'), max_movie_id.show()

        user_movie = ratings_df.filter(f.col('user_id') == x) \
                                .filter(f.col('movie_id') <= max_movie_id.collect()[0][0])\
                                .select('movie_id')
        # print('user_movie'), user_movie.show()

        except_df = df.select(f.col('movie_id')).exceptAll(user_movie)
        # print('except_df'), except_df.show()

        if y > len(top_rate_user.collect()):
            return 'Ему смотреть больше нечего!'
        elif len(except_df.collect()) > 0:
            out_movie_name = movies_df.filter(f.col('movie_id') == except_df.collect()[0][0])\
                                        .select('movie_name', 'genre', 'movie_id')
            return f'Ркомендую фильм "{out_movie_name.collect()[0][0]}" жанр "{out_movie_name.collect()[0][1]}" ({out_movie_name.collect()[0][2]})'
        else:
            y +=1
            # print('-----------------------------')
            # print('|          False            |')
            # print('-----------------------------')


def d_out():
    x = 1
    spark = d_spark_session()
    users_df = spark.read.parquet("users_df.parquet")
    movies_df = spark.read.parquet("movies_df.parquet")
    ratings_df = spark.read.parquet("ratings_df.parquet")
    movieid_avgrate_genre = d_movieid_avgrate_genre(ratings_df, movies_df)
    userid_genre_avgrate = d_userid_genre_avgrate(movies_df, users_df, ratings_df)
    message = search(x, userid_genre_avgrate, movieid_avgrate_genre, movies_df, ratings_df)
    message_file = open("message.txt", "w+")
    message_file.write(message)
    message_file.close()


if __name__ == '__main__':
    d_out()
