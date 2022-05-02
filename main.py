from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import findspark

findspark.init()


def song_data(df, output):
    # create artist dataframe and write to parquet file
    df_artist = df.select(df.artist_id,
                          func.col("artist_name").alias("name"),
                          func.col("artist_location").alias("location"),
                          func.col("artist_latitude").alias("latitude"),
                          func.col("artist_longitude").alias("longitude")).distinct()

    df_artist.write.mode("overwrite").parquet(output + "artist")

    # create song dataframe and write to parquet file
    df_songs = df.select(func.col("song_id"),
                         func.col("title"),
                         func.col("artist_id"),
                         func.col("year"),
                         func.col("duration"))

    df_songs.write.mode("overwrite").partitionBy("year").parquet(output + "songs")


def log_data(df, output, spark):
    # create time dataframe and write to parquet file
    df_time = df.withColumn("start_time", (func.col("ts") / 1000).cast("timestamp"))
    df_time = df_time.withColumn("hour", func.hour(func.col("start_time")))
    df_time = df_time.withColumn("day", func.dayofmonth(df_time.start_time))
    df_time = df_time.withColumn("week", func.weekofyear(df_time.start_time))
    df_time = df_time.withColumn("month", func.month(df_time.start_time))
    df_time = df_time.withColumn("year", func.year(df_time.start_time))
    df_time = df_time.withColumn("weekday", func.dayofweek(df_time.start_time))
    df_time = df_time.select("start_time", "hour", "day", "week", "month", "year", "weekday")

    # df_time.write.mode("overwrite").partitionBy("year", "month").parquet(output + "time")

    # create user dataframe and write to parquet file
    df_user = df.select(func.col("userId").alias("user_id"),
                        func.col("firstName").alias('first_name'),
                        func.col("lastName").alias("last_name"),
                        func.col("gender"),
                        func.col("level")).distinct()

    song_parquet = output + "songs"

    songs = spark.read.parquet(song_parquet)

    songs.where(songs.year != 0).show(n=15)

    df.join(songs, [df.artist_id == songs.artist_id, df.song_id == songs.song_id]).show()

    # df_user.write.mode("overwrite").parquet(output + "user")


def main():
    spark = SparkSession.builder.appName("data_lake_spark").getOrCreate()

    input_dir_song_data = "data/song_data/*/*/*/*.json"
    input_dir_log_data = "data/log_data/*/*/*.json"

    output_dir = "write_data/"

    df_song_data = spark.read.json(input_dir_song_data)
    df_log_data = spark.read.json(input_dir_log_data)

    # song_data(df_song_data, output_dir)
    log_data(df_log_data, output_dir, spark)

    df_log_data.printSchema()
    # df_song_data.show()
    # user_df = df_song.select(func.col())


if __name__ == "__main__":
    main()
