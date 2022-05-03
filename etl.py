from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.window import Window
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

    df_time.write.mode("overwrite").partitionBy("year", "month").parquet(output + "time")

    # create user dataframe and write to parquet file
    df_user = df.select(func.col("userId").alias("user_id"),
                        func.col("firstName").alias('first_name'),
                        func.col("lastName").alias("last_name"),
                        func.col("gender"),
                        func.col("level")).distinct()

    df_user.write.mode("overwrite").parquet(output + "user")

    # create songplay dataframe and write to parquet file

    song_dir = "data/song_data/*/*/*/*.json"

    songs = spark.read.json(song_dir)

    df_songplay = df.join(songs, [df.artist == songs.artist_name, df.song == songs.title, df.length == songs.duration],
                          "inner")

    df_songplay = df_songplay.withColumn("start_time", (func.col("ts") / 1000).cast("timestamp"))

    # Generating sequential id for table
    df_songplay = df_songplay.withColumn("id", func.monotonically_increasing_id())
    df_songplay = df_songplay.withColumn("songplay_id", func.row_number().over(Window.orderBy("id")))

    # select necessary columns
    df_songplay = df_songplay.select("songplay_id", "start_time", func.col("userId").alias("user_id"),
                                     "level", "song_id", "artist_id",
                                     func.col("sessionId").alias("session_id"),
                                     "location",
                                     func.col("userAgent").alias("user_agent"))

    df_songplay.write.mode("overwrite").parquet(output + "songplay")


def main():
    spark = SparkSession.builder.appName("data_lake_spark").getOrCreate()

    input_dir_song_data = "data/song_data/*/*/*/*.json"
    input_dir_log_data = "data/log_data/*/*/*.json"

    output_dir = "write_data/"

    df_song_data = spark.read.json(input_dir_song_data)
    df_log_data = spark.read.json(input_dir_log_data)

    song_data(df_song_data, output_dir)
    log_data(df_log_data, output_dir, spark)


if __name__ == "__main__":
    main()
