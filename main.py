from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import findspark

findspark.init()


def song_data(df, output):

    # create artist data and write to parquet file
    df_artist = df.select(df.artist_id,
                          func.col("artist_name").alias("name"),
                          func.col("artist_location").alias("location"),
                          func.col("artist_latitude").alias("latitude"),
                          func.col("artist_longitude").alias("longitude")).distinct()

    df_artist.write.mode("overwrite").parquet(output + "artist")


def main():
    spark = SparkSession.builder.appName("data_lake_spark").getOrCreate()

    input_dir = "data/song_data/*/*/*/*.json"
    output_dir = "write_data/"
    df_song_data = spark.read.json(input_dir)

    song_data(df_song_data, output_dir)
    df_song_data.printSchema()
    # df_song_data.show()
    # user_df = df_song.select(func.col())


if __name__ == "__main__":
    main()
