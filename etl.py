import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, LongType as Lg, DateType as Date

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    path_song = input_data + 'song_data/*/*/*/*.json'
    
    ### Modify the schema data types
    ### song_id auto increment field
    Song_Schema = R([
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("title",Str()),
        Fld("year",Int()),
    ])
    
    # read song data file
    song_data = spark.read.json(path_song,schema=Song_Schema)   
    #df = 

    # extract columns to create songs table
    song_extracted_fields=['title','artist_id','year','duration']
    songs=song_data.select(song_extracted_fields).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    songs.write.parquet(output_data + 'songs_full/')
    
    # write songs table to parquet files partitioned by year and artist
    #output_path='data/Output/'
    songs.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_extracted_fields=['artist_id','artist_name as name','artist_location as location','artist_latitude as lattitude','artist_longitude as longitude']
    artists=song_data.selectExpr(artists_extracted_fields).dropDuplicates()
    
    # write artists table to parquet files
    artists.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "/log_data/*.json"

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == 'NextSong')

    # extract columns for users table    
    users_extracted_fields=['userId as user_id','firstName as first_name','lastName as last_name','gender','level']
    users=df_log.selectExpr(users_extracted_fields).dropDuplicates()
    
    #output_path='data/Output/'
    # write users table to parquet files
    users.write.parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    from pyspark.sql.types import TimestampType
    import pyspark.sql.functions as F
    from pyspark.sql import types as T
    from datetime import datetime

    get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000) ), T.TimestampType()) 
    df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))
    
    # extract columns to create time table
    time=df_log.select('timestamp').dropDuplicates()\
    .withColumn("hour", hour(col('timestamp')))\
    .withColumn("day", dayofmonth(col('timestamp')))\
    .withColumn("week", weekofyear(col('timestamp')))\
    .withColumn("month", month(col('timestamp')))\
    .withColumn("year", year(col('timestamp')))\
    .withColumn("weekday", date_format(col("timestamp"), 'E')) 
    
    # write time table to parquet files partitioned by year and month
    time.write.partitionBy("year", "month").parquet(output_data + 'time/')

    # read in song data to use for songplays table
    songs_parquet= spark.read.parquet(output_data+ 'songs/*')
    songplay_song_fields=['song_id','artist_id','artist_name as artist','duration as length','title as song']
    songplay_song=songs_parquet.selectExpr(songplay_song_fields)
    
    songplay_log_fields=['ts','userId','level','sessionId','location','userAgent','artist','length','song','timestamp']
    songplay_log=df_log.select(songplay_log_fields).dropDuplicates()

    # extract columns from joined song and log datasets to create songplays table 
    songplay= songplay_log.join(
        songplay_song,
            (songplay_log.artist == songplay_song.artist)
            & (songplay_log.length == songplay_song.length)
            & (songplay_log.song == songplay_song.song),
        "left"
    )
    songplay_final_fields=['timestamp','userId','level','song_id','artist_id','sessionId','location','userAgent']
  
    songplay_final=songplay.select(songplay_final_fields)\
        .withColumn("month", month(col('timestamp')))\
        .withColumn("year", year(col('timestamp')))\
        .withColumn("songplay_id",monotonically_increasing_id())
    
    # write songplays table to parquet files partitioned by year and month
    songplay_final.write.partitionBy("year", "month").parquet(output_data + 'songplay/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://p4-data-lake-bucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
