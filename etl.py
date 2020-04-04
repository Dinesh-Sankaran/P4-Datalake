"""
Filename: etl.py
Version: 1.0.0
Short Description: Build an ETL pipeline that extracts data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.  

"""

#import all necessary packages, some with alias name for easy access
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, expr
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime
from pyspark.sql.types import StructType as ST, StructField as SF, DoubleType as Double, StringType as Str, IntegerType as Int, DateType as Date, DecimalType as Decimal,LongType as Long, TimestampType as Time

#Read config paramters and assign values to user environment variables
config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Entry to spark application, connect the cluster with an application
      
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Read song dataset from S3 bucket and load the dimensional data(song,artist) back to S3 bucket
    
    Parameters:
        spark: To perform spark operations
        input_data: S3 bucket of source data
        output_data: S3 bucket of output data
    """       
    #get filepath to song data file
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
    
    #Schema definition of song dataset
    song_schema = ST(
        [
            SF('num_songs', Int(), True),
            SF('artist_id', Str(), True),
            SF('artist_latitude', Decimal(), True),
            SF('artist_longitude', Decimal(), True),
            SF('artist_location', Str(), True),
            SF('artist_name', Str(), True),
            SF('song_id', Str(), True),
            SF('title', Str(), True),
            SF('duration', Decimal(), True),
            SF('year', Int(), True)
        ])
        
    # read song data file
    df = spark.read.json(song_data,schema=song_schema)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    #Validation: Print first 10rows and count of song table 
    songs_table.show(10)
    print("Total Songs",songs_table.count())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(output_data+"songs.parquet")

    # extract columns to create artists table
    df.createOrReplaceTempView("song_dataset")
    artists_table = spark.sql("""
                            SELECT 
                                DISTINCT artist_id, 
                                artist_name, 
                                artist_location, 
                                artist_latitude, 
                                artist_longitude 
                            FROM song_dataset
                            """)
    
    #Validation: Print first 10rows and count of artist table 
    artists_table.show(10)
    print("Total Artist",artists_table.count())
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data+"artists.parquet")


def process_log_data(spark, input_data, output_data):
    """
    Read log dataset from S3 bucket and load the data(user,time, songplays) back to S3 bucket
    
    Parameters:
        spark: To perform spark operations
        input_data: S3 bucket of source data
        output_data: S3 bucket of output data
    """
        
    # get filepath to log data file
    log_data =os.path.join(input_data,"log_data/*/*/*.json")

    #Schema definition of log dataset
    log_schema = ST(
        [
            SF('artist', Str(), True),
            SF('auth', Str(), True),
            SF('firstName', Str(), True),
            SF('gender', Str(), True),
            SF('itemInSession', Int(), True),
            SF('lastName', Str(), True),
            SF('length', Decimal(), True),
            SF('level', Str(), True),
            SF('location', Str(), True),
            SF('method', Str(), True),
            SF('page', Str(), True),
            SF('registration', Long(), True),
            SF('sessionId', Int(), True),
            SF('song', Str(), True),
            SF('status', Int(), True),
            SF('ts', Long(), True),
            SF('userAgent', Str(), True),
            SF('userId', Str(), True)
        ])
    
    # read log data file
    df = spark.read.json(log_data, schema=log_schema)
    
    # filter by actions for song plays and cast conversion of userid  
    df = df.where((df.page == "NextSong") & (df.userId != "" )).withColumn('userId', expr("cast(userId as int)"))
    
    # extract columns for users table -- page should be removed below  
    users_table = df.select("userId","firstName","lastName","gender","level").dropDuplicates()
    
    #Validation: Print first 10rows and count of users table 
    users_table.show(10)
    print("user count",users_table.count())
        
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data+"users.parquet")
    
    # create timestamp column from original timestamp column
    df = df.withColumn('start_time', (df.ts/1000)) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp((ts/1000)), Time())
    df = df.withColumn('datetime', get_datetime('ts'))
   
    # extract columns to create time table
    df.createOrReplaceTempView("log_dataset")
    time_table = spark.sql("""
                            SELECT  datetime,
                                    hour(datetime) as Hour,
                                    dayofweek(datetime) as dayofweek,
                                    date_format(datetime, 'EEEE') as Week, 
                                    day(datetime) as Day,
                                    month(datetime) as Month,
                                    year(datetime) as Year,
                                    weekday(datetime) as WeekDay 
                            FROM log_dataset
                            """)
    
    #Validation: Print first 10rows and count of time table
    time_table.show(10)
    print("Time Table Total count",time_table.count())
        
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("Year","Month").parquet(output_data+"time.parquet")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT 
                                    Distinct e.ts,
                                    month(e.datetime) as month,
                                    year(e.datetime) as year,
                                    e.userid,
                                    e.level,
                                    s.song_id,
                                    s.artist_id,
                                    e.sessionid,
                                    e.location,
                                    e.useragent 
                            FROM 
                                log_dataset e 
                            join
                                song_dataset s on e.artist=s.artist_name 
                                and e.length=s.duration 
                                and e.song=s.title
                               """ )
    #Validation: Print first 10rows and count of songplays table
    songplays_table.show(10)
    print("songplays count",songplays_table.count())
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+"songplays.parquet")


def main():
    """
    Establish the spark connection and calls the method to process song and log dataset
    
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-data-lake-p4/"
        
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
