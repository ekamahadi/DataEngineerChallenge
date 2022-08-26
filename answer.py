import os
import pandas as pd
import gzip, shutil
from aws_log_parser import AwsLogParser, LogType
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql import functions
from pyspark.sql.window import Window

#Open Spark Session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Spark AWS ELB Log Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def data_preparation():
    """
    Open the Log File and Transform it to Spark DataFrame
    """

    #Extract gzip log file
    with gzip.open(f"./DataEngineerChallengeAnswer/data/sample_log.log.gz", 'rb') as f_in:
        with open(f"./DataEngineerChallengeAnswer/data/sample_log.log", 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    #Parse Log to Panda DataFrame
    parser = AwsLogParser(log_type=LogType.ClassicLoadBalancer)
    entries = parser.read_url(f"file://{os.getcwd()}/DataEngineerChallengeAnswer/data/sample_log.log")
    df_log = pd.DataFrame(entries)
    
    #Add additional information needed based on JSON data
    df_log["client_ip"]=df_log["client"].map(lambda x : x["ip"])
    df_log["url"]=df_log["http_request"].map(lambda x : x["url"])

    #Define Spark DataFrame Schema
    log_schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("elb", StringType(), True),
        StructField("client", StringType(), True),
        StructField("target", StringType(), True),
        StructField("request_processing_time", FloatType(), True),
        StructField("target_processing_time", FloatType(), True),
        StructField("response_processing_time", FloatType(), True),
        StructField("elb_status_code", StringType(), True),
        StructField("target_status_code", StringType(), True),
        StructField("received_bytes", IntegerType(), True),
        StructField("sent_bytes", IntegerType(), True),
        StructField("http_request", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("ssl_cipher", StringType(), True),
        StructField("ssl_protocol", StringType(), True),
        StructField("instance_id", StringType(), True),
        StructField("client_ip", StringType(), True),
        StructField("url", StringType(), True)
    ])

    #Convert to Spark DataFrame and remove unneeded column
    spark_df = spark.createDataFrame(df_log,schema=log_schema)
    spark_df = spark_df.drop("instance_id")
    
    return spark_df

def data_preprocessing(spark_df):
    """
    Preprocess the Data for answering the question
    """
    ##Add temporary columns for adding information
    
    #Determining hit timestamp from client ip before
    spark_df = spark_df.withColumn("timestamp_before",functions.lag("timestamp").over(Window.partitionBy("client_ip").orderBy("timestamp")))
    
    #Interval between client ip hit
    spark_df = spark_df.withColumn("interval_seconds",(functions.col("timestamp") - functions.col("timestamp_before")).cast(IntegerType()))
    
    #Determining whether the hit is the first in the session. Assuming the window of inactivity is 900 secs (15 mins)
    spark_df = spark_df.withColumn("is_first",functions.when(functions.col("interval_seconds") <= 900, 0).otherwise(1))
    
    #Estimating how long the client ip stays in the session. Assuming if no more than one hit in the session, client will stay for 15 mins
    spark_df = spark_df.withColumn("interval_seconds_in_session",(functions.when(functions.col("interval_seconds") <= 900, functions.col("interval_seconds")).otherwise(900)))
    
    return spark_df
    

def create_session(spark_df):
    """
    Sessionize the Log by IP
    """
    ##Creating session id
    
    #Assigning all client data in one session
    spark_df = spark_df.withColumn("session_id",functions.sum("is_first").over(Window.partitionBy("client_ip").orderBy("timestamp")))
    
    #Assigning the generated session based on dense_rank
    spark_df = spark_df.withColumn("session_id",functions.dense_rank().over(Window.orderBy("client_ip", "session_id")))
    
    return spark_df

def average_session_time(spark_df):
    """
    Count Average Session Time per Session Id
    """
    #Selecting only session id and the interval time within session
    df_avg = spark_df.select(functions.col("session_id"),functions.col("interval_seconds_in_session")).toDF("session_id","interval_seconds_in_session")
    
    #Sum all interval seconds to get estimated total time per session
    df_avg = df_avg.groupBy("session_id").agg(functions.sum("interval_seconds_in_session")).toDF("session_id","session_time")
    
    #Get average from all session total time
    df_avg.agg(functions.avg("session_time")).show()

def unique_url_hit_per_session(spark_df):
    """
    Determine unique URL visits per session
    """
    #Selecting only session id and the url target
    df_unique_hit = spark_df.select(functions.col("session_id"),functions.col("url")).toDF("session_id","url")
    
    #Get count distinct url per session
    df_unique_hit.groupBy("session_id").agg(functions.countDistinct("url")).show()

def most_engaged_ip(spark_df):
    """
    Client IP with total seconds in session in descending order
    """
    #Selecting only client ip and the interval time within session
    df_engage = spark_df.select(functions.col("client_ip"),functions.col("interval_seconds_in_session")).toDF("client_ip","interval_seconds_in_session")
    
    #Sum all interval seconds per client ip to get total time per ip and sort by desc
    df_engage = df_engage.groupBy("client_ip").agg(functions.sum("interval_seconds_in_session")).alias("total_time").toDF("client_ip","total_time")
    df_engage.sort(functions.col("total_time").desc()).show()
    

#Main

#Preprocessing
df = data_preparation()
df = data_preprocessing(df)

#Answer no 1
df = create_session(df)
df.show()

#Answer no 2
average_session_time(df)

#Answer no 3
unique_url_hit_per_session(df)

#Answer no 4
most_engaged_ip(df)