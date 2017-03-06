# Here is the step by step connection to spark, reading files from different sources and performing baseic operations on these files.

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from datetime import datetime

# first step is to create a spark context (spark session in later versions). Spark session contains SQLContext, HiveContext and Streaming Context.
conf = SparkConf()
conf.set("spark.executor.memory", "15g")
conf.set("spark.cores.max", "2")
spark = SparkSession \
    .builder \
    .appName("Some Application Name") \
    .config(conf = conf) \
    .master("spark://ip-172-31-46-151.ec2.internal:7077")\ #This is the IP address where your master node is located
    .getOrCreate() # if the spark session is not created, this will create a new one

#Now you need to load files, will try csv from two different sources.
# 1. Loading from your local drive. using the "spark.read" option will create a schema at the load time so you dont need to re do 
#creating a schema. and Yes the data types are string for all the columns. Remeber I used the spark 1.4 and this option was not there.
# in lower versions, you need to load to RDD and convert it to a DataFrame.
df = spark.read.option("header", "true").csv("/home/your_path/filename.txt")

# to view your column data types and structure:
df.printSchema()

# here is to change the column type for the schemas:
df = df.withColumn('myMonthcol', df.myMonthcol.cast("int"))\
    .withColumn('myYearcol', df.myYearcol.cast("int"))\
    .withColumn('totalResultcol', df.totalResultcol.cast("int"))
# what if you want to change some string column to date ? This is what I found most difficult. you can create a user defined function
# and call it while doing the conversion, Yes its kind of slow, but very flexible.

datefunc =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y') if x else datetime.strptime('11/11/1111', '%m/%d/%Y'), DateType())
df = df.withColumn('myDate', datefunc(df.myDate))
# read more about spark funcions and spark datatypes to understand whats going on above.

# and done!

# 2. Loading from S3
df = spark.read.option("header", "true").csv("s3n://AWSAccessKeyId:AWSSecretAccessKey@bucketname/*.txt")
#if you get an error for the above line, make sure you have the "hadoop-aws-2.7.1.jar" file in your SPARK_HOME/jars folder.
#SPARK versions 2.0 above dont kind of have this file included. you need to download it and copy it for yourself.

