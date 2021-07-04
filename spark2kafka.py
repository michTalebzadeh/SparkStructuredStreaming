from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, TimestampType

import os
import locale

locale.setlocale(locale.LC_ALL, 'it_IT')
import pyspark.sql.functions as F

import uuid

spark = (SparkSession
         .builder
         .master('local')
         .appName('TemperatureStreamApp')
         # Add kafka package
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
         .getOrCreate())

schema = StructType() \
            .add("rowkey", StringType()) \
            .add("timestamp", TimestampType()) \
            .add("temperature", IntegerType())
       

streamingDataFrame = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092", ) \
                .option("subscribe", "temperature") \
                .option("failOnDataLoss", "true") \
                .option("includeHeaders", "true") \
                .option("startingOffsets", "earliest") \
                .load() \
                .select(from_json(col("value").cast("string"), schema).alias("parsed_value"))

resultC = streamingDataFrame.select( \
        col("parsed_value.rowkey").alias("rowkey") \
        , col("parsed_value.timestamp").alias("timestamp") \
        , col("parsed_value.temperature").alias("temperature"))


"""
We work out the window and the AVG(temperature) in the window's timeframe below
This should return back the following Dataframe as struct

 root
 |-- window: struct (nullable = false)
 |    |-- start: timestamp (nullable = true)
 |    |-- end: timestamp (nullable = true)
 |-- avg(temperature): double (nullable = true)

"""
"""
your stuff
resultM = resultC. \
        withWatermark("timestamp", "10 seconds"). \
        groupBy(window(resultC.timestamp, "10 seconds", "10 seconds")). \
        avg('temperature')
"""
resultM = resultC. \
        withWatermark("timestamp", "5 minutes"). \
        groupBy(window(resultC.timestamp, "5 minutes", "5 minutes")). \
        avg('temperature')


# We take the above DataFrame and flatten it to get the columns aliased as "startOfWindowFrame", "endOfWindowFrame" and "AVGTemperature"
resultMF = resultM. \
        select( \
        F.col("window.start").alias("startOfWindow") \
        , F.col("window.end").alias("endOfWindow") \
        , F.col("avg(temperature)").alias("AVGTemperature"))

# Kafka producer requires a key, value pair. We generate UUID key as the unique identifier of Kafka record
uuidUdf = F.udf(lambda: str(uuid.uuid4()), StringType())

"""
We take DataFrame resultMF containing temperature info and write it to Kafka. The uuid is serialized as a string and used as the key.
We take all the columns of the DataFrame and serialize them as a JSON string, putting the results in the "value" of the record.
"""
result = resultMF.withColumn("uuid", uuidUdf()) \
        .selectExpr("CAST(uuid AS STRING) AS key",
                    "to_json(struct(startOfWindow, endOfWindow, AVGTemperature)) AS value") \
        .writeStream \
        .outputMode('update') \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092", ) \
        .option("topic", "avgtemperature") \
        .option('checkpointLocation', "file:///ssd/hduser/avgtemperature/chkpt") \
        .queryName("avgtemperature") \
        .start()


print(result)
print("Chiudo:")
print(result.status)
print(result.recentProgress)
print(result.lastProgress)

result.awaitTermination()
