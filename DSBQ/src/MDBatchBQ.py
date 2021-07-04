from __future__ import print_function
from config import config
import sys
#print (sys.path)
from sparkutils import sparkstuff as s	
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType,IntegerType, FloatType, TimestampType
from google.cloud import bigquery
import os
import locale
locale.setlocale(locale.LC_ALL, 'en_GB')
from pyspark.sql import functions as F
import pyspark.sql.functions as F
from decimal import getcontext, Decimal

def f(a):
    stop

def sendToSink(df, batchId):
    if(len(df.take(1))) > 0:
        print(f"""md batchId is {batchId}""")
        df.show(100,False)
        df. persist()
        # write to BigQuery batch table
        #s.writeTableToBQ(df, "append", config['MDVariables']['targetDataset'],config['MDVariables']['targetTable'])
        df.unpersist()
        print(f"""wrote to DB""")
    else:
        print("DataFrame md is empty")

def sendToControl(dfnewtopic, batchId):
    if(len(dfnewtopic.take(1))) > 0:
        #print(f"""newtopic batchId is {batchId}""")
        dfnewtopic.show(100,False)
        queue = dfnewtopic.select(col("queue")).collect()[0][0]
        status = dfnewtopic.select(col("status")).collect()[0][0]
        #print(f"""{queue}, {status}""") 
        if((queue == config['MDVariables']['topic']) & (status == 'false')):
          spark_session = s.spark_session(config['common']['appName'])
          active = spark_session.streams.active
          for e in active:
             #print(e)
             name = e.name
             if(name == config['MDVariables']['topic']):
                print(f"""Terminating streaming process {name}""")
                e.stop()
    else:
        print("DataFrame newtopic is empty")


class MDStreaming:
    def __init__(self,spark_session,spark_context):
        self.spark = spark_session
        self.sc = spark_context
        self.config = config

    def fetch_data(self):
        self.sc.setLogLevel("ERROR")
        #{"rowkey":"c9289c6e-77f5-4a65-9dfb-d6b675d67cff","ticker":"MSFT", "timeissued":"2021-02-23T08:42:23", "price":31.12}
        schema = StructType().add("rowkey", StringType()).add("ticker", StringType()).add("timeissued", TimestampType()).add("price", FloatType())
        newtopicSchema = StructType().add("uuid", StringType()).add("timeissued", TimestampType()).add("queue", StringType()).add("status", StringType())
        checkpoint_path = "file:///ssd/hduser/MDBatchBQ/chkpt"
        try:
            streamingNewtopic = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", config['MDVariables']['bootstrapServers'],) \
                .option("schema.registry.url", config['MDVariables']['schemaRegistryURL']) \
                .option("group.id", config['common']['newtopic']) \
                .option("zookeeper.connection.timeout.ms", config['MDVariables']['zookeeperConnectionTimeoutMs']) \
                .option("rebalance.backoff.ms", config['MDVariables']['rebalanceBackoffMS']) \
                .option("zookeeper.session.timeout.ms", config['MDVariables']['zookeeperSessionTimeOutMs']) \
                .option("auto.commit.interval.ms", config['MDVariables']['autoCommitIntervalMS']) \
                .option("subscribe", config['MDVariables']['newtopic']) \
                .option("failOnDataLoss", "false") \
                .option("includeHeaders", "true") \
                .option("startingOffsets", "latest") \
                .load() \
                .select(from_json(col("value").cast("string"), newtopicSchema).alias("newtopic_value"))

            # construct a streaming dataframe streamingDataFrame that subscribes to topic config['MDVariables']['topic']) -> md (market data)
            streamingDataFrame = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", config['MDVariables']['bootstrapServers'],) \
                .option("schema.registry.url", config['MDVariables']['schemaRegistryURL']) \
                .option("group.id", config['common']['appName']) \
                .option("zookeeper.connection.timeout.ms", config['MDVariables']['zookeeperConnectionTimeoutMs']) \
                .option("rebalance.backoff.ms", config['MDVariables']['rebalanceBackoffMS']) \
                .option("zookeeper.session.timeout.ms", config['MDVariables']['zookeeperSessionTimeOutMs']) \
                .option("auto.commit.interval.ms", config['MDVariables']['autoCommitIntervalMS']) \
                .option("subscribe", config['MDVariables']['topic']) \
                .option("failOnDataLoss", "false") \
                .option("includeHeaders", "true") \
                .option("startingOffsets", "latest") \
                .load() \
                .select(from_json(col("value").cast("string"), schema).alias("parsed_value"))


            #streamingDataFrame.printSchema()
            streamingNewtopic.printSchema()
            #print(streamingNewtopic.isStreaming)

            """   
               "foreach" performs custom write logic on each row and "foreachBatch" performs custom write logic on each micro-batch through sendToSink function
                foreachBatch(sendToSink) expects 2 parameters, first: micro-batch as DataFrame or Dataset and second: unique id for each batch
               Using foreachBatch, we write each micro batch to storage defined in our custom logic. In this case, we store the output of our streaming application to Google BigQuery table.
               Note that we are appending data and column "rowkey" is defined as UUID so it can be used as the primary key
            """
            newtopicResult = streamingNewtopic.select( \
                     col("newtopic_value.uuid").alias("uuid") \
                   , col("newtopic_value.timeissued").alias("timeissued") \
                   , col("newtopic_value.queue").alias("queue") \
                   , col("newtopic_value.status").alias("status")). \
                     writeStream. \
                     outputMode('append'). \
                     option("truncate", "false"). \
                     foreachBatch(sendToControl). \
                     trigger(processingTime='2 seconds'). \
                     queryName(config['MDVariables']['newtopic']). \
                     start()

            result = streamingDataFrame.select( \
                     col("parsed_value.rowkey").alias("rowkey") \
                   , col("parsed_value.ticker").alias("ticker") \
                   , col("parsed_value.timeissued").alias("timeissued") \
                   , col("parsed_value.price").alias("price")). \
                     writeStream. \
                     outputMode('append'). \
                     option("truncate", "false"). \
                     foreachBatch(sendToSink). \
                     trigger(processingTime='30 seconds'). \
                     option('checkpointLocation', checkpoint_path). \
                     queryName(config['MDVariables']['topic']). \
                     start()
            print(result)

        except Exception as e:
                print(f"""{e}, quitting""")
                sys.exit(1)
            
        #print(result.status)
        #print(result.recentProgress)
        #print(result.lastProgress)

        self.spark.streams.awaitAnyTermination()
        result.awaitTermination()
        #newtopicResult.awaitTermination()

if __name__ == "__main__":
    
    #appName = config['common']['appName']
    appName = "batch"
    spark_session = s.spark_session(appName)
    spark_session = s.setSparkConfStreaming(spark_session)
    spark_session = s.setSparkConfBQ(spark_session)
    spark_context = s.sparkcontext()
    mdstreaming = MDStreaming(spark_session, spark_context)
    streamingDataFrame = mdstreaming.fetch_data()
