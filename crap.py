from __future__ import print_function
import sys
import findspark
findspark.init()
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
import google.auth
from google import resumable_media



class UsedFunctions:

  def println(self,lst):
    for ll in lst:
      print(ll[0])

usedFunctions = UsedFunctions()

def spark_session(appName):
  return SparkSession.builder \
        .appName(appName) \
        .enableHiveSupport() \
        .getOrCreate()


def sparkcontext():
  return SparkContext.getOrCreate()

def hivecontext():
  return HiveContext(sparkcontext())


appName = "DS"
spark =spark_session(appName)
sc = sparkcontext()

lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nStarted at");usedFunctions.println(lst)


spark.sparkContext.setLogLevel("ERROR")

#HadoopConf = sc._jsc.hadoopConfiguration()
#HadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
#HadoopConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

#bq = spark._sc._jvm.com.samelamin.spark.bigquery.BigQuerySQLContext(spark._wrapped._jsqlContext)

# needed filters
regionname = "Kensington and Chelsea"
start_date = "2010-01-01"
end_date = "2020-01-01"

#get and set the env variables
projectId ="axial-glow-224522"
datasetLocation="europe-west2"
sourceDataset = "staging"
sourceTable = "ukhouseprices"
inputTable = sourceDataset+"."+sourceTable
fullyQualifiedInputTableId = projectId+":"+inputTable
targetDataset = "ds"
targetTable = "summary"
targetRS = "weights_RS"
targetMD = "weights_MODEL"
targetML = "weights_ML_RESULTS"
outputTable = targetDataset+"."+targetTable
outputRS = targetDataset+"."+targetRS
outputMD = targetDataset+"."+targetMD
outputML = targetDataset+"."+targetML
fullyQualifiedoutputTableId = projectId+":"+outputTable
fullyQualifiedoutputRS = projectId+":"+outputRS
fullyQualifiedoutputMD = projectId+":"+outputMD
fullyQualifiedoutputML = projectId+":"+outputML
jsonKeyFile = "/home/hduser/GCPFirstProject-d75f1b3a9817.json"
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/hduser/GCPFirstProject-d75f1b3a9817.json"
os.environ["spark.yarn.appMasterEnv.GOOGLE_APPLICATION_CREDENTIALS"] = "/home/hduser/GCPFirstProject-d75f1b3a9817.json"

tmp_bucket = "gs://tmp_storage_bucket/tmp"
spark.conf.set("GcpJsonKeyFile",jsonKeyFile)
spark.conf.set("BigQueryProjectId",projectId)
spark.conf.set("BigQueryDatasetLocation",datasetLocation)
spark.conf.set("google.cloud.auth.service.account.enable", "true")
spark.conf.set("fs.gs.project.id", projectId)
spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
spark.conf.set("temporaryGcsBucket",tmp_bucket)

sqltext = ""
from pyspark.sql.window import Window

# read data from the Bigquery table in staging area
print("\nreading data from "+projectId+":"+inputTable)

#source_df = spark.read.format("bigquery").option("table", "publicdata.samples.shakespeare").load()

source_df = spark.read. \
              format("bigquery"). \
              option("credentialsFile",jsonKeyFile). \
              option("project", projectId). \
              option("parentProject", projectId). \
              option("dataset", sourceDataset). \
              option("table", sourceTable). \
              load()
source_df.printSchema()

# Get the data pertaining to {regionname} for 10 years and save it in ds dataset
summary_df = source_df.filter((F.col("Date").between(f'{start_date}', f'{end_date}'))  & (F.col("regionname") == (f'{regionname}')))
rows = summary_df.count()
print(f"Total number of rows for {regionname} is ", rows)
# Save data to a BigQuery table
print("\nsaving data to " + outputTable)

# Save the result set to a BigQuery table. Table is created if it does not exist
print("\nsaving data to " + fullyQualifiedoutputTableId)
summary_df. \
    write. \
              format("bigquery"). \
              option("credentialsFile",jsonKeyFile). \
              option("project", projectId). \
              option("parentProject", projectId). \
              option("dataset", targetDataset). \
              option("table", targetTable). \
              mode("overwrite"). \
              save()
"""
source_df.write.format("bigquery") \
  .option("table","test_python.wordcount_output") \
  .save()
"""
lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nFinished at");usedFunctions.println(lst)
