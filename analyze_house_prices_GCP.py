from __future__ import print_function
import sys
import findspark
findspark.init()
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext


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

# Back to BigQuery table
tmp_bucket = "tmp_storage_bucket/tmp"
# Set the temporary storage location
spark.conf.set("temporaryGcsBucket",tmp_bucket)
spark.sparkContext.setLogLevel("WARN")

HadoopConf = sc._jsc.hadoopConfiguration()
HadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
HadoopConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

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
outputTable2 = "DS.crap"
outputRS = targetDataset+"."+targetRS
outputMD = targetDataset+"."+targetMD
outputML = targetDataset+"."+targetML
fullyQualifiedoutputTableId = projectId+":"+outputTable
fullyQualifiedoutputRS = projectId+":"+outputRS
fullyQualifiedoutputMD = projectId+":"+outputMD
fullyQualifiedoutputML = projectId+":"+outputML
jsonKeyFile="/home/hduser/GCPFirstProject-d75f1b3a9817.json"

spark.conf.set("GcpJsonKeyFile",jsonKeyFile)
spark.conf.set("BigQueryProjectId",projectId)
spark.conf.set("BigQueryDatasetLocation",datasetLocation)
spark.conf.set("google.cloud.auth.service.account.enable", "true")
spark.conf.set("fs.gs.project.id", projectId)
spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
spark.conf.set("temporaryGcsBucket", tmp_bucket)
sqltext = ""

# read data from the Bigquery table in staging area
print("\nreading data from "+projectId+":"+inputTable)
source_df = spark.read. \
              format("bigquery"). \
              option("credentialsFile",jsonKeyFile). \
              option("project", projectId). \
              option("parentProject", projectId). \
              option("dataset", sourceDataset). \
              option("table", sourceTable). \
              load()
source_df.printSchema()
source_df.show(2,False)

# Get the data pertaining to {regionname} for 10 years and save it in ds dataset
summary_df = source_df.filter((F.col("Date").between(f'{start_date}', f'{end_date}'))  & (F.col("regionname") == (f'{regionname}')))
rows = summary_df.count()
print(f"Total number of rows for {regionname} is ", rows)
# Save data to a BigQuery table
print("\nsaving data to " + outputTable)
summary_df. \
    write. \
    format("bigquery"). \
    mode("overwrite"). \
    option("table", fullyQualifiedoutputTableId). \
    save()

print("\nreading data from "+projectId+":"+outputTable)
target_df = spark.read. \
              format("bigquery"). \
              option("credentialsFile",jsonKeyFile). \
              option("project", projectId). \
              option("parentProject", projectId). \
              option("dataset", targetDataset). \
              option("table", targetTable). \
              load()
target_df.show(1,False)

lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nFinished at");usedFunctions.println(lst)
