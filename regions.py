#! /usr/bin/env python3
from __future__ import print_function
import sys
import findspark
findspark.init()
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext

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
##
## Get a DF first 
##
csvlocation="hdfs://rhes75:9000/ds/regionnames.csv"

rows = spark.read.csv(csvlocation, header="true").count()
print("\nnumber of rows is ",rows)
if (rows == 0):
         println("Empty CSV directory, aborting!")
         sys.exit(1)

house_df = spark.read.csv(csvlocation, header="true")

# get rid of spaces and parentheses in the column names


house_df.printSchema()

## Check if table exist otherwise create it
DB = "DS"
tableName = "regionnames"
fullyQualifiedTableName = DB + '.' + tableName
house_df.write.mode("overwrite").saveAsTable(f"""{fullyQualifiedTableName}""")
sys.exit()
