from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import HiveContext
import findspark
findspark.init()

def spark_session(appName):
  return SparkSession.builder \
        .appName(appName) \
        .enableHiveSupport() \
        .getOrCreate()

def sparkcontext():
  return SparkContext.getOrCreate()

def hivecontext():
  return HiveContext(sparkcontext())

def spark_session_local():
    return SparkSession.builder \
        .master('local[1]') \
        .appName(appName) \
        .getOrCreate()

