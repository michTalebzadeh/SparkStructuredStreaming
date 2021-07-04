#! /usr/bin/env python3
from __future__ import print_function
import sys
import findspark
findspark.init()
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col
#import conf.variables as v
#from sparkutils import sparkstuff as s
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import numpy as np
#import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import scatter_matrix
import six
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
## Get a DF first based on Databricks CSV libraries ignore column heading because of column called "Type"
##
csvlocation="hdfs://rhes75:9000/ds/Boston.csv"

rows = spark.read.csv(csvlocation, header="true").count()
print("\nnumber of rows is ",rows)
if (rows == 0):
         println("Empty CSV directory, aborting!")
         sys.exit(1)

house_df = spark.read.csv(csvlocation, header="true")

# Map the columns to correct data types
##
"""
root
 |-- id: double (nullable = true)
 |-- crim: double (nullable = true)
 |-- zn: double (nullable = true)
 |-- indus: double (nullable = true)
 |-- chas: integer (nullable = true)
 |-- nox: double (nullable = true)
 |-- rm: double (nullable = true)
 |-- age: double (nullable = true)
 |-- dis: double (nullable = true)
 |-- rad: integer (nullable = true)
 |-- tax: integer (nullable = true)
 |-- ptratio: double (nullable = true)
 |-- black: double (nullable = true)
 |-- lstat: double (nullable = true)
 |-- medv: double (nullable = true)
"""
for col_name in house_df.columns:
    if(col_name == "chas" or col_name == "rad" or col_name == "tax"):
        house_df = house_df.withColumn(col_name, F.col(col_name).cast("integer"))
    else:
        house_df = house_df.withColumn(col_name, F.col(col_name).cast("double"))
house_df.printSchema()
print(house_df.dtypes)
house_df.createOrReplaceTempView("tmp")
#
## Check if table exist otherwise create it
DB = "test"
tableName = "boston"
fullyQualifiedTableName = DB + '.' + tableName

spark.sql(f"""DROP TABLE IF EXISTS {fullyQualifiedTableName}""")
sqltext = f"""
    CREATE TABLE {fullyQualifiedTableName}(
      id DOUBLE
    , crim DOUBLE
    , zn DOUBLE 
    , indus DOUBLE
    , chas INTEGER
    , nox DOUBLE 
    , rm DOUBLE
    , age DOUBLE
    , dis DOUBLE
    , rad INTEGER
    , tax INTEGER 
    , ptratio DOUBLE 
    , black DOUBLE
    , lstat DOUBLE 
    , medv DOUBLE 
    )
    COMMENT 'from csv file Boston.csv'
    STORED AS PARQUET
    TBLPROPERTIES ( "parquet.compress"="ZLIB" )
"""
spark.sql(sqltext)
sqltext = f"""
          INSERT INTO {fullyQualifiedTableName}
          SELECT *
          FROM tmp
        """
spark.sql(sqltext)
rows = spark.sql(f"""SELECT COUNT(1) FROM {fullyQualifiedTableName}""").collect()[0][0]
print("number of rows is ",rows)
sys.exit()
