#! /usr/bin/env python3
from __future__ import print_function
import sys
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import scatter_matrix


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

house_df =  spark.read.csv(csvlocation, header="true")
house_df.cache()
house_df.printSchema()
house_df.describe().toPandas().transpose()
numeric_features = [t[0] for t in house_df.dtypes if t[1] == 'int' or t[1] == 'double']
sampled_data = house_df.select(numeric_features).sample(False, 0.8).toPandas()
axs = scatter_matrix(sampled_data, figsize=(10, 10))
n = len(sampled_data.columns)
for i in range(n):
    v = axs[i, 0]
    v.yaxis.label.set_rotation(0)
    v.yaxis.label.set_ha('right')
    v.set_yticks(())
    h = axs[n-1, i]
    h.xaxis.label.set_rotation(90)
    h.set_xticks(())
sys.exit()
