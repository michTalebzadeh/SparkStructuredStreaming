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
csvlocation="hdfs://rhes75:9000/data/stg/accounts/ll/18201960/archive"

rows = spark.read.csv(csvlocation, header="true").count()
print("\nnumber of rows is ",rows)
if (rows == 0):
         println("Empty CSV directory, aborting!")
         sys.exit(1)

df = spark.read.csv("hdfs://rhes75:9000/data/stg/accounts/ll/18201960/archive", header="true")

## [Transaction Date: string, Transaction Type: string, Sort Code: string, Account Number: string, Transaction Description: string, Debit Amount: string, Credit Amount: string, Balance: string, : string]

## Map the columns to names
##
a = df. \
         withColumnRenamed("Transaction Date","TRANSACTIONDATE"). \
         withColumnRenamed("Transaction Type", "TRANSACTIONTYPE"). \
         withColumnRenamed("Sort Code", "SORTCODE"). \
         withColumnRenamed("Account Number", "ACCOUNTNUMBER"). \
         withColumnRenamed("Transaction Description", "TRANSACTIONDESCRIPTION"). \
         withColumnRenamed("Debit Amount", "DEBITAMOUNT"). \
         withColumnRenamed("Credit Amount", "CREDITAMOUNT"). \
         withColumnRenamed("Balance", "BALANCE")

a.cache()
a.printSchema()
a.createOrReplaceTempView("tmp")
sqltext = f"""
          SELECT 
                 TRANSACTIONDATE
               , TRANSACTIONTYPE
               , substr(SORTCODE,2, length(SORTCODE)) AS SORTCODE
               , trim(ACCOUNTNUMBER) AS ACCOUNTNUMBER
               , TRANSACTIONDESCRIPTION
               , FLOAT(DEBITAMOUNT)
               , FLOAT(CREDITAMOUNT)
               , FLOAT(BALANCE)
         FROM tmp
        """
clean_df = spark.sql(sqltext)
#clean_df.select("*").show(n=10,truncate=False,vertical=False)
assembler = VectorAssembler(inputCols= ['DEBITAMOUNT'], outputCol= 'features')
assembled = assembler.transform(clean_df)
df2 = assembled.select("TRANSACTIONDATE","TRANSACTIONDESCRIPTION", "DEBITAMOUNT", "features")

# train the model
lr = LinearRegression(featuresCol= 'features', labelCol = 'DEBITAMOUNT')
model = lr.fit(df2)

# make predictions
predictions = model.transform(df2)

# sort by the gap between preiction and price.
predictions.createGlobalTempView("predictions")
spark.sql("""SELECT *, DEBITAMOUNT - predictions AS value from global_temp.predictions ORDER BY DEBITAMOUNT - prediction""").show(n=20,truncate=False,vertical=False)
sys.exit()
