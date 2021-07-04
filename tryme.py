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

def main():
    appName = "DS"
    spark =spark_session(appName)
    #listing_df = spark.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").load(sys.argv[1])
    # get all Lloyds data archived
    listing_df = spark.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").load("hdfs://rhes75:9000/data/stg/accounts/ll/18201960/archive/")
    listing_df.printSchema()
    #listings_df = spark.sqlContext.read.format('parquet').load(sys.argv[1])
    listing_df.createGlobalTempView("listings")
    clean_df = spark.sql("select id, name, square_feet, price from global_temp.listings where square_feet > 0 and price > 0")

    assembler = VectorAssembler(inputCols= ['square_feet'], outputCol= 'features')
    assembled = assembler.transform(clean_df)
    df = assembled.select(['id', 'name', 'features', 'price'])

    # train the model
    lr = LinearRegression(featuresCol= 'features', labelCol = 'price')
    model = lr.fit(df)

    # make predictions
    predictions = model.transform(df)

    # sort by the gap between preiction and price.
    predictions.createGlobalTempView("predictions")
    values = spark.sql("""SELECT *, price - predictions AS value from global_temp.predictions ORDER BY price - prediction""").show(n=20,truncate=False,vertical=False)

if __name__ == '__main__':
    main()

