from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf, col
import random
import string
import math
import usedFunctions as uf
import sparkstuff as s
import sys
import numpy as np
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType, IntegerType, FloatType, DecimalType
from statistics import mean
from math import sqrt
def squared(id):
  return id * id

def testme(AMOUNT_SOLD):
           SQRT((SUM(POWER(AMOUNT_SOLD,2))-(COUNT(1)*POWER(AVG(AMOUNT_SOLD),2)))/(COUNT(1)-1)) 
   
def numpystddev(col):
    return np.std(col)

class Sales:

 def main():
  rec = {}

  appName = "sales"
  spark = s.spark_session(appName)


  settings = [
      ("hive.exec.dynamic.partition", "true"),
      ("hive.exec.dynamic.partition.mode", "nonstrict"),
      ("spark.sql.orc.filterPushdown", "true"),
      ("hive.msck.path.validation", "ignore"),
      ("spark.sql.caseSensitive", "true"),
      ("spark.speculation", "false"),
      ("hive.metastore.authorization.storage.checks", "false"),
      ("hive.metastore.client.connect.retry.delay", "5s"),
      ("hive.metastore.client.socket.timeout", "1800s"),
      ("hive.metastore.connect.retries", "12"),
      ("hive.metastore.execute.setugi", "false"),
      ("hive.metastore.failure.retries", "12"),
      ("hive.metastore.schema.verification", "false"),
      ("hive.metastore.schema.verification.record.version", "false"),
      ("hive.metastore.server.max.threads", "100000"),
      ("hive.metastore.authorization.storage.checks", "/apps/hive/warehouse"),
      ("hive.stats.autogather", "true")
  ]
  spark.sparkContext._conf.setAll(settings)
  sc = s.sparkcontext()
  #print(sc.getConf().getAll())
  hivecontext = s.hivecontext()
  lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
  print("\nStarted at");uf.println(lst)

  #spark.udf.register("squared", squared, IntegerType())
  #df = spark.range(1,6).select(col("id").cast("integer"))
  #df.createOrReplaceTempView("tmp")
  #df.select(col("id"), squared(col("id")).alias("id_squared")).show(10,False)
  #spark.sql("""select id, squared(id) as id_squared from tmp""").show(10,False)
  DB = "oraclehadoop"
  table = "sales"
  rows = spark.sql(f"""SELECT COUNT(1) FROM {DB}.{table}""").collect()[0][0]
  spark.udf.register("numpystddev", numpystddev, FloatType())
  sqltext = f"""
  SELECT
	  rs.Customer_ID
	, rs.Number_of_orders
	, rs.Total_customer_amount
	, rs.Average_order
	, rs.Standard_deviation
        , rs.mystddev
        --, rs.numpystddev
  FROM
  (
	   SELECT cust_id AS Customer_ID
	,  COUNT(amount_sold) AS Number_of_orders
	,  SUM(amount_sold) AS Total_customer_amount
	,  AVG(amount_sold) AS Average_order
	,  STDDEV(amount_sold) AS Standard_deviation
        ,  SQRT((SUM(POWER(AMOUNT_SOLD,2))-(COUNT(1)*POWER(AVG(AMOUNT_SOLD),2)))/(COUNT(1)-1)) AS mystddev
        --,  numpystddev(amount_sold) AS numystddev
           FROM {DB}.{table}
	   GROUP BY cust_id
	   HAVING SUM(amount_sold) > 94000
	   AND AVG(amount_sold) < STDDEV(amount_sold)
  ) rs
  ORDER BY
          3 DESC
  """
  spark.sql(sqltext).show(1000,False)
  df = spark.sql(sqltext)
  df.printSchema()
  lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
  print("\nFinished at");uf.println(lst)
  sys.exit(0)
if __name__ == "__main__":
  print("\nStarting work")
  a = Sales
  a.main()
