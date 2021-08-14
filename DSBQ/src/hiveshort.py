from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.functions import udf, col

from DSBQ.src.configure import config, oracle_url
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.window import Window
from DSBQ.src.configure import config, oracle_url
from DSBQ.sparkutils import sparkstuff as s
from DSBQ.othermisc import usedFunctions as uf

import random
import string
import math
import sys
import pyspark

def main():
    appName = config['common']['appName']
    print("\n appName is", appName)
    spark = s.spark_session(appName)
    sc = s.sparkcontext()
    sc.setLogLevel("ERROR")
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");uf.println(lst)
    ## Do a JDBC to database
    
    df = spark.read \
         .format("jdbc") \
         .option("url", "jdbc:hive2://50.140.197.220:10099/default") \
         .option("dbtable", "test.randomDataPy") \
         .option("user", "hduser") \
         .option("password", "hduser") \
         .load()
    df.show(20,False)
    sc.stop()

if __name__ == "__main__":
  main()

