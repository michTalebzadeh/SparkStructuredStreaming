import sys
import os
import pkgutil
import pkg_resources
print("\n printing sys.path")
for p in sys.path:
   print(p)
user_paths = os.environ['PYTHONPATH'].split(os.pathsep)
print("\n Printing user_paths")
for p in user_paths:
   print(p)
v = sys.version
print("\n python version")
print(v)
print("\nlooping over pkg_resources.working_set")
#for r in pkg_resources.working_set:
#   print(r)

"""
#from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
"""
from DSBQ.src.configure import config, oracle_url
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.window import Window
from DSBQ.src.configure import config, oracle_url
from DSBQ.sparkutils import sparkstuff as s
from DSBQ.othermisc import usedFunctions as uf

def main():
    appName = config['common']['appName']
    print("\n appName is", appName)
    spark_session = s.spark_session(appName)
    spark_context = s.sparkcontext()
    print(spark_session)
    print(spark_context)
    spark_context.setLogLevel("ERROR")
    lst = (spark_session.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");uf.println(lst)
    rdd = spark_context.parallelize([1,2,3,4,5,6,7,8,9,10])
    print(rdd.collect())
    print("\n Printing a line from testme")
    spark_context.stop()


if __name__ == "__main__":
  main()
