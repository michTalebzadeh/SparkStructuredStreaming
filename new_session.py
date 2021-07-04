from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf, col
import random
import string
import math
import sys

appName = "dynamic_ARRAY_generator_parquet"
 
spark = SparkSession.builder \
        .enableHiveSupport() \
        .appName(appName) \
        .getOrCreate()


tmp_table = "randomdata"
rows = spark.newSession().sql(f"""SELECT COUNT(1) FROM global_temp.{tmp_table}""").collect()[0][0]
print ("number of rows is ",rows)
sys.exit(0)
