from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf, col
import random
import string
import math
from delta import *
import sys
import subprocess

class UsedFunctions:

  def randomString(self,length):
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

  def clustered(self,x,numRows):
    return math.floor(x -1)/numRows

  def scattered(self,x,numRows):
    return abs((x -1 % numRows))* 1.0

  def randomised(self,seed,numRows):
    random.seed(seed)
    return abs(random.randint(0, numRows) % numRows) * 1.0

  def padString(self,x,chars,length):
    #n = int(math.log10(x) + 1)
    n = int(x) + 1
    result_str = ''.join(random.choice(chars) for i in range(length-n)) + str(x)
    return result_str

  def padSingleChar(self,chars,length):
    result_str = ''.join(chars for i in range(length))
    return result_str

  def println(self,lst):
    for ll in lst:
      print(ll[0])

usedFunctions = UsedFunctions()

appName="deltatest"
builder = SparkSession.builder \
        .appName(appName) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport()
  

spark = configure_spark_with_delta_pip(builder).getOrCreate()

sc = SparkContext.getOrCreate()

# error level
sc.setLogLevel("ERROR")

lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nStarted at");usedFunctions.println(lst)
hdfs_path_for_delta_table = "hdfs://rhes75:9000/delta/deltaTable"
hdfs_path_for_external_hive_table = "hdfs://rhes75:9000/orc/randomDataDelta"
proc = subprocess.Popen(['hadoop', 'fs', '-test', '-e', hdfs_path_for_delta_table])
proc.communicate()
if proc.returncode != 0:
    print(f"""Path {hdfs_path_for_delta_table} does not exist""")
    sys.exit(1)
proc = subprocess.Popen(['hadoop', 'fs', '-test', '-e', hdfs_path_for_external_hive_table])
proc.communicate()
if proc.returncode != 0:
    print(f"""Path {hdfs_path_for_external_hive_table} does not exist""")
    sys.exit(1)

num_partitions = 10

seed = 100
numRows = abs(random.randint(1, seed))
#
DB = "test"
tableName = "randomDataDelta"
fullyQualifiedTableName =  DB + "."+ tableName
maxID = 0
# Read delta table
df2 = spark.read.format("delta").load(hdfs_path_for_delta_table)
maxID = df2.agg({"id": "max"}).collect()[0][0]
start = 0
if (maxID == 0):
  start = 1
else:
  start = maxID + 1
end = start + numRows - 1
print ("starting at ID = ",start, ",ending on = ",end)
Range = range(start, end)
## This traverses through the Range and increment "x" by one unit each time, and that x value is used in the code to generate random data through Python functions in a class
rdd = sc.parallelize(Range). \
         map(lambda x: (x, usedFunctions.clustered(x,numRows), \
                           usedFunctions.scattered(x,numRows), \
                           usedFunctions.randomised(x,numRows), \
                           usedFunctions.randomString(50), \
                           usedFunctions.padString(x," ",50), \
                           usedFunctions.padSingleChar("x",40)))
df = rdd.toDF(). \
     withColumnRenamed("_1","ID"). \
     withColumnRenamed("_2", "CLUSTERED"). \
     withColumnRenamed("_3", "SCATTERED"). \
     withColumnRenamed("_4", "RANDOMISED"). \
     withColumnRenamed("_5", "RANDOM_STRING"). \
     withColumnRenamed("_6", "SMALL_VC"). \
     withColumnRenamed("_7", "PADDING")

df.write.format("delta").mode("append").save(hdfs_path_for_delta_table)
spark.read.format("delta") \
      .load(hdfs_path_for_delta_table) \
      .repartition(num_partitions) \
      .write \
      .option("orc.compress", "snappy") \
      .orc(hdfs_path_for_external_hive_table, mode="overwrite")
spark.sql(f"""SELECT * FROM {fullyQualifiedTableName} ORDER BY id""").show(10000,False)
rows = spark.sql(f"""SELECT COUNT(1) FROM {fullyQualifiedTableName}""").collect()[0][0]
print (f"""number of rows from ORC table {fullyQualifiedTableName} is {rows}""")
lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nFinished at");usedFunctions.println(lst)
