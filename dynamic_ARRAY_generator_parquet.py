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
import pyspark

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
    n = int(math.log10(x) + 1)
    result_str = ''.join(random.choice(chars) for i in range(length-n)) + str(x)
    return result_str

  def padSingleChar(self,chars,length):
    result_str = ''.join(chars for i in range(length))
    return result_str

  def println(self,lst):
    for ll in lst:
      print(ll[0])

usedFunctions = UsedFunctions()
 
appName = "dynamic_ARRAY_generator_parquet"
 

spark = SparkSession.builder \
        .enableHiveSupport() \
        .appName(appName) \
        .getOrCreate()

spark.conf.set("spark.sql.adaptive.enabled", "true")

sc = SparkContext.getOrCreate()

sc.setLogLevel("ERROR")

lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nStarted at");usedFunctions.println(lst)

numRows = 10   ## do in increment of 50K rows otherwise you blow up driver memory!
#
## Check if table exist otherwise create it
#
DB = "test"
tableName = "randomDataPy"
fullyQualifiedTableName =  DB + "."+ tableName
tmp_table = "randomdata"
rows = 0
sqltext  = ""
if (spark.sql("SHOW TABLES IN test like 'randomDataPy'").count() == 1):
  rows = spark.sql(f"""SELECT COUNT(1) FROM {fullyQualifiedTableName}""").collect()[0][0]
  print ("number of rows is ",rows)
  dfRead = spark.table("test.randomDataPy")
  dfRead.createOrReplaceGlobalTempView(tmp_table)
  rows = spark.sql(f"""select count(1) from global_temp.{tmp_table}""").collect()[0][0]
  print(f""" \nCPC1 Rows in memory table = {rows}""")
else:
  print("\nTable test.randomDataPy does not exist, creating table ")
  sqltext = """
     CREATE TABLE test.randomDataPy(
       ID INT
     , CLUSTERED INT
     , SCATTERED INT
     , RANDOMISED INT
     , RANDOM_STRING VARCHAR(50)
     , SMALL_VC VARCHAR(50)
     , PADDING  VARCHAR(4000)
    )
    STORED AS PARQUET
    """
  spark.sql(sqltext)

start = 0
if (rows == 0):
  start = 1
else:
  maxID = spark.sql("SELECT MAX(id) FROM test.randomDataPy").collect()[0][0]
  start = maxID + 1
end = start + numRows - 1
print ("starting at ID = ",start, ",ending on = ",end)
Range = range(start, end+1)
## This traverses through the Range and increment "x" by one unit each time, and that x value is used in the code to generate random data through Python functions in a class
rdd = sc.parallelize(Range). \
         map(lambda x: (x, usedFunctions.clustered(x,numRows), \
                           usedFunctions.scattered(x,numRows), \
                           usedFunctions.randomised(x,numRows), \
                           usedFunctions.randomString(50), \
                           usedFunctions.padString(x," ",50), \
                           usedFunctions.padSingleChar("x",4000)))
df = rdd.toDF(). \
     withColumnRenamed("_1","ID"). \
     withColumnRenamed("_2", "CLUSTERED"). \
     withColumnRenamed("_3", "SCATTERED"). \
     withColumnRenamed("_4", "RANDOMISED"). \
     withColumnRenamed("_5", "RANDOM_STRING"). \
     withColumnRenamed("_6", "SMALL_VC"). \
     withColumnRenamed("_7", "PADDING")
#df.write.mode("overwrite").saveAsTable("test.ABCD")
df.printSchema()
#df.explain()
# Add new data to dfRead
df = df.union(dfRead)
df.createOrReplaceTempView(tmp_table)
#df.persist(pyspark.StorageLevel.DISK_ONLY)
spark.catalog.uncacheTable(tmp_table)
df.createOrReplaceGlobalTempView(tmp_table)
#df.persist(pyspark.StorageLevel.DISK_ONLY)
spark.catalog.cacheTable(tmp_table)
rows = spark.newSession().sql(f"""select count(1) from global_temp.{tmp_table}""").collect()[0][0]
print(f""" \nnewSession Rows in memory table = {rows}""")
df.count()
print(rdd.toDebugString())
sqltext = f"""
  INSERT INTO TABLE test.randomDataPy
  SELECT
          ID
        , CLUSTERED
        , SCATTERED
        , RANDOMISED
        , RANDOM_STRING
        , SMALL_VC
        , PADDING
  FROM {tmp_table}
  """
spark.sql(sqltext)
spark.sql("""SELECT MIN(id) AS minID, MAX(id) AS maxID FROM test.randomDataPy""").show(n=20,truncate=False,vertical=False)
##spark.sql("""SELECT * FROM test.randomDataPy ORDER BY id""").show(n=20,truncate=False,vertical=False)
lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nFinished at");usedFunctions.println(lst)
