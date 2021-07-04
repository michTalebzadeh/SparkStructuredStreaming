#! /usr/bin/env python3
from __future__ import print_function
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
from sparkutils import sparkstuff as s
import usedFunctions as uf
import locale
locale.setlocale(locale.LC_ALL, 'en_GB')
try:
  import variables as v
except ModuleNotFoundError:
  from conf import variables as v

appName = "ukhouseprices"
spark = s.spark_session(appName)
spark.sparkContext._conf.setAll(v.settings)
sc = s.sparkcontext()
#
# Get data from Hive table
DB = "DS"
tableName = "OCOD_FULL_2020_12"
fullyQualifiedTableName = DB + '.' + tableName
regionname = "KENSINGTON AND CHELSEA"
lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
print("\nStarted at");uf.println(lst)
if (spark.sql(f"""SHOW TABLES IN {v.DSDB} like '{tableName}'""").count() == 1):
    spark.sql(f"""ANALYZE TABLE {fullyQualifiedTableName} compute statistics""")
    rows = spark.sql(f"""SELECT COUNT(1) FROM {fullyQualifiedTableName}""").collect()[0][0]
    print(f"Total number of rows in table {fullyQualifiedTableName} is ",rows)
else:
    print(f"""No such table {fullyQualifiedTableName}""")
    sys.exit(1)

house_df = spark.sql(f"""select * from {fullyQualifiedTableName}""")
rows = spark.sql(f"""SELECT COUNT(1) FROM {fullyQualifiedTableName} where district= '{regionname}'""").collect()[0][0]
print(f"Total number of rows for {regionname} is ", rows)
wSpecD = Window().partitionBy('district')
df2 = house_df. \
                select( \
                      'district' \
                    , F.count('district').over(wSpecD).alias("OffshoreOwned")). \
                distinct()
wSpecR = Window().orderBy(df2['OffshoreOwned'].desc())

df2. \
       select( \
               col("district").alias("District")
             , F.dense_rank().over(wSpecR).alias("rank")
             , col("OffshoreOwned")). \
       filter(col("rank") <= 10). \
       show(10,False)
