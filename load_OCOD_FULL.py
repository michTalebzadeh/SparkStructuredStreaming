#! /usr/bin/env python3
from __future__ import print_function
import sys
import findspark
findspark.init()
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext

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
## Get a DF first 
##
csvlocation="hdfs://rhes75:9000/ds/OCOD_FULL_2020_12.csv"

rows = spark.read.csv(csvlocation, header="true").count()
print("\nnumber of rows is ",rows)
if (rows == 0):
         println("Empty CSV directory, aborting!")
         sys.exit(1)

house_df = spark.read.csv(csvlocation, header="true")

# get rid of spaces and parentheses in the column names

house_df = house_df.withColumnRenamed("Title Number", "TitleNumber") . \
                    withColumnRenamed("Property Address", "PropertyAddress") . \
                    withColumnRenamed("Multiple Address Indicator", "MultipleAddressIndicator"). \
                    withColumnRenamed("Price Paid", "PricePaid") . \
                    withColumnRenamed("Proprietor Name (1)", "ProprietorName1"). \
                    withColumnRenamed("Company Registration No. (1)", "CompanyRegistrationNo1"). \
		    withColumnRenamed("Proprietorship Category (1)", "ProprietorshipCategory1"). \
                    withColumnRenamed("Country Incorporated (1)", "CountryIncorporated1"). \
                    withColumnRenamed("Proprietorship Category (1)", "ProprietorshipCategory1"). \
                    withColumnRenamed("Proprietor (1) Address (1)", "Proprietor1Address1"). \
                    withColumnRenamed("Proprietor (1) Address (2)", "Proprietor1Address2"). \
                    withColumnRenamed("Proprietor (1) Address (3)", "Proprietor1Address3"). \
                    withColumnRenamed("Proprietor Name (2)", "ProprietorName2"). \
                    withColumnRenamed("Company Registration No. (2)", "CompanyRegistrationNo2"). \
                    withColumnRenamed("Proprietorship Category (2)", "ProprietorshipCategory2"). \
                    withColumnRenamed("Country Incorporated (2)", "CountryIncorporated2"). \
                    withColumnRenamed("Proprietor (2) Address (1)", "Proprietor2Address1"). \
                    withColumnRenamed("Proprietor (2) Address (2)", "Proprietor2Address2"). \
                    withColumnRenamed("Proprietor (2) Address (3)", "Proprietor2Address3"). \
                    withColumnRenamed("Proprietor Name (3)", "ProprietorName3"). \
                    withColumnRenamed("Company Registration No. (3)", "CompanyRegistrationNo3"). \
                    withColumnRenamed("Proprietorship Category (3)", "ProprietorshipCategory3"). \
                    withColumnRenamed("Country Incorporated (3)", "CountryIncorporated3"). \
                    withColumnRenamed("Proprietor (3) Address (1)", "Proprietor3Address1"). \
                    withColumnRenamed("Proprietor (3) Address (2)", "Proprietor3Address2"). \
                    withColumnRenamed("Proprietor (3) Address (3)", "Proprietor3Address3"). \
                    withColumnRenamed("Proprietor Name (4)", "ProprietorName4"). \
                    withColumnRenamed("Company Registration No. (4)", "CompanyRegistrationNo4"). \
                    withColumnRenamed("Proprietorship Category (4)", "ProprietorshipCategory4"). \
                    withColumnRenamed("Country Incorporated (4)", "CountryIncorporated4"). \
                    withColumnRenamed("Proprietor (4) Address (1)", "Proprietor4Address1"). \
                    withColumnRenamed("Proprietor (4) Address (2)", "Proprietor4Address2"). \
                    withColumnRenamed("Proprietor (4) Address (3)", "Proprietor4Address3"). \
                    withColumnRenamed("Date Proprietor Added", "DateProprietorAdded"). \
                    withColumnRenamed("Additional Proprietor Indicator", "AdditionalProprietorIndicator")

# Map the columns to correct data types
##
for col_name in house_df.columns:
    if(col_name == "DateProprietorAdded"):
        house_df = house_df.withColumn(col_name, F.col(col_name).cast("date"))
    elif(col_name == "PricePaid"):
        house_df = house_df.withColumn(col_name, F.col(col_name).cast("integer"))

house_df.printSchema()

## Check if table exist otherwise create it
DB = "DS"
tableName = "OCOD_FULL_2020_12"
fullyQualifiedTableName = DB + '.' + tableName
regionname = "Kensington and Chelsea"
house_df.write.mode("overwrite").saveAsTable(f"""{fullyQualifiedTableName}""")
sys.exit()
