import sys
from src.config import config, oracle_url
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.window import Window
from sparkutils import sparkstuff as s
from othermisc import usedFunctions as uf
import locale
locale.setlocale(locale.LC_ALL, 'en_GB')

def main():
    appName = "testme"
    spark_session = s.spark_session(appName)
    spark_context = s.sparkcontext()
    spark_context.setLogLevel("ERROR")
    print(spark_session)
    print(spark_context)
    rdd = spark_context.parallelize([1,2,3,4,5,6,7,8,9,10])
    print(rdd)
    rdd.collect()
    print(f"""\n Printing a line from {appName}""")


if __name__ == "__main__":
  main()
