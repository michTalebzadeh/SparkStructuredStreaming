from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf, col
import random
import string
import math
import pandas as pd
import numpy as np

class UsedFunctions:

  def randomString(self,length):
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

  def clustered(self,x,numRows):
    return math.floor(x -1)/numRows

  def scattered(self,x,numRows):
    return abs((x -1 % numRows))

  def randomised(self,seed,numRows):
    random.seed(seed)
    return abs(random.randint(0, numRows) % numRows)

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

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
HiveContext = HiveContext(sc)

numRows = 5   ## do in increment of 50K rows otherwise you blow up driver memory!
#
## Check if table exist otherwise create it
#
start = 1
end = start + numRows - 1
print ("starting at ID = ",start, ",ending on = ",end)
Range = range(start, end+1)
rdd = sc.parallelize(range(1,4)).map(lambda x: (x, usedFunctions.clustered(x,numRows),usedFunctions.scattered(x,numRows),usedFunctions.randomised(x,numRows),usedFunctions.randomString(50),usedFunctions.padString(x," ",50),usedFunctions.padSingleChar("x",5)))
##rdd = sc.parallelize(Range).map(lambda x: (x, str(x), str(usedFunctions.clustered(x,numRows)), str(usedFunctions.scattered(x,numRows)), str(usedFunctions.randomised(x,numRows)), usedFunctions.randomString(30)), usedFunctions.padString(x," ",50))
###rdd = sc.parallelize(Range).map(lambda i: (i,str(i))).map(lambda i: (i,str(usedFunctions.clustered(5,numRows)))).map(lambda i: (i,str(usedFunctions.scattered(5,numRows)))).map(lambda i: (i,str(usedFunctions.randomised(5,numRows))).map(lambda i: (i,usedFunctions.randomString(30))).map(lambda i: (i,usedFunctions.padString(5," ",50))).map(lambda i: (i,usedFunctions.padSingleChar("x", 40))))
rdd.toDF().show(1000,False)
