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

  def clustered(self,id,numRows):
    return math.floor(id -1)/numRows

  def scattered(self,id,numRows):
    return abs((id -1 % numRows))

  def randomised(self,seed,numRows):
    random.seed(seed)
    return abs(random.randint(0, numRows) % numRows)

  def padString(self,id,chars,length):
    n = int(math.log10(id) + 1)
    result_str = ''.join(random.choice(chars) for i in range(length-n)) + str(id)
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
mm = np.empty(shape=(0,7))
start = 1
end = start + numRows - 1
print ("starting at ID = ",start, ",ending on = ",end)
Range = range(start, end+1)
for i in Range:
  r0 = str(i)
  r1 = str(usedFunctions.clustered(i,numRows)) 
  r2 = str(usedFunctions.scattered(i,numRows))
  r3 = str(usedFunctions.randomised(i,numRows))
  r4 = usedFunctions.randomString(50)
  r5 = usedFunctions.padString(i, " ", 50)
  r6 = usedFunctions.padSingleChar("x", 40)
  mm = np.append(mm,np.array([[r0,r1,r2,r3,r4,r5,r6]]),axis=0)
  ##print(r0,r1,r2,r3,r4,r5,r6)
print("\nAll rows ")
print(mm)
#df = sc.parallelize(mm).toDF
#df.show()
