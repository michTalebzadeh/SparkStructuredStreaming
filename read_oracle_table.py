#! /usr/bin/env python
from __future__ import print_function
# jdbc stuff
import jaydebeapi
import jpype
from jpype import *

# aerospike stuff
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

from aerospike import exception as ex

import sys
import pprint
import csv

class main:
  
  # oracle variables
  driverName = "oracle.jdbc.OracleDriver"
  url= "jdbc:oracle:thin:@rhes564:1521:mydb12"
  _username = "scratchpad"
  _password = "oracle"
  _dbschema = "SCRATCHPAD"
  _dbtable = "DUMMY"
  csv_file_name = "/home/hduser/dba/bin/python/DUMMY.csv"
  
  # aerospike variables
  dbHost = "rhes75"
  dbPort = 3000
  dbConnection = "mich"
  namespace = "test"
  dbPassword = "aerospike"
  dbSet = "oracletoaerospike2"
  dbKey = "ID"
  rec = {}

  def read_oracle_table(self):
    # Check Oracle is accessible
    try:
      connection = jaydebeapi.connect(self.driverName, self.url, [self._username, self._password])
    except jaydebeapi.Error as e:
      print("Error: {0} [{1}]".format(e.msg, e.code))
      sys.exit(1)
    else:
      # Check if table exists
      metadata = connection.jconn.getMetaData()
      rs = metadata.getTables(None, self._dbschema, self._dbtable, None)
      if (rs.next()):
        print("\nTable " + self._dbschema+"."+ self._dbtable + " exists\n")
        cursor = connection.cursor()
        sql="""
              SELECT ID, CLUSTERED, SCATTERED, RANDOMISED, RANDOM_STRING, SMALL_VC, PADDING FROM scratchpad.dummy where ROWNUM <= 10
            """
        cursor.execute(sql)
        # get column descriptions
        columns = [i[0] for i in cursor.description]
        rows = cursor.fetchall()
        # write oracle data to the csv file
        csv_file = open(self.csv_file_name, mode='w')
        writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
        # write coumn headers to csv file
        writer.writerow(columns)
        for row in rows:
          writer.writerow(row)   ## write rows to csv file
 
        print("writing to csv file complete")
        cursor.close()
        connection.close()
        csv_file.close()
        sys.exit(0)
      else:
        print("Table " + self._dbschema+"."+ self._dbtable + " does not exist, quitting!")
        connection.close()
        sys.exit(1)

  def read_aerospike_set(self):
    # Check aerospike is accessible
    try:
      config = { 
          'hosts': [(self.dbHost, self.dbPort)],
          'policy': {'aerospike.POLICY_KEY_SEND': 'true', 'read': {'total_timeout': 1000}}
      }
      client = aerospike.client(config).connect(self.dbConnection, self.dbPassword)
    except ex.ClientError:
      print("Error: {0} [{1}]".format(e.msg, e.code))
      sys.exit(1)
    else:
      print("Connection successful")
      keys = []
      for k in xrange(1,10):
         key = (self.namespace, self.dbSet, str(k))
         keys.append(key)

      records = client.get_many(keys)
      pprint.PrettyPrinter(depth=4).pprint (records)
      print("\nget everyting for one record with pk = '9'")
      (key, meta, bins)= client.get((self.namespace, self.dbSet, '9'))
      print (key)
      print (meta)
      print (bins)
      client.close()
      sys.exit(0)

  def write_aerospike_set(self):
    # Check aerospike is accessible
    try:
      config = {
          'hosts': [(self.dbHost, self.dbPort)],
          'policy': {'aerospike.POLICY_KEY_SEND': 'true', 'read': {'total_timeout': 1000}}
      }
      client = aerospike.client(config).connect(self.dbConnection, self.dbPassword)
    except ex.ClientError:
      print("Error: {0} [{1}]".format(e.msg, e.code))
      sys.exit(1)
    else:
      print("Connection to aerospike successful")
      rec = {}
      # read from csv file
      csv_file = open(self.csv_file_name, mode='r')
      reader = csv.reader(csv_file, delimiter=',')
      rownum = 0
      for row in reader:
        if rownum == 0:
          header = row
        else:
          column = 0
          for col in row:
            # print (rownum,header[colnum],col)
            rec[header[column]] = col
            column += 1
        rownum += 1
        #print(rownum, rec)
        if rec:
          client.put((self.namespace, self.dbSet, str(rownum)), rec)
        rec = {}
      print("writing to aerospike set complete")
      csv_file.close()
      client.close()
      sys.exit(0)



a = main()
option = sys.argv[1]
if option == "1":
  a.read_oracle_table()
elif option == "2":
  a.write_aerospike_set()
elif option == "3":
  a.read_aerospike_set()
else:
  print("incorrect option, valid options are: 1, 2 and 3")
  sys.exit(1)
sys.exit(0)
