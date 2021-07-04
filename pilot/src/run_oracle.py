import cx_Oracle
import pprint
import csv
import sys
class testOracle:
  _username = "scratchpad"
  _password = "oracle"
  _dbschema = "SCRATCHPAD"
  _dbtable = "DUMMY"
  dump_dir = "/tmp/"
  filename = 'DUMMY.csv'
  oracleHost = 'rhes564'
  oraclePort = '1521'
  oracleDB = 'mydb12'
  serviceName = oracleDB + '.mich.local'
  dsn_tns = cx_Oracle.makedsn(oracleHost, oraclePort, service_name=serviceName)
  conn = cx_Oracle.connect(_username, _password, dsn_tns)
  cursor = conn.cursor()
  sqlTable = "SELECT COUNT(1) from USER_TABLES WHERE TABLE_NAME = '"  +_dbtable + "'"
  sql="SELECT ID, CLUSTERED, SCATTERED, RANDOMISED, RANDOM_STRING, SMALL_VC, PADDING FROM " + _dbschema + "." + _dbtable + " WHERE ROWNUM <= 10"
# Check Oracle is accessible
  try:
    conn
  except cx_Oracle.DatabaseError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(1)
  else:
    # Check if table exists
    print(sqlTable)
    cursor.execute (sqlTable)
    if cursor.fetchone()[0] == 1:
      print("\nTable " + _dbschema+"."+ _dbtable + " exists\n")
      cursor.execute(sql)
      # get column descriptions
      columns = [i[0] for i in cursor.description]
      rows = cursor.fetchall()
      # write oracle data to the csv file
      csv_file = open(dump_dir+filename, mode='w')
      writer = csv.writer(csv_file, delimiter=',', lineterminator="\n", quoting=csv.QUOTE_NONNUMERIC)
      # write column headers to csv file
      writer.writerow(columns)
      for row in rows:
        writer.writerow(row)   ## write rows to csv file
      print("writing to csv file " + dump_dir+ filename + " complete")
      cursor.close()
      conn.close()
      csv_file.close()
      sys.exit(0)
    else:
      print("Table " + _dbschema+"."+ _dbtable + " does not exist, quitting!")
      conn.close()
      sys.exit(1)
