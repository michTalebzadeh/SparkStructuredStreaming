import yaml
import sys
import os

with open("/home/hduser/dba/bin/python/DSBQ/conf/config.yml", 'r') as file:
  config: dict = yaml.safe_load(file)
  hive_url = "jdbc:hive2://" + config['hiveVariables']['hiveHost'] + ':' + config['hiveVariables']['hivePort'] + '/default'
  oracle_url = "jdbc:oracle:thin:@" + config['OracleVariables']['oracleHost'] + ":" + config['OracleVariables']['oraclePort'] + ":" + config['OracleVariables']['oracleDB']
  mysql_url = "jdbc:mysql://"+config['MysqlVariables']['MysqlHost']+"/"+config['MysqlVariables']['dbschema']
