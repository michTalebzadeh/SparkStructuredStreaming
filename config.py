import yaml
with open("../conf/config.yml", 'r') as file:
  #config: dict = yaml.load(file.read(), Loader=yaml.FullLoader)
  config: dict = yaml.safe_load(file)
  #print(config.keys())
  #print(config['hiveVariables'].items())
  hive_url = "jdbc:hive2://" + config['hiveVariables']['hiveHost'] + ':' + config['hiveVariables']['hivePort'] + '/default'
  oracle_url = "jdbc:oracle:thin:@" + config['OracleVariables']['oracleHost'] + ":" + config['OracleVariables']['oraclePort'] + ":" + config['OracleVariables']['oracleDB']
  mysql_url = "jdbc:mysql://"+config['MysqlVariables']['MysqlHost']+"/"+config['MysqlVariables']['dbschema']


with open("../conf/config_test.yml", 'r') as file:
  #ctest: dict = yaml.load(file.read(), Loader=yaml.FullLoader)
  ctest: dict = yaml.safe_load(file)
  test_url = "jdbc:mysql://"+ctest['statics']['host']+"/"+ctest['statics']['dbschema']
