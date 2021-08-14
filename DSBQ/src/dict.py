config = {
GCPVariables:
  DB: DS
  bucketname: etcbucket
  datasetLocation: europe-west2
  fullyQualifiedInputTableId: projectId+":"+inputTable
  fullyQualifiedoutputTableId: projectId+":"+outputTable
  inputTable: sourceDataset+"."+sourceTable
  jsonKeyFile: /home/hduser/GCPFirstProject-d75f1b3a9817.json
  outputTable: targetDataset+"."+targetTable
  percentYearlyHousePriceChange: percentyearlyhousepricechange
  projectId: axial-glow-224522
  sourceDataset: staging
  sourceTable: ukhouseprices
  targetDataset: ds
  targetTable: summary
  tmp_bucket: tmp_storage_bucket/tmp
  yearlyAveragePricesAllTable: yearlyaveragepricesAllTable
MDVariables:
  DB: test
  autoCommitIntervalMS: '12000'
  batchInterval: 2
  bootstrapServers: rhes75:9092,rhes75:9093,rhes75:9094,rhes564:9092,rhes564:9093,rhes564:9094,rhes76:9092,rhes76:9093,rhes76:9094
  confidenceLevel: 1.645
  currency: GBP
  datasetLocation: europe-west2
  fullyQualifiedoutputTableId: projectId+":"+outputTable
  fullyQualifiedspeedTableId: projectId+":"=speedTable
  jsonKeyFile: /home/hduser/GCPFirstProject-d75f1b3a9817.json
  mode: append
  movingAverages: 14
  newtopic: newtopic
  op_type: '1'
  outputTable: targetDataset+"."+targetTable
  priceWatch: 300.0
  projectId: axial-glow-224522
  rebalanceBackoffMS: '15000'
  rows_to_insert: '10'
  schemaRegistryURL: http://rhes75:8081
  sparkDefaultParallelism: '12'
  sparkNetworkTimeOut: '3600'
  sparkSerializer: org.apache.spark.serializer.KryoSerializer
  sparkStreamingBackpressurePidMinRate: '2000'
  sparkStreamingKafkaMaxRatePerPartition: '600'
  sparkStreamingReceiverMaxRate: '0'
  sparkStreamingUiRetainedBatches: '5'
  sparkUiRetainedJobs: '100'
  sparkWorkerUiRetainedDrivers: '5'
  sparkWorkerUiRetainedExecutors: '30'
  sparkWorkerUiRetainedStages: '100'
  speedTable: targetDataset+"."+"mdspeed"
  targetDataset: test
  targetSpeedTable: mdspeed
  targetTable: md
  tickerClass: asset
  tickerStatus: valid
  tickerType: short
  tickerWatch: VOD
  tmp_bucket: tmp_storage_bucket/tmp
  topic: md
  zookeeperConnectionTimeoutMs: '10000'
  zookeeperSessionTimeOutMs: '15000'
MysqlVariables:
  MysqlHost: localhost
  Mysql_driver: com.mysql.cj.jdbc.Driver
  Mysql_password: oracle
  Mysql_user: scratchpad
  dbschema: scratchpad
  fetchsize: '1000'
  mode: overwrite
  percentYearlyHousePriceChange: test_percentyearlyhousepricechange
  read_df_rows: 200
  sourceTable: test_ukhouseprices
  yearlyAveragePricesAllTable: test_yearlyAvgPrice
OracleVariables:
  dbschema: SCRATCHPAD
  fetchsize: '1000'
  mode: overwrite
  oracleDB: orasource
  oracleHost: rhes76
  oraclePort: '1521'
  oracle_driver: oracle.jdbc.OracleDriver
  oracle_password: oracle
  oracle_user: scratchpad
  percentYearlyHousePriceChange: percentyearlyhousepricechange
  serviceName: orasource.mich.local
  sourceTable: ukhouseprices
  yearlyAveragePricesAllTable: yearlyaveragepricesAllTable
ParquetVariables:
  append: append
  overwrite: overwrite
  sourceLocation: gs://etcbucket/randomdata/archive/randomdatapy
  sourceSmall: gs://etcbucket/randomdata/archive/randomdatasmall
  targetLocation: gs://etcbucket/randomdatapy_target
RedisVariables:
  Redis_password: hduser
  Redis_user: hduser
  keyColumn: rowkey
  mode: append
  redisDB: '1'
  redisHost: rhes76
  redisPort: '6379'
  targetTable: md
common:
  appName: md
  newtopic: newtopic
hiveVariables:
  Boston_csvlocation: hdfs://rhes75:9000/ds/Boston.csv
  DSDB: DS
  London_csvLocation: hdfs://rhes75:9000/ds/UK-HPI-full-file-2020-01.csv
  fetchsize: '1000'
  hiveHost: rhes75
  hivePort: '10099'
  hive_driver: com.cloudera.hive.jdbc41.HS2Driver
  hive_password: hduser
  hive_user: hduser
  regionname: Kensington and Chelsea
plot_fonts:
  font:
    color: darkred
    family: serif
    size: 10
    weight: normal
  font_small:
    color: darkred
    family: serif
    size: 7
    weight: normal
}

